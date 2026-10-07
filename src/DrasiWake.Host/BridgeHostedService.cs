using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Pipeline;
using Microsoft.Extensions.Hosting;

namespace DrasiWake.Host;

public sealed class BridgeHostedService(
    IChangeSource changeSource,
    IBridgeStore store,
    SignalInbox inbox,
    SessionPartitioner partitioner,
    RecoveryCoordinator recovery,
    DrasiWakeHostSettings settings,
    TimeProvider timeProvider) : ILeaderEpochWorker, IDisposable
{
    private CancellationTokenSource? stopping;
    private Task? completion;
    private Task? partitionerTask;
    private Task? receiverTask;
    private Task? signalTask;
    private Task? dispatchTask;

    public Task Completion => completion ?? Task.CompletedTask;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        stopping = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        partitionerTask = partitioner.RunAsync(stopping.Token);
        receiverTask = ReceiveSignalsAsync(stopping.Token);
        signalTask = ProcessSignalsAsync(stopping.Token);
        dispatchTask = DispatchOutboxAsync(stopping.Token);
        completion = SuperviseWorkersAsync(stopping.Token);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (stopping is null)
            return;

        stopping.Cancel();
        inbox.TryComplete();
        await partitioner.StopAsync(settings.ShutdownTimeout, cancellationToken);
        try
        {
            await Completion.WaitAsync(settings.ShutdownTimeout, timeProvider, cancellationToken);
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
        }
    }

    public void Dispose() => stopping?.Dispose();

    private async Task ReceiveSignalsAsync(CancellationToken cancellationToken)
    {
        await inbox.ReceiveAsync(changeSource.WatchAsync(cancellationToken), cancellationToken);
    }

    private async Task ProcessSignalsAsync(CancellationToken cancellationToken)
    {
        await foreach (var signal in inbox.ReadAllAsync(cancellationToken))
        {
            await recovery.ReconcileAfterReconnectAsync(signal.Query, cancellationToken);
        }
    }

    private async Task DispatchOutboxAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            if (recovery.IsInitialized)
            {
                var items = await store.LoadDispatchableAsync(
                    timeProvider.GetUtcNow(), settings.WorkerCount, cancellationToken);
                foreach (var item in items)
                    await partitioner.EnqueueAsync(item, cancellationToken);
            }

            await Task.Delay(settings.DispatchPollInterval, timeProvider, cancellationToken);
        }
    }

    private async Task SuperviseWorkersAsync(CancellationToken cancellationToken)
    {
        var tasks = new[] { receiverTask!, signalTask!, dispatchTask!, partitionerTask! };
        var firstCompleted = await Task.WhenAny(tasks).ConfigureAwait(false);
        if (!cancellationToken.IsCancellationRequested)
            stopping?.Cancel();

        await Task.WhenAll(tasks).ConfigureAwait(false);
        await firstCompleted.ConfigureAwait(false);
        if (!cancellationToken.IsCancellationRequested)
            throw new InvalidOperationException("A bridge worker completed unexpectedly.");
    }
}