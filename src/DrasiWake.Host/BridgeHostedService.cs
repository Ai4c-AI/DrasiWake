using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Pipeline;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DrasiWake.Host;

public sealed class BridgeHostedService(
    IChangeSource changeSource,
    IBridgeStore store,
    SignalInbox inbox,
    SessionPartitioner partitioner,
    RecoveryCoordinator recovery,
    DrasiWakeHostSettings settings,
    TimeProvider timeProvider,
    ILogger<BridgeHostedService> logger) : IHostedService, IDisposable
{
    private CancellationTokenSource? stopping;
    private Task? partitionerTask;
    private Task? receiverTask;
    private Task? signalTask;
    private Task? dispatchTask;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        stopping = new CancellationTokenSource();
        partitionerTask = partitioner.RunAsync(stopping.Token);
        receiverTask = ReceiveSignalsAsync(stopping.Token);
        signalTask = ProcessSignalsAsync(stopping.Token);
        dispatchTask = DispatchOutboxAsync(stopping.Token);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (stopping is null)
            return;

        stopping.Cancel();
        inbox.TryComplete();
        await partitioner.StopAsync(settings.ShutdownTimeout, cancellationToken);
        var tasks = new[] { receiverTask, signalTask, dispatchTask, partitionerTask }
            .Where(task => task is not null)
            .Cast<Task>()
            .ToArray();
        try
        {
            await Task.WhenAll(tasks).WaitAsync(settings.ShutdownTimeout, timeProvider, cancellationToken);
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
        }
    }

    public void Dispose() => stopping?.Dispose();

    private async Task ReceiveSignalsAsync(CancellationToken cancellationToken)
    {
        try
        {
            await inbox.ReceiveAsync(changeSource.WatchAsync(cancellationToken), cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            logger.LogError("Drasi signal receiver stopped ({ErrorType}).", exception.GetType().Name);
        }
    }

    private async Task ProcessSignalsAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var signal in inbox.ReadAllAsync(cancellationToken))
            {
                try
                {
                    await recovery.ReconcileAfterReconnectAsync(signal.Query, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exception)
                {
                    logger.LogWarning("Snapshot reconciliation failed for query {QueryId} ({ErrorType}).",
                        BridgeTelemetry.StableId(signal.Query.QueryId), exception.GetType().Name);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private async Task DispatchOutboxAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                if (recovery.IsInitialized)
                {
                    var items = await store.LoadDispatchableAsync(
                        timeProvider.GetUtcNow(), settings.WorkerCount, cancellationToken);
                    foreach (var item in items)
                        await partitioner.EnqueueAsync(item, cancellationToken);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                logger.LogError("Outbox poll failed ({ErrorType}).", exception.GetType().Name);
            }

            try
            {
                await Task.Delay(settings.DispatchPollInterval, timeProvider, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
        }
    }
}