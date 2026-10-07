using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Pipeline;

namespace DrasiWake.Host;

public sealed class ReconciliationHostedService(
    IChangeSource changeSource,
    SignalInbox inbox,
    RecoveryCoordinator recovery,
    DrasiWakeHostSettings settings,
    TimeProvider timeProvider) : ILeaderEpochWorker, IDisposable
{
    private CancellationTokenSource? stopping;
    private Task? periodicTask;

    public Task Completion => periodicTask ?? Task.CompletedTask;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        stopping = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        periodicTask = RunPeriodicAsync(stopping.Token);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (stopping is null || periodicTask is null)
            return;

        stopping.Cancel();
        try
        {
            await periodicTask.WaitAsync(settings.ShutdownTimeout, timeProvider, cancellationToken);
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
        }
    }

    public void Dispose() => stopping?.Dispose();

    private async Task RunPeriodicAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(settings.ReconciliationInterval, timeProvider);
        while (await timer.WaitForNextTickAsync(cancellationToken))
        {
            var visibleQueries = await changeSource.EnumerateQueriesAsync(cancellationToken);
            foreach (var query in visibleQueries)
                BridgeTelemetry.RecordQueryVisible(query);
            var queries = inbox.TakeDirtyQueries()
                .Concat(visibleQueries)
                .Distinct()
                .ToArray();
            foreach (var query in queries)
            {
                await recovery.ReconcileAfterReconnectAsync(query, cancellationToken);
            }
        }
    }
}