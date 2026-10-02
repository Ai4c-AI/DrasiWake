using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Pipeline;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DrasiWake.Host;

public sealed class ReconciliationHostedService(
    IChangeSource changeSource,
    SignalInbox inbox,
    RecoveryCoordinator recovery,
    DrasiWakeHostSettings settings,
    TimeProvider timeProvider,
    ILogger<ReconciliationHostedService> logger) : IHostedService, IDisposable
{
    private CancellationTokenSource? stopping;
    private Task? periodicTask;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await recovery.RecoverAsync(cancellationToken);
        stopping = new CancellationTokenSource();
        periodicTask = RunPeriodicAsync(stopping.Token);
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
            try
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
                    try
                    {
                        await recovery.ReconcileAfterReconnectAsync(query, cancellationToken);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (Exception exception)
                    {
                        logger.LogWarning("Periodic snapshot reconciliation failed for query {QueryId} ({ErrorType}).",
                            BridgeTelemetry.StableId(query.QueryId), exception.GetType().Name);
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                logger.LogError("Periodic query enumeration failed ({ErrorType}).", exception.GetType().Name);
            }
        }
    }
}