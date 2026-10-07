using DrasiWake.Core.Pipeline;

namespace DrasiWake.Host;

public interface ILeaderEpochWorker
{
    Task Completion { get; }

    Task StartAsync(CancellationToken cancellationToken);

    Task StopAsync(CancellationToken cancellationToken);
}

public sealed class LeaderWorkerRuntime(
    IRaftLeaderStartupPreparation preparation,
    IRecoveryCoordinator recovery,
    IEnumerable<ILeaderEpochWorker> workers) : ILeaderWorkerRuntime
{
    private readonly IRaftLeaderStartupPreparation _preparation =
        preparation ?? throw new ArgumentNullException(nameof(preparation));
    private readonly IRecoveryCoordinator _recovery = recovery ?? throw new ArgumentNullException(nameof(recovery));
    private readonly ILeaderEpochWorker[] _workers = workers?.ToArray() ?? throw new ArgumentNullException(nameof(workers));
    private readonly List<ILeaderEpochWorker> startedWorkers = [];
    private CancellationTokenSource? stopping;
    private Task completion = Task.CompletedTask;
    private int started;

    public Task Completion => completion;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref started, 1) != 0)
            throw new InvalidOperationException("A leader worker runtime can only be started once.");

        stopping = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        await _preparation.PrepareBeforeRecoveryAsync(stopping.Token).ConfigureAwait(false);
        await _recovery.RecoverAsync(stopping.Token, enqueueDispatchableItems: false).ConfigureAwait(false);
        foreach (var worker in _workers)
        {
            stopping.Token.ThrowIfCancellationRequested();
            await worker.StartAsync(stopping.Token).ConfigureAwait(false);
            startedWorkers.Add(worker);
        }

        completion = ObserveWorkerCompletionAsync(stopping.Token);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        stopping?.Cancel();
        if (stopping is null)
            return;

        try
        {
            var stopTasks = startedWorkers
                .Select(worker => worker.StopAsync(cancellationToken))
                .ToArray();
            await Task.WhenAll(stopTasks).ConfigureAwait(false);
        }
        finally
        {
            stopping.Dispose();
            stopping = null;
        }
    }

    private async Task ObserveWorkerCompletionAsync(CancellationToken cancellationToken)
    {
        if (startedWorkers.Count == 0)
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
            return;
        }

        var firstCompleted = await Task.WhenAny(startedWorkers.Select(worker => worker.Completion))
            .ConfigureAwait(false);
        await firstCompleted.ConfigureAwait(false);
        if (!cancellationToken.IsCancellationRequested)
            throw new InvalidOperationException("A leader epoch worker completed unexpectedly.");
    }
}
