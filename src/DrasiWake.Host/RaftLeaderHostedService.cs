using System.Runtime.CompilerServices;
using DotNext.Net.Cluster.Consensus.Raft;
using DrasiWake.Persistence.Raft;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using DrasiWake.Core.Pipeline;
using DotNext.Net.Cluster.Consensus.Raft.StateMachine;

namespace DrasiWake.Host;

public interface IRaftLeadership
{
    bool IsLeader { get; }

    bool HasQuorum { get; }

    long Term { get; }

    IAsyncEnumerable<RaftLeadershipChange> WatchLeadershipAsync(CancellationToken cancellationToken);
}

public sealed record RaftLeadershipChange(long Term, bool IsLeader, bool HasQuorum);

public sealed class DotNextRaftLeadership(IRaftCluster cluster, TimeProvider timeProvider) : IRaftLeadership
{
    private static readonly TimeSpan LeadershipPollInterval = TimeSpan.FromMilliseconds(100);
    private readonly IRaftCluster _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
    private readonly TimeProvider _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));

    public bool IsLeader => !_cluster.LeadershipToken.IsCancellationRequested;

    public bool HasQuorum =>
        !_cluster.ConsensusToken.IsCancellationRequested &&
        DotNextRaftQuorum.IsAvailable(_cluster);

    public long Term => _cluster.AuditTrail.Term;

    public async IAsyncEnumerable<RaftLeadershipChange> WatchLeadershipAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await _cluster.Readiness.WaitAsync(cancellationToken).ConfigureAwait(false);
        using var telemetry = BridgeTelemetry.RegisterRaftNode(ObserveNode);
        RaftLeadershipChange? previous = null;
        long? catchupStarted = null;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var observation = ObserveNode();
            if (!observation.IsLeader && observation.CommittedIndex > observation.AppliedIndex)
                catchupStarted ??= Stopwatch.GetTimestamp();
            else if (catchupStarted is { } started)
            {
                if (!observation.IsLeader)
                    BridgeTelemetry.RecordFollowerCatchup(Stopwatch.GetElapsedTime(started));
                catchupStarted = null;
            }
            var current = new RaftLeadershipChange(Term, IsLeader, HasQuorum);
            if (current != previous)
            {
                previous = current;
                yield return current;
            }

            var notification = await WaitForLeadershipNotificationAsync(cancellationToken).ConfigureAwait(false);
            if (notification.LeadershipLost || notification.QuorumLost)
            {
                current = new RaftLeadershipChange(
                    Term,
                    notification.LeadershipLost ? false : IsLeader,
                    notification.QuorumLost ? false : HasQuorum);
                if (current != previous)
                {
                    previous = current;
                    yield return current;
                }
            }
        }
    }

    private BridgeTelemetry.RaftNodeObservation ObserveNode()
    {
        var trail = _cluster.AuditTrail;
        return new(IsLeader, HasQuorum, _cluster.Leader is not null,
            trail.LastEntryIndex, trail.LastCommittedEntryIndex,
            trail is WriteAheadLog log ? log.LastAppliedIndex : 0);
    }

    private async Task<LeadershipNotification> WaitForLeadershipNotificationAsync(CancellationToken cancellationToken)
    {
        using var waitCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var leadershipToken = _cluster.LeadershipToken;
        var consensusToken = _cluster.ConsensusToken;
        using var leadershipCancellation = CancellationTokenSource.CreateLinkedTokenSource(
            leadershipToken, waitCancellation.Token);
        using var quorumCancellation = CancellationTokenSource.CreateLinkedTokenSource(
            consensusToken, waitCancellation.Token);
        var poll = Task.Delay(LeadershipPollInterval, _timeProvider, waitCancellation.Token);
        var notifications = new List<Task>(3) { poll };
        Task? leadershipLost = null;
        Task? quorumLost = null;
        if (!leadershipToken.IsCancellationRequested)
        {
            leadershipLost = Task.Delay(Timeout.InfiniteTimeSpan, leadershipCancellation.Token);
            notifications.Add(leadershipLost);
        }
        if (!consensusToken.IsCancellationRequested)
        {
            quorumLost = Task.Delay(Timeout.InfiniteTimeSpan, quorumCancellation.Token);
            notifications.Add(quorumLost);
        }

        var completed = await Task.WhenAny(notifications).ConfigureAwait(false);
        waitCancellation.Cancel();
        try
        {
            await Task.WhenAll(notifications).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Every losing wait is cancelled and observed before disposing its registrations.
        }
        cancellationToken.ThrowIfCancellationRequested();
        return new LeadershipNotification(completed == leadershipLost, completed == quorumLost);
    }

    private readonly record struct LeadershipNotification(bool LeadershipLost, bool QuorumLost);
}

public interface ILeaderWorkerRuntime
{
    Task Completion { get; }

    Task StartAsync(CancellationToken cancellationToken);

    Task StopAsync(CancellationToken cancellationToken);
}

public sealed class RaftLeaderHostedService(
    IRaftLeadership leadership,
    IServiceScopeFactory scopeFactory,
    ILogger<RaftLeaderHostedService> logger,
    TimeProvider? timeProvider = null) : BackgroundService
{
    private const string StartupFailureCategory = "leader-startup-failure";
    private const string WorkerFailureCategory = "leader-worker-failure";
    private const string StopFailureCategory = "leader-stop-failure";
    private readonly IRaftLeadership _leadership = leadership ?? throw new ArgumentNullException(nameof(leadership));
    private readonly IServiceScopeFactory _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
    private readonly ILogger<RaftLeaderHostedService> _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    private readonly TimeProvider _timeProvider = timeProvider ?? TimeProvider.System;
    private long? unavailableSince;
    private long? lastLeaderTerm;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Epoch? epoch = null;
        RaftLeadershipChange? retryChange = null;
        var retryAttempt = 0;
        try
        {
            using var watchCancellation = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            await using var changes = _leadership.WatchLeadershipAsync(watchCancellation.Token)
                .GetAsyncEnumerator(watchCancellation.Token);
            Task<bool> nextChange = changes.MoveNextAsync().AsTask();
            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    if (epoch is null)
                    {
                        if (retryChange is { } retry)
                        {
                            retryChange = null;
                            using var backoffCancellation = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                            var delay = Task.Delay(
                                TimeSpan.FromSeconds(Math.Min(5, Math.Pow(2, Math.Min(retryAttempt++, 3)))),
                                _timeProvider, backoffCancellation.Token);
                            var completed = await Task.WhenAny(nextChange, delay).ConfigureAwait(false);
                            backoffCancellation.Cancel();
                            try { await delay.ConfigureAwait(false); }
                            catch (OperationCanceledException) when (backoffCancellation.IsCancellationRequested) { }
                            stoppingToken.ThrowIfCancellationRequested();
                            if (completed != nextChange && _leadership.IsLeader && _leadership.HasQuorum &&
                                _leadership.Term == retry.Term)
                            {
                                epoch = await CreateEpochAsync(retry, stoppingToken).ConfigureAwait(false);
                                if (!await StartEpochAsync(epoch, nextChange).ConfigureAwait(false))
                                {
                                    if (epoch.RetryStartup)
                                        retryChange = retry;
                                    epoch = null;
                                }
                                else
                                    retryAttempt = 0;
                                continue;
                            }
                        }
                        if (!await nextChange.ConfigureAwait(false))
                            return;

                        var change = changes.Current;
                        ObserveChange(change);
                        nextChange = changes.MoveNextAsync().AsTask();
                        if (IsEligible(change))
                        {
                            epoch = await CreateEpochAsync(change, stoppingToken).ConfigureAwait(false);
                            if (!await StartEpochAsync(epoch, nextChange).ConfigureAwait(false))
                            {
                                if (epoch.RetryStartup)
                                    retryChange = change;
                                epoch = null;
                            }
                            else
                                retryAttempt = 0;
                        }

                        continue;
                    }

                    var completedTask = await Task.WhenAny(nextChange, epoch.Runtime.Completion).ConfigureAwait(false);
                    if (completedTask == nextChange)
                    {
                        if (!await nextChange.ConfigureAwait(false))
                            return;

                        var change = changes.Current;
                        ObserveChange(change);
                        nextChange = changes.MoveNextAsync().AsTask();
                        if (IsEligible(change) && change.Term == epoch.Term)
                            continue;

                        unavailableSince ??= Stopwatch.GetTimestamp();
                        await StopEpochAsync(epoch).ConfigureAwait(false);
                        epoch = null;
                        if (IsEligible(change))
                        {
                            epoch = await CreateEpochAsync(change, stoppingToken).ConfigureAwait(false);
                            if (!await StartEpochAsync(epoch, nextChange).ConfigureAwait(false))
                            {
                                if (epoch.RetryStartup)
                                    retryChange = change;
                                epoch = null;
                            }
                            else
                                retryAttempt = 0;
                        }

                        continue;
                    }

                    try
                    {
                        await epoch.Runtime.Completion.ConfigureAwait(false);
                        if (!stoppingToken.IsCancellationRequested)
                            _logger.LogError("Raft leader epoch failed with category {FailureCategory}.", WorkerFailureCategory);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                    }
                    catch
                    {
                        _logger.LogError("Raft leader epoch failed with category {FailureCategory}.", WorkerFailureCategory);
                    }

                    unavailableSince ??= Stopwatch.GetTimestamp();
                    await StopEpochAsync(epoch).ConfigureAwait(false);
                    epoch = null;
                }
            }
            finally
            {
                watchCancellation.Cancel();
                try
                {
                    await nextChange.ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (watchCancellation.IsCancellationRequested)
                {
                }
            }
        }
        finally
        {
            if (epoch is not null)
                await StopEpochAsync(epoch).ConfigureAwait(false);
        }
    }

    private async Task<Epoch> CreateEpochAsync(RaftLeadershipChange change, CancellationToken stoppingToken)
    {
        var scope = _scopeFactory.CreateAsyncScope();
        try
        {
            var runtime = scope.ServiceProvider.GetRequiredService<ILeaderWorkerRuntime>();
            var epoch = new Epoch(
                change.Term,
                scope,
                runtime,
                CancellationTokenSource.CreateLinkedTokenSource(stoppingToken));
            if (lastLeaderTerm != change.Term)
            {
                BridgeTelemetry.RecordLeaderChange();
                lastLeaderTerm = change.Term;
            }
            epoch.RecoveryActivity = BridgeTelemetry.StartLeaderRecovery();
            try
            {
                epoch.Startup = runtime.StartAsync(epoch.Cancellation.Token);
            }
            catch (Exception exception)
            {
                epoch.Startup = Task.FromException(exception);
            }
            return epoch;
        }
        catch
        {
            await scope.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private async Task<bool> StartEpochAsync(Epoch epoch, Task<bool> nextChange)
    {
        if (await Task.WhenAny(epoch.Startup, nextChange).ConfigureAwait(false) == nextChange)
        {
            await StopEpochAsync(epoch).ConfigureAwait(false);
            return false;
        }

        try
        {
            await epoch.Startup.ConfigureAwait(false);
            CompleteRecovery(epoch, "succeeded");
            if (unavailableSince is { } unavailable)
            {
                BridgeTelemetry.RecordFailover(Stopwatch.GetElapsedTime(unavailable));
                unavailableSince = null;
            }
            return true;
        }
        catch (OperationCanceledException) when (epoch.Cancellation.IsCancellationRequested)
        {
            CompleteRecovery(epoch, "cancelled");
            await StopEpochAsync(epoch).ConfigureAwait(false);
            return false;
        }
        catch (Exception exception)
        {
            epoch.RetryStartup = exception is RetryableLeaderRecoveryException;
            if (epoch.RetryStartup)
                _logger.LogWarning("Raft leader recovery will retry with category {FailureCategory}.",
                    "leader-recovery-transient");
            CompleteRecovery(epoch, "failed");
            await StopEpochAsync(epoch).ConfigureAwait(false);
            return false;
        }
    }

    private async Task StopEpochAsync(Epoch epoch)
    {
        if (!epoch.TryBeginStop())
            return;

        epoch.Cancellation.Cancel();
        CompleteRecovery(epoch, epoch.Startup.IsFaulted ? "failed" : "cancelled");
        try
        {
            await epoch.Startup.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (epoch.Cancellation.IsCancellationRequested)
        {
        }
        catch
        {
            _logger.LogError("Raft leader epoch failed with category {FailureCategory}.", StartupFailureCategory);
        }

        try
        {
            await epoch.Runtime.StopAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
            _logger.LogError("Raft leader epoch failed with category {FailureCategory}.", StopFailureCategory);
            throw new InvalidOperationException("Raft leader epoch stop failed.");
        }
        finally
        {
            await epoch.Scope.DisposeAsync().ConfigureAwait(false);
            epoch.Cancellation.Dispose();
        }
    }

    private static bool IsEligible(RaftLeadershipChange change) => change.IsLeader && change.HasQuorum;

    private void ObserveChange(RaftLeadershipChange change)
    {
        if (!IsEligible(change))
            unavailableSince ??= Stopwatch.GetTimestamp();
    }

    private static void CompleteRecovery(Epoch epoch, string result)
    {
        if (epoch.RecoveryCompleted)
            return;
        epoch.RecoveryCompleted = true;
        epoch.RecoveryActivity?.SetTag("result", result);
        epoch.RecoveryActivity?.Dispose();
        BridgeTelemetry.RecordLeaderRecovery(Stopwatch.GetElapsedTime(epoch.StartedAt), result);
    }

    private sealed class Epoch(
        long term,
        AsyncServiceScope scope,
        ILeaderWorkerRuntime runtime,
        CancellationTokenSource cancellation)
    {
        public long Term { get; } = term;
        public AsyncServiceScope Scope { get; } = scope;
        public ILeaderWorkerRuntime Runtime { get; } = runtime;
        public CancellationTokenSource Cancellation { get; } = cancellation;
        public Task Startup { get; set; } = Task.CompletedTask;
        public long StartedAt { get; } = Stopwatch.GetTimestamp();
        public Activity? RecoveryActivity { get; set; }
        public bool RecoveryCompleted { get; set; }
        public bool RetryStartup { get; set; }
        private int stopping;

        public bool TryBeginStop() => Interlocked.Exchange(ref stopping, 1) == 0;
    }
}
