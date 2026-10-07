using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Security.Cryptography;
using System.Text;
using System.Collections.Concurrent;
using DrasiWake.Core.Domain;

namespace DrasiWake.Core.Pipeline;

public static class BridgeTelemetry
{
    public const string ActivitySourceName = "DrasiWake.Bridge";
    public const string MeterName = "DrasiWake.Bridge";
    public static readonly ActivitySource ActivitySource = new(ActivitySourceName, "1.0.0");
    private static readonly Meter Meter = new(MeterName, "1.0.0");
    private static readonly Counter<long> SignalsReceived = Meter.CreateCounter<long>("drasiwake.signals.received");
    private static readonly Counter<long> SignalOverflows = Meter.CreateCounter<long>("drasiwake.signals.overflow");
    private static readonly UpDownCounter<long> SignalQueueDepth = Meter.CreateUpDownCounter<long>("drasiwake.signals.queue.depth");
    private static readonly Counter<long> Reconciliations = Meter.CreateCounter<long>("drasiwake.reconciliations");
    private static readonly Counter<long> VisibleQueries = Meter.CreateCounter<long>("drasiwake.queries.visible");
    private static readonly Counter<long> RouteOutcomes = Meter.CreateCounter<long>("drasiwake.routes");
    private static readonly Counter<long> OutboxCreated = Meter.CreateCounter<long>("drasiwake.outbox.created");
    private static readonly Counter<long> SnapshotFailures = Meter.CreateCounter<long>("drasiwake.snapshots.failures");
    private static readonly Counter<long> DuplicateFingerprints = Meter.CreateCounter<long>("drasiwake.snapshots.duplicates");
    private static readonly Counter<long> AcceptedWakes = Meter.CreateCounter<long>("drasiwake.wakes.accepted");
    private static readonly Counter<long> RetriedWakes = Meter.CreateCounter<long>("drasiwake.wakes.retries");
    private static readonly Counter<long> DeadLetterWakes = Meter.CreateCounter<long>("drasiwake.wakes.dead_letters");
    private static readonly Counter<long> ExecutionStatusFailures = Meter.CreateCounter<long>("drasiwake.wakes.execution_status.failures");
    private static readonly Histogram<double> QueueAge = Meter.CreateHistogram<double>("drasiwake.outbox.queue.age", "ms");
    private static readonly Histogram<double> AcceptanceLatency = Meter.CreateHistogram<double>("drasiwake.wakes.acceptance.latency", "ms");
    private static readonly Counter<long> RaftLeaderChanges = Meter.CreateCounter<long>("drasiwake.raft.leader.changes");
    private static readonly Counter<long> RaftMembership = Meter.CreateCounter<long>("drasiwake.raft.membership");
    private static readonly Counter<long> RaftSnapshots = Meter.CreateCounter<long>("drasiwake.raft.snapshots");
    private static readonly Counter<long> RaftApplies = Meter.CreateCounter<long>("drasiwake.raft.apply");
    private static readonly Histogram<double> RaftSnapshotDuration = Meter.CreateHistogram<double>("drasiwake.raft.snapshot.duration", "ms");
    private static readonly Histogram<double> RaftApplyDuration = Meter.CreateHistogram<double>("drasiwake.raft.apply.duration", "ms");
    private static readonly Histogram<double> RaftRecoveryDuration = Meter.CreateHistogram<double>("drasiwake.raft.leader.recovery.duration", "ms");
    private static readonly Histogram<double> RaftFailoverDuration = Meter.CreateHistogram<double>("drasiwake.raft.failover.duration", "ms");
    private static readonly Histogram<double> RaftCatchupDuration = Meter.CreateHistogram<double>("drasiwake.raft.follower.catchup.duration", "ms");
    private static readonly ConcurrentDictionary<long, Func<RaftNodeObservation>> RaftNodes = new();
    private static long nextRaftNode;
    private static readonly ObservableGauge<long> RaftRole = Meter.CreateObservableGauge(
        "drasiwake.raft.node.role", () => ObserveRaftNodes(static node =>
            new Measurement<long>(1, new KeyValuePair<string, object?>("role", node.Role))));
    private static readonly ObservableGauge<long> RaftQuorum = Meter.CreateObservableGauge(
        "drasiwake.raft.quorum", () => ObserveRaftNodes(static node => new Measurement<long>(node.HasQuorum ? 1 : 0)));
    private static readonly ObservableGauge<long> RaftWritable = Meter.CreateObservableGauge(
        "drasiwake.raft.writable", () => ObserveRaftNodes(static node =>
            new Measurement<long>(node.IsLeader && node.HasQuorum ? 1 : 0)));
    private static readonly ObservableGauge<long> RaftCommitLag = Meter.CreateObservableGauge(
        "drasiwake.raft.commit.lag", () => ObserveRaftNodes(static node =>
            new Measurement<long>(Math.Max(0, node.LastIndex - node.CommittedIndex))), "{entry}");
    private static readonly ObservableGauge<long> RaftApplyLag = Meter.CreateObservableGauge(
        "drasiwake.raft.apply.lag", () => ObserveRaftNodes(static node =>
            new Measurement<long>(Math.Max(0, node.CommittedIndex - node.AppliedIndex))), "{entry}");
    private static readonly ObservableGauge<long> RaftFollowerCatchupLag = Meter.CreateObservableGauge(
        "drasiwake.raft.follower.catchup.lag", () => ObserveRaftNodes(static node =>
            new Measurement<long>(node.IsLeader ? 0 : Math.Max(0, node.CommittedIndex - node.AppliedIndex))), "{entry}");

    public static Activity? StartLeaderRecovery() => ActivitySource.StartActivity("raft.leader.recovery");

    public static Activity? StartMembership(bool add, string? endpoint)
    {
        var activity = ActivitySource.StartActivity("raft.membership");
        activity?.SetTag("operation", add ? "add" : "remove");
        activity?.SetTag("member.id", StableId(endpoint ?? string.Empty));
        return activity;
    }

    public static void RecordMembership(bool add, string result)
        => RaftMembership.Add(1, new TagList { { "operation", add ? "add" : "remove" }, { "result", result } });

    public static void RecordLeaderChange() => RaftLeaderChanges.Add(1);

    public static void RecordLeaderRecovery(TimeSpan duration, string result)
        => RaftRecoveryDuration.Record(duration.TotalMilliseconds, new TagList { { "result", result } });

    public static void RecordFailover(TimeSpan duration) => RaftFailoverDuration.Record(duration.TotalMilliseconds);

    public static void RecordFollowerCatchup(TimeSpan duration) => RaftCatchupDuration.Record(duration.TotalMilliseconds);

    public static void RecordRaftApply(TimeSpan duration, string result)
    {
        var tags = new TagList { { "result", result } };
        RaftApplies.Add(1, tags);
        RaftApplyDuration.Record(duration.TotalMilliseconds, tags);
    }

    public static void RecordRaftSnapshot(bool restore, TimeSpan duration, string result)
    {
        var tags = new TagList { { "operation", restore ? "restore" : "export" }, { "result", result } };
        RaftSnapshots.Add(1, tags);
        RaftSnapshotDuration.Record(duration.TotalMilliseconds, tags);
    }

    public static IDisposable RegisterRaftNode(Func<RaftNodeObservation> observe)
    {
        ArgumentNullException.ThrowIfNull(observe);
        var id = Interlocked.Increment(ref nextRaftNode);
        RaftNodes[id] = observe;
        return new RaftNodeRegistration(id);
    }

    private static IEnumerable<Measurement<long>> ObserveRaftNodes(
        Func<RaftNodeObservation, Measurement<long>> measurement)
    {
        foreach (var observe in RaftNodes.Values)
        {
            RaftNodeObservation node;
            try { node = observe(); }
            catch (ObjectDisposedException) { continue; }
            yield return measurement(node);
        }
    }

    public readonly record struct RaftNodeObservation(
        bool IsLeader, bool HasQuorum, bool HasKnownLeader, long LastIndex, long CommittedIndex, long AppliedIndex)
    {
        public string Role => IsLeader ? "leader" : HasKnownLeader ? "follower" : "electing";
    }

    private sealed class RaftNodeRegistration(long id) : IDisposable
    {
        public void Dispose() => RaftNodes.TryRemove(id, out _);
    }

    public static Activity? StartReconciliation(QueryIdentity query)
    {
        var activity = ActivitySource.StartActivity("snapshot.reconcile");
        activity?.SetTag("query.id", StableId($"{query.Server.AbsoluteUri}\n{query.InstanceId}\n{query.QueryId}"));
        return activity;
    }

    public static Activity? StartDispatch(WakeOutboxItem item)
    {
        var activity = ActivitySource.StartActivity("outbox.dispatch");
        activity?.SetTag("binding.id", StableId(item.BindingId));
        activity?.SetTag("contract.version", item.ContractVersion);
        return activity;
    }

    public static void RecordSignalQueued(QueryIdentity query)
    {
        var tags = QueryTags(query);
        SignalsReceived.Add(1, tags);
        SignalQueueDepth.Add(1, tags);
    }

    public static void RecordSignalDequeued(QueryIdentity query)
        => SignalQueueDepth.Add(-1, QueryTags(query));

    public static void RecordSignalOverflow(QueryIdentity query)
        => SignalOverflows.Add(1, QueryTags(query));

    public static void RecordReconciliation(QueryIdentity query)
        => Reconciliations.Add(1, QueryTags(query));

    public static void RecordQueryVisible(QueryIdentity query)
        => VisibleQueries.Add(1, QueryTags(query));

    public static void RecordRoute(QueryIdentity query, string? bindingId, string outcome, string? contractVersion = null)
    {
        var tags = new TagList
        {
            { "query.id", StableId($"{query.Server.AbsoluteUri}\n{query.InstanceId}\n{query.QueryId}") },
            { "route.outcome", outcome }
        };
        if (bindingId is not null)
            tags.Add("binding.id", StableId(bindingId));
        if (contractVersion is not null)
            tags.Add("contract.version", contractVersion);
        RouteOutcomes.Add(1, tags);
    }

    public static void RecordOutboxCreated(WakeOutboxItem item)
        => OutboxCreated.Add(1, WakeTags(item));

    public static void RecordSnapshotFailure(QueryIdentity query)
        => SnapshotFailures.Add(1, QueryTags(query));

    public static void RecordDuplicateFingerprint(string bindingId)
        => DuplicateFingerprints.Add(1, new TagList { { "binding.id", StableId(bindingId) } });

    public static void RecordQueueAge(WakeOutboxItem item, TimeSpan age)
        => QueueAge.Record(age.TotalMilliseconds, WakeTags(item));

    public static void RecordAccepted(WakeOutboxItem item)
        => AcceptedWakes.Add(1, WakeTags(item));

    public static void RecordAcceptanceLatency(WakeOutboxItem item, TimeSpan latency)
        => AcceptanceLatency.Record(latency.TotalMilliseconds, WakeTags(item));

    public static void RecordRetry(WakeOutboxItem item, string reasonCode)
        => RetriedWakes.Add(1, WakeTags(item, reasonCode));

    public static void RecordDeadLetter(WakeOutboxItem item, string reasonCode)
        => DeadLetterWakes.Add(1, WakeTags(item, reasonCode));

    public static void RecordExecutionStatusFailure(WakeOutboxItem item)
        => ExecutionStatusFailures.Add(1, WakeTags(item));

    public static string StableId(string value)
        => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value)))[..16].ToLowerInvariant();

    private static TagList QueryTags(QueryIdentity query) => new()
    {
        { "query.id", StableId($"{query.Server.AbsoluteUri}\n{query.InstanceId}\n{query.QueryId}") }
    };

    private static TagList WakeTags(WakeOutboxItem item, string? reasonCode = null)
    {
        var tags = new TagList
        {
            { "binding.id", StableId(item.BindingId) },
            { "contract.version", item.ContractVersion }
        };
        if (reasonCode is not null)
            tags.Add("reason.code", reasonCode);
        return tags;
    }
}