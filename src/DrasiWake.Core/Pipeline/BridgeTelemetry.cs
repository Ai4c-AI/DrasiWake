using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Security.Cryptography;
using System.Text;
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