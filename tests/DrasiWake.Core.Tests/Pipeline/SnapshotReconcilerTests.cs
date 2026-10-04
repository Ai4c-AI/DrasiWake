using System.Runtime.CompilerServices;
using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Contracts;
using DrasiWake.Core.Domain;
using DrasiWake.Core.Pipeline;

namespace DrasiWake.Core.Tests.Pipeline;

public sealed class SnapshotReconcilerTests
{
    [Fact]
    public async Task Repeated_observation_of_the_same_snapshot_creates_one_pending_wake()
    {
        var query = new QueryIdentity(new Uri("http://drasi.test"), null, "orders");
        var snapshot = new QuerySnapshot(query, [System.Text.Json.Nodes.JsonNode.Parse("""{"orderId":"order-1"}""")]);
        var store = new RecordingStore(new RecoveryState([], []));
        var source = new StaticSnapshotSource(snapshot);
        var registry = CreateRegistry(query);
        var reconciler = new SnapshotReconciler(source, store, registry);

        await reconciler.ReconcileAsync(query, CancellationToken.None);
        await reconciler.ReconcileAsync(query, CancellationToken.None);

        Assert.Single(store.OutboxItems);
        Assert.Equal("triage-order", store.OutboxItems[0].Skill);
        Assert.Equal(WakeOutboxStatus.Pending, store.OutboxItems[0].Status);
        Assert.Equal("1.0.0", store.OutboxItems[0].Input["contractVersion"]!.GetValue<string>());
        Assert.Equal("order-1", store.OutboxItems[0].Input["facts"]![0]!["orderId"]!.GetValue<string>());
        var targetProperty = typeof(WakeOutboxItem).GetProperty("OpenClawTarget");
        Assert.NotNull(targetProperty);
        Assert.Equal("sample-gateway", targetProperty.GetValue(store.OutboxItems[0]));
    }

    [Fact]
    public async Task Fact_schema_failure_is_recorded_as_auditable_dead_letter()
    {
        var query = new QueryIdentity(new Uri("http://drasi.test"), null, "orders");
        var snapshot = new QuerySnapshot(query, [System.Text.Json.Nodes.JsonNode.Parse("""{"other":"value"}""")]);
        var store = new RecordingStore(new RecoveryState([], []));
        var reconciler = new SnapshotReconciler(new StaticSnapshotSource(snapshot), store, CreateRegistry(query));

        await reconciler.ReconcileAsync(query, CancellationToken.None);

        var rejected = Assert.Single(store.OutboxItems);
        Assert.Equal(WakeOutboxStatus.DeadLetter, rejected.Status);
        Assert.Equal("payload.schema_invalid", Assert.Single(store.RejectionReasons));
    }

    [Fact]
    public async Task Returning_to_checkpointed_snapshot_supersedes_newer_pending_wake()
    {
        var query = new QueryIdentity(new Uri("http://drasi.test"), null, "orders");
        var snapshot = new QuerySnapshot(query, [System.Text.Json.Nodes.JsonNode.Parse("""{"orderId":"order-1"}""")]);
        var checkpointFingerprint = SnapshotFingerprint.Compute(snapshot);
        var pending = new WakeOutboxItem(
            Guid.NewGuid(),
            "orders-binding",
            "singleton",
            "newer-pending-fingerprint",
            "triage-order",
            new System.Text.Json.Nodes.JsonObject(),
            "1.0.0",
            "drasiwake:pending",
            0,
            DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow,
            WakeOutboxStatus.Pending,
            null,
            null,
            "sample-gateway");
        var store = new RecordingStore(new RecoveryState(
            [pending],
            [new SnapshotCheckpoint("orders-binding", "singleton", checkpointFingerprint, DateTimeOffset.UtcNow)]));
        var reconciler = new SnapshotReconciler(new StaticSnapshotSource(snapshot), store, CreateRegistry(query));

        await reconciler.ReconcileAsync(query, CancellationToken.None);

        Assert.Equal(WakeOutboxStatus.Superseded, Assert.Single(store.OutboxItems).Status);
        Assert.Equal(0, store.OutboxWrites);
    }

    [Fact]
    public async Task Empty_singleton_snapshot_creates_a_wake_with_no_facts()
    {
        var query = new QueryIdentity(new Uri("http://drasi.test"), null, "orders");
        var snapshot = new QuerySnapshot(query, Array.Empty<System.Text.Json.Nodes.JsonNode?>());
        var store = new RecordingStore(new RecoveryState([], []));
        var reconciler = new SnapshotReconciler(new StaticSnapshotSource(snapshot), store, CreateRegistry(query));

        await reconciler.ReconcileAsync(query, CancellationToken.None);

        var wake = Assert.Single(store.OutboxItems);
        Assert.Equal("singleton", wake.SessionId);
        Assert.Empty((System.Text.Json.Nodes.JsonArray)wake.Input["facts"]!);
    }

    [Fact]
    public async Task Removed_session_supersedes_pending_and_receives_an_empty_snapshot()
    {
        var query = new QueryIdentity(new Uri("http://drasi.test"), null, "orders");
        var binding = CreateBinding(query) with { SessionScope = "per-query", AggregateKeyPointer = "/orderId" };
        var oldPending = new WakeOutboxItem(
            Guid.NewGuid(), binding.Id, "removed-session", "old-pending-fingerprint", "triage-order",
            new System.Text.Json.Nodes.JsonObject(), "1.0.0", "drasiwake:old", 0,
            DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, WakeOutboxStatus.Pending, null, null, "sample-gateway");
        var checkpoint = new SnapshotCheckpoint(
            binding.Id, "removed-session", "old-accepted-fingerprint", DateTimeOffset.UtcNow);
        var currentSnapshot = new QuerySnapshot(query,
            [System.Text.Json.Nodes.JsonNode.Parse("""{"orderId":"current-session"}""")]);
        var store = new RecordingStore(new RecoveryState([oldPending], [checkpoint]));
        var reconciler = new SnapshotReconciler(
            new StaticSnapshotSource(currentSnapshot), store, CreateRegistry(query, binding));

        await reconciler.ReconcileAsync(query, CancellationToken.None);

        var pendingWakes = store.OutboxItems.Where(item =>
            item.SessionId == "removed-session" && item.Status == WakeOutboxStatus.Pending).ToArray();
        var emptyWake = Assert.Single(pendingWakes);
        Assert.Empty((System.Text.Json.Nodes.JsonArray)emptyWake.Input["facts"]!);
        Assert.Equal(WakeOutboxStatus.Superseded, Assert.Single(store.OutboxItems, item => item.Id == oldPending.Id).Status);
    }

    [Fact]
    public async Task Snapshot_read_failure_does_not_change_checkpoint_or_create_outbox()
    {
        var query = new QueryIdentity(new Uri("http://drasi.test"), null, "orders");
        var originalCheckpoint = new SnapshotCheckpoint("orders-binding", "singleton", "before", DateTimeOffset.UtcNow);
        var store = new RecordingStore(new RecoveryState([], [originalCheckpoint]));
        var source = new FailingSnapshotSource(query);
        var registry = CreateRegistry(query);
        var reconciler = new SnapshotReconciler(source, store, registry);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => reconciler.ReconcileAsync(query, CancellationToken.None));

        Assert.Equal([originalCheckpoint], store.State.Checkpoints);
        Assert.Equal(0, store.OutboxWrites);
        Assert.Equal(0, store.AcceptanceWrites);
    }

    private static BridgeBinding CreateBinding(QueryIdentity query) => new(
        "orders-binding",
        "drasi-server",
        query.Server,
        query.InstanceId,
        query.QueryId,
        "converge-latest",
        "singleton",
        null,
        null,
        "sample-gateway",
        "triage-order",
        new BridgeContract(
            "1.0.0",
            Path.Combine(AppContext.BaseDirectory, "Contracts", "Fixtures", "order.schema.json")),
        32_768,
        [],
        new RetryPolicy(3, TimeSpan.FromDays(1)),
        new RateLimitPolicy(10, TimeSpan.FromSeconds(1)));

    private static ContractRegistryManager CreateRegistry(QueryIdentity query, BridgeBinding? binding = null)
    {
        var registry = new ContractRegistryManager();
        registry.TryActivate(new ContractRegistryCandidate(
            new ContractRegistry("1.0.0", [binding ?? CreateBinding(query)]),
            []));
        return registry;
    }

    private sealed class FailingSnapshotSource(QueryIdentity query) : IChangeSource
    {
        public async IAsyncEnumerable<ChangeSignal> WatchAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            yield break;
        }

        public ValueTask<IReadOnlyList<QueryIdentity>> EnumerateQueriesAsync(CancellationToken cancellationToken)
            => ValueTask.FromResult<IReadOnlyList<QueryIdentity>>([query]);

        public ValueTask<QuerySnapshot> ReadSnapshotAsync(QueryIdentity requestedQuery, CancellationToken cancellationToken)
            => ValueTask.FromException<QuerySnapshot>(new InvalidOperationException("snapshot unavailable"));
    }

    private sealed class StaticSnapshotSource(QuerySnapshot snapshot) : IChangeSource
    {
        public async IAsyncEnumerable<ChangeSignal> WatchAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            yield break;
        }

        public ValueTask<IReadOnlyList<QueryIdentity>> EnumerateQueriesAsync(CancellationToken cancellationToken)
            => ValueTask.FromResult<IReadOnlyList<QueryIdentity>>([snapshot.Query]);

        public ValueTask<QuerySnapshot> ReadSnapshotAsync(QueryIdentity query, CancellationToken cancellationToken)
            => ValueTask.FromResult(snapshot);
    }

    private sealed class RecordingStore(RecoveryState state) : IBridgeStore
    {
        private readonly List<WakeOutboxItem> outboxItems = [.. state.DispatchableItems];
        private readonly List<SnapshotCheckpoint> checkpoints = [.. state.Checkpoints];
        public RecoveryState State => new(outboxItems, checkpoints);
        public IReadOnlyList<WakeOutboxItem> OutboxItems => outboxItems;
        public int OutboxWrites { get; private set; }
        public int AcceptanceWrites { get; private set; }
        public List<string> RejectionReasons { get; } = [];

        public ValueTask<WakeOutboxItem> CreateOrUpdatePendingWakeAsync(WakeOutboxItem item, CancellationToken cancellationToken)
        {
            var index = outboxItems.FindIndex(existing =>
                existing.Id == item.Id ||
                (existing.BindingId == item.BindingId && existing.SessionId == item.SessionId &&
                 existing.SnapshotFingerprint == item.SnapshotFingerprint &&
                 existing.Status is WakeOutboxStatus.Pending or WakeOutboxStatus.RetryScheduled or WakeOutboxStatus.Dispatching));
            if (index >= 0)
                item = outboxItems[index];
            OutboxWrites++;
            index = outboxItems.FindIndex(existing => existing.Id == item.Id);
            if (index >= 0)
                outboxItems[index] = item;
            else
                outboxItems.Add(item);
            return ValueTask.FromResult(item);
        }

        public ValueTask SupersedePendingWakesAsync(string bindingId, string sessionId, string currentFingerprint, CancellationToken cancellationToken)
        {
            for (var index = 0; index < outboxItems.Count; index++)
            {
                var item = outboxItems[index];
                if (item.BindingId == bindingId && item.SessionId == sessionId &&
                    item.SnapshotFingerprint != currentFingerprint &&
                    item.Status is WakeOutboxStatus.Pending or WakeOutboxStatus.RetryScheduled)
                {
                    outboxItems[index] = item with { Status = WakeOutboxStatus.Superseded };
                }
            }
            return ValueTask.CompletedTask;
        }

        public ValueTask RecordRejectedWakeAsync(WakeOutboxItem item, string reasonCode, CancellationToken cancellationToken)
        {
            RejectionReasons.Add(reasonCode);
            if (outboxItems.All(existing => existing.Id != item.Id))
                outboxItems.Add(item with { Status = WakeOutboxStatus.DeadLetter });
            return ValueTask.CompletedTask;
        }

        public ValueTask<IReadOnlyList<WakeOutboxItem>> LoadDispatchableAsync(DateTimeOffset nowUtc, int limit, CancellationToken cancellationToken)
            => ValueTask.FromResult<IReadOnlyList<WakeOutboxItem>>(State.DispatchableItems);

        public ValueTask MarkAcceptedWithCheckpointAsync(Guid outboxId, WakeAcceptance acceptance, SnapshotCheckpoint checkpoint, CancellationToken cancellationToken)
        {
            AcceptanceWrites++;
            checkpoints.RemoveAll(existing => existing.BindingId == checkpoint.BindingId && existing.SessionId == checkpoint.SessionId);
            checkpoints.Add(checkpoint);
            return ValueTask.CompletedTask;
        }

        public ValueTask UpdateExecutionStatusAsync(WakeExecutionStatus status, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;

        public ValueTask MarkRetryScheduledAsync(Guid outboxId, int attemptCount, DateTimeOffset nextAttemptUtc, string reasonCode, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;

        public ValueTask MarkDeadLetterAsync(Guid outboxId, string reasonCode, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;

        public ValueTask<RecoveryState> LoadRecoveryStateAsync(CancellationToken cancellationToken)
            => ValueTask.FromResult(State);

        public ValueTask EnsureOpenClawTargetsAsync(
            IReadOnlyDictionary<string, string> targetByBindingId,
            IReadOnlySet<string> configuredTargetNames,
            CancellationToken cancellationToken)
            => ValueTask.CompletedTask;
    }
}