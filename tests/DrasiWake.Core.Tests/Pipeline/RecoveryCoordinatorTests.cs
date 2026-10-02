using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Contracts;
using DrasiWake.Core.Domain;
using DrasiWake.Core.Pipeline;

namespace DrasiWake.Core.Tests.Pipeline;

public sealed class RecoveryCoordinatorTests
{
    [Fact]
    public async Task Startup_waits_for_snapshot_before_health_and_reuses_persisted_idempotency_key()
    {
        var query = new QueryIdentity(new Uri("http://drasi.test"), null, "orders");
        var rows = new[] { JsonNode.Parse("""{"orderId":"42"}""") };
        var snapshot = new QuerySnapshot(query, rows);
        var fingerprint = SnapshotFingerprint.Compute(snapshot);
        var pending = new WakeOutboxItem(
            Guid.NewGuid(), "orders-binding", "singleton", fingerprint, "triage-order",
            new JsonObject { ["facts"] = new JsonArray(JsonNode.Parse("""{"orderId":"42"}""")) },
            "1.0.0", "drasiwake:persisted-key", 0, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow,
            WakeOutboxStatus.Pending, null, null);
        var store = new RecordingStore(pending);
        var source = new BlockingSnapshotSource(query, snapshot);
        var reconciler = new SnapshotReconciler(source, store, CreateRegistry(query));
        var processed = new TaskCompletionSource<WakeOutboxItem>(TaskCreationOptions.RunContinuationsAsynchronously);
        var partitioner = new SessionPartitioner(
            4,
            1,
            CreateRegistry(query),
            (item, _) =>
            {
                processed.TrySetResult(item);
                return ValueTask.CompletedTask;
            });
        var coordinator = new RecoveryCoordinator(source, store, reconciler, partitioner);
        using var cancellation = new CancellationTokenSource();
        var workerTask = partitioner.RunAsync(cancellation.Token);

        try
        {
            var recoveryTask = coordinator.RecoverAsync(TestContext.Current.CancellationToken);
            await source.SnapshotReadStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
            Assert.False(coordinator.IsHealthy(query));

            source.ReleaseSnapshot();
            await recoveryTask;
            Assert.True(coordinator.IsHealthy(query));

            var replayed = await processed.Task.WaitAsync(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
            Assert.Equal(pending.IdempotencyKey, replayed.IdempotencyKey);
            Assert.Equal(3, store.RecoveryLoads);
        }
        finally
        {
            source.ReleaseSnapshot();
            cancellation.Cancel();
            try
            {
                await workerTask;
            }
            catch (OperationCanceledException)
            {
            }
            await partitioner.DisposeAsync();
        }
    }

    [Fact]
    public async Task Failed_reconnect_snapshot_keeps_query_unhealthy()
    {
        var query = new QueryIdentity(new Uri("http://drasi.test"), null, "orders");
        var pending = new WakeOutboxItem(
            Guid.NewGuid(), "orders-binding", "singleton", "fingerprint", "triage-order",
            new JsonObject(), "1.0.0", "drasiwake:retry-key", 0, DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow, WakeOutboxStatus.Pending, null, null);
        var store = new RecordingStore(pending);
        var source = new FailingSnapshotSource(query);
        var reconciler = new SnapshotReconciler(source, store, CreateRegistry(query));
        var partitioner = new SessionPartitioner(4, 1, CreateRegistry(query), (_, _) => ValueTask.CompletedTask);
        var coordinator = new RecoveryCoordinator(source, store, reconciler, partitioner);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => coordinator.RecoverAsync(TestContext.Current.CancellationToken));

        Assert.False(coordinator.IsHealthy(query));
        Assert.Equal(1, store.RecoveryLoads);
        await partitioner.DisposeAsync();
    }

    private static ContractRegistryManager CreateRegistry(QueryIdentity query)
    {
        var binding = new BridgeBinding(
            "orders-binding", "drasi-server", query.Server, query.InstanceId, query.QueryId,
            "converge-latest", "singleton", null, null, "triage-order",
            new BridgeContract("1.0.0", Path.Combine(AppContext.BaseDirectory, "Contracts", "Fixtures", "order.schema.json")),
            32768, [], new RetryPolicy(3, TimeSpan.FromDays(1)),
            new RateLimitPolicy(10, TimeSpan.FromSeconds(1)));
        var registry = new ContractRegistryManager();
        registry.TryActivate(new ContractRegistryCandidate(new ContractRegistry("1.0.0", [binding]), []));
        return registry;
    }

    private sealed class BlockingSnapshotSource(QueryIdentity query, QuerySnapshot snapshot) : IChangeSource
    {
        private readonly TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource SnapshotReadStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async IAsyncEnumerable<ChangeSignal> WatchAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            yield break;
        }

        public ValueTask<IReadOnlyList<QueryIdentity>> EnumerateQueriesAsync(CancellationToken cancellationToken)
            => ValueTask.FromResult<IReadOnlyList<QueryIdentity>>([query]);

        public async ValueTask<QuerySnapshot> ReadSnapshotAsync(QueryIdentity requestedQuery, CancellationToken cancellationToken)
        {
            SnapshotReadStarted.TrySetResult();
            await release.Task.WaitAsync(cancellationToken);
            return snapshot;
        }

        public void ReleaseSnapshot() => release.TrySetResult();
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

    private sealed class RecordingStore(WakeOutboxItem pending) : IBridgeStore
    {
        private readonly List<WakeOutboxItem> items = [pending];
        public int RecoveryLoads { get; private set; }

        public ValueTask<WakeOutboxItem> CreateOrUpdatePendingWakeAsync(WakeOutboxItem item, CancellationToken cancellationToken)
        {
            var existing = items.FirstOrDefault(candidate => candidate.Id == item.Id ||
                candidate.BindingId == item.BindingId && candidate.SessionId == item.SessionId &&
                candidate.SnapshotFingerprint == item.SnapshotFingerprint &&
                candidate.Status is WakeOutboxStatus.Pending or WakeOutboxStatus.RetryScheduled);
            if (existing is not null)
                return ValueTask.FromResult(existing);
            items.Add(item);
            return ValueTask.FromResult(item);
        }

        public ValueTask RecordRejectedWakeAsync(WakeOutboxItem item, string reasonCode, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;

        public ValueTask SupersedePendingWakesAsync(string bindingId, string sessionId, string currentFingerprint, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;

        public ValueTask<IReadOnlyList<WakeOutboxItem>> LoadDispatchableAsync(DateTimeOffset nowUtc, int limit, CancellationToken cancellationToken)
            => ValueTask.FromResult<IReadOnlyList<WakeOutboxItem>>(items.Where(item => item.Status is WakeOutboxStatus.Pending or WakeOutboxStatus.RetryScheduled).Take(limit).ToArray());

        public ValueTask MarkAcceptedWithCheckpointAsync(Guid outboxId, WakeAcceptance acceptance, SnapshotCheckpoint checkpoint, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;

        public ValueTask UpdateExecutionStatusAsync(WakeExecutionStatus status, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;

        public ValueTask MarkRetryScheduledAsync(Guid outboxId, int attemptCount, DateTimeOffset nextAttemptUtc, string reasonCode, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;

        public ValueTask MarkDeadLetterAsync(Guid outboxId, string reasonCode, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;

        public ValueTask<RecoveryState> LoadRecoveryStateAsync(CancellationToken cancellationToken)
        {
            RecoveryLoads++;
            return ValueTask.FromResult(new RecoveryState(
                items.Where(item => item.Status is WakeOutboxStatus.Pending or WakeOutboxStatus.RetryScheduled).ToArray(),
                []));
        }
    }
}