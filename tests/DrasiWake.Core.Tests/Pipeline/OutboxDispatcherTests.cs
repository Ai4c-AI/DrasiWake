using System.Text.Json.Nodes;
using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Contracts;
using DrasiWake.Core.Domain;
using DrasiWake.Core.Pipeline;
using DrasiWake.Core.Tests;

namespace DrasiWake.Core.Tests.Pipeline;

public sealed class OutboxDispatcherTests
{
    [Fact]
    public async Task Persists_pending_wake_before_calling_sink()
    {
        var binding = CreateBinding();
        var registry = CreateRegistry(binding);
        var store = new RecordingStore();
        var item = CreateItem();
        await store.CreateOrUpdatePendingWakeAsync(item, CancellationToken.None);
        var sink = new StoreAssertingSink(store, item.Id);
        var dispatcher = new OutboxDispatcher(store, sink, registry);

        await dispatcher.DispatchOneAsync(item, CancellationToken.None);

        Assert.True(sink.SawPersistedPendingItem);
        Assert.Equal(1, store.AcceptanceWrites);
    }

    [Fact]
    public async Task Persists_execution_receipt_after_acceptance_and_checkpoint()
    {
        var store = new RecordingStore();
        var item = CreateItem();
        await store.CreateOrUpdatePendingWakeAsync(item, CancellationToken.None);
        var status = new WakeExecutionStatus("invocation-1", "Completed", DateTimeOffset.UtcNow);
        var sink = new StoreAssertingSink(store, item.Id, status);
        var dispatcher = new OutboxDispatcher(store, sink, CreateRegistry(CreateBinding()));

        await dispatcher.DispatchOneAsync(item, CancellationToken.None);

        Assert.True(sink.SawAcceptanceBeforeStatusRead);
        Assert.Equal(status, store.LastExecutionStatus);
    }

    [Fact]
    public async Task Acceptance_storage_failure_retries_with_the_original_idempotency_key()
    {
        var store = new RecordingStore { FailAcceptanceWrite = true };
        var item = CreateItem();
        await store.CreateOrUpdatePendingWakeAsync(item, CancellationToken.None);
        var sink = new StoreAssertingSink(store, item.Id);
        var dispatcher = new OutboxDispatcher(store, sink, CreateRegistry(CreateBinding()));

        await dispatcher.DispatchOneAsync(item, CancellationToken.None);

        Assert.Equal(item.IdempotencyKey, sink.LastRequest!.IdempotencyKey);
        Assert.Equal(item.Id, store.LastRetryOutboxId);
        Assert.Equal("dispatch.transient_failure", store.LastRetryReason);
        Assert.Equal(0, store.AcceptanceWrites);
    }

    [Fact]
    public async Task Transient_sink_failure_schedules_retry_without_changing_idempotency_key()
    {
        var binding = CreateBinding();
        var store = new RecordingStore();
        var timeProvider = new ManualTimeProvider(DateTimeOffset.Parse("2026-01-01T00:00:00Z"));
        var item = CreateItem() with
        {
            CreatedAtUtc = timeProvider.GetUtcNow(),
            NextAttemptAtUtc = timeProvider.GetUtcNow()
        };
        await store.CreateOrUpdatePendingWakeAsync(item, CancellationToken.None);
        var sink = new FailingSink(new HttpRequestException("Gateway unavailable."));
        var dispatcher = new OutboxDispatcher(store, sink, CreateRegistry(binding), timeProvider);

        await dispatcher.DispatchOneAsync(item, CancellationToken.None);

        Assert.Equal(item.IdempotencyKey, sink.LastRequest!.IdempotencyKey);
        Assert.Equal(item.Id, store.LastRetryOutboxId);
        Assert.Equal(1, store.LastRetryAttemptCount);
        Assert.Equal(timeProvider.GetUtcNow().AddSeconds(1), store.LastRetryNextAttemptUtc);
        Assert.Equal("dispatch.transient_failure", store.LastRetryReason);
        Assert.Equal(0, store.AcceptanceWrites);
    }

    [Fact]
    public async Task Gateway_idempotency_conflict_is_not_retried()
    {
        var binding = CreateBinding();
        var store = new RecordingStore();
        var item = CreateItem();
        await store.CreateOrUpdatePendingWakeAsync(item, CancellationToken.None);
        var sink = new FailingSink(new WakeSinkTestException(WakeSinkFailureKind.ContractConflict));
        var dispatcher = new OutboxDispatcher(store, sink, CreateRegistry(binding));

        await dispatcher.DispatchOneAsync(item, CancellationToken.None);

        Assert.Equal(1, sink.CallCount);
        Assert.Equal(item.Id, store.DeadLetterOutboxId);
        Assert.Equal("gateway.idempotency_conflict", store.DeadLetterReason);
        Assert.Equal(0, store.RetryWrites);
    }

    private static BridgeBinding CreateBinding() => new(
        "orders-binding",
        "drasi-server",
        new Uri("http://drasi.test"),
        null,
        "orders",
        "converge-latest",
        "singleton",
        null,
        null,
        "sample-gateway",
        "triage-order",
        new BridgeContract("1.0.0", "order.schema.json"),
        32_768,
        [],
        new RetryPolicy(3, TimeSpan.FromDays(1)),
        new RateLimitPolicy(10, TimeSpan.FromSeconds(1)));

    private static ContractRegistryManager CreateRegistry(BridgeBinding binding)
    {
        var registry = new ContractRegistryManager();
        registry.TryActivate(new ContractRegistryCandidate(new ContractRegistry("1.0.0", [binding]), []));
        return registry;
    }

    private static WakeOutboxItem CreateItem() => new(
        Guid.NewGuid(),
        "orders-binding",
        "singleton",
        "fingerprint-1",
        "triage-order",
        new JsonObject { ["facts"] = new JsonArray() },
        "1.0.0",
        "drasiwake:outbox-1",
        0,
        DateTimeOffset.UtcNow,
        DateTimeOffset.UtcNow,
        WakeOutboxStatus.Pending,
        null,
        null);

    private sealed class StoreAssertingSink(
        RecordingStore store,
        Guid outboxId,
        WakeExecutionStatus? executionStatus = null) : IWakeSink
    {
        public bool SawPersistedPendingItem { get; private set; }
        public bool SawAcceptanceBeforeStatusRead { get; private set; }
        public WakeRequest? LastRequest { get; private set; }

        public ValueTask<WakeAcceptance> InvokeAsync(WakeRequest request, CancellationToken cancellationToken)
        {
            LastRequest = request;
            SawPersistedPendingItem = store.Items.Any(item => item.Id == outboxId && item.Status == WakeOutboxStatus.Pending);
            return ValueTask.FromResult(new WakeAcceptance("invocation-1", DateTimeOffset.UtcNow));
        }

        public ValueTask<WakeExecutionStatus?> GetStatusAsync(WakeRequest request, CancellationToken cancellationToken)
        {
            SawAcceptanceBeforeStatusRead = store.AcceptanceWrites == 1;
            return ValueTask.FromResult(executionStatus);
        }
    }

    private sealed class FailingSink(Exception exception) : IWakeSink
    {
        public int CallCount { get; private set; }
        public WakeRequest? LastRequest { get; private set; }

        public ValueTask<WakeAcceptance> InvokeAsync(WakeRequest request, CancellationToken cancellationToken)
        {
            CallCount++;
            LastRequest = request;
            return ValueTask.FromException<WakeAcceptance>(exception);
        }

        public ValueTask<WakeExecutionStatus?> GetStatusAsync(WakeRequest request, CancellationToken cancellationToken)
            => ValueTask.FromResult<WakeExecutionStatus?>(null);
    }

    private sealed class WakeSinkTestException(WakeSinkFailureKind kind)
        : WakeSinkException(kind, "test failure");

    private sealed class RecordingStore : IBridgeStore
    {
        public List<WakeOutboxItem> Items { get; } = [];
        public int AcceptanceWrites { get; private set; }
        public WakeExecutionStatus? LastExecutionStatus { get; private set; }
        public bool FailAcceptanceWrite { get; init; }
        public int RetryWrites { get; private set; }
        public Guid? LastRetryOutboxId { get; private set; }
        public int LastRetryAttemptCount { get; private set; }
        public DateTimeOffset? LastRetryNextAttemptUtc { get; private set; }
        public string? LastRetryReason { get; private set; }
        public Guid? DeadLetterOutboxId { get; private set; }
        public string? DeadLetterReason { get; private set; }

        public ValueTask<WakeOutboxItem> CreateOrUpdatePendingWakeAsync(WakeOutboxItem item, CancellationToken cancellationToken)
        {
            Items.Add(item);
            return ValueTask.FromResult(item);
        }

        public ValueTask RecordRejectedWakeAsync(WakeOutboxItem item, string reasonCode, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;

        public ValueTask SupersedePendingWakesAsync(string bindingId, string sessionId, string currentFingerprint, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;

        public ValueTask<IReadOnlyList<WakeOutboxItem>> LoadDispatchableAsync(DateTimeOffset nowUtc, int limit, CancellationToken cancellationToken)
            => ValueTask.FromResult<IReadOnlyList<WakeOutboxItem>>(Items);

        public ValueTask MarkAcceptedWithCheckpointAsync(Guid outboxId, WakeAcceptance acceptance, SnapshotCheckpoint checkpoint, CancellationToken cancellationToken)
        {
            if (FailAcceptanceWrite)
                return ValueTask.FromException(new IOException("Injected acceptance write failure."));
            AcceptanceWrites++;
            return ValueTask.CompletedTask;
        }

        public ValueTask UpdateExecutionStatusAsync(WakeExecutionStatus status, CancellationToken cancellationToken)
        {
            LastExecutionStatus = status;
            return ValueTask.CompletedTask;
        }

        public ValueTask MarkRetryScheduledAsync(Guid outboxId, int attemptCount, DateTimeOffset nextAttemptUtc, string reasonCode, CancellationToken cancellationToken)
        {
            RetryWrites++;
            LastRetryOutboxId = outboxId;
            LastRetryAttemptCount = attemptCount;
            LastRetryNextAttemptUtc = nextAttemptUtc;
            LastRetryReason = reasonCode;
            return ValueTask.CompletedTask;
        }

        public ValueTask MarkDeadLetterAsync(Guid outboxId, string reasonCode, CancellationToken cancellationToken)
        {
            DeadLetterOutboxId = outboxId;
            DeadLetterReason = reasonCode;
            return ValueTask.CompletedTask;
        }

        public ValueTask<RecoveryState> LoadRecoveryStateAsync(CancellationToken cancellationToken)
            => ValueTask.FromResult(new RecoveryState(Items, []));
    }
}
