using System.Text.Json.Nodes;
using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Domain;
using DrasiWake.Persistence.Raft;
using DrasiWake.Persistence.SonnetDB;
using DrasiWake.Persistence.SonnetDB.Replication;
using Microsoft.EntityFrameworkCore;

namespace DrasiWake.Persistence.Raft.Tests;

public sealed class RaftBridgeStoreTests
{
    [Fact]
    public async Task Business_mutations_are_mapped_to_replicated_commands()
    {
        await using var database = await TestDatabase.CreateAsync();
        var executor = new ProjectionBackedExecutor(database.Projection);
        var store = new RaftBridgeStore(executor, database.Projection);
        var cancellationToken = TestContext.Current.CancellationToken;
        var now = DateTimeOffset.Parse("2026-10-07T02:03:04Z");
        var wake = CreateWake(now);

        var created = await store.CreateOrUpdatePendingWakeAsync(wake, cancellationToken);
        Assert.Equal(wake.Id, created.Id);
        AssertCommand<CreateOrUpdatePendingWakePayload>(
            executor, 0, BridgeCommandKind.CreateOrUpdatePendingWake,
            payload =>
            {
                Assert.Equal(wake.Id, payload.Item.Id);
                Assert.Equal(wake.IdempotencyKey, payload.Item.IdempotencyKey);
                Assert.True(JsonNode.DeepEquals(wake.Input, payload.Item.Input));
            });

        var rejected = wake with
        {
            Id = Guid.NewGuid(),
            IdempotencyKey = $"rejected-{Guid.NewGuid():N}",
            Status = WakeOutboxStatus.DeadLetter
        };
        await store.RecordRejectedWakeAsync(rejected, "payload.invalid", cancellationToken);
        AssertCommand<RecordRejectedWakePayload>(
            executor, 1, BridgeCommandKind.RecordRejectedWake,
            payload =>
            {
                Assert.Equal(rejected.Id, payload.Item.Id);
                Assert.Equal(rejected.Status, payload.Item.Status);
                Assert.Equal("payload.invalid", payload.ReasonCode);
            });

        await store.SupersedePendingWakesAsync("binding", "session", "new-fingerprint", cancellationToken);
        AssertCommand<SupersedePendingWakesPayload>(
            executor, 2, BridgeCommandKind.SupersedePendingWakes,
            payload =>
            {
                Assert.Equal("binding", payload.BindingId);
                Assert.Equal("session", payload.SessionId);
                Assert.Equal("new-fingerprint", payload.CurrentFingerprint);
            });

        var candidate = wake with
        {
            Id = Guid.NewGuid(),
            IdempotencyKey = $"candidate-{Guid.NewGuid():N}",
            NextAttemptAtUtc = now.AddMinutes(-1)
        };
        await ApplyCommandAsync(database.Projection,
            BridgeCommandKind.CreateOrUpdatePendingWake,
            new CreateOrUpdatePendingWakePayload(candidate), cancellationToken);
        executor.Commands.Clear();
        var claimedAt = now;
        var claimed = await store.LoadDispatchableAsync(claimedAt, 10, cancellationToken);
        Assert.Equal(candidate.Id, Assert.Single(claimed).Id);
        Assert.Equal(WakeOutboxStatus.Dispatching, claimed[0].Status);
        AssertCommand<ClaimDispatchablePayload>(
            executor, 0, BridgeCommandKind.ClaimDispatchable,
            payload =>
            {
                Assert.Equal(new[] { candidate.Id }, payload.OutboxIds);
                Assert.Equal(claimedAt, payload.ClaimedAtUtc);
            });

        var accepted = new WakeAcceptance("invocation-1", now);
        var checkpoint = new SnapshotCheckpoint("binding", "session", "fingerprint", now);
        await store.MarkAcceptedWithCheckpointAsync(candidate.Id, accepted, checkpoint, cancellationToken);
        AssertCommand<MarkAcceptedWithCheckpointPayload>(
            executor, 1, BridgeCommandKind.MarkAcceptedWithCheckpoint,
            payload =>
            {
                Assert.Equal(candidate.Id, payload.OutboxId);
                Assert.Equal(accepted, payload.Acceptance);
                Assert.Equal(checkpoint, payload.Checkpoint);
            });

        var execution = new WakeExecutionStatus("invocation-1", "Completed", now.AddSeconds(1));
        await store.UpdateExecutionStatusAsync(execution, cancellationToken);
        AssertCommand<UpdateExecutionStatusPayload>(
            executor, 2, BridgeCommandKind.UpdateExecutionStatus,
            payload => Assert.Equal(execution, payload.Status));

        await store.MarkRetryScheduledAsync(candidate.Id, 3, now.AddMinutes(2), "gateway.retry", cancellationToken);
        AssertCommand<MarkRetryScheduledPayload>(
            executor, 3, BridgeCommandKind.MarkRetryScheduled,
            payload =>
            {
                Assert.Equal(candidate.Id, payload.OutboxId);
                Assert.Equal(3, payload.AttemptCount);
                Assert.Equal(now.AddMinutes(2), payload.NextAttemptUtc);
                Assert.Equal("gateway.retry", payload.ReasonCode);
            });

        await store.MarkDeadLetterAsync(candidate.Id, "gateway.terminal", cancellationToken);
        AssertCommand<MarkDeadLetterPayload>(
            executor, 4, BridgeCommandKind.MarkDeadLetter,
            payload =>
            {
                Assert.Equal(candidate.Id, payload.OutboxId);
                Assert.Equal("gateway.terminal", payload.ReasonCode);
            });

        await store.EnsureOpenClawTargetsAsync(
            new Dictionary<string, string> { ["binding"] = "target" },
            new HashSet<string> { "target" },
            new Dictionary<string, TimeSpan> { ["binding"] = TimeSpan.FromHours(1) },
            new Dictionary<string, TimeSpan> { ["target"] = TimeSpan.FromHours(2) },
            cancellationToken);
        AssertCommand<EnsureOpenClawTargetsPayload>(
            executor, 5, BridgeCommandKind.EnsureOpenClawTargets,
            payload =>
            {
                Assert.Equal("target", payload.TargetByBindingId["binding"]);
                Assert.Contains("target", payload.ConfiguredTargetNames);
                Assert.Equal(TimeSpan.FromHours(1), payload.MaximumRetryAgeByBindingId["binding"]);
                Assert.Equal(TimeSpan.FromHours(2), payload.IdempotencyRetentionByTarget["target"]);
            });
    }

    [Fact]
    public async Task Committed_create_returns_the_projection_value_not_a_local_write_result()
    {
        await using var database = await TestDatabase.CreateAsync();
        var executor = new ProjectionBackedExecutor(database.Projection);
        var store = new RaftBridgeStore(executor, database.Projection);
        var wake = CreateWake(DateTimeOffset.UtcNow);

        var result = await store.CreateOrUpdatePendingWakeAsync(wake, TestContext.Current.CancellationToken);

        Assert.Equal(wake.Id, result.Id);
        Assert.Equal(wake.IdempotencyKey, result.IdempotencyKey);
        Assert.True(JsonNode.DeepEquals(wake.Input, result.Input));
        Assert.Equal(1, await database.Projection.GetLastAppliedIndexAsync(TestContext.Current.CancellationToken));
        var projected = await database.Projection.FindPendingWakeResultAsync(
            wake.Id, wake.BindingId, wake.SessionId, wake.SnapshotFingerprint,
            TestContext.Current.CancellationToken);
        Assert.NotNull(projected);
        Assert.Equal(wake.Id, projected.Id);
    }

    [Fact]
    public async Task No_quorum_does_not_apply_a_local_business_mutation()
    {
        await using var database = await TestDatabase.CreateAsync();
        var executor = new ProjectionBackedExecutor(database.Projection) { HasQuorum = false };
        var store = new RaftBridgeStore(executor, database.Projection);
        var wake = CreateWake(DateTimeOffset.UtcNow);

        await Assert.ThrowsAsync<InjectedReplicationFailure>(async () =>
            await store.CreateOrUpdatePendingWakeAsync(wake, TestContext.Current.CancellationToken));

        Assert.Single(executor.Commands);
        Assert.False(executor.HasQuorum);
        Assert.Equal(BridgeCommandKind.CreateOrUpdatePendingWake, executor.Commands[0].Kind);
        Assert.Equal(0, await database.Projection.GetLastAppliedIndexAsync(TestContext.Current.CancellationToken));
        Assert.Null(await database.Projection.FindPendingWakeResultAsync(
            wake.Id, wake.BindingId, wake.SessionId, wake.SnapshotFingerprint,
            TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Failed_dispatch_claim_returns_no_gateway_items_and_leaves_candidates_dispatchable()
    {
        await using var database = await TestDatabase.CreateAsync();
        var wake = CreateWake(DateTimeOffset.UtcNow.AddMinutes(-1));
        await ApplyCommandAsync(
            database.Projection,
            BridgeCommandKind.CreateOrUpdatePendingWake,
            new CreateOrUpdatePendingWakePayload(wake),
            TestContext.Current.CancellationToken);
        var executor = new ProjectionBackedExecutor(database.Projection) { HasQuorum = false };
        var store = new RaftBridgeStore(executor, database.Projection);

        await Assert.ThrowsAsync<InjectedReplicationFailure>(async () =>
            await store.LoadDispatchableAsync(
                DateTimeOffset.UtcNow, 10, TestContext.Current.CancellationToken));

        var item = Assert.Single(await database.Projection.LoadDispatchableCandidatesAsync(
            DateTimeOffset.UtcNow, 10, TestContext.Current.CancellationToken));
        Assert.Equal(wake.Id, item.Id);
        Assert.Equal(WakeOutboxStatus.Pending, item.Status);
        Assert.Equal(wake.Id, Assert.Single(executor.Commands[0]
            .DeserializePayload<ClaimDispatchablePayload>().OutboxIds));
    }

    [Fact]
    public async Task Concurrent_dispatch_claims_do_not_poison_the_log_or_return_each_others_rows()
    {
        await using var database = await TestDatabase.CreateAsync();
        var now = DateTimeOffset.UtcNow;
        var wake = CreateWake(now.AddMinutes(-1));
        await ApplyCommandAsync(
            database.Projection,
            BridgeCommandKind.CreateOrUpdatePendingWake,
            new CreateOrUpdatePendingWakePayload(wake),
            TestContext.Current.CancellationToken);
        var executor = new ConcurrentClaimExecutor(database.Projection);
        var firstStore = new RaftBridgeStore(executor, database.Projection);
        var secondStore = new RaftBridgeStore(executor, database.Projection);

        var claims = await Task.WhenAll(
            firstStore.LoadDispatchableAsync(now, 10, TestContext.Current.CancellationToken).AsTask(),
            secondStore.LoadDispatchableAsync(now, 10, TestContext.Current.CancellationToken).AsTask());

        Assert.Equal(2, executor.Commands.Count);
        Assert.Equal(3, await database.Projection.GetLastAppliedIndexAsync(TestContext.Current.CancellationToken));
        var returned = claims.SelectMany(items => items).ToArray();
        Assert.Equal(wake.Id, Assert.Single(returned).Id);
        Assert.Equal(1, claims.Count(items => items.Count == 1));
        Assert.Equal(1, claims.Count(items => items.Count == 0));
        var snapshot = await database.Projection.ExportSnapshotAsync(TestContext.Current.CancellationToken);
        Assert.Equal(executor.AppliedCommands[0].CommandId, Assert.Single(snapshot.WakeOutbox).ClaimCommandId);

        var nextCommand = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.SetConfigurationFingerprint,
            new SetConfigurationFingerprintPayload("still-healthy"));
        await executor.ReplicateAsync(nextCommand, TestContext.Current.CancellationToken);
        Assert.Equal(4, await database.Projection.GetLastAppliedIndexAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Recovery_is_committed_before_state_is_read_and_keeps_idempotency_key()
    {
        await using var database = await TestDatabase.CreateAsync();
        var wake = CreateWake(DateTimeOffset.UtcNow.AddMinutes(-1));
        await ApplyCommandAsync(
            database.Projection,
            BridgeCommandKind.CreateOrUpdatePendingWake,
            new CreateOrUpdatePendingWakePayload(wake),
            TestContext.Current.CancellationToken);
        await ApplyCommandAsync(
            database.Projection,
            BridgeCommandKind.ClaimDispatchable,
            new ClaimDispatchablePayload([wake.Id], DateTimeOffset.UtcNow, Guid.NewGuid()),
            TestContext.Current.CancellationToken);

        var executor = new ProjectionBackedExecutor(database.Projection);
        var store = new RaftBridgeStore(executor, database.Projection);
        var recovered = await store.LoadRecoveryStateAsync(TestContext.Current.CancellationToken);

        var item = Assert.Single(recovered.DispatchableItems);
        Assert.Equal(wake.IdempotencyKey, item.IdempotencyKey);
        Assert.Equal(WakeOutboxStatus.RetryScheduled, item.Status);
        Assert.Equal(
            item.NextAttemptAtUtc.ToUnixTimeMilliseconds(),
            executor.Commands.Single().DeserializePayload<RecoverInterruptedDispatchesPayload>()
                .RecoveredAtUtc.ToUnixTimeMilliseconds());
        Assert.Equal(3, await database.Projection.GetLastAppliedIndexAsync(TestContext.Current.CancellationToken));
        AssertCommand<RecoverInterruptedDispatchesPayload>(
            executor, 0, BridgeCommandKind.RecoverInterruptedDispatches, _ => { });
    }

    [Fact]
    public async Task Repeated_acceptance_is_validated_against_committed_projection_state()
    {
        await using var database = await TestDatabase.CreateAsync();
        var wake = CreateWake(DateTimeOffset.UtcNow);
        await ApplyCommandAsync(
            database.Projection,
            BridgeCommandKind.CreateOrUpdatePendingWake,
            new CreateOrUpdatePendingWakePayload(wake),
            TestContext.Current.CancellationToken);
        var executor = new ProjectionBackedExecutor(database.Projection);
        var store = new RaftBridgeStore(executor, database.Projection);
        var acceptedAt = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        var checkpoint = new SnapshotCheckpoint(
            wake.BindingId, wake.SessionId, wake.SnapshotFingerprint, acceptedAt);
        var original = new WakeAcceptance("original-invocation", checkpoint.AcceptedAtUtc);
        await store.MarkAcceptedWithCheckpointAsync(wake.Id, original, checkpoint, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await store.MarkAcceptedWithCheckpointAsync(
                wake.Id,
                new WakeAcceptance("different-invocation", checkpoint.AcceptedAtUtc.AddSeconds(1)),
                checkpoint with { AcceptedAtUtc = checkpoint.AcceptedAtUtc.AddSeconds(1) },
                TestContext.Current.CancellationToken));

        await store.MarkAcceptedWithCheckpointAsync(
            wake.Id,
            original,
            checkpoint with { AcceptedAtUtc = checkpoint.AcceptedAtUtc.AddSeconds(1) },
            TestContext.Current.CancellationToken);

        var projected = await database.Projection.ReadWakeAsync(wake.Id, TestContext.Current.CancellationToken);
        Assert.NotNull(projected);
        Assert.Equal(WakeOutboxStatus.Accepted, projected.Status);
        Assert.Equal(original.InvocationId, projected.InvocationId);
        var persistedCheckpoint = Assert.Single(
            (await database.Projection.ReadRecoveryStateAsync(TestContext.Current.CancellationToken)).Checkpoints);
        Assert.Equal(checkpoint.AcceptedAtUtc, persistedCheckpoint.AcceptedAtUtc);
        Assert.Collection(
            executor.Commands,
            command => Assert.Equal(BridgeCommandKind.MarkAcceptedWithCheckpoint, command.Kind),
            command => Assert.Equal(BridgeCommandKind.MarkAcceptedWithCheckpoint, command.Kind));
        Assert.Equal(3, await database.Projection.GetLastAppliedIndexAsync(TestContext.Current.CancellationToken));
    }

    private static WakeOutboxItem CreateWake(DateTimeOffset now) => new(
        Guid.NewGuid(),
        "binding",
        "session",
        "fingerprint",
        "skill",
        new JsonObject { ["value"] = 1 },
        "v1",
        $"idem-{Guid.NewGuid():N}",
        0,
        now,
        now,
        WakeOutboxStatus.Pending,
        null,
        null,
        "target");

    private static void AssertCommand<TPayload>(
        ProjectionBackedExecutor executor,
        int commandIndex,
        BridgeCommandKind expectedKind,
        Action<TPayload> assertPayload)
    {
        var command = executor.Commands[commandIndex];
        Assert.Equal(expectedKind, command.Kind);
        command.Validate();
        assertPayload(command.DeserializePayload<TPayload>());
    }

    private static async Task ApplyCommandAsync<TPayload>(
        IRaftBridgeProjection projection,
        BridgeCommandKind kind,
        TPayload payload,
        CancellationToken cancellationToken)
    {
        var command = ReplicatedBridgeCommand.Create(kind, payload);
        await projection.ApplyReplicatedCommandAsync(
            command,
            await projection.GetLastAppliedIndexAsync(cancellationToken) + 1,
            cancellationToken);
    }

    private sealed class ProjectionBackedExecutor(IRaftBridgeProjection projection) : IRaftCommandExecutor
    {
        public List<ReplicatedBridgeCommand> Commands { get; } = [];
        public bool IsLeader { get; init; } = true;
        public bool HasQuorum { get; init; } = true;

        public async ValueTask ReplicateAsync(
            ReplicatedBridgeCommand command,
            CancellationToken cancellationToken)
        {
            Commands.Add(command);
            if (!IsLeader || !HasQuorum)
                throw new InjectedReplicationFailure();

            var index = await projection.GetLastAppliedIndexAsync(cancellationToken) + 1;
            await projection.ApplyReplicatedCommandAsync(command, index, cancellationToken);
        }
    }

    private sealed class ConcurrentClaimExecutor(IRaftBridgeProjection projection) : IRaftCommandExecutor
    {
        private readonly TaskCompletionSource _bothCommandsReady =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly SemaphoreSlim _applyLock = new(1, 1);
        private int _arrivals;
        private long _lastAppliedIndex;

        public List<ReplicatedBridgeCommand> Commands { get; } = [];
        public List<ReplicatedBridgeCommand> AppliedCommands { get; } = [];
        public bool IsLeader => true;
        public bool HasQuorum => true;

        public async ValueTask ReplicateAsync(
            ReplicatedBridgeCommand command,
            CancellationToken cancellationToken)
        {
            lock (Commands)
            {
                Commands.Add(command);
                if (Commands.Count == 2)
                    _bothCommandsReady.TrySetResult();
            }

            if (Interlocked.Increment(ref _arrivals) <= 2)
                await _bothCommandsReady.Task.WaitAsync(cancellationToken);

            await _applyLock.WaitAsync(cancellationToken);
            try
            {
                var nextIndex = Math.Max(
                    _lastAppliedIndex,
                    await projection.GetLastAppliedIndexAsync(cancellationToken)) + 1;
                await projection.ApplyReplicatedCommandAsync(command, nextIndex, cancellationToken);
                _lastAppliedIndex = nextIndex;
                AppliedCommands.Add(command);
            }
            finally
            {
                _applyLock.Release();
            }
        }
    }

    private sealed class InjectedReplicationFailure : Exception;

    private sealed class TestDatabase(DbContextOptions<BridgeDbContext> options) : IAsyncDisposable
    {
        public IRaftBridgeProjection Projection { get; } =
            new SonnetBridgeStore(new ContextFactory(options));
        public string DirectoryPath { get; private set; } = string.Empty;

        public static async Task<TestDatabase> CreateAsync()
        {
            var directory = Path.Combine(AppContext.BaseDirectory, "raft-store-testdata", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(directory);
            var options = new DbContextOptionsBuilder<BridgeDbContext>()
                .UseSonnetDB($"Data Source={directory}")
                .Options;
            var database = new TestDatabase(options) { DirectoryPath = directory };
            await using var context = new BridgeDbContext(options);
            await context.Database.MigrateAsync(TestContext.Current.CancellationToken);
            return database;
        }

        public ValueTask DisposeAsync()
        {
            Directory.Delete(DirectoryPath, recursive: true);
            return ValueTask.CompletedTask;
        }

        private sealed class ContextFactory(DbContextOptions<BridgeDbContext> options)
            : IDbContextFactory<BridgeDbContext>
        {
            public BridgeDbContext CreateDbContext() => new(options);

            public Task<BridgeDbContext> CreateDbContextAsync(CancellationToken cancellationToken = default) =>
                Task.FromResult(new BridgeDbContext(options));
        }
    }
}
