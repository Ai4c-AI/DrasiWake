using System.Text.Json.Nodes;
using System.Text.Json;
using System.Data.Common;
using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Domain;
using DrasiWake.Persistence.SonnetDB;
using DrasiWake.Persistence.SonnetDB.Entities;
using DrasiWake.Persistence.SonnetDB.Replication;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using CoreSnapshotCheckpoint = DrasiWake.Core.Domain.SnapshotCheckpoint;
using SnapshotCheckpointEntity = DrasiWake.Persistence.SonnetDB.Entities.SnapshotCheckpoint;

namespace DrasiWake.Persistence.SonnetDB.Tests;

public sealed class ReplicatedProjectionTests
{
    [Fact]
    public void Sonnet_bridge_store_is_projection_only_and_exposes_no_business_mutation_api()
    {
        Assert.False(typeof(IBridgeStore).IsAssignableFrom(typeof(SonnetBridgeStore)));
        Assert.Null(typeof(SonnetBridgeStore).GetMethod(nameof(IBridgeStore.CreateOrUpdatePendingWakeAsync)));
        Assert.Null(typeof(SonnetBridgeStore).GetMethod(nameof(IBridgeStore.MarkAcceptedWithCheckpointAsync)));
        Assert.Null(typeof(SonnetBridgeStore).GetMethod(nameof(IBridgeStore.LoadDispatchableAsync)));
        Assert.Null(typeof(SonnetBridgeStore).GetMethod(nameof(IBridgeStore.LoadRecoveryStateAsync)));
    }

    [Fact]
    public void Every_replicated_command_kind_round_trips_and_has_a_stable_identifier()
    {
        var now = DateTimeOffset.Parse("2026-10-07T01:02:03Z");
        var wake = CreateWake(now);
        var subscription = new SubscriptionSnapshot(
            "query-key", "https://drasi", "instance", "query", true, now, null, null);
        var mapping = new KeyMappingSnapshot("scope", "identity", "session", "active", now);
        var commands = new[]
        {
            ReplicatedBridgeCommand.Create(BridgeCommandKind.CreateOrUpdatePendingWake,
                new CreateOrUpdatePendingWakePayload(wake)),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.RecordRejectedWake,
                new RecordRejectedWakePayload(wake with { Status = WakeOutboxStatus.DeadLetter }, "rejected")),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.SupersedePendingWakes,
                new SupersedePendingWakesPayload("binding", "session", "fingerprint")),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.ClaimDispatchable,
                new ClaimDispatchablePayload([wake.Id], now, Guid.NewGuid())),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.MarkAcceptedWithCheckpoint,
                new MarkAcceptedWithCheckpointPayload(
                    wake.Id,
                    new WakeAcceptance("invocation", now),
                    new CoreSnapshotCheckpoint("binding", "session", "fingerprint", now))),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.UpdateExecutionStatus,
                new UpdateExecutionStatusPayload(new WakeExecutionStatus("invocation", "Completed", now))),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.MarkRetryScheduled,
                new MarkRetryScheduledPayload(wake.Id, 2, now, "retry")),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.MarkDeadLetter,
                new MarkDeadLetterPayload(wake.Id, "dead")),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.RecoverInterruptedDispatches,
                new RecoverInterruptedDispatchesPayload(now)),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.EnsureOpenClawTargets,
                new EnsureOpenClawTargetsPayload(
                    new Dictionary<string, string> { ["binding"] = "target" },
                    new HashSet<string> { "target" },
                    new Dictionary<string, TimeSpan> { ["binding"] = TimeSpan.FromHours(1) },
                    new Dictionary<string, TimeSpan> { ["target"] = TimeSpan.FromHours(2) })),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.SetConfigurationFingerprint,
                new SetConfigurationFingerprintPayload("config-fingerprint")),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.UpsertSubscription,
                new UpsertSubscriptionPayload(subscription)),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.RemoveSubscription,
                new RemoveSubscriptionPayload("query-key")),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.UpsertKeyMapping,
                new UpsertKeyMappingPayload(mapping)),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.RemoveKeyMapping,
                new RemoveKeyMappingPayload("scope", "identity"))
        };

        Assert.Equal(15, Enum.GetValues<BridgeCommandKind>().Length);
        foreach (var command in commands)
        {
            var restored = ReplicatedBridgeCommand.Deserialize(command.Serialize());
            Assert.Equal(ReplicatedBridgeCommand.CurrentSchemaVersion, restored.SchemaVersion);
            Assert.Equal(command.Kind, restored.Kind);
            Assert.Equal(command.CommandId, restored.CommandId);
            Assert.Equal(command.Payload.GetRawText(), restored.Payload.GetRawText());
        }

        var first = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.EnsureOpenClawTargets,
            new EnsureOpenClawTargetsPayload(
                new Dictionary<string, string> { ["a"] = "A", ["b"] = "B" },
                new HashSet<string> { "A", "B" },
                new Dictionary<string, TimeSpan>(),
                new Dictionary<string, TimeSpan>()));
        var sameContentDifferentOrder = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.EnsureOpenClawTargets,
            new EnsureOpenClawTargetsPayload(
                new Dictionary<string, string> { ["b"] = "B", ["a"] = "A" },
                new HashSet<string> { "B", "A" },
                new Dictionary<string, TimeSpan>(),
                new Dictionary<string, TimeSpan>()));
        var changed = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.EnsureOpenClawTargets,
            new EnsureOpenClawTargetsPayload(
                new Dictionary<string, string> { ["a"] = "different", ["b"] = "B" },
                new HashSet<string> { "A", "B" },
                new Dictionary<string, TimeSpan>(),
                new Dictionary<string, TimeSpan>()));

        Assert.Equal(first.CommandId, sameContentDifferentOrder.CommandId);
        Assert.NotEqual(first.CommandId, changed.CommandId);
    }

    [Fact]
    public void Unsupported_command_versions_and_tampered_command_ids_are_rejected()
    {
        var command = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.SetConfigurationFingerprint,
            new SetConfigurationFingerprintPayload("configuration"));
        var unsupportedVersion = JsonNode.Parse(command.Serialize())!.AsObject();
        unsupportedVersion[nameof(command.SchemaVersion)] = ReplicatedBridgeCommand.CurrentSchemaVersion + 1;
        var incorrectId = JsonNode.Parse(command.Serialize())!.AsObject();
        incorrectId[nameof(command.CommandId)] = "not-the-command-hash";

        Assert.Throws<InvalidOperationException>(() =>
            ReplicatedBridgeCommand.Deserialize(unsupportedVersion.ToJsonString()));
        Assert.Throws<InvalidOperationException>(() =>
            ReplicatedBridgeCommand.Deserialize(incorrectId.ToJsonString()));
    }

    [Fact]
    public async Task Applying_same_or_old_index_is_idempotent_and_conflicts_are_rejected()
    {
        await using var database = await TestDatabase.CreateAsync();
        var store = database.Store;
        var wake = CreateWake(DateTimeOffset.UtcNow);
        var command = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.CreateOrUpdatePendingWake,
            new CreateOrUpdatePendingWakePayload(wake));

        await store.ApplyReplicatedCommandAsync(command, 1, TestContext.Current.CancellationToken);
        await store.ApplyReplicatedCommandAsync(command, 1, TestContext.Current.CancellationToken);
        var configCommand = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.SetConfigurationFingerprint,
            new SetConfigurationFingerprintPayload("config-at-index-2"));
        await store.ApplyReplicatedCommandAsync(
            configCommand,
            2,
            TestContext.Current.CancellationToken);
        await store.ApplyReplicatedCommandAsync(command, 1, TestContext.Current.CancellationToken);

        Assert.Equal(2, await store.GetLastAppliedIndexAsync(TestContext.Current.CancellationToken));
        Assert.Single(await database.ReadOutboxAsync());

        var gap = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.MarkDeadLetter,
            new MarkDeadLetterPayload(wake.Id, "gap"));
        await store.ApplyReplicatedCommandAsync(gap, 4, TestContext.Current.CancellationToken);

        var conflicting = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.MarkDeadLetter,
            new MarkDeadLetterPayload(wake.Id, "conflicting"));
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await store.ApplyReplicatedCommandAsync(conflicting, 4, TestContext.Current.CancellationToken));

        Assert.Equal(4, await store.GetLastAppliedIndexAsync(TestContext.Current.CancellationToken));
        Assert.Equal(WakeOutboxStatus.DeadLetter, Assert.Single(await database.ReadOutboxAsync()).Status);
    }

    [Fact]
    public async Task Applying_commands_skips_raft_configuration_entries_not_sent_to_the_state_machine()
    {
        await using var database = await TestDatabase.CreateAsync();
        var first = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.SetConfigurationFingerprint,
            new SetConfigurationFingerprintPayload("first"));
        var second = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.SetConfigurationFingerprint,
            new SetConfigurationFingerprintPayload("second"));

        await database.Store.ApplyReplicatedCommandAsync(first, 1, TestContext.Current.CancellationToken);
        await database.Store.ApplyReplicatedCommandAsync(second, 3, TestContext.Current.CancellationToken);

        Assert.Equal(3, await database.Store.GetLastAppliedIndexAsync(TestContext.Current.CancellationToken));
        Assert.Equal(
            "second",
            (await database.Store.ExportSnapshotAsync(TestContext.Current.CancellationToken))
                .ConfigurationFingerprint);
    }

    [Fact]
    public async Task Payloadless_log_entries_advance_and_restore_the_projection_index()
    {
        await using var source = await TestDatabase.CreateAsync();
        await using var target = await TestDatabase.CreateAsync();
        var cancellationToken = TestContext.Current.CancellationToken;

        await source.Store.AdvanceAppliedIndexAsync(1, cancellationToken);
        await source.Store.AdvanceAppliedIndexAsync(1, cancellationToken);
        var snapshot = await source.Store.ExportSnapshotAsync(cancellationToken);
        Assert.Equal(1, snapshot.LastAppliedIndex);
        Assert.Null(snapshot.LastAppliedCommandId);

        await target.Store.RestoreSnapshotAsync(snapshot, cancellationToken);
        await target.Store.ApplyReplicatedCommandAsync(
            ReplicatedBridgeCommand.Create(
                BridgeCommandKind.SetConfigurationFingerprint,
                new SetConfigurationFingerprintPayload("after-noop")),
            2,
            cancellationToken);

        var restored = await target.Store.ExportSnapshotAsync(cancellationToken);
        Assert.Equal(2, restored.LastAppliedIndex);
        Assert.Equal("after-noop", restored.ConfigurationFingerprint);
    }

    [Fact]
    public async Task Failed_replicated_acceptance_preserves_outbox_checkpoint_and_applied_index()
    {
        await using var database = await TestDatabase.CreateAsync();
        var wake = CreateWake(DateTimeOffset.UtcNow);
        await database.Store.ApplyReplicatedCommandAsync(
            ReplicatedBridgeCommand.Create(
                BridgeCommandKind.CreateOrUpdatePendingWake,
                new CreateOrUpdatePendingWakePayload(wake)),
            1,
            TestContext.Current.CancellationToken);
        var checkpoint = new CoreSnapshotCheckpoint(
            wake.BindingId, wake.SessionId, wake.SnapshotFingerprint, DateTimeOffset.UtcNow);
        var command = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.MarkAcceptedWithCheckpoint,
            new MarkAcceptedWithCheckpointPayload(
                wake.Id,
                new WakeAcceptance("accepted-invocation", checkpoint.AcceptedAtUtc),
                checkpoint));
        var failingStore = new SonnetBridgeStore(new TestContextFactory(
            new DbContextOptionsBuilder<BridgeDbContext>(database.Options)
                .AddInterceptors(new FailOnSaveChangesInterceptor())
                .Options));

        await Assert.ThrowsAsync<InjectedStoreFailure>(async () =>
            await failingStore.ApplyReplicatedCommandAsync(
                command, 2, TestContext.Current.CancellationToken));

        Assert.Equal(1, await database.Store.GetLastAppliedIndexAsync(TestContext.Current.CancellationToken));
        Assert.Equal(WakeOutboxStatus.Pending, Assert.Single(await database.ReadOutboxAsync()).Status);
        Assert.Empty(await database.ReadCheckpointsAsync());
    }

    [Fact]
    public async Task Candidate_read_does_not_claim_and_replicated_claim_uses_the_exact_ids_and_time()
    {
        await using var database = await TestDatabase.CreateAsync();
        var first = CreateWake(DateTimeOffset.UtcNow.AddMinutes(-2));
        var second = CreateWake(DateTimeOffset.UtcNow.AddMinutes(-1));
        await database.InsertOutboxAsync(first, second);
        var store = database.Store;
        var claimTime = DateTimeOffset.UtcNow;

        var candidates = await store.LoadDispatchableCandidatesAsync(
            claimTime, 1, TestContext.Current.CancellationToken);
        Assert.Equal(first.Id, Assert.Single(candidates).Id);
        Assert.All(await database.ReadOutboxAsync(), item => Assert.Equal(WakeOutboxStatus.Pending, item.Status));

        var claim = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.ClaimDispatchable,
            new ClaimDispatchablePayload([second.Id], claimTime, Guid.NewGuid()));
        await store.ApplyReplicatedCommandAsync(claim, 1, TestContext.Current.CancellationToken);

        var rows = await database.ReadOutboxAsync();
        Assert.Equal(WakeOutboxStatus.Dispatching, rows.Single(item => item.Id == second.Id).Status);
        Assert.Equal(WakeOutboxStatus.Pending, rows.Single(item => item.Id == first.Id).Status);
    }

    [Fact]
    public async Task Recovery_is_a_replicated_mutation_and_recovery_state_read_is_read_only()
    {
        await using var database = await TestDatabase.CreateAsync();
        var wake = CreateWake(DateTimeOffset.UtcNow) with { Status = WakeOutboxStatus.Dispatching };
        await database.InsertOutboxAsync(wake);

        var before = await database.Store.ReadRecoveryStateAsync(TestContext.Current.CancellationToken);
        Assert.Empty(before.DispatchableItems);
        Assert.Equal(WakeOutboxStatus.Dispatching, Assert.Single(await database.ReadOutboxAsync()).Status);

        var recoveredAt = DateTimeOffset.Parse("2026-10-07T01:02:03Z");
        var command = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.RecoverInterruptedDispatches,
            new RecoverInterruptedDispatchesPayload(recoveredAt));
        await database.Store.ApplyReplicatedCommandAsync(command, 1, TestContext.Current.CancellationToken);

        var row = Assert.Single(await database.ReadOutboxAsync());
        Assert.Equal(WakeOutboxStatus.RetryScheduled, row.Status);
        Assert.Equal(recoveredAt, row.NextAttemptAtUtc);
        Assert.Equal("dispatch.recovered", row.LastErrorCode);
    }

    [Fact]
    public async Task Every_command_kind_applies_its_typed_mutation_and_advances_the_projection()
    {
        await using var database = await TestDatabase.CreateAsync();
        var now = DateTimeOffset.Parse("2026-10-07T01:02:03Z");
        var pending = CreateWake(now);
        var rejected = CreateWake(now.AddSeconds(1)) with { Status = WakeOutboxStatus.DeadLetter };
        var targetless = CreateWake(now.AddSeconds(2));

        async Task Apply<T>(BridgeCommandKind kind, T payload)
        {
            var next = await database.Store.GetLastAppliedIndexAsync(TestContext.Current.CancellationToken) + 1;
            await database.Store.ApplyReplicatedCommandAsync(
                ReplicatedBridgeCommand.Create(kind, payload), next, TestContext.Current.CancellationToken);
        }

        await Apply(BridgeCommandKind.CreateOrUpdatePendingWake, new CreateOrUpdatePendingWakePayload(pending));
        await Apply(BridgeCommandKind.SupersedePendingWakes,
            new SupersedePendingWakesPayload(pending.BindingId, pending.SessionId, "new-fingerprint"));
        await Apply(BridgeCommandKind.RecordRejectedWake,
            new RecordRejectedWakePayload(rejected, "rejected"));
        await Apply(BridgeCommandKind.MarkDeadLetter, new MarkDeadLetterPayload(rejected.Id, "dead"));

        await database.InsertOutboxEntityAsync(new WakeOutbox
        {
            Id = targetless.Id,
            BindingId = targetless.BindingId,
            SessionId = targetless.SessionId,
            SnapshotFingerprint = targetless.SnapshotFingerprint,
            Skill = targetless.Skill,
            OpenClawTarget = null,
            InputJson = targetless.Input.ToJsonString(),
            ContractVersion = targetless.ContractVersion,
            IdempotencyKey = targetless.IdempotencyKey,
            AttemptCount = targetless.AttemptCount,
            CreatedAtUtc = targetless.CreatedAtUtc,
            NextAttemptAtUtc = targetless.NextAttemptAtUtc,
            Status = targetless.Status,
            Version = 1
        });
        await Apply(BridgeCommandKind.EnsureOpenClawTargets,
            new EnsureOpenClawTargetsPayload(
                new Dictionary<string, string> { [targetless.BindingId] = "target" },
                new HashSet<string> { "target" },
                new Dictionary<string, TimeSpan> { [targetless.BindingId] = TimeSpan.FromHours(1) },
                new Dictionary<string, TimeSpan> { ["target"] = TimeSpan.FromHours(2) }));
        await Apply(BridgeCommandKind.SetConfigurationFingerprint,
            new SetConfigurationFingerprintPayload("config-fingerprint"));

        var subscription = new SubscriptionSnapshot(
            "query-key", "https://drasi", null, "query", false, null, null, "offline");
        await Apply(BridgeCommandKind.UpsertSubscription, new UpsertSubscriptionPayload(subscription));
        await Apply(BridgeCommandKind.RemoveSubscription, new RemoveSubscriptionPayload(subscription.QueryKey));
        var mapping = new KeyMappingSnapshot("scope", "identity", "session", "active", now);
        await Apply(BridgeCommandKind.UpsertKeyMapping, new UpsertKeyMappingPayload(mapping));
        await Apply(BridgeCommandKind.RemoveKeyMapping,
            new RemoveKeyMappingPayload(mapping.ContractScope, mapping.CanonicalIdentity));

        await Apply(BridgeCommandKind.ClaimDispatchable,
        new ClaimDispatchablePayload([targetless.Id], now.AddSeconds(3), Guid.NewGuid()));
        await Apply(BridgeCommandKind.MarkRetryScheduled,
            new MarkRetryScheduledPayload(targetless.Id, 1, now.AddMinutes(1), "retry"));
        await Apply(BridgeCommandKind.ClaimDispatchable,
        new ClaimDispatchablePayload([targetless.Id], now.AddMinutes(2), Guid.NewGuid()));
        var checkpoint = new CoreSnapshotCheckpoint(
            targetless.BindingId, targetless.SessionId, targetless.SnapshotFingerprint, now.AddMinutes(2));
        await Apply(BridgeCommandKind.MarkAcceptedWithCheckpoint,
            new MarkAcceptedWithCheckpointPayload(
                targetless.Id, new WakeAcceptance("invocation", checkpoint.AcceptedAtUtc), checkpoint));
        await Apply(BridgeCommandKind.UpdateExecutionStatus,
            new UpdateExecutionStatusPayload(new WakeExecutionStatus("invocation", "Running", now)));
        await Apply(BridgeCommandKind.UpdateExecutionStatus,
            new UpdateExecutionStatusPayload(new WakeExecutionStatus("invocation", "Completed", now)));
        await database.InsertOutboxEntityAsync(new WakeOutbox
        {
            Id = Guid.NewGuid(),
            BindingId = "interrupted-binding",
            SessionId = "interrupted-session",
            SnapshotFingerprint = "interrupted-fingerprint",
            Skill = "skill",
            OpenClawTarget = "target",
            InputJson = "{}",
            ContractVersion = "v1",
            IdempotencyKey = $"interrupted-{Guid.NewGuid():N}",
            CreatedAtUtc = now,
            NextAttemptAtUtc = now,
            Status = WakeOutboxStatus.Dispatching,
            Version = 1
        });
        await Apply(BridgeCommandKind.RecoverInterruptedDispatches,
            new RecoverInterruptedDispatchesPayload(now.AddMinutes(3)));

        var snapshot = await database.Store.ExportSnapshotAsync(TestContext.Current.CancellationToken);
        Assert.Equal(17, await database.Store.GetLastAppliedIndexAsync(TestContext.Current.CancellationToken));
        Assert.Equal("config-fingerprint", snapshot.ConfigurationFingerprint);
        Assert.Empty(snapshot.Subscriptions);
        Assert.Empty(snapshot.KeyMappings);
        Assert.Equal(WakeOutboxStatus.Superseded, snapshot.WakeOutbox.Single(item => item.Id == pending.Id).Status);
        Assert.Equal("dead", snapshot.WakeOutbox.Single(item => item.Id == rejected.Id).LastErrorCode);
        Assert.Equal("target", snapshot.WakeOutbox.Single(item => item.Id == targetless.Id).OpenClawTarget);
        Assert.Equal(WakeOutboxStatus.Completed, snapshot.WakeOutbox.Single(item => item.Id == targetless.Id).Status);
        Assert.Contains(snapshot.WakeOutbox, item =>
            item.Status == WakeOutboxStatus.RetryScheduled && item.LastErrorCode == "dispatch.recovered");
        Assert.Single(snapshot.SnapshotCheckpoints);
    }

    [Fact]
    public async Task Snapshot_round_trip_preserves_every_table_nullable_fields_json_and_timestamps()
    {
        await using var source = await TestDatabase.CreateAsync();
        await using var target = await TestDatabase.CreateAsync();
        var timestamp = DateTimeOffset.Parse("2026-10-07T01:02:03.456Z");
        await source.InsertSnapshotRowsAsync(
            new SubscriptionSnapshot("query-key", "https://drasi", null, "query", true, timestamp, null, null),
            new SnapshotCheckpointSnapshot("binding", "session", "fingerprint", timestamp),
            new KeyMappingSnapshot("scope", "identity", "session", "active", timestamp),
            new WakeOutboxSnapshot(
                Guid.Parse("11111111-1111-1111-1111-111111111111"),
                "binding", "session", "fingerprint", "skill", null, """{"a":[1,true],"b":null}""",
                "v1", "idem-key", 3, timestamp, timestamp.AddMinutes(2), WakeOutboxStatus.RetryScheduled,
                null, "trace", "retry.reason", timestamp.AddDays(2), 7),
            lastAppliedIndex: 4,
            lastAppliedCommandId: "command-4",
            configurationFingerprint: "config-4");

        var snapshot = await source.Store.ExportSnapshotAsync(TestContext.Current.CancellationToken);
        await target.Store.RestoreSnapshotAsync(snapshot, TestContext.Current.CancellationToken);
        var restored = await target.Store.ExportSnapshotAsync(TestContext.Current.CancellationToken);

        Assert.Equal(JsonSerializer.Serialize(snapshot), JsonSerializer.Serialize(restored));
        Assert.Equal(snapshot.Subscriptions, restored.Subscriptions);
        Assert.Equal(snapshot.SnapshotCheckpoints, restored.SnapshotCheckpoints);
        Assert.Equal(snapshot.KeyMappings, restored.KeyMappings);
        Assert.Equal(snapshot.WakeOutbox, restored.WakeOutbox);
        Assert.Equal("command-4", restored.LastAppliedCommandId);
        Assert.Equal("config-4", restored.ConfigurationFingerprint);
    }

    [Fact]
    public async Task Snapshot_export_is_consistent_with_a_concurrent_command()
    {
        await using var database = await TestDatabase.CreateAsync();
        var interceptor = new PauseAtSubscriptionsReadInterceptor();
        var exportingStore = new SonnetBridgeStore(new TestContextFactory(
            new DbContextOptionsBuilder<BridgeDbContext>(database.Options)
                .AddInterceptors(interceptor)
                .Options));
        var cancellationToken = TestContext.Current.CancellationToken;
        var exportTask = exportingStore.ExportSnapshotAsync(cancellationToken).AsTask();
        await interceptor.Paused.WaitAsync(TimeSpan.FromSeconds(10), cancellationToken);

        var wake = CreateWake(DateTimeOffset.UtcNow);
        var command = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.CreateOrUpdatePendingWake,
            new CreateOrUpdatePendingWakePayload(wake));
        var applyTask = database.Store.ApplyReplicatedCommandAsync(command, 1, cancellationToken).AsTask();
        try
        {
            await Task.WhenAny(applyTask, Task.Delay(TimeSpan.FromSeconds(2), cancellationToken));
        }
        finally
        {
            interceptor.Resume();
        }

        var snapshot = await exportTask;
        await applyTask;

        Assert.Equal(snapshot.WakeOutbox.Count, snapshot.LastAppliedIndex);
    }

    [Fact]
    public async Task Invalid_snapshot_is_rejected_without_changing_the_existing_projection()
    {
        await using var database = await TestDatabase.CreateAsync();
        var wake = CreateWake(DateTimeOffset.UtcNow);
        await database.InsertOutboxAsync(wake);
        var before = await database.Store.ExportSnapshotAsync(TestContext.Current.CancellationToken);

        var duplicate = before with
        {
            SnapshotCheckpoints =
            [
                new SnapshotCheckpointSnapshot("binding", "session", "one", DateTimeOffset.UtcNow),
                new SnapshotCheckpointSnapshot("binding", "session", "two", DateTimeOffset.UtcNow)
            ]
        };
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await database.Store.RestoreSnapshotAsync(duplicate, TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await database.Store.RestoreSnapshotAsync(
                before with { SchemaVersion = BridgeStoreSnapshot.CurrentSchemaVersion + 1 },
                TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await database.Store.RestoreSnapshotAsync(
                before with { LastAppliedIndex = -1 },
                TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await database.Store.RestoreSnapshotAsync(
                before with
                {
                    WakeOutbox =
                    [
                        before.WakeOutbox.Single() with { BindingId = string.Empty }
                    ]
                },
                TestContext.Current.CancellationToken));

        Assert.Equal(
            JsonSerializer.Serialize(before),
            JsonSerializer.Serialize(await database.Store.ExportSnapshotAsync(TestContext.Current.CancellationToken)));
    }

    [Fact]
    public async Task Snapshot_save_failure_rolls_back_deletions_and_projection_metadata()
    {
        await using var database = await TestDatabase.CreateAsync();
        var oldWake = CreateWake(DateTimeOffset.UtcNow);
        await database.InsertOutboxAsync(oldWake);
        await database.Store.ApplyReplicatedCommandAsync(
            ReplicatedBridgeCommand.Create(
                BridgeCommandKind.SetConfigurationFingerprint,
                new SetConfigurationFingerprintPayload("old-config")),
            1,
            TestContext.Current.CancellationToken);
        var before = await database.Store.ExportSnapshotAsync(TestContext.Current.CancellationToken);
        var incoming = new BridgeStoreSnapshot(
            BridgeStoreSnapshot.CurrentSchemaVersion,
            7,
            new string('a', 64),
            "new-config",
            [],
            [],
            [],
            [new WakeOutboxSnapshot(
                Guid.NewGuid(), "new-binding", "new-session", "new-fingerprint", "new-skill",
                "target", "{}", "v1", "new-idempotency-key", 0, DateTimeOffset.UtcNow,
                DateTimeOffset.UtcNow, WakeOutboxStatus.Pending, null, null, null, null, 1)]);
        var failingStore = new SonnetBridgeStore(new TestContextFactory(
            new DbContextOptionsBuilder<BridgeDbContext>(database.Options)
                .AddInterceptors(new FailOnSecondSaveChangesInterceptor())
                .Options));

        await Assert.ThrowsAsync<InjectedStoreFailure>(async () =>
            await failingStore.RestoreSnapshotAsync(incoming, TestContext.Current.CancellationToken));

        var after = await database.Store.ExportSnapshotAsync(TestContext.Current.CancellationToken);
        Assert.Equal(JsonSerializer.Serialize(before), JsonSerializer.Serialize(after));
    }

    private static WakeOutboxItem CreateWake(DateTimeOffset createdAtUtc) => new(
        Guid.NewGuid(),
        "binding",
        "session",
        "fingerprint",
        "skill",
        new JsonObject { ["value"] = 1 },
        "v1",
        $"idem-{Guid.NewGuid():N}",
        0,
        createdAtUtc,
        createdAtUtc,
        WakeOutboxStatus.Pending,
        null,
        null,
        "target");

    private sealed class TestDatabase : IAsyncDisposable
    {
        private readonly string _databaseDirectory;
        private readonly DbContextOptions<BridgeDbContext> _options;

        private TestDatabase(string databaseDirectory, DbContextOptions<BridgeDbContext> options)
        {
            _databaseDirectory = databaseDirectory;
            _options = options;
            Store = new SonnetBridgeStore(new TestContextFactory(options));
        }

        public SonnetBridgeStore Store { get; }
        public DbContextOptions<BridgeDbContext> Options => _options;

        public static async Task<TestDatabase> CreateAsync()
        {
            var directory = Path.Combine(AppContext.BaseDirectory, "sonnetdb-testdata", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(directory);
            var options = new DbContextOptionsBuilder<BridgeDbContext>()
                .UseSonnetDB($"Data Source={directory}")
                .Options;
            var database = new TestDatabase(directory, options);
            await using var context = new BridgeDbContext(options);
            await context.Database.MigrateAsync(TestContext.Current.CancellationToken);
            return database;
        }

        public async Task InsertOutboxAsync(params WakeOutboxItem[] items)
        {
            await using var context = new BridgeDbContext(_options);
            context.WakeOutbox.AddRange(items.Select(item => new WakeOutbox
            {
                Id = item.Id,
                BindingId = item.BindingId,
                SessionId = item.SessionId,
                SnapshotFingerprint = item.SnapshotFingerprint,
                Skill = item.Skill,
                OpenClawTarget = item.OpenClawTarget,
                InputJson = item.Input.ToJsonString(),
                ContractVersion = item.ContractVersion,
                IdempotencyKey = item.IdempotencyKey,
                AttemptCount = item.AttemptCount,
                CreatedAtUtc = item.CreatedAtUtc,
                NextAttemptAtUtc = item.NextAttemptAtUtc,
                Status = item.Status,
                InvocationId = item.InvocationId,
                TraceId = item.TraceId,
                Version = 1
            }));
            await context.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        public async Task InsertOutboxEntityAsync(WakeOutbox item)
        {
            await using var context = new BridgeDbContext(_options);
            context.WakeOutbox.Add(item);
            await context.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        public async Task InsertSnapshotRowsAsync(
            SubscriptionSnapshot subscription,
            SnapshotCheckpointSnapshot checkpoint,
            KeyMappingSnapshot mapping,
            WakeOutboxSnapshot outbox,
            long lastAppliedIndex,
            string lastAppliedCommandId,
            string configurationFingerprint)
        {
            await using var context = new BridgeDbContext(_options);
            context.Subscriptions.Add(new SubscriptionState
            {
                QueryKey = subscription.QueryKey,
                ServerUri = subscription.ServerUri,
                InstanceId = subscription.InstanceId,
                QueryId = subscription.QueryId,
                IsConnected = subscription.IsConnected,
                LastConnectedAtUtc = subscription.LastConnectedAtUtc,
                LastReconciledAtUtc = subscription.LastReconciledAtUtc,
                LastErrorCode = subscription.LastErrorCode
            });
            context.SnapshotCheckpoints.Add(new SnapshotCheckpointEntity
            {
                BindingId = checkpoint.BindingId,
                SessionId = checkpoint.SessionId,
                Fingerprint = checkpoint.Fingerprint,
                AcceptedAtUtc = checkpoint.AcceptedAtUtc
            });
            context.KeyMappings.Add(new KeyMapping
            {
                ContractScope = mapping.ContractScope,
                CanonicalIdentity = mapping.CanonicalIdentity,
                SessionId = mapping.SessionId,
                State = mapping.State,
                UpdatedAtUtc = mapping.UpdatedAtUtc
            });
            context.WakeOutbox.Add(new WakeOutbox
            {
                Id = outbox.Id,
                BindingId = outbox.BindingId,
                SessionId = outbox.SessionId,
                SnapshotFingerprint = outbox.SnapshotFingerprint,
                Skill = outbox.Skill,
                OpenClawTarget = outbox.OpenClawTarget,
                InputJson = outbox.InputJson,
                ContractVersion = outbox.ContractVersion,
                IdempotencyKey = outbox.IdempotencyKey,
                AttemptCount = outbox.AttemptCount,
                CreatedAtUtc = outbox.CreatedAtUtc,
                NextAttemptAtUtc = outbox.NextAttemptAtUtc,
                Status = outbox.Status,
                InvocationId = outbox.InvocationId,
                TraceId = outbox.TraceId,
                LastErrorCode = outbox.LastErrorCode,
                RetainUntilUtc = outbox.RetainUntilUtc,
                Version = outbox.Version
            });
            context.RaftProjectionStates.Add(new RaftProjectionState
            {
                Id = RaftProjectionState.SingletonId,
                LastAppliedIndex = lastAppliedIndex,
                LastAppliedCommandId = lastAppliedCommandId,
                ConfigurationFingerprint = configurationFingerprint
            });
            await context.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        public async Task<IReadOnlyList<WakeOutboxSnapshot>> ReadOutboxAsync()
        {
            await using var context = new BridgeDbContext(_options);
            return await context.WakeOutbox.AsNoTracking()
                .OrderBy(item => item.Id)
                .Select(item => new WakeOutboxSnapshot(
                    item.Id, item.BindingId, item.SessionId, item.SnapshotFingerprint, item.Skill,
                    item.OpenClawTarget, item.InputJson, item.ContractVersion, item.IdempotencyKey,
                    item.AttemptCount, item.CreatedAtUtc, item.NextAttemptAtUtc, item.Status,
                    item.InvocationId, item.TraceId, item.LastErrorCode, item.RetainUntilUtc, item.Version))
                .ToListAsync(TestContext.Current.CancellationToken);
        }

        public async Task<IReadOnlyList<SnapshotCheckpointSnapshot>> ReadCheckpointsAsync()
        {
            await using var context = new BridgeDbContext(_options);
            return await context.SnapshotCheckpoints.AsNoTracking()
                .Select(item => new SnapshotCheckpointSnapshot(
                    item.BindingId, item.SessionId, item.Fingerprint, item.AcceptedAtUtc))
                .ToListAsync(TestContext.Current.CancellationToken);
        }

        public ValueTask DisposeAsync()
        {
            Directory.Delete(_databaseDirectory, recursive: true);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class TestContextFactory(DbContextOptions<BridgeDbContext> options)
        : IDbContextFactory<BridgeDbContext>
    {
        public BridgeDbContext CreateDbContext() => new(options);

        public Task<BridgeDbContext> CreateDbContextAsync(CancellationToken cancellationToken = default) =>
            Task.FromResult(CreateDbContext());
    }

    private sealed class FailOnSaveChangesInterceptor : SaveChangesInterceptor
    {
        public override ValueTask<InterceptionResult<int>> SavingChangesAsync(
            DbContextEventData eventData,
            InterceptionResult<int> result,
            CancellationToken cancellationToken = default) =>
            ValueTask.FromException<InterceptionResult<int>>(new InjectedStoreFailure());
    }

    private sealed class FailOnSecondSaveChangesInterceptor : SaveChangesInterceptor
    {
        private int _saveCount;

        public override ValueTask<InterceptionResult<int>> SavingChangesAsync(
            DbContextEventData eventData,
            InterceptionResult<int> result,
            CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref _saveCount) == 2)
                throw new InjectedStoreFailure();
            return ValueTask.FromResult(result);
        }
    }

    private sealed class PauseAtSubscriptionsReadInterceptor : DbCommandInterceptor
    {
        private readonly TaskCompletionSource _paused =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _resume =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _hasPaused;

        public Task Paused => _paused.Task;

        public void Resume() => _resume.TrySetResult();

        public override async ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(
            DbCommand command,
            CommandEventData eventData,
            InterceptionResult<DbDataReader> result,
            CancellationToken cancellationToken = default)
        {
            if (command.CommandText.Contains("Subscriptions", StringComparison.Ordinal) &&
                Interlocked.Exchange(ref _hasPaused, 1) == 0)
            {
                _paused.TrySetResult();
                await _resume.Task.WaitAsync(cancellationToken);
            }

            return result;
        }
    }

    private sealed class InjectedStoreFailure : Exception;
}
