using DrasiWake.Core.Domain;
using DrasiWake.Persistence.SonnetDB;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using DrasiWake.Persistence.SonnetDB.Entities;
using DrasiWake.Persistence.SonnetDB.Replication;
using CoreSnapshotCheckpoint = DrasiWake.Core.Domain.SnapshotCheckpoint;
using SnapshotCheckpointEntity = DrasiWake.Persistence.SonnetDB.Entities.SnapshotCheckpoint;
using WakeOutboxEntity = DrasiWake.Persistence.SonnetDB.Entities.WakeOutbox;

namespace DrasiWake.Persistence.SonnetDB.Tests;

public sealed class AtomicAcceptanceTests
{
    [Fact]
    public async Task Failed_transaction_changes_neither_outbox_nor_checkpoint()
    {
        var databaseDirectory = Path.Combine(Path.GetTempPath(), $"DrasiWake-{Guid.NewGuid():N}");
        Directory.CreateDirectory(databaseDirectory);
        try
        {
            var connectionString = $"Data Source={databaseDirectory}";
            var plainOptions = CreateOptions(connectionString);
            var pendingWake = CreatePendingWake();
            var oldCheckpoint = new SnapshotCheckpointEntity
            {
                BindingId = pendingWake.BindingId,
                SessionId = pendingWake.SessionId,
                Fingerprint = "old-fingerprint",
                AcceptedAtUtc = DateTimeOffset.UtcNow.AddMinutes(-1)
            };

            await using (var setupContext = new BridgeDbContext(plainOptions))
            {
                await setupContext.Database.MigrateAsync(TestContext.Current.CancellationToken);
                setupContext.Set<WakeOutboxEntity>().Add(pendingWake);
                setupContext.Set<SnapshotCheckpointEntity>().Add(oldCheckpoint);
                await setupContext.SaveChangesAsync(TestContext.Current.CancellationToken);
            }

            var failingOptions = CreateOptions(connectionString, new FailOnFirstSaveChangesInterceptor());
            var store = new SonnetBridgeStore(new TestContextFactory(failingOptions));
            var newCheckpoint = new CoreSnapshotCheckpoint(
                pendingWake.BindingId,
                pendingWake.SessionId,
                pendingWake.SnapshotFingerprint,
                DateTimeOffset.UtcNow);
            var acceptance = new WakeAcceptance("invocation-failed-transaction", newCheckpoint.AcceptedAtUtc);

            await Assert.ThrowsAsync<InjectedStoreFailure>(async () =>
                await ApplyAsync(
                    store,
                    BridgeCommandKind.MarkAcceptedWithCheckpoint,
                    new MarkAcceptedWithCheckpointPayload(pendingWake.Id, acceptance, newCheckpoint),
                    TestContext.Current.CancellationToken));

            await using var verificationContext = new BridgeDbContext(plainOptions);
            var persistedWake = await verificationContext.Set<WakeOutboxEntity>()
                .SingleAsync(item => item.Id == pendingWake.Id, TestContext.Current.CancellationToken);
            var persistedCheckpoint = await verificationContext.Set<SnapshotCheckpointEntity>()
                .SingleAsync(
                    checkpoint => checkpoint.BindingId == pendingWake.BindingId &&
                                  checkpoint.SessionId == pendingWake.SessionId,
                    TestContext.Current.CancellationToken);

            Assert.Equal(WakeOutboxStatus.Pending, persistedWake.Status);
            Assert.Equal(oldCheckpoint.Fingerprint, persistedCheckpoint.Fingerprint);
        }
        finally
        {
            Directory.Delete(databaseDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task Accepted_checkpoint_survives_database_reopen()
    {
        var databaseDirectory = Path.Combine(Path.GetTempPath(), $"DrasiWake-{Guid.NewGuid():N}");
        Directory.CreateDirectory(databaseDirectory);
        try
        {
            var options = CreateOptions($"Data Source={databaseDirectory}");
            var pendingWake = CreatePendingWake();
            await using (var initialContext = new BridgeDbContext(options))
            {
                await initialContext.Database.MigrateAsync(TestContext.Current.CancellationToken);
            }

            var store = new SonnetBridgeStore(new TestContextFactory(options));
            await ApplyAsync(
                store,
                BridgeCommandKind.CreateOrUpdatePendingWake,
                new CreateOrUpdatePendingWakePayload(ToDomain(pendingWake)),
                TestContext.Current.CancellationToken);
            var acceptedAt = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            var acceptance = new WakeAcceptance("invocation-reopen", acceptedAt);
            var checkpoint = new CoreSnapshotCheckpoint(
                pendingWake.BindingId,
                pendingWake.SessionId,
                pendingWake.SnapshotFingerprint,
                acceptedAt);
            await ApplyAsync(
                store,
                BridgeCommandKind.MarkAcceptedWithCheckpoint,
                new MarkAcceptedWithCheckpointPayload(pendingWake.Id, acceptance, checkpoint),
                TestContext.Current.CancellationToken);

            await using var reopenedContext = new BridgeDbContext(options);
            await reopenedContext.Database.MigrateAsync(TestContext.Current.CancellationToken);
            var migrations = await reopenedContext.Database.GetAppliedMigrationsAsync(
                TestContext.Current.CancellationToken);
            var persistedWake = await reopenedContext.WakeOutbox.AsNoTracking()
                .SingleAsync(item => item.Id == pendingWake.Id, TestContext.Current.CancellationToken);
            var persistedCheckpoint = await reopenedContext.SnapshotCheckpoints.AsNoTracking()
                .SingleAsync(
                    item => item.BindingId == pendingWake.BindingId && item.SessionId == pendingWake.SessionId,
                    TestContext.Current.CancellationToken);

            Assert.Equal(4, migrations.Count());
            Assert.Equal(WakeOutboxStatus.Accepted, persistedWake.Status);
            Assert.Equal(pendingWake.OpenClawTarget, persistedWake.OpenClawTarget);
            Assert.Equal(pendingWake.SnapshotFingerprint, persistedCheckpoint.Fingerprint);
        }
        finally
        {
            Directory.Delete(databaseDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task Legacy_pending_row_is_backfilled_after_nullable_target_migration()
    {
        var databaseDirectory = Path.Combine(Path.GetTempPath(), $"DrasiWake-{Guid.NewGuid():N}");
        Directory.CreateDirectory(databaseDirectory);
        try
        {
            var options = CreateOptions($"Data Source={databaseDirectory}");
            var outboxId = Guid.NewGuid();
            var now = DateTimeOffset.UtcNow;
            const string bindingId = "binding-1";
            const string sessionId = "session-legacy";
            const string snapshotFingerprint = "legacy-fingerprint";
            const string skill = "triage-order";
            const string inputJson = "{}";
            const string contractVersion = "v1";
            var idempotencyKey = $"drasiwake:{outboxId:N}";
            string? nullValue = null;

            await using (var context = new BridgeDbContext(options))
            {
                var migrator = context.GetService<IMigrator>();
                await migrator.MigrateAsync(
                    "20261002145302_InitialBridgeState",
                    TestContext.Current.CancellationToken);
                await context.Database.ExecuteSqlInterpolatedAsync($"""
                    INSERT INTO "WakeOutbox" (
                        "Id", "BindingId", "SessionId", "SnapshotFingerprint", "Skill", "InputJson",
                        "ContractVersion", "IdempotencyKey", "AttemptCount", "CreatedAtUtc",
                        "NextAttemptAtUtc", "Status", "InvocationId", "TraceId", "LastErrorCode",
                        "RetainUntilUtc", "Version")
                    VALUES (
                        {outboxId}, {bindingId}, {sessionId}, {snapshotFingerprint}, {skill}, {inputJson},
                        {contractVersion}, {idempotencyKey}, {0}, {now}, {now}, {0}, {nullValue},
                        {nullValue}, {nullValue}, {nullValue}, {1})
                    """, TestContext.Current.CancellationToken);

                await migrator.MigrateAsync(cancellationToken: TestContext.Current.CancellationToken);
                var beforeBackfill = await context.WakeOutbox.AsNoTracking()
                    .SingleAsync(item => item.Id == outboxId, TestContext.Current.CancellationToken);
                Assert.Null(beforeBackfill.OpenClawTarget);
            }

            var store = new SonnetBridgeStore(new TestContextFactory(options));
            await ApplyAsync(
                store,
                BridgeCommandKind.EnsureOpenClawTargets,
                new EnsureOpenClawTargetsPayload(
                    new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        [bindingId] = "sample-gateway"
                    },
                    new HashSet<string>(StringComparer.Ordinal) { "sample-gateway" },
                    new Dictionary<string, TimeSpan>(StringComparer.Ordinal)
                    {
                        [bindingId] = TimeSpan.FromDays(1)
                    },
                    new Dictionary<string, TimeSpan>(StringComparer.Ordinal)
                    {
                        ["sample-gateway"] = TimeSpan.FromDays(30)
                    }),
                TestContext.Current.CancellationToken);

            await using var verificationContext = new BridgeDbContext(options);
            var persisted = await verificationContext.WakeOutbox.AsNoTracking()
                .SingleAsync(item => item.Id == outboxId, TestContext.Current.CancellationToken);
            Assert.Equal("sample-gateway", persisted.OpenClawTarget);
        }
        finally
        {
            Directory.Delete(databaseDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task Acceptance_receipt_and_checkpoint_are_persisted_together()
    {
        var databaseDirectory = Path.Combine(Path.GetTempPath(), $"DrasiWake-{Guid.NewGuid():N}");
        Directory.CreateDirectory(databaseDirectory);
        try
        {
            var options = CreateOptions($"Data Source={databaseDirectory}");
            var pendingWake = CreatePendingWake();
            await using (var context = new BridgeDbContext(options))
            {
                await context.Database.MigrateAsync(TestContext.Current.CancellationToken);
            }

            var store = new SonnetBridgeStore(new TestContextFactory(options));
            await ApplyAsync(
                store,
                BridgeCommandKind.CreateOrUpdatePendingWake,
                new CreateOrUpdatePendingWakePayload(ToDomain(pendingWake)),
                TestContext.Current.CancellationToken);
            var acceptance = new WakeAcceptance("invocation-42", DateTimeOffset.UtcNow);
            var checkpoint = new CoreSnapshotCheckpoint(
                pendingWake.BindingId,
                pendingWake.SessionId,
                pendingWake.SnapshotFingerprint,
                acceptance.AcceptedAtUtc);

            await ApplyAsync(
                store,
                BridgeCommandKind.MarkAcceptedWithCheckpoint,
                new MarkAcceptedWithCheckpointPayload(pendingWake.Id, acceptance, checkpoint),
                TestContext.Current.CancellationToken);

            await using var verificationContext = new BridgeDbContext(options);
            var persistedWake = await verificationContext.WakeOutbox.AsNoTracking()
                .SingleAsync(item => item.Id == pendingWake.Id, TestContext.Current.CancellationToken);
            var persistedCheckpoint = await verificationContext.SnapshotCheckpoints.AsNoTracking()
                .SingleAsync(item => item.BindingId == pendingWake.BindingId && item.SessionId == pendingWake.SessionId,
                    TestContext.Current.CancellationToken);

            Assert.Equal(WakeOutboxStatus.Accepted, persistedWake.Status);
            Assert.Equal(acceptance.InvocationId, persistedWake.InvocationId);
            Assert.Equal(checkpoint.Fingerprint, persistedCheckpoint.Fingerprint);
        }
        finally
        {
            Directory.Delete(databaseDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task Execution_receipt_updates_outbox_without_changing_checkpoint()
    {
        var databaseDirectory = Path.Combine(Path.GetTempPath(), $"DrasiWake-{Guid.NewGuid():N}");
        Directory.CreateDirectory(databaseDirectory);
        try
        {
            var options = CreateOptions($"Data Source={databaseDirectory}");
            var pendingWake = CreatePendingWake();
            await using (var context = new BridgeDbContext(options))
            {
                await context.Database.MigrateAsync(TestContext.Current.CancellationToken);
            }

            var store = new SonnetBridgeStore(new TestContextFactory(options));
            await ApplyAsync(
                store,
                BridgeCommandKind.CreateOrUpdatePendingWake,
                new CreateOrUpdatePendingWakePayload(ToDomain(pendingWake)),
                TestContext.Current.CancellationToken);
            var acceptedAt = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            var checkpoint = new CoreSnapshotCheckpoint(
                pendingWake.BindingId,
                pendingWake.SessionId,
                pendingWake.SnapshotFingerprint,
                acceptedAt);
            var acceptance = new WakeAcceptance("invocation-execution", acceptedAt);
            await ApplyAsync(
                store,
                BridgeCommandKind.MarkAcceptedWithCheckpoint,
                new MarkAcceptedWithCheckpointPayload(pendingWake.Id, acceptance, checkpoint),
                TestContext.Current.CancellationToken);

            await ApplyAsync(
                store,
                BridgeCommandKind.UpdateExecutionStatus,
                new UpdateExecutionStatusPayload(
                    new WakeExecutionStatus("invocation-execution", "Completed", acceptedAt.AddSeconds(5))),
                TestContext.Current.CancellationToken);

            await using var verificationContext = new BridgeDbContext(options);
            var persistedWake = await verificationContext.WakeOutbox.AsNoTracking()
                .SingleAsync(item => item.Id == pendingWake.Id, TestContext.Current.CancellationToken);
            var persistedCheckpoint = await verificationContext.SnapshotCheckpoints.AsNoTracking()
                .SingleAsync(item => item.BindingId == pendingWake.BindingId && item.SessionId == pendingWake.SessionId,
                    TestContext.Current.CancellationToken);

            Assert.Equal(WakeOutboxStatus.Completed, persistedWake.Status);
            Assert.Equal(checkpoint.Fingerprint, persistedCheckpoint.Fingerprint);
            Assert.Equal(checkpoint.AcceptedAtUtc, persistedCheckpoint.AcceptedAtUtc);
        }
        finally
        {
            Directory.Delete(databaseDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task Rejected_wake_is_persisted_as_dead_letter_once_with_reason_code()
    {
        var databaseDirectory = Path.Combine(Path.GetTempPath(), $"DrasiWake-{Guid.NewGuid():N}");
        Directory.CreateDirectory(databaseDirectory);
        try
        {
            var options = CreateOptions($"Data Source={databaseDirectory}");
            await using (var context = new BridgeDbContext(options))
            {
                await context.Database.MigrateAsync(TestContext.Current.CancellationToken);
            }

            var entity = CreatePendingWake();
            var rejectedWake = ToDomain(entity) with { Status = WakeOutboxStatus.DeadLetter };
            var store = new SonnetBridgeStore(new TestContextFactory(options));
            var rejectedPayload = new RecordRejectedWakePayload(rejectedWake, "payload.schema_invalid");
            await ApplyAsync(store, BridgeCommandKind.RecordRejectedWake, rejectedPayload, TestContext.Current.CancellationToken);
            await ApplyAsync(store, BridgeCommandKind.RecordRejectedWake, rejectedPayload, TestContext.Current.CancellationToken);

            await using var verificationContext = new BridgeDbContext(options);
            var persisted = await verificationContext.WakeOutbox.AsNoTracking()
                .SingleAsync(item => item.Id == rejectedWake.Id, TestContext.Current.CancellationToken);

            Assert.Equal(WakeOutboxStatus.DeadLetter, persisted.Status);
            Assert.Equal("payload.schema_invalid", persisted.LastErrorCode);
            Assert.Equal(rejectedWake.IdempotencyKey, persisted.IdempotencyKey);
        }
        finally
        {
            Directory.Delete(databaseDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task LoadDispatchableAsync_claims_due_items_in_stable_order_once()
    {
        var databaseDirectory = Path.Combine(Path.GetTempPath(), $"DrasiWake-{Guid.NewGuid():N}");
        Directory.CreateDirectory(databaseDirectory);
        try
        {
            var options = CreateOptions($"Data Source={databaseDirectory}");
            var first = CreatePendingWake();
            var second = CreatePendingWake();
            first.NextAttemptAtUtc = DateTimeOffset.UtcNow.AddMinutes(-2);
            second.NextAttemptAtUtc = DateTimeOffset.UtcNow.AddMinutes(-1);
            await using (var context = new BridgeDbContext(options))
            {
                await context.Database.MigrateAsync(TestContext.Current.CancellationToken);
                context.WakeOutbox.AddRange(first, second);
                await context.SaveChangesAsync(TestContext.Current.CancellationToken);
            }

            var nowUtc = DateTimeOffset.UtcNow;
            var projection = new SonnetBridgeStore(new TestContextFactory(options));
            var firstCandidate = Assert.Single(await projection.LoadDispatchableCandidatesAsync(
                nowUtc, 1, TestContext.Current.CancellationToken));
            var firstClaimCommand = ReplicatedBridgeCommand.Create(
                BridgeCommandKind.ClaimDispatchable,
                new ClaimDispatchablePayload([firstCandidate.Id], nowUtc, Guid.NewGuid()));
            await projection.ApplyReplicatedCommandAsync(
                firstClaimCommand, 1, TestContext.Current.CancellationToken);
            var firstClaim = await projection.ReadClaimedDispatchableAsync(
                [firstCandidate.Id], firstClaimCommand.CommandId, TestContext.Current.CancellationToken);

            var secondCandidate = Assert.Single(await projection.LoadDispatchableCandidatesAsync(
                nowUtc, 1, TestContext.Current.CancellationToken));
            var secondClaimCommand = ReplicatedBridgeCommand.Create(
                BridgeCommandKind.ClaimDispatchable,
                new ClaimDispatchablePayload([secondCandidate.Id], nowUtc, Guid.NewGuid()));
            await projection.ApplyReplicatedCommandAsync(
                secondClaimCommand, 2, TestContext.Current.CancellationToken);
            var secondClaim = await projection.ReadClaimedDispatchableAsync(
                [secondCandidate.Id], secondClaimCommand.CommandId, TestContext.Current.CancellationToken);
            var emptyClaim = await projection.LoadDispatchableCandidatesAsync(
                nowUtc, 1, TestContext.Current.CancellationToken);

            Assert.Equal(first.Id, Assert.Single(firstClaim).Id);
            Assert.Equal(second.Id, Assert.Single(secondClaim).Id);
            Assert.Equal(WakeOutboxStatus.Dispatching, firstClaim[0].Status);
            Assert.Equal(WakeOutboxStatus.Dispatching, secondClaim[0].Status);
            Assert.Empty(emptyClaim);
        }
        finally
        {
            Directory.Delete(databaseDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task CreateOrUpdatePendingWakeAsync_does_not_rewrite_superseded_item()
    {
        var databaseDirectory = Path.Combine(Path.GetTempPath(), $"DrasiWake-{Guid.NewGuid():N}");
        Directory.CreateDirectory(databaseDirectory);
        try
        {
            var options = CreateOptions($"Data Source={databaseDirectory}");
            var superseded = CreatePendingWake();
            superseded.Status = WakeOutboxStatus.Superseded;
            await using (var context = new BridgeDbContext(options))
            {
                await context.Database.MigrateAsync(TestContext.Current.CancellationToken);
                context.WakeOutbox.Add(superseded);
                await context.SaveChangesAsync(TestContext.Current.CancellationToken);
            }

            var projection = new SonnetBridgeStore(new TestContextFactory(options));
            var replacement = ToDomain(superseded) with
            {
                SnapshotFingerprint = "replacement-fingerprint",
                Status = WakeOutboxStatus.Pending,
                OpenClawTarget = "sample-gateway"
            };
            await ApplyAsync(
                projection,
                BridgeCommandKind.CreateOrUpdatePendingWake,
                new CreateOrUpdatePendingWakePayload(replacement),
                TestContext.Current.CancellationToken);
            var persisted = await projection.FindPendingWakeResultAsync(
                replacement.Id,
                replacement.BindingId,
                replacement.SessionId,
                replacement.SnapshotFingerprint,
                TestContext.Current.CancellationToken);

            Assert.NotNull(persisted);
            Assert.Equal(WakeOutboxStatus.Superseded, persisted.Status);
            Assert.Equal(superseded.SnapshotFingerprint, persisted.SnapshotFingerprint);
        }
        finally
        {
            Directory.Delete(databaseDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task Repeated_acceptance_does_not_demote_completed_outbox()
    {
        var databaseDirectory = Path.Combine(Path.GetTempPath(), $"DrasiWake-{Guid.NewGuid():N}");
        Directory.CreateDirectory(databaseDirectory);
        try
        {
            var options = CreateOptions($"Data Source={databaseDirectory}");
            var pendingWake = CreatePendingWake();
            await using (var context = new BridgeDbContext(options))
            {
                await context.Database.MigrateAsync(TestContext.Current.CancellationToken);
            }

            var projection = new SonnetBridgeStore(new TestContextFactory(options));
            await ApplyAsync(
                projection,
                BridgeCommandKind.CreateOrUpdatePendingWake,
                new CreateOrUpdatePendingWakePayload(ToDomain(pendingWake)),
                TestContext.Current.CancellationToken);
            var checkpoint = new CoreSnapshotCheckpoint(
                pendingWake.BindingId,
                pendingWake.SessionId,
                pendingWake.SnapshotFingerprint,
                DateTimeOffset.UtcNow);
            var acceptance = new WakeAcceptance("invocation-repeat", checkpoint.AcceptedAtUtc);
            var acceptedCommand = new MarkAcceptedWithCheckpointPayload(pendingWake.Id, acceptance, checkpoint);
            await ApplyAsync(
                projection,
                BridgeCommandKind.MarkAcceptedWithCheckpoint,
                acceptedCommand,
                TestContext.Current.CancellationToken);

            await ApplyAsync(
                projection,
                BridgeCommandKind.UpdateExecutionStatus,
                new UpdateExecutionStatusPayload(
                    new WakeExecutionStatus("invocation-repeat", "Completed", checkpoint.AcceptedAtUtc)),
                TestContext.Current.CancellationToken);

            await ApplyAsync(
                projection,
                BridgeCommandKind.MarkAcceptedWithCheckpoint,
                acceptedCommand,
                TestContext.Current.CancellationToken);

            await using var verificationContext = new BridgeDbContext(options);
            var status = await verificationContext.WakeOutbox.AsNoTracking()
                .Where(item => item.Id == pendingWake.Id)
                .Select(item => item.Status)
                .SingleAsync(TestContext.Current.CancellationToken);
            Assert.Equal(WakeOutboxStatus.Completed, status);
        }
        finally
        {
            Directory.Delete(databaseDirectory, recursive: true);
        }
    }

    private static WakeOutboxEntity CreatePendingWake() => new()
    {
        Id = Guid.NewGuid(),
        BindingId = "binding-1",
        SessionId = "session-1",
        SnapshotFingerprint = "new-fingerprint",
        Skill = "triage-order",
        OpenClawTarget = "sample-gateway",
        InputJson = "{}",
        ContractVersion = "v1",
        IdempotencyKey = $"drasiwake:{Guid.NewGuid():N}",
        AttemptCount = 0,
        CreatedAtUtc = DateTimeOffset.UtcNow,
        NextAttemptAtUtc = DateTimeOffset.UtcNow,
        Status = WakeOutboxStatus.Pending
    };

    private static WakeOutboxItem ToDomain(WakeOutboxEntity item) => new(
        item.Id,
        item.BindingId,
        item.SessionId,
        item.SnapshotFingerprint,
        item.Skill,
        new System.Text.Json.Nodes.JsonObject(),
        item.ContractVersion,
        item.IdempotencyKey,
        item.AttemptCount,
        item.CreatedAtUtc,
        item.NextAttemptAtUtc,
        item.Status,
        item.InvocationId,
        item.TraceId,
        item.OpenClawTarget!);

    private static async Task ApplyAsync<TPayload>(
        SonnetBridgeStore projection,
        BridgeCommandKind kind,
        TPayload payload,
        CancellationToken cancellationToken)
    {
        var command = ReplicatedBridgeCommand.Create(kind, payload);
        var nextIndex = await projection.GetLastAppliedIndexAsync(cancellationToken) + 1;
        await projection.ApplyReplicatedCommandAsync(command, nextIndex, cancellationToken);
    }

    private static DbContextOptions<BridgeDbContext> CreateOptions(
        string connectionString,
        IInterceptor? interceptor = null)
    {
        var builder = new DbContextOptionsBuilder<BridgeDbContext>()
            .UseSonnetDB(connectionString);
        if (interceptor is not null)
        {
            builder.AddInterceptors(interceptor);
        }

        return builder.Options;
    }

    private sealed class TestContextFactory(DbContextOptions<BridgeDbContext> options)
        : IDbContextFactory<BridgeDbContext>
    {
        public BridgeDbContext CreateDbContext() => new(options);

        public Task<BridgeDbContext> CreateDbContextAsync(CancellationToken cancellationToken = default) =>
            Task.FromResult(CreateDbContext());
    }

    private sealed class FailOnFirstSaveChangesInterceptor : SaveChangesInterceptor
    {
        private int _saveCount;

        public override ValueTask<InterceptionResult<int>> SavingChangesAsync(
            DbContextEventData eventData,
            InterceptionResult<int> result,
            CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref _saveCount) == 1)
            {
                throw new InjectedStoreFailure();
            }

            return ValueTask.FromResult(result);
        }
    }

    private sealed class InjectedStoreFailure : Exception;
}