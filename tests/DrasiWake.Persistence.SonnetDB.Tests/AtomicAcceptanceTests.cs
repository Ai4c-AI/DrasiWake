using DrasiWake.Core.Domain;
using DrasiWake.Persistence.SonnetDB;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using DrasiWake.Persistence.SonnetDB.Entities;
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

            var failingOptions = CreateOptions(connectionString, new FailOnSecondSaveChangesInterceptor());
            var store = new SonnetBridgeStore(new TestContextFactory(failingOptions));
            var newCheckpoint = new CoreSnapshotCheckpoint(
                pendingWake.BindingId,
                pendingWake.SessionId,
                pendingWake.SnapshotFingerprint,
                DateTimeOffset.UtcNow);
            var acceptance = new WakeAcceptance("invocation-failed-transaction", newCheckpoint.AcceptedAtUtc);

            await Assert.ThrowsAsync<InjectedStoreFailure>(async () =>
                await store.MarkAcceptedWithCheckpointAsync(
                    pendingWake.Id,
                    acceptance,
                    newCheckpoint,
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
            await store.CreateOrUpdatePendingWakeAsync(ToDomain(pendingWake), TestContext.Current.CancellationToken);
            var acceptedAt = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            await store.MarkAcceptedWithCheckpointAsync(
                pendingWake.Id,
                new WakeAcceptance("invocation-reopen", acceptedAt),
                new CoreSnapshotCheckpoint(
                    pendingWake.BindingId,
                    pendingWake.SessionId,
                    pendingWake.SnapshotFingerprint,
                    acceptedAt),
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

            Assert.Single(migrations);
            Assert.Equal(WakeOutboxStatus.Accepted, persistedWake.Status);
            Assert.Equal(pendingWake.SnapshotFingerprint, persistedCheckpoint.Fingerprint);
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
            await store.CreateOrUpdatePendingWakeAsync(ToDomain(pendingWake), TestContext.Current.CancellationToken);
            var acceptance = new WakeAcceptance("invocation-42", DateTimeOffset.UtcNow);
            var checkpoint = new CoreSnapshotCheckpoint(
                pendingWake.BindingId,
                pendingWake.SessionId,
                pendingWake.SnapshotFingerprint,
                acceptance.AcceptedAtUtc);

            await store.MarkAcceptedWithCheckpointAsync(
                pendingWake.Id,
                acceptance,
                checkpoint,
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
            await store.CreateOrUpdatePendingWakeAsync(ToDomain(pendingWake), TestContext.Current.CancellationToken);
            var acceptedAt = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            var checkpoint = new CoreSnapshotCheckpoint(
                pendingWake.BindingId,
                pendingWake.SessionId,
                pendingWake.SnapshotFingerprint,
                acceptedAt);
            await store.MarkAcceptedWithCheckpointAsync(
                pendingWake.Id,
                new WakeAcceptance("invocation-execution", acceptedAt),
                checkpoint,
                TestContext.Current.CancellationToken);

            await store.UpdateExecutionStatusAsync(
                new WakeExecutionStatus("invocation-execution", "Completed", acceptedAt.AddSeconds(5)),
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
            await store.RecordRejectedWakeAsync(
                rejectedWake,
                "payload.schema_invalid",
                TestContext.Current.CancellationToken);
            await store.RecordRejectedWakeAsync(
                rejectedWake,
                "payload.schema_invalid",
                TestContext.Current.CancellationToken);

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

            var store = new SonnetBridgeStore(new TestContextFactory(options));
            var nowUtc = DateTimeOffset.UtcNow;
            var firstClaim = await store.LoadDispatchableAsync(nowUtc, 1, TestContext.Current.CancellationToken);
            var secondClaim = await store.LoadDispatchableAsync(nowUtc, 1, TestContext.Current.CancellationToken);
            var emptyClaim = await store.LoadDispatchableAsync(nowUtc, 1, TestContext.Current.CancellationToken);

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

            var store = new SonnetBridgeStore(new TestContextFactory(options));
            var replacement = ToDomain(superseded) with
            {
                SnapshotFingerprint = "replacement-fingerprint",
                Status = WakeOutboxStatus.Pending
            };
            var persisted = await store.CreateOrUpdatePendingWakeAsync(
                replacement,
                TestContext.Current.CancellationToken);

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

            var store = new SonnetBridgeStore(new TestContextFactory(options));
            await store.CreateOrUpdatePendingWakeAsync(ToDomain(pendingWake), TestContext.Current.CancellationToken);
            var checkpoint = new CoreSnapshotCheckpoint(
                pendingWake.BindingId,
                pendingWake.SessionId,
                pendingWake.SnapshotFingerprint,
                DateTimeOffset.UtcNow);
            var acceptance = new WakeAcceptance("invocation-repeat", checkpoint.AcceptedAtUtc);
            await store.MarkAcceptedWithCheckpointAsync(pendingWake.Id, acceptance, checkpoint, TestContext.Current.CancellationToken);

            await using (var completedContext = new BridgeDbContext(options))
            {
                var persisted = await completedContext.WakeOutbox.SingleAsync(
                    item => item.Id == pendingWake.Id,
                    TestContext.Current.CancellationToken);
                persisted.Status = WakeOutboxStatus.Completed;
                await completedContext.SaveChangesAsync(TestContext.Current.CancellationToken);
            }

            await store.MarkAcceptedWithCheckpointAsync(pendingWake.Id, acceptance, checkpoint, TestContext.Current.CancellationToken);

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
        item.TraceId);

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

    private sealed class FailOnSecondSaveChangesInterceptor : SaveChangesInterceptor
    {
        private int _saveCount;

        public override ValueTask<InterceptionResult<int>> SavingChangesAsync(
            DbContextEventData eventData,
            InterceptionResult<int> result,
            CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref _saveCount) == 2)
            {
                throw new InjectedStoreFailure();
            }

            return ValueTask.FromResult(result);
        }
    }

    private sealed class InjectedStoreFailure : Exception;
}