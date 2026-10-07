using System.Collections.Concurrent;
using System.Data;
using System.Text.Json.Nodes;
using DrasiWake.Core.Domain;
using DrasiWake.Persistence.SonnetDB.Entities;
using DrasiWake.Persistence.SonnetDB.Replication;
using Microsoft.EntityFrameworkCore;

namespace DrasiWake.Persistence.SonnetDB;

public sealed class SonnetBridgeStore(IDbContextFactory<BridgeDbContext> contextFactory) : IRaftBridgeProjection
{
    private static readonly ConcurrentDictionary<string, SemaphoreSlim> DatabaseLocks =
        new(StringComparer.OrdinalIgnoreCase);

    private static async ValueTask<IDisposable> AcquireDatabaseLockAsync(
        BridgeDbContext context,
        CancellationToken cancellationToken)
    {
        var dataSource = context.Database.GetDbConnection().DataSource;
        if (string.IsNullOrWhiteSpace(dataSource))
            throw new InvalidOperationException("SonnetDB context has no database data source.");

        var databasePath = Path.GetFullPath(dataSource);
        var gate = DatabaseLocks.GetOrAdd(databasePath, static _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(cancellationToken);
        return new DatabaseLockLease(gate);
    }

    private sealed class DatabaseLockLease(SemaphoreSlim gate) : IDisposable
    {
        public void Dispose() => gate.Release();
    }

    public async ValueTask<long> GetLastAppliedIndexAsync(CancellationToken cancellationToken)
    {
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        using var databaseLock = await AcquireDatabaseLockAsync(context, cancellationToken);
        var state = await context.RaftProjectionStates.AsNoTracking()
            .SingleOrDefaultAsync(item => item.Id == RaftProjectionState.SingletonId, cancellationToken);
        return state?.LastAppliedIndex ?? 0;
    }

    public async ValueTask AdvanceAppliedIndexAsync(long raftIndex, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(raftIndex, 1);

        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        using var databaseLock = await AcquireDatabaseLockAsync(context, cancellationToken);
        await using var transaction = await context.Database.BeginTransactionAsync(cancellationToken);
        var state = await context.RaftProjectionStates.SingleOrDefaultAsync(
            item => item.Id == RaftProjectionState.SingletonId,
            cancellationToken);
        if (state is null)
        {
            state = new RaftProjectionState();
            context.RaftProjectionStates.Add(state);
        }

        if (raftIndex < state.LastAppliedIndex)
        {
            await transaction.CommitAsync(cancellationToken);
            return;
        }

        if (raftIndex == state.LastAppliedIndex)
        {
            if (state.LastAppliedCommandId is not null)
                throw new InvalidOperationException($"Raft index '{raftIndex}' was already applied with a command.");
            await transaction.CommitAsync(cancellationToken);
            return;
        }

        state.LastAppliedIndex = raftIndex;
        state.LastAppliedCommandId = null;
        await context.SaveChangesAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    public async ValueTask ApplyReplicatedCommandAsync(
        ReplicatedBridgeCommand command,
        long raftIndex,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(command);
        command.Validate();
        ArgumentOutOfRangeException.ThrowIfLessThan(raftIndex, 1);

        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        using var databaseLock = await AcquireDatabaseLockAsync(context, cancellationToken);
        await using var transaction = await context.Database.BeginTransactionAsync(cancellationToken);

        var state = await context.RaftProjectionStates.SingleOrDefaultAsync(
            item => item.Id == RaftProjectionState.SingletonId,
            cancellationToken);
        if (state is null)
        {
            state = new RaftProjectionState();
            context.RaftProjectionStates.Add(state);
        }

        if (raftIndex < state.LastAppliedIndex)
        {
            await transaction.CommitAsync(cancellationToken);
            return;
        }

        if (raftIndex == state.LastAppliedIndex)
        {
            if (!string.Equals(command.CommandId, state.LastAppliedCommandId, StringComparison.Ordinal))
                throw new InvalidOperationException($"Raft index '{raftIndex}' was already applied with a different command ID.");

            await transaction.CommitAsync(cancellationToken);
            return;
        }

        await ApplyCommandAsync(context, state, command, cancellationToken);
        state.LastAppliedIndex = raftIndex;
        state.LastAppliedCommandId = command.CommandId;
        await context.SaveChangesAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    public async ValueTask<BridgeStoreSnapshot> ExportSnapshotAsync(CancellationToken cancellationToken)
    {
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        using var databaseLock = await AcquireDatabaseLockAsync(context, cancellationToken);
        await using var transaction = await context.Database.BeginTransactionAsync(
            IsolationLevel.Serializable,
            cancellationToken);
        var state = await context.RaftProjectionStates.AsNoTracking()
            .SingleOrDefaultAsync(item => item.Id == RaftProjectionState.SingletonId, cancellationToken);
        var subscriptions = await context.Subscriptions.AsNoTracking()
            .OrderBy(item => item.QueryKey)
            .Select(item => new SubscriptionSnapshot(
                item.QueryKey, item.ServerUri, item.InstanceId, item.QueryId, item.IsConnected,
                item.LastConnectedAtUtc, item.LastReconciledAtUtc, item.LastErrorCode))
            .ToListAsync(cancellationToken);
        var checkpoints = await context.SnapshotCheckpoints.AsNoTracking()
            .OrderBy(item => item.BindingId)
            .ThenBy(item => item.SessionId)
            .Select(item => new SnapshotCheckpointSnapshot(
                item.BindingId, item.SessionId, item.Fingerprint, item.AcceptedAtUtc))
            .ToListAsync(cancellationToken);
        var mappings = await context.KeyMappings.AsNoTracking()
            .OrderBy(item => item.ContractScope)
            .ThenBy(item => item.CanonicalIdentity)
            .Select(item => new KeyMappingSnapshot(
                item.ContractScope, item.CanonicalIdentity, item.SessionId, item.State, item.UpdatedAtUtc))
            .ToListAsync(cancellationToken);
        var outbox = await context.WakeOutbox.AsNoTracking()
            .OrderBy(item => item.Id)
            .Select(item => new WakeOutboxSnapshot(
                item.Id, item.BindingId, item.SessionId, item.SnapshotFingerprint, item.Skill,
                item.OpenClawTarget, item.InputJson, item.ContractVersion, item.IdempotencyKey,
                item.AttemptCount, item.CreatedAtUtc, item.NextAttemptAtUtc, item.Status,
                item.InvocationId, item.TraceId, item.LastErrorCode, item.RetainUntilUtc, item.Version,
                item.ClaimCommandId))
            .ToListAsync(cancellationToken);

        await transaction.CommitAsync(cancellationToken);
        return new BridgeStoreSnapshot(
            BridgeStoreSnapshot.CurrentSchemaVersion,
            state?.LastAppliedIndex ?? 0,
            state?.LastAppliedCommandId,
            state?.ConfigurationFingerprint,
            subscriptions,
            checkpoints,
            mappings,
            outbox);
    }

    public async ValueTask RestoreSnapshotAsync(BridgeStoreSnapshot snapshot, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        ValidateSnapshot(snapshot);

        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        using var databaseLock = await AcquireDatabaseLockAsync(context, cancellationToken);
        await using var transaction = await context.Database.BeginTransactionAsync(cancellationToken);

        context.Subscriptions.RemoveRange(await context.Subscriptions.ToListAsync(cancellationToken));
        context.SnapshotCheckpoints.RemoveRange(await context.SnapshotCheckpoints.ToListAsync(cancellationToken));
        context.KeyMappings.RemoveRange(await context.KeyMappings.ToListAsync(cancellationToken));
        context.WakeOutbox.RemoveRange(await context.WakeOutbox.ToListAsync(cancellationToken));
        await context.SaveChangesAsync(cancellationToken);

        context.Subscriptions.AddRange(snapshot.Subscriptions.Select(item => new SubscriptionState
        {
            QueryKey = item.QueryKey,
            ServerUri = item.ServerUri,
            InstanceId = item.InstanceId,
            QueryId = item.QueryId,
            IsConnected = item.IsConnected,
            LastConnectedAtUtc = item.LastConnectedAtUtc,
            LastReconciledAtUtc = item.LastReconciledAtUtc,
            LastErrorCode = item.LastErrorCode
        }));
        context.SnapshotCheckpoints.AddRange(snapshot.SnapshotCheckpoints.Select(item => new Entities.SnapshotCheckpoint
        {
            BindingId = item.BindingId,
            SessionId = item.SessionId,
            Fingerprint = item.Fingerprint,
            AcceptedAtUtc = item.AcceptedAtUtc
        }));
        context.KeyMappings.AddRange(snapshot.KeyMappings.Select(item => new KeyMapping
        {
            ContractScope = item.ContractScope,
            CanonicalIdentity = item.CanonicalIdentity,
            SessionId = item.SessionId,
            State = item.State,
            UpdatedAtUtc = item.UpdatedAtUtc
        }));
        context.WakeOutbox.AddRange(snapshot.WakeOutbox.Select(item => new WakeOutbox
        {
            Id = item.Id,
            BindingId = item.BindingId,
            SessionId = item.SessionId,
            SnapshotFingerprint = item.SnapshotFingerprint,
            Skill = item.Skill,
            OpenClawTarget = item.OpenClawTarget,
            InputJson = item.InputJson,
            ContractVersion = item.ContractVersion,
            IdempotencyKey = item.IdempotencyKey,
            AttemptCount = item.AttemptCount,
            CreatedAtUtc = item.CreatedAtUtc,
            NextAttemptAtUtc = item.NextAttemptAtUtc,
            Status = item.Status,
            InvocationId = item.InvocationId,
            TraceId = item.TraceId,
            LastErrorCode = item.LastErrorCode,
            RetainUntilUtc = item.RetainUntilUtc,
            Version = item.Version,
            ClaimCommandId = item.ClaimCommandId
        }));

        var state = await context.RaftProjectionStates.SingleOrDefaultAsync(
            item => item.Id == RaftProjectionState.SingletonId,
            cancellationToken);
        state ??= new RaftProjectionState();
        state.LastAppliedIndex = snapshot.LastAppliedIndex;
        state.LastAppliedCommandId = snapshot.LastAppliedCommandId;
        state.ConfigurationFingerprint = snapshot.ConfigurationFingerprint;
        if (context.Entry(state).State == EntityState.Detached)
            context.RaftProjectionStates.Add(state);

        await context.SaveChangesAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    public async ValueTask<IReadOnlyList<WakeOutboxItem>> LoadDispatchableCandidatesAsync(
        DateTimeOffset nowUtc,
        int limit,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        using var databaseLock = await AcquireDatabaseLockAsync(context, cancellationToken);
        var items = await context.WakeOutbox.AsNoTracking()
            .Where(item => (item.Status == WakeOutboxStatus.Pending || item.Status == WakeOutboxStatus.RetryScheduled) &&
                           item.NextAttemptAtUtc <= nowUtc)
            .OrderBy(item => item.NextAttemptAtUtc)
            .ThenBy(item => item.CreatedAtUtc)
            .ThenBy(item => item.Id)
            .Take(limit)
            .ToListAsync(cancellationToken);
        return items.Select(ToDomain).ToArray();
    }

    public async ValueTask<RecoveryState> ReadRecoveryStateAsync(CancellationToken cancellationToken)
    {
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        using var databaseLock = await AcquireDatabaseLockAsync(context, cancellationToken);
        var dispatchable = await context.WakeOutbox.AsNoTracking()
            .Where(item => item.Status == WakeOutboxStatus.Pending || item.Status == WakeOutboxStatus.RetryScheduled)
            .OrderBy(item => item.NextAttemptAtUtc)
            .ThenBy(item => item.CreatedAtUtc)
            .ThenBy(item => item.Id)
            .ToListAsync(cancellationToken);
        var checkpoints = await context.SnapshotCheckpoints.AsNoTracking()
            .OrderBy(item => item.BindingId)
            .ThenBy(item => item.SessionId)
            .Select(item => new Core.Domain.SnapshotCheckpoint(
                item.BindingId, item.SessionId, item.Fingerprint, item.AcceptedAtUtc))
            .ToListAsync(cancellationToken);
        return new RecoveryState(dispatchable.Select(ToDomain).ToArray(), checkpoints);
    }

    public async ValueTask<IReadOnlyList<WakeOutboxItem>> ReadClaimedDispatchableAsync(
        IReadOnlyList<Guid> outboxIds,
        string claimCommandId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(outboxIds);
        ArgumentException.ThrowIfNullOrWhiteSpace(claimCommandId);
        if (outboxIds.Count == 0)
            return [];

        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        using var databaseLock = await AcquireDatabaseLockAsync(context, cancellationToken);
        var claimed = await context.WakeOutbox.AsNoTracking()
            .Where(item => outboxIds.Contains(item.Id) &&
                           item.Status == WakeOutboxStatus.Dispatching &&
                           item.ClaimCommandId == claimCommandId)
            .ToDictionaryAsync(item => item.Id, cancellationToken);
        return outboxIds
            .Where(claimed.ContainsKey)
            .Select(id => ToDomain(claimed[id]))
            .ToArray();
    }

    public async ValueTask<WakeOutboxItem?> FindPendingWakeResultAsync(
        Guid requestedId,
        string bindingId,
        string sessionId,
        string snapshotFingerprint,
        CancellationToken cancellationToken)
    {
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        using var databaseLock = await AcquireDatabaseLockAsync(context, cancellationToken);
        var wake = await context.WakeOutbox.AsNoTracking()
            .SingleOrDefaultAsync(item => item.Id == requestedId, cancellationToken);
        wake ??= await context.WakeOutbox.AsNoTracking()
            .Where(item => item.BindingId == bindingId &&
                           item.SessionId == sessionId &&
                           item.SnapshotFingerprint == snapshotFingerprint &&
                           (item.Status == WakeOutboxStatus.Pending ||
                            item.Status == WakeOutboxStatus.RetryScheduled ||
                            item.Status == WakeOutboxStatus.Dispatching))
            .OrderBy(item => item.CreatedAtUtc)
            .ThenBy(item => item.Id)
            .FirstOrDefaultAsync(cancellationToken);
        return wake is null ? null : ToDomain(wake);
    }

    public async ValueTask<WakeOutboxItem?> ReadWakeAsync(Guid outboxId, CancellationToken cancellationToken)
    {
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        using var databaseLock = await AcquireDatabaseLockAsync(context, cancellationToken);
        var wake = await context.WakeOutbox.AsNoTracking()
            .SingleOrDefaultAsync(item => item.Id == outboxId, cancellationToken);
        return wake is null ? null : ToDomain(wake);
    }
    private static async ValueTask ApplyCommandAsync(
        BridgeDbContext context,
        RaftProjectionState state,
        ReplicatedBridgeCommand command,
        CancellationToken cancellationToken)
    {
        switch (command.Kind)
        {
            case BridgeCommandKind.CreateOrUpdatePendingWake:
                await ApplyCreateOrUpdatePendingWakeAsync(
                    context, command.DeserializePayload<CreateOrUpdatePendingWakePayload>().Item, cancellationToken);
                break;
            case BridgeCommandKind.RecordRejectedWake:
            {
                var payload = command.DeserializePayload<RecordRejectedWakePayload>();
                await ApplyRecordRejectedWakeAsync(context, payload.Item, payload.ReasonCode, cancellationToken);
                break;
            }
            case BridgeCommandKind.SupersedePendingWakes:
            {
                var payload = command.DeserializePayload<SupersedePendingWakesPayload>();
                await ApplySupersedePendingWakesAsync(
                    context, payload.BindingId, payload.SessionId, payload.CurrentFingerprint, cancellationToken);
                break;
            }
            case BridgeCommandKind.ClaimDispatchable:
                await ApplyClaimDispatchableAsync(
                    context, command.DeserializePayload<ClaimDispatchablePayload>(),
                    command.CommandId, cancellationToken);
                break;
            case BridgeCommandKind.MarkAcceptedWithCheckpoint:
                await ApplyMarkAcceptedAsync(
                    context, command.DeserializePayload<MarkAcceptedWithCheckpointPayload>(), cancellationToken);
                break;
            case BridgeCommandKind.UpdateExecutionStatus:
                await ApplyExecutionStatusAsync(
                    context, command.DeserializePayload<UpdateExecutionStatusPayload>().Status, cancellationToken);
                break;
            case BridgeCommandKind.MarkRetryScheduled:
                await ApplyRetryScheduledAsync(
                    context, command.DeserializePayload<MarkRetryScheduledPayload>(), cancellationToken);
                break;
            case BridgeCommandKind.MarkDeadLetter:
                await ApplyDeadLetterAsync(
                    context, command.DeserializePayload<MarkDeadLetterPayload>(), cancellationToken);
                break;
            case BridgeCommandKind.RecoverInterruptedDispatches:
                await ApplyInterruptedRecoveryAsync(
                    context, command.DeserializePayload<RecoverInterruptedDispatchesPayload>(), cancellationToken);
                break;
            case BridgeCommandKind.EnsureOpenClawTargets:
                await ApplyOpenClawTargetsAsync(
                    context, command.DeserializePayload<EnsureOpenClawTargetsPayload>(), cancellationToken);
                break;
            case BridgeCommandKind.SetConfigurationFingerprint:
            {
                var fingerprint = command.DeserializePayload<SetConfigurationFingerprintPayload>()
                    .ConfigurationFingerprint;
                ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);
                state.ConfigurationFingerprint = fingerprint;
                break;
            }
            case BridgeCommandKind.UpsertSubscription:
                await ApplyUpsertSubscriptionAsync(
                    context, command.DeserializePayload<UpsertSubscriptionPayload>().Subscription, cancellationToken);
                break;
            case BridgeCommandKind.RemoveSubscription:
            {
                var queryKey = command.DeserializePayload<RemoveSubscriptionPayload>().QueryKey;
                ArgumentException.ThrowIfNullOrWhiteSpace(queryKey);
                var subscription = await context.Subscriptions.FindAsync([queryKey], cancellationToken);
                if (subscription is not null)
                    context.Subscriptions.Remove(subscription);
                break;
            }
            case BridgeCommandKind.UpsertKeyMapping:
                await ApplyUpsertKeyMappingAsync(
                    context, command.DeserializePayload<UpsertKeyMappingPayload>().Mapping, cancellationToken);
                break;
            case BridgeCommandKind.RemoveKeyMapping:
            {
                var payload = command.DeserializePayload<RemoveKeyMappingPayload>();
                ArgumentException.ThrowIfNullOrWhiteSpace(payload.ContractScope);
                ArgumentException.ThrowIfNullOrWhiteSpace(payload.CanonicalIdentity);
                var mapping = await context.KeyMappings.FindAsync(
                    [payload.ContractScope, payload.CanonicalIdentity], cancellationToken);
                if (mapping is not null)
                    context.KeyMappings.Remove(mapping);
                break;
            }
            default:
                throw new InvalidOperationException($"Unsupported replicated bridge command kind '{command.Kind}'.");
        }
    }

    private static async ValueTask ApplyCreateOrUpdatePendingWakeAsync(
        BridgeDbContext context,
        WakeOutboxItem item,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(item);
        if (item.Status != WakeOutboxStatus.Pending)
            throw new ArgumentException("Only pending wake items can be created or updated.", nameof(item));

        var existing = await context.WakeOutbox.SingleOrDefaultAsync(
            wake => wake.Id == item.Id, cancellationToken);
        existing ??= await context.WakeOutbox
            .Where(wake => wake.BindingId == item.BindingId &&
                           wake.SessionId == item.SessionId &&
                           wake.SnapshotFingerprint == item.SnapshotFingerprint &&
                           (wake.Status == WakeOutboxStatus.Pending ||
                            wake.Status == WakeOutboxStatus.RetryScheduled ||
                            wake.Status == WakeOutboxStatus.Dispatching))
            .OrderBy(wake => wake.CreatedAtUtc)
            .ThenBy(wake => wake.Id)
            .FirstOrDefaultAsync(cancellationToken);
        if (existing is not null)
        {
            if (existing.Status is not (WakeOutboxStatus.Pending or WakeOutboxStatus.RetryScheduled))
                return;

            Copy(item, existing);
            existing.Version++;
            return;
        }

        context.WakeOutbox.Add(ToEntity(item));
    }

    private static async ValueTask ApplyRecordRejectedWakeAsync(
        BridgeDbContext context,
        WakeOutboxItem item,
        string reasonCode,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(item);
        ArgumentException.ThrowIfNullOrWhiteSpace(reasonCode);
        if (item.Status != WakeOutboxStatus.DeadLetter)
            throw new ArgumentException("Rejected wake records must be dead letters.", nameof(item));
        if (await context.WakeOutbox.AnyAsync(wake => wake.Id == item.Id, cancellationToken))
            return;

        var rejected = ToEntity(item);
        rejected.LastErrorCode = reasonCode;
        context.WakeOutbox.Add(rejected);
    }

    private static async ValueTask ApplySupersedePendingWakesAsync(
        BridgeDbContext context,
        string bindingId,
        string sessionId,
        string currentFingerprint,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(bindingId);
        ArgumentException.ThrowIfNullOrWhiteSpace(sessionId);
        ArgumentException.ThrowIfNullOrWhiteSpace(currentFingerprint);
        var pending = await context.WakeOutbox
            .Where(wake => wake.BindingId == bindingId &&
                           wake.SessionId == sessionId &&
                           wake.SnapshotFingerprint != currentFingerprint &&
                           (wake.Status == WakeOutboxStatus.Pending || wake.Status == WakeOutboxStatus.RetryScheduled))
            .ToListAsync(cancellationToken);
        foreach (var wake in pending)
        {
            wake.Status = WakeOutboxStatus.Superseded;
            wake.LastErrorCode = "snapshot.superseded";
            wake.Version++;
        }
    }

    private static async ValueTask ApplyClaimDispatchableAsync(
        BridgeDbContext context,
        ClaimDispatchablePayload payload,
        string claimCommandId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(payload.OutboxIds);
        if (payload.OutboxIds.Count == 0 ||
            payload.OutboxIds.Any(id => id == Guid.Empty) ||
            payload.OutboxIds.Distinct().Count() != payload.OutboxIds.Count ||
            payload.ClaimId == Guid.Empty)
            throw new InvalidOperationException("A dispatch claim must contain distinct, non-empty outbox IDs.");

        var items = await context.WakeOutbox
            .Where(item => payload.OutboxIds.Contains(item.Id))
            .ToListAsync(cancellationToken);
        foreach (var item in items)
        {
            if (item.Status is not (WakeOutboxStatus.Pending or WakeOutboxStatus.RetryScheduled) ||
                item.NextAttemptAtUtc > payload.ClaimedAtUtc)
                continue;
            item.Status = WakeOutboxStatus.Dispatching;
            item.ClaimCommandId = claimCommandId;
            item.Version++;
        }
    }

    private static async ValueTask ApplyMarkAcceptedAsync(
        BridgeDbContext context,
        MarkAcceptedWithCheckpointPayload payload,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(payload.Acceptance);
        ArgumentNullException.ThrowIfNull(payload.Checkpoint);
        ArgumentException.ThrowIfNullOrWhiteSpace(payload.Acceptance.InvocationId);

        var wake = await context.WakeOutbox.SingleAsync(item => item.Id == payload.OutboxId, cancellationToken);
        var checkpoint = payload.Checkpoint;
        if (!string.Equals(wake.BindingId, checkpoint.BindingId, StringComparison.Ordinal) ||
            !string.Equals(wake.SessionId, checkpoint.SessionId, StringComparison.Ordinal) ||
            !string.Equals(wake.SnapshotFingerprint, checkpoint.Fingerprint, StringComparison.Ordinal))
            throw new InvalidOperationException("Accepted checkpoint must match the outbox binding, session, and fingerprint.");

        if (wake.Status is WakeOutboxStatus.Accepted or WakeOutboxStatus.Executing or WakeOutboxStatus.Completed)
        {
            if (!string.Equals(wake.InvocationId, payload.Acceptance.InvocationId, StringComparison.Ordinal))
                throw new InvalidOperationException("Repeated acceptance must refer to the original Gateway invocation.");
            var acceptedCheckpoint = await context.SnapshotCheckpoints.FindAsync(
                [checkpoint.BindingId, checkpoint.SessionId], cancellationToken);
            if (acceptedCheckpoint is null ||
                !string.Equals(acceptedCheckpoint.Fingerprint, checkpoint.Fingerprint, StringComparison.Ordinal))
                throw new InvalidOperationException("Accepted outbox item must have its matching snapshot checkpoint.");
            return;
        }

        if (wake.Status is not (WakeOutboxStatus.Pending or WakeOutboxStatus.Dispatching or WakeOutboxStatus.RetryScheduled))
            throw new InvalidOperationException($"Outbox item in state '{wake.Status}' cannot be accepted.");

        wake.Status = WakeOutboxStatus.Accepted;
        wake.InvocationId = payload.Acceptance.InvocationId;
        wake.ClaimCommandId = null;
        wake.Version++;

        var persistedCheckpoint = await context.SnapshotCheckpoints.FindAsync(
            [checkpoint.BindingId, checkpoint.SessionId], cancellationToken);
        if (persistedCheckpoint is null)
        {
            context.SnapshotCheckpoints.Add(new Entities.SnapshotCheckpoint
            {
                BindingId = checkpoint.BindingId,
                SessionId = checkpoint.SessionId,
                Fingerprint = checkpoint.Fingerprint,
                AcceptedAtUtc = checkpoint.AcceptedAtUtc
            });
        }
        else
        {
            persistedCheckpoint.Fingerprint = checkpoint.Fingerprint;
            persistedCheckpoint.AcceptedAtUtc = checkpoint.AcceptedAtUtc;
        }
    }

    private static async ValueTask ApplyExecutionStatusAsync(
        BridgeDbContext context,
        WakeExecutionStatus status,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(status);
        ArgumentException.ThrowIfNullOrWhiteSpace(status.InvocationId);
        var targetStatus = status.State switch
        {
            "Running" => WakeOutboxStatus.Executing,
            "Completed" => WakeOutboxStatus.Completed,
            "Uncertain" => WakeOutboxStatus.DeadLetter,
            _ => throw new ArgumentException($"Unsupported Gateway invocation state '{status.State}'.", nameof(status))
        };
        var wake = await context.WakeOutbox.SingleAsync(
            item => item.InvocationId == status.InvocationId, cancellationToken);
        if (wake.Status == WakeOutboxStatus.Completed)
            return;
        if (wake.Status is not (WakeOutboxStatus.Accepted or WakeOutboxStatus.Executing))
            throw new InvalidOperationException($"Outbox item in state '{wake.Status}' cannot receive execution status.");
        wake.Status = targetStatus;
        if (targetStatus == WakeOutboxStatus.DeadLetter)
            wake.LastErrorCode = "gateway.invocation_uncertain";
        wake.Version++;
    }

    private static async ValueTask ApplyRetryScheduledAsync(
        BridgeDbContext context,
        MarkRetryScheduledPayload payload,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(payload.AttemptCount);
        ArgumentException.ThrowIfNullOrWhiteSpace(payload.ReasonCode);
        var wake = await context.WakeOutbox.SingleAsync(item => item.Id == payload.OutboxId, cancellationToken);
        wake.AttemptCount = payload.AttemptCount;
        wake.NextAttemptAtUtc = payload.NextAttemptUtc;
        wake.LastErrorCode = payload.ReasonCode;
        wake.Status = WakeOutboxStatus.RetryScheduled;
        wake.ClaimCommandId = null;
        wake.Version++;
    }

    private static async ValueTask ApplyDeadLetterAsync(
        BridgeDbContext context,
        MarkDeadLetterPayload payload,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(payload.ReasonCode);
        var wake = await context.WakeOutbox.SingleAsync(item => item.Id == payload.OutboxId, cancellationToken);
        wake.LastErrorCode = payload.ReasonCode;
        wake.Status = WakeOutboxStatus.DeadLetter;
        wake.ClaimCommandId = null;
        wake.Version++;
    }

    private static async ValueTask ApplyInterruptedRecoveryAsync(
        BridgeDbContext context,
        RecoverInterruptedDispatchesPayload payload,
        CancellationToken cancellationToken)
    {
        if (payload.RecoveredAtUtc == default)
            throw new ArgumentException("Recovery timestamp is required.", nameof(payload));
        var interrupted = await context.WakeOutbox
            .Where(item => item.Status == WakeOutboxStatus.Dispatching)
            .ToListAsync(cancellationToken);
        foreach (var item in interrupted)
        {
            item.Status = WakeOutboxStatus.RetryScheduled;
            item.ClaimCommandId = null;
            item.NextAttemptAtUtc = payload.RecoveredAtUtc;
            item.LastErrorCode = "dispatch.recovered";
            item.Version++;
        }
    }

    private static async ValueTask ApplyOpenClawTargetsAsync(
        BridgeDbContext context,
        EnsureOpenClawTargetsPayload payload,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(payload.TargetByBindingId);
        ArgumentNullException.ThrowIfNull(payload.ConfiguredTargetNames);
        ArgumentNullException.ThrowIfNull(payload.MaximumRetryAgeByBindingId);
        ArgumentNullException.ThrowIfNull(payload.IdempotencyRetentionByTarget);
        var activeItems = await context.WakeOutbox
            .Where(item =>
                item.Status == WakeOutboxStatus.Pending ||
                item.Status == WakeOutboxStatus.Dispatching ||
                item.Status == WakeOutboxStatus.RetryScheduled ||
                item.Status == WakeOutboxStatus.Accepted ||
                item.Status == WakeOutboxStatus.Executing)
            .OrderBy(item => item.Id)
            .ToListAsync(cancellationToken);

        foreach (var item in activeItems)
        {
            var target = item.OpenClawTarget;
            if (target is null)
            {
                if (!payload.TargetByBindingId.TryGetValue(item.BindingId, out var bindingTarget) ||
                    string.IsNullOrWhiteSpace(bindingTarget))
                    throw new InvalidOperationException(
                        $"Active wake outbox item '{item.Id}' for binding '{item.BindingId}' has no configured binding target.");
                target = bindingTarget;
                item.OpenClawTarget = target;
                item.Version++;
            }
            else if (string.IsNullOrWhiteSpace(target))
            {
                throw new InvalidOperationException(
                    $"Active wake outbox item '{item.Id}' for binding '{item.BindingId}' has an empty or whitespace OpenClaw target.");
            }

            if (!payload.ConfiguredTargetNames.Contains(target))
                throw new InvalidOperationException(
                    $"Active wake outbox item '{item.Id}' for binding '{item.BindingId}' references unconfigured OpenClaw target '{target}'.");
            if (!payload.IdempotencyRetentionByTarget.TryGetValue(target, out var idempotencyRetention))
                throw new InvalidOperationException(
                    $"Active wake outbox item '{item.Id}' references OpenClaw target '{target}' without an idempotency retention policy.");
            if (payload.TargetByBindingId.ContainsKey(item.BindingId))
            {
                if (!payload.MaximumRetryAgeByBindingId.TryGetValue(item.BindingId, out var maximumRetryAge))
                    throw new InvalidOperationException(
                        $"Active wake outbox item '{item.Id}' for current binding '{item.BindingId}' has no retry-age policy.");
                if (idempotencyRetention < maximumRetryAge)
                    throw new InvalidOperationException(
                        $"Active wake outbox item '{item.Id}' for binding '{item.BindingId}' references OpenClaw target '{target}' whose idempotency retention does not cover the binding's maximum outbox retry age.");
            }
        }
    }

    private static async ValueTask ApplyUpsertSubscriptionAsync(
        BridgeDbContext context,
        SubscriptionSnapshot item,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(item);
        ValidateRequired(item.QueryKey, nameof(item.QueryKey));
        ValidateRequired(item.ServerUri, nameof(item.ServerUri));
        ValidateRequired(item.QueryId, nameof(item.QueryId));
        var entity = await context.Subscriptions.FindAsync([item.QueryKey], cancellationToken);
        if (entity is null)
        {
            entity = new SubscriptionState { QueryKey = item.QueryKey };
            context.Subscriptions.Add(entity);
        }
        entity.ServerUri = item.ServerUri;
        entity.InstanceId = item.InstanceId;
        entity.QueryId = item.QueryId;
        entity.IsConnected = item.IsConnected;
        entity.LastConnectedAtUtc = item.LastConnectedAtUtc;
        entity.LastReconciledAtUtc = item.LastReconciledAtUtc;
        entity.LastErrorCode = item.LastErrorCode;
    }

    private static async ValueTask ApplyUpsertKeyMappingAsync(
        BridgeDbContext context,
        KeyMappingSnapshot item,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(item);
        ValidateRequired(item.ContractScope, nameof(item.ContractScope));
        ValidateRequired(item.CanonicalIdentity, nameof(item.CanonicalIdentity));
        ValidateRequired(item.SessionId, nameof(item.SessionId));
        ValidateRequired(item.State, nameof(item.State));
        var entity = await context.KeyMappings.FindAsync(
            [item.ContractScope, item.CanonicalIdentity], cancellationToken);
        if (entity is null)
        {
            entity = new KeyMapping
            {
                ContractScope = item.ContractScope,
                CanonicalIdentity = item.CanonicalIdentity
            };
            context.KeyMappings.Add(entity);
        }
        entity.SessionId = item.SessionId;
        entity.State = item.State;
        entity.UpdatedAtUtc = item.UpdatedAtUtc;
    }

    private static void ValidateSnapshot(BridgeStoreSnapshot snapshot)
    {
        if (snapshot.SchemaVersion != BridgeStoreSnapshot.CurrentSchemaVersion)
            throw new InvalidOperationException($"Unsupported bridge store snapshot schema version '{snapshot.SchemaVersion}'.");
        if (snapshot.LastAppliedIndex < 0)
            throw new InvalidOperationException("Snapshot applied index cannot be negative.");
        if (snapshot.LastAppliedIndex == 0 && !string.IsNullOrEmpty(snapshot.LastAppliedCommandId))
            throw new InvalidOperationException("An unapplied snapshot cannot contain a last command ID.");
        if (snapshot.ConfigurationFingerprint is not null && string.IsNullOrWhiteSpace(snapshot.ConfigurationFingerprint))
            throw new InvalidOperationException("Snapshot configuration fingerprint cannot be empty.");
        ArgumentNullException.ThrowIfNull(snapshot.Subscriptions);
        ArgumentNullException.ThrowIfNull(snapshot.SnapshotCheckpoints);
        ArgumentNullException.ThrowIfNull(snapshot.KeyMappings);
        ArgumentNullException.ThrowIfNull(snapshot.WakeOutbox);

        EnsureUnique(snapshot.Subscriptions, item => item.QueryKey, "subscription query key");
        EnsureUnique(
            snapshot.Subscriptions,
            item => $"{item.ServerUri}\0{item.InstanceId}\0{item.QueryId}",
            "subscription identity");
        EnsureUnique(
            snapshot.SnapshotCheckpoints,
            item => $"{item.BindingId}\0{item.SessionId}",
            "snapshot checkpoint key");
        EnsureUnique(
            snapshot.KeyMappings,
            item => $"{item.ContractScope}\0{item.CanonicalIdentity}",
            "key mapping key");
        EnsureUnique(snapshot.WakeOutbox, item => item.Id, "outbox ID");
        EnsureUnique(snapshot.WakeOutbox, item => item.IdempotencyKey, "outbox idempotency key");

        foreach (var item in snapshot.Subscriptions)
        {
            ArgumentNullException.ThrowIfNull(item);
            ValidateRequired(item.QueryKey, nameof(item.QueryKey));
            ValidateRequired(item.ServerUri, nameof(item.ServerUri));
            ValidateRequired(item.QueryId, nameof(item.QueryId));
        }
        foreach (var item in snapshot.SnapshotCheckpoints)
        {
            ArgumentNullException.ThrowIfNull(item);
            ValidateRequired(item.BindingId, nameof(item.BindingId));
            ValidateRequired(item.SessionId, nameof(item.SessionId));
            ValidateRequired(item.Fingerprint, nameof(item.Fingerprint));
            if (item.AcceptedAtUtc == default)
                throw new InvalidOperationException("Snapshot checkpoint acceptance timestamp is required.");
        }
        foreach (var item in snapshot.KeyMappings)
        {
            ArgumentNullException.ThrowIfNull(item);
            ValidateRequired(item.ContractScope, nameof(item.ContractScope));
            ValidateRequired(item.CanonicalIdentity, nameof(item.CanonicalIdentity));
            ValidateRequired(item.SessionId, nameof(item.SessionId));
            ValidateRequired(item.State, nameof(item.State));
            if (item.UpdatedAtUtc == default)
                throw new InvalidOperationException("Key mapping update timestamp is required.");
        }
        foreach (var item in snapshot.WakeOutbox)
        {
            ArgumentNullException.ThrowIfNull(item);
            if (item.Id == Guid.Empty)
                throw new InvalidOperationException("Snapshot outbox ID is required.");
            ValidateRequired(item.BindingId, nameof(item.BindingId));
            ValidateRequired(item.SessionId, nameof(item.SessionId));
            ValidateRequired(item.SnapshotFingerprint, nameof(item.SnapshotFingerprint));
            ValidateRequired(item.Skill, nameof(item.Skill));
            ValidateRequired(item.InputJson, nameof(item.InputJson));
            ValidateRequired(item.ContractVersion, nameof(item.ContractVersion));
            ValidateRequired(item.IdempotencyKey, nameof(item.IdempotencyKey));
            if (item.AttemptCount < 0 || item.Version < 0 ||
                item.CreatedAtUtc == default || item.NextAttemptAtUtc == default ||
                !Enum.IsDefined(item.Status))
                throw new InvalidOperationException($"Snapshot outbox item '{item.Id}' has invalid required fields.");
            try
            {
                if (JsonNode.Parse(item.InputJson) is not JsonObject)
                    throw new InvalidOperationException($"Snapshot outbox item '{item.Id}' input JSON must be an object.");
            }
            catch (System.Text.Json.JsonException exception)
            {
                throw new InvalidOperationException($"Snapshot outbox item '{item.Id}' input JSON is invalid.", exception);
            }
        }
    }

    private static void EnsureUnique<T, TKey>(IEnumerable<T> items, Func<T, TKey> keySelector, string name)
        where TKey : notnull
    {
        if (items.GroupBy(keySelector).Any(group => group.Count() > 1))
            throw new InvalidOperationException($"Snapshot contains duplicate {name} values.");
    }

    private static void ValidateRequired(string? value, string name)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException($"Snapshot field '{name}' is required.");
    }

    private static WakeOutbox ToEntity(WakeOutboxItem item) => new()
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
    };

    private static WakeOutboxItem ToDomain(WakeOutbox item) => new(
        item.Id,
        item.BindingId,
        item.SessionId,
        item.SnapshotFingerprint,
        item.Skill,
        JsonNode.Parse(item.InputJson)?.AsObject() ?? new JsonObject(),
        item.ContractVersion,
        item.IdempotencyKey,
        item.AttemptCount,
        item.CreatedAtUtc,
        item.NextAttemptAtUtc,
        item.Status,
        item.InvocationId,
        item.TraceId,
        item.OpenClawTarget ?? throw new InvalidOperationException(
            $"Wake outbox item '{item.Id}' has no OpenClaw target."));

    private static void Copy(WakeOutboxItem source, WakeOutbox target)
    {
        target.BindingId = source.BindingId;
        target.SessionId = source.SessionId;
        target.SnapshotFingerprint = source.SnapshotFingerprint;
        target.Skill = source.Skill;
        target.OpenClawTarget = source.OpenClawTarget;
        target.InputJson = source.Input.ToJsonString();
        target.ContractVersion = source.ContractVersion;
        target.IdempotencyKey = source.IdempotencyKey;
        target.AttemptCount = source.AttemptCount;
        target.CreatedAtUtc = source.CreatedAtUtc;
        target.NextAttemptAtUtc = source.NextAttemptAtUtc;
        target.Status = source.Status;
        target.InvocationId = source.InvocationId;
        target.TraceId = source.TraceId;
    }
}