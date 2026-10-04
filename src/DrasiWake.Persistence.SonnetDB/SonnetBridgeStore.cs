using System.Text.Json.Nodes;
using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Domain;
using DrasiWake.Persistence.SonnetDB.Entities;
using Microsoft.EntityFrameworkCore;

namespace DrasiWake.Persistence.SonnetDB;

public sealed class SonnetBridgeStore(IDbContextFactory<BridgeDbContext> contextFactory) : IBridgeStore
{
    public async ValueTask<WakeOutboxItem> CreateOrUpdatePendingWakeAsync(
        WakeOutboxItem item,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(item);
        if (item.Status != WakeOutboxStatus.Pending)
        {
            throw new ArgumentException("Only pending wake items can be created or updated.", nameof(item));
        }

        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        var existing = await context.WakeOutbox.SingleOrDefaultAsync(
            wake => wake.Id == item.Id,
            cancellationToken);
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
            {
                return ToDomain(existing);
            }

            Copy(item, existing);
            existing.Version++;
        }
        else
        {
            existing = ToEntity(item);
            context.WakeOutbox.Add(existing);
        }

        await context.SaveChangesAsync(cancellationToken);
        return ToDomain(existing);
    }

    public async ValueTask RecordRejectedWakeAsync(
        WakeOutboxItem item,
        string reasonCode,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(item);
        ArgumentException.ThrowIfNullOrWhiteSpace(reasonCode);
        if (item.Status != WakeOutboxStatus.DeadLetter)
            throw new ArgumentException("Rejected wake records must be dead letters.", nameof(item));

        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        var exists = await context.WakeOutbox.AnyAsync(wake => wake.Id == item.Id, cancellationToken);
        if (exists)
            return;

        var rejected = ToEntity(item);
        rejected.LastErrorCode = reasonCode;
        context.WakeOutbox.Add(rejected);
        await context.SaveChangesAsync(cancellationToken);
    }

    public async ValueTask SupersedePendingWakesAsync(
        string bindingId,
        string sessionId,
        string currentFingerprint,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(bindingId);
        ArgumentException.ThrowIfNullOrWhiteSpace(sessionId);
        ArgumentException.ThrowIfNullOrWhiteSpace(currentFingerprint);

        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
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

        if (pending.Count > 0)
            await context.SaveChangesAsync(cancellationToken);
    }

    public async ValueTask<IReadOnlyList<WakeOutboxItem>> LoadDispatchableAsync(
        DateTimeOffset nowUtc,
        int limit,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        var items = await context.WakeOutbox
            .Where(item => (item.Status == WakeOutboxStatus.Pending || item.Status == WakeOutboxStatus.RetryScheduled) &&
                           item.NextAttemptAtUtc <= nowUtc)
            .OrderBy(item => item.NextAttemptAtUtc)
            .ThenBy(item => item.CreatedAtUtc)
            .ThenBy(item => item.Id)
            .Take(limit)
            .ToListAsync(cancellationToken);

        foreach (var item in items)
        {
            item.Status = WakeOutboxStatus.Dispatching;
            item.Version++;
        }

        await context.SaveChangesAsync(cancellationToken);
        return items.Select(ToDomain).ToArray();
    }

    public async ValueTask MarkAcceptedWithCheckpointAsync(
        Guid outboxId,
        WakeAcceptance acceptance,
        Core.Domain.SnapshotCheckpoint checkpoint,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(acceptance);
        ArgumentNullException.ThrowIfNull(checkpoint);
        ArgumentException.ThrowIfNullOrWhiteSpace(acceptance.InvocationId);
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        await using var transaction = await context.Database.BeginTransactionAsync(cancellationToken);

        var wake = await context.WakeOutbox.SingleAsync(item => item.Id == outboxId, cancellationToken);
        if (!string.Equals(wake.BindingId, checkpoint.BindingId, StringComparison.Ordinal) ||
            !string.Equals(wake.SessionId, checkpoint.SessionId, StringComparison.Ordinal) ||
            !string.Equals(wake.SnapshotFingerprint, checkpoint.Fingerprint, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("Accepted checkpoint must match the outbox binding, session, and fingerprint.");
        }

        if (wake.Status is WakeOutboxStatus.Accepted or WakeOutboxStatus.Executing or WakeOutboxStatus.Completed)
        {
            if (!string.Equals(wake.InvocationId, acceptance.InvocationId, StringComparison.Ordinal))
                throw new InvalidOperationException("Repeated acceptance must refer to the original Gateway invocation.");
            var acceptedCheckpoint = await context.SnapshotCheckpoints.FindAsync(
                [checkpoint.BindingId, checkpoint.SessionId],
                cancellationToken);
            if (acceptedCheckpoint is null ||
                !string.Equals(acceptedCheckpoint.Fingerprint, checkpoint.Fingerprint, StringComparison.Ordinal))
            {
                throw new InvalidOperationException("Accepted outbox item must have its matching snapshot checkpoint.");
            }

            await transaction.CommitAsync(cancellationToken);
            return;
        }

        if (wake.Status is not (WakeOutboxStatus.Pending or WakeOutboxStatus.Dispatching or WakeOutboxStatus.RetryScheduled))
        {
            throw new InvalidOperationException($"Outbox item in state '{wake.Status}' cannot be accepted.");
        }

        wake.Status = WakeOutboxStatus.Accepted;
        wake.InvocationId = acceptance.InvocationId;
        wake.Version++;
        await context.SaveChangesAsync(cancellationToken);

        var persistedCheckpoint = await context.SnapshotCheckpoints.FindAsync(
            [checkpoint.BindingId, checkpoint.SessionId],
            cancellationToken);
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

        await context.SaveChangesAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    public async ValueTask UpdateExecutionStatusAsync(
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

        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        var wake = await context.WakeOutbox.SingleAsync(
            item => item.InvocationId == status.InvocationId,
            cancellationToken);
        if (wake.Status == WakeOutboxStatus.Completed)
            return;
        if (wake.Status is not (WakeOutboxStatus.Accepted or WakeOutboxStatus.Executing))
            throw new InvalidOperationException($"Outbox item in state '{wake.Status}' cannot receive execution status.");

        wake.Status = targetStatus;
        if (targetStatus == WakeOutboxStatus.DeadLetter)
            wake.LastErrorCode = "gateway.invocation_uncertain";
        wake.Version++;
        await context.SaveChangesAsync(cancellationToken);
    }

    public async ValueTask MarkRetryScheduledAsync(
        Guid outboxId,
        int attemptCount,
        DateTimeOffset nextAttemptUtc,
        string reasonCode,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(attemptCount);
        ArgumentException.ThrowIfNullOrWhiteSpace(reasonCode);
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        var wake = await context.WakeOutbox.SingleAsync(item => item.Id == outboxId, cancellationToken);
        wake.AttemptCount = attemptCount;
        wake.NextAttemptAtUtc = nextAttemptUtc;
        wake.LastErrorCode = reasonCode;
        wake.Status = WakeOutboxStatus.RetryScheduled;
        wake.Version++;
        await context.SaveChangesAsync(cancellationToken);
    }

    public async ValueTask MarkDeadLetterAsync(
        Guid outboxId,
        string reasonCode,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reasonCode);
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        var wake = await context.WakeOutbox.SingleAsync(item => item.Id == outboxId, cancellationToken);
        wake.LastErrorCode = reasonCode;
        wake.Status = WakeOutboxStatus.DeadLetter;
        wake.Version++;
        await context.SaveChangesAsync(cancellationToken);
    }

    public async ValueTask<RecoveryState> LoadRecoveryStateAsync(CancellationToken cancellationToken)
    {
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        var interrupted = await context.WakeOutbox
            .Where(item => item.Status == WakeOutboxStatus.Dispatching)
            .ToListAsync(cancellationToken);
        foreach (var item in interrupted)
        {
            item.Status = WakeOutboxStatus.RetryScheduled;
            item.NextAttemptAtUtc = DateTimeOffset.UtcNow;
            item.LastErrorCode = "dispatch.recovered";
            item.Version++;
        }

        if (interrupted.Count > 0)
        {
            await context.SaveChangesAsync(cancellationToken);
        }

        var dispatchable = await context.WakeOutbox
            .Where(item => item.Status == WakeOutboxStatus.Pending || item.Status == WakeOutboxStatus.RetryScheduled)
            .OrderBy(item => item.NextAttemptAtUtc)
            .ThenBy(item => item.CreatedAtUtc)
            .ThenBy(item => item.Id)
            .Select(item => ToDomain(item))
            .ToListAsync(cancellationToken);
        var checkpoints = await context.SnapshotCheckpoints
            .AsNoTracking()
            .Select(checkpoint => new Core.Domain.SnapshotCheckpoint(
                checkpoint.BindingId,
                checkpoint.SessionId,
                checkpoint.Fingerprint,
                checkpoint.AcceptedAtUtc))
            .ToListAsync(cancellationToken);
        return new RecoveryState(dispatchable, checkpoints);
    }

    public async ValueTask EnsureOpenClawTargetsAsync(
        IReadOnlyDictionary<string, string> targetByBindingId,
        IReadOnlySet<string> configuredTargetNames,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(targetByBindingId);
        ArgumentNullException.ThrowIfNull(configuredTargetNames);

        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        await using var transaction = await context.Database.BeginTransactionAsync(cancellationToken);
        var activeItems = await context.WakeOutbox
            .Where(item =>
                item.Status == WakeOutboxStatus.Pending ||
                item.Status == WakeOutboxStatus.Dispatching ||
                item.Status == WakeOutboxStatus.RetryScheduled ||
                item.Status == WakeOutboxStatus.Accepted ||
                item.Status == WakeOutboxStatus.Executing)
            .OrderBy(item => item.Id)
            .ToListAsync(cancellationToken);

        var changed = false;
        foreach (var item in activeItems)
        {
            if (string.IsNullOrWhiteSpace(item.OpenClawTarget))
            {
                if (!targetByBindingId.TryGetValue(item.BindingId, out var bindingTarget) ||
                    string.IsNullOrWhiteSpace(bindingTarget))
                {
                    throw new InvalidOperationException(
                        $"Active wake outbox item '{item.Id}' for binding '{item.BindingId}' has no configured binding target.");
                }

                item.OpenClawTarget = bindingTarget;
                item.Version++;
                changed = true;
            }

            if (!configuredTargetNames.Contains(item.OpenClawTarget))
            {
                throw new InvalidOperationException(
                    $"Active wake outbox item '{item.Id}' for binding '{item.BindingId}' references unconfigured OpenClaw target '{item.OpenClawTarget}'.");
            }
        }

        if (changed)
            await context.SaveChangesAsync(cancellationToken);

        await transaction.CommitAsync(cancellationToken);
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