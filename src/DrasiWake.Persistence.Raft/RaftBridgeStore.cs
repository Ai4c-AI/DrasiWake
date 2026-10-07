using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Domain;
using DrasiWake.Persistence.SonnetDB.Replication;

namespace DrasiWake.Persistence.Raft;

public sealed class RaftBridgeStore(
    IRaftCommandExecutor commandExecutor,
    IRaftBridgeProjection projection) : IBridgeStore
{
    private readonly IRaftCommandExecutor _commandExecutor =
        commandExecutor ?? throw new ArgumentNullException(nameof(commandExecutor));
    private readonly IRaftBridgeProjection _projection =
        projection ?? throw new ArgumentNullException(nameof(projection));

    public async ValueTask<WakeOutboxItem> CreateOrUpdatePendingWakeAsync(
        WakeOutboxItem item,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(item);
        if (item.Status != WakeOutboxStatus.Pending)
            throw new ArgumentException("Only pending wake items can be created or updated.", nameof(item));
        var command = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.CreateOrUpdatePendingWake,
            new CreateOrUpdatePendingWakePayload(item));
        await _commandExecutor.ReplicateAsync(command, cancellationToken).ConfigureAwait(false);
        return await _projection.FindPendingWakeResultAsync(
                   item.Id,
                   item.BindingId,
                   item.SessionId,
                   item.SnapshotFingerprint,
                   cancellationToken)
                   .ConfigureAwait(false)
               ?? throw new InvalidOperationException(
                   $"Committed pending wake command '{command.CommandId}' has no matching projection result.");
    }

    public ValueTask RecordRejectedWakeAsync(
        WakeOutboxItem item,
        string reasonCode,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(item);
        ArgumentException.ThrowIfNullOrWhiteSpace(reasonCode);
        if (item.Status != WakeOutboxStatus.DeadLetter)
            throw new ArgumentException("Rejected wake records must be dead letters.", nameof(item));
        var command = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.RecordRejectedWake,
            new RecordRejectedWakePayload(item, reasonCode));
        return _commandExecutor.ReplicateAsync(command, cancellationToken);
    }

    public ValueTask SupersedePendingWakesAsync(
        string bindingId,
        string sessionId,
        string currentFingerprint,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(bindingId);
        ArgumentException.ThrowIfNullOrWhiteSpace(sessionId);
        ArgumentException.ThrowIfNullOrWhiteSpace(currentFingerprint);
        var command = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.SupersedePendingWakes,
            new SupersedePendingWakesPayload(bindingId, sessionId, currentFingerprint));
        return _commandExecutor.ReplicateAsync(command, cancellationToken);
    }

    public async ValueTask<IReadOnlyList<WakeOutboxItem>> LoadDispatchableAsync(
        DateTimeOffset nowUtc,
        int limit,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        var candidates = await _projection.LoadDispatchableCandidatesAsync(
                nowUtc, limit, cancellationToken)
            .ConfigureAwait(false);
        if (candidates.Count == 0)
            return [];

        var outboxIds = candidates.Select(item => item.Id).ToArray();
        var command = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.ClaimDispatchable,
            new ClaimDispatchablePayload(outboxIds, nowUtc, Guid.NewGuid()));
        await _commandExecutor.ReplicateAsync(command, cancellationToken).ConfigureAwait(false);

        var claimed = await _projection.ReadClaimedDispatchableAsync(
                outboxIds, command.CommandId, cancellationToken)
            .ConfigureAwait(false);
        return claimed;
    }

    public async ValueTask MarkAcceptedWithCheckpointAsync(
        Guid outboxId,
        WakeAcceptance acceptance,
        SnapshotCheckpoint checkpoint,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(acceptance);
        ArgumentNullException.ThrowIfNull(checkpoint);
        ArgumentException.ThrowIfNullOrWhiteSpace(acceptance.InvocationId);
        var wake = await _projection.ReadWakeAsync(outboxId, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"Outbox item '{outboxId}' does not exist.");
        if (!string.Equals(wake.BindingId, checkpoint.BindingId, StringComparison.Ordinal) ||
            !string.Equals(wake.SessionId, checkpoint.SessionId, StringComparison.Ordinal) ||
            !string.Equals(wake.SnapshotFingerprint, checkpoint.Fingerprint, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "Accepted checkpoint must match the outbox binding, session, and fingerprint.");
        }

        if (wake.Status is WakeOutboxStatus.Accepted or WakeOutboxStatus.Executing or WakeOutboxStatus.Completed)
        {
            if (!string.Equals(wake.InvocationId, acceptance.InvocationId, StringComparison.Ordinal))
                throw new InvalidOperationException("Repeated acceptance must refer to the original Gateway invocation.");
            var recovery = await _projection.ReadRecoveryStateAsync(cancellationToken).ConfigureAwait(false);
            var acceptedCheckpoint = recovery.Checkpoints.SingleOrDefault(
                item => item.BindingId == checkpoint.BindingId && item.SessionId == checkpoint.SessionId);
            if (acceptedCheckpoint is null ||
                !string.Equals(acceptedCheckpoint.Fingerprint, checkpoint.Fingerprint, StringComparison.Ordinal))
            {
                throw new InvalidOperationException("Accepted outbox item must have its matching snapshot checkpoint.");
            }
        }
        else if (wake.Status is not
                 (WakeOutboxStatus.Pending or WakeOutboxStatus.Dispatching or WakeOutboxStatus.RetryScheduled))
        {
            throw new InvalidOperationException($"Outbox item in state '{wake.Status}' cannot be accepted.");
        }

        var command = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.MarkAcceptedWithCheckpoint,
            new MarkAcceptedWithCheckpointPayload(outboxId, acceptance, checkpoint));
        await _commandExecutor.ReplicateAsync(command, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask UpdateExecutionStatusAsync(
        WakeExecutionStatus status,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(status);
        ArgumentException.ThrowIfNullOrWhiteSpace(status.InvocationId);
        _ = status.State switch
        {
            "Running" or "Completed" or "Uncertain" => true,
            _ => throw new ArgumentException(
                $"Unsupported Gateway invocation state '{status.State}'.", nameof(status))
        };
        var command = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.UpdateExecutionStatus,
            new UpdateExecutionStatusPayload(status));
        return _commandExecutor.ReplicateAsync(command, cancellationToken);
    }

    public ValueTask MarkRetryScheduledAsync(
        Guid outboxId,
        int attemptCount,
        DateTimeOffset nextAttemptUtc,
        string reasonCode,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(attemptCount);
        ArgumentException.ThrowIfNullOrWhiteSpace(reasonCode);
        var command = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.MarkRetryScheduled,
            new MarkRetryScheduledPayload(outboxId, attemptCount, nextAttemptUtc, reasonCode));
        return _commandExecutor.ReplicateAsync(command, cancellationToken);
    }

    public ValueTask MarkDeadLetterAsync(
        Guid outboxId,
        string reasonCode,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reasonCode);
        var command = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.MarkDeadLetter,
            new MarkDeadLetterPayload(outboxId, reasonCode));
        return _commandExecutor.ReplicateAsync(command, cancellationToken);
    }

    public async ValueTask<RecoveryState> LoadRecoveryStateAsync(CancellationToken cancellationToken)
    {
        var recoveryTime = DateTimeOffset.UtcNow;
        var command = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.RecoverInterruptedDispatches,
            new RecoverInterruptedDispatchesPayload(recoveryTime));
        await _commandExecutor.ReplicateAsync(command, cancellationToken).ConfigureAwait(false);
        return await _projection.ReadRecoveryStateAsync(cancellationToken).ConfigureAwait(false);
    }

    public ValueTask EnsureOpenClawTargetsAsync(
        IReadOnlyDictionary<string, string> targetByBindingId,
        IReadOnlySet<string> configuredTargetNames,
        IReadOnlyDictionary<string, TimeSpan> maximumRetryAgeByBindingId,
        IReadOnlyDictionary<string, TimeSpan> idempotencyRetentionByTarget,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(targetByBindingId);
        ArgumentNullException.ThrowIfNull(configuredTargetNames);
        ArgumentNullException.ThrowIfNull(maximumRetryAgeByBindingId);
        ArgumentNullException.ThrowIfNull(idempotencyRetentionByTarget);
        var command = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.EnsureOpenClawTargets,
            new EnsureOpenClawTargetsPayload(
                targetByBindingId,
                configuredTargetNames,
                maximumRetryAgeByBindingId,
                idempotencyRetentionByTarget));
        return _commandExecutor.ReplicateAsync(command, cancellationToken);
    }
}
