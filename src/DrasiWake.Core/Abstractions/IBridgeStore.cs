using DrasiWake.Core.Domain;

namespace DrasiWake.Core.Abstractions;

public interface IBridgeStore
{
    ValueTask<WakeOutboxItem> CreateOrUpdatePendingWakeAsync(WakeOutboxItem item, CancellationToken cancellationToken);
    ValueTask RecordRejectedWakeAsync(WakeOutboxItem item, string reasonCode, CancellationToken cancellationToken);
    ValueTask SupersedePendingWakesAsync(string bindingId, string sessionId, string currentFingerprint, CancellationToken cancellationToken);
    ValueTask<IReadOnlyList<WakeOutboxItem>> LoadDispatchableAsync(DateTimeOffset nowUtc, int limit, CancellationToken cancellationToken);
    ValueTask MarkAcceptedWithCheckpointAsync(Guid outboxId, WakeAcceptance acceptance, SnapshotCheckpoint checkpoint, CancellationToken cancellationToken);
    ValueTask UpdateExecutionStatusAsync(WakeExecutionStatus status, CancellationToken cancellationToken);
    ValueTask MarkRetryScheduledAsync(Guid outboxId, int attemptCount, DateTimeOffset nextAttemptUtc, string reasonCode, CancellationToken cancellationToken);
    ValueTask MarkDeadLetterAsync(Guid outboxId, string reasonCode, CancellationToken cancellationToken);
    ValueTask<RecoveryState> LoadRecoveryStateAsync(CancellationToken cancellationToken);
    ValueTask EnsureOpenClawTargetsAsync(
        IReadOnlyDictionary<string, string> targetByBindingId,
        IReadOnlySet<string> configuredTargetNames,
        IReadOnlyDictionary<string, TimeSpan> maximumRetryAgeByBindingId,
        IReadOnlyDictionary<string, TimeSpan> idempotencyRetentionByTarget,
        CancellationToken cancellationToken);
}