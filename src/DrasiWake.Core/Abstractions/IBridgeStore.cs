using DrasiWake.Core.Domain;

namespace DrasiWake.Core.Abstractions;

public interface IBridgeStore
{
    ValueTask<WakeOutboxItem> CreateOrUpdatePendingWakeAsync(WakeOutboxItem item, CancellationToken cancellationToken);
    ValueTask<IReadOnlyList<WakeOutboxItem>> LoadDispatchableAsync(DateTimeOffset nowUtc, int limit, CancellationToken cancellationToken);
    ValueTask MarkAcceptedWithCheckpointAsync(Guid outboxId, SnapshotCheckpoint checkpoint, CancellationToken cancellationToken);
    ValueTask MarkRetryScheduledAsync(Guid outboxId, int attemptCount, DateTimeOffset nextAttemptUtc, string reasonCode, CancellationToken cancellationToken);
    ValueTask MarkDeadLetterAsync(Guid outboxId, string reasonCode, CancellationToken cancellationToken);
    ValueTask<RecoveryState> LoadRecoveryStateAsync(CancellationToken cancellationToken);
}