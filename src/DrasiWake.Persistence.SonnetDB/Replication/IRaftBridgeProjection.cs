using DrasiWake.Core.Domain;

namespace DrasiWake.Persistence.SonnetDB.Replication;

public interface IRaftBridgeProjection
{
    ValueTask<long> GetLastAppliedIndexAsync(CancellationToken cancellationToken);
    ValueTask AdvanceAppliedIndexAsync(long raftIndex, CancellationToken cancellationToken);
    ValueTask ApplyReplicatedCommandAsync(
        ReplicatedBridgeCommand command,
        long raftIndex,
        CancellationToken cancellationToken);
    ValueTask<BridgeStoreSnapshot> ExportSnapshotAsync(CancellationToken cancellationToken);
    ValueTask RestoreSnapshotAsync(BridgeStoreSnapshot snapshot, CancellationToken cancellationToken);

    ValueTask<IReadOnlyList<WakeOutboxItem>> LoadDispatchableCandidatesAsync(
        DateTimeOffset nowUtc,
        int limit,
        CancellationToken cancellationToken);
    ValueTask<IReadOnlyList<WakeOutboxItem>> ReadClaimedDispatchableAsync(
        IReadOnlyList<Guid> outboxIds,
        string claimCommandId,
        CancellationToken cancellationToken);
    ValueTask<WakeOutboxItem?> FindPendingWakeResultAsync(
        Guid requestedId,
        string bindingId,
        string sessionId,
        string snapshotFingerprint,
        CancellationToken cancellationToken);
    ValueTask<WakeOutboxItem?> ReadWakeAsync(Guid outboxId, CancellationToken cancellationToken);
    ValueTask<RecoveryState> ReadRecoveryStateAsync(CancellationToken cancellationToken);
}
