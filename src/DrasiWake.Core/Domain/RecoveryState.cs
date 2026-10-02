namespace DrasiWake.Core.Domain;

public sealed record RecoveryState(
    IReadOnlyList<WakeOutboxItem> DispatchableItems,
    IReadOnlyList<SnapshotCheckpoint> Checkpoints);