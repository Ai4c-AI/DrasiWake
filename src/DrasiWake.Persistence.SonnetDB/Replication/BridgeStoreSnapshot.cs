using DrasiWake.Core.Domain;

namespace DrasiWake.Persistence.SonnetDB.Replication;

public sealed record BridgeStoreSnapshot(
    int SchemaVersion,
    long LastAppliedIndex,
    string? LastAppliedCommandId,
    string? ConfigurationFingerprint,
    IReadOnlyList<SubscriptionSnapshot> Subscriptions,
    IReadOnlyList<SnapshotCheckpointSnapshot> SnapshotCheckpoints,
    IReadOnlyList<KeyMappingSnapshot> KeyMappings,
    IReadOnlyList<WakeOutboxSnapshot> WakeOutbox)
{
    public const int CurrentSchemaVersion = 1;
}

public sealed record SubscriptionSnapshot(
    string QueryKey,
    string ServerUri,
    string? InstanceId,
    string QueryId,
    bool IsConnected,
    DateTimeOffset? LastConnectedAtUtc,
    DateTimeOffset? LastReconciledAtUtc,
    string? LastErrorCode);

public sealed record SnapshotCheckpointSnapshot(
    string BindingId,
    string SessionId,
    string Fingerprint,
    DateTimeOffset AcceptedAtUtc);

public sealed record KeyMappingSnapshot(
    string ContractScope,
    string CanonicalIdentity,
    string SessionId,
    string State,
    DateTimeOffset UpdatedAtUtc);

public sealed record WakeOutboxSnapshot(
    Guid Id,
    string BindingId,
    string SessionId,
    string SnapshotFingerprint,
    string Skill,
    string? OpenClawTarget,
    string InputJson,
    string ContractVersion,
    string IdempotencyKey,
    int AttemptCount,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset NextAttemptAtUtc,
    WakeOutboxStatus Status,
    string? InvocationId,
    string? TraceId,
    string? LastErrorCode,
    DateTimeOffset? RetainUntilUtc,
    int Version,
    string? ClaimCommandId = null);
