using DrasiWake.Core.Domain;

namespace DrasiWake.Persistence.SonnetDB.Entities;

public sealed class WakeOutbox
{
    public Guid Id { get; set; }
    public string BindingId { get; set; } = string.Empty;
    public string SessionId { get; set; } = string.Empty;
    public string SnapshotFingerprint { get; set; } = string.Empty;
    public string Skill { get; set; } = string.Empty;
    public string? OpenClawTarget { get; set; }
    public string InputJson { get; set; } = "{}";
    public string ContractVersion { get; set; } = string.Empty;
    public string IdempotencyKey { get; set; } = string.Empty;
    public int AttemptCount { get; set; }
    public DateTimeOffset CreatedAtUtc { get; set; }
    public DateTimeOffset NextAttemptAtUtc { get; set; }
    public WakeOutboxStatus Status { get; set; }
    public string? InvocationId { get; set; }
    public string? TraceId { get; set; }
    public string? LastErrorCode { get; set; }
    public DateTimeOffset? RetainUntilUtc { get; set; }
    public int Version { get; set; }
}