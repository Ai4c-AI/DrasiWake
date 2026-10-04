using System.Text.Json.Nodes;

namespace DrasiWake.Core.Domain;

public enum WakeOutboxStatus
{
    Pending,
    Dispatching,
    Accepted,
    Executing,
    Completed,
    RetryScheduled,
    DeadLetter,
    Superseded
}

public sealed record WakeOutboxItem(
    Guid Id,
    string BindingId,
    string SessionId,
    string SnapshotFingerprint,
    string Skill,
    JsonObject Input,
    string ContractVersion,
    string IdempotencyKey,
    int AttemptCount,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset NextAttemptAtUtc,
    WakeOutboxStatus Status,
    string? InvocationId,
    string? TraceId,
    string OpenClawTarget);