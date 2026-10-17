using System.Text.Json.Nodes;
using DrasiWake.Core.Domain;

namespace DrasiWake.Core.Contracts;

/// <summary>Admin API outbox list entry (no payload or key material).</summary>
public sealed record OutboxListItem(
    Guid Id,
    string BindingId,
    string SessionId,
    string Skill,
    string ContractVersion,
    WakeOutboxStatus Status,
    int AttemptCount,
    string OpenClawTarget,
    int ReplayCount,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset NextAttemptAtUtc,
    DateTimeOffset? FirstDispatchedAtUtc);

/// <summary>Admin API outbox detail entry with the full rendered payload and audit trail.</summary>
public sealed record OutboxDetail(
    Guid Id,
    string BindingId,
    string SessionId,
    string SnapshotFingerprint,
    string Skill,
    string ContractVersion,
    WakeOutboxStatus Status,
    int AttemptCount,
    string OpenClawTarget,
    string IdempotencyKey,
    string? InvocationId,
    string? TraceId,
    int ReplayCount,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset NextAttemptAtUtc,
    DateTimeOffset? FirstDispatchedAtUtc,
    Guid? SupersededByOutboxId,
    DateTimeOffset? LastReplayedAtUtc,
    JsonObject Input);

/// <summary>Admin API cluster overview.</summary>
public sealed record AdminClusterStatus(
    string NodeId,
    string Mode,
    string Role,
    bool HasQuorum,
    bool Writable,
    long LastAppliedIndex,
    IReadOnlyList<AdminClusterMember> Members);

public sealed record AdminClusterMember(string Endpoint, bool IsLeader);

/// <summary>Admin API downstream probe snapshot entry.</summary>
public sealed record AdminDownstreamStatus(
    string Downstream,
    string Address,
    DateTimeOffset ProbedAtUtc,
    bool Reachable,
    long LatencyMs,
    string? ErrorClass);

/// <summary>Admin API per-check health detail.</summary>
public sealed record AdminHealthCheckDetail(
    string Name,
    string Status,
    double DurationMs,
    string? Description);

/// <summary>Admin API replay request body (optional reason for the audit trail).</summary>
public sealed record OutboxReplayRequest(string? Reason);

/// <summary>Admin API outbox list query. Keyset pagination over (CreatedAtUtc, Id) descending.</summary>
public sealed record OutboxQuery(
    IReadOnlyList<WakeOutboxStatus>? Statuses,
    string? BindingId,
    string? SessionId,
    int Limit,
    DateTimeOffset? CursorCreatedBeforeUtc,
    Guid? CursorIdBefore)
{
    public const int DefaultLimit = 50;
    public const int MaxLimit = 500;
}

public static class AdminDtosMapper
{
    public static OutboxListItem ToListItem(WakeOutboxItem item) => new(
        item.Id,
        item.BindingId,
        item.SessionId,
        item.Skill,
        item.ContractVersion,
        item.Status,
        item.AttemptCount,
        item.OpenClawTarget,
        item.ReplayCount,
        item.CreatedAtUtc,
        item.NextAttemptAtUtc,
        item.FirstDispatchedAtUtc);

    public static OutboxDetail ToDetail(WakeOutboxItem item) => new(
        item.Id,
        item.BindingId,
        item.SessionId,
        item.SnapshotFingerprint,
        item.Skill,
        item.ContractVersion,
        item.Status,
        item.AttemptCount,
        item.OpenClawTarget,
        item.IdempotencyKey,
        item.InvocationId,
        item.TraceId,
        item.ReplayCount,
        item.CreatedAtUtc,
        item.NextAttemptAtUtc,
        item.FirstDispatchedAtUtc,
        item.SupersededByOutboxId,
        item.LastReplayedAtUtc,
        item.Input);
}
