using System.Text.Json.Nodes;

namespace DrasiWake.Core.Domain;

public sealed record WakeRequest(
    string BindingId,
    string SessionId,
    string Skill,
    JsonObject Input,
    string IdempotencyKey,
    string ContractVersion,
    string? TraceId,
    string OpenClawTarget);