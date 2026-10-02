using DrasiWake.Core.Domain;

namespace DrasiWake.Adapters.OpenClaw;

public sealed class OpenClawIdempotencyConflictException(string message)
    : WakeSinkException(WakeSinkFailureKind.ContractConflict, message);

public sealed class OpenClawInvocationUncertainException(string invocationId, string? message)
    : WakeSinkException(WakeSinkFailureKind.OutcomeUncertain, message ?? "Gateway reports that the MetaSkill invocation outcome is uncertain.")
{
    public string InvocationId { get; } = invocationId;
}