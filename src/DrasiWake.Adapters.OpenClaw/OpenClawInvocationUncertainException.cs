namespace DrasiWake.Adapters.OpenClaw;

public sealed class OpenClawIdempotencyConflictException(string message) : InvalidOperationException(message);

public sealed class OpenClawInvocationUncertainException(string invocationId, string? message)
    : Exception(message ?? "Gateway reports that the MetaSkill invocation outcome is uncertain.")
{
    public string InvocationId { get; } = invocationId;
}