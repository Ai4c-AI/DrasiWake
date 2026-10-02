namespace DrasiWake.Core.Domain;

public sealed record WakeExecutionStatus(
    string InvocationId,
    string State,
    DateTimeOffset? UpdatedAtUtc);