namespace DrasiWake.Core.Domain;

public sealed record WakeAcceptance(string InvocationId, DateTimeOffset AcceptedAtUtc);