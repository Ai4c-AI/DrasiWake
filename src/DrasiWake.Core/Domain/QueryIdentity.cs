namespace DrasiWake.Core.Domain;

public sealed record QueryIdentity(Uri Server, string? InstanceId, string QueryId);