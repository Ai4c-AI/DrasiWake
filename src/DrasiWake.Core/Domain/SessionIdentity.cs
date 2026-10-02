namespace DrasiWake.Core.Domain;

public sealed record SessionIdentity(string Value);

public sealed record SessionIdentityResolution(SessionIdentity? Identity, string? ErrorCode)
{
    public bool IsSuccess => Identity is not null && ErrorCode is null;
}