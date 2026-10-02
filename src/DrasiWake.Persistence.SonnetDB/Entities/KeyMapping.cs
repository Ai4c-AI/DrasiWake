namespace DrasiWake.Persistence.SonnetDB.Entities;

public sealed class KeyMapping
{
    public string ContractScope { get; set; } = string.Empty;
    public string CanonicalIdentity { get; set; } = string.Empty;
    public string SessionId { get; set; } = string.Empty;
    public string State { get; set; } = "active";
    public DateTimeOffset UpdatedAtUtc { get; set; }
}