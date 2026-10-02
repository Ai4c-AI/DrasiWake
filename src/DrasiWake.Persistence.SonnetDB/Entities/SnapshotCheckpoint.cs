namespace DrasiWake.Persistence.SonnetDB.Entities;

public sealed class SnapshotCheckpoint
{
    public string BindingId { get; set; } = string.Empty;
    public string SessionId { get; set; } = string.Empty;
    public string Fingerprint { get; set; } = string.Empty;
    public DateTimeOffset AcceptedAtUtc { get; set; }
}