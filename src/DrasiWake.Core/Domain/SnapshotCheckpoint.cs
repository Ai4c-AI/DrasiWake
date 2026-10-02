namespace DrasiWake.Core.Domain;

public sealed record SnapshotCheckpoint(
    string BindingId,
    string SessionId,
    string Fingerprint,
    DateTimeOffset AcceptedAtUtc);