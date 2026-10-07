namespace DrasiWake.Persistence.SonnetDB.Entities;

public sealed class RaftProjectionState
{
    public const int SingletonId = 1;

    public int Id { get; set; } = SingletonId;
    public long LastAppliedIndex { get; set; }
    public string? LastAppliedCommandId { get; set; }
    public string? ConfigurationFingerprint { get; set; }
}
