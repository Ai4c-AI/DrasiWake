namespace DrasiWake.Core.Domain;

public enum WakeSinkFailureKind
{
    ContractConflict,
    OutcomeUncertain
}

public abstract class WakeSinkException(WakeSinkFailureKind kind, string message) : Exception(message)
{
    public WakeSinkFailureKind Kind { get; } = kind;
}