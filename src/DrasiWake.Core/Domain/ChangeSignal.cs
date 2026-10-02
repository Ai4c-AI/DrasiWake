namespace DrasiWake.Core.Domain;

public sealed record ChangeSignal(
    QueryIdentity Query,
    DateTimeOffset ObservedAtUtc,
    string? SignalId = null);