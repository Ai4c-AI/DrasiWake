namespace DrasiWake.Core.Contracts;

public sealed record CanonicalIdentityDefinition(
    string OntologyContext,
    string EntityType,
    string EntityIdPointer);

public sealed record RetryPolicy(int MaxAttempts, TimeSpan MaxAge);

public sealed record RateLimitPolicy(int PermitLimit, TimeSpan Window);

public sealed record BridgeBinding(
    string Id,
    string Source,
    Uri Server,
    string? InstanceId,
    string? QueryId,
    string DeliveryMode,
    string SessionScope,
    string? AggregateKeyPointer,
    CanonicalIdentityDefinition? CanonicalIdentity,
    string MetaSkill,
    BridgeContract Contract,
    int MaxPayloadBytes,
    IReadOnlyList<string> TriggerPhrases,
    RetryPolicy Retry,
    RateLimitPolicy RateLimit);