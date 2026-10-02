namespace DrasiWake.Adapters.OpenClaw;

public sealed class OpenClawOptions
{
    public Uri? BaseAddress { get; init; }
    public string? BearerToken { get; init; }
    public int MaxRetryAttempts { get; init; } = 3;
    public TimeSpan RetryBaseDelay { get; init; } = TimeSpan.FromMilliseconds(200);
    public TimeSpan MaxRetryDelay { get; init; } = TimeSpan.FromSeconds(5);
    public TimeSpan GatewayIdempotencyRetention { get; init; } = TimeSpan.FromDays(30);
    public TimeSpan MaximumOutboxRetryAge { get; init; } = TimeSpan.FromDays(7);

    public void Validate()
    {
        if (MaxRetryAttempts < 0)
            throw new InvalidOperationException("OpenClaw MaxRetryAttempts cannot be negative.");
        if (RetryBaseDelay < TimeSpan.Zero || MaxRetryDelay < RetryBaseDelay)
            throw new InvalidOperationException("OpenClaw retry delays are invalid.");
        if (GatewayIdempotencyRetention <= TimeSpan.Zero || MaximumOutboxRetryAge <= TimeSpan.Zero)
            throw new InvalidOperationException("OpenClaw idempotency retention and outbox retry age must be positive.");
        if (GatewayIdempotencyRetention < MaximumOutboxRetryAge)
            throw new InvalidOperationException("Gateway idempotency retention must cover the maximum outbox retry age.");
        if (BaseAddress is not null && (!BaseAddress.IsAbsoluteUri || (BaseAddress.Scheme != Uri.UriSchemeHttp && BaseAddress.Scheme != Uri.UriSchemeHttps)))
            throw new InvalidOperationException("OpenClaw BaseAddress must be an absolute HTTP or HTTPS URI.");
        if (string.IsNullOrWhiteSpace(BearerToken) && BearerToken is not null)
            throw new InvalidOperationException("OpenClaw BearerToken cannot be empty.");
    }
}