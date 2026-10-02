using DrasiWake.Adapters.OpenClaw;

namespace DrasiWake.Adapters.OpenClaw.Tests;

public sealed class OpenClawOptionsTests
{
    [Fact]
    public void Rejects_gateway_retention_shorter_than_outbox_retry_age()
    {
        var options = new OpenClawOptions
        {
            GatewayIdempotencyRetention = TimeSpan.FromDays(6),
            MaximumOutboxRetryAge = TimeSpan.FromDays(7)
        };

        Assert.Throws<InvalidOperationException>(options.Validate);
    }
}