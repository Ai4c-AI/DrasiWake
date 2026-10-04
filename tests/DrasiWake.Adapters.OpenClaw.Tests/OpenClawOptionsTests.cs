using DrasiWake.Adapters.OpenClaw;

namespace DrasiWake.Adapters.OpenClaw.Tests;

public sealed class OpenClawOptionsTests
{
    [Fact]
    public void Rejects_negative_retry_attempts()
    {
        var options = new OpenClawOptions { MaxRetryAttempts = -1 };

        Assert.Throws<InvalidOperationException>(options.Validate);
    }
}