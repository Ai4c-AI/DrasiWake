namespace DrasiWake.Adapters.OpenClaw;

public sealed class OpenClawOptions
{
    public int MaxRetryAttempts { get; init; } = 3;
    public TimeSpan RetryBaseDelay { get; init; } = TimeSpan.FromMilliseconds(200);
    public TimeSpan MaxRetryDelay { get; init; } = TimeSpan.FromSeconds(5);

    public void Validate()
    {
        if (MaxRetryAttempts < 0)
            throw new InvalidOperationException("OpenClaw MaxRetryAttempts cannot be negative.");
        if (RetryBaseDelay < TimeSpan.Zero || MaxRetryDelay < RetryBaseDelay)
            throw new InvalidOperationException("OpenClaw retry delays are invalid.");
    }
}