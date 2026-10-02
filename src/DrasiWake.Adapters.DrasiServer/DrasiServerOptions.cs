namespace DrasiWake.Adapters.DrasiServer;

public sealed record DrasiServerOptions(Uri ServerUri)
{
    public int SignalCapacity { get; init; } = 256;
    public TimeSpan InitialReconnectDelay { get; init; } = TimeSpan.FromSeconds(1);
    public TimeSpan MaxReconnectDelay { get; init; } = TimeSpan.FromSeconds(30);

    internal void Validate()
    {
        ArgumentNullException.ThrowIfNull(ServerUri);
        if (!ServerUri.IsAbsoluteUri)
        {
            throw new ArgumentException("Server URI must be absolute.", nameof(ServerUri));
        }

        if (SignalCapacity < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(SignalCapacity));
        }

        if (InitialReconnectDelay <= TimeSpan.Zero || MaxReconnectDelay < InitialReconnectDelay)
        {
            throw new ArgumentOutOfRangeException(nameof(InitialReconnectDelay));
        }
    }
}