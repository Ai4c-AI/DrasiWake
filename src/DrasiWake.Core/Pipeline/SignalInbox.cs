using System.Threading.Channels;
using DrasiWake.Core.Domain;

namespace DrasiWake.Core.Pipeline;

public sealed class SignalInbox
{
    private readonly Channel<ChangeSignal> channel;
    private readonly object dirtyLock = new();
    private readonly HashSet<QueryIdentity> dirtyQueries = [];

    public SignalInbox(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        channel = Channel.CreateBounded<ChangeSignal>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false
        });
    }

    public bool TryWrite(ChangeSignal signal)
    {
        ArgumentNullException.ThrowIfNull(signal);
        if (channel.Writer.TryWrite(signal))
            return true;

        lock (dirtyLock)
            dirtyQueries.Add(signal.Query);
        return false;
    }

    public bool TryRead(out ChangeSignal? signal) => channel.Reader.TryRead(out signal);

    public IAsyncEnumerable<ChangeSignal> ReadAllAsync(CancellationToken cancellationToken = default)
        => channel.Reader.ReadAllAsync(cancellationToken);

    public async Task ReceiveAsync(
        IAsyncEnumerable<ChangeSignal> signals,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(signals);
        await foreach (var signal in signals.WithCancellation(cancellationToken))
            TryWrite(signal);
    }

    public IReadOnlyList<QueryIdentity> TakeDirtyQueries()
    {
        lock (dirtyLock)
        {
            var queries = dirtyQueries.ToArray();
            dirtyQueries.Clear();
            return queries;
        }
    }

    public bool TryComplete(Exception? exception = null) => channel.Writer.TryComplete(exception);
}