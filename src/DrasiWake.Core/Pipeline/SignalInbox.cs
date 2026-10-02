using System.Threading.Channels;
using System.Runtime.CompilerServices;
using DrasiWake.Core.Domain;

namespace DrasiWake.Core.Pipeline;

public sealed class SignalInbox
{
    private readonly Channel<ChangeSignal> channel;
    private readonly object dirtyLock = new();
    private readonly HashSet<QueryIdentity> dirtyQueries = [];
    private int queueDepth;

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
        Interlocked.Increment(ref queueDepth);
        if (channel.Writer.TryWrite(signal))
        {
            BridgeTelemetry.RecordSignalQueued(signal.Query);
            return true;
        }

        Interlocked.Decrement(ref queueDepth);

        lock (dirtyLock)
            dirtyQueries.Add(signal.Query);
        BridgeTelemetry.RecordSignalOverflow(signal.Query);
        return false;
    }

    public bool TryRead(out ChangeSignal? signal)
    {
        if (!channel.Reader.TryRead(out signal))
            return false;
        Interlocked.Decrement(ref queueDepth);
        BridgeTelemetry.RecordSignalDequeued(signal!.Query);
        return true;
    }

    public async IAsyncEnumerable<ChangeSignal> ReadAllAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var signal in channel.Reader.ReadAllAsync(cancellationToken))
        {
            Interlocked.Decrement(ref queueDepth);
            BridgeTelemetry.RecordSignalDequeued(signal.Query);
            yield return signal;
        }
    }

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