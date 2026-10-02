using System.Runtime.CompilerServices;
using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Domain;
using DrasiWake.Core.Pipeline;

namespace DrasiWake.Core.Tests.Pipeline;

public sealed class SignalInboxTests
{
    [Fact]
    public void Full_inbox_marks_query_dirty_without_blocking_signal_producer()
    {
        var inbox = new SignalInbox(1);
        var firstQuery = new QueryIdentity(new Uri("http://drasi.test"), null, "orders-1");
        var overflowQuery = new QueryIdentity(new Uri("http://drasi.test"), null, "orders-2");

        Assert.True(inbox.TryWrite(new ChangeSignal(firstQuery, DateTimeOffset.UtcNow)));
        Assert.False(inbox.TryWrite(new ChangeSignal(overflowQuery, DateTimeOffset.UtcNow)));

        Assert.Equal([overflowQuery], inbox.TakeDirtyQueries());
        Assert.Empty(inbox.TakeDirtyQueries());
        Assert.True(inbox.TryRead(out var signal));
        Assert.Equal(firstQuery, signal!.Query);
    }

    [Fact]
    public async Task Receiving_watch_signals_never_reads_query_snapshots()
    {
        var firstQuery = new QueryIdentity(new Uri("http://drasi.test"), null, "orders-1");
        var overflowQuery = new QueryIdentity(new Uri("http://drasi.test"), null, "orders-2");
        var source = new SignalSource(firstQuery, overflowQuery);
        var inbox = new SignalInbox(1);

        await inbox.ReceiveAsync(source.WatchAsync(TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);

        Assert.Equal(0, source.SnapshotReadCount);
        Assert.Equal([overflowQuery], inbox.TakeDirtyQueries());
    }

    private sealed class SignalSource(QueryIdentity firstQuery, QueryIdentity overflowQuery) : IChangeSource
    {
        public int SnapshotReadCount { get; private set; }

        public async IAsyncEnumerable<ChangeSignal> WatchAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            yield return new ChangeSignal(firstQuery, DateTimeOffset.UtcNow);
            await Task.Yield();
            yield return new ChangeSignal(overflowQuery, DateTimeOffset.UtcNow);
        }

        public ValueTask<IReadOnlyList<QueryIdentity>> EnumerateQueriesAsync(CancellationToken cancellationToken)
            => ValueTask.FromResult<IReadOnlyList<QueryIdentity>>([firstQuery, overflowQuery]);

        public ValueTask<QuerySnapshot> ReadSnapshotAsync(QueryIdentity query, CancellationToken cancellationToken)
        {
            SnapshotReadCount++;
            return ValueTask.FromException<QuerySnapshot>(new InvalidOperationException("SSE receive must not read snapshots."));
        }
    }
}