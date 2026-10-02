using DrasiWake.Core.Domain;

namespace DrasiWake.Core.Abstractions;

public interface IChangeSource
{
    IAsyncEnumerable<ChangeSignal> WatchAsync(CancellationToken cancellationToken);
    ValueTask<IReadOnlyList<QueryIdentity>> EnumerateQueriesAsync(CancellationToken cancellationToken);
    ValueTask<QuerySnapshot> ReadSnapshotAsync(QueryIdentity query, CancellationToken cancellationToken);
}