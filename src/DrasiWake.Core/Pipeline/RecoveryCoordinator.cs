using System.Collections.Concurrent;
using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Domain;

namespace DrasiWake.Core.Pipeline;

public interface IRecoveryCoordinator
{
    Task RecoverAsync(CancellationToken cancellationToken);

    Task RecoverAsync(CancellationToken cancellationToken, bool enqueueDispatchableItems)
        => RecoverAsync(cancellationToken);

    Task ReconcileAfterReconnectAsync(QueryIdentity query, CancellationToken cancellationToken);
}

public sealed class RecoveryCoordinator(
    IChangeSource changeSource,
    IBridgeStore store,
    SnapshotReconciler reconciler,
    SessionPartitioner partitioner) : IRecoveryCoordinator
{
    private readonly ConcurrentDictionary<QueryIdentity, bool> queryHealth = new();
    private int initialized;

    public bool IsInitialized => Volatile.Read(ref initialized) != 0;

    public bool IsHealthy(QueryIdentity query)
        => queryHealth.TryGetValue(query, out var healthy) && healthy;

    public Task RecoverAsync(CancellationToken cancellationToken)
        => RecoverAsync(cancellationToken, enqueueDispatchableItems: true);

    public async Task RecoverAsync(CancellationToken cancellationToken, bool enqueueDispatchableItems)
    {
        Volatile.Write(ref initialized, 0);
        _ = await store.LoadRecoveryStateAsync(cancellationToken);
        var queries = await changeSource.EnumerateQueriesAsync(cancellationToken);
        foreach (var query in queries)
            await ReconcileAfterReconnectAsync(query, cancellationToken);

        var recoveredState = await store.LoadRecoveryStateAsync(cancellationToken);
        if (enqueueDispatchableItems)
        {
            foreach (var item in recoveredState.DispatchableItems)
                await partitioner.EnqueueAsync(item, cancellationToken);
        }
        Volatile.Write(ref initialized, 1);
    }

    public async Task ReconcileAfterReconnectAsync(QueryIdentity query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        queryHealth[query] = false;
        await reconciler.ReconcileAsync(query, cancellationToken);
        queryHealth[query] = true;
    }
}