using DotNext.Net.Cluster.Consensus.Raft;
using DotNext.Net.Cluster;
using DrasiWake.Persistence.SonnetDB.Replication;

namespace DrasiWake.Persistence.Raft;

public sealed class DotNextRaftCommandExecutor(IRaftCluster cluster) : IRaftCommandExecutor
{
    private readonly IRaftCluster _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
    private readonly Func<long, CancellationToken, ValueTask> _waitForApplyAsync =
        (index, cancellationToken) =>
            (cluster ?? throw new ArgumentNullException(nameof(cluster)))
            .AuditTrail.WaitForApplyAsync(index, cancellationToken);

    internal DotNextRaftCommandExecutor(
        IRaftCluster cluster,
        Func<long, CancellationToken, ValueTask> waitForApplyAsync)
        : this(cluster)
    {
        _waitForApplyAsync = waitForApplyAsync ?? throw new ArgumentNullException(nameof(waitForApplyAsync));
    }

    public bool IsLeader => !_cluster.LeadershipToken.IsCancellationRequested;

    public bool HasQuorum =>
        !_cluster.ConsensusToken.IsCancellationRequested &&
        DotNextRaftQuorum.IsAvailable(_cluster);

    public async ValueTask ReplicateAsync(
        ReplicatedBridgeCommand command,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(command);
        if (!IsLeader)
            throw new InvalidOperationException("Only the Raft leader can submit bridge commands.");
        if (!HasQuorum)
            throw new InvalidOperationException("Bridge commands cannot be submitted without a Raft quorum.");

        var payload = BridgeReplicationSerializer.SerializeCommand(command);
        await _cluster.ReplicateAsync(payload, null, cancellationToken).ConfigureAwait(false);
        var committedIndex = _cluster.AuditTrail.LastCommittedEntryIndex;
        if (committedIndex < 1)
            throw new InvalidOperationException("The committed Raft command has no valid log index.");

        await _waitForApplyAsync(committedIndex, cancellationToken).ConfigureAwait(false);
    }
}

public static class DotNextRaftQuorum
{
    public static bool IsAvailable(IRaftCluster cluster)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        return !cluster.ConsensusToken.IsCancellationRequested && HasAvailableMajority(cluster);
    }

    public static bool HasAvailableMajority(IRaftCluster cluster)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        var members = cluster.Members.ToArray();
        if (members.Length == 0)
            return false;

        var availableCount = 0;
        foreach (var member in members)
        {
            try
            {
                if (member.Status == ClusterMemberStatus.Available)
                    availableCount++;
            }
            catch (ObjectDisposedException)
            {
                // A peer removed during reconfiguration cannot contribute to a quorum observation.
            }
        }

        return availableCount >= members.Length / 2 + 1;
    }
}
