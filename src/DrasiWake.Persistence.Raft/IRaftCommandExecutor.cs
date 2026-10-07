using DrasiWake.Persistence.SonnetDB.Replication;

namespace DrasiWake.Persistence.Raft;

public interface IRaftCommandExecutor
{
    ValueTask ReplicateAsync(ReplicatedBridgeCommand command, CancellationToken cancellationToken);

    bool IsLeader { get; }

    bool HasQuorum { get; }
}
