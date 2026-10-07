using System.Buffers;
using System.Text;
using DotNext.IO;
using DotNext.Net.Cluster.Consensus.Raft.StateMachine;
using DrasiWake.Persistence.SonnetDB.Replication;
using Microsoft.Extensions.Logging;

namespace DrasiWake.Persistence.Raft;

public sealed class RaftBridgeStateMachine : SimpleStateMachine
{
    private readonly IRaftBridgeProjection _projection;
    private readonly int _snapshotFrequency;
    private readonly ILogger<RaftBridgeStateMachine> _logger;
    private int _commandsSinceSnapshot;

    public RaftBridgeStateMachine(
        IRaftBridgeProjection projection,
        DirectoryInfo snapshotLocation,
        int snapshotFrequency,
        ILogger<RaftBridgeStateMachine> logger)
        : base(snapshotLocation)
    {
        ArgumentNullException.ThrowIfNull(projection);
        ArgumentNullException.ThrowIfNull(snapshotLocation);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentOutOfRangeException.ThrowIfLessThan(snapshotFrequency, 1);

        _projection = projection;
        _snapshotFrequency = snapshotFrequency;
        _logger = logger;
    }

    protected override ValueTask<bool> ApplyAsync(LogEntry entry, CancellationToken token)
    {
        if (!entry.TryGetPayload(out ReadOnlySequence<byte> payload) || payload.IsEmpty)
            return ApplyNoPayloadEntryAsync(entry.Index, token);

        return ApplyPayloadAsync(payload.ToArray(), entry.Index, token);
    }

    protected override ValueTask PersistAsync(IAsyncBinaryWriter writer, CancellationToken token) =>
        PersistSnapshotAsync(writer, token);

    protected override ValueTask RestoreAsync(FileInfo snapshotFile, CancellationToken token) =>
        RestoreSnapshotAsync(snapshotFile, token);

    internal async ValueTask<bool> ApplyPayloadAsync(
        ReadOnlyMemory<byte> payload,
        long logIndex,
        CancellationToken cancellationToken)
    {
        try
        {
            var command = BridgeReplicationSerializer.DeserializeCommand(payload);
            return await ApplyCommandAsync(command, logIndex, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            _logger.LogError(exception, "Failed to apply Raft bridge command at log index {LogIndex}.", logIndex);
            throw;
        }
    }

    internal async ValueTask<bool> ApplyNoPayloadEntryAsync(
        long logIndex,
        CancellationToken cancellationToken)
    {
        try
        {
            await _projection.AdvanceAppliedIndexAsync(logIndex, cancellationToken).ConfigureAwait(false);
            return false;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            _logger.LogError(exception, "Failed to advance Raft bridge projection at log index {LogIndex}.", logIndex);
            throw;
        }
    }

    internal async ValueTask<bool> ApplyCommandAsync(
        ReplicatedBridgeCommand command,
        long logIndex,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(command);
        command.Validate();
        await _projection.ApplyReplicatedCommandAsync(command, logIndex, cancellationToken).ConfigureAwait(false);
        return ++_commandsSinceSnapshot >= _snapshotFrequency;
    }

    internal async ValueTask PersistSnapshotAsync(IAsyncBinaryWriter writer, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(writer);
        var snapshot = await _projection.ExportSnapshotAsync(cancellationToken).ConfigureAwait(false);
        var payload = BridgeReplicationSerializer.SerializeSnapshot(snapshot);
        await writer.WriteAsync(payload, token: cancellationToken).ConfigureAwait(false);
        _commandsSinceSnapshot = 0;
    }

    internal async ValueTask RestoreSnapshotAsync(FileInfo snapshotFile, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(snapshotFile);
        var payload = await File.ReadAllBytesAsync(snapshotFile.FullName, cancellationToken).ConfigureAwait(false);
        var snapshot = BridgeReplicationSerializer.DeserializeSnapshot(payload);
        await _projection.RestoreSnapshotAsync(snapshot, cancellationToken).ConfigureAwait(false);
        _commandsSinceSnapshot = 0;
    }
}
