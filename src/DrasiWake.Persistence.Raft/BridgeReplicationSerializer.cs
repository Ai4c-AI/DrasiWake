using System.Text.Json;
using System.Text.Json.Serialization;
using DrasiWake.Persistence.SonnetDB.Replication;

namespace DrasiWake.Persistence.Raft;

internal static class BridgeReplicationSerializer
{
    private static readonly JsonSerializerOptions Options = CreateOptions();

    internal static byte[] SerializeCommand(ReplicatedBridgeCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);
        command.Validate();
        return JsonSerializer.SerializeToUtf8Bytes(command, Options);
    }

    internal static ReplicatedBridgeCommand DeserializeCommand(ReadOnlyMemory<byte> payload)
    {
        var command = JsonSerializer.Deserialize<ReplicatedBridgeCommand>(payload.Span, Options)
            ?? throw new InvalidDataException("Replicated bridge command payload is empty.");
        command.Validate();
        return command;
    }

    internal static byte[] SerializeSnapshot(BridgeStoreSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        return JsonSerializer.SerializeToUtf8Bytes(snapshot, Options);
    }

    internal static BridgeStoreSnapshot DeserializeSnapshot(ReadOnlyMemory<byte> payload)
    {
        var snapshot = JsonSerializer.Deserialize<BridgeStoreSnapshot>(payload.Span, Options)
            ?? throw new InvalidDataException("Bridge projection snapshot is empty.");
        if (snapshot.SchemaVersion != BridgeStoreSnapshot.CurrentSchemaVersion)
            throw new InvalidDataException($"Unsupported bridge projection snapshot version '{snapshot.SchemaVersion}'.");
        return snapshot;
    }

    private static JsonSerializerOptions CreateOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = false,
            WriteIndented = false
        };
        options.Converters.Add(new JsonStringEnumConverter());
        options.MakeReadOnly(populateMissingResolver: true);
        return options;
    }
}
