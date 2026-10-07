using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using DrasiWake.Core.Domain;

namespace DrasiWake.Persistence.SonnetDB.Replication;

public enum BridgeCommandKind
{
    CreateOrUpdatePendingWake,
    RecordRejectedWake,
    SupersedePendingWakes,
    ClaimDispatchable,
    MarkAcceptedWithCheckpoint,
    UpdateExecutionStatus,
    MarkRetryScheduled,
    MarkDeadLetter,
    RecoverInterruptedDispatches,
    EnsureOpenClawTargets,
    SetConfigurationFingerprint,
    UpsertSubscription,
    RemoveSubscription,
    UpsertKeyMapping,
    RemoveKeyMapping
}

public sealed record ReplicatedBridgeCommand(
    int SchemaVersion,
    string CommandId,
    BridgeCommandKind Kind,
    JsonElement Payload)
{
    public const int CurrentSchemaVersion = 1;

    private static readonly JsonSerializerOptions SerializerOptions = CreateSerializerOptions();

    public static ReplicatedBridgeCommand Create<TPayload>(BridgeCommandKind kind, TPayload payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        if (!Enum.IsDefined(kind))
            throw new ArgumentOutOfRangeException(nameof(kind));

        var canonicalPayload = Canonicalize(JsonSerializer.SerializeToElement(payload, SerializerOptions));
        var commandId = ComputeCommandId(kind, canonicalPayload);
        return new ReplicatedBridgeCommand(
            CurrentSchemaVersion,
            commandId,
            kind,
            JsonDocument.Parse(canonicalPayload).RootElement.Clone());
    }

    public TPayload DeserializePayload<TPayload>()
    {
        Validate();
        return Payload.Deserialize<TPayload>(SerializerOptions)
            ?? throw new InvalidOperationException($"Command payload for '{Kind}' is null.");
    }

    public string Serialize()
    {
        Validate();
        return JsonSerializer.Serialize(new CommandDocument(SchemaVersion, CommandId, Kind, Payload), SerializerOptions);
    }

    public static ReplicatedBridgeCommand Deserialize(string json)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(json);
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        if (root.ValueKind != JsonValueKind.Object ||
            !root.TryGetProperty(nameof(SchemaVersion), out var schemaVersion) ||
            !root.TryGetProperty(nameof(CommandId), out var commandId) ||
            !root.TryGetProperty(nameof(Kind), out var kind) ||
            !root.TryGetProperty(nameof(Payload), out var payload))
        {
            throw new InvalidOperationException("Replicated bridge command envelope is incomplete.");
        }

        var command = new ReplicatedBridgeCommand(
            schemaVersion.GetInt32(),
            commandId.GetString() ?? string.Empty,
            kind.Deserialize<BridgeCommandKind>(SerializerOptions),
            payload.Clone());
        command.Validate();
        return command;
    }

    public void Validate()
    {
        if (SchemaVersion != CurrentSchemaVersion)
            throw new InvalidOperationException($"Unsupported replicated bridge command schema version '{SchemaVersion}'.");
        if (!Enum.IsDefined(Kind))
            throw new InvalidOperationException($"Unsupported replicated bridge command kind '{Kind}'.");
        if (Payload.ValueKind is JsonValueKind.Undefined or JsonValueKind.Null)
            throw new InvalidOperationException("Replicated bridge command payload is required.");

        var canonicalPayload = Canonicalize(Payload);
        if (!string.Equals(CommandId, ComputeCommandId(Kind, canonicalPayload), StringComparison.Ordinal))
            throw new InvalidOperationException("Replicated bridge command ID does not match its kind and payload.");
    }

    private static string ComputeCommandId(BridgeCommandKind kind, string canonicalPayload)
    {
        var bytes = Encoding.UTF8.GetBytes($"{kind}\n{canonicalPayload}");
        return Convert.ToHexStringLower(SHA256.HashData(bytes));
    }

    private static string Canonicalize(JsonElement element)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
            WriteCanonical(writer, element);
        return Encoding.UTF8.GetString(stream.ToArray());
    }

    private static void WriteCanonical(Utf8JsonWriter writer, JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                writer.WriteStartObject();
                foreach (var property in element.EnumerateObject().OrderBy(property => property.Name, StringComparer.Ordinal))
                {
                    writer.WritePropertyName(property.Name);
                    WriteCanonical(writer, property.Value);
                }
                writer.WriteEndObject();
                break;
            case JsonValueKind.Array:
                writer.WriteStartArray();
                foreach (var item in element.EnumerateArray())
                    WriteCanonical(writer, item);
                writer.WriteEndArray();
                break;
            default:
                element.WriteTo(writer);
                break;
        }
    }

    private static JsonSerializerOptions CreateSerializerOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = false,
            WriteIndented = false
        };
        options.Converters.Add(new StringSetJsonConverter());
        options.Converters.Add(new JsonStringEnumConverter());
        return options;
    }

    private sealed class StringSetJsonConverter : JsonConverter<IReadOnlySet<string>>
    {
        public override IReadOnlySet<string> Read(
            ref Utf8JsonReader reader,
            Type typeToConvert,
            JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.StartArray)
                throw new JsonException("A string set must be encoded as a JSON array.");
            var values = new HashSet<string>(StringComparer.Ordinal);
            while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
            {
                if (reader.TokenType != JsonTokenType.String || reader.GetString() is not { } value)
                    throw new JsonException("A string set can contain only string values.");
                values.Add(value);
            }
            if (reader.TokenType != JsonTokenType.EndArray)
                throw new JsonException("The string set JSON array is incomplete.");
            return values;
        }

        public override void Write(
            Utf8JsonWriter writer,
            IReadOnlySet<string> value,
            JsonSerializerOptions options)
        {
            writer.WriteStartArray();
            foreach (var item in value.OrderBy(item => item, StringComparer.Ordinal))
                writer.WriteStringValue(item);
            writer.WriteEndArray();
        }
    }

    private sealed record CommandDocument(
        int SchemaVersion,
        string CommandId,
        BridgeCommandKind Kind,
        JsonElement Payload);
}

public sealed record CreateOrUpdatePendingWakePayload(WakeOutboxItem Item);
public sealed record RecordRejectedWakePayload(WakeOutboxItem Item, string ReasonCode);
public sealed record SupersedePendingWakesPayload(string BindingId, string SessionId, string CurrentFingerprint);
public sealed record ClaimDispatchablePayload(
    IReadOnlyList<Guid> OutboxIds,
    DateTimeOffset ClaimedAtUtc,
    Guid ClaimId);
public sealed record MarkAcceptedWithCheckpointPayload(
    Guid OutboxId,
    WakeAcceptance Acceptance,
    SnapshotCheckpoint Checkpoint);
public sealed record UpdateExecutionStatusPayload(WakeExecutionStatus Status);
public sealed record MarkRetryScheduledPayload(
    Guid OutboxId,
    int AttemptCount,
    DateTimeOffset NextAttemptUtc,
    string ReasonCode);
public sealed record MarkDeadLetterPayload(Guid OutboxId, string ReasonCode);
public sealed record RecoverInterruptedDispatchesPayload(DateTimeOffset RecoveredAtUtc);
public sealed record EnsureOpenClawTargetsPayload(
    IReadOnlyDictionary<string, string> TargetByBindingId,
    IReadOnlySet<string> ConfiguredTargetNames,
    IReadOnlyDictionary<string, TimeSpan> MaximumRetryAgeByBindingId,
    IReadOnlyDictionary<string, TimeSpan> IdempotencyRetentionByTarget);
public sealed record SetConfigurationFingerprintPayload(string ConfigurationFingerprint);
public sealed record UpsertSubscriptionPayload(SubscriptionSnapshot Subscription);
public sealed record RemoveSubscriptionPayload(string QueryKey);
public sealed record UpsertKeyMappingPayload(KeyMappingSnapshot Mapping);
public sealed record RemoveKeyMappingPayload(string ContractScope, string CanonicalIdentity);
