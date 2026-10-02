using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using DrasiWake.Core.Contracts;

namespace DrasiWake.Core.Domain;

public sealed class SessionIdentityResolver
{
    public SessionIdentityResolution Resolve(BridgeBinding binding, JsonObject row)
    {
        ArgumentNullException.ThrowIfNull(binding);
        ArgumentNullException.ThrowIfNull(row);

        if (string.Equals(binding.SessionScope, "singleton", StringComparison.OrdinalIgnoreCase))
        {
            return Success("singleton");
        }

        if (string.Equals(binding.SessionScope, "shared-canonical", StringComparison.OrdinalIgnoreCase))
        {
            return ResolveShared(binding, row);
        }

        if (!string.Equals(binding.SessionScope, "per-query", StringComparison.OrdinalIgnoreCase))
        {
            return Reject("identity.scope_invalid");
        }

        if (binding.QueryId is null)
        {
            return Reject("identity.query_missing");
        }

        var aggregateKey = ReadScalar(row, binding.AggregateKeyPointer, out var errorCode);
        if (errorCode is not null)
        {
            return Reject(errorCode);
        }

        var query = new QueryIdentity(binding.Server, binding.InstanceId, binding.QueryId);
        return Success(CreateIdentity(
            "query",
            query.Server.AbsoluteUri,
            query.InstanceId,
            query.QueryId,
            aggregateKey));
    }

    private static SessionIdentityResolution ResolveShared(BridgeBinding binding, JsonObject row)
    {
        var mapping = binding.CanonicalIdentity;
        if (mapping is null || string.IsNullOrWhiteSpace(mapping.OntologyContext) ||
            string.IsNullOrWhiteSpace(mapping.EntityType))
        {
            return Reject("identity.required");
        }

        var entityId = ReadScalar(row, mapping.EntityIdPointer, out var errorCode);
        if (errorCode is not null)
        {
            return Reject(errorCode);
        }

        var identity = new CanonicalIdentity(mapping.OntologyContext, mapping.EntityType, entityId!);
        return Success(CreateIdentity("shared", identity.OntologyContext, identity.EntityType, identity.EntityId));
    }

    private static string? ReadScalar(JsonObject row, string? pointer, out string? errorCode)
    {
        errorCode = null;
        if (!IsValidJsonPointer(pointer))
        {
            errorCode = "identity.pointer_invalid";
            return null;
        }

        JsonNode? current = row;
        if (pointer!.Length > 0)
        {
            foreach (var encodedSegment in pointer[1..].Split('/'))
            {
                var segment = encodedSegment.Replace("~1", "/", StringComparison.Ordinal)
                    .Replace("~0", "~", StringComparison.Ordinal);
                if (current is JsonObject jsonObject && jsonObject.TryGetPropertyValue(segment, out var property))
                {
                    current = property;
                }
                else if (current is JsonArray jsonArray &&
                         int.TryParse(segment, NumberStyles.None, CultureInfo.InvariantCulture, out var index) &&
                         index >= 0 && index < jsonArray.Count)
                {
                    current = jsonArray[index];
                }
                else
                {
                    errorCode = "identity.key_missing";
                    return null;
                }
            }
        }

        if (current is null)
        {
            errorCode = "identity.key_missing";
            return null;
        }

        using var document = JsonDocument.Parse(current.ToJsonString());
        var element = document.RootElement;
        var value = element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number or JsonValueKind.True or JsonValueKind.False => element.GetRawText(),
            _ => null
        };

        if (string.IsNullOrWhiteSpace(value))
        {
            errorCode = "identity.key_missing";
            return null;
        }

        return value;
    }

    private static bool IsValidJsonPointer(string? pointer)
    {
        if (pointer is null || (pointer.Length > 0 && pointer[0] != '/')) return false;

        for (var index = 0; index < pointer.Length; index++)
        {
            if (pointer[index] != '~') continue;
            if (index + 1 >= pointer.Length || (pointer[index + 1] != '0' && pointer[index + 1] != '1')) return false;
            index++;
        }

        return true;
    }

    private static string CreateIdentity(string scope, params string?[] parts)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(parts);
        var hash = SHA256.HashData(payload);
        return $"{scope}:{Convert.ToHexString(hash).ToLowerInvariant()}";
    }

    private static SessionIdentityResolution Success(string value) =>
        new(new SessionIdentity(value), null);

    private static SessionIdentityResolution Reject(string errorCode) =>
        new(null, errorCode);
}