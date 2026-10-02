using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using DrasiWake.Core.Contracts;
using DrasiWake.Core.Domain;
using Json.Schema;

namespace DrasiWake.Core.Pipeline;

public sealed class WakePayloadRenderer
{
    public (JsonObject Input, string Fingerprint) Render(
        BridgeBinding binding,
        QueryIdentity query,
        string sessionId,
        IEnumerable<JsonNode?> rows)
    {
        var canonicalRows = SnapshotFingerprint.CanonicalizeRows(rows);
        var snapshotFingerprint = SnapshotFingerprint.Compute(new QuerySnapshot(query, canonicalRows));
        JsonSchema schema;
        try
        {
            schema = JsonSchema.FromText(File.ReadAllText(binding.Contract.FactSchemaPath));
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or JsonException)
        {
            throw new WakePayloadRejectedException("payload.schema_unavailable", exception.Message);
        }

        foreach (var row in canonicalRows)
        {
            if (row is not JsonObject)
                throw new WakePayloadRejectedException("payload.fact_invalid", "Each fact must be a JSON object.");

            using var document = JsonDocument.Parse(row.ToJsonString());
            if (!schema.Evaluate(document.RootElement).IsValid)
                throw new WakePayloadRejectedException("payload.schema_invalid", "A fact does not satisfy the binding schema.");
        }

        var facts = new JsonArray(canonicalRows.Select(row => row?.DeepClone()).ToArray());
        var input = new JsonObject
        {
            ["bindingId"] = binding.Id,
            ["contractVersion"] = binding.Contract.Version,
            ["snapshotFingerprint"] = snapshotFingerprint,
            ["sessionId"] = sessionId,
            ["facts"] = facts
        };
        if (Encoding.UTF8.GetByteCount(input.ToJsonString()) > binding.MaxPayloadBytes)
            throw new WakePayloadRejectedException("payload.size_exceeded", "Rendered wake payload exceeds the binding limit.");

        return (input, snapshotFingerprint);
    }
}