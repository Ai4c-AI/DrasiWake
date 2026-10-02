using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace DrasiWake.Core.Domain;

public static class SnapshotFingerprint
{
    private static readonly IComparer<byte[]> Utf8Comparer = Comparer<byte[]>.Create(
        static (left, right) => left.AsSpan().SequenceCompareTo(right));

    public static string Compute(QuerySnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        var rows = snapshot.Rows
            .Select(Canonicalize)
            .Select(row => row?.ToJsonString() ?? "null")
            .Select(json => (Json: json, Utf8: Encoding.UTF8.GetBytes(json)))
            .OrderBy(item => item.Utf8, Utf8Comparer)
            .Select(item => item.Json);
        var canonicalJson = $"[{string.Join(',', rows)}]";
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(canonicalJson));
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    private static JsonNode? Canonicalize(JsonNode? node)
    {
        return node switch
        {
            JsonObject jsonObject => CanonicalizeObject(jsonObject),
            JsonArray jsonArray => CanonicalizeArray(jsonArray),
            _ => node?.DeepClone()
        };
    }

    private static JsonObject CanonicalizeObject(JsonObject source)
    {
        var result = new JsonObject();
        foreach (var property in source.OrderBy(property => property.Key, StringComparer.Ordinal))
        {
            result.Add(property.Key, Canonicalize(property.Value));
        }

        return result;
    }

    private static JsonArray CanonicalizeArray(JsonArray source)
    {
        var result = new JsonArray();
        foreach (var item in source)
        {
            result.Add(Canonicalize(item));
        }

        return result;
    }
}