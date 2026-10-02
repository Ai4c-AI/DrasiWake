using System.Text.Json.Nodes;
using DrasiWake.Core.Domain;

namespace DrasiWake.Core.Tests.Domain;

public sealed class SnapshotFingerprintTests
{
    [Fact]
    public void Fingerprint_ignores_property_and_result_row_order()
    {
        var first = Hash("[{\"id\":2,\"meta\":{\"b\":1,\"a\":2}},{\"id\":1}]");
        var second = Hash("[{\"id\":1},{\"meta\":{\"a\":2,\"b\":1},\"id\":2}]");

        Assert.Equal(first, second);
    }

    [Fact]
    public void Fingerprint_changes_when_fact_changes() =>
        Assert.NotEqual(Hash("[{\"id\":1}]"), Hash("[{\"id\":2}]"));

    private static string Hash(string json)
    {
        var rows = JsonNode.Parse(json)!.AsArray().Select(row => row?.DeepClone()).ToArray();
        var query = new QueryIdentity(new Uri("http://localhost:8080"), null, "orders");
        return SnapshotFingerprint.Compute(new QuerySnapshot(query, rows));
    }
}