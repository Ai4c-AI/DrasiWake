using System.Text.Json.Nodes;

namespace DrasiWake.Core.Domain;

public sealed record QuerySnapshot(QueryIdentity Query, IReadOnlyList<JsonNode?> Rows);