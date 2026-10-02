using System.Text.Json;
using System.Text.Json.Nodes;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using DrasiWake.Core.Domain;

namespace DrasiWake.Adapters.DrasiServer;

public sealed class DrasiServerClient(HttpClient httpClient)
{
    public async ValueTask<QuerySnapshot> ReadSnapshotAsync(
        QueryIdentity query,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);

        var data = await GetDataArrayAsync(BuildResultsUri(query), cancellationToken);
        var rows = data.EnumerateArray()
            .Select(row => JsonNode.Parse(row.GetRawText()))
            .ToArray();
        return new QuerySnapshot(query, rows);
    }

    public async IAsyncEnumerable<ChangeSignal> AttachAsync(
        QueryIdentity query,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);

        using var request = new HttpRequestMessage(HttpMethod.Get, BuildAttachUri(query));
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/event-stream"));
        using var response = await httpClient.SendAsync(
            request,
            HttpCompletionOption.ResponseHeadersRead,
            cancellationToken);
        response.EnsureSuccessStatusCode();

        if (!string.Equals(response.Content.Headers.ContentType?.MediaType, "text/event-stream", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidDataException("Drasi Server attach response must use text/event-stream.");
        }

        yield return new ChangeSignal(query, DateTimeOffset.UtcNow);

        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        await foreach (var signal in DrasiSseReader.ReadAsync(stream, query, cancellationToken)
                           .WithCancellation(cancellationToken))
        {
            yield return signal;
        }
    }

    public async ValueTask<IReadOnlyList<QueryIdentity>> EnumerateQueriesAsync(
        Uri server,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(server);
        if (!server.IsAbsoluteUri)
        {
            throw new ArgumentException("Server URI must be absolute.", nameof(server));
        }

        var queries = new List<QueryIdentity>();
        var instances = await GetDataArrayAsync(BuildApiUri(server, "/api/v1/instances"), cancellationToken);
        foreach (var instance in instances.EnumerateArray())
        {
            var instanceId = GetRequiredId(instance);
            var queryPath = $"/api/v1/instances/{Uri.EscapeDataString(instanceId)}/queries";
            var instanceQueries = await GetDataArrayAsync(BuildApiUri(server, queryPath), cancellationToken);
            foreach (var query in instanceQueries.EnumerateArray())
            {
                queries.Add(new QueryIdentity(server, instanceId, GetRequiredId(query)));
            }
        }

        return queries;
    }

    private async ValueTask<JsonElement> GetDataArrayAsync(Uri requestUri, CancellationToken cancellationToken)
    {
        using var response = await httpClient.GetAsync(
            requestUri,
            HttpCompletionOption.ResponseHeadersRead,
            cancellationToken);
        response.EnsureSuccessStatusCode();

        await using var content = await response.Content.ReadAsStreamAsync(cancellationToken);
        using var document = await JsonDocument.ParseAsync(content, cancellationToken: cancellationToken);
        var root = document.RootElement;
        if (!root.TryGetProperty("success", out var success) || !success.GetBoolean())
        {
            var error = root.TryGetProperty("error", out var errorElement)
                ? errorElement.GetString()
                : null;
            throw new InvalidDataException(error ?? "Drasi Server returned an unsuccessful response.");
        }

        if (!root.TryGetProperty("data", out var data) || data.ValueKind != JsonValueKind.Array)
        {
            throw new InvalidDataException("Drasi Server results response must contain a data array.");
        }

        return data.Clone();
    }

    private static string GetRequiredId(JsonElement item)
    {
        if (!item.TryGetProperty("id", out var id) || id.ValueKind != JsonValueKind.String ||
            string.IsNullOrWhiteSpace(id.GetString()))
        {
            throw new InvalidDataException("Drasi Server component list item must contain a non-empty id.");
        }

        return id.GetString()!;
    }

    private static Uri BuildResultsUri(QueryIdentity query)
    {
        var route = query.InstanceId is null
            ? $"/api/v1/queries/{Uri.EscapeDataString(query.QueryId)}/results"
            : $"/api/v1/instances/{Uri.EscapeDataString(query.InstanceId)}/queries/{Uri.EscapeDataString(query.QueryId)}/results";
        return BuildApiUri(query.Server, route);
    }

    private static Uri BuildAttachUri(QueryIdentity query)
    {
        var route = query.InstanceId is null
            ? $"/api/v1/queries/{Uri.EscapeDataString(query.QueryId)}/attach"
            : $"/api/v1/instances/{Uri.EscapeDataString(query.InstanceId)}/queries/{Uri.EscapeDataString(query.QueryId)}/attach";
        return BuildApiUri(query.Server, route);
    }

    private static Uri BuildApiUri(Uri server, string route)
    {
        var path = server.AbsolutePath.TrimEnd('/');
        return new Uri(
            $"{server.GetLeftPart(UriPartial.Authority)}{path}{route}",
            UriKind.Absolute);
    }
}