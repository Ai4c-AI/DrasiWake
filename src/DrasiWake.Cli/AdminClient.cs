using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using DrasiWake.Core.Contracts;

namespace DrasiWake.Cli;

/// <summary>Thin HTTP client over the DrasiWake admin API. Exposes raw JSON plus typed errors.</summary>
public sealed class AdminClient
{
    public sealed class AdminApiException(string code, string message, int statusCode)
        : Exception($"[{code}] {message}")
    {
        public string Code { get; } = code;
        public int StatusCode { get; } = statusCode;
    }

    public static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() }
    };

    private readonly HttpClient httpClient;

    public AdminClient(Uri baseUrl, string? bearerToken)
    {
        httpClient = new HttpClient { BaseAddress = baseUrl, Timeout = TimeSpan.FromSeconds(15) };
        if (!string.IsNullOrEmpty(bearerToken))
            httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Authorization", $"Bearer {bearerToken}");
    }

    public async Task<string> GetStringAsync(string requestUri, CancellationToken cancellationToken)
    {
        using var response = await httpClient.GetAsync(requestUri, cancellationToken).ConfigureAwait(false);
        return await ReadAsync(response, cancellationToken).ConfigureAwait(false);
    }

    public async Task<string> PostAsync(string requestUri, string jsonBody, CancellationToken cancellationToken)
    {
        using var content = new StringContent(jsonBody, Encoding.UTF8, "application/json");
        using var response = await httpClient.PostAsync(requestUri, content, cancellationToken).ConfigureAwait(false);
        return await ReadAsync(response, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<string> ReadAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        if (response.IsSuccessStatusCode)
            return body;

        string code = $"http_{(int)response.StatusCode}";
        string message = body;
        try
        {
            using var document = JsonDocument.Parse(body);
            if (document.RootElement.TryGetProperty("error", out var error) &&
                error.TryGetProperty("code", out var errorCode))
            {
                code = errorCode.GetString() ?? code;
                if (error.TryGetProperty("message", out var errorMessage))
                    message = errorMessage.GetString() ?? message;
            }
        }
        catch (JsonException)
        {
        }

        throw new AdminApiException(code, message, (int)response.StatusCode);
    }

    // Typed helpers over the shared DTO contracts (used for table rendering).

    public async Task<OutboxListResponse> ListOutboxAsync(
        IReadOnlyList<string> statuses,
        string? bindingId,
        string? sessionId,
        int limit,
        string? cursor,
        CancellationToken cancellationToken)
    {
        var query = BuildListQuery(statuses, bindingId, sessionId, limit, cursor);
        var json = await GetStringAsync($"/admin/outbox{query}", cancellationToken).ConfigureAwait(false);
        return JsonSerializer.Deserialize<OutboxListResponse>(json, JsonOptions)
            ?? throw new AdminApiException("admin.invalid_response", "The outbox list response was empty.", 200);
    }

    public async Task<OutboxListResponse> ListDeadLettersAsync(
        int limit,
        string? cursor,
        CancellationToken cancellationToken)
    {
        var query = BuildListQuery([], null, null, limit, cursor);
        var json = await GetStringAsync($"/admin/dead-letters{query}", cancellationToken).ConfigureAwait(false);
        return JsonSerializer.Deserialize<OutboxListResponse>(json, JsonOptions)
            ?? throw new AdminApiException("admin.invalid_response", "The dead-letter response was empty.", 200);
    }

    public async Task<string> GetOutboxAsync(Guid id, CancellationToken cancellationToken) =>
        await GetStringAsync($"/admin/outbox/{id}", cancellationToken).ConfigureAwait(false);

    public async Task<string> ReplayAsync(Guid id, string? reason, CancellationToken cancellationToken) =>
        await PostAsync(
            $"/admin/outbox/{id}/replay",
            JsonSerializer.Serialize(new OutboxReplayRequest(reason), JsonOptions),
            cancellationToken).ConfigureAwait(false);

    public async Task<string> GetClusterAsync(CancellationToken cancellationToken) =>
        await GetStringAsync("/admin/cluster", cancellationToken).ConfigureAwait(false);

    public async Task<string> GetDownstreamsAsync(CancellationToken cancellationToken) =>
        await GetStringAsync("/admin/downstreams", cancellationToken).ConfigureAwait(false);

    public async Task<string> GetHealthAsync(CancellationToken cancellationToken) =>
        await GetStringAsync("/admin/health", cancellationToken).ConfigureAwait(false);

    internal static string BuildListQuery(
        IReadOnlyList<string> statuses,
        string? bindingId,
        string? sessionId,
        int limit,
        string? cursor)
    {
        var parameters = new List<string>();
        foreach (var status in statuses)
            parameters.Add($"status={Uri.EscapeDataString(status)}");
        if (!string.IsNullOrEmpty(bindingId))
            parameters.Add($"bindingId={Uri.EscapeDataString(bindingId)}");
        if (!string.IsNullOrEmpty(sessionId))
            parameters.Add($"sessionId={Uri.EscapeDataString(sessionId)}");
        parameters.Add($"limit={limit}");
        if (!string.IsNullOrEmpty(cursor))
            parameters.Add($"cursor={Uri.EscapeDataString(cursor)}");
        return "?" + string.Join("&", parameters);
    }
}

/// <summary>Admin list envelope: rows plus the keyset cursor for the next page.</summary>
public sealed record OutboxListResponse(OutboxListItem[] Items, string? NextCursor);
