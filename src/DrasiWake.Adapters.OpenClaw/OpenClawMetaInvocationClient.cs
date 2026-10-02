using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Domain;

namespace DrasiWake.Adapters.OpenClaw;

public sealed class OpenClawMetaInvocationClient : IWakeSink
{
    private const string InvocationPath = "/api/integration/meta-invocations";
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly HttpClient httpClient;
    private readonly OpenClawOptions options;

    public OpenClawMetaInvocationClient(HttpClient httpClient, OpenClawOptions? options = null)
    {
        this.httpClient = httpClient;
        this.options = options ?? new OpenClawOptions();
        this.options.Validate();
        if (this.options.BaseAddress is not null)
            httpClient.BaseAddress ??= this.options.BaseAddress;
    }

    public async ValueTask<WakeAcceptance> InvokeAsync(WakeRequest request, CancellationToken cancellationToken)
    {
        using var response = await SendWithRetryAsync(request, cancellationToken);
        var result = await ReadInvocationResponseAsync(response, cancellationToken);
        if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
        {
            if (result.Status == "Uncertain" && result.InvocationId != Guid.Empty)
                throw new OpenClawInvocationUncertainException(result.InvocationId.ToString("D"), result.Error);
            throw new OpenClawIdempotencyConflictException(result.Error ?? "Gateway rejected a conflicting Idempotency-Key.");
        }
        if (!response.IsSuccessStatusCode && response.StatusCode != System.Net.HttpStatusCode.Conflict)
            response.EnsureSuccessStatusCode();

        return new WakeAcceptance(result.InvocationId.ToString("D"), result.CreatedAtUtc);
    }

    public ValueTask<WakeExecutionStatus?> GetStatusAsync(
        WakeRequest request,
        CancellationToken cancellationToken)
        => GetStatusFromReplayAsync(request, cancellationToken);

    private async ValueTask<WakeExecutionStatus?> GetStatusFromReplayAsync(
        WakeRequest request,
        CancellationToken cancellationToken)
    {
        using var response = await SendWithRetryAsync(request, cancellationToken);
        var result = await ReadInvocationResponseAsync(response, cancellationToken);
        if (response.StatusCode == System.Net.HttpStatusCode.Conflict && result.Status != "Uncertain")
            throw new OpenClawIdempotencyConflictException(result.Error ?? "Gateway rejected a conflicting Idempotency-Key.");
        if (!response.IsSuccessStatusCode && response.StatusCode != System.Net.HttpStatusCode.Conflict)
            response.EnsureSuccessStatusCode();

        return new WakeExecutionStatus(result.InvocationId.ToString("D"), result.Status, result.CreatedAtUtc);
    }

    private async Task<HttpResponseMessage> SendWithRetryAsync(WakeRequest request, CancellationToken cancellationToken)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                using var message = CreateRequest(request);
                var response = await httpClient.SendAsync(message, cancellationToken);
                if (!IsTransient(response.StatusCode) || attempt >= options.MaxRetryAttempts)
                    return response;
                response.Dispose();
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && attempt < options.MaxRetryAttempts)
            {
            }
            catch (HttpRequestException) when (attempt < options.MaxRetryAttempts)
            {
            }

            var delayMilliseconds = Math.Min(
                options.RetryBaseDelay.TotalMilliseconds * Math.Pow(2, attempt),
                options.MaxRetryDelay.TotalMilliseconds);
            await Task.Delay(TimeSpan.FromMilliseconds(delayMilliseconds), cancellationToken);
        }
    }

    private static bool IsTransient(System.Net.HttpStatusCode statusCode)
        => statusCode == System.Net.HttpStatusCode.RequestTimeout
            || (int)statusCode == 429
            || (int)statusCode >= 500;

    private static async Task<MetaInvocationResponse> ReadInvocationResponseAsync(
        HttpResponseMessage response,
        CancellationToken cancellationToken)
    {
        try
        {
            return await response.Content.ReadFromJsonAsync<MetaInvocationResponse>(JsonOptions, cancellationToken)
                ?? throw new JsonException("OpenClaw returned an empty MetaSkill invocation response.");
        }
        catch (JsonException) when (response.StatusCode == System.Net.HttpStatusCode.Conflict)
        {
            throw new OpenClawIdempotencyConflictException("Gateway rejected a conflicting Idempotency-Key.");
        }
    }

    private HttpRequestMessage CreateRequest(WakeRequest request)
    {
        var message = new HttpRequestMessage(HttpMethod.Post, InvocationPath)
        {
            Content = JsonContent.Create(new MetaInvocationRequest(
                request.Skill,
                request.Input.ToJsonString(),
                request.SessionId), options: JsonOptions)
        };
        message.Headers.TryAddWithoutValidation("Idempotency-Key", request.IdempotencyKey);
        if (!string.IsNullOrWhiteSpace(options.BearerToken))
            message.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", options.BearerToken);
        return message;
    }

    private sealed record MetaInvocationRequest(
        [property: JsonPropertyName("skill")] string Skill,
        [property: JsonPropertyName("input")] string Input,
        [property: JsonPropertyName("sessionId")] string SessionId);

    private sealed record MetaInvocationResponse(
        [property: JsonPropertyName("invocationId")] Guid InvocationId,
        [property: JsonPropertyName("status")] string Status,
        [property: JsonPropertyName("error")] string? Error,
        [property: JsonPropertyName("createdAtUtc")] DateTimeOffset CreatedAtUtc);
}