using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Contracts;
using DrasiWake.Core.Domain;
using DrasiWake.Core.Pipeline;
using DrasiWake.Host.Health;
using DrasiWake.Persistence.SonnetDB.Replication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace DrasiWake.Host.Admin;

/// <summary>Maps the admin API onto an already bound loopback/admin Kestrel branch.</summary>
public static class AdminApiEndpointExtensions
{
    public const string NotFoundCode = "outbox.not_found";
    public const string NotReplayableCode = "outbox.not_replayable";
    public const string NotLeaderCode = "admin.not_leader";
    public const string InvalidRequestCode = "admin.invalid_request";

    internal static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() }
    };

    public static void MapAdminApi(this IApplicationBuilder app)
    {
        var bearerToken = ResolveBearerToken(app);
        app.UseRouting();
        app.Use(next =>
        {
            var middleware = new AdminAuthenticationMiddleware(next, bearerToken);
            return middleware.InvokeAsync;
        });
        app.UseEndpoints(ConfigureEndpoints);
    }

    private static string? ResolveBearerToken(IApplicationBuilder app)
    {
        var settings = app.ApplicationServices.GetRequiredService<DrasiWakeHostSettings>();
        return settings.Cluster.Mode == RaftClusterMode.Cluster
            ? settings.Cluster.ManagementBearerToken
            : settings.Admin.BearerToken;
    }

    private static void ConfigureEndpoints(IEndpointRouteBuilder endpoints)
    {
        endpoints.MapHealthChecks("/healthz", new HealthCheckOptions
        {
            Predicate = registration => registration.Tags.Contains(HealthCheckTags.Liveness),
            AllowCachingResponses = false,
            ResponseWriter = HealthResponseWriter.WriteLivenessAsync
        });
        endpoints.MapHealthChecks("/readyz", new HealthCheckOptions
        {
            Predicate = registration => !registration.Tags.Contains(HealthCheckTags.Liveness),
            AllowCachingResponses = false,
            ResponseWriter = HealthResponseWriter.WriteReadinessAsync,
            ResultStatusCodes =
            {
                [HealthStatus.Healthy] = StatusCodes.Status200OK,
                [HealthStatus.Degraded] = StatusCodes.Status200OK,
                [HealthStatus.Unhealthy] = StatusCodes.Status503ServiceUnavailable
            }
        });

        endpoints.MapGet("/admin/outbox", async context =>
        {
            var query = await ParseListQueryAsync(context).ConfigureAwait(false);
            if (query is null)
                return;
            var projection = context.RequestServices.GetRequiredService<IRaftBridgeProjection>();
            var items = await projection.QueryOutboxAsync(
                query, context.RequestAborted).ConfigureAwait(false);
            await WriteListAsync(context, items).ConfigureAwait(false);
        });

        endpoints.MapGet("/admin/outbox/{id:guid}", async context =>
        {
            var id = ParseRouteId(context);
            var projection = context.RequestServices.GetRequiredService<IRaftBridgeProjection>();
            var item = await projection.ReadWakeAsync(id, context.RequestAborted).ConfigureAwait(false);
            if (item is null)
            {
                await WriteErrorAsync(context, StatusCodes.Status404NotFound,
                    NotFoundCode, $"Outbox item '{id}' was not found.").ConfigureAwait(false);
                return;
            }

            await WriteJsonAsync(context, StatusCodes.Status200OK, AdminDtosMapper.ToDetail(item))
                .ConfigureAwait(false);
        });

        endpoints.MapGet("/admin/dead-letters", async context =>
        {
            var query = await ParseListQueryAsync(context).ConfigureAwait(false);
            if (query is null)
                return;
            var projection = context.RequestServices.GetRequiredService<IRaftBridgeProjection>();
            var items = await projection.QueryOutboxAsync(
                query with { Statuses = [WakeOutboxStatus.DeadLetter] },
                context.RequestAborted).ConfigureAwait(false);
            await WriteListAsync(context, items).ConfigureAwait(false);
        });

        endpoints.MapPost("/admin/outbox/{id:guid}/replay", async context =>
        {
            var id = ParseRouteId(context);
            var reason = await TryReadReplayReasonAsync(context).ConfigureAwait(false);
            if (reason is null)
                return;

            var leadership = context.RequestServices.GetRequiredService<IRaftLeadership>();
            if (!leadership.IsLeader)
            {
                await WriteErrorAsync(context, StatusCodes.Status503ServiceUnavailable,
                    NotLeaderCode, "Replay requires the Raft leader; forward this request to the current leader.")
                    .ConfigureAwait(false);
                return;
            }

            var projection = context.RequestServices.GetRequiredService<IRaftBridgeProjection>();
            var existing = await projection.ReadWakeAsync(id, context.RequestAborted).ConfigureAwait(false);
            if (existing is null)
            {
                await WriteErrorAsync(context, StatusCodes.Status404NotFound,
                    NotFoundCode, $"Outbox item '{id}' was not found.").ConfigureAwait(false);
                return;
            }

            try
            {
                var store = context.RequestServices.GetRequiredService<IBridgeStore>();
                var replayed = await store.ReplayDeadLetterAsync(
                    id, context.RequestServices.GetRequiredService<TimeProvider>().GetUtcNow(),
                    reason, context.RequestAborted).ConfigureAwait(false);
                await WriteJsonAsync(context, StatusCodes.Status200OK, AdminDtosMapper.ToDetail(replayed))
                    .ConfigureAwait(false);
            }
            catch (InvalidOperationException exception)
            {
                await WriteErrorAsync(context, StatusCodes.Status409Conflict,
                    NotReplayableCode, exception.Message).ConfigureAwait(false);
            }
        });

        endpoints.MapGet("/admin/cluster", async context =>
        {
            var settings = context.RequestServices.GetRequiredService<DrasiWakeHostSettings>();
            var leadership = context.RequestServices.GetRequiredService<IRaftLeadership>();
            var projection = context.RequestServices.GetRequiredService<IRaftBridgeProjection>();
            var observation = leadership is DotNextRaftLeadership concrete
                ? concrete.ObserveNode()
                : new BridgeTelemetry.RaftNodeObservation(
                    leadership.IsLeader, leadership.HasQuorum, leadership.HasQuorum, 0, 0, 0);
            var members = context.RequestServices.GetService<IRaftMembershipRuntime>()?.Members
                ?? [new RaftMembershipMember(settings.Cluster.ListenAddress, leadership.IsLeader)];
            var lastAppliedIndex = await projection.GetLastAppliedIndexAsync(context.RequestAborted)
                .ConfigureAwait(false);
            var status = new AdminClusterStatus(
                settings.Cluster.NodeId,
                settings.Cluster.Mode.ToString(),
                observation.Role,
                leadership.HasQuorum,
                leadership.IsLeader && leadership.HasQuorum,
                lastAppliedIndex,
                members.Select(member => new AdminClusterMember(
                    member.Endpoint.AbsoluteUri, member.IsLeader)).ToArray());
            await WriteJsonAsync(context, StatusCodes.Status200OK, status).ConfigureAwait(false);
        });

        endpoints.MapGet("/admin/downstreams", async context =>
        {
            var probeService = context.RequestServices.GetRequiredService<DownstreamProbeService>();
            var snapshot = probeService.Snapshot();
            var status = snapshot.Keys
                .OrderBy(name => name, StringComparer.Ordinal)
                .Select(name => new AdminDownstreamStatus(
                    name,
                    snapshot[name].Address.ToString(),
                    snapshot[name].ProbedAtUtc,
                    snapshot[name].Reachable,
                    snapshot[name].LatencyMs,
                    snapshot[name].ErrorClass))
                .ToArray();
            await WriteJsonAsync(context, StatusCodes.Status200OK, status).ConfigureAwait(false);
        });

        endpoints.MapGet("/admin/health", async context =>
        {
            var healthService = context.RequestServices.GetRequiredService<HealthCheckService>();
            var report = await healthService.CheckHealthAsync(
                registration => !registration.Tags.Contains(HealthCheckTags.Liveness),
                context.RequestAborted).ConfigureAwait(false);
            var details = report.Entries
                .OrderBy(entry => entry.Key, StringComparer.Ordinal)
                .Select(entry => new AdminHealthCheckDetail(
                    entry.Key,
                    entry.Value.Status.ToString(),
                    entry.Value.Duration.TotalMilliseconds,
                    entry.Value.Description ?? entry.Value.Exception?.Message))
                .ToArray();
            await WriteJsonAsync(context, StatusCodes.Status200OK, new
            {
                status = report.Status.ToString(),
                checks = details
            }).ConfigureAwait(false);
        });
    }

    internal static async Task<OutboxQuery?> ParseListQueryAsync(HttpContext context)
    {
        var request = context.Request.Query;
        var statuses = new List<WakeOutboxStatus>();
        foreach (var statusValue in request["status"])
        {
            if (!Enum.TryParse<WakeOutboxStatus>(statusValue, ignoreCase: true, out var parsed))
            {
                await WriteErrorAsync(context, StatusCodes.Status400BadRequest,
                    InvalidRequestCode, $"Unknown outbox status '{statusValue}'.").ConfigureAwait(false);
                return null;
            }

            statuses.Add(parsed);
        }

        if (!int.TryParse(request["limit"], out var limit) || limit <= 0)
            limit = OutboxQuery.DefaultLimit;
        if (limit > OutboxQuery.MaxLimit)
            limit = OutboxQuery.MaxLimit;

        DateTimeOffset? cursorCreated = null;
        Guid? cursorId = null;
        if (request["cursor"].Count > 0)
        {
            if (!TryDecodeCursor(request["cursor"][0], out var decodedCreated, out var decodedId))
            {
                await WriteErrorAsync(context, StatusCodes.Status400BadRequest,
                    InvalidRequestCode, "The cursor value is invalid.").ConfigureAwait(false);
                return null;
            }

            cursorCreated = decodedCreated;
            cursorId = decodedId;
        }

        return new OutboxQuery(
            statuses.Count > 0 ? statuses : null,
            request["bindingId"].FirstOrDefault(),
            request["sessionId"].FirstOrDefault(),
            limit,
            cursorCreated,
            cursorId);
    }

    internal static string EncodeCursor(DateTimeOffset createdAtUtc, Guid id)
    {
        var payload = JsonSerializer.Serialize(new CursorPayload(createdAtUtc, id), JsonOptions);
        return Base64UrlEncode(Encoding.UTF8.GetBytes(payload));
    }

    internal static bool TryDecodeCursor(
        string? value,
        out DateTimeOffset createdAtUtc,
        out Guid id)
    {
        createdAtUtc = default;
        id = Guid.Empty;
        if (string.IsNullOrWhiteSpace(value) || value.Length > 512)
            return false;
        try
        {
            var payload = Encoding.UTF8.GetString(Base64UrlDecode(value));
            var parsed = JsonSerializer.Deserialize<CursorPayload>(payload, JsonOptions);
            if (parsed is null)
                return false;
            createdAtUtc = parsed.CreatedAtUtc;
            id = parsed.Id;
            return parsed.Id != Guid.Empty;
        }
        catch (FormatException)
        {
            return false;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static async Task<string?> TryReadReplayReasonAsync(HttpContext context)
    {
        string body;
        try
        {
            using var reader = new StreamReader(context.Request.Body, Encoding.UTF8);
            body = await reader.ReadToEndAsync(context.RequestAborted).ConfigureAwait(false);
        }
        catch (InvalidOperationException)
        {
            body = string.Empty;
        }

        if (string.IsNullOrWhiteSpace(body))
            return string.Empty;

        try
        {
            using var document = JsonDocument.Parse(body);
            if (document.RootElement.ValueKind == JsonValueKind.Object &&
                document.RootElement.TryGetProperty("reason", out var reason) &&
                reason.ValueKind == JsonValueKind.String)
            {
                var text = reason.GetString();
                if (text is { Length: > 500 })
                {
                    await WriteErrorAsync(context, StatusCodes.Status400BadRequest,
                        InvalidRequestCode, "The replay reason must not exceed 500 characters.")
                        .ConfigureAwait(false);
                    return null;
                }

                return text;
            }

            return string.Empty;
        }
        catch (JsonException)
        {
            await WriteErrorAsync(context, StatusCodes.Status400BadRequest,
                InvalidRequestCode, "The replay request body must be a JSON object or empty.")
                .ConfigureAwait(false);
            return null;
        }
    }

    private static Guid ParseRouteId(HttpContext context) =>
        Guid.Parse(context.Request.RouteValues["id"]!.ToString()!, CultureInfo.InvariantCulture);

    internal static async Task WriteListAsync(HttpContext context, IReadOnlyList<WakeOutboxItem> items)
    {
        // The cursor anchors the last returned row; clients pass it back for the next page.
        var nextCursor = items.Count > 0
            ? EncodeCursor(items[^1].CreatedAtUtc, items[^1].Id)
            : null;

        var payload = new
        {
            items = items.Select(AdminDtosMapper.ToListItem).ToArray(),
            nextCursor
        };
        await WriteJsonAsync(context, StatusCodes.Status200OK, payload).ConfigureAwait(false);
    }

    internal static async Task WriteErrorAsync(HttpContext context, int statusCode, string code, string message)
    {
        context.Response.StatusCode = statusCode;
        await WriteJsonAsync(context, statusCode, new
        {
            error = new { code, message }
        }).ConfigureAwait(false);
    }

    private static async Task WriteJsonAsync(HttpContext context, int statusCode, object payload)
    {
        context.Response.StatusCode = statusCode;
        context.Response.ContentType = "application/json; charset=utf-8";
        await context.Response.WriteAsync(
            JsonSerializer.Serialize(payload, JsonOptions), context.RequestAborted).ConfigureAwait(false);
    }

    private static string Base64UrlEncode(byte[] bytes) =>
        Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    private static byte[] Base64UrlDecode(string value)
    {
        var base64 = value.Replace('-', '+').Replace('_', '/');
        var remainder = base64.Length % 4;
        if (remainder == 2)
            base64 += "==";
        else if (remainder == 3)
            base64 += "=";
        return Convert.FromBase64String(base64);
    }

    private sealed record CursorPayload(DateTimeOffset CreatedAtUtc, Guid Id);
}
