using System.Text.Json;
using DrasiWake.Core.Pipeline;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace DrasiWake.Host.Health;

/// <summary>Minimal JSON writers for the unauthenticated probe endpoints.</summary>
public static class HealthResponseWriter
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public static Task WriteLivenessAsync(HttpContext httpContext, HealthReport report)
    {
        return WriteAsync(httpContext, new { status = report.Status.ToString() });
    }

    public static Task WriteReadinessAsync(HttpContext httpContext, HealthReport report)
    {
        foreach (var (name, entry) in report.Entries)
            BridgeTelemetry.RecordHealthCheck(name, entry.Status.ToString());
        return WriteAsync(httpContext, new
        {
            status = report.Status.ToString(),
            checks = report.Entries.Count
        });
    }

    private static async Task WriteAsync(HttpContext httpContext, object payload)
    {
        httpContext.Response.ContentType = "application/json; charset=utf-8";
        await httpContext.Response.WriteAsync(JsonSerializer.Serialize(payload, JsonOptions), httpContext.RequestAborted)
            .ConfigureAwait(false);
    }
}
