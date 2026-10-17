using System.Collections.Concurrent;
using System.Diagnostics;
using DrasiWake.Core.Pipeline;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;

namespace DrasiWake.Host.Health;

public sealed record DownstreamProbeResult(
    string Downstream,
    Uri Address,
    DateTimeOffset ProbedAtUtc,
    bool Reachable,
    long LatencyMs,
    string? ErrorClass);

/// <summary>
/// Background connectivity prober for the downstream services (Drasi server and every OpenClaw
/// target). Results are cached in-process and surfaced through readiness checks and the admin
/// API. Probes never carry credentials: any HTTP response — including 401 — means "reachable".
/// </summary>
public sealed class DownstreamProbeService(
    DrasiWakeHostSettings settings,
    IHttpClientFactory httpClientFactory,
    TimeProvider timeProvider) : BackgroundService
{
    public const string HttpClientName = "DrasiWake.DownstreamProbe";
    public const string DrasiDownstreamName = "drasi";
    internal static readonly TimeSpan ProbeTimeout = TimeSpan.FromSeconds(2);

    private readonly DrasiWakeHostSettings _settings = settings ?? throw new ArgumentNullException(nameof(settings));
    private readonly IHttpClientFactory _httpClientFactory = httpClientFactory ?? throw new ArgumentNullException(nameof(httpClientFactory));
    private readonly TimeProvider _timeProvider = timeProvider ?? TimeProvider.System;
    private readonly ConcurrentDictionary<string, DownstreamProbeResult> results = new(StringComparer.Ordinal);

    public IReadOnlyDictionary<string, DownstreamProbeResult> Snapshot()
    {
        var snapshot = new Dictionary<string, DownstreamProbeResult>(results.Count, StringComparer.Ordinal);
        foreach (var (name, result) in results)
            snapshot[name] = result;
        return snapshot;
    }

    public DownstreamProbeResult? GetResult(string downstream) =>
        results.TryGetValue(downstream, out var result) ? result : null;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var targets = BuildTargets(_settings);
        while (!stoppingToken.IsCancellationRequested)
        {
            foreach (var (name, address) in targets)
            {
                if (stoppingToken.IsCancellationRequested)
                    return;
                await ProbeAsync(name, address, stoppingToken).ConfigureAwait(false);
            }

            try
            {
                await Task.Delay(_settings.Admin.DownstreamProbeInterval, _timeProvider, stoppingToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
        }
    }

    internal async Task<DownstreamProbeResult> ProbeAsync(
        string downstream,
        Uri address,
        CancellationToken cancellationToken)
    {
        using var activity = BridgeTelemetry.StartDownstreamProbe(downstream);
        var client = _httpClientFactory.CreateClient(HttpClientName);
        var startedAt = Stopwatch.GetTimestamp();
        var errorClass = await ProbeForErrorAsync(client, address, cancellationToken).ConfigureAwait(false);
        var latencyMs = (long)Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds;
        var probedAtUtc = _timeProvider.GetUtcNow();
        // Any HTTP response proves network reachability; only transport-level failures
        // (timeout / connection) make the downstream unreachable. 4xx/5xx keep an error
        // class for the readiness checks to surface.
        var reachable = errorClass is null || errorClass.StartsWith("http_", StringComparison.Ordinal);
        if (!reachable)
            BridgeTelemetry.RecordDownstreamProbeFailure(downstream, errorClass!);
        activity?.SetTag("probe.reachable", reachable);
        activity?.SetTag("probe.error.class", errorClass);
        var result = new DownstreamProbeResult(downstream, address, probedAtUtc, reachable, latencyMs, errorClass);
        results[downstream] = result;
        return result;
    }

    private async Task<string?> ProbeForErrorAsync(
        HttpClient client,
        Uri address,
        CancellationToken cancellationToken)
    {
        using var probeCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        probeCancellation.CancelAfter(ProbeTimeout);
        try
        {
            using var response = await client.GetAsync(address, HttpCompletionOption.ResponseHeadersRead, probeCancellation.Token)
                .ConfigureAwait(false);
            var statusCode = (int)response.StatusCode;
            if (statusCode is >= 200 and < 300 || statusCode == StatusCodes.Status401Unauthorized)
                return null;
            return statusCode < 500 ? "http_4xx" : "http_5xx";
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return "timeout";
        }
        catch (HttpRequestException)
        {
            return "connection";
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return "connection";
        }
    }

    internal static List<(string Downstream, Uri Address)> BuildTargets(DrasiWakeHostSettings settings)
    {
        var targets = new List<(string, Uri)> { (DrasiDownstreamName, new Uri(settings.Drasi.ServerUri, "health")) };
        foreach (var (targetName, targetOptions) in settings.OpenClawTargets)
            targets.Add((targetName, new Uri(targetOptions.BaseAddress, "api/integration/status")));
        return targets;
    }
}
