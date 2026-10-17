using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using DrasiWake.Host;
using DrasiWake.Host.Health;
using DrasiWake.LocalEnvironment;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;

namespace DrasiWake.IntegrationTests;

public sealed class HealthEndpointTests
{
    [Fact]
    public async Task Downstream_probe_classifies_reachability_without_credentials()
    {
        var handler = new StubDownstreamHandler();
        using var httpClient = new HttpClient(handler);
        var settings = CreateSettings(new Dictionary<string, string?>
        {
            ["DrasiWake:Drasi:ServerUri"] = "http://127.0.0.1:65101/",
            ["DrasiWake:OpenClaw:Targets:sample-gateway:BaseAddress"] = "http://127.0.0.1:65102/",
            ["DrasiWake:OpenClaw:Targets:sample-gateway:GatewayIdempotencyRetention"] = "30.00:00:00"
        });
        var probeService = new DownstreamProbeService(
            settings,
            new SingleClientHttpClientFactory(httpClient),
            TimeProvider.System);

        var healthy = await probeService.ProbeAsync(
            "ok", new Uri("http://stub.test/ok"), TestContext.Current.CancellationToken);
        var authRequired = await probeService.ProbeAsync(
            "auth", new Uri("http://stub.test/auth-required"), TestContext.Current.CancellationToken);
        var serverError = await probeService.ProbeAsync(
            "broken", new Uri("http://stub.test/server-error"), TestContext.Current.CancellationToken);
        var notFound = await probeService.ProbeAsync(
            "missing", new Uri("http://stub.test/not-found"), TestContext.Current.CancellationToken);
        var refused = await probeService.ProbeAsync(
            "refused", new Uri("http://stub.test/refused"), TestContext.Current.CancellationToken);

        Assert.True(healthy.Reachable);
        Assert.Null(healthy.ErrorClass);
        Assert.True(authRequired.Reachable, "A 401 response means the service is reachable.");
        Assert.Null(authRequired.ErrorClass);
        Assert.True(serverError.Reachable);
        Assert.Equal("http_5xx", serverError.ErrorClass);
        Assert.True(notFound.Reachable);
        Assert.Equal("http_4xx", notFound.ErrorClass);
        Assert.False(refused.Reachable);
        Assert.Equal("connection", refused.ErrorClass);
    }

    [Fact]
    public async Task Probe_service_refreshes_cached_results_on_schedule()
    {
        var handler = new StubDownstreamHandler();
        using var httpClient = new HttpClient(handler);
        var settings = CreateSettings(new Dictionary<string, string?>
        {
            ["DrasiWake:Admin:DownstreamProbeInterval"] = "00:00:00.100"
        });
        var probeService = new DownstreamProbeService(
            settings,
            new SingleClientHttpClientFactory(httpClient),
            TimeProvider.System);

        await probeService.StartAsync(TestContext.Current.CancellationToken);
        try
        {
            var deadline = DateTimeOffset.UtcNow.AddSeconds(5);
            while (probeService.GetResult(DownstreamProbeService.DrasiDownstreamName) is null)
            {
                if (DateTimeOffset.UtcNow >= deadline)
                    throw new TimeoutException("The drasi probe result was not cached in time.");
                await Task.Delay(TimeSpan.FromMilliseconds(20), TestContext.Current.CancellationToken);
            }

            var snapshot = probeService.Snapshot();
            Assert.Equal(2, snapshot.Count);
            Assert.True(snapshot.ContainsKey(DownstreamProbeService.DrasiDownstreamName));
            Assert.True(snapshot.ContainsKey("sample-gateway"));
            Assert.Equal(
                new Uri("http://127.0.0.1:65101/health"),
                snapshot[DownstreamProbeService.DrasiDownstreamName].Address);
        }
        finally
        {
            await probeService.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task OpenClaw_readiness_checks_follow_the_probe_cache()
    {
        var handler = new StubDownstreamHandler();
        using var httpClient = new HttpClient(handler);
        var settings = CreateSettings(new Dictionary<string, string?>
        {
            ["DrasiWake:OpenClaw:Targets:sample-gateway:BaseAddress"] = "http://127.0.0.1:65102/",
            ["DrasiWake:OpenClaw:Targets:sample-gateway:GatewayIdempotencyRetention"] = "30.00:00:00"
        });
        var probeService = new DownstreamProbeService(
            settings,
            new SingleClientHttpClientFactory(httpClient),
            TimeProvider.System);

        var targetCheck = new OpenClawTargetHealthCheck("sample-gateway", probeService);
        var aggregateCheck = new OpenClawAggregateHealthCheck(settings, probeService);

        var pending = await targetCheck.CheckHealthAsync(new HealthCheckContext());
        Assert.Equal(HealthStatus.Degraded, pending.Status);

        await probeService.ProbeAsync(
            "sample-gateway", new Uri("http://stub.test/refused"), TestContext.Current.CancellationToken);

        var degraded = await targetCheck.CheckHealthAsync(new HealthCheckContext());
        Assert.Equal(HealthStatus.Degraded, degraded.Status);

        var aggregatedWhileDown = await aggregateCheck.CheckHealthAsync(new HealthCheckContext());
        Assert.Equal(HealthStatus.Unhealthy, aggregatedWhileDown.Status);

        await probeService.ProbeAsync(
            "sample-gateway", new Uri("http://stub.test/ok"), TestContext.Current.CancellationToken);
        var healthy = await targetCheck.CheckHealthAsync(new HealthCheckContext());
        Assert.Equal(HealthStatus.Healthy, healthy.Status);
        var aggregatedWhileUp = await aggregateCheck.CheckHealthAsync(new HealthCheckContext());
        Assert.Equal(HealthStatus.Healthy, aggregatedWhileUp.Status);
    }

    [Fact]
    public void Admin_settings_reject_raft_port_collision_and_non_root_paths()
    {
        var collision = CreateSettings(new Dictionary<string, string?>
        {
            ["DrasiWake:Admin:ApiAddress"] = "http://127.0.0.1:50051/"
        });
        var collisionError = Assert.Throws<InvalidOperationException>(collision.Validate);
        Assert.Contains("DrasiWake:Admin:ApiAddress", collisionError.Message, StringComparison.Ordinal);

        var nonRootError = Assert.Throws<InvalidOperationException>(() => CreateSettings(new Dictionary<string, string?>
        {
            ["DrasiWake:Admin:ApiAddress"] = "http://127.0.0.1:5103/admin"
        }));
        Assert.Contains("DrasiWake:Admin:ApiAddress", nonRootError.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Admin_listener_serves_liveness_and_readiness_endpoints()
    {
        var (root, registryPath) = await CreateSampleRegistryAsync();
        var adminPort = ReserveThenClosePort();
        var raftPort = ReserveThenClosePort();
        var configuration = CreateConfiguration(new Dictionary<string, string?>
        {
            ["DrasiWake:Registry:Path"] = registryPath,
            ["DrasiWake:Database:Path"] = Path.Combine(root, "database"),
            ["DrasiWake:Cluster:ListenAddress"] = $"http://127.0.0.1:{raftPort}/",
            ["DrasiWake:Cluster:InitialMembers:0"] = $"http://127.0.0.1:{raftPort}/",
            ["DrasiWake:Cluster:RaftDataPath"] = Path.Combine(root, "raft"),
            ["DrasiWake:Admin:ApiAddress"] = $"http://127.0.0.1:{adminPort}/",
            ["DrasiWake:Admin:DownstreamProbeInterval"] = "00:01:00"
        });
        using var host = DrasiWakeHostBuilder.CreateHost(configuration);

        try
        {
            await host.StartAsync(TestContext.Current.CancellationToken);
            using var client = new HttpClient
            {
                BaseAddress = new Uri($"http://127.0.0.1:{adminPort}/"),
                Timeout = TimeSpan.FromSeconds(5)
            };

            using var liveness = await client.GetAsync("/healthz", TestContext.Current.CancellationToken);
            Assert.Equal(HttpStatusCode.OK, liveness.StatusCode);
            var livenessBody = await liveness.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
            Assert.Contains("\"status\":\"Healthy\"", livenessBody, StringComparison.Ordinal);

            using var readiness = await client.GetAsync("/readyz", TestContext.Current.CancellationToken);
            Assert.Equal(HttpStatusCode.ServiceUnavailable, readiness.StatusCode);
            var readinessBody = await readiness.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
            using var document = JsonDocument.Parse(readinessBody);
            Assert.Equal("Unhealthy", document.RootElement.GetProperty("status").GetString());
            Assert.True(document.RootElement.GetProperty("checks").GetInt32() >= 6);
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
            host.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }

    private static DrasiWakeHostSettings CreateSettings(IReadOnlyDictionary<string, string?> overrides)
    {
        var values = new Dictionary<string, string?>
        {
            ["DrasiWake:Drasi:ServerUri"] = "http://127.0.0.1:65101/",
            ["DrasiWake:OpenClaw:Targets:sample-gateway:BaseAddress"] = "http://127.0.0.1:65102/",
            ["DrasiWake:OpenClaw:Targets:sample-gateway:GatewayIdempotencyRetention"] = "30.00:00:00",
            ["DrasiWake:Database:Path"] = Path.Combine(Path.GetTempPath(), $"DrasiWake-health-{Guid.NewGuid():N}"),
            ["DrasiWake:Registry:Path"] = Path.Combine(Path.GetTempPath(), $"DrasiWake-health-{Guid.NewGuid():N}.yaml")
        };
        foreach (var entry in overrides)
            values[entry.Key] = entry.Value;
        return DrasiWakeHostSettings.FromConfiguration(
            new ConfigurationBuilder().AddInMemoryCollection(values).Build());
    }

    private static IConfiguration CreateConfiguration(IReadOnlyDictionary<string, string?> overrides)
    {
        var values = new Dictionary<string, string?>
        {
            ["DrasiWake:Drasi:ServerUri"] = "http://127.0.0.1:65101/",
            ["DrasiWake:OpenClaw:Targets:sample-gateway:BaseAddress"] = "http://127.0.0.1:65102/",
            ["DrasiWake:OpenClaw:Targets:sample-gateway:GatewayIdempotencyRetention"] = "30.00:00:00",
            ["DrasiWake:Database:Path"] = Path.Combine(Path.GetTempPath(), $"DrasiWake-health-{Guid.NewGuid():N}"),
            ["DrasiWake:Registry:Path"] = Path.Combine(Path.GetTempPath(), $"DrasiWake-health-{Guid.NewGuid():N}.yaml")
        };
        foreach (var entry in overrides)
            values[entry.Key] = entry.Value;
        return new ConfigurationBuilder().AddInMemoryCollection(values).Build();
    }

    private static async Task<(string Root, string RegistryPath)> CreateSampleRegistryAsync()
    {
        var repositoryRoot = RepositoryRootLocator.Find(AppContext.BaseDirectory);
        var sourceDirectory = Path.Combine(repositoryRoot, "src", "DrasiWake.Host", "contracts");
        var root = Path.Combine(Path.GetTempPath(), $"DrasiWake-health-host-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        var registryPath = Path.Combine(root, "bindings.yaml");
        var yaml = await File.ReadAllTextAsync(
            Path.Combine(sourceDirectory, "sample-binding.yaml"),
            TestContext.Current.CancellationToken);
        await File.WriteAllTextAsync(registryPath, yaml, TestContext.Current.CancellationToken);
        File.Copy(
            Path.Combine(sourceDirectory, "sample-facts.schema.json"),
            Path.Combine(root, "sample-facts.schema.json"));
        return (root, registryPath);
    }

    private static int ReserveThenClosePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }

    private sealed class SingleClientHttpClientFactory(HttpClient client) : IHttpClientFactory
    {
        public HttpClient CreateClient(string name) => client;
    }

    private sealed class StubDownstreamHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            if (request.RequestUri!.AbsolutePath == "/refused")
            {
                return Task.FromException<HttpResponseMessage>(new HttpRequestException(
                    "No connection could be made because the target machine actively refused it."));
            }

            var statusCode = request.RequestUri!.AbsolutePath switch
            {
                "/ok" or "/health" or "/api/integration/status" => HttpStatusCode.OK,
                "/auth-required" => HttpStatusCode.Unauthorized,
                "/server-error" => HttpStatusCode.InternalServerError,
                _ => HttpStatusCode.NotFound
            };
            var response = new HttpResponseMessage(statusCode)
            {
                Content = new StringContent("{}", Encoding.UTF8, "application/json")
            };
            return Task.FromResult(response);
        }
    }
}
