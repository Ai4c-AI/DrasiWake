using System.Collections.Concurrent;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using DrasiWake.Adapters.DrasiServer;
using DrasiWake.Adapters.OpenClaw;
using DrasiWake.Core.Domain;
using DrasiWake.LocalEnvironment;
using System.Text.Json.Nodes;

namespace DrasiWake.IntegrationTests;

[Collection(AspireContractCollection.Name)]
public sealed class RealAspireSmokeTests
{
    private readonly AspireContractFixture _aspire;

    public RealAspireSmokeTests(AspireContractFixture aspire) => _aspire = aspire;

    [Fact]
    public void Compose_services_are_started_and_have_discovered_addresses()
    {
        if (Environment.GetEnvironmentVariable("DRASIWAKE_RUN_REAL_ASPIRE_SMOKE") != "1")
            Assert.Skip("Set DRASIWAKE_RUN_REAL_ASPIRE_SMOKE=1 and configure a model provider key to start the Aspire-managed Compose services; a missing Gateway token is temporary for smoke only.");

        Assert.NotNull(_aspire.State?.DrasiServerUri);
        Assert.NotNull(_aspire.State?.OpenClawBaseAddress);
    }
}

[Collection(AspireContractCollection.Name)]
public sealed class RealDrasiContractTests
{
    private readonly AspireContractFixture _aspire;

    public RealDrasiContractTests(AspireContractFixture aspire) => _aspire = aspire;

    [Fact]
    public async Task Configured_Drasi_supports_enumeration_results_and_attach_routes()
    {
        if (Environment.GetEnvironmentVariable("DRASIWAKE_RUN_REAL_DRASI_CONTRACT_TESTS") != "1")
            Assert.Skip("Set DRASIWAKE_RUN_REAL_DRASI_CONTRACT_TESTS=1 and configure AppHost model/provider settings; repository fixture instance/query IDs are used by default.");

        var serverUri = _aspire.State?.DrasiServerUri
            ?? throw new InvalidOperationException("Drasi Compose did not publish an endpoint through Aspire.");
        var instanceId = ConfiguredOrDefault("DRASIWAKE_REAL_DRASI_INSTANCE_ID", "drasiwake-sensor-monitor");
        var queryId = ConfiguredOrDefault("DRASIWAKE_REAL_DRASI_QUERY_ID", "sensor-readings");
        var query = new QueryIdentity(serverUri, instanceId, queryId);
        using var handler = new PathRecordingHandler();
        using var httpClient = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(30) };
        var client = new DrasiServerClient(httpClient);

        var queries = await client.EnumerateQueriesAsync(serverUri, TestContext.Current.CancellationToken);
        Assert.Contains(query, queries);
        var snapshot = await client.ReadSnapshotAsync(query, TestContext.Current.CancellationToken);
        Assert.Equal(query, snapshot.Query);

        using var attachTimeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        attachTimeout.CancelAfter(TimeSpan.FromSeconds(30));
        await using var attach = client.AttachAsync(query, attachTimeout.Token)
            .GetAsyncEnumerator(attachTimeout.Token);
        Assert.True(await attach.MoveNextAsync().AsTask().WaitAsync(attachTimeout.Token));
        Assert.Equal(query, attach.Current.Query);
        Assert.Contains($"/api/v1/instances/{Uri.EscapeDataString(instanceId)}/queries/{Uri.EscapeDataString(queryId)}/results", handler.Paths);
        Assert.Contains($"/api/v1/instances/{Uri.EscapeDataString(instanceId)}/queries/{Uri.EscapeDataString(queryId)}/attach", handler.Paths);
        Assert.DoesNotContain(handler.Paths, path => path.Contains("events/stream", StringComparison.Ordinal));
    }

    private static string ConfiguredOrDefault(string name, string defaultValue)
        => Environment.GetEnvironmentVariable(name) is { Length: > 0 } value
            ? value
            : defaultValue;

    private sealed class PathRecordingHandler : DelegatingHandler
    {
        public PathRecordingHandler() : base(new HttpClientHandler()) { }

        public ConcurrentQueue<string> Paths { get; } = new();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Paths.Enqueue(request.RequestUri!.AbsolutePath);
            return await base.SendAsync(request, cancellationToken);
        }
    }
}

[Collection(AspireContractCollection.Name)]
public sealed class RealGatewayContractTests
{
    private readonly AspireContractFixture _aspire;

    public RealGatewayContractTests(AspireContractFixture aspire) => _aspire = aspire;

    [Fact]
    public async Task Configured_Gateway_replays_same_key_and_retention_covers_retry_age()
    {
        if (Environment.GetEnvironmentVariable("DRASIWAKE_RUN_REAL_GATEWAY_CONTRACT_TESTS") != "1")
            Assert.Skip("Set DRASIWAKE_RUN_REAL_GATEWAY_CONTRACT_TESTS=1 and configure AppHost model/provider settings plus the Gateway token; the repository fixture MetaSkill is used by default.");

        var baseAddress = _aspire.State?.OpenClawBaseAddress
            ?? throw new InvalidOperationException("OpenClaw Compose did not publish an endpoint through Aspire.");
        var token = _aspire.Options?.AuthToken
            ?? throw new InvalidOperationException("OpenClaw AppHost secrets were not loaded.");
        var skill = ConfiguredOrDefault(
            "DRASIWAKE_REAL_GATEWAY_TEST_SKILL",
            "drasiwake-sensor-reading-summary");
        var gatewayBuild = Environment.GetEnvironmentVariable("DRASIWAKE_REAL_GATEWAY_BUILD") ?? "openclaw.net:local";
        var retention = TimeSpan.Parse(
            Environment.GetEnvironmentVariable("DRASIWAKE_REAL_GATEWAY_IDEMPOTENCY_RETENTION") ?? "30.00:00:00",
            System.Globalization.CultureInfo.InvariantCulture);
        var maximumRetryAge = TimeSpan.Parse(
            Environment.GetEnvironmentVariable("DRASIWAKE_REAL_MAX_OUTBOX_RETRY_AGE") ?? "7.00:00:00",
            System.Globalization.CultureInfo.InvariantCulture);
        var options = new OpenClawOptions
        {
            BaseAddress = baseAddress,
            BearerToken = token,
            MaxRetryAttempts = 0,
            GatewayIdempotencyRetention = retention,
            MaximumOutboxRetryAge = maximumRetryAge
        };
        options.Validate();

        using var httpClient = new HttpClient { BaseAddress = baseAddress, Timeout = TimeSpan.FromMinutes(2) };
        httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
        var runId = Guid.NewGuid().ToString("N");
        var sessionId = $"bridge-v1-contract-{runId}";
        using var sessionResponse = await httpClient.PostAsJsonAsync(
            "/apps/chat",
            new { message = "Prepare a session for the gateway contract test.", sessionId },
            TestContext.Current.CancellationToken);
        sessionResponse.EnsureSuccessStatusCode();
        var sessionResponseBody = await sessionResponse.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        Assert.Contains($"\"sessionId\":\"{sessionId}\"", sessionResponseBody, StringComparison.Ordinal);

        var client = new OpenClawMetaInvocationClient(httpClient, options);
        var request = new WakeRequest(
            "real-contract-test",
            sessionId,
            skill,
            new JsonObject
            {
                ["contractTestRunId"] = runId,
                ["SensorId"] = "drasiwake-contract-sensor",
                ["Temperature"] = 21.5,
                ["Humidity"] = 43.2
            },
            $"drasiwake:real-contract:{runId}",
            "1.0.0",
            null,
            "sensor-gateway");

        var acceptances = await Task.WhenAll(
            client.InvokeAsync(request, TestContext.Current.CancellationToken).AsTask(),
            client.InvokeAsync(request, TestContext.Current.CancellationToken).AsTask());

        Assert.Equal(acceptances[0].InvocationId, acceptances[1].InvocationId);
        Assert.True(retention >= maximumRetryAge);
        Console.WriteLine($"Gateway contract evidence: build={gatewayBuild}; idempotencyRetention={retention}; maximumRetryAge={maximumRetryAge}; invocation={acceptances[0].InvocationId}");
    }

    private static string ConfiguredOrDefault(string name, string defaultValue)
        => Environment.GetEnvironmentVariable(name) is { Length: > 0 } value
            ? value
            : defaultValue;
}