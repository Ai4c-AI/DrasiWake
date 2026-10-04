using System.Collections.Concurrent;
using System.Net;
using System.Text.Json;
using System.Text.Json.Nodes;
using DrasiWake.Adapters.DrasiServer;
using DrasiWake.Adapters.OpenClaw;
using DrasiWake.Core.Contracts;
using DrasiWake.Core.Domain;
using DrasiWake.Core.Pipeline;
using DrasiWake.Persistence.SonnetDB;
using DrasiWake.Persistence.SonnetDB.Entities;
using Microsoft.EntityFrameworkCore;
using SnapshotCheckpointEntity = DrasiWake.Persistence.SonnetDB.Entities.SnapshotCheckpoint;

namespace DrasiWake.IntegrationTests.Fixtures;

internal sealed class BridgeTestFixture : IAsyncDisposable
{
    private readonly string rootDirectory;
    private readonly string databaseDirectory;
    private readonly HttpClient drasiHttpClient;
    private readonly HttpClient gatewayHttpClient;
    private DbContextOptions<BridgeDbContext> databaseOptions;
    private SonnetDatabaseOwner databaseOwner;

    private BridgeTestFixture(
        string rootDirectory,
        string databaseDirectory,
        QueryIdentity query,
        ContractRegistryManager registry,
        DbContextOptions<BridgeDbContext> databaseOptions,
        SonnetDatabaseOwner databaseOwner,
        LocalDrasiHandler drasiHandler,
        LocalGatewayHandler gatewayHandler)
    {
        this.rootDirectory = rootDirectory;
        this.databaseDirectory = databaseDirectory;
        Query = query;
        Registry = registry;
        this.databaseOptions = databaseOptions;
        this.databaseOwner = databaseOwner;
        GatewayHandler = gatewayHandler;
        drasiHttpClient = new HttpClient(drasiHandler);
        gatewayHttpClient = new HttpClient(gatewayHandler) { BaseAddress = new Uri("http://gateway.test") };
        ChangeSource = new DrasiChangeSource(
            new DrasiServerClient(drasiHttpClient),
            new DrasiServerOptions(query.Server)
            {
                InitialReconnectDelay = TimeSpan.FromHours(1),
                MaxReconnectDelay = TimeSpan.FromHours(1)
            });
        Store = CreateStore();
        Reconciler = new SnapshotReconciler(ChangeSource, Store, Registry);
        GatewayClient = new OpenClawMetaInvocationClient(
            gatewayHttpClient,
            new OpenClawOptions { MaxRetryAttempts = 0 },
            new OpenClawTargetOptions(
                new Uri("http://gateway.test/"),
                null,
                TimeSpan.FromDays(30)));
        Dispatcher = new OutboxDispatcher(Store, GatewayClient, Registry);
        DrasiHandler = drasiHandler;
    }

    public QueryIdentity Query { get; }
    public ContractRegistryManager Registry { get; }
    public LocalDrasiHandler DrasiHandler { get; }
    public LocalGatewayHandler GatewayHandler { get; }
    public DrasiChangeSource ChangeSource { get; }
    public SonnetBridgeStore Store { get; }
    public SnapshotReconciler Reconciler { get; }
    public OpenClawMetaInvocationClient GatewayClient { get; }
    public OutboxDispatcher Dispatcher { get; }
    public IDbContextFactory<BridgeDbContext> ContextFactory => new TestContextFactory(databaseOptions);

    public static async Task<BridgeTestFixture> CreateAsync(CancellationToken cancellationToken = default)
    {
        var rootDirectory = Path.Combine(Path.GetTempPath(), $"DrasiWake-Integration-{Guid.NewGuid():N}");
        var contractsDirectory = Path.Combine(rootDirectory, "contracts");
        var databaseDirectory = Path.Combine(rootDirectory, "database");
        Directory.CreateDirectory(contractsDirectory);
        Directory.CreateDirectory(databaseDirectory);
        await File.WriteAllTextAsync(
            Path.Combine(contractsDirectory, "facts.schema.json"),
            "{\"type\":\"object\",\"required\":[\"orderId\",\"state\"],\"properties\":{\"orderId\":{\"type\":\"string\"},\"state\":{\"type\":\"string\"}}}",
            cancellationToken);
        var registryPath = Path.Combine(contractsDirectory, "bindings.yaml");
        await File.WriteAllTextAsync(registryPath, CreateRegistryYaml(), cancellationToken);

        var candidate = await new ContractRegistryLoader().LoadCandidateAsync(registryPath, cancellationToken);
        if (!candidate.IsValid || candidate.Registry is null)
            throw new InvalidOperationException(string.Join("; ", candidate.Errors.Select(error => error.Code)));
        var registry = new ContractRegistryManager();
        if (!registry.TryActivate(candidate))
            throw new InvalidOperationException("The integration test registry could not be activated.");

        var server = new Uri("http://drasi.test");
        var query = new QueryIdentity(server, "east", "orders");
        var databaseOptions = new DbContextOptionsBuilder<BridgeDbContext>()
            .UseSonnetDB($"Data Source={databaseDirectory}")
            .Options;
        var databaseOwner = SonnetDatabaseOwner.Acquire(databaseDirectory);
        try
        {
            await using var context = new BridgeDbContext(databaseOptions);
            await context.Database.MigrateAsync(cancellationToken);
            return new BridgeTestFixture(
                rootDirectory,
                databaseDirectory,
                query,
                registry,
                databaseOptions,
                databaseOwner,
                new LocalDrasiHandler(),
                new LocalGatewayHandler());
        }
        catch
        {
            databaseOwner.Dispose();
            Directory.Delete(rootDirectory, recursive: true);
            throw;
        }
    }

    public SonnetBridgeStore CreateStore() => new(ContextFactory);

    public async Task RestartDatabaseAsync(CancellationToken cancellationToken = default)
    {
        databaseOwner.Dispose();
        databaseOptions = new DbContextOptionsBuilder<BridgeDbContext>()
            .UseSonnetDB($"Data Source={databaseDirectory}")
            .Options;
        databaseOwner = SonnetDatabaseOwner.Acquire(databaseDirectory);
        await using var context = new BridgeDbContext(databaseOptions);
        await context.Database.MigrateAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<WakeOutbox>> ReadOutboxAsync(CancellationToken cancellationToken = default)
    {
        await using var context = await ContextFactory.CreateDbContextAsync(cancellationToken);
        return await context.WakeOutbox.AsNoTracking().OrderBy(item => item.CreatedAtUtc).ToListAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<SnapshotCheckpointEntity>> ReadCheckpointsAsync(CancellationToken cancellationToken = default)
    {
        await using var context = await ContextFactory.CreateDbContextAsync(cancellationToken);
        return await context.SnapshotCheckpoints.AsNoTracking().ToListAsync(cancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        gatewayHttpClient.Dispose();
        drasiHttpClient.Dispose();
        databaseOwner.Dispose();
        Directory.Delete(rootDirectory, recursive: true);
        return ValueTask.CompletedTask;
    }

    private static string CreateRegistryYaml() => string.Join(Environment.NewLine,
    [
        "version: 1.0.0",
        "bindings:",
        "  - id: orders-binding",
        "    source: drasi-server",
        "    server: http://drasi.test",
        "    instanceId: east",
        "    queryId: orders",
        "    deliveryMode: converge-latest",
        "    sessionScope: singleton",
        "    openClawTarget: sample-gateway",
        "    metaSkill: triage-order",
        "    contractVersion: 1.0.0",
        "    factSchemaPath: facts.schema.json",
        "    maxPayloadBytes: 32768",
        "    retry:",
        "      maxAttempts: 3",
        "      maxAgeSeconds: 86400",
        "    rateLimit:",
        "      permitLimit: 20",
        "      windowMilliseconds: 1000"
    ]);

    private sealed class TestContextFactory(DbContextOptions<BridgeDbContext> options)
        : IDbContextFactory<BridgeDbContext>
    {
        public BridgeDbContext CreateDbContext() => new(options);

        public Task<BridgeDbContext> CreateDbContextAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(CreateDbContext());
        }
    }
}

internal sealed class LocalDrasiHandler : HttpMessageHandler
{
    private readonly object gate = new();
    private JsonArray rows = [];
    private bool resultsUnavailable;

    public ConcurrentQueue<string> Paths { get; } = new();
    public int ResultsReadCount { get; private set; }

    public void SetResults(params string[] jsonRows)
    {
        lock (gate)
            rows = new JsonArray(jsonRows.Select(json => JsonNode.Parse(json)).ToArray());
    }

    public void FailResultsReads()
    {
        lock (gate)
            resultsUnavailable = true;
    }

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var path = request.RequestUri!.AbsolutePath;
        Paths.Enqueue(path);
        if (path == "/api/v1/instances")
            return Task.FromResult(JsonResponse("{\"success\":true,\"data\":[{\"id\":\"east\"}],\"error\":null}"));
        if (path == "/api/v1/instances/east/queries")
            return Task.FromResult(JsonResponse("{\"success\":true,\"data\":[{\"id\":\"orders\"}],\"error\":null}"));
        if (path == "/api/v1/instances/east/queries/orders/results")
        {
            string data;
            lock (gate)
            {
                ResultsReadCount++;
                if (resultsUnavailable)
                    return Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable));
                data = rows.ToJsonString();
            }
            return Task.FromResult(JsonResponse($"{{\"success\":true,\"data\":{data},\"error\":null}}"));
        }
        if (path == "/api/v1/instances/east/queries/orders/attach")
        {
            var response = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(": keep-alive\n\ndata: {\"kind\":\"hint\"}\n\n")
            };
            response.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("text/event-stream");
            return Task.FromResult(response);
        }
        return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotFound));
    }

    private static HttpResponseMessage JsonResponse(string json) => new(HttpStatusCode.OK)
    {
        Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json")
    };
}

internal sealed class LocalGatewayHandler : HttpMessageHandler
{
    private readonly object gate = new();
    private readonly Dictionary<string, (string Body, Guid InvocationId, int RequestCount)> invocations = new(StringComparer.Ordinal);

    public ConcurrentQueue<(string Key, string Body)> Requests { get; } = new();
    public int ExecutionCount { get; private set; }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        if (request.Method != HttpMethod.Post || request.RequestUri?.AbsolutePath != "/api/integration/meta-invocations")
            return new HttpResponseMessage(HttpStatusCode.NotFound);

        var key = request.Headers.GetValues("Idempotency-Key").Single();
        var body = await request.Content!.ReadAsStringAsync(cancellationToken);
        Requests.Enqueue((key, body));
        Guid invocationId;
        int requestCount;
        lock (gate)
        {
            if (!invocations.TryGetValue(key, out var existing))
            {
                existing = (body, Guid.NewGuid(), 0);
                ExecutionCount++;
            }
            if (!string.Equals(existing.Body, body, StringComparison.Ordinal))
                return new HttpResponseMessage(HttpStatusCode.Conflict);
            requestCount = existing.RequestCount + 1;
            invocationId = existing.InvocationId;
            invocations[key] = (existing.Body, invocationId, requestCount);
        }

        var status = requestCount > 1 ? "Completed" : "Running";
        var payload = JsonSerializer.Serialize(new
        {
            invocationId,
            status,
            error = (string?)null,
            createdAtUtc = DateTimeOffset.UtcNow
        });
        return new HttpResponseMessage(HttpStatusCode.Accepted)
        {
            Content = new StringContent(payload, System.Text.Encoding.UTF8, "application/json")
        };
    }
}