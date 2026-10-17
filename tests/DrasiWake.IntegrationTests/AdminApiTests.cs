using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using DrasiWake.Host;
using DrasiWake.Host.Admin;
using DrasiWake.LocalEnvironment;
using DrasiWake.Persistence.Raft;
using DrasiWake.Persistence.SonnetDB;
using DrasiWake.Persistence.SonnetDB.Entities;
using Microsoft.AspNetCore.Http;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace DrasiWake.IntegrationTests;

public sealed class AdminApiTests
{
    // ---------- authentication middleware (unit level) ----------

    [Fact]
    public async Task Middleware_allows_loopback_calls_when_no_token_is_configured()
    {
        var (called, next) = CreateNext();
        var middleware = new AdminAuthenticationMiddleware(next, bearerToken: null);
        var context = CreateRequest(remoteIp: IPAddress.Loopback, authorization: null);

        await middleware.InvokeAsync(context);

        Assert.True(called.Task.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task Middleware_requires_the_token_for_every_caller_once_configured()
    {
        var (called, next) = CreateNext();
        var middleware = new AdminAuthenticationMiddleware(next, bearerToken: "admin-test-token");

        var anonymous = CreateRequest(IPAddress.Loopback, null);
        await middleware.InvokeAsync(anonymous);
        Assert.Equal(StatusCodes.Status401Unauthorized, anonymous.Response.StatusCode);
        Assert.False(called.Task.IsCompleted);

        var wrongToken = CreateRequest(IPAddress.Loopback, "Bearer wrong-test-token");
        await middleware.InvokeAsync(wrongToken);
        Assert.Equal(StatusCodes.Status401Unauthorized, wrongToken.Response.StatusCode);
        Assert.False(called.Task.IsCompleted);

        var validToken = CreateRequest(IPAddress.Loopback, "Bearer admin-test-token");
        await middleware.InvokeAsync(validToken);
        Assert.True(called.Task.IsCompletedSuccessfully);
        Assert.Equal(StatusCodes.Status200OK, validToken.Response.StatusCode);
    }

    [Fact]
    public async Task Middleware_rejects_non_loopback_callers_when_no_token_is_configured()
    {
        var (called, next) = CreateNext();
        var middleware = new AdminAuthenticationMiddleware(next, bearerToken: null);
        var context = CreateRequest(IPAddress.Parse("192.0.2.10"), null);

        await middleware.InvokeAsync(context);

        Assert.Equal(StatusCodes.Status401Unauthorized, context.Response.StatusCode);
        Assert.False(called.Task.IsCompleted);
    }

    [Fact]
    public async Task Middleware_always_allows_the_probe_endpoints()
    {
        var (called, next) = CreateNext();
        var middleware = new AdminAuthenticationMiddleware(next, bearerToken: "admin-test-token");
        var readyz = CreateRequest(IPAddress.Parse("192.0.2.10"), null, path: "/readyz");

        await middleware.InvokeAsync(readyz);

        Assert.True(called.Task.IsCompletedSuccessfully);
        Assert.Equal(StatusCodes.Status200OK, readyz.Response.StatusCode);
    }

    [Fact]
    public void Admin_cursor_round_trips_and_rejects_garbage()
    {
        var createdAt = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        var id = Guid.NewGuid();
        var encoded = AdminApiEndpointExtensions.EncodeCursor(createdAt, id);

        Assert.True(AdminApiEndpointExtensions.TryDecodeCursor(encoded, out var decodedAt, out var decodedId));
        Assert.Equal(createdAt, decodedAt);
        Assert.Equal(id, decodedId);
        Assert.False(AdminApiEndpointExtensions.TryDecodeCursor("not-a-cursor", out _, out _));
        Assert.False(AdminApiEndpointExtensions.TryDecodeCursor("", out _, out _));
    }

    // ---------- admin endpoints (real Kestrel, single node) ----------

    [Fact]
    public async Task Admin_api_serves_outbox_queries_and_replays_dead_letters()
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
            ["DrasiWake:Admin:DownstreamProbeInterval"] = "00:01:00",
            ["DrasiWake:DispatchPollInterval"] = "00:05:00"
        });
        using var host = DrasiWakeHostBuilder.CreateHost(configuration);
        var baseUrl = $"http://127.0.0.1:{adminPort}/";

        try
        {
            await host.StartAsync(TestContext.Current.CancellationToken);
            await WaitForLeaderAsync(host, TestContext.Current.CancellationToken);
            var seeded = await SeedOutboxRowsAsync(host, TestContext.Current.CancellationToken);
            using var client = new HttpClient { BaseAddress = new Uri(baseUrl) };

            // List: pagination over CreatedAtUtc desc with keyset cursor.
            var page1 = await GetJsonDocumentAsync(
                client, "/admin/outbox?limit=2", TestContext.Current.CancellationToken);
            var page1Items = page1.RootElement.GetProperty("items");
            Assert.Equal(2, page1Items.GetArrayLength());
            var cursor = page1.RootElement.GetProperty("nextCursor").GetString();
            Assert.False(string.IsNullOrEmpty(cursor));

            var page2 = await GetJsonDocumentAsync(
                client, $"/admin/outbox?limit=2&cursor={Uri.EscapeDataString(cursor!)}",
                TestContext.Current.CancellationToken);
            var page2Items = page2.RootElement.GetProperty("items");
            Assert.True(page2Items.GetArrayLength() >= 1);
            var listedIds = page1Items.EnumerateArray()
                .Concat(page2Items.EnumerateArray())
                .Select(item => item.GetProperty("id").GetGuid())
                .ToHashSet();
            Assert.Equal(seeded.Keys.ToHashSet(), listedIds);

            // Status filter and dead-letter convenience view.
            var deadLetters = await GetJsonDocumentAsync(
                client, "/admin/dead-letters", TestContext.Current.CancellationToken);
            Assert.Equal(2, deadLetters.RootElement.GetProperty("items").GetArrayLength());
            var filtered = await GetJsonDocumentAsync(
                client, "/admin/outbox?status=Pending", TestContext.Current.CancellationToken);
            Assert.Equal(1, filtered.RootElement.GetProperty("items").GetArrayLength());

            // Detail includes the audit columns.
            var deadId = seeded.First(kvp => kvp.Value == "DeadLetter").Key;
            var detail = await GetJsonDocumentAsync(
                client, $"/admin/outbox/{deadId}", TestContext.Current.CancellationToken);
            Assert.Equal("DeadLetter", detail.RootElement.GetProperty("status").GetString());
            Assert.Equal("triage-order", detail.RootElement.GetProperty("skill").GetString());
            Assert.Equal(
                JsonValueKind.Number,
                detail.RootElement.GetProperty("input").GetProperty("value").ValueKind);

            // Replay flips the row back to Pending and keeps the idempotency key.
            var replayResponse = await client.PostAsync(
                $"/admin/outbox/{deadId}/replay",
                new StringContent("""{"reason":"admin-api-test"}""", Encoding.UTF8, "application/json"),
                TestContext.Current.CancellationToken);
            Assert.Equal(HttpStatusCode.OK, replayResponse.StatusCode);
            var replayed = await GetJsonDocumentAsync(
                client, $"/admin/outbox/{deadId}", TestContext.Current.CancellationToken);
            Assert.Equal("Pending", replayed.RootElement.GetProperty("status").GetString());
            Assert.Equal(1, replayed.RootElement.GetProperty("replayCount").GetInt32());
            Assert.Equal(
                detail.RootElement.GetProperty("idempotencyKey").GetString(),
                replayed.RootElement.GetProperty("idempotencyKey").GetString());
            Assert.Equal(
                JsonValueKind.Null,
                detail.RootElement.GetProperty("lastReplayedAtUtc").ValueKind);
            Assert.NotEqual(
                JsonValueKind.Null,
                replayed.RootElement.GetProperty("lastReplayedAtUtc").ValueKind);

            // Replaying a non-dead-letter row is rejected.
            using var conflict = await client.PostAsync(
                $"/admin/outbox/{deadId}/replay",
                new StringContent("""{"reason":"again"}""", Encoding.UTF8, "application/json"),
                TestContext.Current.CancellationToken);
            Assert.Equal(HttpStatusCode.Conflict, conflict.StatusCode);
            var conflictBody = await conflict.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
            Assert.Contains("outbox.not_replayable", conflictBody, StringComparison.Ordinal);

            // Unknown id → 404 with the fixed error code.
            using var missing = await client.GetAsync(
                $"/admin/outbox/{Guid.NewGuid()}", TestContext.Current.CancellationToken);
            Assert.Equal(HttpStatusCode.NotFound, missing.StatusCode);
            var missingBody = await missing.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
            Assert.Contains("outbox.not_found", missingBody, StringComparison.Ordinal);

            // Cluster overview.
            var cluster = await GetJsonDocumentAsync(
                client, "/admin/cluster", TestContext.Current.CancellationToken);
            Assert.Equal("local", cluster.RootElement.GetProperty("nodeId").GetString());
            Assert.Equal("SingleNode", cluster.RootElement.GetProperty("mode").GetString());
            Assert.Equal("leader", cluster.RootElement.GetProperty("role").GetString());
            Assert.True(cluster.RootElement.GetProperty("writable").GetBoolean());

            // Downstream probe snapshot and health detail.
            var downstreams = await GetJsonDocumentAsync(
                client, "/admin/downstreams", TestContext.Current.CancellationToken);
            Assert.True(downstreams.RootElement.GetArrayLength() >= 2);
            var health = await GetJsonDocumentAsync(
                client, "/admin/health", TestContext.Current.CancellationToken);
            Assert.True(health.RootElement.GetProperty("checks").GetArrayLength() >= 6);
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
            host.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Admin_api_requires_the_configured_bearer_token()
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
            ["DrasiWake:Admin:BearerToken"] = "single-node-admin-test-token",
            ["DrasiWake:Admin:DownstreamProbeInterval"] = "00:01:00"
        });
        using var host = DrasiWakeHostBuilder.CreateHost(configuration);
        var baseUrl = $"http://127.0.0.1:{adminPort}/";

        try
        {
            await host.StartAsync(TestContext.Current.CancellationToken);
            using var client = new HttpClient { BaseAddress = new Uri(baseUrl) };

            using var anonymous = await client.GetAsync(
                "/admin/outbox", TestContext.Current.CancellationToken);
            Assert.Equal(HttpStatusCode.Unauthorized, anonymous.StatusCode);
            var anonymousBody = await anonymous.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
            Assert.Contains("admin.unauthorized", anonymousBody, StringComparison.Ordinal);

            // Probe endpoints stay anonymous even with a token configured.
            using var readyz = await client.GetAsync("/readyz", TestContext.Current.CancellationToken);
            Assert.Equal(HttpStatusCode.ServiceUnavailable, readyz.StatusCode);

            using var authorized = new HttpRequestMessage(HttpMethod.Get, "/admin/outbox");
            authorized.Headers.TryAddWithoutValidation("Authorization", "Bearer single-node-admin-test-token");
            using var allowed = await client.SendAsync(authorized, TestContext.Current.CancellationToken);
            Assert.Equal(HttpStatusCode.OK, allowed.StatusCode);
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
            host.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }

    // ---------- helpers ----------

    private static (TaskCompletionSource, RequestDelegate) CreateNext()
    {
        var called = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        RequestDelegate next = _ =>
        {
            called.TrySetResult();
            return Task.CompletedTask;
        };
        return (called, next);
    }

    private static DefaultHttpContext CreateRequest(IPAddress remoteIp, string? authorization, string path = "/admin/outbox")
    {
        var context = new DefaultHttpContext();
        context.Connection.RemoteIpAddress = remoteIp;
        context.Request.Path = path;
        if (authorization is not null)
            context.Request.Headers.Authorization = authorization;
        context.Response.StatusCode = StatusCodes.Status200OK;
        return context;
    }

    private static async Task<JsonDocument> GetJsonDocumentAsync(
        HttpClient client,
        string requestUri,
        CancellationToken cancellationToken)
    {
        using var response = await client.GetAsync(requestUri, cancellationToken);
        response.EnsureSuccessStatusCode();
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        return JsonDocument.Parse(body);
    }

    private static async Task WaitForLeaderAsync(IHost host, CancellationToken cancellationToken)
    {
        var leadership = host.Services.GetRequiredService<IRaftLeadership>();
        var deadline = DateTimeOffset.UtcNow.AddSeconds(15);
        while (!leadership.IsLeader || !leadership.HasQuorum)
        {
            if (DateTimeOffset.UtcNow >= deadline)
                throw new TimeoutException("The single-node Raft cluster did not elect a leader in time.");
            await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
        }
    }

    /// <summary>Inserts outbox rows directly into the projection database (read-path fixtures).</summary>
    private static async Task<Dictionary<Guid, string>> SeedOutboxRowsAsync(
        IHost host,
        CancellationToken cancellationToken)
    {
        var contextFactory = host.Services.GetRequiredService<IDbContextFactory<BridgeDbContext>>();
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        var rows = new List<(Guid Id, string Status, DateTimeOffset CreatedAtUtc, string IdempotencyKey)>();
        var baseTime = DateTimeOffset.FromUnixTimeMilliseconds(
            DateTimeOffset.UtcNow.AddMinutes(-10).ToUnixTimeMilliseconds());
        for (var index = 0; index < 3; index++)
        {
            rows.Add((
                Guid.NewGuid(),
                index < 2 ? "DeadLetter" : "Pending",
                baseTime.AddSeconds(index),
                $"idem-seed-{Guid.NewGuid():N}"));
        }

        foreach (var (id, status, createdAt, idempotencyKey) in rows)
        {
            context.WakeOutbox.Add(new WakeOutbox
            {
                Id = id,
                BindingId = "sample-orders",
                SessionId = $"session-{id:N}",
                SnapshotFingerprint = $"fp-{id:N}",
                Skill = "triage-order",
                OpenClawTarget = "sample-gateway",
                InputJson = """{"value":1}""",
                ContractVersion = "1.0.0",
                IdempotencyKey = idempotencyKey,
                AttemptCount = 1,
                CreatedAtUtc = createdAt,
                NextAttemptAtUtc = createdAt,
                Status = Enum.Parse<DrasiWake.Core.Domain.WakeOutboxStatus>(status),
                ReplayCount = 0,
                Version = 1
            });
        }

        await context.SaveChangesAsync(cancellationToken);
        return rows.ToDictionary(row => row.Id, row => row.Status);
    }

    private static IConfiguration CreateConfiguration(IReadOnlyDictionary<string, string?> overrides)
    {
        var values = new Dictionary<string, string?>
        {
            ["DrasiWake:Drasi:ServerUri"] = "http://127.0.0.1:65101/",
            ["DrasiWake:OpenClaw:Targets:sample-gateway:BaseAddress"] = "http://127.0.0.1:65102/",
            ["DrasiWake:OpenClaw:Targets:sample-gateway:GatewayIdempotencyRetention"] = "30.00:00:00",
            ["DrasiWake:Database:Path"] = Path.Combine(Path.GetTempPath(), $"DrasiWake-admin-{Guid.NewGuid():N}"),
            ["DrasiWake:Registry:Path"] = Path.Combine(Path.GetTempPath(), $"DrasiWake-admin-{Guid.NewGuid():N}.yaml")
        };
        foreach (var entry in overrides)
            values[entry.Key] = entry.Value;
        return new ConfigurationBuilder().AddInMemoryCollection(values).Build();
    }

    private static async Task<(string Root, string RegistryPath)> CreateSampleRegistryAsync()
    {
        var repositoryRoot = RepositoryRootLocator.Find(AppContext.BaseDirectory);
        var sourceDirectory = Path.Combine(repositoryRoot, "src", "DrasiWake.Host", "contracts");
        var root = Path.Combine(Path.GetTempPath(), $"DrasiWake-admin-host-{Guid.NewGuid():N}");
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
}
