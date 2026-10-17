using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using DrasiWake.Cli;
using DrasiWake.Host;
using DrasiWake.LocalEnvironment;
using DrasiWake.Persistence.Raft;
using DrasiWake.Persistence.SonnetDB;
using DrasiWake.Persistence.SonnetDB.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace DrasiWake.IntegrationTests;

/// <summary>
/// End-to-end acceptance for the drasiwakectl CLI (design S5 / A1): table output,
/// --json output that stays machine-parseable, replay confirmation gate, and the
/// 0 / 1 / 2 / 3 exit-code contract.
/// </summary>
public sealed class CliTests
{
    static CliTests()
    {
        // The CLI must observe raw connection failures (exit 2). Sandboxed CI
        // environments often inject HTTP(S)_PROXY which turns unreachable targets
        // into a proxied 502 response instead; bypass it for this test process.
        foreach (var variable in new[] { "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy" })
            Environment.SetEnvironmentVariable(variable, null);
    }

    [Fact]
    public async Task Cli_lists_dead_letters_replays_and_reads_back_end_to_end()
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
        var url = $"http://127.0.0.1:{adminPort}";

        try
        {
            await host.StartAsync(TestContext.Current.CancellationToken);
            await WaitForLeaderAsync(host, TestContext.Current.CancellationToken);
            var seeded = await SeedOutboxRowsAsync(host, TestContext.Current.CancellationToken);
            var deadId = seeded.First(kvp => kvp.Value == "DeadLetter").Key;

            // Table output for the dead-letter view contains the seeded rows.
            var tableStdout = new StringWriter();
            var tableStderr = new StringWriter();
            var tableExit = DrasiWakeCli.Run(
                ["outbox", "dead-letters", "--url", url], tableStdout, tableStderr);
            Assert.Equal(DrasiWakeCli.ExitSuccess, tableExit);
            var table = tableStdout.ToString();
            foreach (var id in seeded.Where(row => row.Value == "DeadLetter").Select(row => row.Key))
                Assert.Contains(id.ToString("N")[..8], table, StringComparison.Ordinal);
            Assert.Contains("DeadLetter", table, StringComparison.Ordinal);
            Assert.Contains("triage-order", table, StringComparison.Ordinal);
            Assert.Contains("sample-orders", table, StringComparison.Ordinal);

            // --json output is parseable and exposes the keyset cursor for jq pipelines.
            var jsonStdout = new StringWriter();
            var jsonExit = DrasiWakeCli.Run(
                ["outbox", "list", "--json", "--limit", "2", "--url", url],
                jsonStdout, new StringWriter());
            Assert.Equal(DrasiWakeCli.ExitSuccess, jsonExit);
            using var parsed = JsonDocument.Parse(jsonStdout.ToString());
            Assert.Equal(2, parsed.RootElement.GetProperty("items").GetArrayLength());
            Assert.False(
                string.IsNullOrEmpty(parsed.RootElement.GetProperty("nextCursor").GetString()));

            // cluster status through the CLI reports the local leader.
            var clusterStdout = new StringWriter();
            var clusterExit = DrasiWakeCli.Run(
                ["cluster", "status", "--json", "--url", url], clusterStdout, new StringWriter());
            Assert.Equal(DrasiWakeCli.ExitSuccess, clusterExit);
            using var cluster = JsonDocument.Parse(clusterStdout.ToString());
            Assert.Equal("local", cluster.RootElement.GetProperty("nodeId").GetString());
            Assert.True(cluster.RootElement.GetProperty("writable").GetBoolean());

            // Replay with --yes flips the row back to Pending end to end.
            var replayStdout = new StringWriter();
            var replayExit = DrasiWakeCli.Run(
                ["outbox", "replay", deadId.ToString(), "--yes", "--reason", "cli-test", "--url", url],
                replayStdout, new StringWriter());
            Assert.Equal(DrasiWakeCli.ExitSuccess, replayExit);

            var detailStdout = new StringWriter();
            var detailExit = DrasiWakeCli.Run(
                ["outbox", "get", deadId.ToString(), "--json", "--url", url],
                detailStdout, new StringWriter());
            Assert.Equal(DrasiWakeCli.ExitSuccess, detailExit);
            using var detail = JsonDocument.Parse(detailStdout.ToString());
            Assert.Equal("Pending", detail.RootElement.GetProperty("status").GetString());
            Assert.Equal(1, detail.RootElement.GetProperty("replayCount").GetInt32());
            Assert.NotEqual(
                JsonValueKind.Null,
                detail.RootElement.GetProperty("lastReplayedAtUtc").ValueKind);
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
            host.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Cli_replay_without_yes_prints_summary_and_conflict_maps_to_exit_3()
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
        var url = $"http://127.0.0.1:{adminPort}";

        try
        {
            await host.StartAsync(TestContext.Current.CancellationToken);
            await WaitForLeaderAsync(host, TestContext.Current.CancellationToken);
            var seeded = await SeedOutboxRowsAsync(host, TestContext.Current.CancellationToken);
            var deadId = seeded.First(kvp => kvp.Value == "DeadLetter").Key;
            var pendingId = seeded.First(kvp => kvp.Value == "Pending").Key;

            // Missing --yes: print what would be replayed, exit 1, change nothing.
            var stdout = new StringWriter();
            var stderr = new StringWriter();
            var exit = DrasiWakeCli.Run(
                ["outbox", "replay", deadId.ToString(), "--url", url], stdout, stderr);
            Assert.Equal(DrasiWakeCli.ExitUsage, exit);
            Assert.Contains("Re-run with --yes", stdout.ToString(), StringComparison.Ordinal);
            Assert.Contains("DeadLetter", stdout.ToString(), StringComparison.Ordinal);

            // Server-side business error (replay of a non-dead-letter row) → exit 3.
            var conflictStderr = new StringWriter();
            var conflictExit = DrasiWakeCli.Run(
                ["outbox", "replay", pendingId.ToString(), "--yes", "--url", url],
                new StringWriter(), conflictStderr);
            Assert.Equal(DrasiWakeCli.ExitServer, conflictExit);
            Assert.Contains(
                "outbox.not_replayable", conflictStderr.ToString(), StringComparison.Ordinal);

            // The guarded replay left the dead letter untouched.
            var detailStdout = new StringWriter();
            _ = DrasiWakeCli.Run(
                ["outbox", "get", deadId.ToString(), "--json", "--url", url],
                detailStdout, new StringWriter());
            using var detail = JsonDocument.Parse(detailStdout.ToString());
            Assert.Equal("DeadLetter", detail.RootElement.GetProperty("status").GetString());
            Assert.Equal(0, detail.RootElement.GetProperty("replayCount").GetInt32());
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
            host.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void Cli_usage_errors_exit_1_without_touching_the_network()
    {
        // Unknown command.
        var unknownStderr = new StringWriter();
        var unknownExit = DrasiWakeCli.Run(
            ["bogus", "--url", "http://127.0.0.1:1"], new StringWriter(), unknownStderr);
        Assert.Equal(DrasiWakeCli.ExitUsage, unknownExit);
        Assert.Contains("Unknown command 'bogus'", unknownStderr.ToString(), StringComparison.Ordinal);

        // Invalid --url.
        var urlStderr = new StringWriter();
        var urlExit = DrasiWakeCli.Run(
            ["health", "--url", "not-a-uri"], new StringWriter(), urlStderr);
        Assert.Equal(DrasiWakeCli.ExitUsage, urlExit);
        Assert.Contains("--url", urlStderr.ToString(), StringComparison.Ordinal);

        // Missing replay id.
        var replayStderr = new StringWriter();
        var replayExit = DrasiWakeCli.Run(
            ["outbox", "replay", "--url", "http://127.0.0.1:1"], new StringWriter(), replayStderr);
        Assert.Equal(DrasiWakeCli.ExitUsage, replayExit);

        // help succeeds with zero.
        var helpExit = DrasiWakeCli.Run(["help"], new StringWriter(), new StringWriter());
        Assert.Equal(DrasiWakeCli.ExitSuccess, helpExit);
    }

    [Fact]
    public void Cli_connection_failures_map_to_exit_2()
    {
        var port = ReserveThenClosePort();
        var stderr = new StringWriter();
        var exit = DrasiWakeCli.Run(
            ["health", "--url", $"http://127.0.0.1:{port}"], new StringWriter(), stderr);
        Assert.Equal(DrasiWakeCli.ExitConnection, exit);
        Assert.Contains(
            "Cannot reach the DrasiWake admin API", stderr.ToString(), StringComparison.Ordinal);
    }

    // ---------- helpers (mirrors AdminApiTests fixtures) ----------

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

    private static async Task<Dictionary<Guid, string>> SeedOutboxRowsAsync(
        IHost host,
        CancellationToken cancellationToken)
    {
        var contextFactory = host.Services.GetRequiredService<IDbContextFactory<BridgeDbContext>>();
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        var rows = new List<(Guid Id, string Status, DateTimeOffset CreatedAtUtc)>();
        var baseTime = DateTimeOffset.FromUnixTimeMilliseconds(
            DateTimeOffset.UtcNow.AddMinutes(-10).ToUnixTimeMilliseconds());
        for (var index = 0; index < 3; index++)
            rows.Add((Guid.NewGuid(), index < 2 ? "DeadLetter" : "Pending", baseTime.AddSeconds(index)));

        foreach (var (id, status, createdAt) in rows)
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
                IdempotencyKey = $"idem-cli-{id:N}",
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
            ["DrasiWake:Database:Path"] = Path.Combine(Path.GetTempPath(), $"DrasiWake-cli-{Guid.NewGuid():N}"),
            ["DrasiWake:Registry:Path"] = Path.Combine(Path.GetTempPath(), $"DrasiWake-cli-{Guid.NewGuid():N}.yaml")
        };
        foreach (var entry in overrides)
            values[entry.Key] = entry.Value;
        return new ConfigurationBuilder().AddInMemoryCollection(values).Build();
    }

    private static async Task<(string Root, string RegistryPath)> CreateSampleRegistryAsync()
    {
        var repositoryRoot = RepositoryRootLocator.Find(AppContext.BaseDirectory);
        var sourceDirectory = Path.Combine(repositoryRoot, "src", "DrasiWake.Host", "contracts");
        var root = Path.Combine(Path.GetTempPath(), $"DrasiWake-cli-host-{Guid.NewGuid():N}");
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
