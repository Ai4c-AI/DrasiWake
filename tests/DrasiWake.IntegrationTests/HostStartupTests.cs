using DrasiWake.Host;
using DrasiWake.Core.Contracts;
using DrasiWake.Core.Domain;
using DrasiWake.Core.Pipeline;
using DrasiWake.LocalEnvironment;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.Text.Json.Nodes;

namespace DrasiWake.IntegrationTests;

public sealed class HostStartupTests
{
    [Fact]
    public void Legacy_configuration_defaults_to_a_stable_single_node_cluster()
    {
        var settings = DrasiWakeHostSettings.FromConfiguration(CreateConfiguration(
            new Dictionary<string, string?>()));

        Assert.Equal(RaftClusterMode.SingleNode, settings.Cluster.Mode);
        Assert.Equal("local", settings.Cluster.NodeId);
        Assert.Equal(new Uri("http://127.0.0.1:50051/"), settings.Cluster.ListenAddress);
        Assert.Equal(Path.GetFullPath(settings.DatabasePath + "-raft"), settings.Cluster.RaftDataPath);
        Assert.Equal([settings.Cluster.ListenAddress], settings.Cluster.InitialMembers);
        Assert.Null(settings.Cluster.ManagementAddress);
        Assert.Null(settings.Cluster.ManagementBearerToken);
    }

    [Fact]
    public void Valid_cluster_configuration_binds_three_unique_https_members()
    {
        var settings = DrasiWakeHostSettings.FromConfiguration(CreateClusterConfiguration());

        Assert.Equal(RaftClusterMode.Cluster, settings.Cluster.Mode);
        Assert.Equal("node-a", settings.Cluster.NodeId);
        Assert.Equal(new Uri("https://node-a.test:5101/"), settings.Cluster.ListenAddress);
        Assert.Equal(
            [
                new Uri("https://node-a.test:5101/"),
                new Uri("https://node-b.test:5101/"),
                new Uri("https://node-c.test:5101/")
            ],
            settings.Cluster.InitialMembers);
        Assert.Equal(1000, settings.Cluster.SnapshotFrequency);
        Assert.Equal("test-only-token", settings.Cluster.ManagementBearerToken);
    }

    [Fact]
    public void Cluster_section_requires_an_explicit_mode()
    {
        var exception = Assert.Throws<InvalidOperationException>(() =>
            DrasiWakeHostSettings.FromConfiguration(CreateConfiguration(new Dictionary<string, string?>
            {
                ["DrasiWake:Cluster:NodeId"] = "node-a"
            })));

        Assert.Contains("DrasiWake:Cluster:Mode", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Cluster_mode_requires_snapshot_frequency()
    {
        var exception = Assert.Throws<InvalidOperationException>(() =>
            DrasiWakeHostSettings.FromConfiguration(CreateClusterConfiguration(
                new Dictionary<string, string?>
                {
                    ["DrasiWake:Cluster:SnapshotFrequency"] = null
                })));

        Assert.Contains("DrasiWake:Cluster:SnapshotFrequency", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("DrasiWake:Cluster:Mode", "1")]
    [InlineData("DrasiWake:Cluster:Mode", " ")]
    [InlineData("DrasiWake:Cluster:NodeId", " ")]
    [InlineData("DrasiWake:Cluster:ListenAddress", "http://node-a.test:5101/")]
    [InlineData("DrasiWake:Cluster:Management:Address", "http://management.test:5102/")]
    [InlineData("DrasiWake:Cluster:RaftDataPath", "")]
    [InlineData("DrasiWake:Cluster:Certificate:Path", "")]
    [InlineData("DrasiWake:Cluster:Certificate:Password", "")]
    [InlineData("DrasiWake:Cluster:Management:BearerToken", "")]
    [InlineData("DrasiWake:Cluster:InitialMembers:1", "https://node-a.test:5101/")]
    [InlineData("DrasiWake:Cluster:InitialMembers:2", "not a uri")]
    [InlineData("DrasiWake:Cluster:SnapshotFrequency", "0")]
    [InlineData("DrasiWake:Cluster:SnapshotFrequency", "-1")]
    public void Invalid_cluster_configuration_is_rejected_with_its_key(string key, string value)
    {
        var values = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [key] = value
        };
        var exception = Assert.Throws<InvalidOperationException>(() =>
            DrasiWakeHostSettings.FromConfiguration(CreateClusterConfiguration(values)));

        Assert.Contains(key, exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("test-only-token", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Cluster_mode_requires_management_token_without_echoing_secret_values()
    {
        var exception = Assert.Throws<InvalidOperationException>(() =>
            DrasiWakeHostSettings.FromConfiguration(CreateClusterConfiguration(
                new Dictionary<string, string?>
                {
                    ["DrasiWake:Cluster:Management:BearerToken"] = null
                })));

        Assert.Contains("DrasiWake:Cluster:Management:BearerToken", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("test-only-token", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Cluster_mode_rejects_raft_path_equal_to_database_path()
    {
        var databasePath = Path.Combine(Path.GetTempPath(), $"DrasiWake-raft-{Guid.NewGuid():N}");
        var exception = Assert.Throws<InvalidOperationException>(() =>
            DrasiWakeHostSettings.FromConfiguration(CreateClusterConfiguration(
                new Dictionary<string, string?>
                {
                    ["DrasiWake:Database:Path"] = databasePath,
                    ["DrasiWake:Cluster:RaftDataPath"] = databasePath
                })));

        Assert.Contains("DrasiWake:Cluster:RaftDataPath", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("test-only-token", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Cluster_mode_requires_local_listen_address_in_initial_members()
    {
        var exception = Assert.Throws<InvalidOperationException>(() =>
            DrasiWakeHostSettings.FromConfiguration(CreateClusterConfiguration(
                new Dictionary<string, string?>
                {
                    ["DrasiWake:Cluster:InitialMembers:0"] = "https://node-b.test:5101/",
                    ["DrasiWake:Cluster:InitialMembers:1"] = "https://node-c.test:5101/",
                    ["DrasiWake:Cluster:InitialMembers:2"] = "https://node-d.test:5101/"
                })));

        Assert.Contains("DrasiWake:Cluster:InitialMembers", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Explicit_single_node_mode_does_not_require_management_secrets()
    {
        var settings = DrasiWakeHostSettings.FromConfiguration(CreateConfiguration(new Dictionary<string, string?>
        {
            ["DrasiWake:Cluster:Mode"] = "SingleNode",
            ["DrasiWake:Cluster:Management:BearerToken"] = null
        }));

        Assert.Equal(RaftClusterMode.SingleNode, settings.Cluster.Mode);
        Assert.Null(settings.Cluster.ManagementBearerToken);
    }

    [Fact]
    public async Task Invalid_registry_fails_host_startup()
    {
        using var host = DrasiWakeHostBuilder.CreateHost(CreateConfiguration(new Dictionary<string, string?>
        {
            ["DrasiWake:Registry:Path"] = Path.Combine(Path.GetTempPath(), $"missing-{Guid.NewGuid():N}.yaml")
        }));

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => host.StartAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public void Named_target_environment_keys_bind_host_gateway_and_token()
    {
        var configuration = CreateConfiguration(new Dictionary<string, string?>
        {
            ["DrasiWake:Drasi:ServerUri"] = "http://127.0.0.1:45123/",
            ["DrasiWake:OpenClaw:Targets:sample-gateway:BaseAddress"] = "http://127.0.0.1:45678/",
            ["DrasiWake:OpenClaw:Targets:sample-gateway:BearerToken"] = "gateway-test-token"
        });

        var settings = DrasiWakeHostSettings.FromConfiguration(configuration);

        Assert.Equal(new Uri("http://127.0.0.1:45123/"), settings.Drasi.ServerUri);
        Assert.Equal(new Uri("http://127.0.0.1:45678/"), settings.OpenClawTargets["sample-gateway"].BaseAddress);
        Assert.Equal("gateway-test-token", settings.OpenClawTargets["sample-gateway"].BearerToken);
    }

    [Fact]
    public void Named_gateway_target_settings_bind_by_logical_name()
    {
        var configuration = CreateConfiguration(new Dictionary<string, string?>
        {
            ["DrasiWake:OpenClaw:Targets:sensor-gateway:BaseAddress"] = "https://sensor-gateway.test/",
            ["DrasiWake:OpenClaw:Targets:sensor-gateway:BearerToken"] = "sensor-token",
            ["DrasiWake:OpenClaw:Targets:sensor-gateway:GatewayIdempotencyRetention"] = "15.00:00:00"
        });

        var settings = DrasiWakeHostSettings.FromConfiguration(configuration);
        var targetsProperty = settings.GetType().GetProperty("OpenClawTargets");
        Assert.NotNull(targetsProperty);
        var targets = Assert.IsAssignableFrom<System.Collections.IDictionary>(targetsProperty.GetValue(settings));
        var target = targets["sensor-gateway"];
        Assert.NotNull(target);
        var targetType = target.GetType();
        Assert.Equal(
            new Uri("https://sensor-gateway.test/"),
            targetType.GetProperty("BaseAddress")!.GetValue(target));
        Assert.Equal("sensor-token", targetType.GetProperty("BearerToken")!.GetValue(target));
        Assert.Equal(
            TimeSpan.FromDays(15),
            targetType.GetProperty("GatewayIdempotencyRetention")!.GetValue(target));
    }

    [Theory]
    [InlineData("ftp://gateway.test/")]
    [InlineData("/relative")]
    public void Invalid_named_target_address_is_rejected(string address)
    {
        var exception = Assert.Throws<InvalidOperationException>(() =>
            DrasiWakeHostSettings.FromConfiguration(CreateConfiguration(new Dictionary<string, string?>
            {
                ["DrasiWake:OpenClaw:Targets:sample-gateway:BaseAddress"] = address
            })));

        Assert.Contains("DrasiWake:OpenClaw:Targets:sample-gateway:BaseAddress", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Malformed_named_target_credential_is_rejected_without_echoing_secret()
    {
        const string token = "token with spaces";
        var exception = Assert.Throws<InvalidOperationException>(() =>
            DrasiWakeHostSettings.FromConfiguration(CreateConfiguration(new Dictionary<string, string?>
            {
                ["DrasiWake:OpenClaw:Targets:sample-gateway:BearerToken"] = token
            })));

        Assert.Contains("BearerToken", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(token, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Named_target_requires_idempotency_retention_configuration()
    {
        var exception = Assert.Throws<InvalidOperationException>(() =>
            DrasiWakeHostSettings.FromConfiguration(CreateConfiguration(new Dictionary<string, string?>
            {
                ["DrasiWake:OpenClaw:Targets:sample-gateway:GatewayIdempotencyRetention"] = null
            })));

        Assert.Contains("GatewayIdempotencyRetention", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Binding_with_unknown_target_fails_host_startup()
    {
        var (root, registryPath) = await CreateSampleRegistryWithTargetAsync("missing-gateway");
        var databasePath = Path.Combine(root, "database");
        using var host = DrasiWakeHostBuilder.CreateHost(CreateConfiguration(new Dictionary<string, string?>
        {
            ["DrasiWake:Registry:Path"] = registryPath,
            ["DrasiWake:Database:Path"] = databasePath
        }));

        try
        {
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => host.Services.GetRequiredService<HostStartupValidator>()
                    .StartAsync(TestContext.Current.CancellationToken));

            Assert.Contains("missing-gateway", exception.Message, StringComparison.Ordinal);
            Assert.False(Directory.Exists(databasePath));
        }
        finally
        {
            host.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Target_retention_shorter_than_binding_retry_age_fails_host_startup()
    {
        var (root, registryPath) = await CreateSampleRegistryWithTargetAsync("sample-gateway");
        var databasePath = Path.Combine(root, "database");
        using var host = DrasiWakeHostBuilder.CreateHost(CreateConfiguration(new Dictionary<string, string?>
        {
            ["DrasiWake:Registry:Path"] = registryPath,
            ["DrasiWake:Database:Path"] = databasePath,
            ["DrasiWake:OpenClaw:Targets:sample-gateway:GatewayIdempotencyRetention"] = "01:00:00"
        }));

        try
        {
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => host.Services.GetRequiredService<HostStartupValidator>()
                    .StartAsync(TestContext.Current.CancellationToken));

            Assert.Contains("sample-gateway", exception.Message, StringComparison.Ordinal);
            Assert.Contains("retention", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.False(Directory.Exists(databasePath));
        }
        finally
        {
            host.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Aspire_sensor_binding_matches_fixture_and_accepts_sensor_rows()
    {
        var repositoryRoot = RepositoryRootLocator.Find(AppContext.BaseDirectory);
        var registryPath = Path.Combine(
            repositoryRoot,
            "src",
            "DrasiWake.Host",
            "contracts",
            "aspire-sensor-binding.yaml");
        var candidate = await new ContractRegistryLoader().LoadCandidateAsync(
            registryPath,
            TestContext.Current.CancellationToken);

        Assert.True(candidate.IsValid, string.Join("; ", candidate.Errors.Select(error => error.Message)));
        var binding = Assert.Single(candidate.Registry!.Bindings);
        Assert.Equal("drasi-server", binding.Source);
        Assert.Equal(new Uri("http://127.0.0.1:8080/"), binding.Server);
        Assert.Equal("drasiwake-sensor-monitor", binding.InstanceId);
        Assert.Equal("sensor-readings", binding.QueryId);
        Assert.Equal("drasiwake-sensor-reading-summary", binding.MetaSkill);

        var renderer = new WakePayloadRenderer();
        var query = new QueryIdentity(binding.Server, binding.InstanceId, binding.QueryId!);
        var rendered = renderer.Render(
            binding,
            query,
            "aspire-e2e-test",
            [JsonNode.Parse("""{"SensorId":"sensor-1","Temperature":21.5,"Humidity":45.2}""")]);

        Assert.Single((JsonArray)rendered.Input["facts"]!);
        Assert.Throws<WakePayloadRejectedException>(() => renderer.Render(
            binding,
            query,
            "aspire-e2e-test",
            [JsonNode.Parse("""{"SensorId":"sensor-1","Temperature":21.5}""")]));
    }

    [Theory]
    [InlineData("DrasiWake:WorkerCount", "not-a-number")]
    [InlineData("DrasiWake:DispatchPollInterval", "not-a-duration")]
    public void Invalid_numeric_configuration_is_rejected(string key, string value)
    {
        var exception = Assert.Throws<InvalidOperationException>(() =>
            DrasiWakeHostBuilder.CreateHost(CreateConfiguration(new Dictionary<string, string?>
            {
                [key] = value
            })));

        Assert.Contains(key, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Valid_registry_migrates_database_and_database_directory_has_single_owner()
    {
        var root = Path.Combine(Path.GetTempPath(), $"DrasiWake-host-{Guid.NewGuid():N}");
        var registryPath = Path.Combine(root, "contracts", "bindings.yaml");
        Directory.CreateDirectory(Path.GetDirectoryName(registryPath)!);
        await File.WriteAllTextAsync(Path.Combine(root, "contracts", "facts.schema.json"),
            "{\"type\":\"object\"}", TestContext.Current.CancellationToken);
        await File.WriteAllTextAsync(registryPath, SampleRegistry, TestContext.Current.CancellationToken);
        var candidate = await new ContractRegistryLoader().LoadCandidateAsync(registryPath, TestContext.Current.CancellationToken);
        Assert.True(candidate.IsValid, string.Join("; ", candidate.Errors.Select(error => error.Message)));
        var configuration = CreateConfiguration(new Dictionary<string, string?>
        {
            ["DrasiWake:Registry:Path"] = registryPath,
            ["DrasiWake:Database:Path"] = Path.Combine(root, "database")
        });

        var firstHost = DrasiWakeHostBuilder.CreateHost(configuration);
        var secondHost = DrasiWakeHostBuilder.CreateHost(configuration);
        var firstValidator = firstHost.Services.GetRequiredService<HostStartupValidator>();
        var secondValidator = secondHost.Services.GetRequiredService<HostStartupValidator>();
        try
        {
            await firstValidator.StartAsync(TestContext.Current.CancellationToken);
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => secondValidator.StartAsync(TestContext.Current.CancellationToken));
            await firstValidator.StopAsync(CancellationToken.None);
            await secondValidator.StartAsync(TestContext.Current.CancellationToken);
        }
        finally
        {
            firstHost.Dispose();
            secondHost.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }

    private static IConfiguration CreateConfiguration(IReadOnlyDictionary<string, string?> overrides)
    {
        var values = new Dictionary<string, string?>
        {
            ["DrasiWake:Drasi:ServerUri"] = "http://127.0.0.1:8080",
            ["DrasiWake:OpenClaw:Targets:sample-gateway:BaseAddress"] = "http://127.0.0.1:8081",
            ["DrasiWake:OpenClaw:Targets:sample-gateway:GatewayIdempotencyRetention"] = "30.00:00:00",
            ["DrasiWake:Database:Path"] = Path.Combine(Path.GetTempPath(), $"DrasiWake-test-{Guid.NewGuid():N}"),
            ["DrasiWake:Registry:Path"] = Path.Combine(Path.GetTempPath(), $"missing-{Guid.NewGuid():N}.yaml")
        };
        foreach (var entry in overrides)
            values[entry.Key] = entry.Value;
        return new ConfigurationBuilder().AddInMemoryCollection(values).Build();
    }

    private static IConfiguration CreateClusterConfiguration(
        IReadOnlyDictionary<string, string?>? overrides = null)
    {
        var values = new Dictionary<string, string?>
        {
            ["DrasiWake:Cluster:Mode"] = "Cluster",
            ["DrasiWake:Cluster:NodeId"] = "node-a",
            ["DrasiWake:Cluster:ListenAddress"] = "https://node-a.test:5101/",
            ["DrasiWake:Cluster:RaftDataPath"] = Path.Combine(Path.GetTempPath(), $"DrasiWake-raft-{Guid.NewGuid():N}"),
            ["DrasiWake:Cluster:InitialMembers:0"] = "https://node-a.test:5101/",
            ["DrasiWake:Cluster:InitialMembers:1"] = "https://node-b.test:5101/",
            ["DrasiWake:Cluster:InitialMembers:2"] = "https://node-c.test:5101/",
            ["DrasiWake:Cluster:Certificate:Path"] = "certificates/node.pfx",
            ["DrasiWake:Cluster:Certificate:Password"] = "test-only-password",
            ["DrasiWake:Cluster:Management:Address"] = "https://node-a.test:5102/",
            ["DrasiWake:Cluster:Management:BearerToken"] = "test-only-token",
            ["DrasiWake:Cluster:SnapshotFrequency"] = "1000"
        };
        if (overrides is not null)
        {
            foreach (var entry in overrides)
                values[entry.Key] = entry.Value;
        }

        return CreateConfiguration(values);
    }

    private static async Task<(string Root, string RegistryPath)> CreateSampleRegistryWithTargetAsync(string target)
    {
        var repositoryRoot = RepositoryRootLocator.Find(AppContext.BaseDirectory);
        var sourceDirectory = Path.Combine(repositoryRoot, "src", "DrasiWake.Host", "contracts");
        var root = Path.Combine(Path.GetTempPath(), $"DrasiWake-target-startup-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        var registryPath = Path.Combine(root, "bindings.yaml");
        var yaml = await File.ReadAllTextAsync(Path.Combine(sourceDirectory, "sample-binding.yaml"), TestContext.Current.CancellationToken);
        yaml = yaml.Replace("openClawTarget: sample-gateway", $"openClawTarget: {target}", StringComparison.Ordinal);
        await File.WriteAllTextAsync(registryPath, yaml, TestContext.Current.CancellationToken);
        File.Copy(Path.Combine(sourceDirectory, "sample-facts.schema.json"), Path.Combine(root, "sample-facts.schema.json"));
        return (root, registryPath);
    }

    private static readonly string SampleRegistry = string.Join(Environment.NewLine,
    [
        "version: 1.0.0",
        "bindings:",
        "  - id: test-binding",
        "    source: drasi-server",
        "    server: http://127.0.0.1:8080",
        "    instanceId: default",
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
        "      permitLimit: 10",
        "      windowMilliseconds: 1000"
    ]);
}