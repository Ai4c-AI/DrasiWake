using DrasiWake.Host;
using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Contracts;
using DrasiWake.Core.Domain;
using DrasiWake.Core.Pipeline;
using DrasiWake.LocalEnvironment;
using DrasiWake.Persistence.Raft;
using DrasiWake.Persistence.SonnetDB;
using DrasiWake.Persistence.SonnetDB.Entities;
using DrasiWake.Persistence.SonnetDB.Replication;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using System.Text.Json.Nodes;
using ProtoBuf.Grpc.Client;

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
    [InlineData("DrasiWake:Cluster:Management:Address", "https://node-a.test:5101/")]
    [InlineData("DrasiWake:Cluster:Management:Address", "https://management.test:5102/admin")]
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
        Assert.Null(settings.Cluster.ManagementAddress);
    }

    [Fact]
    public void Single_node_host_does_not_register_management_services()
    {
        using var host = DrasiWakeHostBuilder.CreateHost(CreateConfiguration(new Dictionary<string, string?>
        {
            ["DrasiWake:Cluster:Mode"] = "SingleNode",
            ["DrasiWake:Cluster:Management:BearerToken"] = null
        }));

        Assert.Null(host.Services.GetService<IRaftMembershipManager>());
        Assert.Null(host.Services.GetService<IClusterCompatibilityProvider>());
    }

    [Fact]
    public void Single_node_mode_rejects_https_listen_address_without_tls_configuration()
    {
        var exception = Assert.Throws<InvalidOperationException>(() =>
            DrasiWakeHostSettings.FromConfiguration(CreateConfiguration(new Dictionary<string, string?>
            {
                ["DrasiWake:Cluster:Mode"] = "SingleNode",
                ["DrasiWake:Cluster:ListenAddress"] = "https://127.0.0.1:50051/"
            })));

        Assert.Contains("DrasiWake:Cluster:ListenAddress", exception.Message, StringComparison.Ordinal);
        Assert.Contains("TLS", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Business_configuration_fingerprint_is_stable_and_excludes_credentials()
    {
        var first = DrasiWakeHostSettings.FromConfiguration(CreateConfiguration(new Dictionary<string, string?>
        {
            ["DrasiWake:OpenClaw:Targets:sample-gateway:BearerToken"] = "first-test-token"
        }));
        var sameBusinessSettings = DrasiWakeHostSettings.FromConfiguration(CreateConfiguration(new Dictionary<string, string?>
        {
            ["DrasiWake:OpenClaw:Targets:sample-gateway:BearerToken"] = "second-test-token"
        }));
        var changedBusinessSettings = DrasiWakeHostSettings.FromConfiguration(CreateConfiguration(new Dictionary<string, string?>
        {
            ["DrasiWake:OpenClaw:Targets:sample-gateway:BaseAddress"] = "https://gateway.test/"
        }));

        var firstFingerprint = ClusterBusinessConfigurationFingerprint.Compute(first, ContractRegistry.Empty);

        Assert.Equal(firstFingerprint,
            ClusterBusinessConfigurationFingerprint.Compute(sameBusinessSettings, ContractRegistry.Empty));
        Assert.NotEqual(firstFingerprint,
            ClusterBusinessConfigurationFingerprint.Compute(changedBusinessSettings, ContractRegistry.Empty));
        Assert.DoesNotContain("first-test-token", firstFingerprint, StringComparison.Ordinal);
        Assert.DoesNotContain("second-test-token", firstFingerprint, StringComparison.Ordinal);
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
            await AssertProjectionDidNotMutateBusinessStateAsync(firstHost, expectFingerprint: false);
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

    [Fact]
    public void Production_bridge_store_registration_uses_only_the_raft_adapter()
    {
        using var host = DrasiWakeHostBuilder.CreateHost(CreateConfiguration(new Dictionary<string, string?>()));

        Assert.IsType<RaftBridgeStore>(host.Services.GetRequiredService<IBridgeStore>());
        Assert.IsType<SonnetBridgeStore>(host.Services.GetRequiredService<IRaftBridgeProjection>());
        Assert.IsType<DotNextRaftCommandExecutor>(host.Services.GetRequiredService<IRaftCommandExecutor>());
        Assert.False(typeof(IBridgeStore).IsAssignableFrom(typeof(SonnetBridgeStore)));
        Assert.Contains(host.Services.GetServices<IHostedService>(), service => service is HostStartupValidator);
        Assert.Contains(host.Services.GetServices<IHostedService>(), service => service is RaftLeaderHostedService);
        Assert.DoesNotContain(host.Services.GetServices<IHostedService>(), service =>
            service.GetType() == typeof(BridgeHostedService) ||
            service.GetType() == typeof(ReconciliationHostedService));
    }

    [Fact]
    public async Task Single_node_host_starts_raft_and_worker_services_only_after_migration()
    {
        var (root, registryPath) = await CreateSampleRegistryWithTargetAsync("sample-gateway");
        var databasePath = Path.Combine(root, "database");
        var port = GetAvailablePort();
        var configuration = CreateConfiguration(new Dictionary<string, string?>
        {
            ["DrasiWake:Registry:Path"] = registryPath,
            ["DrasiWake:Database:Path"] = databasePath,
            ["DrasiWake:Cluster:ListenAddress"] = $"http://127.0.0.1:{port}/",
            ["DrasiWake:Cluster:InitialMembers:0"] = $"http://127.0.0.1:{port}/",
            ["DrasiWake:Cluster:RaftDataPath"] = Path.Combine(root, "raft")
        });
        using var host = DrasiWakeHostBuilder.CreateHost(configuration);

        try
        {
            await host.StartAsync(TestContext.Current.CancellationToken);
            await using var context = await host.Services.GetRequiredService<
                    Microsoft.EntityFrameworkCore.IDbContextFactory<BridgeDbContext>>()
                .CreateDbContextAsync(TestContext.Current.CancellationToken);
            Assert.Empty(context.Database.GetPendingMigrations());

            var cluster = host.Services.GetRequiredService<DotNext.Net.Cluster.Consensus.Raft.IRaftCluster>();
            await WaitForLeadershipAsync(cluster, TestContext.Current.CancellationToken);
            var projection = host.Services.GetRequiredService<IRaftBridgeProjection>();
            var timeout = DateTimeOffset.UtcNow.AddSeconds(10);
            while (await projection.GetLastAppliedIndexAsync(TestContext.Current.CancellationToken) == 0)
            {
                if (DateTimeOffset.UtcNow >= timeout)
                    throw new TimeoutException("Leader startup preparation did not commit its configuration fingerprint.");
                await Task.Delay(TimeSpan.FromMilliseconds(25), TestContext.Current.CancellationToken);
            }
            await AssertProjectionDidNotMutateBusinessStateAsync(host, expectFingerprint: true);
            Assert.Equal(
                ClusterBusinessConfigurationFingerprint.Compute(
                    host.Services.GetRequiredService<DrasiWakeHostSettings>(),
                    host.Services.GetRequiredService<ContractRegistryManager>().Active),
                (await projection.ExportSnapshotAsync(TestContext.Current.CancellationToken)).ConfigurationFingerprint);
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
            host.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Multi_node_configuration_starts_an_http2_tls_raft_listener()
    {
        var (root, registryPath) = await CreateSampleRegistryWithTargetAsync("sample-gateway");
        const string certificatePassword = "host-startup-test-password";
        var certificatePath = Path.Combine(root, "raft-listener.pfx");
        var certificateThumbprint = CreateServerCertificate(certificatePath, certificatePassword);
        var port = GetAvailablePort();
        var managementPort = GetAvailablePort();
        var listenAddress = new Uri($"https://127.0.0.1:{port}/");
        var managementAddress = new Uri($"https://127.0.0.1:{managementPort}/");
        var config = CreateClusterConfiguration(new Dictionary<string, string?>
        {
            ["DrasiWake:Registry:Path"] = registryPath,
            ["DrasiWake:Database:Path"] = Path.Combine(root, "database"),
            ["DrasiWake:Cluster:ListenAddress"] = listenAddress.ToString(),
            ["DrasiWake:Cluster:RaftDataPath"] = Path.Combine(root, "raft"),
            ["DrasiWake:Cluster:InitialMembers:0"] = listenAddress.ToString(),
            ["DrasiWake:Cluster:InitialMembers:1"] = $"https://127.0.0.1:{GetAvailablePort()}/",
            ["DrasiWake:Cluster:InitialMembers:2"] = $"https://127.0.0.1:{GetAvailablePort()}/",
            ["DrasiWake:Cluster:Certificate:Path"] = certificatePath,
            ["DrasiWake:Cluster:Certificate:Password"] = certificatePassword,
            ["DrasiWake:Cluster:Management:Address"] = managementAddress.ToString()
        });
        using var host = DrasiWakeHostBuilder.CreateHost(config);

        try
        {
            await host.StartAsync(TestContext.Current.CancellationToken);
            using var certificate = host.Services.GetRequiredService<RaftClusterSettings>().LoadServerCertificate();
            Assert.True(certificate.HasPrivateKey);
            Assert.Equal(certificateThumbprint, certificate.GetCertHashString(), ignoreCase: true);
            var addresses = host.Services.GetRequiredService<IServer>()
                .Features.Get<IServerAddressesFeature>()!.Addresses;
            Assert.Contains(listenAddress.AbsoluteUri.TrimEnd('/'), addresses);
            Assert.Contains(managementAddress.AbsoluteUri.TrimEnd('/'), addresses);
            Assert.NotEqual(listenAddress.Port, managementAddress.Port);
            using var managementSocket = new TcpClient();
            await managementSocket.ConnectAsync(IPAddress.Loopback, managementPort, TestContext.Current.CancellationToken);
            using var managementTls = new System.Net.Security.SslStream(
                managementSocket.GetStream(),
                leaveInnerStreamOpen: false,
                (_, _, _, _) => true);
            await managementTls.AuthenticateAsClientAsync(
                new System.Net.Security.SslClientAuthenticationOptions
                {
                    TargetHost = "127.0.0.1",
                    ApplicationProtocols = [SslApplicationProtocol.Http2]
                },
                TestContext.Current.CancellationToken);
            Assert.Equal(SslApplicationProtocol.Http2, managementTls.NegotiatedApplicationProtocol);

            using var httpHandler = new SocketsHttpHandler
            {
                SslOptions = { RemoteCertificateValidationCallback = (_, _, _, _) => true }
            };
            using var managementChannel = GrpcChannel.ForAddress(
                managementAddress,
                new GrpcChannelOptions { HttpHandler = httpHandler });
            var managementRpc = managementChannel.CreateGrpcService<IClusterMembershipService>();
            var managementFailure = await Assert.ThrowsAsync<RpcException>(() =>
                managementRpc.GetCompatibility(new ClusterCompatibilityRequest()));
            Assert.Equal(StatusCode.Unauthenticated, managementFailure.StatusCode);

            using var raftChannel = GrpcChannel.ForAddress(
                listenAddress,
                new GrpcChannelOptions { HttpHandler = httpHandler });
            var raftRpc = raftChannel.CreateGrpcService<IClusterMembershipService>();
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            var raftFailure = await Assert.ThrowsAsync<RpcException>(() =>
                raftRpc.GetCompatibility(
                    new ClusterCompatibilityRequest(),
                    new ProtoBuf.Grpc.CallContext(new CallOptions(cancellationToken: timeout.Token))));
            Assert.NotEqual(StatusCode.Unauthenticated, raftFailure.StatusCode);

        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
            host.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }

    private static async Task AssertProjectionDidNotMutateBusinessStateAsync(IHost host, bool expectFingerprint)
    {
        await using var context = await host.Services.GetRequiredService<
                Microsoft.EntityFrameworkCore.IDbContextFactory<BridgeDbContext>>()
            .CreateDbContextAsync(TestContext.Current.CancellationToken);
        var projectionState = await context.Set<RaftProjectionState>()
            .SingleOrDefaultAsync(state => state.Id == RaftProjectionState.SingletonId);
        Assert.Equal(expectFingerprint, projectionState?.ConfigurationFingerprint is not null);
        Assert.Empty(await context.Subscriptions.ToListAsync());
        Assert.Empty(await context.SnapshotCheckpoints.ToListAsync());
        Assert.Empty(await context.KeyMappings.ToListAsync());
        Assert.Empty(await context.WakeOutbox.ToListAsync());
    }

    private static int GetAvailablePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    private static string CreateServerCertificate(string path, string password)
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            "CN=127.0.0.1",
            rsa,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(
            X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment,
            false));
        var subjectAlternativeNames = new SubjectAlternativeNameBuilder();
        subjectAlternativeNames.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(subjectAlternativeNames.Build());
        using var certificate = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddMinutes(-1),
            DateTimeOffset.UtcNow.AddDays(1));
        File.WriteAllBytes(path, certificate.Export(X509ContentType.Pfx, password));
        return certificate.GetCertHashString();
    }

    private static async Task WaitForLeadershipAsync(
        DotNext.Net.Cluster.Consensus.Raft.IRaftCluster cluster,
        CancellationToken cancellationToken)
    {
        var timeout = DateTimeOffset.UtcNow.AddSeconds(10);
        while (cluster.LeadershipToken.IsCancellationRequested)
        {
            if (DateTimeOffset.UtcNow >= timeout)
                throw new TimeoutException("The single-node Raft cluster did not elect its leader.");
            await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);
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