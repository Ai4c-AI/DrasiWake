using DrasiWake.Host;
using Microsoft.Extensions.Configuration;

namespace DrasiWake.IntegrationTests;

public sealed class HaConfigurationDocumentationTests
{
    [Fact]
    public void Three_documented_nodes_validate_with_secret_provider_overrides_and_independent_paths()
    {
        var root = RepositoryRoot();
        var operations = File.ReadAllText(Path.Combine(root, "docs", "bridge-core-v1-operations.md"));
        var nodes = new List<DrasiWakeHostSettings>();
        foreach (var name in new[] { "node-a", "node-b", "node-c" })
        {
            var relativePath = $"docs\\examples\\raft\\{name}.json";
            var path = Path.Combine(root, relativePath);
            Assert.True(File.Exists(path), $"Missing documented configuration: {relativePath}");
            Assert.Contains($"{name}.json", operations, StringComparison.Ordinal);
            var secretless = new ConfigurationBuilder().AddJsonFile(path).Build();
            Assert.Null(secretless["DrasiWake:Cluster:Certificate:Password"]);
            Assert.Null(secretless["DrasiWake:Cluster:Management:BearerToken"]);
            var configuration = new ConfigurationBuilder().AddConfiguration(secretless)
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["DrasiWake:Cluster:Certificate:Password"] = "test-only-not-a-deployment-secret",
                    ["DrasiWake:Cluster:Management:BearerToken"] = "shared-test-only-not-a-deployment-secret"
                }).Build();
            var settings = DrasiWakeHostSettings.FromConfiguration(configuration);
            settings.Validate();
            Assert.Equal(RaftClusterMode.Cluster, settings.Cluster.Mode);
            Assert.Equal(name, settings.Cluster.NodeId);
            Assert.Equal(3, settings.Cluster.InitialMembers.Count);
            Assert.NotEqual(settings.DatabasePath, settings.Cluster.RaftDataPath);
            nodes.Add(settings);
        }
        Assert.Equal(3, nodes.Select(n => n.Cluster.NodeId).Distinct().Count());
        Assert.Equal(3, nodes.Select(n => n.Cluster.ListenAddress).Distinct().Count());
        Assert.Equal(3, nodes.Select(n => n.Cluster.ManagementAddress).Distinct().Count());
        Assert.All(nodes, node => Assert.Equal(nodes[0].Cluster.ManagementAddress!.Port, node.Cluster.ManagementAddress!.Port));
        Assert.Equal(6, nodes.SelectMany(n => new[] { n.DatabasePath, n.Cluster.RaftDataPath }).Distinct().Count());
        Assert.All(nodes, node => Assert.Equal(nodes[0].Cluster.InitialMembers, node.Cluster.InitialMembers));
    }

    [Fact]
    public void Checked_in_developer_settings_remain_secretless_single_node()
    {
        var configuration = new ConfigurationBuilder()
            .AddJsonFile(Path.Combine(RepositoryRoot(), "src", "DrasiWake.Host", "appsettings.json")).Build();
        var settings = DrasiWakeHostSettings.FromConfiguration(configuration);
        settings.Validate();
        Assert.Equal(RaftClusterMode.SingleNode, settings.Cluster.Mode);
        Assert.True(settings.Cluster.ListenAddress.IsLoopback);
        Assert.Single(settings.Cluster.InitialMembers);
        // The developer default keeps the admin plane on loopback without a token.
        Assert.True(settings.Admin.ApiAddress.IsLoopback);
        Assert.Null(settings.Admin.BearerToken);
        Assert.Null(configuration["DrasiWake:Cluster:Certificate:Password"]);
        Assert.Null(configuration["DrasiWake:Cluster:Management:BearerToken"]);
        Assert.Null(configuration["DrasiWake:OpenClaw:Targets:sample-gateway:BearerToken"]);
    }

    [Fact]
    public void Admin_plane_keys_are_documented_and_example_nodes_bind_distinct_admin_ports()
    {
        var root = RepositoryRoot();
        var operations = File.ReadAllText(Path.Combine(root, "docs", "bridge-core-v1-operations.md"));

        // The operations manual must document every new configuration key and the
        // operator entry points (gate: a new key without docs turns this red).
        Assert.Contains("DrasiWake:Admin:ApiAddress", operations, StringComparison.Ordinal);
        Assert.Contains("DrasiWake:Admin:DownstreamProbeInterval", operations, StringComparison.Ordinal);
        Assert.Contains("DrasiWake:Admin:CommitLagDegradedAfter", operations, StringComparison.Ordinal);
        Assert.Contains("DrasiWake:Admin:BearerToken", operations, StringComparison.Ordinal);
        Assert.Contains("drasiwakectl", operations, StringComparison.Ordinal);
        Assert.Contains("/readyz", operations, StringComparison.Ordinal);
        Assert.Contains("outbox.not_replayable", operations, StringComparison.Ordinal);

        foreach (var name in new[] { "node-a", "node-b", "node-c" })
        {
            var relativePath = $"docs\\examples\\raft\\{name}.json";
            var path = Path.Combine(root, relativePath);
            Assert.True(File.Exists(path), $"Missing documented configuration: {relativePath}");
            var configuration = new ConfigurationBuilder().AddJsonFile(path).Build();

            // Examples stay secretless; the admin token is injected per deployment.
            Assert.Null(configuration["DrasiWake:Admin:BearerToken"]);

            // Cluster examples require the certificate password / management token at
            // parse time; inject test-only values (real deployments use a secret provider).
            var configurationWithSecrets = new ConfigurationBuilder().AddConfiguration(configuration)
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["DrasiWake:Cluster:Certificate:Password"] = "test-only-not-a-deployment-secret",
                    ["DrasiWake:Cluster:Management:BearerToken"] = "shared-test-only-not-a-deployment-secret"
                }).Build();
            var settings = DrasiWakeHostSettings.FromConfiguration(configurationWithSecrets);
            settings.Validate();
            var adminPort = settings.Admin.ApiAddress.Port;
            Assert.NotEqual(settings.Cluster.ListenAddress.Port, adminPort);
            if (settings.Cluster.ManagementAddress is { } managementAddress)
                Assert.NotEqual(managementAddress.Port, adminPort);
            Assert.True(settings.Admin.DownstreamProbeInterval > TimeSpan.Zero);
            Assert.True(settings.Admin.CommitLagDegradedAfter > TimeSpan.Zero);
        }
    }

    private static string RepositoryRoot()
    {
        for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
            if (File.Exists(Path.Combine(directory.FullName, "DrasiWake.sln")))
                return directory.FullName;
        throw new InvalidOperationException("Repository root was not found.");
    }
}
