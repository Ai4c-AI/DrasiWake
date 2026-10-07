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
        Assert.Null(configuration["DrasiWake:Cluster:Certificate:Password"]);
        Assert.Null(configuration["DrasiWake:Cluster:Management:BearerToken"]);
        Assert.Null(configuration["DrasiWake:OpenClaw:Targets:sample-gateway:BearerToken"]);
    }

    private static string RepositoryRoot()
    {
        for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
            if (File.Exists(Path.Combine(directory.FullName, "DrasiWake.sln")))
                return directory.FullName;
        throw new InvalidOperationException("Repository root was not found.");
    }
}
