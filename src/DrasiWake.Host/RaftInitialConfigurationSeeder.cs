using DotNext.Net;
using DotNext.Net.Cluster.Consensus.Raft.Membership;
using Microsoft.AspNetCore.Connections;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace DrasiWake.Host;

internal sealed class RaftInitialConfigurationSeeder(
    IConfiguration configuration,
    RaftClusterSettings settings,
    IClusterConfigurationStorage<UriEndPoint> storage) : IHostedService
{
    private const string InitialMembersConfigurationKey = "DrasiWake:Cluster:BootstrapMembers";

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var current = await storage.LoadConfigurationAsync(cancellationToken).ConfigureAwait(false);
        if (current.Members.Count != 0)
            return;

        var members = ReadInitialMembers(configuration, settings);
        var seeded = current;
        foreach (var member in members)
            seeded = seeded.Add(new UriEndPoint(member));

        await storage.SaveConfigurationAsync(seeded, 0L, cancellationToken).ConfigureAwait(false);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private static IReadOnlyList<Uri> ReadInitialMembers(
        IConfiguration configuration,
        RaftClusterSettings settings)
    {
        var configuredMembers = configuration.GetSection(InitialMembersConfigurationKey)
            .GetChildren()
            .OrderBy(section => int.TryParse(section.Key, out var index) ? index : int.MaxValue)
            .Select(section => section.Value)
            .ToArray();
        if (configuredMembers.Length == 0)
            return settings.InitialMembers;

        var members = new List<Uri>(configuredMembers.Length);
        foreach (var value in configuredMembers)
        {
            if (string.IsNullOrWhiteSpace(value) ||
                !Uri.TryCreate(value, UriKind.Absolute, out var endpoint) ||
                !settings.InitialMembers.Contains(endpoint))
            {
                throw new InvalidOperationException(
                    $"Configuration '{InitialMembersConfigurationKey}' must contain absolute initial-member endpoints.");
            }

            members.Add(endpoint);
        }

        return members;
    }
}
