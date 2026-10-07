using System.Reflection;
using DotNext.Net.Cluster.Consensus.Raft;
using DrasiWake.Host;

namespace DrasiWake.IntegrationTests;

public sealed class LeadershipNotificationLifetimeTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Upstream_notification_releases_waits_without_cancelling_other_upstream(bool leadershipLost)
    {
        using var leader = new CancellationTokenSource();
        using var quorum = new CancellationTokenSource();
        var cluster = DispatchProxy.Create<IRaftCluster, TokenCluster>();
        var proxy = (TokenCluster)(object)cluster;
        proxy.Leader = leader.Token;
        proxy.Quorum = quorum.Token;
        var watcher = new DotNextRaftLeadership(cluster, TimeProvider.System);
        var wait = typeof(DotNextRaftLeadership).GetMethod(
            "WaitForLeadershipNotificationAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var pending = (Task)wait.Invoke(watcher, [CancellationToken.None])!;
        (leadershipLost ? leader : quorum).Cancel();
        await pending.WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);
        var result = pending.GetType().GetProperty("Result")!.GetValue(pending)!;
        Assert.Equal(leadershipLost, result.GetType().GetProperty("LeadershipLost")!.GetValue(result));
        Assert.Equal(!leadershipLost, result.GetType().GetProperty("QuorumLost")!.GetValue(result));
        Assert.False((leadershipLost ? quorum : leader).IsCancellationRequested);
        Assert.Equal(0, RegistrationCount(leader));
        Assert.Equal(0, RegistrationCount(quorum));
    }

    [Fact]
    public async Task Poll_and_watcher_cancellation_release_all_notification_registrations()
    {
        using var leader = new CancellationTokenSource();
        using var quorum = new CancellationTokenSource();
        var cluster = DispatchProxy.Create<IRaftCluster, TokenCluster>();
        var proxy = (TokenCluster)(object)cluster;
        proxy.Leader = leader.Token;
        proxy.Quorum = quorum.Token;
        var watcher = new DotNextRaftLeadership(cluster, TimeProvider.System);
        var wait = typeof(DotNextRaftLeadership).GetMethod(
            "WaitForLeadershipNotificationAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        for (var index = 0; index < 12; index++)
        {
            await ((Task)wait.Invoke(watcher, [CancellationToken.None])!);
            Assert.Equal(0, RegistrationCount(leader));
            Assert.Equal(0, RegistrationCount(quorum));
        }
        using var stop = new CancellationTokenSource();
        var pending = (Task)wait.Invoke(watcher, [stop.Token])!;
        stop.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        Assert.Equal(0, RegistrationCount(leader));
        Assert.Equal(0, RegistrationCount(quorum));
        Assert.False(leader.IsCancellationRequested);
        Assert.False(quorum.IsCancellationRequested);
    }

    private static int RegistrationCount(CancellationTokenSource source)
    {
        // Measure the actual retained callback list, not just the observed leadership DTO.
        const BindingFlags fields = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
        var registrations = typeof(CancellationTokenSource).GetField("_registrations", fields)!.GetValue(source);
        if (registrations is null)
            return 0;
        var node = registrations.GetType().GetField("Callbacks", fields)!.GetValue(registrations);
        var count = 0;
        while (node is not null)
        {
            count++;
            node = node.GetType().GetField("Next", fields)!.GetValue(node);
        }
        return count;
    }

    public class TokenCluster : DispatchProxy
    {
        public CancellationToken Leader { get; set; }
        public CancellationToken Quorum { get; set; }
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) => targetMethod?.Name switch
        {
            "get_LeadershipToken" => Leader,
            "get_ConsensusToken" => Quorum,
            _ => throw new NotSupportedException(targetMethod?.Name)
        };
    }
}
