using DotNext.Net.Cluster;
using DrasiWake.Adapters.DrasiServer;
using DrasiWake.Core.Domain;
using DrasiWake.Host;
using DrasiWake.IntegrationTests.Fixtures;
using DrasiWake.Persistence.Raft;
using Microsoft.Extensions.DependencyInjection;

namespace DrasiWake.IntegrationTests;

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class RaftFailoverCollection
{
    public const string Name = "Raft failover";
}

[Collection(RaftFailoverCollection.Name)]
public sealed class RaftFailoverTests
{
    [Theory]
    [InlineData(DrasiSnapshotFailure.ServiceUnavailable)]
    [InlineData(DrasiSnapshotFailure.TruncatedBody)]
    public async Task Stable_leader_retries_actual_Drasi_snapshot_and_recovers_before_gateway_dispatch(
        DrasiSnapshotFailure snapshotFailure)
    {
        await using var cluster = await RaftClusterFixture.CreateAsync(
            3, TestContext.Current.CancellationToken, authoritativeSnapshot: true, snapshotFailure: snapshotFailure);
        var leader = await cluster.WaitForLeaderAsync(TestContext.Current.CancellationToken);
        var term = cluster.Term(leader);
        try
        {
            await cluster.SnapshotRequested.WaitAsync(TimeSpan.FromSeconds(8), TestContext.Current.CancellationToken);
            Assert.Equal(term, cluster.Term(leader));
            Assert.Empty(cluster.Gateway.Requests);
            Assert.Empty((await cluster.ReadProjectionAsync(leader, TestContext.Current.CancellationToken)).WakeOutbox);
            cluster.ReleaseSnapshot();
            await cluster.Gateway.FirstRequestReceived.WaitAsync(
                TimeSpan.FromSeconds(8), TestContext.Current.CancellationToken);
            Assert.Equal(term, cluster.Term(leader));
            var snapshotWake = Assert.Single(
                (await cluster.ReadProjectionAsync(leader, TestContext.Current.CancellationToken)).WakeOutbox);
            Assert.Equal("orders-binding", snapshotWake.BindingId);
            Assert.Equal("singleton", snapshotWake.SessionId);
            Assert.Contains("snapshot-order", snapshotWake.InputJson);
            Assert.Equal(snapshotWake.IdempotencyKey, Assert.Single(cluster.Gateway.Requests).IdempotencyKey);
        }
        finally
        {
            cluster.ReleaseSnapshot();
        }
    }

    [Fact]
    public async Task Truncated_actual_Drasi_result_body_raises_ResponseEnded_HttpIOException()
    {
        await using var services = await LocalClusterServices.StartAsync(
            TestContext.Current.CancellationToken, authoritativeSnapshot: true,
            snapshotFailure: DrasiSnapshotFailure.TruncatedBody);
        using var httpClient = new HttpClient();
        var client = new DrasiServerClient(httpClient);
        var exception = await Assert.ThrowsAsync<HttpIOException>(() =>
            client.ReadSnapshotAsync(new QueryIdentity(services.BaseAddress, "east", "orders"),
                TestContext.Current.CancellationToken).AsTask());
        Assert.Equal(HttpRequestError.ResponseEnded, exception.HttpRequestError);
    }

    [Fact]
    public async Task Replicated_projection_survives_follower_restart_from_its_snapshot_and_log()
    {
        const int snapshotFrequency = 8;
        await using var cluster = await RaftClusterFixture.CreateAsync(
            3, TestContext.Current.CancellationToken, snapshotFrequency);
        var leader = await cluster.WaitForLeaderAsync(TestContext.Current.CancellationToken);
        var acceptedWake = await cluster.CreatePendingWakeAsync(leader, TestContext.Current.CancellationToken);
        var acceptedAt = DateTimeOffset.UtcNow;
        await cluster.Store(leader).MarkAcceptedWithCheckpointAsync(
            acceptedWake.Id,
            new WakeAcceptance("snapshot-invocation", acceptedAt),
            new SnapshotCheckpoint(
                acceptedWake.BindingId, acceptedWake.SessionId, acceptedWake.SnapshotFingerprint, acceptedAt),
            TestContext.Current.CancellationToken);
        var acceptedIndex = cluster.LastCommittedIndex(leader);
        var follower = cluster.Nodes.First(node => node != leader);
        for (var index = 0; index < snapshotFrequency; index++)
            await cluster.CreatePendingWakeAsync(leader, TestContext.Current.CancellationToken);
        await cluster.WaitForAllAvailableProjectionsAsync(
            cluster.LastCommittedIndex(leader), TestContext.Current.CancellationToken);
        await cluster.WaitForPersistedSnapshotAsync(follower, acceptedIndex, TestContext.Current.CancellationToken);
        var persistedSnapshot = await cluster.ReadLatestPersistedSnapshotAsync(
            follower, TestContext.Current.CancellationToken);
        Assert.NotNull(persistedSnapshot);
        Assert.Equal("snapshot-invocation",
            persistedSnapshot.WakeOutbox.Single(item => item.Id == acceptedWake.Id).InvocationId);

        var wake = await cluster.CreatePendingWakeAsync(leader, TestContext.Current.CancellationToken);
        var committedIndex = cluster.LastCommittedIndex(leader);

        await cluster.WaitForAllAvailableProjectionsAsync(committedIndex, TestContext.Current.CancellationToken);
        await cluster.AssertAvailableProjectionsEqualAsync(TestContext.Current.CancellationToken);
        var expectedState = await cluster.ReadProjectionAsync(follower, TestContext.Current.CancellationToken);

        await cluster.StopNodeWithoutMembershipChangeAsync(follower, TestContext.Current.CancellationToken);
        var stoppedSnapshot = await cluster.ReadLatestPersistedSnapshotAsync(
            follower, TestContext.Current.CancellationToken);
        Assert.NotNull(stoppedSnapshot);
        Assert.True(stoppedSnapshot.LastAppliedIndex < committedIndex);
        Assert.DoesNotContain(stoppedSnapshot.WakeOutbox, item => item.Id == wake.Id);
        cluster.SetAsideStoppedProjection(follower);
        Assert.False(Directory.Exists(follower.DatabasePath));
        await cluster.RestartNodeAsync(follower, TestContext.Current.CancellationToken);
        await cluster.WaitForProjectionAsync(follower, committedIndex, TestContext.Current.CancellationToken);

        var recoveredState = await cluster.ReadProjectionAsync(follower, TestContext.Current.CancellationToken);
        Assert.Equal(wake.IdempotencyKey, recoveredState.WakeOutbox.Single(item => item.Id == wake.Id).IdempotencyKey);
        Assert.Equal("snapshot-invocation",
            recoveredState.WakeOutbox.Single(item => item.Id == acceptedWake.Id).InvocationId);
        Assert.Equal(acceptedWake.SnapshotFingerprint, Assert.Single(recoveredState.SnapshotCheckpoints).Fingerprint);
        Assert.True(cluster.BusinessStateEquals(expectedState, recoveredState),
            $"Before restart: {System.Text.Json.JsonSerializer.Serialize(expectedState)}\nAfter restart: {System.Text.Json.JsonSerializer.Serialize(recoveredState)}");
        await cluster.AssertAvailableProjectionsEqualAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task New_leader_replays_gateway_acceptance_with_the_same_idempotency_key()
    {
        await using var cluster = await RaftClusterFixture.CreateAsync(3, TestContext.Current.CancellationToken);
        var oldLeader = await cluster.WaitForLeaderAsync(TestContext.Current.CancellationToken);
        var oldTerm = cluster.Term(oldLeader);
        cluster.Gateway.HoldAcceptedResponse();
        var wake = await cluster.CreatePendingWakeAsync(
            oldLeader, TestContext.Current.CancellationToken, dispatchImmediately: true);
        await cluster.Gateway.FirstRequestReceived.WaitAsync(
            RaftClusterFixture.OperationTimeout,
            TestContext.Current.CancellationToken);
        var stateBeforeFailure = await cluster.ReadProjectionAsync(oldLeader, TestContext.Current.CancellationToken);
        Assert.Equal(WakeOutboxStatus.Dispatching, Assert.Single(stateBeforeFailure.WakeOutbox).Status);
        Assert.Empty(stateBeforeFailure.SnapshotCheckpoints);
        await cluster.WaitForAllAvailableProjectionsAsync(
            cluster.LastCommittedIndex(oldLeader), TestContext.Current.CancellationToken);
        await cluster.StopNodeWithoutMembershipChangeAsync(oldLeader, TestContext.Current.CancellationToken);
        cluster.Gateway.ReleaseFirstResponseAsTimeout();

        var newLeader = await cluster.WaitForLeaderAsync(
            TestContext.Current.CancellationToken,
            minimumTermExclusive: oldTerm);
        await cluster.WaitForWakeStatusAsync(
            newLeader,
            wake.Id,
            WakeOutboxStatus.Completed,
            TestContext.Current.CancellationToken);

        Assert.Single(cluster.Gateway.LogicalInvocations);
        Assert.True(cluster.Gateway.Requests.Count >= 2);
        Assert.All(cluster.Gateway.Requests, request => Assert.Equal(wake.IdempotencyKey, request.IdempotencyKey));
        Assert.Equal(
            WakeOutboxStatus.Completed,
            (await cluster.ReadProjectionAsync(newLeader, TestContext.Current.CancellationToken)).WakeOutbox.Single().Status);
        await cluster.AssertAvailableProjectionsEqualAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Losing_quorum_prevents_commands_and_gateway_dispatch_until_a_member_recovers()
    {
        await using var cluster = await RaftClusterFixture.CreateAsync(3, TestContext.Current.CancellationToken);
        var leader = await cluster.WaitForLeaderAsync(TestContext.Current.CancellationToken);
        var pending = await cluster.CreatePendingWakeAsync(leader, TestContext.Current.CancellationToken);
        var committedIndex = cluster.LastCommittedIndex(leader);
        await cluster.WaitForAllAvailableProjectionsAsync(committedIndex, TestContext.Current.CancellationToken);
        var survivor = leader;
        var stopped = cluster.Nodes.Where(node => node != survivor).ToArray();
        foreach (var node in stopped)
            await cluster.StopNodeWithoutMembershipChangeAsync(node, TestContext.Current.CancellationToken);

        await cluster.WaitForQuorumLossAsync(survivor, TestContext.Current.CancellationToken);
        var liveMembers = survivor.Cluster.Members.ToArray();
        var availableMembers = liveMembers.Count(member =>
        {
            try
            {
                return member.Status == ClusterMemberStatus.Available;
            }
            catch (ObjectDisposedException)
            {
                return false;
            }
        });
        Assert.True(availableMembers < liveMembers.Length / 2 + 1);
        var leadership = survivor.Services.GetRequiredService<IRaftLeadership>();
        Assert.False(leadership.HasQuorum);
        var before = await cluster.ReadProjectionAsync(survivor, TestContext.Current.CancellationToken);
        var rejectedWake = cluster.CreateWake();
        var writeRejection = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            cluster.Store(survivor).CreateOrUpdatePendingWakeAsync(
                rejectedWake,
                TestContext.Current.CancellationToken).AsTask());
        Assert.True(writeRejection.Message.Contains("quorum", StringComparison.Ordinal) ||
            writeRejection.Message.Contains("leader", StringComparison.Ordinal));
        await Assert.ThrowsAnyAsync<InvalidOperationException>(() =>
            cluster.Store(survivor).LoadDispatchableAsync(
                DateTimeOffset.UtcNow.AddHours(2),
                10,
                TestContext.Current.CancellationToken).AsTask());

        var after = await cluster.ReadProjectionAsync(survivor, TestContext.Current.CancellationToken);
        Assert.True(cluster.BusinessStateEquals(before, after));
        Assert.Empty(cluster.Gateway.LogicalInvocations);
        Assert.Equal(pending.Id, Assert.Single(after.WakeOutbox).Id);

        await cluster.RestartNodeAsync(stopped[0], TestContext.Current.CancellationToken);
        var recoveredLeader = await cluster.WaitForLeaderAsync(TestContext.Current.CancellationToken);
        var restoredIndex = cluster.LastCommittedIndex(recoveredLeader);
        await cluster.WaitForAllAvailableProjectionsAsync(restoredIndex, TestContext.Current.CancellationToken);
        var postRecoveryWake = await cluster.CreatePendingWakeAsync(
            recoveredLeader,
            TestContext.Current.CancellationToken);
        await cluster.WaitForAllAvailableProjectionsAsync(
            cluster.LastCommittedIndex(recoveredLeader),
            TestContext.Current.CancellationToken);
        await cluster.AssertAvailableProjectionsEqualAsync(TestContext.Current.CancellationToken);
        Assert.NotEqual(pending.Id, postRecoveryWake.Id);
    }

    [Fact]
    public async Task Authenticated_membership_add_catchup_and_remove_preserve_the_remaining_quorum()
    {
        await using var cluster = await RaftClusterFixture.CreateAsync(
            3,
            TestContext.Current.CancellationToken,
            snapshotFrequency: 100);
        var leader = await cluster.WaitForLeaderAsync(TestContext.Current.CancellationToken);
        var priorWake = await cluster.CreatePendingWakeAsync(leader, TestContext.Current.CancellationToken);
        var candidate = await cluster.StartFourthNodeAsync(TestContext.Current.CancellationToken);

        await cluster.AddMemberThroughManagementGrpcAsync(
            leader,
            candidate,
            TestContext.Current.CancellationToken);
        Assert.Contains(candidate.ListenAddress, cluster.Members(leader));
        var addIndex = cluster.LastCommittedIndex(leader);
        await cluster.WaitForProjectionAsync(candidate, addIndex, TestContext.Current.CancellationToken);
        Assert.Contains(
            (await cluster.ReadProjectionAsync(candidate, TestContext.Current.CancellationToken)).WakeOutbox,
            item => item.Id == priorWake.Id);
        await cluster.AssertAvailableProjectionsEqualAsync(TestContext.Current.CancellationToken);

        await cluster.RemoveMemberThroughManagementGrpcAsync(
            leader,
            candidate,
            TestContext.Current.CancellationToken);
        Assert.DoesNotContain(candidate.ListenAddress, cluster.Members(leader));
        var indexAtRemoval = await cluster.LastAppliedIndexAsync(
            candidate,
            TestContext.Current.CancellationToken);
        var wake = await cluster.CreatePendingWakeAsync(leader, TestContext.Current.CancellationToken);
        var laterIndex = cluster.LastCommittedIndex(leader);
        await cluster.WaitForAllAvailableProjectionsAsync(laterIndex, TestContext.Current.CancellationToken);

        Assert.True(laterIndex > indexAtRemoval);
        Assert.Equal(
            indexAtRemoval,
            await cluster.LastAppliedIndexAsync(candidate, TestContext.Current.CancellationToken));
        Assert.DoesNotContain(
            (await cluster.ReadProjectionAsync(candidate, TestContext.Current.CancellationToken))
                .WakeOutbox,
            item => item.Id == wake.Id);
    }
}
