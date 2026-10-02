using System.Text.Json;
using DrasiWake.Core.Domain;
using DrasiWake.Core.Pipeline;
using DrasiWake.IntegrationTests.Fixtures;

namespace DrasiWake.IntegrationTests;

public sealed class DrasiConvergenceTests
{
    [Fact]
    public async Task Results_read_failure_preserves_persisted_checkpoint_and_outbox()
    {
        await using var fixture = await BridgeTestFixture.CreateAsync(TestContext.Current.CancellationToken);
        fixture.DrasiHandler.SetResults("{\"orderId\":\"order-42\",\"state\":\"accepted\"}");
        await fixture.Reconciler.ReconcileAsync(fixture.Query, TestContext.Current.CancellationToken);
        var pending = Assert.Single(await fixture.Store.LoadDispatchableAsync(
            DateTimeOffset.UtcNow.AddSeconds(1), 10, TestContext.Current.CancellationToken));
        await fixture.Dispatcher.DispatchOneAsync(pending, TestContext.Current.CancellationToken);

        var originalCheckpoint = Assert.Single(await fixture.ReadCheckpointsAsync(TestContext.Current.CancellationToken));
        var originalOutboxIds = (await fixture.ReadOutboxAsync(TestContext.Current.CancellationToken))
            .Select(item => item.Id)
            .Order()
            .ToArray();

        fixture.DrasiHandler.FailResultsReads();
        await Assert.ThrowsAsync<HttpRequestException>(
            () => fixture.Reconciler.ReconcileAsync(fixture.Query, TestContext.Current.CancellationToken));

        var persistedCheckpoint = Assert.Single(await fixture.ReadCheckpointsAsync(TestContext.Current.CancellationToken));
        var persistedOutboxIds = (await fixture.ReadOutboxAsync(TestContext.Current.CancellationToken))
            .Select(item => item.Id)
            .Order()
            .ToArray();
        Assert.Equal(originalCheckpoint.BindingId, persistedCheckpoint.BindingId);
        Assert.Equal(originalCheckpoint.SessionId, persistedCheckpoint.SessionId);
        Assert.Equal(originalCheckpoint.Fingerprint, persistedCheckpoint.Fingerprint);
        Assert.Equal(originalCheckpoint.AcceptedAtUtc, persistedCheckpoint.AcceptedAtUtc);
        Assert.Equal(originalOutboxIds, persistedOutboxIds);
    }

    [Fact]
    public async Task Repeated_reordered_and_lost_hints_converge_to_the_latest_results_snapshot()
    {
        await using var fixture = await BridgeTestFixture.CreateAsync(TestContext.Current.CancellationToken);
        var partitioner = new SessionPartitioner(8, 1, fixture.Registry, (_, _) => ValueTask.CompletedTask);
        await using var partitionerScope = partitioner;
        var recovery = new RecoveryCoordinator(fixture.ChangeSource, fixture.Store, fixture.Reconciler, partitioner);
        var inbox = new SignalInbox(1);

        fixture.DrasiHandler.SetResults("{\"orderId\":\"order-42\",\"state\":\"new\"}");
        await fixture.Reconciler.ReconcileAsync(fixture.Query, TestContext.Current.CancellationToken);
        await fixture.Reconciler.ReconcileAsync(fixture.Query, TestContext.Current.CancellationToken);
        var firstPending = Assert.Single((await fixture.Store.LoadRecoveryStateAsync(TestContext.Current.CancellationToken)).DispatchableItems);

        var observedAt = DateTimeOffset.UtcNow;
        Assert.True(inbox.TryWrite(new ChangeSignal(fixture.Query, observedAt.AddSeconds(1), "newer-hint")));
        Assert.False(inbox.TryWrite(new ChangeSignal(fixture.Query, observedAt, "older-hint")));
        fixture.DrasiHandler.SetResults("{\"orderId\":\"order-42\",\"state\":\"processing\"}");
        Assert.True(inbox.TryRead(out var queuedSignal));
        await recovery.ReconcileAfterReconnectAsync(queuedSignal!.Query, TestContext.Current.CancellationToken);

        fixture.DrasiHandler.SetResults("{\"orderId\":\"order-42\",\"state\":\"completed\"}");
        await recovery.ReconcileAfterReconnectAsync(fixture.Query, TestContext.Current.CancellationToken);
        foreach (var dirtyQuery in inbox.TakeDirtyQueries())
            await recovery.ReconcileAfterReconnectAsync(dirtyQuery, TestContext.Current.CancellationToken);

        var currentState = await fixture.Store.LoadRecoveryStateAsync(TestContext.Current.CancellationToken);
        var latestPending = Assert.Single(currentState.DispatchableItems);
        Assert.NotEqual(firstPending.Id, latestPending.Id);
        Assert.Equal("completed", latestPending.Input["facts"]![0]!["state"]!.GetValue<string>());
        Assert.True(fixture.DrasiHandler.ResultsReadCount >= 5);

        var claimed = Assert.Single(await fixture.Store.LoadDispatchableAsync(
            DateTimeOffset.UtcNow.AddSeconds(1), 10, TestContext.Current.CancellationToken));
        await fixture.Dispatcher.DispatchOneAsync(claimed, TestContext.Current.CancellationToken);

        var persistedOutbox = await fixture.ReadOutboxAsync(TestContext.Current.CancellationToken);
        var persistedCheckpoint = Assert.Single(await fixture.ReadCheckpointsAsync(TestContext.Current.CancellationToken));
        var acceptedWake = Assert.Single(persistedOutbox, item => item.Id == latestPending.Id);
        Assert.Equal(WakeOutboxStatus.Completed, acceptedWake.Status);
        Assert.Equal(latestPending.SnapshotFingerprint, persistedCheckpoint.Fingerprint);
        Assert.Equal("drasiwake:" + latestPending.Id.ToString("N"), acceptedWake.IdempotencyKey);
        Assert.Equal(1, fixture.GatewayHandler.ExecutionCount);

        var gatewayRequests = fixture.GatewayHandler.Requests.ToArray();
        Assert.Equal(2, gatewayRequests.Length);
        Assert.All(gatewayRequests, sent => Assert.Equal(acceptedWake.IdempotencyKey, sent.Key));
        Assert.Equal(gatewayRequests[0].Body, gatewayRequests[1].Body);
        using var requestJson = JsonDocument.Parse(gatewayRequests[0].Body);
        Assert.Equal("triage-order", requestJson.RootElement.GetProperty("skill").GetString());
        Assert.Equal("singleton", requestJson.RootElement.GetProperty("sessionId").GetString());
        using var wakeInput = JsonDocument.Parse(requestJson.RootElement.GetProperty("input").GetString()!);
        Assert.Equal("completed", wakeInput.RootElement.GetProperty("facts")[0].GetProperty("state").GetString());
        Assert.Contains("/api/v1/instances/east/queries/orders/results", fixture.DrasiHandler.Paths);
        Assert.DoesNotContain(fixture.DrasiHandler.Paths, path => path.Contains("events/stream", StringComparison.Ordinal));
    }
}