using System.Text.Json.Nodes;
using DrasiWake.Core.Domain;
using DrasiWake.Core.Pipeline;
using DrasiWake.IntegrationTests.Fixtures;

namespace DrasiWake.IntegrationTests;

public sealed class CrashRecoveryTests
{
    [Fact]
    public async Task Restart_reconciles_latest_results_before_requeueing_pending_work()
    {
        await using var fixture = await BridgeTestFixture.CreateAsync(TestContext.Current.CancellationToken);
        var pending = await CreatePendingWakeAsync(fixture);
        fixture.DrasiHandler.SetResults("{\"orderId\":\"order-42\",\"state\":\"completed\"}");
        await fixture.RestartDatabaseAsync(TestContext.Current.CancellationToken);

        var reopenedStore = fixture.CreateStore();
        var reconciler = new SnapshotReconciler(fixture.ChangeSource, reopenedStore, fixture.Registry);
        var processed = new TaskCompletionSource<WakeOutboxItem>(TaskCreationOptions.RunContinuationsAsynchronously);
        var partitioner = new SessionPartitioner(
            4,
            1,
            fixture.Registry,
            (item, _) =>
            {
                processed.TrySetResult(item);
                return ValueTask.CompletedTask;
            });
        var recovery = new RecoveryCoordinator(fixture.ChangeSource, reopenedStore, reconciler, partitioner);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var workerTask = partitioner.RunAsync(cancellation.Token);

        try
        {
            await recovery.RecoverAsync(TestContext.Current.CancellationToken);
            var recovered = await processed.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            var outbox = await fixture.ReadOutboxAsync(TestContext.Current.CancellationToken);

            Assert.True(recovery.IsInitialized);
            Assert.NotEqual(pending.Id, recovered.Id);
            Assert.Equal("completed", recovered.Input["facts"]![0]!["state"]!.GetValue<string>());
            Assert.Equal(WakeOutboxStatus.Superseded, Assert.Single(outbox, item => item.Id == pending.Id).Status);
            Assert.Contains("/api/v1/instances/east/queries/orders/results", fixture.DrasiHandler.Paths);
        }
        finally
        {
            cancellation.Cancel();
            try
            {
                await workerTask;
            }
            catch (OperationCanceledException)
            {
            }
            await partitioner.DisposeAsync();
        }
    }

    [Fact]
    public async Task Outbox_persisted_before_dispatch_is_replayed_after_database_reopen_with_the_same_key()
    {
        await using var fixture = await BridgeTestFixture.CreateAsync(TestContext.Current.CancellationToken);
        var pending = await CreatePendingWakeAsync(fixture);
        await fixture.RestartDatabaseAsync(TestContext.Current.CancellationToken);
        var reopenedStore = fixture.CreateStore();
        var recovered = Assert.Single((await reopenedStore.LoadRecoveryStateAsync(TestContext.Current.CancellationToken)).DispatchableItems);
        var dispatcher = new OutboxDispatcher(reopenedStore, fixture.GatewayClient, fixture.Registry);
        var claimed = Assert.Single(await reopenedStore.LoadDispatchableAsync(
            DateTimeOffset.UtcNow.AddSeconds(1), 10, TestContext.Current.CancellationToken));

        Assert.Equal(pending.Id, recovered.Id);
        Assert.Equal(pending.IdempotencyKey, recovered.IdempotencyKey);
        await dispatcher.DispatchOneAsync(claimed, TestContext.Current.CancellationToken);

        var persisted = Assert.Single(await fixture.ReadOutboxAsync(TestContext.Current.CancellationToken));
        Assert.Equal(WakeOutboxStatus.Completed, persisted.Status);
        Assert.Equal(pending.IdempotencyKey, persisted.IdempotencyKey);
        Assert.Equal(1, fixture.GatewayHandler.ExecutionCount);
    }

    [Fact]
    public async Task Gateway_acceptance_before_local_commit_is_replayed_idempotently_after_restart()
    {
        await using var fixture = await BridgeTestFixture.CreateAsync(TestContext.Current.CancellationToken);
        var pending = await CreatePendingWakeAsync(fixture);
        var firstAcceptance = await fixture.GatewayClient.InvokeAsync(ToRequest(pending), TestContext.Current.CancellationToken);

        await fixture.RestartDatabaseAsync(TestContext.Current.CancellationToken);
        var reopenedStore = fixture.CreateStore();
        var recovered = Assert.Single((await reopenedStore.LoadRecoveryStateAsync(TestContext.Current.CancellationToken)).DispatchableItems);
        var claimed = Assert.Single(await reopenedStore.LoadDispatchableAsync(
            DateTimeOffset.UtcNow.AddSeconds(1), 10, TestContext.Current.CancellationToken));
        await new OutboxDispatcher(reopenedStore, fixture.GatewayClient, fixture.Registry)
            .DispatchOneAsync(claimed, TestContext.Current.CancellationToken);

        var persisted = Assert.Single(await fixture.ReadOutboxAsync(TestContext.Current.CancellationToken));
        Assert.Equal(pending.IdempotencyKey, recovered.IdempotencyKey);
        Assert.Equal(firstAcceptance.InvocationId, persisted.InvocationId);
        Assert.Equal(WakeOutboxStatus.Completed, persisted.Status);
        Assert.Equal(1, fixture.GatewayHandler.ExecutionCount);
        Assert.All(fixture.GatewayHandler.Requests, sent => Assert.Equal(pending.IdempotencyKey, sent.Key));
    }

    [Fact]
    public async Task Acceptance_checkpoint_survive_restart_before_execution_receipt_without_false_completion()
    {
        await using var fixture = await BridgeTestFixture.CreateAsync(TestContext.Current.CancellationToken);
        var pending = await CreatePendingWakeAsync(fixture);
        var claimed = Assert.Single(await fixture.Store.LoadDispatchableAsync(
            DateTimeOffset.UtcNow.AddSeconds(1), 10, TestContext.Current.CancellationToken));
        var acceptance = await fixture.GatewayClient.InvokeAsync(ToRequest(claimed), TestContext.Current.CancellationToken);
        var checkpoint = new SnapshotCheckpoint(
            claimed.BindingId,
            claimed.SessionId,
            claimed.SnapshotFingerprint,
            acceptance.AcceptedAtUtc);
        await fixture.Store.MarkAcceptedWithCheckpointAsync(
            claimed.Id,
            acceptance,
            checkpoint,
            TestContext.Current.CancellationToken);

        await fixture.RestartDatabaseAsync(TestContext.Current.CancellationToken);
        var reopenedStore = fixture.CreateStore();
        var recoveryState = await reopenedStore.LoadRecoveryStateAsync(TestContext.Current.CancellationToken);
        var persistedWake = Assert.Single(await fixture.ReadOutboxAsync(TestContext.Current.CancellationToken));
        var persistedCheckpoint = Assert.Single(await fixture.ReadCheckpointsAsync(TestContext.Current.CancellationToken));

        Assert.Empty(recoveryState.DispatchableItems);
        Assert.Equal(pending.IdempotencyKey, persistedWake.IdempotencyKey);
        Assert.Equal(acceptance.InvocationId, persistedWake.InvocationId);
        Assert.Equal(WakeOutboxStatus.Accepted, persistedWake.Status);
        Assert.NotEqual(WakeOutboxStatus.Completed, persistedWake.Status);
        Assert.Equal(checkpoint.Fingerprint, persistedCheckpoint.Fingerprint);
        Assert.Single(fixture.GatewayHandler.Requests);
        Assert.Equal(1, fixture.GatewayHandler.ExecutionCount);
    }

    private static async Task<WakeOutboxItem> CreatePendingWakeAsync(BridgeTestFixture fixture)
    {
        fixture.DrasiHandler.SetResults("{\"orderId\":\"order-42\",\"state\":\"ready\"}");
        await fixture.Reconciler.ReconcileAsync(fixture.Query, TestContext.Current.CancellationToken);
        return Assert.Single((await fixture.Store.LoadRecoveryStateAsync(TestContext.Current.CancellationToken)).DispatchableItems);
    }

    private static WakeRequest ToRequest(WakeOutboxItem item) => new(
        item.BindingId,
        item.SessionId,
        item.Skill,
        item.Input,
        item.IdempotencyKey,
        item.ContractVersion,
        item.TraceId);
}