using System.Collections.Concurrent;
using System.Text.Json.Nodes;
using DrasiWake.Core.Contracts;
using DrasiWake.Core.Domain;
using DrasiWake.Core.Pipeline;
using DrasiWake.Core.Tests;

namespace DrasiWake.Core.Tests.Pipeline;

public sealed class SessionPartitionerTests
{
    [Fact]
    public async Task Same_session_is_fifo_while_another_session_runs_concurrently()
    {
        var firstSessionStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirstSession = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondSessionCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondFirstSessionItemCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var completed = new ConcurrentQueue<string>();

        async ValueTask ProcessAsync(WakeOutboxItem item, CancellationToken cancellationToken)
        {
            if (item.TraceId == "A1")
            {
                firstSessionStarted.TrySetResult();
                await releaseFirstSession.Task.WaitAsync(cancellationToken);
            }
            else if (item.TraceId == "B1")
            {
                secondSessionCompleted.TrySetResult();
            }

            completed.Enqueue(item.TraceId!);
            if (item.TraceId == "A2")
                secondFirstSessionItemCompleted.TrySetResult();
        }

        var partitioner = new SessionPartitioner(8, 2, CreateRegistry(), ProcessAsync);
        using var cancellation = new CancellationTokenSource();
        var runTask = partitioner.RunAsync(cancellation.Token);

        await partitioner.EnqueueAsync(CreateItem("session-A", "A1"), TestContext.Current.CancellationToken);
        await firstSessionStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
        await partitioner.EnqueueAsync(CreateItem("session-A", "A2"), TestContext.Current.CancellationToken);
        await partitioner.EnqueueAsync(CreateItem("session-B", "B1"), TestContext.Current.CancellationToken);

        await secondSessionCompleted.Task.WaitAsync(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
        Assert.False(secondFirstSessionItemCompleted.Task.IsCompleted);

        releaseFirstSession.TrySetResult();
        await secondFirstSessionItemCompleted.Task.WaitAsync(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
        var completedItems = completed.ToArray();
        Assert.True(Array.IndexOf(completedItems, "A1") < Array.IndexOf(completedItems, "A2"));
        Assert.Contains("B1", completedItems);

        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => runTask);
        await partitioner.DisposeAsync();
    }

    [Fact]
    public async Task Binding_rate_limit_waits_without_dropping_outbox_work()
    {
        var timeProvider = new ManualTimeProvider(DateTimeOffset.Parse("2026-01-01T00:00:00Z"));
        var firstCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var executed = new ConcurrentQueue<string>();

        ValueTask ProcessAsync(WakeOutboxItem item, CancellationToken cancellationToken)
        {
            executed.Enqueue(item.TraceId!);
            (item.TraceId == "first" ? firstCompleted : secondCompleted).TrySetResult();
            return ValueTask.CompletedTask;
        }

        var partitioner = new SessionPartitioner(
            4,
            2,
            CreateRegistry(new RateLimitPolicy(1, TimeSpan.FromMinutes(1))),
            ProcessAsync,
            timeProvider);
        using var cancellation = new CancellationTokenSource();
        var runTask = partitioner.RunAsync(cancellation.Token);
        try
        {
            var first = CreateItem("session-A", "first") with
            {
                CreatedAtUtc = timeProvider.GetUtcNow(),
                NextAttemptAtUtc = timeProvider.GetUtcNow()
            };
            await partitioner.EnqueueAsync(first, TestContext.Current.CancellationToken);
            await firstCompleted.Task.WaitAsync(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
            var second = CreateItem("session-B", "second") with
            {
                CreatedAtUtc = timeProvider.GetUtcNow(),
                NextAttemptAtUtc = timeProvider.GetUtcNow().AddMinutes(2)
            };
            await partitioner.EnqueueAsync(second, TestContext.Current.CancellationToken);
            await Task.Delay(TimeSpan.FromMilliseconds(100), TestContext.Current.CancellationToken);
            if (runTask.IsFaulted)
                await runTask;
            Assert.False(secondCompleted.Task.IsCompleted);
            await timeProvider.WaitForTimerAsync(TestContext.Current.CancellationToken);

            Assert.Equal(["first"], executed.ToArray());
            timeProvider.Advance(TimeSpan.FromMinutes(1));
            Assert.False(secondCompleted.Task.IsCompleted);
            timeProvider.Advance(TimeSpan.FromMinutes(1));
            await secondCompleted.Task.WaitAsync(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
            Assert.Equal(["first", "second"], executed.ToArray());
        }
        finally
        {
            cancellation.Cancel();
            try
            {
                await runTask;
            }
            catch (OperationCanceledException)
            {
            }
            await partitioner.DisposeAsync();
        }
    }

    [Fact]
    public async Task Duplicate_outbox_enqueue_is_processed_once()
    {
        var processStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseProcess = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var callCount = 0;
        async ValueTask ProcessAsync(WakeOutboxItem item, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref callCount);
            processStarted.TrySetResult();
            await releaseProcess.Task.WaitAsync(cancellationToken);
        }

        var partitioner = new SessionPartitioner(4, 1, CreateRegistry(), ProcessAsync);
        using var cancellation = new CancellationTokenSource();
        var runTask = partitioner.RunAsync(cancellation.Token);
        var item = CreateItem("session-A", "same-id");
        try
        {
            await partitioner.EnqueueAsync(item, TestContext.Current.CancellationToken);
            await processStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
            await partitioner.EnqueueAsync(item, TestContext.Current.CancellationToken);
            releaseProcess.TrySetResult();
            await Task.Delay(TimeSpan.FromMilliseconds(100), TestContext.Current.CancellationToken);

            Assert.Equal(1, callCount);
        }
        finally
        {
            releaseProcess.TrySetResult();
            cancellation.Cancel();
            try
            {
                await runTask;
            }
            catch (OperationCanceledException)
            {
            }
            await partitioner.DisposeAsync();
        }
    }

    [Fact]
    public async Task Stop_cancels_workers_without_marking_unaccepted_work_complete()
    {
        var processStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var workerCancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        async ValueTask ProcessAsync(WakeOutboxItem item, CancellationToken cancellationToken)
        {
            processStarted.TrySetResult();
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                workerCancelled.TrySetResult();
                throw;
            }
        }

        var partitioner = new SessionPartitioner(4, 1, CreateRegistry(), ProcessAsync);
        var runTask = partitioner.RunAsync(CancellationToken.None);
        var item = CreateItem("session-A", "stopping") with { Status = WakeOutboxStatus.Pending };
        await partitioner.EnqueueAsync(item, TestContext.Current.CancellationToken);
        await processStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);

        await partitioner.StopAsync(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);

        Assert.True(workerCancelled.Task.IsCompleted);
        Assert.Equal(WakeOutboxStatus.Pending, item.Status);
        await partitioner.DisposeAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => runTask);
    }

    [Fact]
    public async Task Worker_failure_stops_sibling_workers_and_propagates()
    {
        var partitioner = new SessionPartitioner(
            4,
            2,
            CreateRegistry(),
            (_, _) => ValueTask.FromException(new InvalidOperationException("worker failed")));
        var runTask = partitioner.RunAsync(CancellationToken.None);

        try
        {
            await partitioner.EnqueueAsync(
                CreateItem("session-A", "failure"),
                TestContext.Current.CancellationToken);
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await runTask.WaitAsync(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken));
            Assert.Equal("worker failed", exception.Message);
        }
        finally
        {
            if (!runTask.IsCompleted)
                await partitioner.StopAsync(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
            await partitioner.DisposeAsync();
        }
    }

    private static ContractRegistryManager CreateRegistry(RateLimitPolicy? rateLimit = null)
    {
        var binding = new BridgeBinding(
            "binding-1", "drasi-server", new Uri("http://drasi.test"), null, "orders",
            "converge-latest", "singleton", null, null, "sample-gateway", "triage-order",
            new BridgeContract("1.0.0", "order.schema.json"), 32768, [],
            new RetryPolicy(3, TimeSpan.FromDays(1)),
            rateLimit ?? new RateLimitPolicy(10, TimeSpan.FromHours(1)));
        var registry = new ContractRegistryManager();
        registry.TryActivate(new ContractRegistryCandidate(new ContractRegistry("1.0.0", [binding]), []));
        return registry;
    }

    private static WakeOutboxItem CreateItem(string sessionId, string traceId) => new(
        Guid.NewGuid(), "binding-1", sessionId, $"fingerprint-{traceId}", "triage-order",
        new JsonObject(), "1.0.0", $"drasiwake:{traceId}", 0, DateTimeOffset.UtcNow,
        DateTimeOffset.UtcNow, WakeOutboxStatus.Dispatching, null, traceId, "sample-gateway");
}