using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using DrasiWake.Core.Domain;
using DrasiWake.Core.Pipeline;
using DrasiWake.Host;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DrasiWake.IntegrationTests;

public sealed class LeaderWorkerLifecycleTests
{
    [Theory]
    [InlineData(HttpRequestError.ResponseEnded, true)]
    [InlineData(HttpRequestError.ConnectionError, false)]
    [InlineData(HttpRequestError.HttpProtocolError, false)]
    [InlineData(HttpRequestError.InvalidResponse, false)]
    public void Only_premature_HTTP_body_EOF_is_transient_among_HttpIOExceptions(
        HttpRequestError error, bool retryable)
    {
        Assert.Equal(retryable,
            RetryableLeaderRecoveryException.IsTransient(new HttpIOException(error, "private upstream detail")));
    }

    [Fact]
    public void Ordinary_IO_failures_are_not_transient_recovery_failures()
    {
        Assert.False(RetryableLeaderRecoveryException.IsTransient(new IOException("private storage detail")));
        Assert.False(RetryableLeaderRecoveryException.IsTransient(new InvalidDataException("corrupt persistence")));
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task Premature_HTTP_body_EOF_is_marked_only_during_recovery(
        bool failPreparation, bool retryable)
    {
        var events = new ConcurrentQueue<string>();
        var failure = new HttpIOException(HttpRequestError.ResponseEnded, "private upstream detail");
        var preparation = new ObservableStartupPreparation(events) { Failure = failPreparation ? failure : null };
        var recovery = new ObservableRecovery(events) { Failure = failPreparation ? null : failure };
        var runtime = new LeaderWorkerRuntime(preparation, recovery, [new ObservableWorker(events)]);
        var exception = await Record.ExceptionAsync(() => runtime.StartAsync(TestContext.Current.CancellationToken));
        Assert.NotNull(exception);
        if (retryable)
            Assert.Same(failure, Assert.IsType<RetryableLeaderRecoveryException>(exception).InnerException);
        else
            Assert.Same(failure, exception);
        Assert.DoesNotContain("worker-start", events);
        await runtime.StopAsync(TestContext.Current.CancellationToken);
    }

    [Theory]
    [InlineData(true, 503, false)]
    [InlineData(false, 503, true)]
    [InlineData(false, 401, false)]
    [InlineData(false, 400, false)]
    public async Task Only_transient_recovery_HTTP_errors_are_marked_for_retry(
        bool failPreparation, int status, bool retryable)
    {
        var events = new ConcurrentQueue<string>();
        var failure = new HttpRequestException("private upstream detail", null, (System.Net.HttpStatusCode)status);
        var preparation = new ObservableStartupPreparation(events) { Failure = failPreparation ? failure : null };
        var recovery = new ObservableRecovery(events) { Failure = failPreparation ? null : failure };
        var runtime = new LeaderWorkerRuntime(preparation, recovery, [new ObservableWorker(events)]);
        var exception = await Record.ExceptionAsync(() => runtime.StartAsync(TestContext.Current.CancellationToken));
        Assert.NotNull(exception);
        Assert.Equal(retryable, exception != failure);
        Assert.DoesNotContain("worker-start", events);
        await runtime.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Host_shutdown_cancels_transient_recovery_backoff_without_another_scope()
    {
        var leadership = new FakeRaftLeadership(new RaftLeadershipChange(32, true, true));
        using var services = CreateServices(leadership, _ => new ObservableRuntime
        {
            StartupFailure = new RetryableLeaderRecoveryException(
                new HttpRequestException("unavailable", null, System.Net.HttpStatusCode.ServiceUnavailable))
        });
        var hostedService = CreateHostedService(leadership, services);
        await hostedService.StartAsync(TestContext.Current.CancellationToken);
        await services.WaitForRuntimeCountAsync(1);
        await services.GetRuntime(0).Stopped.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await hostedService.StopAsync(TestContext.Current.CancellationToken)
            .WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);
        Assert.Equal(1, services.RuntimeCount);
    }

    [Fact]
    public async Task Transient_startup_failure_retries_unchanged_leadership_after_prior_runtime_stops()
    {
        var leadership = new FakeRaftLeadership(new RaftLeadershipChange(30, true, true));
        var attempts = 0;
        using var services = CreateServices(leadership, _ =>
        {
            var firstAttempt = Interlocked.Increment(ref attempts) == 1;
            var runtime = new ObservableRuntime
            {
                StartupFailure = firstAttempt
                    ? new RetryableLeaderRecoveryException(
                        new HttpRequestException("private upstream detail", null, System.Net.HttpStatusCode.ServiceUnavailable))
                    : null
            };
            if (firstAttempt)
                runtime.HoldStop();
            return runtime;
        });
        var logger = new CapturingLogger<RaftLeaderHostedService>();
        var hostedService = new RaftLeaderHostedService(
            leadership, services.GetRequiredService<IServiceScopeFactory>(), logger);
        try
        {
            await hostedService.StartAsync(TestContext.Current.CancellationToken);
            await services.WaitForRuntimeCountAsync(1);
            var first = services.GetRuntime(0);
            await first.StopEntered.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            Assert.Equal(1, services.RuntimeCount);
            first.AllowStopToFinish();
            await first.Stopped.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            await services.WaitForRuntimeCountAsync(2);
            await services.GetRuntime(1).Started.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            Assert.True(first.EpochCancellationRequested);
            Assert.NotEqual(first.ComponentIdentity, services.GetRuntime(1).ComponentIdentity);
            Assert.True(first.ScopeDisposed);
            Assert.Equal(30, leadership.Term);
            Assert.Contains(logger.Messages, message => message.Contains("leader-recovery-transient", StringComparison.Ordinal));
            Assert.DoesNotContain(logger.Messages, message => message.Contains("private upstream detail", StringComparison.Ordinal));
        }
        finally
        {
            if (services.RuntimeCount > 0)
                services.GetRuntime(0).AllowStopToFinish();
            await hostedService.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Permanent_failure_or_quorum_loss_does_not_retry(bool loseQuorum)
    {
        var leadership = new FakeRaftLeadership(new RaftLeadershipChange(31, true, true));
        using var services = CreateServices(leadership, _ => new ObservableRuntime
        {
            StartupFailure = loseQuorum
                ? new RetryableLeaderRecoveryException(
                    new HttpRequestException("unavailable", null, System.Net.HttpStatusCode.ServiceUnavailable))
                : new InvalidDataException("corrupt persistence or invalid configuration")
        });
        var hostedService = CreateHostedService(leadership, services);
        try
        {
            await hostedService.StartAsync(TestContext.Current.CancellationToken);
            await services.WaitForRuntimeCountAsync(1);
            await services.GetRuntime(0).Stopped.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            if (loseQuorum)
                leadership.Publish(new RaftLeadershipChange(31, true, false));
            await Task.Delay(1500, TestContext.Current.CancellationToken);
            Assert.Equal(1, services.RuntimeCount);
        }
        finally
        {
            await hostedService.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task Follower_does_not_create_a_leader_runtime()
    {
        var leadership = new FakeRaftLeadership(new RaftLeadershipChange(1, false, true));
        using var services = CreateServices(leadership, _ => new ObservableRuntime());
        var hostedService = CreateHostedService(leadership, services);

        await hostedService.StartAsync(TestContext.Current.CancellationToken);
        await leadership.WatchStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal(0, services.RuntimeCount);

        await hostedService.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Eligible_term_creates_only_one_runtime()
    {
        var leadership = new FakeRaftLeadership(new RaftLeadershipChange(4, true, true));
        using var services = CreateServices(leadership, _ => new ObservableRuntime());
        var hostedService = CreateHostedService(leadership, services);

        await hostedService.StartAsync(TestContext.Current.CancellationToken);
        await services.WaitForRuntimeCountAsync(1);
        await services.GetRuntime(0).Started.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        leadership.Publish(new RaftLeadershipChange(4, true, true));
        leadership.Publish(new RaftLeadershipChange(4, true, true));
        await Task.Delay(100, TestContext.Current.CancellationToken);

        Assert.Equal(1, services.RuntimeCount);

        await hostedService.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Losing_quorum_stops_the_epoch_and_a_new_term_gets_fresh_components()
    {
        var leadership = new FakeRaftLeadership(new RaftLeadershipChange(8, true, true));
        using var services = CreateServices(leadership, _ => new ObservableRuntime());
        var hostedService = CreateHostedService(leadership, services);

        await hostedService.StartAsync(TestContext.Current.CancellationToken);
        await services.WaitForRuntimeCountAsync(1);
        var previousRuntime = services.GetRuntime(0);
        await previousRuntime.Started.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        leadership.Publish(new RaftLeadershipChange(8, true, false));
        await previousRuntime.Stopped.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.True(previousRuntime.EpochCancellationRequested);

        leadership.Publish(new RaftLeadershipChange(9, true, true));
        await services.WaitForRuntimeCountAsync(2);
        await services.GetRuntime(1).Started.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.NotEqual(previousRuntime.ComponentIdentity, services.GetRuntime(1).ComponentIdentity);
        await hostedService.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task New_term_does_not_start_until_previous_runtime_finishes_stopping()
    {
        var leadership = new FakeRaftLeadership(new RaftLeadershipChange(10, true, true));
        using var services = CreateServices(leadership, _ => new ObservableRuntime());
        var hostedService = CreateHostedService(leadership, services);

        await hostedService.StartAsync(TestContext.Current.CancellationToken);
        await services.WaitForRuntimeCountAsync(1);
        var previousRuntime = services.GetRuntime(0);
        await previousRuntime.Started.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        previousRuntime.HoldStop();
        leadership.Publish(new RaftLeadershipChange(11, true, true));
        await previousRuntime.StopEntered.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal(1, services.RuntimeCount);

        previousRuntime.AllowStopToFinish();
        await services.WaitForRuntimeCountAsync(2);
        await services.GetRuntime(1).Started.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.NotEqual(previousRuntime.ComponentIdentity, services.GetRuntime(1).ComponentIdentity);
        await hostedService.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Host_shutdown_waits_for_the_current_runtime_to_stop()
    {
        var leadership = new FakeRaftLeadership(new RaftLeadershipChange(11, true, true));
        using var services = CreateServices(leadership, _ => new ObservableRuntime());
        var hostedService = CreateHostedService(leadership, services);

        await hostedService.StartAsync(TestContext.Current.CancellationToken);
        await services.WaitForRuntimeCountAsync(1);
        var runtime = services.GetRuntime(0);
        await runtime.Started.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        runtime.HoldStop();

        var stopping = hostedService.StopAsync(TestContext.Current.CancellationToken);
        await runtime.StopEntered.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.False(stopping.IsCompleted);

        runtime.AllowStopToFinish();
        await stopping;
        Assert.True(runtime.Stopped.Task.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task Startup_preparation_and_recovery_complete_before_workers_start()
    {
        var events = new ConcurrentQueue<string>();
        var preparation = new ObservableStartupPreparation(events);
        var recovery = new ObservableRecovery(events);
        var worker = new ObservableWorker(events);
        var runtime = new LeaderWorkerRuntime(preparation, recovery, [worker]);

        await runtime.StartAsync(TestContext.Current.CancellationToken);

        Assert.Equal(["prepare", "recover", "worker-start"], events.ToArray());
        Assert.False(recovery.EnqueueDispatchableItems);

        await runtime.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Worker_failure_faults_runtime_completion()
    {
        var events = new ConcurrentQueue<string>();
        var worker = new ObservableWorker(events);
        var runtime = new LeaderWorkerRuntime(
            new ObservableStartupPreparation(events),
            new ObservableRecovery(events),
            [worker]);

        await runtime.StartAsync(TestContext.Current.CancellationToken);
        worker.Fail(new InvalidOperationException("worker failed"));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => runtime.Completion);
        Assert.Equal("worker failed", exception.Message);

        await runtime.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Worker_failure_is_logged_as_a_fixed_category_and_stops_the_epoch()
    {
        var leadership = new FakeRaftLeadership(new RaftLeadershipChange(12, true, true));
        var logger = new CapturingLogger<RaftLeaderHostedService>();
        using var services = CreateServices(leadership, _ => new ObservableRuntime());
        var hostedService = new RaftLeaderHostedService(leadership, services.GetRequiredService<IServiceScopeFactory>(), logger);

        await hostedService.StartAsync(TestContext.Current.CancellationToken);
        await services.WaitForRuntimeCountAsync(1);
        var runtime = services.GetRuntime(0);
        runtime.Fail(new InvalidOperationException("worker failed"));

        await runtime.Stopped.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Contains(logger.Messages, message => message.Contains("worker-failure", StringComparison.Ordinal));

        await hostedService.StopAsync(TestContext.Current.CancellationToken);
    }

    private static TestServiceProvider CreateServices(
        FakeRaftLeadership leadership,
        Func<IServiceProvider, ObservableRuntime> runtimeFactory)
    {
        var createdRuntimes = new ConcurrentQueue<ObservableRuntime>();
        var collection = new ServiceCollection();
        collection.AddSingleton<IRaftLeadership>(leadership);
        collection.AddScoped<ILeaderWorkerRuntime>(provider =>
        {
            var runtime = runtimeFactory(provider);
            createdRuntimes.Enqueue(runtime);
            return runtime;
        });
        return new TestServiceProvider(collection.BuildServiceProvider(), createdRuntimes);
    }

    private static RaftLeaderHostedService CreateHostedService(
        FakeRaftLeadership leadership,
        TestServiceProvider services)
        => new(leadership, services.GetRequiredService<IServiceScopeFactory>(),
            new CapturingLogger<RaftLeaderHostedService>());

    private sealed class TestServiceProvider(
        ServiceProvider provider,
        ConcurrentQueue<ObservableRuntime> runtimes) : IDisposable, IServiceProvider
    {
        public int RuntimeCount => runtimes.Count;

        public object? GetService(Type serviceType) => provider.GetService(serviceType);

        public T GetRequiredService<T>() where T : notnull => provider.GetRequiredService<T>();

        public ObservableRuntime GetRuntime(int index) => runtimes.ElementAt(index);

        public async Task WaitForRuntimeCountAsync(int count)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            while (RuntimeCount < count)
                await Task.Delay(10, timeout.Token);
        }

        public void Dispose() => provider.Dispose();
    }

    private sealed class FakeRaftLeadership(RaftLeadershipChange initial) : IRaftLeadership
    {
        private readonly Channel<RaftLeadershipChange> changes = Channel.CreateUnbounded<RaftLeadershipChange>();
        private RaftLeadershipChange current = initial;

        public bool IsLeader => current.IsLeader;
        public bool HasQuorum => current.HasQuorum;
        public long Term => current.Term;
        public TaskCompletionSource WatchStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Publish(RaftLeadershipChange change)
        {
            current = change;
            changes.Writer.TryWrite(change);
        }

        public async IAsyncEnumerable<RaftLeadershipChange> WatchLeadershipAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            WatchStarted.TrySetResult();
            yield return current;
            await foreach (var change in changes.Reader.ReadAllAsync(cancellationToken))
                yield return change;
        }
    }

    private sealed class ObservableRuntime : ILeaderWorkerRuntime, IDisposable
    {
        public Exception? StartupFailure { get; init; }
        public bool ScopeDisposed { get; private set; }
        public void Dispose() => ScopeDisposed = true;
        private readonly TaskCompletionSource completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Guid ComponentIdentity { get; } = Guid.NewGuid();
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource StopEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private Task stopGate = Task.CompletedTask;
        private TaskCompletionSource? allowStopToFinish;
        public TaskCompletionSource Stopped { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public bool EpochCancellationRequested { get; private set; }
        public Task Completion => completion.Task;

        public Task StartAsync(CancellationToken cancellationToken)
        {
            if (StartupFailure is { } failure)
                return Task.FromException(failure);
            Started.TrySetResult();
            return Task.CompletedTask;
        }

        public void HoldStop()
        {
            allowStopToFinish = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            stopGate = allowStopToFinish.Task;
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            EpochCancellationRequested = true;
            StopEntered.TrySetResult();
            await stopGate.WaitAsync(cancellationToken);
            completion.TrySetResult();
            Stopped.TrySetResult();
        }

        public void AllowStopToFinish() => allowStopToFinish?.TrySetResult();

        public void Fail(Exception exception) => completion.TrySetException(exception);
    }

    private sealed class ObservableStartupPreparation(ConcurrentQueue<string> events) : IRaftLeaderStartupPreparation
    {
        public Exception? Failure { get; init; }
        public Task PrepareBeforeRecoveryAsync(CancellationToken cancellationToken)
        {
            events.Enqueue("prepare");
            return Failure is { } failure ? Task.FromException(failure) : Task.CompletedTask;
        }
    }

    private sealed class ObservableRecovery(ConcurrentQueue<string> events) : IRecoveryCoordinator
    {
        public Exception? Failure { get; init; }
        public bool? EnqueueDispatchableItems { get; private set; }

        public Task RecoverAsync(CancellationToken cancellationToken)
        {
            events.Enqueue("recover");
            return Task.CompletedTask;
        }

        public Task RecoverAsync(CancellationToken cancellationToken, bool enqueueDispatchableItems)
        {
            EnqueueDispatchableItems = enqueueDispatchableItems;
            events.Enqueue("recover");
            return Failure is { } failure ? Task.FromException(failure) : Task.CompletedTask;
        }

        public Task ReconcileAfterReconnectAsync(QueryIdentity query, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    private sealed class ObservableWorker(ConcurrentQueue<string> events) : ILeaderEpochWorker
    {
        private readonly TaskCompletionSource completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Completion => completion.Task;

        public Task StartAsync(CancellationToken cancellationToken)
        {
            events.Enqueue("worker-start");
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            completion.TrySetResult();
            return Task.CompletedTask;
        }

        public void Fail(Exception exception) => completion.TrySetException(exception);
    }

    private sealed class CapturingLogger<T> : ILogger<T>
    {
        public ConcurrentQueue<string> Messages { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
            => Messages.Enqueue(formatter(state, exception));
    }
}
