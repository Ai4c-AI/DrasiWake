using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using DrasiWake.Core.Domain;
using DrasiWake.Core.Pipeline;
using DrasiWake.Host;
using DrasiWake.IntegrationTests.Fixtures;
using Grpc.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ProtoBuf.Grpc;

namespace DrasiWake.IntegrationTests;

[Collection(RaftFailoverCollection.Name)]
public sealed class TelemetryRedactionTests
{
    private const string MemberSecret = "private-member.test";
    private const string TokenSecret = "private-management-token";
    private const string PasswordSecret = "private-certificate-password";
    private const string FactSecret = "private-fact-payload";
    private const string ExceptionSecret = "private-exception-text";

    [Theory]
    [InlineData(StatusCode.Unauthenticated)]
    [InlineData(StatusCode.Unimplemented)]
    public async Task Membership_rpc_failures_do_not_return_arbitrary_authentication_error_details(StatusCode status)
    {
        using var capture = new TelemetryCapture();
        var logger = new CaptureLogger<ClusterMembershipGrpcService>();
        var service = new ClusterMembershipGrpcService(ClusterSettings(), new Compatibility(),
            new FailingMembership(new RpcException(new Status(status, ExceptionSecret))), logger, TimeProvider.System);
        var exception = await Assert.ThrowsAsync<RpcException>(() =>
            service.Add(new ClusterMemberRequest { Endpoint = $"https://{MemberSecret}:5101/" },
                new CallContext(service, new ManagementContext(TokenSecret))));
        Assert.Equal(status, exception.StatusCode);
        AssertRedacted(capture.Text.Concat(logger.Messages).Append(exception.ToString()));
    }

    [Fact]
    public async Task Epoch_stop_failure_remains_fatal_but_does_not_expose_exception_text_to_host()
    {
        using var capture = new TelemetryCapture();
        var leadership = new Leadership();
        var runtime = new Runtime { StopFailure = true };
        var logger = new CaptureLogger<RaftLeaderHostedService>();
        using var services = new ServiceCollection().AddScoped<ILeaderWorkerRuntime>(_ => runtime).BuildServiceProvider();
        using var service = new RaftLeaderHostedService(leadership,
            services.GetRequiredService<IServiceScopeFactory>(), logger);
        await service.StartAsync(TestContext.Current.CancellationToken);
        leadership.Publish(new(1, true, true));
        await WaitUntilAsync(() => capture.Activities.Any(a => a.OperationName == "raft.leader.recovery"));
        leadership.Publish(new(1, true, false));
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            service.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        await service.StopAsync(TestContext.Current.CancellationToken);
        AssertRedacted(capture.Text.Concat(logger.Messages).Append(exception.ToString()));
    }

    [Fact]
    public void Certificate_load_failure_can_be_logged_without_exposing_secret_configuration_or_inner_exception()
    {
        var settings = ClusterSettings($"artifacts\\{PasswordSecret}\\{MemberSecret}.pfx");
        var logger = new CaptureLogger<RaftClusterSettings>();
        var exception = Assert.Throws<InvalidOperationException>(() => settings.LoadServerCertificate());
        logger.LogError(exception, "Certificate startup failed.");

        AssertRedacted(logger.Messages);
        Assert.Null(exception.InnerException);
    }

    [Fact]
    public async Task Arbitrary_membership_exception_category_is_reduced_to_a_fixed_result()
    {
        using var capture = new TelemetryCapture();
        var logger = new CaptureLogger<ClusterMembershipGrpcService>();
        var service = new ClusterMembershipGrpcService(ClusterSettings(), new Compatibility(),
            new CategoryMembership(), logger, TimeProvider.System);
        await Assert.ThrowsAsync<RpcException>(() =>
            service.Add(new ClusterMemberRequest { Endpoint = $"https://{MemberSecret}:5101/" },
                new CallContext(service, new ManagementContext(TokenSecret))));
        AssertRedacted(capture.Text.Concat(logger.Messages));
        Assert.Contains(capture.Measurements, measurement =>
            measurement.Name == "drasiwake.raft.membership" &&
            measurement.Tags.Any(tag => tag.Key == "result" && tag.Value?.ToString() == "failed"));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Leader_startup_failures_emit_one_fixed_recovery_result_without_escaping_to_host_logs(bool synchronous)
    {
        using var capture = new TelemetryCapture();
        var leadership = new Leadership();
        var runtime = new Runtime { StartupFailure = true, SynchronousFailure = synchronous };
        var logger = new CaptureLogger<RaftLeaderHostedService>();
        using var services = new ServiceCollection().AddScoped<ILeaderWorkerRuntime>(_ => runtime).BuildServiceProvider();
        using var service = new RaftLeaderHostedService(leadership,
            services.GetRequiredService<IServiceScopeFactory>(), logger);
        await service.StartAsync(TestContext.Current.CancellationToken);
        leadership.Publish(new(1, true, true));
        await runtime.Stopped.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await service.StopAsync(TestContext.Current.CancellationToken);

        var recovery = Assert.Single(capture.Activities, a => a.OperationName == "raft.leader.recovery");
        Assert.Equal("failed", recovery.GetTagItem("result"));
        Assert.Single(capture.Measurements, m => m.Name == "drasiwake.raft.leader.recovery.duration");
        AssertRedacted(capture.Text.Concat(logger.Messages));
    }

    [Fact]
    public async Task Real_nodes_emit_role_quorum_lag_catchup_and_snapshot_telemetry()
    {
        using var capture = new TelemetryCapture();
        await using var cluster = await RaftClusterFixture.CreateAsync(
            3, TestContext.Current.CancellationToken, snapshotFrequency: 8);
        var leader = await cluster.WaitForLeaderAsync(TestContext.Current.CancellationToken);
        for (var index = 0; index < 10; index++)
            await cluster.CreatePendingWakeAsync(leader, TestContext.Current.CancellationToken);
        var committedIndex = cluster.LastCommittedIndex(leader);
        await cluster.WaitForAllAvailableProjectionsAsync(committedIndex, TestContext.Current.CancellationToken);
        var follower = cluster.Nodes.First(node => node != leader);
        await cluster.WaitForPersistedSnapshotAsync(follower, 1, TestContext.Current.CancellationToken);
        await cluster.StopNodeWithoutMembershipChangeAsync(follower, TestContext.Current.CancellationToken);
        cluster.SetAsideStoppedProjection(follower);
        await cluster.RestartNodeAsync(follower, TestContext.Current.CancellationToken);
        await cluster.WaitForProjectionAsync(follower, committedIndex, TestContext.Current.CancellationToken);
        capture.Collect();

        foreach (var name in new[]
        {
            "drasiwake.raft.node.role", "drasiwake.raft.quorum", "drasiwake.raft.writable",
            "drasiwake.raft.commit.lag", "drasiwake.raft.apply.lag", "drasiwake.raft.follower.catchup.lag",
            "drasiwake.raft.snapshots", "drasiwake.raft.snapshot.duration", "drasiwake.raft.apply.duration"
        })
            Assert.Contains(capture.Measurements, measurement => measurement.Name == name);
        Assert.Contains(capture.Measurements, m => m.Name == "drasiwake.raft.snapshots" &&
            m.Tags.Any(t => t.Key == "operation" && t.Value?.ToString() == "restore"));
        Assert.Contains(capture.Measurements, m => m.Name == "drasiwake.raft.node.role" &&
            m.Tags.Any(t => t.Key == "role" && t.Value?.ToString() == "leader"));
        Assert.Contains(capture.Measurements, m => m.Name == "drasiwake.raft.node.role" &&
            m.Tags.Any(t => t.Key == "role" && t.Value?.ToString() == "follower"));
        Assert.Contains(capture.Measurements, m => m.Name == "drasiwake.raft.writable" && m.Value == 1);
        Assert.Contains(capture.Measurements, m => m.Name == "drasiwake.raft.writable" && m.Value == 0);
        Assert.All(capture.Measurements.Where(m => m.Name.EndsWith(".lag", StringComparison.Ordinal)),
            m => Assert.True(m.Value >= 0));
        foreach (var node in cluster.Nodes)
            foreach (var value in capture.Text)
            {
                Assert.DoesNotContain(node.NodeId, value, StringComparison.Ordinal);
                Assert.DoesNotContain(node.ListenAddress.AbsoluteUri, value, StringComparison.Ordinal);
                Assert.DoesNotContain(node.ManagementAddress.AbsoluteUri, value, StringComparison.Ordinal);
            }
        AssertRedacted(capture.Text);
        Assert.All(capture.Measurements.Where(m => m.Name.StartsWith("drasiwake.raft.", StringComparison.Ordinal)),
            m => Assert.DoesNotContain(m.Tags, tag => tag.Key is "node.id" or "member.id" or "endpoint" or "term"));
        foreach (var node in cluster.Nodes.Where(node => node != leader))
            await cluster.StopNodeWithoutMembershipChangeAsync(node, TestContext.Current.CancellationToken);
        await cluster.WaitForQuorumLossAsync(leader, TestContext.Current.CancellationToken);
        var collectedBeforeLoss = capture.Measurements.Count;
        capture.Collect();
        var lossMeasurements = capture.Measurements.Skip(collectedBeforeLoss).ToArray();
        Assert.Single(lossMeasurements, m => m.Name == "drasiwake.raft.node.role");
        Assert.Contains(lossMeasurements, m => m.Name == "drasiwake.raft.quorum" && m.Value == 0);
        Assert.Contains(lossMeasurements, m => m.Name == "drasiwake.raft.writable" && m.Value == 0);
    }

    [Fact]
    public async Task Membership_application_path_emits_hashed_activity_and_fixed_metric_results()
    {
        using var capture = new TelemetryCapture();
        var logger = new CaptureLogger<ClusterMembershipGrpcService>();
        var settings = ClusterSettings();
        var service = new ClusterMembershipGrpcService(
            settings, new Compatibility(), new FailingMembership(), logger, TimeProvider.System);
        var context = new CallContext(service, new ManagementContext(TokenSecret));
        var endpoint = $"https://{MemberSecret}:5101/";

        var exception = await Assert.ThrowsAsync<RpcException>(() =>
            service.Add(new ClusterMemberRequest { Endpoint = endpoint }, context));
        Assert.Equal(StatusCode.Internal, exception.StatusCode);
        await service.Remove(new ClusterMemberRequest { Endpoint = endpoint }, context);
        await Assert.ThrowsAsync<RpcException>(() =>
            service.Add(new ClusterMemberRequest { Endpoint = $"https://user:{FactSecret}@{MemberSecret}/" }, context));
        await Assert.ThrowsAsync<RpcException>(() =>
            service.Add(new ClusterMemberRequest { Endpoint = endpoint },
                new CallContext(service, new ManagementContext(PasswordSecret))));

        Assert.Contains(capture.Activities, activity => activity.OperationName == "raft.membership" &&
            activity.GetTagItem("member.id")?.ToString() == BridgeTelemetry.StableId(endpoint) &&
            activity.GetTagItem("result")?.ToString() == "failed");
        Assert.Contains(capture.Measurements, measurement =>
            measurement.Name == "drasiwake.raft.membership" &&
            measurement.Tags.Any(tag => tag.Key == "result" && tag.Value?.ToString() == "succeeded"));
        Assert.Contains(capture.Measurements, measurement =>
            measurement.Name == "drasiwake.raft.membership" &&
            measurement.Tags.Any(tag => tag.Key == "result" && tag.Value?.ToString() == "unauthenticated"));
        AssertRedacted(capture.Text.Concat(logger.Messages).Append(exception.ToString()));
    }

    [Fact]
    public async Task Leader_lifecycle_reports_recovery_failover_and_redacts_runtime_failures()
    {
        using var capture = new TelemetryCapture();
        var leadership = new Leadership();
        var runtimes = new ConcurrentQueue<Runtime>();
        var logger = new CaptureLogger<RaftLeaderHostedService>();
        using var services = new ServiceCollection()
            .AddScoped<ILeaderWorkerRuntime>(_ =>
            {
                var runtime = new Runtime();
                runtimes.Enqueue(runtime);
                return runtime;
            }).BuildServiceProvider();
        using var service = new RaftLeaderHostedService(leadership,
            services.GetRequiredService<IServiceScopeFactory>(), logger);
        await service.StartAsync(TestContext.Current.CancellationToken);
        leadership.Publish(new(1, true, true));
        await WaitUntilAsync(() => runtimes.Count == 1 && capture.Activities.Any(a => a.OperationName == "raft.leader.recovery"));
        var first = runtimes.First();
        leadership.Publish(new(1, true, false));
        await first.Stopped.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        leadership.Publish(new(2, true, true));
        await WaitUntilAsync(() => runtimes.Count == 2 &&
            capture.Measurements.Any(m => m.Name == "drasiwake.raft.failover.duration"));
        runtimes.Last().Fail();
        await runtimes.Last().Stopped.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await service.StopAsync(TestContext.Current.CancellationToken);

        Assert.Contains(capture.Measurements, m => m.Name == "drasiwake.raft.leader.changes");
        Assert.Contains(capture.Measurements, m => m.Name == "drasiwake.raft.leader.recovery.duration");
        AssertRedacted(capture.Text.Concat(logger.Messages));
    }

    private static RaftClusterSettings ClusterSettings(string? certificatePath = null) => RaftClusterSettings.FromConfiguration(
        new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["DrasiWake:Cluster:Mode"] = "Cluster",
            ["DrasiWake:Cluster:NodeId"] = MemberSecret,
            ["DrasiWake:Cluster:ListenAddress"] = "https://127.0.0.1:5101/",
            ["DrasiWake:Cluster:InitialMembers:0"] = "https://127.0.0.1:5101/",
            ["DrasiWake:Cluster:RaftDataPath"] = "artifacts\\telemetry-raft",
            ["DrasiWake:Cluster:Certificate:Path"] = certificatePath ?? "private.pfx",
            ["DrasiWake:Cluster:Certificate:Password"] = PasswordSecret,
            ["DrasiWake:Cluster:Management:Address"] = "https://127.0.0.1:5102/",
            ["DrasiWake:Cluster:Management:BearerToken"] = TokenSecret,
            ["DrasiWake:Cluster:SnapshotFrequency"] = "1000"
        }).Build(), "artifacts\\telemetry-db");

    private static void AssertRedacted(IEnumerable<string> values)
    {
        foreach (var value in values)
            foreach (var secret in new[] { MemberSecret, TokenSecret, PasswordSecret, FactSecret, ExceptionSecret })
                Assert.DoesNotContain(secret, value, StringComparison.Ordinal);
    }

    private static async Task WaitUntilAsync(Func<bool> predicate)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!predicate())
            await Task.Delay(10, timeout.Token);
    }

    private sealed class TelemetryCapture : IDisposable
    {
        private readonly ActivityListener activityListener;
        private readonly MeterListener meterListener;
        public ConcurrentQueue<Activity> Activities { get; } = new();
        public ConcurrentQueue<(string Name, double Value, KeyValuePair<string, object?>[] Tags)> Measurements { get; } = new();
        public IEnumerable<string> Text => Activities.SelectMany(a => a.TagObjects.Select(t => $"{t.Key}={t.Value}"))
            .Concat(Measurements.SelectMany(m => m.Tags.Select(t => $"{t.Key}={t.Value}")));

        public TelemetryCapture()
        {
            activityListener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == BridgeTelemetry.ActivitySourceName,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
                ActivityStopped = Activities.Enqueue
            };
            ActivitySource.AddActivityListener(activityListener);
            meterListener = new MeterListener
            {
                InstrumentPublished = (instrument, listener) =>
                {
                    if (instrument.Meter.Name == BridgeTelemetry.MeterName)
                        listener.EnableMeasurementEvents(instrument);
                }
            };
            meterListener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
                Measurements.Enqueue((instrument.Name, value, tags.ToArray())));
            meterListener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
                Measurements.Enqueue((instrument.Name, value, tags.ToArray())));
            meterListener.Start();
        }

        public void Dispose()
        {
            activityListener.Dispose();
            meterListener.Dispose();
        }

        public void Collect() => meterListener.RecordObservableInstruments();
    }

    private sealed class CaptureLogger<T> : ILogger<T>
    {
        public ConcurrentQueue<string> Messages { get; } = new();
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
            Exception? exception, Func<TState, Exception?, string> formatter) =>
            Messages.Enqueue(formatter(state, exception) + exception?.ToString());
    }

    private sealed class Compatibility : IClusterCompatibilityProvider
    {
        public Task<ClusterCompatibilityResponse> GetLocalCompatibilityAsync(CancellationToken cancellationToken)
            => Task.FromResult(new ClusterCompatibilityResponse());
    }

    private sealed class FailingMembership(Exception? failure = null) : IRaftMembershipManager
    {
        public Task AddMemberAsync(Uri endpoint, CancellationToken cancellationToken)
            => Task.FromException(failure ?? new InvalidOperationException(
                $"{MemberSecret} {TokenSecret} {PasswordSecret} {FactSecret} {ExceptionSecret}"));
        public Task RemoveMemberAsync(Uri endpoint, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class CategoryMembership : IRaftMembershipManager
    {
        public Task AddMemberAsync(Uri endpoint, CancellationToken cancellationToken)
            => Task.FromException(new ClusterMembershipException(StatusCode.Unavailable, ExceptionSecret));
        public Task RemoveMemberAsync(Uri endpoint, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class Leadership : IRaftLeadership
    {
        private readonly Channel<RaftLeadershipChange> changes = Channel.CreateUnbounded<RaftLeadershipChange>();
        public bool IsLeader => true;
        public bool HasQuorum => true;
        public long Term => 1;
        public void Publish(RaftLeadershipChange change) => changes.Writer.TryWrite(change);
        public async IAsyncEnumerable<RaftLeadershipChange> WatchLeadershipAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (var change in changes.Reader.ReadAllAsync(cancellationToken))
                yield return change;
        }
    }

    private sealed class Runtime : ILeaderWorkerRuntime
    {
        private readonly TaskCompletionSource completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Stopped { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public bool StartupFailure { get; init; }
        public bool SynchronousFailure { get; init; }
        public bool StopFailure { get; init; }
        public Task Completion => completion.Task;
        public Task StartAsync(CancellationToken cancellationToken)
        {
            if (!StartupFailure)
                return Task.CompletedTask;
            var exception = new InvalidOperationException(
                $"{MemberSecret} {TokenSecret} {PasswordSecret} {FactSecret} {ExceptionSecret}");
            if (SynchronousFailure)
                throw exception;
            return Task.FromException(exception);
        }
        public Task StopAsync(CancellationToken cancellationToken)
        {
            Stopped.TrySetResult();
            if (StopFailure)
                throw new InvalidOperationException(
                    $"{MemberSecret} {TokenSecret} {PasswordSecret} {FactSecret} {ExceptionSecret}");
            return Task.CompletedTask;
        }
        public void Fail() => completion.TrySetException(new InvalidOperationException(
            $"{MemberSecret} {TokenSecret} {PasswordSecret} {FactSecret} {ExceptionSecret}"));
    }

    private sealed class ManagementContext(string token) : ServerCallContext
    {
        protected override string MethodCore => "membership";
        protected override string HostCore => "localhost";
        protected override string PeerCore => "localhost";
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => new() { { "authorization", $"Bearer {token}" } };
        protected override CancellationToken CancellationTokenCore => TestContext.Current.CancellationToken;
        protected override Metadata ResponseTrailersCore { get; } = new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => new("test", new Dictionary<string, List<AuthProperty>>());
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options)
            => throw new NotSupportedException();
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
    }

    [Fact]
    public void Activities_and_metrics_do_not_expose_query_or_wake_secrets()
    {
        const string secret = "customer-identity-and-private-payload";
        var query = new QueryIdentity(new Uri($"https://user:{secret}@drasi.test/{secret}?token={secret}"), secret, secret);
        var item = new WakeOutboxItem(
            Guid.NewGuid(), secret, secret, secret, secret, new System.Text.Json.Nodes.JsonObject { ["secret"] = secret },
            "1.0.0", secret, 0, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, WakeOutboxStatus.Pending, null, secret, "sample-gateway");
        using var activityListener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == BridgeTelemetry.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
        };
        ActivitySource.AddActivityListener(activityListener);
        var metricTags = new List<KeyValuePair<string, object?>>();
        using var meterListener = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == BridgeTelemetry.MeterName)
                    listener.EnableMeasurementEvents(instrument);
            }
        };
        meterListener.SetMeasurementEventCallback<long>((_, _, tags, _) => metricTags.AddRange(tags.ToArray()));
        meterListener.Start();

        using var activity = BridgeTelemetry.StartReconciliation(query);
        BridgeTelemetry.RecordRetry(item, "dispatch.transient_failure");

        Assert.NotNull(activity);
        Assert.All(activity!.Tags, tag => Assert.DoesNotContain(secret, tag.Value, StringComparison.Ordinal));
        Assert.NotEmpty(metricTags);
        Assert.All(metricTags, tag => Assert.DoesNotContain(secret, tag.Value?.ToString(), StringComparison.Ordinal));
        Assert.Equal(BridgeTelemetry.StableId(secret), metricTags.Single(tag => tag.Key == "binding.id").Value);
    }
}