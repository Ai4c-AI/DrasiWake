using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using DotNext.IO;
using DotNext.Net.Cluster.Consensus.Raft;
using DotNext.Net.Cluster.Consensus.Raft.Http;
using DotNext.Net.Cluster.Consensus.Raft.StateMachine;
using DotNext.Net.Http;
using DrasiWake.Core.Domain;
using DrasiWake.Persistence.Raft;
using DrasiWake.Persistence.SonnetDB;
using DrasiWake.Persistence.SonnetDB.Replication;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging;
using System.Diagnostics.Metrics;
using DrasiWake.Core.Pipeline;
using Microsoft.Extensions.Hosting;

namespace DrasiWake.Persistence.Raft.Tests;

public sealed class RaftBridgeStateMachineTests
{
    [Fact]
    public async Task TelemetryRedaction_application_failure_does_not_log_fact_or_exception_text()
    {
        const string secret = "private-fact-payload-and-exception-text";
        await using var database = await TestDatabase.CreateAsync();
        var logger = new RedactionLogger();
        await using var stateMachine = new RaftBridgeStateMachine(
            database.Store, new DirectoryInfo(Path.Combine(database.DirectoryPath, "snapshot")), 2, logger);
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Meter.Name == BridgeTelemetry.MeterName)
                    meterListener.EnableMeasurementEvents(instrument);
            }
        };
        var measurements = new List<string>();
        listener.SetMeasurementEventCallback<long>((instrument, _, tags, _) =>
            measurements.Add(instrument.Name + string.Join(",", tags.ToArray().Select(t => $"{t.Key}={t.Value}"))));
        listener.Start();
        var wake = CreateWake(DateTimeOffset.UtcNow) with { Input = new JsonObject { ["privateFact"] = secret } };
        await stateMachine.ApplyPayloadAsync(BridgeReplicationSerializer.SerializeCommand(
            ReplicatedBridgeCommand.Create(BridgeCommandKind.CreateOrUpdatePendingWake,
                new CreateOrUpdatePendingWakePayload(wake))), 1, TestContext.Current.CancellationToken);
        using var stream = new MemoryStream();
        await stateMachine.PersistSnapshotAsync(IAsyncBinaryWriter.Create(stream, new byte[4096]),
            TestContext.Current.CancellationToken);
        var snapshotPath = Path.Combine(database.DirectoryPath, "redaction-snapshot.json");
        await File.WriteAllBytesAsync(snapshotPath, stream.ToArray(), TestContext.Current.CancellationToken);
        await stateMachine.RestoreSnapshotAsync(new FileInfo(snapshotPath), TestContext.Current.CancellationToken);
        var command = ReplicatedBridgeCommand.Create(BridgeCommandKind.MarkAcceptedWithCheckpoint,
            new MarkAcceptedWithCheckpointPayload(Guid.NewGuid(),
                new WakeAcceptance(secret, DateTimeOffset.UtcNow),
                new SnapshotCheckpoint(secret, secret, secret, DateTimeOffset.UtcNow)));
        await Assert.ThrowsAnyAsync<Exception>(async () => await stateMachine.ApplyPayloadAsync(
            BridgeReplicationSerializer.SerializeCommand(command), 2, TestContext.Current.CancellationToken));

        Assert.NotEmpty(logger.Messages);
        Assert.All(logger.Messages.Concat(measurements), message =>
            Assert.DoesNotContain(secret, message, StringComparison.Ordinal));
        Assert.All(logger.Exceptions, exception => Assert.Null(exception));
        Assert.Contains(measurements, m => m.StartsWith("drasiwake.raft.apply", StringComparison.Ordinal));
        Assert.Contains(measurements, m => m.Contains("operation=export", StringComparison.Ordinal));
        Assert.Contains(measurements, m => m.Contains("operation=restore", StringComparison.Ordinal));
    }

    private sealed class RedactionLogger : ILogger<RaftBridgeStateMachine>
    {
        public List<string> Messages { get; } = [];
        public List<Exception?> Exceptions { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            Messages.Add(formatter(state, exception) + exception?.ToString());
            Exceptions.Add(exception);
        }
    }

    [Fact]
    public void Command_payload_bytes_are_deterministic_versioned_and_reject_invalid_envelopes()
    {
        var now = DateTimeOffset.Parse("2026-10-07T01:02:03Z");
        var wake = CreateWake(now);
        var subscription = new SubscriptionSnapshot(
            "query-key", "https://drasi", "instance", "query", true, now, null, null);
        var mapping = new KeyMappingSnapshot("scope", "identity", "session", "active", now);
        var commands = new[]
        {
            ReplicatedBridgeCommand.Create(BridgeCommandKind.CreateOrUpdatePendingWake,
                new CreateOrUpdatePendingWakePayload(wake)),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.RecordRejectedWake,
                new RecordRejectedWakePayload(wake with { Status = WakeOutboxStatus.DeadLetter }, "rejected")),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.SupersedePendingWakes,
                new SupersedePendingWakesPayload("binding", "session", "fingerprint")),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.ClaimDispatchable,
                new ClaimDispatchablePayload([wake.Id], now, Guid.NewGuid())),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.MarkAcceptedWithCheckpoint,
                new MarkAcceptedWithCheckpointPayload(
                    wake.Id,
                    new WakeAcceptance("invocation", now),
                    new SnapshotCheckpoint("binding", "session", "fingerprint", now))),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.UpdateExecutionStatus,
                new UpdateExecutionStatusPayload(new WakeExecutionStatus("invocation", "Completed", now))),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.MarkRetryScheduled,
                new MarkRetryScheduledPayload(wake.Id, 2, now, "retry")),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.MarkDeadLetter,
                new MarkDeadLetterPayload(wake.Id, "dead")),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.RecoverInterruptedDispatches,
                new RecoverInterruptedDispatchesPayload(now)),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.EnsureOpenClawTargets,
                new EnsureOpenClawTargetsPayload(
                    new Dictionary<string, string> { ["binding"] = "target" },
                    new HashSet<string> { "target" },
                    new Dictionary<string, TimeSpan> { ["binding"] = TimeSpan.FromHours(1) },
                    new Dictionary<string, TimeSpan> { ["target"] = TimeSpan.FromHours(2) })),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.SetConfigurationFingerprint,
                new SetConfigurationFingerprintPayload("configuration")),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.UpsertSubscription,
                new UpsertSubscriptionPayload(subscription)),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.RemoveSubscription,
                new RemoveSubscriptionPayload("query-key")),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.UpsertKeyMapping,
                new UpsertKeyMappingPayload(mapping)),
            ReplicatedBridgeCommand.Create(BridgeCommandKind.RemoveKeyMapping,
                new RemoveKeyMappingPayload("scope", "identity"))
        };
        Assert.Equal(15, commands.Length);

        foreach (var supportedCommand in commands)
        {
            var serialized = BridgeReplicationSerializer.SerializeCommand(supportedCommand);
            var restoredCommand = BridgeReplicationSerializer.DeserializeCommand(serialized);

            Assert.Equal(supportedCommand.SchemaVersion, restoredCommand.SchemaVersion);
            Assert.Equal(supportedCommand.Kind, restoredCommand.Kind);
            Assert.Equal(supportedCommand.CommandId, restoredCommand.CommandId);
            Assert.Equal(supportedCommand.Payload.GetRawText(), restoredCommand.Payload.GetRawText());
        }

        var command = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.SetConfigurationFingerprint,
            new SetConfigurationFingerprintPayload("configuration"));
        var firstBytes = BridgeReplicationSerializer.SerializeCommand(command);
        var secondBytes = BridgeReplicationSerializer.SerializeCommand(command);
        var restored = BridgeReplicationSerializer.DeserializeCommand(firstBytes);
        var unsupported = JsonNode.Parse(command.Serialize())!.AsObject();
        unsupported[nameof(command.SchemaVersion)] = ReplicatedBridgeCommand.CurrentSchemaVersion + 1;

        Assert.Equal(firstBytes, secondBytes);
        Assert.Equal(ReplicatedBridgeCommand.CurrentSchemaVersion, restored.SchemaVersion);
        Assert.Equal(command.CommandId, restored.CommandId);
        Assert.Equal(
            "configuration",
            restored.DeserializePayload<SetConfigurationFingerprintPayload>().ConfigurationFingerprint);
        using var serializedDocument = JsonDocument.Parse(firstBytes);
        Assert.Equal(
            new[] { "CommandId", "Kind", "Payload", "SchemaVersion" },
            serializedDocument.RootElement.EnumerateObject().Select(property => property.Name).OrderBy(name => name));
        Assert.Equal(command.Serialize(), Encoding.UTF8.GetString(firstBytes));
        Assert.DoesNotContain("machine-secret", Encoding.UTF8.GetString(firstBytes), StringComparison.Ordinal);
        Assert.DoesNotContain("C:\\node-data", Encoding.UTF8.GetString(firstBytes), StringComparison.Ordinal);
        Assert.Throws<InvalidOperationException>(() =>
            BridgeReplicationSerializer.DeserializeCommand(Encoding.UTF8.GetBytes(unsupported.ToJsonString())));
        Assert.Throws<InvalidOperationException>(() =>
            BridgeReplicationSerializer.DeserializeCommand(
                Encoding.UTF8.GetBytes("""{"SchemaVersion":1,"CommandId":"bad","Kind":"SetConfigurationFingerprint","Payload":null}""")));
        Assert.Throws<JsonException>(() =>
            BridgeReplicationSerializer.DeserializeCommand(Encoding.UTF8.GetBytes("not-json")));
    }

    [Fact]
    public async Task State_machine_applies_commands_at_the_provided_log_index()
    {
        await using var database = await TestDatabase.CreateAsync();
        await using var stateMachine = CreateStateMachine(database.Store, database.DirectoryPath);
        var cancellationToken = TestContext.Current.CancellationToken;

        var first = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.SetConfigurationFingerprint,
            new SetConfigurationFingerprintPayload("configuration-1"));
        var second = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.SetConfigurationFingerprint,
            new SetConfigurationFingerprintPayload("configuration-2"));

        Assert.False(await stateMachine.ApplyCommandAsync(first, 1, cancellationToken));
        Assert.True(await stateMachine.ApplyCommandAsync(second, 2, cancellationToken));

        var snapshot = await database.Store.ExportSnapshotAsync(cancellationToken);
        Assert.Equal(2, snapshot.LastAppliedIndex);
        Assert.Equal(second.CommandId, snapshot.LastAppliedCommandId);
        Assert.Equal("configuration-2", snapshot.ConfigurationFingerprint);
    }

    [Fact]
    public async Task Payloadless_dotnext_log_entries_advance_the_projection_before_the_next_command()
    {
        await using var database = await TestDatabase.CreateAsync();
        await using var stateMachine = CreateStateMachine(database.Store, database.DirectoryPath);
        var cancellationToken = TestContext.Current.CancellationToken;
        var entry = CreatePayloadlessLogEntry(term: 2, index: 1);
        var command = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.SetConfigurationFingerprint,
            new SetConfigurationFingerprintPayload("after-noop"));

        Assert.Equal(1, entry.Index);
        Assert.Equal(2, entry.Term);
        Assert.False(await ApplyLogEntryAsync(stateMachine, entry, cancellationToken));
        Assert.False(await stateMachine.ApplyPayloadAsync(
            BridgeReplicationSerializer.SerializeCommand(command),
            2,
            cancellationToken));

        var snapshot = await database.Store.ExportSnapshotAsync(cancellationToken);
        Assert.Equal(2, snapshot.LastAppliedIndex);
        Assert.Equal(command.CommandId, snapshot.LastAppliedCommandId);
        Assert.Equal("after-noop", snapshot.ConfigurationFingerprint);
    }

    [Fact]
    public async Task Invalid_log_payload_and_projection_failure_leave_the_projection_index_unchanged()
    {
        await using var database = await TestDatabase.CreateAsync();
        await using var stateMachine = CreateStateMachine(database.Store, database.DirectoryPath);
        var cancellationToken = TestContext.Current.CancellationToken;

        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await stateMachine.ApplyPayloadAsync(
                Encoding.UTF8.GetBytes("""{"SchemaVersion":99}"""),
                1,
                cancellationToken));

        var now = DateTimeOffset.Parse("2026-10-07T01:02:03Z");
        var wake = CreateWake(now);
        var command = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.MarkAcceptedWithCheckpoint,
            new MarkAcceptedWithCheckpointPayload(
                wake.Id,
                new WakeAcceptance("missing-outbox-invocation", now),
                new SnapshotCheckpoint(
                    wake.BindingId,
                    wake.SessionId,
                    wake.SnapshotFingerprint,
                    now)));
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await stateMachine.ApplyCommandAsync(command, 2, cancellationToken));

        Assert.Equal(0, await database.Store.GetLastAppliedIndexAsync(cancellationToken));
    }

    [Fact]
    public async Task Snapshot_callbacks_round_trip_the_complete_projection_and_checkpoint()
    {
        await using var source = await TestDatabase.CreateAsync();
        await using var target = await TestDatabase.CreateAsync();
        await using var sourceStateMachine = CreateStateMachine(source.Store, source.DirectoryPath);
        await using var targetStateMachine = CreateStateMachine(target.Store, target.DirectoryPath);
        var cancellationToken = TestContext.Current.CancellationToken;
        var now = DateTimeOffset.Parse("2026-10-07T01:02:03Z");
        var subscription = new SubscriptionSnapshot(
            "query-key", "https://drasi", "instance", "query", true, now, now, null);
        var mapping = new KeyMappingSnapshot("scope", "identity", "session", "active", now);
        var wake = CreateWake(now);
        var command = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.SetConfigurationFingerprint,
            new SetConfigurationFingerprintPayload("snapshot-configuration"));
        var accepted = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.MarkAcceptedWithCheckpoint,
            new MarkAcceptedWithCheckpointPayload(
                wake.Id,
                new WakeAcceptance("snapshot-invocation", now),
                new SnapshotCheckpoint(wake.BindingId, wake.SessionId, wake.SnapshotFingerprint, now)));
        await sourceStateMachine.ApplyCommandAsync(command, 1, cancellationToken);
        await sourceStateMachine.ApplyCommandAsync(
            ReplicatedBridgeCommand.Create(
                BridgeCommandKind.UpsertSubscription,
                new UpsertSubscriptionPayload(subscription)),
            2,
            cancellationToken);
        await sourceStateMachine.ApplyCommandAsync(
            ReplicatedBridgeCommand.Create(
                BridgeCommandKind.UpsertKeyMapping,
                new UpsertKeyMappingPayload(mapping)),
            3,
            cancellationToken);
        await sourceStateMachine.ApplyCommandAsync(
            ReplicatedBridgeCommand.Create(
                BridgeCommandKind.CreateOrUpdatePendingWake,
                new CreateOrUpdatePendingWakePayload(wake)),
            4,
            cancellationToken);
        await sourceStateMachine.ApplyCommandAsync(
            ReplicatedBridgeCommand.Create(
                BridgeCommandKind.ClaimDispatchable,
                new ClaimDispatchablePayload([wake.Id], now, Guid.NewGuid())),
            5,
            cancellationToken);
        await sourceStateMachine.ApplyCommandAsync(accepted, 6, cancellationToken);

        using var stream = new MemoryStream();
        var writer = IAsyncBinaryWriter.Create(stream, new byte[4096]);
        await sourceStateMachine.PersistSnapshotAsync(writer, cancellationToken);
        var snapshotPath = Path.Combine(source.DirectoryPath, "snapshot.json");
        await File.WriteAllBytesAsync(snapshotPath, stream.ToArray(), cancellationToken);

        await targetStateMachine.RestoreSnapshotAsync(new FileInfo(snapshotPath), cancellationToken);

        var restored = await target.Store.ExportSnapshotAsync(cancellationToken);
        Assert.Equal(6, restored.LastAppliedIndex);
        Assert.Equal(accepted.CommandId, restored.LastAppliedCommandId);
        Assert.Equal("snapshot-configuration", restored.ConfigurationFingerprint);
        Assert.Equal(subscription, Assert.Single(restored.Subscriptions));
        Assert.Equal(mapping, Assert.Single(restored.KeyMappings));
        Assert.Equal(
            new SnapshotCheckpointSnapshot(wake.BindingId, wake.SessionId, wake.SnapshotFingerprint, now),
            Assert.Single(restored.SnapshotCheckpoints));
        Assert.Equal(WakeOutboxStatus.Accepted, Assert.Single(restored.WakeOutbox).Status);

        var invalidSnapshotPath = Path.Combine(source.DirectoryPath, "invalid-snapshot.json");
        await File.WriteAllBytesAsync(
            invalidSnapshotPath,
            JsonSerializer.SerializeToUtf8Bytes(restored with
            {
                SchemaVersion = BridgeStoreSnapshot.CurrentSchemaVersion + 1
            }),
            cancellationToken);
        await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await targetStateMachine.RestoreSnapshotAsync(new FileInfo(invalidSnapshotPath), cancellationToken));
        Assert.Equal(
            JsonSerializer.Serialize(restored),
            JsonSerializer.Serialize(await target.Store.ExportSnapshotAsync(cancellationToken)));
    }

    [Fact]
    public async Task Single_node_host_replays_the_committed_log_into_a_fresh_projection()
    {
        await using var database = await TestDatabase.CreateAsync();
        await using var restoredDatabase = await TestDatabase.CreateAsync();
        using var testDirectory = TestDirectory.Create();
        var raftDirectory = Path.Combine(testDirectory.DirectoryPath, "raft");
        var snapshotDirectory = Path.Combine(raftDirectory, "snapshots");
        var port = GetAvailablePort();
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstCommand = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.SetConfigurationFingerprint,
            new SetConfigurationFingerprintPayload("persisted-configuration-1"));
        var secondCommand = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.SetConfigurationFingerprint,
            new SetConfigurationFingerprintPayload("persisted-configuration-2"));

        var host = await StartSingleNodeHostAsync(
            database.Store,
            raftDirectory,
            snapshotDirectory,
            port,
            cancellationToken);
        try
        {
            var cluster = host.Services.GetRequiredService<IRaftCluster>();
            var executor = new DotNextRaftCommandExecutor(cluster);
            await executor.ReplicateAsync(firstCommand, cancellationToken);
            await executor.ReplicateAsync(secondCommand, cancellationToken);
            Assert.Equal(2, await database.Store.GetLastAppliedIndexAsync(cancellationToken));
        }
        finally
        {
            await DisposeHostAsync(host, cancellationToken);
        }
        var reopenedHost = await StartSingleNodeHostAsync(
            restoredDatabase.Store,
            raftDirectory,
            snapshotDirectory,
            port,
            cancellationToken);
        try
        {
            var restored = await restoredDatabase.Store.ExportSnapshotAsync(cancellationToken);
            Assert.Equal(2, restored.LastAppliedIndex);
            Assert.Equal(secondCommand.CommandId, restored.LastAppliedCommandId);
            Assert.Equal("persisted-configuration-2", restored.ConfigurationFingerprint);
        }
        finally
        {
            await DisposeHostAsync(reopenedHost, cancellationToken);
        }
    }

    [Fact]
    public async Task Command_executor_rejects_non_leader_and_no_quorum_without_fallback()
    {
        var nonLeaderCluster = ClusterProxy.Create(isLeader: false, hasQuorum: true);
        var noQuorumCluster = ClusterProxy.Create(isLeader: true, hasQuorum: false);
        var nonLeaderExecutor = new DotNextRaftCommandExecutor(nonLeaderCluster.Cluster);
        var noQuorumExecutor = new DotNextRaftCommandExecutor(noQuorumCluster.Cluster);
        var command = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.SetConfigurationFingerprint,
            new SetConfigurationFingerprintPayload("configuration"));

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await nonLeaderExecutor.ReplicateAsync(command, TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await noQuorumExecutor.ReplicateAsync(command, TestContext.Current.CancellationToken));
        Assert.Equal(0, nonLeaderCluster.Proxy.ReplicationCalls);
        Assert.Equal(0, noQuorumCluster.Proxy.ReplicationCalls);
    }

    [Fact]
    public async Task Command_executor_waits_for_local_application_and_propagates_wait_cancellation()
    {
        await using var database = await TestDatabase.CreateAsync();
        using var directory = TestDirectory.Create();
        var raftDirectory = Path.Combine(directory.DirectoryPath, "raft");
        var host = await StartSingleNodeHostAsync(
            database.Store,
            raftDirectory,
            Path.Combine(raftDirectory, "snapshots"),
            GetAvailablePort(),
            TestContext.Current.CancellationToken);
        var applicationWaitStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseApplicationWait = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        long waitedIndex = 0;
        var cluster = host.Services.GetRequiredService<IRaftCluster>();
        var executor = new DotNextRaftCommandExecutor(cluster, async (index, token) =>
        {
            waitedIndex = index;
            applicationWaitStarted.TrySetResult();
            await releaseApplicationWait.Task.WaitAsync(token);
        });
        var command = ReplicatedBridgeCommand.Create(
            BridgeCommandKind.SetConfigurationFingerprint,
            new SetConfigurationFingerprintPayload("configuration"));

        try
        {
            var replication = executor.ReplicateAsync(command, TestContext.Current.CancellationToken).AsTask();
            var firstCompletion = await Task.WhenAny(applicationWaitStarted.Task, replication)
                .WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            Assert.Same(applicationWaitStarted.Task, firstCompletion);
            Assert.False(replication.IsCompleted);
            Assert.Equal(1, waitedIndex);

            releaseApplicationWait.TrySetResult();
            await replication;
            Assert.Equal(1, await database.Store.GetLastAppliedIndexAsync(TestContext.Current.CancellationToken));

            using var cancellation = new CancellationTokenSource();
            var canceledWaitStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var canceledExecutor = new DotNextRaftCommandExecutor(cluster, async (_, token) =>
            {
                canceledWaitStarted.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, token);
            });
            var canceledReplication = canceledExecutor.ReplicateAsync(
                ReplicatedBridgeCommand.Create(
                    BridgeCommandKind.SetConfigurationFingerprint,
                    new SetConfigurationFingerprintPayload("canceled-wait")),
                cancellation.Token).AsTask();
            await canceledWaitStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => canceledReplication);
            Assert.Equal(2, await database.Store.GetLastAppliedIndexAsync(TestContext.Current.CancellationToken));
        }
        finally
        {
            releaseApplicationWait.TrySetResult();
            await DisposeHostAsync(host, TestContext.Current.CancellationToken);
        }
    }

    private static RaftBridgeStateMachine CreateStateMachine(SonnetBridgeStore store, string directory) =>
        new(store, new DirectoryInfo(Path.Combine(directory, "snapshots")),
            2,
            NullLogger<RaftBridgeStateMachine>.Instance);

    private static LogEntry CreatePayloadlessLogEntry(long term, long index)
    {
        var constructor = typeof(LogEntry).GetConstructor(
            BindingFlags.Instance | BindingFlags.NonPublic,
            binder: null,
            types: [typeof(long), typeof(long)],
            modifiers: null)
            ?? throw new InvalidOperationException("DotNext LogEntry no-payload constructor was not found.");
        return (LogEntry)constructor.Invoke([term, index]);
    }

    private static ValueTask<bool> ApplyLogEntryAsync(
        RaftBridgeStateMachine stateMachine,
        LogEntry entry,
        CancellationToken cancellationToken)
    {
        var apply = typeof(RaftBridgeStateMachine).GetMethod(
            "ApplyAsync",
            BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("RaftBridgeStateMachine.ApplyAsync was not found.");
        return (ValueTask<bool>)apply.Invoke(stateMachine, [entry, cancellationToken])!;
    }

    private static WakeOutboxItem CreateWake(DateTimeOffset createdAtUtc) => new(
        Guid.NewGuid(),
        "binding",
        "session",
        "fingerprint",
        "skill",
        new JsonObject { ["value"] = 1 },
        "v1",
        $"idem-{Guid.NewGuid():N}",
        0,
        createdAtUtc,
        createdAtUtc,
        WakeOutboxStatus.Pending,
        null,
        null,
        "target");

    private static async Task<IHost> StartSingleNodeHostAsync(
        IRaftBridgeProjection projection,
        string raftDirectory,
        string snapshotDirectory,
        int port,
        CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(raftDirectory);
        Directory.CreateDirectory(snapshotDirectory);
        var hostBuilder = Host.CreateDefaultBuilder()
            .ConfigureWebHostDefaults(webHost => webHost
                .Configure(app => app.UseConsensusProtocolHandler())
                .ConfigureKestrel(options => options.Listen(
                    IPAddress.Loopback,
                    port,
                    listenOptions => listenOptions.Protocols = HttpProtocols.Http2)))
            .ConfigureServices(services =>
            {
                services.AddSingleton(projection);
                services
                    .UsePersistentConfigurationStorage(Path.Combine(raftDirectory, "configuration"))
                    .UseStateMachine<RaftBridgeStateMachine>(new WriteAheadLog.Options
                    {
                        Location = Path.Combine(raftDirectory, "wal"),
                        FlushInterval = Timeout.InfiniteTimeSpan
                    });
                services.Replace(ServiceDescriptor.Singleton<RaftBridgeStateMachine>(provider =>
                {
                    var stateMachine = new RaftBridgeStateMachine(
                        provider.GetRequiredService<IRaftBridgeProjection>(),
                        new DirectoryInfo(snapshotDirectory),
                        100,
                        NullLogger<RaftBridgeStateMachine>.Instance);
                    stateMachine.RestoreAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult();
                    return stateMachine;
                }));
            })
            .JoinCluster((memberConfiguration, _, _) =>
            {
                memberConfiguration.PublicEndPoint = new Uri($"http://127.0.0.1:{port}");
                memberConfiguration.ProtocolVersion = HttpProtocolVersion.Http2;
                memberConfiguration.ProtocolVersionPolicy = HttpVersionPolicy.RequestVersionExact;
                memberConfiguration.ColdStart = true;
            });

        var host = hostBuilder.Build();
        try
        {
            await host.StartAsync(cancellationToken);
            var cluster = host.Services.GetRequiredService<IRaftCluster>();
            await cluster.Readiness.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            await cluster.WaitForLeadershipAsync(cancellationToken);
            return host;
        }
        catch
        {
            await DisposeHostAsync(host, cancellationToken);
            throw;
        }
    }

    private static async ValueTask DisposeHostAsync(IHost host, CancellationToken cancellationToken)
    {
        await host.StopAsync(cancellationToken);
        if (host is IAsyncDisposable asyncDisposable)
            await asyncDisposable.DisposeAsync();
        else
            host.Dispose();
    }

    private static int GetAvailablePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    private sealed class TestDatabase(DbContextOptions<BridgeDbContext> options) : IAsyncDisposable
    {
        public SonnetBridgeStore Store { get; } = new(new ContextFactory(options));
        public string DirectoryPath { get; private set; } = string.Empty;

        public static async Task<TestDatabase> CreateAsync()
        {
            var directory = Path.Combine(
                AppContext.BaseDirectory,
                "raft-testdata",
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(directory);
            var options = new DbContextOptionsBuilder<BridgeDbContext>()
                .UseSonnetDB($"Data Source={directory}")
                .Options;
            var database = new TestDatabase(options) { DirectoryPath = directory };
            await using var context = new BridgeDbContext(options);
            await context.Database.MigrateAsync(TestContext.Current.CancellationToken);
            return database;
        }

        public ValueTask DisposeAsync()
        {
            Directory.Delete(DirectoryPath, recursive: true);
            return ValueTask.CompletedTask;
        }

        private sealed class ContextFactory(DbContextOptions<BridgeDbContext> options)
            : IDbContextFactory<BridgeDbContext>
        {
            public BridgeDbContext CreateDbContext() => new(options);

            public Task<BridgeDbContext> CreateDbContextAsync(CancellationToken cancellationToken = default) =>
                Task.FromResult(new BridgeDbContext(options));
        }

    }

    private sealed class TestDirectory : IDisposable
    {
        private TestDirectory(string directoryPath) => DirectoryPath = directoryPath;

        public string DirectoryPath { get; }

        public static TestDirectory Create()
        {
            var directory = Path.Combine(AppContext.BaseDirectory, "raft-testdata", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(directory);
            return new TestDirectory(directory);
        }

        public void Dispose() => Directory.Delete(DirectoryPath, recursive: true);
    }

    private class ClusterProxy : DispatchProxy
    {
        private readonly CancellationTokenSource _leadership = new();
        private readonly CancellationTokenSource _consensus = new();

        public int ReplicationCalls { get; private set; }

        public static (IRaftCluster Cluster, ClusterProxy Proxy) Create(bool isLeader, bool hasQuorum)
        {
            var cluster = DispatchProxy.Create<IRaftCluster, ClusterProxy>();
            var proxy = (ClusterProxy)(object)cluster;
            if (!isLeader)
                proxy._leadership.Cancel();
            if (!hasQuorum)
                proxy._consensus.Cancel();
            return (cluster, proxy);
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            var name = targetMethod?.Name ?? string.Empty;
            if (name == "get_LeadershipToken")
                return _leadership.Token;
            if (name == "get_ConsensusToken")
                return _consensus.Token;
            ReplicationCalls++;
            throw new NotSupportedException($"Unexpected DotNext cluster invocation: {name}.");
        }
    }
}
