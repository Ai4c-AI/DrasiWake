using System.Collections.Concurrent;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Text.Json.Serialization;
using DotNext.Net.Cluster;
using DotNext.Net.Cluster.Consensus.Raft;
using DotNext.Net.Cluster.Consensus.Raft.Http;
using DrasiWake.Adapters.DrasiServer;
using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Contracts;
using DrasiWake.Core.Domain;
using DrasiWake.Core.Pipeline;
using DrasiWake.Host;
using DrasiWake.Persistence.Raft;
using DrasiWake.Persistence.SonnetDB.Replication;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ProtoBuf.Grpc;
using ProtoBuf.Grpc.Client;

namespace DrasiWake.IntegrationTests.Fixtures;

internal sealed class RaftClusterFixture : IAsyncDisposable
{
    internal static readonly TimeSpan OperationTimeout = TimeSpan.FromSeconds(60);

    private const string ManagementToken = "integration-test-management-token";
    private const string CertificatePassword = "integration-test-certificate-password";
    private const string DirectoryPrefix = "DrasiWake-RaftCluster-";
    private readonly string rootDirectory;
    private readonly string certificateDirectory;
    private readonly string registryDirectory;
    private readonly LocalClusterServices localServices;
    private readonly X509Certificate2 rootCertificate;
    private readonly List<RaftClusterNode> nodes = [];
    private readonly int snapshotFrequency;
    private readonly IPAddress[] nodeAddresses =
    [
        IPAddress.Parse("127.0.0.1"),
        IPAddress.Parse("127.0.0.2"),
        IPAddress.Parse("127.0.0.3"),
        IPAddress.Parse("127.0.0.4")
    ];
    private readonly int managementPort;
    private bool disposed;

    private RaftClusterFixture(
        string rootDirectory,
        LocalClusterServices localServices,
        X509Certificate2 rootCertificate,
        int managementPort,
        int snapshotFrequency)
    {
        this.rootDirectory = rootDirectory;
        certificateDirectory = Path.Combine(rootDirectory, "certificates");
        registryDirectory = Path.Combine(rootDirectory, "registry");
        this.localServices = localServices;
        this.rootCertificate = rootCertificate;
        this.managementPort = managementPort;
        this.snapshotFrequency = snapshotFrequency;
        Directory.CreateDirectory(certificateDirectory);
        Directory.CreateDirectory(registryDirectory);
    }

    public IReadOnlyList<RaftClusterNode> Nodes => nodes;
    public LocalGatewayService Gateway => localServices.Gateway;

    public static async Task<RaftClusterFixture> CreateAsync(
        int nodeCount,
        CancellationToken cancellationToken = default,
        int snapshotFrequency = 2)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(nodeCount, 3);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(nodeCount, 4);
        ArgumentOutOfRangeException.ThrowIfLessThan(snapshotFrequency, 1);

        var rootDirectory = Path.Combine(
            GetFixtureArtifactRoot(),
            $"{DirectoryPrefix}{Guid.NewGuid():N}");
        Directory.CreateDirectory(rootDirectory);

        X509Certificate2? rootCertificate = null;
        LocalClusterServices? localServices = null;
        RaftClusterFixture? fixture = null;
        try
        {
            rootCertificate = CreateRootCertificate();
            localServices = await LocalClusterServices.StartAsync(cancellationToken);
            var managementPort = FindSharedLoopbackPort(
                [IPAddress.Parse("127.0.0.1"), IPAddress.Parse("127.0.0.2"), IPAddress.Parse("127.0.0.3"), IPAddress.Parse("127.0.0.4")]);

            fixture = new RaftClusterFixture(
                rootDirectory,
                localServices,
                rootCertificate,
                managementPort,
                snapshotFrequency);
            await fixture.WriteRegistryAsync(cancellationToken);
            fixture.PrepareNodes();
            foreach (var node in fixture.nodes.Take(nodeCount))
                await node.StartAsync(cancellationToken).ConfigureAwait(false);
            await fixture.WaitForLeaderAsync(cancellationToken).ConfigureAwait(false);

            return fixture;
        }
        catch
        {
            if (fixture is not null)
            {
                await fixture.DisposeAsync();
            }
            else
            {
                if (localServices is not null)
                    await localServices.DisposeAsync();
                rootCertificate?.Dispose();
                await DeleteNamedRootDirectoryAsync(rootDirectory);
            }

            throw;
        }
    }

    public async Task<RaftClusterNode> WaitForLeaderAsync(
        CancellationToken cancellationToken,
        long? minimumTermExclusive = null)
    {
        using var timeout = CreateTimeout(cancellationToken);
        using var poll = new PeriodicTimer(TimeSpan.FromMilliseconds(50));
        RaftClusterNode? stableLeader = null;
        long stableTerm = -1;
        long stableSince = 0;
        while (true)
        {
            var eligibleNodes = nodes
                .Where(node => node.IsRunning)
                .Select(node => (Node: node, Leadership: node.Services.GetRequiredService<IRaftLeadership>()))
                .Where(candidate => candidate.Leadership.IsLeader &&
                    candidate.Leadership.HasQuorum &&
                    (minimumTermExclusive is null || candidate.Leadership.Term > minimumTermExclusive.Value))
                .ToArray();
            if (eligibleNodes.Length == 1)
            {
                var candidate = eligibleNodes[0];
                if (!ReferenceEquals(stableLeader, candidate.Node) || stableTerm != candidate.Leadership.Term)
                {
                    stableLeader = candidate.Node;
                    stableTerm = candidate.Leadership.Term;
                    stableSince = System.Diagnostics.Stopwatch.GetTimestamp();
                }
                else if (TimeProvider.System.GetElapsedTime(stableSince) >= candidate.Node.Cluster.ElectionTimeout)
                {
                    return candidate.Node;
                }
            }
            else
            {
                stableLeader = null;
                stableTerm = -1;
            }

            if (!await poll.WaitForNextTickAsync(timeout.Token).ConfigureAwait(false))
                throw new InvalidOperationException("Leader polling ended before a stable leader was observed.");
        }
    }

    public async Task<WakeOutboxItem> CreatePendingWakeAsync(
        RaftClusterNode leader,
        CancellationToken cancellationToken,
        bool dispatchImmediately = false)
    {
        var pending = CreateWake(dispatchImmediately);
        using var timeout = CreateTimeout(cancellationToken);
        while (true)
        {
            try
            {
                return await Store(leader)
                    .CreateOrUpdatePendingWakeAsync(pending, timeout.Token)
                    .ConfigureAwait(false);
            }
            catch (InvalidOperationException exception) when (
                !timeout.IsCancellationRequested &&
                (exception is NotLeaderException ||
                    exception.Message == "Only the Raft leader can submit bridge commands." ||
                    exception.Message == "Bridge commands cannot be submitted without a Raft quorum."))
            {
                // A commit can race an election; replay the same outbox ID and idempotency key.
                leader.Services.GetRequiredService<ILogger<RaftClusterFixture>>()
                    .LogWarning("Fixture wake submission raced a Raft election; retrying the same wake.");
                leader = await WaitForLeaderAsync(timeout.Token).ConfigureAwait(false);
            }
        }
    }

    public WakeOutboxItem CreateWake(bool dispatchImmediately = false)
    {
        var now = DateTimeOffset.UtcNow;
        var id = Guid.NewGuid();
        return new WakeOutboxItem(
            id,
            "orders-binding",
            $"integration-session-{id:N}",
            $"snapshot-{id:N}",
            "triage-order",
            new System.Text.Json.Nodes.JsonObject
            {
                ["orderId"] = $"order-{id:N}",
                ["state"] = "ready"
            },
            "1.0.0",
            $"drasiwake:{id:N}",
            0,
            now,
            dispatchImmediately ? now : now.AddHours(1),
            WakeOutboxStatus.Pending,
            null,
            null,
            "sample-gateway");
    }

    public IBridgeStore Store(RaftClusterNode node)
        => node.Services.GetRequiredService<IBridgeStore>();

    public long Term(RaftClusterNode node)
        => node.Cluster.AuditTrail.Term;

    public long LastCommittedIndex(RaftClusterNode node)
        => node.Cluster.AuditTrail.LastCommittedEntryIndex;

    public async Task<long> LastAppliedIndexAsync(
        RaftClusterNode node,
        CancellationToken cancellationToken)
        => await node.Projection.GetLastAppliedIndexAsync(cancellationToken).ConfigureAwait(false);

    public IReadOnlyList<Uri> Members(RaftClusterNode node)
        => node.Services.GetRequiredService<IRaftMembershipRuntime>()
            .Members
            .Select(member => member.Endpoint)
            .ToArray();

    public IReadOnlyList<FileInfo> GetSnapshotFiles(RaftClusterNode node)
    {
        var snapshotDirectory = Path.Combine(node.RaftDataPath, "snapshots");
        if (!Directory.Exists(snapshotDirectory))
            return [];

        return Directory.EnumerateFiles(snapshotDirectory, "*", SearchOption.AllDirectories)
            .Where(path => !path.EndsWith(".tmp", StringComparison.OrdinalIgnoreCase))
            .Select(path => new FileInfo(path))
            .Where(file => file.Exists && file.Length > 0)
            .ToArray();
    }

    public async Task<BridgeStoreSnapshot?> ReadLatestPersistedSnapshotAsync(
        RaftClusterNode node,
        CancellationToken cancellationToken)
    {
        BridgeStoreSnapshot? latest = null;
        foreach (var file in GetSnapshotFiles(node))
        {
            var snapshot = JsonSerializer.Deserialize<BridgeStoreSnapshot>(
                await File.ReadAllBytesAsync(file.FullName, cancellationToken).ConfigureAwait(false),
                new JsonSerializerOptions { Converters = { new JsonStringEnumConverter() } })
                ?? throw new InvalidDataException($"Fixture snapshot '{file.Name}' is empty.");
            if (latest is null || snapshot.LastAppliedIndex > latest.LastAppliedIndex)
                latest = snapshot;
        }
        return latest;
    }

    public async Task WaitForPersistedSnapshotAsync(
        RaftClusterNode node,
        long minimumIndex,
        CancellationToken cancellationToken)
    {
        using var timeout = CreateTimeout(cancellationToken);
        using var poll = new PeriodicTimer(TimeSpan.FromMilliseconds(50));
        while ((await ReadLatestPersistedSnapshotAsync(node, timeout.Token).ConfigureAwait(false))
            ?.LastAppliedIndex is not { } index || index < minimumIndex)
        {
            if (!await poll.WaitForNextTickAsync(timeout.Token).ConfigureAwait(false))
                throw new InvalidOperationException("Snapshot polling ended before the durable boundary was observed.");
        }
    }

    public void SetAsideStoppedProjection(RaftClusterNode node)
    {
        if (!nodes.Contains(node) || node.IsRunning ||
            !Path.GetFullPath(node.DatabasePath).StartsWith(
                Path.GetFullPath(rootDirectory) + Path.DirectorySeparatorChar,
                StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Only a stopped fixture-owned projection can be reset.");

        Directory.Move(node.DatabasePath, node.DatabasePath + ".before-rebuild");
    }

    internal static HttpClientHandler CreateTrustedHttpHandler(X509Certificate2 trustedRoot)
        => new()
        {
            ServerCertificateCustomValidationCallback = (_, certificate, _, errors) =>
            {
                if (certificate is null ||
                    errors is not SslPolicyErrors.None and not SslPolicyErrors.RemoteCertificateChainErrors)
                    return false;

                using var chain = new X509Chain();
                chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                chain.ChainPolicy.CustomTrustStore.Add(trustedRoot);
                chain.ChainPolicy.ApplicationPolicy.Add(new Oid("1.3.6.1.5.5.7.3.1"));
                chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                return chain.Build(certificate);
            }
        };

    public async Task<BridgeStoreSnapshot> ReadProjectionAsync(
        RaftClusterNode node,
        CancellationToken cancellationToken)
        => await node.Projection.ExportSnapshotAsync(cancellationToken).ConfigureAwait(false);

    public async Task WaitForWakeStatusAsync(
        RaftClusterNode node,
        Guid wakeId,
        WakeOutboxStatus status,
        CancellationToken cancellationToken)
    {
        using var timeout = CreateTimeout(cancellationToken);
        using var poll = new PeriodicTimer(TimeSpan.FromMilliseconds(100));
        while (true)
        {
            var wake = await node.Projection.ReadWakeAsync(wakeId, timeout.Token).ConfigureAwait(false);
            if (wake?.Status == status)
                return;

            if (!await poll.WaitForNextTickAsync(timeout.Token).ConfigureAwait(false))
                throw new InvalidOperationException(
                    $"Node '{node.NodeId}' stopped polling before wake '{wakeId}' reached status '{status}'.");
        }
    }

    public bool BusinessStateEquals(BridgeStoreSnapshot left, BridgeStoreSnapshot right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        var normalizedLeft = left with { LastAppliedIndex = 0, LastAppliedCommandId = null };
        var normalizedRight = right with { LastAppliedIndex = 0, LastAppliedCommandId = null };
        return string.Equals(
            JsonSerializer.Serialize(normalizedLeft),
            JsonSerializer.Serialize(normalizedRight),
            StringComparison.Ordinal);
    }

    public async Task WaitForAllAvailableProjectionsAsync(
        long raftIndex,
        CancellationToken cancellationToken)
    {
        var activeMembers = await GetActiveMemberNodesAsync(cancellationToken).ConfigureAwait(false);
        var latestCommittedIndex = Math.Max(
            raftIndex,
            activeMembers.Max(LastCommittedIndex));
        foreach (var node in activeMembers)
            await WaitForProjectionAsync(node, latestCommittedIndex, cancellationToken).ConfigureAwait(false);
    }

    public async Task WaitForProjectionAsync(
        RaftClusterNode node,
        long raftIndex,
        CancellationToken cancellationToken)
    {
        using var timeout = CreateTimeout(cancellationToken);
        await node.Cluster.Readiness.WaitAsync(timeout.Token).ConfigureAwait(false);
        await node.Cluster.AuditTrail.WaitForApplyAsync(raftIndex, timeout.Token).ConfigureAwait(false);
        using var poll = new PeriodicTimer(TimeSpan.FromMilliseconds(25));
        while (await node.Projection.GetLastAppliedIndexAsync(timeout.Token).ConfigureAwait(false) < raftIndex)
        {
            if (!await poll.WaitForNextTickAsync(timeout.Token).ConfigureAwait(false))
                break;
        }
    }

    public async Task AssertAvailableProjectionsEqualAsync(CancellationToken cancellationToken)
    {
        var activeMembers = await GetActiveMemberNodesAsync(cancellationToken).ConfigureAwait(false);
        if (activeMembers.Count < 2)
        {
            var memberSummary = string.Join(
                "; ",
                nodes.Where(node => node.IsRunning).Select(node =>
                    $"{node.NodeId}=[{string.Join(", ", Members(node))}]"));
            throw new InvalidOperationException(
                $"At least two active Raft projections are required for equality checks. Runtime members: {memberSummary}");
        }

        using var timeout = CreateTimeout(cancellationToken);
        while (true)
        {
            var projections = await Task.WhenAll(activeMembers.Select(node =>
                ReadProjectionAsync(node, timeout.Token))).ConfigureAwait(false);
            var latestIndex = projections.Max(snapshot => snapshot.LastAppliedIndex);
            var laggingNodes = activeMembers
                .Zip(projections)
                .Where(pair => pair.Second.LastAppliedIndex < latestIndex)
                .Select(pair => pair.First)
                .ToArray();

            if (laggingNodes.Length != 0)
            {
                foreach (var node in laggingNodes)
                    await WaitForProjectionAsync(node, latestIndex, timeout.Token).ConfigureAwait(false);
                continue;
            }

            var first = projections[0];
            for (var index = 1; index < projections.Length; index++)
            {
                if (BusinessStateEquals(first, projections[index]))
                    continue;

                throw new InvalidOperationException(
                    $"Raft projection business state differs between nodes '{activeMembers[0].NodeId}' " +
                    $"and '{activeMembers[index].NodeId}' at index {latestIndex}.");
            }

            return;
        }
    }

    public async Task WaitForQuorumLossAsync(
        RaftClusterNode node,
        CancellationToken cancellationToken)
    {
        using var timeout = CreateTimeout(cancellationToken);
        using var poll = new PeriodicTimer(TimeSpan.FromMilliseconds(50));
        var leadership = node.Services.GetRequiredService<IRaftLeadership>();
        try
        {
            while (leadership.HasQuorum || DotNextRaftQuorum.HasAvailableMajority(node.Cluster))
            {
                if (!await poll.WaitForNextTickAsync(timeout.Token).ConfigureAwait(false))
                    break;
            }
            return;
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            var states = string.Join(
                "; ",
                nodes.Where(candidate => candidate.IsRunning).Select(candidate =>
                    $"{candidate.NodeId}: leader={candidate.Services.GetRequiredService<IRaftLeadership>().IsLeader}, " +
                    $"quorum={candidate.Services.GetRequiredService<IRaftLeadership>().HasQuorum}, " +
                    $"term={candidate.Cluster.AuditTrail.Term}, " +
                    $"leadershipTokenCanceled={candidate.Cluster.LeadershipToken.IsCancellationRequested}, " +
                    $"consensusTokenCanceled={candidate.Cluster.ConsensusToken.IsCancellationRequested}, " +
                    $"electionTimeout={candidate.Cluster.ElectionTimeout}, " +
                    $"members=[{string.Join(", ", candidate.Cluster.Members.Select(member =>
                        member is IClusterMember clusterMember
                            ? $"{clusterMember.GetType().Name}:status={clusterMember.Status};leader={clusterMember.IsLeader}"
                            : member.GetType().Name))}]"));
            throw new TimeoutException(
                $"Timed out waiting for quorum loss on node '{node.NodeId}'. Live Raft state: {states}");
        }

        throw new InvalidOperationException($"Node '{node.NodeId}' stopped watching leadership before quorum loss.");
    }

    public async Task StopNodeWithoutMembershipChangeAsync(
        RaftClusterNode node,
        CancellationToken cancellationToken)
        => await node.StopAsync(cancellationToken).ConfigureAwait(false);

    public async Task RestartNodeAsync(
        RaftClusterNode node,
        CancellationToken cancellationToken)
        => await node.StartAsync(cancellationToken).ConfigureAwait(false);

    public async Task<RaftClusterNode> StartFourthNodeAsync(CancellationToken cancellationToken)
    {
        if (nodes.Count < 4)
            throw new InvalidOperationException("The fourth node configuration was not prepared.");
        var candidate = nodes[3];
        await candidate.StartAsync(cancellationToken).ConfigureAwait(false);
        return candidate;
    }

    public async Task AddMemberThroughManagementGrpcAsync(
        RaftClusterNode leader,
        RaftClusterNode candidate,
        CancellationToken cancellationToken)
    {
        using var timeout = CreateTimeout(cancellationToken);
        using var channel = GrpcChannel.ForAddress(
            leader.ManagementAddress,
            new GrpcChannelOptions { HttpHandler = CreateTrustedHttpHandler(rootCertificate) });
        var client = channel.CreateGrpcService<IClusterMembershipService>();
        var headers = new Metadata { { "authorization", $"Bearer {ManagementToken}" } };
        var response = await client.Add(
            new ClusterMemberRequest { Endpoint = candidate.ListenAddress.AbsoluteUri },
            new CallContext(new CallOptions(headers: headers, cancellationToken: timeout.Token)))
            .WaitAsync(OperationTimeout, cancellationToken)
            .ConfigureAwait(false);
        if (!response.Succeeded)
            throw new InvalidOperationException("Authenticated Raft membership add did not succeed.");
    }

    public async Task RemoveMemberThroughManagementGrpcAsync(
        RaftClusterNode leader,
        RaftClusterNode candidate,
        CancellationToken cancellationToken)
    {
        using var timeout = CreateTimeout(cancellationToken);
        using var channel = GrpcChannel.ForAddress(
            leader.ManagementAddress,
            new GrpcChannelOptions { HttpHandler = CreateTrustedHttpHandler(rootCertificate) });
        var client = channel.CreateGrpcService<IClusterMembershipService>();
        var headers = new Metadata { { "authorization", $"Bearer {ManagementToken}" } };
        var response = await client.Remove(
            new ClusterMemberRequest { Endpoint = candidate.ListenAddress.AbsoluteUri },
            new CallContext(new CallOptions(headers: headers, cancellationToken: timeout.Token)))
            .WaitAsync(OperationTimeout, cancellationToken)
            .ConfigureAwait(false);
        if (!response.Succeeded)
            throw new InvalidOperationException("Authenticated Raft membership remove did not succeed.");
    }

    public async ValueTask DisposeAsync()
    {
        if (disposed)
            return;
        disposed = true;

        var failures = new List<Exception>();
        foreach (var node in nodes.AsEnumerable().Reverse())
        {
            try
            {
                await node.StopAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                failures.Add(new InvalidOperationException($"Failed to stop fixture node '{node.NodeId}'.", exception));
            }
        }

        try
        {
            await localServices.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            failures.Add(exception);
        }
        finally
        {
            rootCertificate.Dispose();
        }
        // DotNext 6.9.0 awaits its final SnapshotWriter on disposal but leaves its handle to finalization.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        try
        {
            await DeleteNamedRootDirectoryAsync(rootDirectory).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            failures.Add(exception);
        }
        if (failures.Count != 0)
            throw new AggregateException("Raft fixture cleanup failed.", failures);
    }

    private void PrepareNodes()
    {
        var addresses = nodeAddresses;
        var listenPorts = new HashSet<int> { managementPort };
        var raftEndpoints = new Uri[4];
        var managementEndpoints = new Uri[4];
        for (var index = 0; index < 4; index++)
        {
            var raftPort = FindUniquePort(addresses[index], listenPorts);
            raftEndpoints[index] = new Uri($"https://{addresses[index]}:{raftPort}/");
            managementEndpoints[index] = new Uri($"https://{addresses[index]}:{managementPort}/");
        }

        for (var index = 0; index < 4; index++)
        {
            var nodeDirectory = Path.Combine(rootDirectory, "nodes", $"node-{index + 1}");
            var databasePath = Path.Combine(nodeDirectory, "sonnetdb");
            var raftDataPath = Path.Combine(nodeDirectory, "raft");
            var certificatePath = Path.Combine(certificateDirectory, $"node-{index + 1}.pfx");
            Directory.CreateDirectory(nodeDirectory);
            WriteNodeCertificate(
                rootCertificate,
                addresses[index],
                certificatePath);
            var configuration = CreateNodeConfiguration(
                index,
                raftEndpoints,
                managementEndpoints[index],
                databasePath,
                raftDataPath,
                certificatePath);
            nodes.Add(new RaftClusterNode(
                $"node-{index + 1}",
                raftEndpoints[index],
                managementEndpoints[index],
                databasePath,
                raftDataPath,
                configuration,
                rootCertificate));
        }
    }

    private IConfiguration CreateNodeConfiguration(
        int nodeIndex,
        IReadOnlyList<Uri> raftEndpoints,
        Uri managementAddress,
        string databasePath,
        string raftDataPath,
        string certificatePath)
    {
        var values = new Dictionary<string, string?>
        {
            ["Logging:LogLevel:Default"] = "Warning",
            ["Logging:LogLevel:Microsoft.AspNetCore"] = "Warning",
            ["Logging:LogLevel:DrasiWake.Host.ClusterMembershipGrpcService"] = "Information",
            ["DrasiWake:Drasi:ServerUri"] = localServices.BaseAddress.AbsoluteUri,
            ["DrasiWake:Drasi:InitialReconnectDelay"] = "00:00:01",
            ["DrasiWake:Drasi:MaxReconnectDelay"] = "00:00:02",
            ["DrasiWake:Database:Path"] = databasePath,
            ["DrasiWake:Registry:Path"] = Path.Combine(registryDirectory, "bindings.yaml"),
            ["DrasiWake:WorkerCount"] = "1",
            ["DrasiWake:ChannelCapacity"] = "16",
            ["DrasiWake:ReconciliationInterval"] = "00:01:00",
            ["DrasiWake:DispatchPollInterval"] = "00:00:00.100",
            ["DrasiWake:OpenClaw:MaxRetryAttempts"] = "0",
            ["DrasiWake:OpenClaw:Targets:sample-gateway:BaseAddress"] = localServices.BaseAddress.AbsoluteUri,
            ["DrasiWake:OpenClaw:Targets:sample-gateway:GatewayIdempotencyRetention"] = "30.00:00:00",
            ["DrasiWake:Cluster:Mode"] = "Cluster",
            ["DrasiWake:Cluster:NodeId"] = $"node-{nodeIndex + 1}",
            ["DrasiWake:Cluster:ListenAddress"] = raftEndpoints[nodeIndex].AbsoluteUri,
            ["DrasiWake:Cluster:RaftDataPath"] = raftDataPath,
            ["DrasiWake:Cluster:Certificate:Path"] = certificatePath,
            ["DrasiWake:Cluster:Certificate:Password"] = CertificatePassword,
            ["DrasiWake:Cluster:Management:Address"] = managementAddress.AbsoluteUri,
            ["DrasiWake:Cluster:Management:BearerToken"] = ManagementToken,
            ["DrasiWake:Cluster:SnapshotFrequency"] = snapshotFrequency.ToString(
                System.Globalization.CultureInfo.InvariantCulture)
        };
        var initialMembers = nodeIndex < 3
            ? raftEndpoints.Take(3).ToArray()
            : raftEndpoints.ToArray();
        for (var index = 0; index < initialMembers.Length; index++)
        {
            values[$"DrasiWake:Cluster:InitialMembers:{index}"] = initialMembers[index].AbsoluteUri;
        }
        for (var index = 0; index < 3; index++)
            values[$"DrasiWake:Cluster:BootstrapMembers:{index}"] = raftEndpoints[index].AbsoluteUri;
        return new ConfigurationBuilder().AddInMemoryCollection(values).Build();
    }

    private async Task WriteRegistryAsync(CancellationToken cancellationToken)
    {
        await File.WriteAllTextAsync(
            Path.Combine(registryDirectory, "facts.schema.json"),
            "{\"type\":\"object\",\"required\":[\"orderId\",\"state\"],\"properties\":{\"orderId\":{\"type\":\"string\"},\"state\":{\"type\":\"string\"}}}",
            cancellationToken).ConfigureAwait(false);
        await File.WriteAllTextAsync(
            Path.Combine(registryDirectory, "bindings.yaml"),
            string.Join(
                Environment.NewLine,
                [
                    "version: 1.0.0",
                    "bindings:",
                    "  - id: orders-binding",
                    "    source: drasi-server",
                    "    server: http://drasi.test",
                    "    instanceId: east",
                    "    queryId: orders",
                    "    deliveryMode: converge-latest",
                    "    sessionScope: singleton",
                    "    openClawTarget: sample-gateway",
                    "    metaSkill: triage-order",
                    "    contractVersion: 1.0.0",
                    "    factSchemaPath: facts.schema.json",
                    "    maxPayloadBytes: 32768",
                    "    retry:",
                    "      maxAttempts: 3",
                    "      maxAgeSeconds: 86400",
                    "    rateLimit:",
                    "      permitLimit: 20",
                    "      windowMilliseconds: 1000"
                ]),
            cancellationToken).ConfigureAwait(false);
    }

    private async Task<IReadOnlyList<RaftClusterNode>> GetActiveMemberNodesAsync(
        CancellationToken cancellationToken)
    {
        var leader = await WaitForLeaderAsync(cancellationToken).ConfigureAwait(false);
        var members = Members(leader).ToHashSet();
        return nodes.Where(node => node.IsRunning && members.Contains(node.ListenAddress)).ToArray();
    }

    private static CancellationTokenSource CreateTimeout(CancellationToken cancellationToken)
    {
        var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        linked.CancelAfter(OperationTimeout);
        return linked;
    }

    private static int FindSharedLoopbackPort(IReadOnlyList<IPAddress> addresses)
    {
        for (var attempt = 0; attempt < 100; attempt++)
        {
            var port = GetAvailablePort(addresses[0]);
            var listeners = new List<TcpListener>();
            try
            {
                foreach (var address in addresses)
                {
                    var listener = new TcpListener(address, port);
                    listener.Start();
                    listeners.Add(listener);
                }
                return port;
            }
            catch (SocketException)
            {
            }
            finally
            {
                foreach (var listener in listeners)
                    listener.Stop();
            }
        }

        throw new InvalidOperationException("A shared dynamically selected test management port could not be reserved.");
    }

    private static int FindUniquePort(IPAddress address, ISet<int> usedPorts)
    {
        while (true)
        {
            var port = GetAvailablePort(address);
            if (usedPorts.Add(port))
                return port;
        }
    }

    private static int GetAvailablePort(IPAddress address)
    {
        var listener = new TcpListener(address, 0);
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }

    private static X509Certificate2 CreateRootCertificate()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            "CN=DrasiWake Raft Integration Test Root",
            rsa,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(
            X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign,
            true));
        request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));
        return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddDays(2));
    }

    private static void WriteNodeCertificate(
        X509Certificate2 rootCertificate,
        IPAddress address,
        string path)
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN={address}",
            rsa,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(
            X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment,
            false));
        var enhancedKeyUsage = new OidCollection { new("1.3.6.1.5.5.7.3.1") };
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(enhancedKeyUsage, false));
        var subjectAlternativeNames = new SubjectAlternativeNameBuilder();
        subjectAlternativeNames.AddIpAddress(address);
        request.CertificateExtensions.Add(subjectAlternativeNames.Build());
        request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));

        var serialNumber = RandomNumberGenerator.GetBytes(16);
        using var publicCertificate = request.Create(
            rootCertificate,
            DateTimeOffset.UtcNow.AddMinutes(-1),
            DateTimeOffset.UtcNow.AddDays(1),
            serialNumber);
        using var certificate = publicCertificate.CopyWithPrivateKey(rsa);
        File.WriteAllBytes(path, certificate.Export(X509ContentType.Pfx, CertificatePassword));
    }

    private static async Task DeleteNamedRootDirectoryAsync(string directory)
    {
        var fullDirectory = Path.GetFullPath(directory);
        var artifactRoot = Path.GetFullPath(GetFixtureArtifactRoot());
        if (!Path.GetDirectoryName(fullDirectory)!.Equals(
                artifactRoot.TrimEnd(Path.DirectorySeparatorChar),
                StringComparison.OrdinalIgnoreCase) ||
            !Path.GetFileName(fullDirectory).StartsWith(DirectoryPrefix, StringComparison.Ordinal) ||
            !Guid.TryParseExact(Path.GetFileName(fullDirectory)[DirectoryPrefix.Length..], "N", out _))
        {
            throw new InvalidOperationException("Refusing to clean a non-fixture or non-specific temporary directory.");
        }

        for (var attempt = 0; attempt < 10 && Directory.Exists(fullDirectory); attempt++)
        {
            try
            {
                Directory.Delete(fullDirectory, recursive: true);
                return;
            }
            catch (IOException) when (attempt < 9)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(Math.Min(100 * (1 << Math.Min(attempt, 4)), 1600)))
                    .ConfigureAwait(false);
            }
            catch (UnauthorizedAccessException) when (attempt < 9)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(Math.Min(100 * (1 << Math.Min(attempt, 4)), 1600)))
                    .ConfigureAwait(false);
            }
        }

        if (Directory.Exists(fullDirectory))
            throw new IOException("A named Raft integration-test directory remained locked after all hosts were disposed.");
    }

    private static string GetFixtureArtifactRoot()
        => Path.Combine(AppContext.BaseDirectory, "test-artifacts");

}

internal sealed class RaftClusterNode(
    string nodeId,
    Uri listenAddress,
    Uri managementAddress,
    string databasePath,
    string raftDataPath,
    IConfiguration configuration,
    X509Certificate2 trustedRoot) : IAsyncDisposable
{
    private IHost? host;

    public string NodeId { get; } = nodeId;
    public Uri ListenAddress { get; } = listenAddress;
    public Uri ManagementAddress { get; } = managementAddress;
    public string DatabasePath { get; } = databasePath;
    public string RaftDataPath { get; } = raftDataPath;
    public bool IsRunning => host is not null;
    public IHost Host => host ?? throw new InvalidOperationException($"Node '{NodeId}' is not running.");
    public IServiceProvider Services => Host.Services;
    public IRaftCluster Cluster => Services.GetRequiredService<IRaftCluster>();
    public IRaftBridgeProjection Projection => Services.GetRequiredService<IRaftBridgeProjection>();

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (host is not null)
            throw new InvalidOperationException($"Node '{NodeId}' is already running.");
        host = DrasiWakeHostBuilder.CreateHost(
            configuration,
            services =>
            {
                services.PostConfigure<HttpClusterMemberConfiguration>(options =>
                {
                    options.LowerElectionTimeout = 2000;
                    options.UpperElectionTimeout = 4000;
                });
                services.AddHttpClient(DrasiWakeHostBuilder.RaftHttpClientName)
                    .ConfigurePrimaryHttpMessageHandler(
                        () => RaftClusterFixture.CreateTrustedHttpHandler(trustedRoot));
                services.AddHttpClient(DrasiWakeHostBuilder.ManagementHttpClientName)
                    .ConfigurePrimaryHttpMessageHandler(
                        () => RaftClusterFixture.CreateTrustedHttpHandler(trustedRoot));
            });
        try
        {
            await host.StartAsync(cancellationToken)
                .WaitAsync(RaftClusterFixture.OperationTimeout, cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            await DisposeHostAsync().ConfigureAwait(false);
            throw;
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (host is null)
            return;

        var stoppingHost = host;
        host = null;
        try
        {
            await stoppingHost.StopAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await DisposeHostResourcesAsync(stoppingHost).ConfigureAwait(false);
        }
    }

    public ValueTask DisposeAsync() => new(StopAsync(CancellationToken.None));

    private async Task DisposeHostAsync()
    {
        if (host is not { } currentHost)
            return;
        host = null;
        await DisposeHostResourcesAsync(currentHost).ConfigureAwait(false);
    }

    private static async ValueTask DisposeHostResourcesAsync(IHost currentHost)
    {
        if (currentHost is IAsyncDisposable asyncHost)
            await asyncHost.DisposeAsync().ConfigureAwait(false);
        else
            currentHost.Dispose();
    }
}

internal sealed class LocalClusterServices : IAsyncDisposable
{
    private const string JsonContentType = "application/json";
    private readonly WebApplication application;

    private LocalClusterServices(WebApplication application, Uri baseAddress, LocalGatewayService gateway)
    {
        this.application = application;
        BaseAddress = baseAddress;
        Gateway = gateway;
    }

    public Uri BaseAddress { get; }
    public LocalGatewayService Gateway { get; }

    public static async Task<LocalClusterServices> StartAsync(CancellationToken cancellationToken)
    {
        var gateway = new LocalGatewayService();
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            EnvironmentName = Environments.Development
        });
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.WebHost.ConfigureKestrel(options => options.Listen(IPAddress.Loopback, 0));
        var application = builder.Build();

        application.MapGet("/api/v1/instances", () =>
            Results.Content("{\"success\":true,\"data\":[{\"id\":\"east\"}],\"error\":null}", JsonContentType));
        application.MapGet("/api/v1/instances/east/queries", () =>
            Results.Content("{\"success\":true,\"data\":[{\"id\":\"orders\"}],\"error\":null}", JsonContentType));
        application.MapGet("/api/v1/instances/east/queries/orders/results", () =>
            Results.Content("{\"success\":true,\"data\":[],\"error\":null}", JsonContentType));
        application.MapGet(
            "/api/v1/instances/east/queries/orders/attach",
            async (HttpContext context) =>
            {
                context.Response.ContentType = "text/event-stream";
                await context.Response.WriteAsync(": keep-alive\n\n", context.RequestAborted).ConfigureAwait(false);
                await context.Response.Body.FlushAsync(context.RequestAborted).ConfigureAwait(false);
                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, context.RequestAborted).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)
                {
                }
            });
        application.MapPost(
            "/api/integration/meta-invocations",
            (HttpContext context) => gateway.HandleRequestAsync(context));
        await application.StartAsync(cancellationToken).ConfigureAwait(false);

        var server = application.Services.GetRequiredService<Microsoft.AspNetCore.Hosting.Server.IServer>();
        var address = server.Features.Get<IServerAddressesFeature>()?.Addresses.SingleOrDefault()
            ?? throw new InvalidOperationException("The local fake Drasi/Gateway server did not publish its dynamic address.");
        return new LocalClusterServices(application, new Uri(address), gateway);
    }

    public async ValueTask DisposeAsync()
    {
        await application.StopAsync(CancellationToken.None).ConfigureAwait(false);
        await application.DisposeAsync().ConfigureAwait(false);
    }
}

internal sealed record LocalGatewayRequest(string IdempotencyKey, string Body);
internal sealed record LogicalGatewayInvocation(string IdempotencyKey, Guid InvocationId);

internal sealed class LocalGatewayService
{
    private readonly ConcurrentDictionary<string, Guid> invocationIds = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, int> requestCounts = new(StringComparer.Ordinal);
    private readonly ConcurrentQueue<LocalGatewayRequest> requests = new();
    private readonly TaskCompletionSource firstRequestReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private TaskCompletionSource releaseFirstResponse = NewSignal();
    private int holdFirstResponse;
    private int releaseAsTimeout;

    public IReadOnlyList<LocalGatewayRequest> Requests => requests.ToArray();
    public IReadOnlyList<LogicalGatewayInvocation> LogicalInvocations
        => invocationIds.Select(pair => new LogicalGatewayInvocation(pair.Key, pair.Value)).ToArray();
    public Task FirstRequestReceived => firstRequestReceived.Task;

    public void HoldAcceptedResponse()
    {
        releaseFirstResponse = NewSignal();
        Volatile.Write(ref releaseAsTimeout, 0);
        Volatile.Write(ref holdFirstResponse, 1);
    }

    public void ReleaseFirstResponseAsTimeout()
    {
        Volatile.Write(ref releaseAsTimeout, 1);
        releaseFirstResponse.TrySetResult();
    }

    public async Task HandleRequestAsync(HttpContext context)
    {
        var idempotencyKey = context.Request.Headers["Idempotency-Key"].SingleOrDefault();
        if (string.IsNullOrWhiteSpace(idempotencyKey))
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            return;
        }

        using var document = await JsonDocument.ParseAsync(
            context.Request.Body,
            cancellationToken: context.RequestAborted).ConfigureAwait(false);
        var body = document.RootElement.GetRawText();
        var invocationId = invocationIds.GetOrAdd(idempotencyKey, static _ => Guid.NewGuid());
        requests.Enqueue(new LocalGatewayRequest(idempotencyKey, body));
        var requestNumber = requestCounts.AddOrUpdate(idempotencyKey, 1, static (_, count) => count + 1);
        firstRequestReceived.TrySetResult();

        if (requestNumber == 1 && Interlocked.Exchange(ref holdFirstResponse, 0) == 1)
        {
            await releaseFirstResponse.Task.WaitAsync(context.RequestAborted).ConfigureAwait(false);
            if (Volatile.Read(ref releaseAsTimeout) == 1)
            {
                context.Response.StatusCode = StatusCodes.Status504GatewayTimeout;
                await context.Response.WriteAsJsonAsync(
                    Response(invocationId, "Running"),
                    context.RequestAborted).ConfigureAwait(false);
                return;
            }
        }

        var status = requestNumber > 1 ? "Completed" : "Running";
        context.Response.StatusCode = StatusCodes.Status202Accepted;
        await context.Response.WriteAsJsonAsync(
            Response(invocationId, status),
            context.RequestAborted).ConfigureAwait(false);
    }

    private static object Response(Guid invocationId, string status) => new
    {
        invocationId,
        status,
        error = (string?)null,
        createdAtUtc = DateTimeOffset.UtcNow
    };

    private static TaskCompletionSource NewSignal()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
