using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Security.Authentication;
using DotNext.Net;
using DotNext.Net.Cluster;
using DotNext.Net.Cluster.Consensus.Raft;
using DotNext.Net.Cluster.Consensus.Raft.Http;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Connections;
using Microsoft.Extensions.Http;
using ProtoBuf.Grpc;
using ProtoBuf.Grpc.Client;

namespace DrasiWake.Host;

public sealed class ClusterCompatibilityProvider(
    DrasiWakeHostSettings settings,
    DrasiWake.Core.Contracts.ContractRegistryManager registryManager,
    DrasiWake.Persistence.SonnetDB.Replication.IRaftBridgeProjection projection)
    : IClusterCompatibilityProvider
{
    private readonly DrasiWakeHostSettings _settings = settings ?? throw new ArgumentNullException(nameof(settings));
    private readonly DrasiWake.Core.Contracts.ContractRegistryManager _registryManager =
        registryManager ?? throw new ArgumentNullException(nameof(registryManager));
    private readonly DrasiWake.Persistence.SonnetDB.Replication.IRaftBridgeProjection _projection =
        projection ?? throw new ArgumentNullException(nameof(projection));

    public async Task<ClusterCompatibilityResponse> GetLocalCompatibilityAsync(CancellationToken cancellationToken)
    {
        var registry = _registryManager.Active;
        if (ReferenceEquals(registry, DrasiWake.Core.Contracts.ContractRegistry.Empty))
            throw new InvalidOperationException("The validated contract registry is not active.");

        var fingerprint = ClusterBusinessConfigurationFingerprint.Compute(_settings, registry);
        var committedFingerprint = (await _projection.ExportSnapshotAsync(cancellationToken).ConfigureAwait(false))
            .ConfigurationFingerprint;
        if (committedFingerprint is not null &&
            !string.Equals(committedFingerprint, fingerprint, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("The local configuration differs from the committed cluster configuration.");
        }

        var version = typeof(ClusterCompatibilityProvider).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ??
            typeof(ClusterCompatibilityProvider).Assembly.GetName().Version?.ToString() ??
            "0.0.0";
        return new ClusterCompatibilityResponse
        {
            NodeId = _settings.Cluster.NodeId,
            ApplicationVersion = version,
            ConfigurationFingerprint = fingerprint,
            CommandSchemaVersion = ClusterCompatibility.CurrentCommandSchemaVersion,
            EnvelopeSchemaVersion =
                DrasiWake.Persistence.SonnetDB.Replication.BridgeStoreSnapshot.CurrentSchemaVersion
        };
    }
}

public sealed class ClusterCompatibilityProbe(
    RaftClusterSettings settings,
    IHttpMessageHandlerFactory? messageHandlerFactory = null) : IClusterCompatibilityProbe
{
    private readonly RaftClusterSettings _settings = settings ?? throw new ArgumentNullException(nameof(settings));
    private readonly IHttpMessageHandlerFactory? _messageHandlerFactory = messageHandlerFactory;

    public async Task<ClusterCompatibility> GetCompatibilityAsync(
        Uri endpoint,
        CancellationToken cancellationToken)
    {
        ValidateManagementEndpoint(endpoint);
        using var channel = GrpcChannel.ForAddress(
            endpoint,
            new GrpcChannelOptions
            {
                HttpHandler = _messageHandlerFactory?.CreateHandler(DrasiWakeHostBuilder.ManagementHttpClientName) ??
                    new HttpClientHandler()
            });
        var client = channel.CreateGrpcService<IClusterMembershipService>();
        var headers = new Metadata
        {
            { "authorization", $"Bearer {_settings.ManagementBearerToken}" }
        };

        try
        {
            var response = await client.GetCompatibility(
                new ClusterCompatibilityRequest(),
                new CallContext(new CallOptions(headers: headers, cancellationToken: cancellationToken)))
                .ConfigureAwait(false);
            return new ClusterCompatibility(
                response.NodeId,
                response.ApplicationVersion,
                response.ConfigurationFingerprint,
                response.CommandSchemaVersion,
                response.EnvelopeSchemaVersion);
        }
        catch (RpcException exception)
        {
            throw MapRemoteFailure(exception.StatusCode);
        }
        catch (HttpRequestException exception) when (
            exception.HttpRequestError == HttpRequestError.SecureConnectionError ||
            HasAuthenticationFailure(exception))
        {
            throw new ClusterMembershipException(StatusCode.FailedPrecondition, "tls-trust-failure");
        }
        catch (AuthenticationException)
        {
            throw new ClusterMembershipException(StatusCode.FailedPrecondition, "tls-trust-failure");
        }
        catch (HttpRequestException)
        {
            throw new ClusterMembershipException(StatusCode.Unavailable, "compatibility-unavailable");
        }
    }

    private static void ValidateManagementEndpoint(Uri endpoint)
    {
        if (!RaftMembershipManager.IsValidEndpoint(endpoint))
            throw new ClusterMembershipException(StatusCode.InvalidArgument, "invalid-endpoint");
    }

    internal static ClusterMembershipException MapRemoteFailure(StatusCode statusCode)
        => statusCode switch
        {
            StatusCode.Unauthenticated => new ClusterMembershipException(StatusCode.FailedPrecondition, "remote-auth-failure"),
            StatusCode.FailedPrecondition => new ClusterMembershipException(StatusCode.FailedPrecondition, "remote-incompatible"),
            StatusCode.InvalidArgument => new ClusterMembershipException(StatusCode.InvalidArgument, "invalid-endpoint"),
            StatusCode.Unavailable => new ClusterMembershipException(StatusCode.Unavailable, "compatibility-unavailable"),
            _ => new ClusterMembershipException(StatusCode.Unavailable, "compatibility-unavailable")
        };

    private static bool HasAuthenticationFailure(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is AuthenticationException)
                return true;
        }
        return false;
    }

}

public sealed class GrpcClusterMembershipForwarder(RaftClusterSettings settings) : IClusterMembershipForwarder
{
    private readonly RaftClusterSettings _settings = settings ?? throw new ArgumentNullException(nameof(settings));

    public async Task ForwardAsync(
        Uri managementEndpoint,
        ClusterMemberRequest request,
        bool add,
        CancellationToken cancellationToken)
    {
        if (!RaftMembershipManager.IsValidEndpoint(managementEndpoint))
            throw new ClusterMembershipException(StatusCode.InvalidArgument, "invalid-management-endpoint");

        using var channel = GrpcChannel.ForAddress(managementEndpoint);
        var client = channel.CreateGrpcService<IClusterMembershipService>();
        var headers = new Metadata
        {
            { "authorization", $"Bearer {_settings.ManagementBearerToken}" }
        };
        try
        {
            var callContext = new CallContext(new CallOptions(
                headers: headers,
                cancellationToken: cancellationToken));
            if (add)
                await client.Add(request, callContext).ConfigureAwait(false);
            else
                await client.Remove(request, callContext).ConfigureAwait(false);
        }
        catch (RpcException exception)
        {
            throw ClusterCompatibilityProbe.MapRemoteFailure(exception.StatusCode);
        }
        catch (HttpRequestException)
        {
            throw new ClusterMembershipException(StatusCode.Unavailable, "leader-unavailable");
        }
    }
}

public sealed record RaftMembershipMember(Uri Endpoint, bool IsLeader);

public interface IRaftMembershipRuntime
{
    Uri LocalEndpoint { get; }
    IReadOnlyList<RaftMembershipMember> Members { get; }
    bool IsLeader { get; }
    bool HasQuorum { get; }
    long CurrentTerm { get; }
    Task<bool> AddMemberAsync(Uri endpoint, CancellationToken cancellationToken);
    Task<bool> RemoveMemberAsync(Uri endpoint, CancellationToken cancellationToken);
}

public sealed class DotNextRaftMembershipRuntime(
    IRaftHttpCluster cluster,
    DrasiWake.Persistence.Raft.IRaftCommandExecutor commandExecutor) : IRaftMembershipRuntime
{
    private readonly IRaftHttpCluster _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
    private readonly DrasiWake.Persistence.Raft.IRaftCommandExecutor _commandExecutor =
        commandExecutor ?? throw new ArgumentNullException(nameof(commandExecutor));

    public Uri LocalEndpoint => _cluster.LocalMemberAddress;

    public IReadOnlyList<RaftMembershipMember> Members
        => ((IRaftCluster)_cluster).Members
            .OfType<IPeer>()
            .Select(peer => new RaftMembershipMember(
                EndpointToUri(peer.EndPoint),
                peer is IClusterMember clusterMember && clusterMember.IsLeader))
            .ToArray();

    public bool IsLeader => _commandExecutor.IsLeader;
    public bool HasQuorum => _commandExecutor.HasQuorum;
    public long CurrentTerm => _cluster.AuditTrail.Term;

    public Task<bool> AddMemberAsync(Uri endpoint, CancellationToken cancellationToken)
        => _cluster.AddMemberAsync(endpoint, cancellationToken);

    public Task<bool> RemoveMemberAsync(Uri endpoint, CancellationToken cancellationToken)
        => _cluster.RemoveMemberAsync(endpoint, cancellationToken);

    private static Uri EndpointToUri(EndPoint endpoint)
        => endpoint switch
        {
            UriEndPoint uriEndPoint when uriEndPoint.Uri.Scheme == Uri.UriSchemeHttps => uriEndPoint.Uri,
            IPEndPoint ipEndPoint => new UriBuilder(
                Uri.UriSchemeHttps,
                ipEndPoint.Address.ToString(),
                ipEndPoint.Port).Uri,
            DnsEndPoint dnsEndPoint => new UriBuilder(
                Uri.UriSchemeHttps,
                dnsEndPoint.Host,
                dnsEndPoint.Port).Uri,
            _ => throw new InvalidOperationException("DotNext returned an unsupported cluster member endpoint.")
        };
}

public sealed class RaftMembershipManager(
    RaftClusterSettings settings,
    IRaftMembershipRuntime runtime,
    IClusterCompatibilityProvider compatibilityProvider,
    IClusterCompatibilityProbe compatibilityProbe,
    IClusterMembershipForwarder forwarder) : IForwardedRaftMembershipManager
{
    private readonly RaftClusterSettings _settings = settings ?? throw new ArgumentNullException(nameof(settings));
    private readonly IRaftMembershipRuntime _runtime = runtime ?? throw new ArgumentNullException(nameof(runtime));
    private readonly IClusterCompatibilityProvider _compatibilityProvider =
        compatibilityProvider ?? throw new ArgumentNullException(nameof(compatibilityProvider));
    private readonly IClusterCompatibilityProbe _compatibilityProbe =
        compatibilityProbe ?? throw new ArgumentNullException(nameof(compatibilityProbe));
    private readonly IClusterMembershipForwarder _forwarder =
        forwarder ?? throw new ArgumentNullException(nameof(forwarder));
    private readonly SemaphoreSlim _changeGate = new(1, 1);

    public Task AddMemberAsync(Uri endpoint, CancellationToken cancellationToken)
        => RouteOrAddAsync(endpoint, expectedLeaderNodeId: null, expectedLeaderTerm: null, cancellationToken);

    public Task RemoveMemberAsync(Uri endpoint, CancellationToken cancellationToken)
        => RouteOrRemoveAsync(endpoint, expectedLeaderNodeId: null, expectedLeaderTerm: null, cancellationToken);

    public Task AddForwardedMemberAsync(
        Uri endpoint,
        string expectedLeaderNodeId,
        long expectedLeaderTerm,
        CancellationToken cancellationToken)
        => RouteOrAddAsync(endpoint, expectedLeaderNodeId, expectedLeaderTerm, cancellationToken);

    public Task RemoveForwardedMemberAsync(
        Uri endpoint,
        string expectedLeaderNodeId,
        long expectedLeaderTerm,
        CancellationToken cancellationToken)
        => RouteOrRemoveAsync(endpoint, expectedLeaderNodeId, expectedLeaderTerm, cancellationToken);

    private async Task RouteOrAddAsync(
        Uri endpoint,
        string? expectedLeaderNodeId,
        long? expectedLeaderTerm,
        CancellationToken cancellationToken)
    {
        ValidateRaftEndpoint(endpoint);
        await _changeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (expectedLeaderTerm is not null)
                EnsureForwardedLeader(expectedLeaderNodeId, expectedLeaderTerm.Value);
            else if (await RedirectIfFollowerAsync(endpoint, add: true, cancellationToken).ConfigureAwait(false))
                return;

            EnsureWritableQuorum();
            var currentMembers = GetMemberEndpoints();
            if (currentMembers.Any(member => SameEndpoint(member, endpoint)))
                throw new ClusterMembershipException(StatusCode.AlreadyExists, "duplicate-endpoint");

            var candidateManagementAddress = ManagementAddressFor(endpoint);
            var candidate = await _compatibilityProbe.GetCompatibilityAsync(
                candidateManagementAddress,
                cancellationToken).ConfigureAwait(false);
            await EnsureCompatible(candidate, cancellationToken).ConfigureAwait(false);

            var localCompatibility = await GetLocalCompatibilityAsync(cancellationToken).ConfigureAwait(false);
            if (string.Equals(candidate.NodeId, localCompatibility.NodeId, StringComparison.Ordinal))
                throw new ClusterMembershipException(StatusCode.AlreadyExists, "duplicate-node-id");

            foreach (var memberEndpoint in currentMembers)
            {
                if (SameEndpoint(memberEndpoint, _runtime.LocalEndpoint))
                    continue;
                var compatibility = await _compatibilityProbe.GetCompatibilityAsync(
                    ManagementAddressFor(memberEndpoint),
                    cancellationToken).ConfigureAwait(false);
                await EnsureCompatible(compatibility, cancellationToken).ConfigureAwait(false);
                if (string.Equals(candidate.NodeId, compatibility.NodeId, StringComparison.Ordinal))
                    throw new ClusterMembershipException(StatusCode.AlreadyExists, "duplicate-node-id");
            }

            EnsureCurrentLeader(expectedLeaderNodeId, expectedLeaderTerm);
            var added = await _runtime.AddMemberAsync(endpoint, cancellationToken).ConfigureAwait(false);
            if (!added)
                throw new ClusterMembershipException(StatusCode.Unavailable, "membership-not-committed");
        }
        catch (ClusterMembershipException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (NotLeaderException)
        {
            throw new ClusterMembershipException(StatusCode.Aborted, "stale-leader");
        }
        catch
        {
            throw new ClusterMembershipException(StatusCode.Unavailable, "membership-change-failed");
        }
        finally
        {
            _changeGate.Release();
        }
    }

    private async Task RouteOrRemoveAsync(
        Uri endpoint,
        string? expectedLeaderNodeId,
        long? expectedLeaderTerm,
        CancellationToken cancellationToken)
    {
        ValidateRaftEndpoint(endpoint);
        await _changeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (expectedLeaderTerm is not null)
                EnsureForwardedLeader(expectedLeaderNodeId, expectedLeaderTerm.Value);
            else if (await RedirectIfFollowerAsync(endpoint, add: false, cancellationToken).ConfigureAwait(false))
                return;

            EnsureWritableQuorum();
            var currentMembers = GetMemberEndpoints();
            if (!currentMembers.Any(member => SameEndpoint(member, endpoint)))
                throw new ClusterMembershipException(StatusCode.NotFound, "member-not-found");
            if (currentMembers.Count <= 1)
                throw new ClusterMembershipException(StatusCode.FailedPrecondition, "last-member");

            EnsureCurrentLeader(expectedLeaderNodeId, expectedLeaderTerm);
            var removed = await _runtime.RemoveMemberAsync(endpoint, cancellationToken).ConfigureAwait(false);
            if (!removed)
                throw new ClusterMembershipException(StatusCode.Unavailable, "membership-not-committed");
        }
        catch (ClusterMembershipException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (NotLeaderException)
        {
            throw new ClusterMembershipException(StatusCode.Aborted, "stale-leader");
        }
        catch
        {
            throw new ClusterMembershipException(StatusCode.Unavailable, "membership-change-failed");
        }
        finally
        {
            _changeGate.Release();
        }
    }

    private async Task<bool> RedirectIfFollowerAsync(
        Uri endpoint,
        bool add,
        CancellationToken cancellationToken)
    {
        EnsureWritableQuorum();
        if (_runtime.IsLeader)
            return false;

        var leaderEndpoint = _runtime.Members.FirstOrDefault(member => member.IsLeader)?.Endpoint;
        if (leaderEndpoint is null)
            throw new ClusterMembershipException(StatusCode.Unavailable, "leader-unavailable");

        var leaderManagementAddress = ManagementAddressFor(leaderEndpoint);
        var leaderCompatibility = await _compatibilityProbe.GetCompatibilityAsync(
            leaderManagementAddress,
            cancellationToken).ConfigureAwait(false);
        var request = new ClusterMemberRequest
        {
            Endpoint = endpoint.AbsoluteUri,
            ExpectedLeaderNodeId = leaderCompatibility.NodeId,
            ExpectedLeaderTerm = _runtime.CurrentTerm
        };
        await _forwarder.ForwardAsync(
            leaderManagementAddress,
            request,
            add,
            cancellationToken).ConfigureAwait(false);
        return true;
    }

    private void EnsureForwardedLeader(string? expectedLeaderNodeId, long expectedLeaderTerm)
    {
        if (!_runtime.IsLeader ||
            !_runtime.HasQuorum ||
            _runtime.CurrentTerm != expectedLeaderTerm ||
            !string.Equals(expectedLeaderNodeId, _settings.NodeId, StringComparison.Ordinal))
        {
            throw new ClusterMembershipException(StatusCode.Aborted, "stale-leader");
        }
    }

    private void EnsureCurrentLeader(string? expectedLeaderNodeId, long? expectedLeaderTerm)
    {
        if (expectedLeaderTerm is not null)
        {
            EnsureForwardedLeader(expectedLeaderNodeId, expectedLeaderTerm.Value);
            return;
        }

        if (!_runtime.HasQuorum)
            throw new ClusterMembershipException(StatusCode.Unavailable, "no-quorum");
        if (!_runtime.IsLeader)
            throw new ClusterMembershipException(StatusCode.Aborted, "stale-leader");
    }

    private void EnsureWritableQuorum()
    {
        if (!_runtime.HasQuorum)
            throw new ClusterMembershipException(StatusCode.Unavailable, "no-quorum");
        if (!_runtime.IsLeader)
        {
            // A follower may route the operation, but must not invoke DotNext membership methods.
            return;
        }
    }

    private IReadOnlyList<Uri> GetMemberEndpoints()
        => _runtime.Members.Select(member => Normalize(member.Endpoint)).Distinct().ToArray();

    private async Task<ClusterCompatibility> GetLocalCompatibilityAsync(CancellationToken cancellationToken)
    {
        var result = await _compatibilityProvider.GetLocalCompatibilityAsync(cancellationToken).ConfigureAwait(false);
        return new ClusterCompatibility(
            result.NodeId,
            result.ApplicationVersion,
            result.ConfigurationFingerprint,
            result.CommandSchemaVersion,
            result.EnvelopeSchemaVersion);
    }

    private async Task EnsureCompatible(ClusterCompatibility candidate, CancellationToken cancellationToken)
    {
        var local = await GetLocalCompatibilityAsync(cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(candidate.NodeId) ||
            string.IsNullOrWhiteSpace(candidate.ConfigurationFingerprint) ||
            !string.Equals(candidate.ApplicationVersion, local.ApplicationVersion, StringComparison.Ordinal) ||
            candidate.CommandSchemaVersion != local.CommandSchemaVersion ||
            candidate.EnvelopeSchemaVersion != local.EnvelopeSchemaVersion ||
            !string.Equals(candidate.ConfigurationFingerprint, local.ConfigurationFingerprint, StringComparison.Ordinal))
        {
            throw new ClusterMembershipException(StatusCode.FailedPrecondition, "incompatible-member");
        }
    }

    private Uri ManagementAddressFor(Uri raftEndpoint)
        => new UriBuilder(Uri.UriSchemeHttps, raftEndpoint.DnsSafeHost, _settings.ManagementAddress!.Port).Uri;

    private static void ValidateRaftEndpoint(Uri endpoint)
    {
        if (!IsValidEndpoint(endpoint))
            throw new ClusterMembershipException(StatusCode.InvalidArgument, "invalid-endpoint");
    }

    internal static bool IsValidEndpoint(Uri? endpoint)
        => endpoint is not null &&
            endpoint.IsAbsoluteUri &&
            endpoint.Scheme == Uri.UriSchemeHttps &&
            !string.IsNullOrWhiteSpace(endpoint.Host) &&
            endpoint.Port is > 0 and <= 65535 &&
            string.IsNullOrWhiteSpace(endpoint.UserInfo) &&
            string.IsNullOrWhiteSpace(endpoint.Query) &&
            string.IsNullOrWhiteSpace(endpoint.Fragment) &&
            endpoint.AbsolutePath is "" or "/";

    private static Uri Normalize(Uri endpoint)
        => new UriBuilder(endpoint)
        {
            Host = endpoint.IdnHost.ToLowerInvariant(),
            Path = "/",
            Query = string.Empty,
            Fragment = string.Empty,
            UserName = string.Empty,
            Password = string.Empty
        }.Uri;

    private static bool SameEndpoint(Uri left, Uri right)
        => Normalize(left) == Normalize(right);
}
