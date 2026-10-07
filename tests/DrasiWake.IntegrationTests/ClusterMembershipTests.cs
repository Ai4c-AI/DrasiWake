using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using DrasiWake.Host;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using ProtoBuf.Grpc;
using ProtoBuf.Grpc.Client;
using ProtoBuf.Grpc.Configuration;
using ProtoBuf.Grpc.Server;

namespace DrasiWake.IntegrationTests;

public sealed class ClusterMembershipTests
{
    private const string BearerSecret = "management-secret-that-must-never-appear-in-errors";
    private static readonly Uri Candidate = new("https://candidate.test:5101/");

    [Theory]
    [InlineData("GetCompatibility", null)]
    [InlineData("GetCompatibility", "not-a-bearer-token")]
    [InlineData("GetCompatibility", "Bearer wrong-secret")]
    [InlineData("Add", null)]
    [InlineData("Add", "not-a-bearer-token")]
    [InlineData("Add", "Bearer wrong-secret")]
    [InlineData("Remove", null)]
    [InlineData("Remove", "not-a-bearer-token")]
    [InlineData("Remove", "Bearer wrong-secret")]
    public async Task Every_management_rpc_rejects_missing_malformed_or_wrong_bearer_credentials(
        string operation,
        string? authorization)
    {
        await using var server = await TestServer.StartAsync();
        var client = server.CreateClient(authorization);

        var exception = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            switch (operation)
            {
                case "GetCompatibility":
                    await client.GetCompatibility(new ClusterCompatibilityRequest());
                    break;
                case "Add":
                    await client.Add(new ClusterMemberRequest { Endpoint = Candidate.AbsoluteUri });
                    break;
                case "Remove":
                    await client.Remove(new ClusterMemberRequest { Endpoint = Candidate.AbsoluteUri });
                    break;
            }
        });

        Assert.Equal(StatusCode.Unauthenticated, exception.StatusCode);
        Assert.DoesNotContain(BearerSecret, exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("wrong-secret", exception.Message, StringComparison.Ordinal);
        Assert.Equal(0, server.Membership.AddCalls);
        Assert.Equal(0, server.Membership.RemoveCalls);
        Assert.Equal(0, server.CompatibilityProbe.Calls);
    }

    [Fact]
    public async Task Authorized_compatibility_add_and_remove_requests_are_dispatched_once()
    {
        await using var server = await TestServer.StartAsync();
        var client = server.CreateClient($"Bearer {BearerSecret}");

        var compatibility = await client.GetCompatibility(new ClusterCompatibilityRequest());
        await client.Add(new ClusterMemberRequest { Endpoint = Candidate.AbsoluteUri });
        await client.Remove(new ClusterMemberRequest { Endpoint = Candidate.AbsoluteUri });

        Assert.Equal("node-test", compatibility.NodeId);
        Assert.Equal(1, server.CompatibilityProbe.Calls);
        Assert.Equal(1, server.Membership.AddCalls);
        Assert.Equal(1, server.Membership.RemoveCalls);
        Assert.Equal(Candidate, server.Membership.LastAddedEndpoint);
        Assert.Equal(Candidate, server.Membership.LastRemovedEndpoint);
    }

    [Fact]
    public async Task Authorized_follower_membership_requests_are_forwarded_exactly_once()
    {
        var forwardingManager = new CountingMembershipManager();
        await using var server = await TestServer.StartAsync(forwardingManager);
        var client = server.CreateClient($"Bearer {BearerSecret}");

        await client.Add(new ClusterMemberRequest { Endpoint = Candidate.AbsoluteUri });

        Assert.Equal(1, forwardingManager.AddCalls);
        Assert.Equal(Candidate, forwardingManager.LastAddedEndpoint);
    }

    [Fact]
    public async Task Management_service_errors_are_fixed_and_do_not_echo_candidate_or_exception_text()
    {
        const string internalFailure = "sensitive internal failure text";
        var membership = new CountingMembershipManager
        {
            AddFailure = new InvalidOperationException(internalFailure)
        };
        await using var server = await TestServer.StartAsync(membership);
        var client = server.CreateClient($"Bearer {BearerSecret}");

        var exception = await Assert.ThrowsAsync<RpcException>(() =>
            client.Add(new ClusterMemberRequest { Endpoint = Candidate.AbsoluteUri }));

        Assert.Equal(StatusCode.Internal, exception.StatusCode);
        Assert.DoesNotContain(internalFailure, exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(Candidate.AbsoluteUri, exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(BearerSecret, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Membership_changes_without_quorum_are_rejected_before_compatibility_probe_or_dotnext()
    {
        var (manager, runtime, probe, _) = CreateMembershipManager(hasQuorum: false);

        var exception = await Assert.ThrowsAsync<ClusterMembershipException>(() =>
            manager.AddMemberAsync(Candidate, CancellationToken.None));

        Assert.Equal(StatusCode.Unavailable, exception.StatusCode);
        Assert.Equal(0, probe.Calls);
        Assert.Equal(0, runtime.AddCalls);
    }

    [Theory]
    [InlineData("http://candidate.test:5101/")]
    [InlineData("https://candidate.test:5101/path")]
    [InlineData("https://user@candidate.test:5101/")]
    [InlineData("https://candidate.test:5101/?unsafe=1")]
    public async Task Add_rejects_non_https_or_invalid_candidate_endpoints(string endpointText)
    {
        var (manager, runtime, probe, _) = CreateMembershipManager();
        var endpoint = new Uri(endpointText);

        var exception = await Assert.ThrowsAsync<ClusterMembershipException>(() =>
            manager.AddMemberAsync(endpoint, CancellationToken.None));

        Assert.Equal(StatusCode.InvalidArgument, exception.StatusCode);
        Assert.Equal(0, probe.Calls);
        Assert.Equal(0, runtime.AddCalls);
    }

    [Fact]
    public async Task Candidate_with_a_duplicate_node_id_is_rejected_before_dotnext_add()
    {
        var existing = new Uri("https://existing.test:5101/");
        var (manager, runtime, probe, _) = CreateMembershipManager(
            members:
            [
                new RaftMembershipMember(new Uri("https://node-test.test:5101/"), true),
                new RaftMembershipMember(existing, false)
            ],
            candidateCompatibility: uri => new ClusterCompatibility(
                uri.Host == "candidate.test" ? "node-duplicate" : "node-duplicate",
                "1.0.0",
                "0123456789abcdef"));

        var exception = await Assert.ThrowsAsync<ClusterMembershipException>(() =>
            manager.AddMemberAsync(Candidate, CancellationToken.None));

        Assert.Equal(StatusCode.AlreadyExists, exception.StatusCode);
        Assert.Equal(2, probe.Calls);
        Assert.Equal(0, runtime.AddCalls);
    }

    [Theory]
    [InlineData("2.0.0", "0123456789abcdef", 1, 1)]
    [InlineData("1.0.0", "different-fingerprint", 1, 1)]
    [InlineData("1.0.0", "0123456789abcdef", 2, 1)]
    [InlineData("1.0.0", "0123456789abcdef", 1, 2)]
    public async Task Incompatible_application_command_or_configuration_prevents_add(
        string version,
        string fingerprint,
        int commandSchemaVersion,
        int envelopeSchemaVersion)
    {
        var (manager, runtime, _, _) = CreateMembershipManager(
            candidateCompatibility: _ => new ClusterCompatibility(
                "candidate-node",
                version,
                fingerprint,
                commandSchemaVersion,
                envelopeSchemaVersion));

        var exception = await Assert.ThrowsAsync<ClusterMembershipException>(() =>
            manager.AddMemberAsync(Candidate, CancellationToken.None));

        Assert.Equal(StatusCode.FailedPrecondition, exception.StatusCode);
        Assert.Equal(0, runtime.AddCalls);
    }

    [Fact]
    public async Task Candidate_tls_certificate_must_be_trusted_before_compatibility_is_accepted()
    {
        await using var server = await TestServer.StartAsync();
        var settings = TestServer.CreateClusterSettings(server.ManagementEndpoint.Port);
        var runtime = new FakeMembershipRuntime(
            settings.ListenAddress,
            isLeader: true,
            hasQuorum: true,
            [new RaftMembershipMember(settings.ListenAddress, true)]);
        var compatibility = new CountingCompatibilityProbe(new ClusterCompatibility(
            settings.NodeId,
            "1.0.0",
            "0123456789abcdef"));
        var manager = new RaftMembershipManager(
            settings,
            runtime,
            compatibility,
            new ClusterCompatibilityProbe(settings),
            new FakeMembershipForwarder());

        var exception = await Assert.ThrowsAsync<ClusterMembershipException>(() =>
            manager.AddMemberAsync(new Uri("https://127.0.0.1:5101/"), CancellationToken.None));

        Assert.Equal(StatusCode.Unavailable, exception.StatusCode);
        Assert.Equal(0, runtime.AddCalls);
    }

    [Fact]
    public async Task Adding_an_existing_endpoint_is_rejected_before_probe_or_dotnext()
    {
        var (manager, runtime, probe, _) = CreateMembershipManager(
            members: [new RaftMembershipMember(Candidate, false)]);

        var exception = await Assert.ThrowsAsync<ClusterMembershipException>(() =>
            manager.AddMemberAsync(Candidate, CancellationToken.None));

        Assert.Equal(StatusCode.AlreadyExists, exception.StatusCode);
        Assert.Equal(0, probe.Calls);
        Assert.Equal(0, runtime.AddCalls);
    }

    [Fact]
    public async Task Removing_the_last_cluster_member_is_rejected_without_dotnext_call()
    {
        var local = new Uri("https://node-test.test:5101/");
        var (manager, runtime, _, _) = CreateMembershipManager(
            members: [new RaftMembershipMember(local, true)],
            localEndpoint: local);

        var exception = await Assert.ThrowsAsync<ClusterMembershipException>(() =>
            manager.RemoveMemberAsync(local, CancellationToken.None));

        Assert.Equal(StatusCode.FailedPrecondition, exception.StatusCode);
        Assert.Equal(0, runtime.RemoveCalls);
    }

    [Fact]
    public async Task Leader_executes_a_compatible_add_once()
    {
        var (manager, runtime, _, _) = CreateMembershipManager();

        await manager.AddMemberAsync(Candidate, CancellationToken.None);

        Assert.Equal(1, runtime.AddCalls);
        Assert.Equal(Candidate, runtime.LastAddedEndpoint);
    }

    [Fact]
    public async Task Follower_forwards_add_once_and_never_calls_dotnext_locally()
    {
        var leader = new Uri("https://leader.test:5101/");
        var (manager, runtime, _, forwarder) = CreateMembershipManager(
            isLeader: false,
            members:
            [
                new RaftMembershipMember(new Uri("https://follower.test:5101/"), false),
                new RaftMembershipMember(leader, true)
            ],
            candidateCompatibility: uri => new ClusterCompatibility(
                uri.Host == "leader.test" ? "node-leader" : "node-candidate",
                "1.0.0",
                "0123456789abcdef"));

        await manager.AddMemberAsync(Candidate, CancellationToken.None);

        Assert.Equal(1, forwarder.Calls);
        Assert.Equal(Candidate.AbsoluteUri, forwarder.LastRequest?.Endpoint);
        Assert.Equal("node-leader", forwarder.LastRequest?.ExpectedLeaderNodeId);
        Assert.Equal(0, runtime.AddCalls);
    }

    [Fact]
    public async Task Forwarded_change_with_stale_leader_or_term_is_rejected()
    {
        var (manager, runtime, _, _) = CreateMembershipManager();

        var exception = await Assert.ThrowsAsync<ClusterMembershipException>(() =>
            manager.AddForwardedMemberAsync(
                Candidate,
                "old-leader",
                runtime.CurrentTerm - 1,
                CancellationToken.None));

        Assert.Equal(StatusCode.Aborted, exception.StatusCode);
        Assert.Equal(0, runtime.AddCalls);
    }

    [Fact]
    public async Task Forwarded_change_is_rechecked_before_commit_if_the_leader_term_changes_during_compatibility_probe()
    {
        var (manager, runtime, _, _) = CreateMembershipManager();
        var probe = new FakeCompatibilityProbe(_ =>
        {
            runtime.CurrentTerm++;
            return new ClusterCompatibility("candidate-node", "1.0.0", "0123456789abcdef");
        });
        manager = new RaftMembershipManager(
            TestServer.CreateClusterSettings(),
            runtime,
            new CountingCompatibilityProbe(new ClusterCompatibility("node-test", "1.0.0", "0123456789abcdef")),
            probe,
            new FakeMembershipForwarder());

        var exception = await Assert.ThrowsAsync<ClusterMembershipException>(() =>
            manager.AddForwardedMemberAsync(Candidate, "node-test", runtime.CurrentTerm, CancellationToken.None));

        Assert.Equal(StatusCode.Aborted, exception.StatusCode);
        Assert.Equal(0, runtime.AddCalls);
    }

    private sealed class TestServer : IAsyncDisposable
    {
        private readonly WebApplication app;
        private readonly X509Certificate2 certificate;
        private readonly int port;

        private TestServer(
            WebApplication app,
            X509Certificate2 certificate,
            int port,
            CountingMembershipManager membership,
            CountingCompatibilityProbe compatibilityProbe)
        {
            this.app = app;
            this.certificate = certificate;
            this.port = port;
            Membership = membership;
            CompatibilityProbe = compatibilityProbe;
        }

        public CountingMembershipManager Membership { get; }
        public CountingCompatibilityProbe CompatibilityProbe { get; }
        public Uri ManagementEndpoint => new($"https://127.0.0.1:{port}/");

        public static async Task<TestServer> StartAsync(CountingMembershipManager? membership = null)
        {
            var actualMembership = membership ?? new CountingMembershipManager();
            var compatibilityProbe = new CountingCompatibilityProbe(new ClusterCompatibility(
                "node-test",
                "1.0.0",
                "0123456789abcdef"));
            var certificate = CreateServerCertificate();
            var port = GetFreePort();
            var builder = WebApplication.CreateBuilder();
            builder.WebHost.ConfigureKestrel(options =>
                options.Listen(IPAddress.Loopback, port, endpoint =>
                {
                    endpoint.Protocols = HttpProtocols.Http2;
                    endpoint.UseHttps(certificate);
                }));
            builder.Services.AddSingleton(CreateClusterSettings());
            builder.Services.AddSingleton<IClusterCompatibilityProvider>(compatibilityProbe);
            builder.Services.AddSingleton<IRaftMembershipManager>(actualMembership);
            builder.Services.AddSingleton<TimeProvider>(TimeProvider.System);
            builder.Services.AddCodeFirstGrpc();

            var app = builder.Build();
            app.MapGrpcService<ClusterMembershipGrpcService>();
            await app.StartAsync();
            return new TestServer(app, certificate, port, actualMembership, compatibilityProbe);
        }

        public IClusterMembershipService CreateClient(string? authorization)
        {
            var handler = new SocketsHttpHandler
            {
                SslOptions =
                {
                    RemoteCertificateValidationCallback = (_, _, _, _) => true
                }
            };
            var channel = GrpcChannel.ForAddress(
                new Uri($"https://127.0.0.1:{port}/"),
                new GrpcChannelOptions { HttpHandler = handler });
            var client = channel.CreateGrpcService<IClusterMembershipService>();
            return authorization is null
                ? client
                : new AuthenticatedClient(client, authorization);
        }

        public async ValueTask DisposeAsync()
        {
            await app.StopAsync();
            await app.DisposeAsync();
            certificate.Dispose();
        }

        internal static RaftClusterSettings CreateClusterSettings(int? managementPort = null)
        {
            var port = GetFreePort();
            var values = new Dictionary<string, string?>
            {
                ["DrasiWake:Cluster:Mode"] = "Cluster",
                ["DrasiWake:Cluster:NodeId"] = "node-test",
                ["DrasiWake:Cluster:ListenAddress"] = $"https://127.0.0.1:{port}/",
                ["DrasiWake:Cluster:InitialMembers:0"] = $"https://127.0.0.1:{port}/",
                ["DrasiWake:Cluster:RaftDataPath"] = Path.Combine(Path.GetTempPath(), $"raft-{Guid.NewGuid():N}"),
                ["DrasiWake:Cluster:Certificate:Path"] = "test-only.pfx",
                ["DrasiWake:Cluster:Certificate:Password"] = "test-only",
                ["DrasiWake:Cluster:Management:Address"] = $"https://127.0.0.1:{managementPort ?? GetFreePort()}/",
                ["DrasiWake:Cluster:Management:BearerToken"] = BearerSecret,
                ["DrasiWake:Cluster:SnapshotFrequency"] = "1000"
            };
            return RaftClusterSettings.FromConfiguration(
                new ConfigurationBuilder().AddInMemoryCollection(values).Build(),
                Path.Combine(Path.GetTempPath(), $"db-{Guid.NewGuid():N}"));
        }

        private static X509Certificate2 CreateServerCertificate()
        {
            using var key = RSA.Create(2048);
            var request = new CertificateRequest(
                "CN=localhost",
                key,
                HashAlgorithmName.SHA256,
                RSASignaturePadding.Pkcs1);
            request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
            request.CertificateExtensions.Add(new X509KeyUsageExtension(
                X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment,
                false));
            var san = new SubjectAlternativeNameBuilder();
            san.AddIpAddress(IPAddress.Loopback);
            request.CertificateExtensions.Add(san.Build());
            using var created = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddHours(1));
            return X509CertificateLoader.LoadPkcs12(created.Export(X509ContentType.Pfx), password: null);
        }

        private static int GetFreePort()
        {
            using var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
    }

    private sealed class AuthenticatedClient(IClusterMembershipService inner, string authorization)
        : IClusterMembershipService
    {
        private CallContext Context() => new(new CallOptions(headers: new Metadata
        {
            { "authorization", authorization }
        }));

        public Task<ClusterCompatibilityResponse> GetCompatibility(
            ClusterCompatibilityRequest request,
            CallContext context = default) => inner.GetCompatibility(request, Context());

        public Task<ClusterManagementResponse> Add(
            ClusterMemberRequest request,
            CallContext context = default) => inner.Add(request, Context());

        public Task<ClusterManagementResponse> Remove(
            ClusterMemberRequest request,
            CallContext context = default) => inner.Remove(request, Context());
    }

    private sealed class CountingMembershipManager : IRaftMembershipManager
    {
        public int AddCalls { get; private set; }
        public int RemoveCalls { get; private set; }
        public Uri? LastAddedEndpoint { get; private set; }
        public Uri? LastRemovedEndpoint { get; private set; }
        public Exception? AddFailure { get; init; }

        public Task AddMemberAsync(Uri endpoint, CancellationToken cancellationToken)
        {
            AddCalls++;
            LastAddedEndpoint = endpoint;
            return AddFailure is null ? Task.CompletedTask : Task.FromException(AddFailure);
        }

        public Task RemoveMemberAsync(Uri endpoint, CancellationToken cancellationToken)
        {
            RemoveCalls++;
            LastRemovedEndpoint = endpoint;
            return Task.CompletedTask;
        }
    }

    private sealed class CountingCompatibilityProbe(ClusterCompatibility compatibility)
        : IClusterCompatibilityProvider
    {
        public int Calls { get; private set; }

        public Task<ClusterCompatibilityResponse> GetLocalCompatibilityAsync(CancellationToken cancellationToken)
        {
            Calls++;
            return Task.FromResult(new ClusterCompatibilityResponse
            {
                NodeId = compatibility.NodeId,
                ApplicationVersion = compatibility.ApplicationVersion,
                ConfigurationFingerprint = compatibility.ConfigurationFingerprint,
                CommandSchemaVersion = compatibility.CommandSchemaVersion,
                EnvelopeSchemaVersion = compatibility.EnvelopeSchemaVersion
            });
        }
    }

    private static (RaftMembershipManager Manager, FakeMembershipRuntime Runtime, FakeCompatibilityProbe Probe,
        FakeMembershipForwarder Forwarder) CreateMembershipManager(
        bool isLeader = true,
        bool hasQuorum = true,
        IReadOnlyList<RaftMembershipMember>? members = null,
        Func<Uri, ClusterCompatibility>? candidateCompatibility = null,
        Uri? localEndpoint = null)
    {
        var settings = TestServer.CreateClusterSettings();
        var local = localEndpoint ?? members?.FirstOrDefault()?.Endpoint ?? settings.ListenAddress;
        var runtime = new FakeMembershipRuntime(
            local,
            isLeader,
            hasQuorum,
            members ?? [new RaftMembershipMember(local, isLeader)]);
        var compatibility = new CountingCompatibilityProbe(new ClusterCompatibility(
            settings.NodeId,
            "1.0.0",
            "0123456789abcdef"));
        var probe = new FakeCompatibilityProbe(candidateCompatibility ??
            (_ => new ClusterCompatibility("node-candidate", "1.0.0", "0123456789abcdef")));
        var forwarder = new FakeMembershipForwarder();
        return (
            new RaftMembershipManager(settings, runtime, compatibility, probe, forwarder),
            runtime,
            probe,
            forwarder);
    }

    private sealed class FakeMembershipRuntime(
        Uri localEndpoint,
        bool isLeader,
        bool hasQuorum,
        IReadOnlyList<RaftMembershipMember> members) : IRaftMembershipRuntime
    {
        public Uri LocalEndpoint { get; } = localEndpoint;
        public IReadOnlyList<RaftMembershipMember> Members { get; } = members;
        public bool IsLeader { get; set; } = isLeader;
        public bool HasQuorum { get; } = hasQuorum;
        public long CurrentTerm { get; set; } = 7;
        public int AddCalls { get; private set; }
        public int RemoveCalls { get; private set; }
        public Uri? LastAddedEndpoint { get; private set; }

        public Task<bool> AddMemberAsync(Uri endpoint, CancellationToken cancellationToken)
        {
            AddCalls++;
            LastAddedEndpoint = endpoint;
            return Task.FromResult(true);
        }

        public Task<bool> RemoveMemberAsync(Uri endpoint, CancellationToken cancellationToken)
        {
            RemoveCalls++;
            return Task.FromResult(true);
        }
    }

    private sealed class FakeCompatibilityProbe(Func<Uri, ClusterCompatibility> compatibility)
        : IClusterCompatibilityProbe
    {
        public int Calls { get; private set; }

        public Task<ClusterCompatibility> GetCompatibilityAsync(Uri endpoint, CancellationToken cancellationToken)
        {
            Calls++;
            return Task.FromResult(compatibility(endpoint));
        }
    }

    private sealed class FakeMembershipForwarder : IClusterMembershipForwarder
    {
        public int Calls { get; private set; }
        public ClusterMemberRequest? LastRequest { get; private set; }

        public Task ForwardAsync(
            Uri managementEndpoint,
            ClusterMemberRequest request,
            bool add,
            CancellationToken cancellationToken)
        {
            Calls++;
            LastRequest = request;
            return Task.CompletedTask;
        }
    }
}
