using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using DotNext.Net.Cluster.Consensus.Raft;
using DotNext.Net.Cluster.Consensus.Raft.Http;
using DotNext.Net.Cluster.Consensus.Raft.StateMachine;
using DotNext.Net.Http;
using DrasiWake.Adapters.DrasiServer;
using DrasiWake.Adapters.OpenClaw;
using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Contracts;
using DrasiWake.Core.Pipeline;
using DrasiWake.Persistence.Raft;
using DrasiWake.Persistence.SonnetDB;
using DrasiWake.Persistence.SonnetDB.Replication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using ProtoBuf.Grpc.Server;

namespace DrasiWake.Host;

public static class DrasiWakeHostBuilder
{
    internal const string RaftHttpClientName = "DrasiWake.Raft";
    internal const string ManagementHttpClientName = "DrasiWake.Management";

    public static IHost CreateHost(string[] args)
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateDefaultBuilder(args);
        ConfigureHost(builder, null);
        return builder.Build();
    }

    public static IHost CreateHost(IConfiguration configuration)
        => CreateHost(configuration, null);

    internal static IHost CreateHost(
        IConfiguration configuration,
        Action<IServiceCollection>? configureAdditionalServices)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var builder = Microsoft.Extensions.Hosting.Host.CreateDefaultBuilder();
        builder.ConfigureAppConfiguration((_, config) => config.AddConfiguration(configuration));
        ConfigureHost(builder, configureAdditionalServices);
        return builder.Build();
    }

    private static void ConfigureHost(
        IHostBuilder builder,
        Action<IServiceCollection>? configureAdditionalServices)
    {
        builder.ConfigureAppConfiguration((_, configBuilder) =>
        {
            var configuration = configBuilder.Build();
            var hostSettings = DrasiWakeHostSettings.FromConfiguration(configuration);
            var seedMembers = hostSettings.Cluster.InitialMembers
                .Where(address => !Uri.Equals(address, hostSettings.Cluster.ListenAddress))
                .Select((address, index) => new KeyValuePair<string, string?>(
                    $"members:{index}",
                    address.AbsoluteUri));
            configBuilder.AddInMemoryCollection(seedMembers);
        });

        builder.ConfigureServices((context, services) =>
            RegisterStartupServices(services, DrasiWakeHostSettings.FromConfiguration(context.Configuration)));
        if (configureAdditionalServices is not null)
            builder.ConfigureServices((_, services) => configureAdditionalServices(services));

        builder.ConfigureWebHostDefaults(webBuilder =>
        {
            webBuilder.ConfigureKestrel((context, options) =>
                ConfigureRaftListener(options, DrasiWakeHostSettings.FromConfiguration(context.Configuration)));
            webBuilder.Configure(app =>
            {
                var cluster = app.ApplicationServices.GetRequiredService<RaftClusterSettings>();
                if (cluster.Mode == RaftClusterMode.Cluster)
                {
                    app.MapWhen(
                        context => context.Connection.LocalPort == cluster.ManagementAddress!.Port,
                        management =>
                        {
                            management.UseRouting();
                            management.UseEndpoints(endpoints =>
                                endpoints.MapGrpcService<ClusterMembershipGrpcService>());
                        });
                }

                app.UseConsensusProtocolHandler();
            });
        });

        builder.JoinCluster((memberConfiguration, configuration, _) =>
        {
            var settings = DrasiWakeHostSettings.FromConfiguration(configuration).Cluster;
            memberConfiguration.PublicEndPoint = settings.ListenAddress;
            memberConfiguration.ProtocolVersion = HttpProtocolVersion.Http2;
            memberConfiguration.ProtocolVersionPolicy = HttpVersionPolicy.RequestVersionExact;
            if (settings.Mode == RaftClusterMode.Cluster)
                memberConfiguration.ClientHandlerName = RaftHttpClientName;
            memberConfiguration.ColdStart = IsBootstrapMember(settings);
        });
    }

    private static void ConfigureRaftListener(
        KestrelServerOptions options,
        DrasiWakeHostSettings hostSettings)
    {
        var cluster = hostSettings.Cluster;
        var certificate = cluster.Mode == RaftClusterMode.Cluster
            ? options.ApplicationServices.GetRequiredService<X509Certificate2>()
            : null;
        foreach (var address in ResolveAddresses(cluster.ListenAddress))
        {
            options.Listen(address, cluster.ListenAddress.Port, endpoint =>
            {
                endpoint.Protocols = HttpProtocols.Http2;
                if (certificate is not null)
                    endpoint.UseHttps(certificate);
            });
        }

        if (cluster.Mode == RaftClusterMode.Cluster)
        {
            foreach (var address in ResolveAddresses(cluster.ManagementAddress!))
            {
                options.Listen(address, cluster.ManagementAddress!.Port, endpoint =>
                {
                    endpoint.Protocols = HttpProtocols.Http2;
                    endpoint.UseHttps(certificate!);
                });
            }
        }
    }

    private static IReadOnlyList<IPAddress> ResolveAddresses(Uri address)
    {
        if (IPAddress.TryParse(address.Host, out var parsedAddress))
            return [parsedAddress];

        IPAddress[] resolvedAddresses;
        try
        {
            resolvedAddresses = Dns.GetHostAddresses(address.DnsSafeHost);
        }
        catch (System.Net.Sockets.SocketException exception)
        {
            throw new InvalidOperationException("A configured Raft listener address could not be resolved.", exception);
        }
        if (resolvedAddresses.Length == 0)
            throw new InvalidOperationException("A configured Raft listener address could not be resolved.");
        return resolvedAddresses;
    }

    private static bool IsBootstrapMember(RaftClusterSettings settings)
    {
        if (settings.Mode == RaftClusterMode.SingleNode)
            return true;

        var bootstrapAddress = settings.InitialMembers
            .Select(address => address.AbsoluteUri)
            .OrderBy(address => address, StringComparer.OrdinalIgnoreCase)
            .First();
        return string.Equals(
            settings.ListenAddress.AbsoluteUri,
            bootstrapAddress,
            StringComparison.OrdinalIgnoreCase);
    }

    private static void RegisterStartupServices(IServiceCollection services, DrasiWakeHostSettings settings)
    {
        services.AddSingleton(settings);
        services.AddSingleton(settings.Cluster);
        services.AddSingleton<TimeProvider>(TimeProvider.System);
        services.AddSingleton<DrasiServerOptions>(settings.Drasi);
        services.AddSingleton<OpenClawOptions>(settings.OpenClaw);
        services.AddSingleton<ContractRegistryManager>();
        services.AddSingleton<ContractRegistryLoader>();
        services.AddDbContextFactory<BridgeDbContext>(options =>
            options.UseSonnetDB($"Data Source={settings.DatabasePath}"));
        services.AddSingleton<SonnetBridgeStore>();
        services.AddSingleton<IRaftBridgeProjection>(provider =>
            provider.GetRequiredService<SonnetBridgeStore>());
        services.AddSingleton<IRaftCommandExecutor, DotNextRaftCommandExecutor>();
        services.AddSingleton<IBridgeStore, RaftBridgeStore>();
        if (settings.Cluster.Mode == RaftClusterMode.Cluster)
        {
            services.AddSingleton<X509Certificate2>(_ => settings.Cluster.LoadServerCertificate());
            services.AddCodeFirstGrpc();
            services.AddSingleton<IClusterCompatibilityProvider, ClusterCompatibilityProvider>();
            services.AddSingleton<IClusterCompatibilityProbe, ClusterCompatibilityProbe>();
            services.AddSingleton<IClusterMembershipForwarder, GrpcClusterMembershipForwarder>();
            services.AddSingleton<IRaftMembershipRuntime, DotNextRaftMembershipRuntime>();
            services.AddSingleton<IRaftMembershipManager, RaftMembershipManager>();
        }

        services.UsePersistentConfigurationStorage(
            Path.Combine(settings.Cluster.RaftDataPath, "cluster-configuration"));
        services.AddSingleton<IHostedService, RaftInitialConfigurationSeeder>();
        services.UseStateMachine<RaftBridgeStateMachine>(new WriteAheadLog.Options
        {
            Location = Path.Combine(settings.Cluster.RaftDataPath, "log"),
            FlushInterval = Timeout.InfiniteTimeSpan
        });
        services.Replace(ServiceDescriptor.Singleton<RaftBridgeStateMachine>(provider =>
        {
            provider.GetRequiredService<HostStartupValidator>()
                .StartAsync(CancellationToken.None).GetAwaiter().GetResult();
            var stateMachine = ActivatorUtilities.CreateInstance<RaftBridgeStateMachine>(
                provider,
                new DirectoryInfo(Path.Combine(settings.Cluster.RaftDataPath, "snapshots")),
                settings.Cluster.SnapshotFrequency);
            stateMachine.RestoreAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult();
            return stateMachine;
        }));

        services.AddSingleton<IRaftLeaderStartupPreparation, RaftLeaderStartupPreparation>();
        services.AddSingleton<IRaftLeadership, DotNextRaftLeadership>();
        services.AddHttpClient<DrasiServerClient>();
        services.AddHttpClient(RaftHttpClientName);
        services.AddHttpClient(ManagementHttpClientName);
        services.AddSingleton<IChangeSource>(provider => new DrasiChangeSource(
            provider.GetRequiredService<DrasiServerClient>(),
            settings.Drasi,
            provider.GetRequiredService<TimeProvider>()));
        services.AddSingleton<IWakeSink>(provider => new TargetRoutedWakeSink(
            provider.GetRequiredService<IHttpClientFactory>(),
            settings.OpenClaw,
            settings.OpenClawTargets));
        services.AddScoped<SignalInbox>(_ => new SignalInbox(settings.SignalCapacity));
        services.AddScoped<SnapshotReconciler>();
        services.AddScoped<OutboxDispatcher>();
        services.AddScoped<SessionPartitioner>(provider => new SessionPartitioner(
            settings.SignalCapacity,
            settings.WorkerCount,
            provider.GetRequiredService<ContractRegistryManager>(),
            async (item, cancellationToken) => await provider.GetRequiredService<OutboxDispatcher>()
                .DispatchOneAsync(item, cancellationToken),
            provider.GetRequiredService<TimeProvider>()));
        services.AddScoped<RecoveryCoordinator>();
        services.AddScoped<IRecoveryCoordinator>(provider => provider.GetRequiredService<RecoveryCoordinator>());
        services.AddScoped<BridgeHostedService>();
        services.AddScoped<ILeaderEpochWorker>(provider => provider.GetRequiredService<BridgeHostedService>());
        services.AddScoped<ReconciliationHostedService>();
        services.AddScoped<ILeaderEpochWorker>(provider => provider.GetRequiredService<ReconciliationHostedService>());
        services.AddScoped<LeaderWorkerRuntime>();
        services.AddScoped<ILeaderWorkerRuntime>(provider => provider.GetRequiredService<LeaderWorkerRuntime>());
        services.AddSingleton<RaftLeaderHostedService>();
        services.AddSingleton<HostStartupValidator>();
        services.AddSingleton<IHostedService>(provider =>
            provider.GetRequiredService<HostStartupValidator>());
        services.AddSingleton<IHostedService>(provider =>
            provider.GetRequiredService<RaftLeaderHostedService>());
        services.AddOpenTelemetry()
            .WithTracing(tracing => tracing.AddSource(BridgeTelemetry.ActivitySourceName))
            .WithMetrics(metrics => metrics.AddMeter(BridgeTelemetry.MeterName));
    }
}
