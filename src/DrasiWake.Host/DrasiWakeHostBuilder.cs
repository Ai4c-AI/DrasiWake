using DrasiWake.Adapters.DrasiServer;
using DrasiWake.Adapters.OpenClaw;
using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Contracts;
using DrasiWake.Core.Pipeline;
using DrasiWake.Persistence.SonnetDB;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace DrasiWake.Host;

public static class DrasiWakeHostBuilder
{
    public static IHost CreateHost(string[] args)
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder(args);
        RegisterStartupServices(builder.Services, builder.Configuration);
        return builder.Build();
    }

    public static IHost CreateHost(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Configuration.AddConfiguration(configuration);
        RegisterStartupServices(builder.Services, builder.Configuration);
        return builder.Build();
    }

    private static void RegisterStartupServices(IServiceCollection services, IConfiguration configuration)
    {
        var settings = DrasiWakeHostSettings.FromConfiguration(configuration);
        services.AddSingleton(settings);
        services.AddSingleton<TimeProvider>(TimeProvider.System);
        services.AddSingleton<DrasiServerOptions>(settings.Drasi);
        services.AddSingleton<OpenClawOptions>(settings.OpenClaw);
        services.AddSingleton<ContractRegistryManager>();
        services.AddSingleton<ContractRegistryLoader>();
        services.AddDbContextFactory<BridgeDbContext>(options =>
            options.UseSonnetDB($"Data Source={settings.DatabasePath}"));
        services.AddSingleton<IBridgeStore, SonnetBridgeStore>();
        services.AddHttpClient<DrasiServerClient>();
        services.AddSingleton<IChangeSource>(provider => new DrasiChangeSource(
            provider.GetRequiredService<DrasiServerClient>(),
            settings.Drasi,
            provider.GetRequiredService<TimeProvider>()));
        services.AddHttpClient<OpenClawMetaInvocationClient>();
        services.AddTransient<IWakeSink>(provider => provider.GetRequiredService<OpenClawMetaInvocationClient>());
        services.AddSingleton<SignalInbox>(_ => new SignalInbox(settings.SignalCapacity));
        services.AddSingleton<SnapshotReconciler>();
        services.AddSingleton<OutboxDispatcher>();
        services.AddSingleton<SessionPartitioner>(provider => new SessionPartitioner(
            settings.SignalCapacity,
            settings.WorkerCount,
            provider.GetRequiredService<ContractRegistryManager>(),
            async (item, cancellationToken) => await provider.GetRequiredService<OutboxDispatcher>()
                .DispatchOneAsync(item, cancellationToken),
            provider.GetRequiredService<TimeProvider>()));
        services.AddSingleton<RecoveryCoordinator>();
        services.AddSingleton<BridgeHostedService>();
        services.AddSingleton<ReconciliationHostedService>();
        services.AddSingleton<HostStartupValidator>();
        services.AddSingleton<IHostedService>(provider => provider.GetRequiredService<HostStartupValidator>());
        services.AddSingleton<IHostedService>(provider => provider.GetRequiredService<BridgeHostedService>());
        services.AddSingleton<IHostedService>(provider => provider.GetRequiredService<ReconciliationHostedService>());
        services.AddOpenTelemetry()
            .WithTracing(tracing => tracing.AddSource(BridgeTelemetry.ActivitySourceName))
            .WithMetrics(metrics => metrics.AddMeter(BridgeTelemetry.MeterName));
    }
}