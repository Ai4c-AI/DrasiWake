using System.Globalization;
using DrasiWake.Adapters.DrasiServer;
using DrasiWake.Adapters.OpenClaw;
using Microsoft.Extensions.Configuration;

namespace DrasiWake.Host;

/// <summary>Admin plane settings: health endpoints, downstream probing and readiness thresholds.</summary>
public sealed record AdminSettings(
    Uri ApiAddress,
    TimeSpan DownstreamProbeInterval,
    TimeSpan CommitLagDegradedAfter,
    string? BearerToken)
{
    public void Validate()
    {
        ArgumentNullException.ThrowIfNull(ApiAddress);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(DownstreamProbeInterval, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(CommitLagDegradedAfter, TimeSpan.Zero);
        if (ApiAddress.Host is "0.0.0.0" or "::" or "[::]")
        {
            throw new InvalidOperationException(
                "DrasiWake:Admin:ApiAddress must bind an explicit host name or address; wildcard bindings are not allowed.");
        }
    }
}

public sealed record DrasiWakeHostSettings(
    DrasiServerOptions Drasi,
    OpenClawOptions OpenClaw,
    IReadOnlyDictionary<string, OpenClawTargetOptions> OpenClawTargets,
    string DatabasePath,
    string RegistryPath,
    int SignalCapacity,
    int WorkerCount,
    TimeSpan ReconciliationInterval,
    TimeSpan ShutdownTimeout,
    TimeSpan DispatchPollInterval,
    RaftClusterSettings Cluster,
    AdminSettings Admin)
{
    public static DrasiWakeHostSettings FromConfiguration(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var drasiUri = ReadUri(configuration, "DrasiWake:Drasi:ServerUri");
        var databasePath = ReadRequired(configuration, "DrasiWake:Database:Path");
        var fullDatabasePath = Path.GetFullPath(databasePath);
        var registryPath = ReadRequired(configuration, "DrasiWake:Registry:Path");
        var clusterSettings = RaftClusterSettings.FromConfiguration(configuration, fullDatabasePath);
        var signalCapacity = ReadInt(configuration, "DrasiWake:ChannelCapacity", 256);
        var workerCount = ReadInt(configuration, "DrasiWake:WorkerCount", 4);
        var openClawTargets = ReadOpenClawTargets(configuration);
        var drasiOptions = new DrasiServerOptions(drasiUri)
        {
            SignalCapacity = signalCapacity,
            InitialReconnectDelay = ReadTimeSpan(configuration, "DrasiWake:Drasi:InitialReconnectDelay", TimeSpan.FromSeconds(1)),
            MaxReconnectDelay = ReadTimeSpan(configuration, "DrasiWake:Drasi:MaxReconnectDelay", TimeSpan.FromSeconds(30))
        };
        var openClawOptions = new OpenClawOptions
        {
            MaxRetryAttempts = ReadInt(configuration, "DrasiWake:OpenClaw:MaxRetryAttempts", 3),
            RetryBaseDelay = ReadTimeSpan(configuration, "DrasiWake:OpenClaw:RetryBaseDelay", TimeSpan.FromMilliseconds(200)),
            MaxRetryDelay = ReadTimeSpan(configuration, "DrasiWake:OpenClaw:MaxRetryDelay", TimeSpan.FromSeconds(5))
        };

        return new DrasiWakeHostSettings(
            drasiOptions,
            openClawOptions,
            openClawTargets,
            fullDatabasePath,
            Path.GetFullPath(registryPath),
            signalCapacity,
            workerCount,
            ReadTimeSpan(configuration, "DrasiWake:ReconciliationInterval", TimeSpan.FromMinutes(1)),
            ReadTimeSpan(configuration, "DrasiWake:ShutdownTimeout", TimeSpan.FromSeconds(30)),
            ReadTimeSpan(configuration, "DrasiWake:DispatchPollInterval", TimeSpan.FromMilliseconds(250)),
            clusterSettings,
            ReadAdminSettings(configuration));
    }

    public void Validate()
    {
        Cluster.Validate();
        ArgumentNullException.ThrowIfNull(Drasi.ServerUri);
        if (!Drasi.ServerUri.IsAbsoluteUri || Drasi.SignalCapacity < 1 ||
            Drasi.InitialReconnectDelay <= TimeSpan.Zero || Drasi.MaxReconnectDelay < Drasi.InitialReconnectDelay)
        {
            throw new InvalidOperationException("Drasi Server configuration is invalid.");
        }
        OpenClaw.Validate();
        foreach (var (targetName, targetOptions) in OpenClawTargets)
            targetOptions.Validate(targetName);
        ArgumentOutOfRangeException.ThrowIfLessThan(SignalCapacity, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(WorkerCount, 1);
        ArgumentException.ThrowIfNullOrWhiteSpace(DatabasePath);
        ArgumentException.ThrowIfNullOrWhiteSpace(RegistryPath);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(ReconciliationInterval, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(ShutdownTimeout, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(DispatchPollInterval, TimeSpan.Zero);
        Admin.Validate();
        if (Admin.ApiAddress.Port == Cluster.ListenAddress.Port)
        {
            throw new InvalidOperationException(
                "DrasiWake:Admin:ApiAddress port must differ from the Raft listen port.");
        }
        if (Cluster.ManagementAddress is { } managementAddress && Admin.ApiAddress.Port == managementAddress.Port)
        {
            throw new InvalidOperationException(
                "DrasiWake:Admin:ApiAddress port must differ from the cluster management port.");
        }
    }

    private static AdminSettings ReadAdminSettings(IConfiguration configuration)
    {
        var addressValue = configuration["DrasiWake:Admin:ApiAddress"] ?? "http://127.0.0.1:5103/";
        if (!Uri.TryCreate(addressValue, UriKind.Absolute, out var address) ||
            (address.Scheme != Uri.UriSchemeHttp && address.Scheme != Uri.UriSchemeHttps) ||
            address.PathAndQuery != "/" ||
            address.Fragment.Length > 0 ||
            address.UserInfo.Length > 0)
        {
            throw new InvalidOperationException(
                "Configuration 'DrasiWake:Admin:ApiAddress' must be an absolute HTTP or HTTPS URI with an empty path, no query, fragment or userinfo.");
        }

        return new AdminSettings(
            address,
            ReadTimeSpan(configuration, "DrasiWake:Admin:DownstreamProbeInterval", TimeSpan.FromSeconds(10)),
            ReadTimeSpan(configuration, "DrasiWake:Admin:CommitLagDegradedAfter", TimeSpan.FromSeconds(5)),
            configuration["DrasiWake:Admin:BearerToken"]);
    }

    private static string ReadRequired(IConfiguration configuration, string key)
        => configuration[key] is { Length: > 0 } value
            ? value
            : throw new InvalidOperationException($"Required configuration '{key}' is missing.");

    private static Uri ReadUri(IConfiguration configuration, string key)
    {
        var value = ReadRequired(configuration, key);
        if (!Uri.TryCreate(value, UriKind.Absolute, out var uri) ||
            (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps))
        {
            throw new InvalidOperationException($"Configuration '{key}' must be an absolute HTTP or HTTPS URI.");
        }
        return uri;
    }

    private static IReadOnlyDictionary<string, OpenClawTargetOptions> ReadOpenClawTargets(
        IConfiguration configuration)
    {
        var targets = new Dictionary<string, OpenClawTargetOptions>(StringComparer.Ordinal);
        foreach (var section in configuration.GetSection("DrasiWake:OpenClaw:Targets").GetChildren())
        {
            var baseAddressKey = $"{section.Path}:BaseAddress";
            var bearerTokenKey = $"{section.Path}:BearerToken";
            var retentionKey = $"{section.Path}:GatewayIdempotencyRetention";
            var targetOptions = new OpenClawTargetOptions(
                ReadUri(configuration, baseAddressKey),
                configuration[bearerTokenKey],
                ReadRequiredTimeSpan(configuration, retentionKey));
            targetOptions.Validate(section.Key);
            targets.Add(section.Key, targetOptions);
        }

        return targets;
    }

    private static int ReadInt(IConfiguration configuration, string key, int defaultValue)
    {
        if (configuration[key] is not { } value)
            return defaultValue;
        if (int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
            return parsed;
        throw new InvalidOperationException($"Configuration '{key}' must be an integer.");
    }

    private static TimeSpan ReadTimeSpan(IConfiguration configuration, string key, TimeSpan defaultValue)
    {
        if (configuration[key] is not { } value)
            return defaultValue;
        if (TimeSpan.TryParse(value, CultureInfo.InvariantCulture, out var parsed))
            return parsed;
        throw new InvalidOperationException($"Configuration '{key}' must be a valid time interval.");
    }

    private static TimeSpan ReadRequiredTimeSpan(IConfiguration configuration, string key)
    {
        var value = ReadRequired(configuration, key);
        if (TimeSpan.TryParse(value, CultureInfo.InvariantCulture, out var parsed))
            return parsed;
        throw new InvalidOperationException($"Configuration '{key}' must be a valid time interval.");
    }
}