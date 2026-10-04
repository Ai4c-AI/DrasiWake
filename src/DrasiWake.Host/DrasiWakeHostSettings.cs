using System.Globalization;
using DrasiWake.Adapters.DrasiServer;
using DrasiWake.Adapters.OpenClaw;
using Microsoft.Extensions.Configuration;

namespace DrasiWake.Host;

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
    TimeSpan DispatchPollInterval)
{
    public static DrasiWakeHostSettings FromConfiguration(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var drasiUri = ReadUri(configuration, "DrasiWake:Drasi:ServerUri");
        var databasePath = ReadRequired(configuration, "DrasiWake:Database:Path");
        var registryPath = ReadRequired(configuration, "DrasiWake:Registry:Path");
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
            Path.GetFullPath(databasePath),
            Path.GetFullPath(registryPath),
            signalCapacity,
            workerCount,
            ReadTimeSpan(configuration, "DrasiWake:ReconciliationInterval", TimeSpan.FromMinutes(1)),
            ReadTimeSpan(configuration, "DrasiWake:ShutdownTimeout", TimeSpan.FromSeconds(30)),
            ReadTimeSpan(configuration, "DrasiWake:DispatchPollInterval", TimeSpan.FromMilliseconds(250)));
    }

    public void Validate()
    {
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