using System.Globalization;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Configuration;

namespace DrasiWake.Host;

public enum RaftClusterMode
{
    SingleNode,
    Cluster
}

public sealed class RaftClusterSettings
{
    private readonly string databasePath;

    private RaftClusterSettings(
        string databasePath,
        RaftClusterMode mode,
        string nodeId,
        Uri listenAddress,
        string raftDataPath,
        IReadOnlyList<Uri> initialMembers,
        string? certificatePath,
        string? certificatePassword,
        Uri? managementAddress,
        string? managementBearerToken,
        int snapshotFrequency)
    {
        this.databasePath = databasePath;
        Mode = mode;
        NodeId = nodeId;
        ListenAddress = listenAddress;
        RaftDataPath = raftDataPath;
        InitialMembers = initialMembers;
        CertificatePath = certificatePath;
        CertificatePassword = certificatePassword;
        ManagementAddress = managementAddress;
        ManagementBearerToken = managementBearerToken;
        SnapshotFrequency = snapshotFrequency;
    }

    public RaftClusterMode Mode { get; }
    public string NodeId { get; }
    public Uri ListenAddress { get; }
    public string RaftDataPath { get; }
    public IReadOnlyList<Uri> InitialMembers { get; }
    public string? CertificatePath { get; }
    public string? CertificatePassword { get; }
    public Uri? ManagementAddress { get; }
    public string? ManagementBearerToken { get; }
    public int SnapshotFrequency { get; }

    public static RaftClusterSettings FromConfiguration(IConfiguration configuration, string databasePath)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentException.ThrowIfNullOrWhiteSpace(databasePath);

        var fullDatabasePath = Path.GetFullPath(databasePath);
        var mode = ReadMode(configuration);
        var nodeId = ReadOptional(configuration, "DrasiWake:Cluster:NodeId") ??
            (mode == RaftClusterMode.SingleNode
                ? "local"
                : throw Missing("DrasiWake:Cluster:NodeId"));
        if (string.IsNullOrWhiteSpace(nodeId))
            throw Invalid("DrasiWake:Cluster:NodeId", "must not be empty.");

        var listenAddress = ReadAddress(
            configuration,
            "DrasiWake:Cluster:ListenAddress",
            mode == RaftClusterMode.SingleNode ? new Uri("http://127.0.0.1:50051/") : null,
            requireHttps: mode == RaftClusterMode.Cluster);

        var raftDataPathValue = ReadOptional(configuration, "DrasiWake:Cluster:RaftDataPath") ??
            (mode == RaftClusterMode.SingleNode
                ? fullDatabasePath + "-raft"
                : throw Missing("DrasiWake:Cluster:RaftDataPath"));
        if (string.IsNullOrWhiteSpace(raftDataPathValue))
            throw Invalid("DrasiWake:Cluster:RaftDataPath", "must not be empty.");
        var raftDataPath = Path.GetFullPath(raftDataPathValue);

        var initialMembers = ReadInitialMembers(configuration, listenAddress, mode);
        var certificatePath = mode == RaftClusterMode.Cluster
            ? ReadRequired(configuration, "DrasiWake:Cluster:Certificate:Path")
            : null;
        var certificatePassword = mode == RaftClusterMode.Cluster
            ? ReadRequired(configuration, "DrasiWake:Cluster:Certificate:Password")
            : null;
        var managementAddress = mode == RaftClusterMode.Cluster
            ? ReadAddress(configuration, "DrasiWake:Cluster:Management:Address", null, requireHttps: true)
            : null;
        var managementBearerToken = mode == RaftClusterMode.Cluster
            ? ReadRequired(configuration, "DrasiWake:Cluster:Management:BearerToken")
            : null;
        var snapshotFrequency = mode == RaftClusterMode.Cluster
            ? ReadRequiredInteger(configuration, "DrasiWake:Cluster:SnapshotFrequency")
            : ReadInteger(configuration, "DrasiWake:Cluster:SnapshotFrequency", defaultValue: 1000);

        var settings = new RaftClusterSettings(
            fullDatabasePath,
            mode,
            nodeId,
            listenAddress,
            raftDataPath,
            initialMembers,
            certificatePath,
            certificatePassword,
            managementAddress,
            managementBearerToken,
            snapshotFrequency);
        settings.Validate();
        return settings;
    }

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(NodeId))
            throw Invalid("DrasiWake:Cluster:NodeId", "must not be empty.");
        ArgumentException.ThrowIfNullOrWhiteSpace(RaftDataPath);
        if (PathsEqual(databasePath, RaftDataPath))
            throw Invalid("DrasiWake:Cluster:RaftDataPath", "must be independent from DrasiWake:Database:Path.");
        if (SnapshotFrequency <= 0)
            throw Invalid("DrasiWake:Cluster:SnapshotFrequency", "must be a positive integer.");

        if (InitialMembers.Count == 0)
            throw Invalid("DrasiWake:Cluster:InitialMembers", "must contain at least one absolute URI.");

        var memberSet = new HashSet<Uri>();
        for (var index = 0; index < InitialMembers.Count; index++)
        {
            var member = InitialMembers[index];
            if (!IsValidEndpoint(member) ||
                (Mode == RaftClusterMode.Cluster && member.Scheme != Uri.UriSchemeHttps))
            {
                var schemeRequirement = Mode == RaftClusterMode.Cluster ? "HTTPS" : "HTTP or HTTPS";
                throw Invalid(
                    $"DrasiWake:Cluster:InitialMembers:{index}",
                    $"must be a valid absolute {schemeRequirement} URI.");
            }

            if (!memberSet.Add(member))
                throw Invalid($"DrasiWake:Cluster:InitialMembers:{index}", "must not duplicate another member URI.");
        }

        if (!memberSet.Contains(ListenAddress))
            throw Invalid("DrasiWake:Cluster:InitialMembers", "must include DrasiWake:Cluster:ListenAddress.");

        if (Mode == RaftClusterMode.SingleNode)
        {
            if (ListenAddress.Scheme != Uri.UriSchemeHttp)
                throw Invalid(
                    "DrasiWake:Cluster:ListenAddress",
                    "SingleNode mode requires an HTTP loopback address because TLS is only configured in Cluster mode.");
            if (!ListenAddress.IsLoopback)
                throw Invalid("DrasiWake:Cluster:ListenAddress", "SingleNode mode requires one loopback member.");
            if (InitialMembers.Count != 1)
                throw Invalid("DrasiWake:Cluster:InitialMembers", "SingleNode mode requires exactly one member.");
            return;
        }

        if (!IsValidEndpoint(ListenAddress) || ListenAddress.Scheme != Uri.UriSchemeHttps)
            throw Invalid("DrasiWake:Cluster:ListenAddress", "must be an absolute HTTPS URI.");
        if (string.IsNullOrWhiteSpace(CertificatePath))
            throw Missing("DrasiWake:Cluster:Certificate:Path");
        if (string.IsNullOrWhiteSpace(CertificatePassword))
            throw Missing("DrasiWake:Cluster:Certificate:Password");
        if (ManagementAddress is null || ManagementAddress.Scheme != Uri.UriSchemeHttps ||
            !IsValidEndpoint(ManagementAddress))
        {
            throw Invalid("DrasiWake:Cluster:Management:Address", "must be an absolute HTTPS URI.");
        }
        if (ManagementAddress.Port == ListenAddress.Port)
        {
            throw Invalid(
                "DrasiWake:Cluster:Management:Address",
                "must use a separate TCP port from DrasiWake:Cluster:ListenAddress.");
        }
        if (string.IsNullOrWhiteSpace(ManagementBearerToken))
            throw Missing("DrasiWake:Cluster:Management:BearerToken");
    }

    public X509Certificate2 LoadServerCertificate()
    {
        if (Mode != RaftClusterMode.Cluster)
            throw new InvalidOperationException("A TLS server certificate is only configured for Cluster mode.");

        X509Certificate2 certificate;
        try
        {
            certificate = X509CertificateLoader.LoadPkcs12FromFile(
                Path.GetFullPath(CertificatePath!),
                CertificatePassword,
                X509KeyStorageFlags.DefaultKeySet);
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or CryptographicException)
        {
            throw new InvalidOperationException(
                "The configured Raft TLS certificate could not be loaded.",
                exception);
        }

        var now = DateTime.UtcNow;
        var enhancedKeyUsage = certificate.Extensions
            .OfType<X509EnhancedKeyUsageExtension>()
            .SingleOrDefault();
        if (!certificate.HasPrivateKey ||
            now < certificate.NotBefore.ToUniversalTime() ||
            now > certificate.NotAfter.ToUniversalTime() ||
            (enhancedKeyUsage is not null &&
             !enhancedKeyUsage.EnhancedKeyUsages
                 .Cast<Oid>()
                 .Any(oid => oid.Value == "1.3.6.1.5.5.7.3.1")))
        {
            certificate.Dispose();
            throw new InvalidOperationException(
                "The configured Raft TLS certificate must be current, include its private key, and be valid for server authentication.");
        }

        return certificate;
    }

    public void ValidateServerCertificateNames(X509Certificate2 certificate)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        if (Mode != RaftClusterMode.Cluster)
            return;

        if (!certificate.MatchesHostname(ListenAddress.DnsSafeHost, allowWildcards: true, allowCommonName: true))
        {
            throw Invalid(
                "DrasiWake:Cluster:Certificate:Path",
                "must be valid for the configured Raft listener address.");
        }

        if (ManagementAddress is null ||
            !certificate.MatchesHostname(ManagementAddress.DnsSafeHost, allowWildcards: true, allowCommonName: true))
        {
            throw Invalid(
                "DrasiWake:Cluster:Certificate:Path",
                "must be valid for the configured management listener address.");
        }
    }

    private static RaftClusterMode ReadMode(IConfiguration configuration)
    {
        const string key = "DrasiWake:Cluster:Mode";
        var value = configuration[key];
        if (string.IsNullOrWhiteSpace(value))
        {
            if (configuration.GetSection("DrasiWake:Cluster").GetChildren().Any())
                throw Missing(key);
            return RaftClusterMode.SingleNode;
        }
        if (string.Equals(value, nameof(RaftClusterMode.SingleNode), StringComparison.OrdinalIgnoreCase))
            return RaftClusterMode.SingleNode;
        if (string.Equals(value, nameof(RaftClusterMode.Cluster), StringComparison.OrdinalIgnoreCase))
            return RaftClusterMode.Cluster;
        throw Invalid(key, "must be SingleNode or Cluster.");
    }

    private static int ReadRequiredInteger(IConfiguration configuration, string key)
    {
        var value = ReadRequired(configuration, key);
        if (int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
            return parsed;
        throw Invalid(key, "must be an integer.");
    }

    private static Uri ReadAddress(
        IConfiguration configuration,
        string key,
        Uri? defaultValue,
        bool requireHttps)
    {
        var value = ReadOptional(configuration, key);
        if (value is null)
        {
            if (defaultValue is not null)
                return defaultValue;
            throw Missing(key);
        }
        if (!Uri.TryCreate(value, UriKind.Absolute, out var address) ||
            !IsValidEndpoint(address) ||
            (requireHttps && address.Scheme != Uri.UriSchemeHttps))
        {
            throw Invalid(key, requireHttps ? "must be an absolute HTTPS URI." : "must be an absolute HTTP or HTTPS URI.");
        }

        return Normalize(address);
    }

    private static IReadOnlyList<Uri> ReadInitialMembers(
        IConfiguration configuration,
        Uri listenAddress,
        RaftClusterMode mode)
    {
        var memberSections = configuration.GetSection("DrasiWake:Cluster:InitialMembers")
            .GetChildren()
            .OrderBy(section => int.TryParse(section.Key, NumberStyles.None, CultureInfo.InvariantCulture, out var index)
                ? index
                : int.MaxValue)
            .ToArray();
        if (memberSections.Length == 0 && mode == RaftClusterMode.SingleNode)
            return [listenAddress];
        if (memberSections.Length == 0)
            throw Invalid("DrasiWake:Cluster:InitialMembers", "must contain at least one absolute URI.");

        var members = new List<Uri>(memberSections.Length);
        var uniqueMembers = new HashSet<Uri>();
        foreach (var section in memberSections)
        {
            var key = $"{section.Path}";
            if (string.IsNullOrWhiteSpace(section.Value) ||
                !Uri.TryCreate(section.Value, UriKind.Absolute, out var member) ||
                !IsValidEndpoint(member) ||
                (mode == RaftClusterMode.Cluster && member.Scheme != Uri.UriSchemeHttps))
            {
                var schemeRequirement = mode == RaftClusterMode.Cluster ? "HTTPS" : "HTTP or HTTPS";
                throw Invalid(key, $"must be a valid absolute {schemeRequirement} URI.");
            }
            var normalizedMember = Normalize(member);
            if (!uniqueMembers.Add(normalizedMember))
                throw Invalid(key, "must not duplicate another member URI.");
            members.Add(normalizedMember);
        }
        return members;
    }

    private static int ReadInteger(IConfiguration configuration, string key, int defaultValue)
    {
        var value = configuration[key];
        if (value is null)
            return defaultValue;
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
            throw Invalid(key, "must be an integer.");
        return parsed;
    }

    private static string ReadRequired(IConfiguration configuration, string key)
    {
        var value = configuration[key];
        return string.IsNullOrWhiteSpace(value) ? throw Missing(key) : value;
    }

    private static string? ReadOptional(IConfiguration configuration, string key)
    {
        var value = configuration[key];
        return string.IsNullOrWhiteSpace(value) ? null : value;
    }

    private static bool IsValidEndpoint(Uri address)
        => address.IsAbsoluteUri &&
            (address.Scheme == Uri.UriSchemeHttp || address.Scheme == Uri.UriSchemeHttps) &&
            !string.IsNullOrWhiteSpace(address.Host) &&
            address.Port is > 0 and <= 65535 &&
            string.IsNullOrWhiteSpace(address.UserInfo) &&
            string.IsNullOrWhiteSpace(address.Query) &&
            string.IsNullOrWhiteSpace(address.Fragment) &&
            address.AbsolutePath is "" or "/";

    private static Uri Normalize(Uri address)
    {
        var builder = new UriBuilder(address)
        {
            Host = address.IdnHost.ToLowerInvariant()
        };
        return builder.Uri;
    }

    private static bool PathsEqual(string left, string right)
    {
        var comparison = OperatingSystem.IsWindows()
            ? StringComparison.OrdinalIgnoreCase
            : StringComparison.Ordinal;
        return string.Equals(
            Path.TrimEndingDirectorySeparator(Path.GetFullPath(left)),
            Path.TrimEndingDirectorySeparator(Path.GetFullPath(right)),
            comparison);
    }

    private static InvalidOperationException Missing(string key)
        => Invalid(key, "is required.");

    private static InvalidOperationException Invalid(string key, string requirement)
        => new($"Configuration '{key}' {requirement}");
}
