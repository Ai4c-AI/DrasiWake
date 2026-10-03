using System.Net;

namespace DrasiWake.LocalEnvironment;

public static class ComposePortParser
{
    public static Uri Parse(string output, string serviceName)
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentException.ThrowIfNullOrWhiteSpace(serviceName);

        var mappings = output.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (mappings.Length != 1)
        {
            throw new FormatException(
                $"Expected one published port for Compose service '{serviceName}', found {mappings.Length}.");
        }

        var mapping = mappings[0];
        var separator = mapping.LastIndexOf(':');
        if (separator <= 0 || separator == mapping.Length - 1)
        {
            throw InvalidMapping(serviceName);
        }

        var host = mapping[..separator].Trim('[', ']');
        var portText = mapping[(separator + 1)..];
        if (!int.TryParse(portText, out var port) || port is < 1 or > 65535)
        {
            throw InvalidMapping(serviceName);
        }

        if (host.Length == 0 || host == "*" || host == "0.0.0.0" || host == "::")
        {
            host = IPAddress.Loopback.ToString();
        }
        else if (IPAddress.TryParse(host, out var address))
        {
            host = address.ToString();
        }
        else if (host.Contains(':', StringComparison.Ordinal))
        {
            throw InvalidMapping(serviceName);
        }

        return new UriBuilder(Uri.UriSchemeHttp, host, port).Uri;
    }

    private static FormatException InvalidMapping(string serviceName) =>
        new($"Compose returned an invalid published port for service '{serviceName}'.");
}