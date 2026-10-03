using System.Text.RegularExpressions;
using YamlDotNet.RepresentationModel;

namespace DrasiWake.LocalEnvironment;

public static partial class ComposeFileInspector
{
    public static IReadOnlySet<int> GetDefaultHostPorts(string composeFilePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(composeFilePath);

        using var reader = File.OpenText(composeFilePath);
        var yaml = new YamlStream();
        yaml.Load(reader);

        if (yaml.Documents.Count != 1 || yaml.Documents[0].RootNode is not YamlMappingNode root ||
            !root.Children.TryGetValue(new YamlScalarNode("services"), out var servicesNode) ||
            servicesNode is not YamlMappingNode services)
        {
            throw new FormatException("Compose file must contain a services mapping.");
        }

        var ports = new HashSet<int>();
        foreach (var serviceNode in services.Children.Values.OfType<YamlMappingNode>())
        {
            if (serviceNode.Children.TryGetValue(new YamlScalarNode("profiles"), out var profilesNode) &&
                profilesNode is YamlSequenceNode profiles && profiles.Children.Count > 0)
            {
                continue;
            }

            if (!serviceNode.Children.TryGetValue(new YamlScalarNode("ports"), out var portsNode) ||
                portsNode is not YamlSequenceNode declaredPorts)
            {
                continue;
            }

            foreach (var portNode in declaredPorts.Children.OfType<YamlScalarNode>())
            {
                if (TryGetDefaultHostPort(portNode.Value ?? string.Empty, out var port))
                {
                    ports.Add(port);
                }
            }
        }

        return ports;
    }

    private static bool TryGetDefaultHostPort(string declaration, out int port)
    {
        var expanded = DefaultInterpolationRegex().Replace(declaration, "$1");
        var parts = expanded.Split(':');
        if (parts.Length < 2 || !int.TryParse(parts[^2], out port) || port is < 1 or > 65535)
        {
            port = default;
            return false;
        }

        return true;
    }

    [GeneratedRegex(@"\$\{[^}:]+:-([^}]+)\}", RegexOptions.CultureInvariant)]
    private static partial Regex DefaultInterpolationRegex();
}