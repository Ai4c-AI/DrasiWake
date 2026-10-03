using Microsoft.Extensions.Configuration;

namespace DrasiWake.LocalEnvironment;

public sealed class ComposeEnvironmentOptions
{
    public ComposeEnvironmentOptions(
        string repositoryRoot,
        string drasiRepositoryPath,
        string openClawRepositoryPath,
        string modelProviderKey,
        string authToken,
        string modelProviderEndpoint = "",
        string modelName = "")
    {
        RepositoryRoot = repositoryRoot;
        DrasiRepositoryPath = drasiRepositoryPath;
        OpenClawRepositoryPath = openClawRepositoryPath;
        ModelProviderKey = modelProviderKey;
        AuthToken = authToken;
        ModelProviderEndpoint = modelProviderEndpoint;
        ModelName = modelName;
    }

    public string RepositoryRoot { get; }

    public string DrasiRepositoryPath { get; }

    public string OpenClawRepositoryPath { get; }

    public string ModelProviderKey { get; }

    public string AuthToken { get; }

    public string ModelProviderEndpoint { get; }

    public string ModelName { get; }

    public static ComposeEnvironmentOptions FromConfiguration(
        IConfiguration configuration,
        string repositoryRoot)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentException.ThrowIfNullOrWhiteSpace(repositoryRoot);

        var root = Path.GetFullPath(repositoryRoot);
        return new ComposeEnvironmentOptions(
            root,
            ResolvePath(configuration["DrasiWake:DevEnvironment:DrasiRepositoryPath"], "drasi-server", root),
            ResolvePath(configuration["DrasiWake:DevEnvironment:OpenClawRepositoryPath"], "openclaw.net", root),
            GetConfiguredValue(
                configuration,
                "DrasiWake:DevEnvironment:OpenClaw:ModelProviderKey",
                "MODEL_PROVIDER_KEY",
                "LLM_API_KEY"),
            configuration["DrasiWake:DevEnvironment:OpenClaw:AuthToken"] ?? string.Empty,
            GetConfiguredValue(
                configuration,
                "DrasiWake:DevEnvironment:OpenClaw:ModelProviderEndpoint",
                "MODEL_PROVIDER_ENDPOINT",
                "LLM_BASE_URL"),
            GetConfiguredValue(
                configuration,
                "DrasiWake:DevEnvironment:OpenClaw:ModelName",
                "MODEL_PROVIDER_MODEL",
                "LLM_MODEL_NAME"));
    }

    private static string GetConfiguredValue(
        IConfiguration configuration,
        string primaryKey,
        params string[] fallbackKeys)
    {
        var value = configuration[primaryKey];
        if (!string.IsNullOrWhiteSpace(value))
        {
            return value;
        }

        foreach (var fallbackKey in fallbackKeys)
        {
            value = configuration[fallbackKey];
            if (!string.IsNullOrWhiteSpace(value))
            {
                return value;
            }
        }

        return string.Empty;
    }

    private static string ResolvePath(string? configuredPath, string defaultDirectory, string root)
    {
        var path = string.IsNullOrWhiteSpace(configuredPath)
            ? Path.Combine(root, "..", defaultDirectory)
            : Path.IsPathRooted(configuredPath)
                ? configuredPath
                : Path.Combine(root, configuredPath);

        return Path.GetFullPath(path);
    }
}