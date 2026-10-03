namespace DrasiWake.LocalEnvironment;

public sealed class ComposeStackDefinition
{
    public ComposeStackDefinition(
        ComposeStackResource resource,
        string repositoryPath,
        string projectName,
        string serviceName,
        int containerPort,
        IReadOnlyList<string> containerNames,
        IReadOnlyDictionary<string, string> environment,
        IReadOnlyList<string> secretValues,
        IReadOnlyList<string> removedEnvironmentVariables)
    {
        Resource = resource ?? throw new ArgumentNullException(nameof(resource));
        ArgumentException.ThrowIfNullOrWhiteSpace(repositoryPath);
        ArgumentException.ThrowIfNullOrWhiteSpace(projectName);
        ArgumentException.ThrowIfNullOrWhiteSpace(serviceName);
        ArgumentOutOfRangeException.ThrowIfLessThan(containerPort, 1);
        ArgumentNullException.ThrowIfNull(containerNames);
        ArgumentNullException.ThrowIfNull(environment);
        ArgumentNullException.ThrowIfNull(secretValues);
        ArgumentNullException.ThrowIfNull(removedEnvironmentVariables);

        RepositoryPath = Path.GetFullPath(repositoryPath);
        ProjectName = projectName;
        ServiceName = serviceName;
        ContainerPort = containerPort;
        ContainerNames = containerNames.ToArray();
        Environment = new Dictionary<string, string>(environment, StringComparer.OrdinalIgnoreCase);
        SecretValues = secretValues.ToArray();
        RemovedEnvironmentVariables = removedEnvironmentVariables.ToArray();
    }

    public ComposeStackResource Resource { get; }

    public string RepositoryPath { get; }

    public string ComposeFilePath => Path.Combine(RepositoryPath, "docker-compose.yml");

    public string ProjectName { get; }

    public string ServiceName { get; }

    public int ContainerPort { get; }

    public IReadOnlyList<string> ContainerNames { get; }

    public IReadOnlyDictionary<string, string> Environment { get; }

    public IReadOnlyList<string> SecretValues { get; }

    public IReadOnlyList<string> RemovedEnvironmentVariables { get; }

    public static IReadOnlyList<ComposeStackDefinition> Create(
        ComposeEnvironmentOptions options,
        ComposeStackResource drasiResource,
        ComposeStackResource openClawResource)
    {
        ArgumentNullException.ThrowIfNull(options);
        var runId = Guid.NewGuid().ToString("N");
        var openClawEnvironment = new Dictionary<string, string>
        {
            ["MODEL_PROVIDER_KEY"] = options.ModelProviderKey,
            ["OPENCLAW_AUTH_TOKEN"] = options.AuthToken
        };
        if (!string.IsNullOrWhiteSpace(options.ModelProviderEndpoint))
        {
            openClawEnvironment["MODEL_PROVIDER_ENDPOINT"] = options.ModelProviderEndpoint;
        }
        if (!string.IsNullOrWhiteSpace(options.ModelName))
        {
            openClawEnvironment["OPENCLAW_MODEL"] = options.ModelName;
        }

        return
        [
            new ComposeStackDefinition(
                drasiResource,
                options.DrasiRepositoryPath,
                $"drasiwake-drasi-{runId}",
                "drasi-server",
                8080,
                ["drasi-server", "drasi-postgres"],
                new Dictionary<string, string>(),
                [options.ModelProviderKey, options.AuthToken],
                [
                    "MODEL_PROVIDER_KEY",
                    "OPENCLAW_AUTH_TOKEN",
                    "MODEL_PROVIDER_ENDPOINT",
                    "OPENCLAW_MODEL",
                    "MODEL_PROVIDER_MODEL",
                    "LLM_API_KEY",
                    "LLM_BASE_URL",
                    "LLM_MODEL_NAME",
                    "DrasiWake__DevEnvironment__OpenClaw__ModelProviderKey",
                    "DrasiWake__DevEnvironment__OpenClaw__ModelProviderEndpoint",
                    "DrasiWake__DevEnvironment__OpenClaw__ModelName",
                    "DrasiWake__DevEnvironment__OpenClaw__AuthToken"
                ]),
            new ComposeStackDefinition(
                openClawResource,
                options.OpenClawRepositoryPath,
                $"drasiwake-openclaw-{runId}",
                "openclaw",
                18789,
                ["openclaw-gateway", "openclaw-caddy"],
                openClawEnvironment,
                [options.ModelProviderKey, options.AuthToken],
                [
                    "MODEL_PROVIDER_KEY",
                    "MODEL_PROVIDER_ENDPOINT",
                    "MODEL_PROVIDER_MODEL",
                    "OPENCLAW_MODEL",
                    "LLM_API_KEY",
                    "LLM_BASE_URL",
                    "LLM_MODEL_NAME",
                    "DrasiWake__DevEnvironment__OpenClaw__ModelProviderKey",
                    "DrasiWake__DevEnvironment__OpenClaw__ModelProviderEndpoint",
                    "DrasiWake__DevEnvironment__OpenClaw__ModelName",
                    "DrasiWake__DevEnvironment__OpenClaw__AuthToken"
                ])
        ];
    }
}