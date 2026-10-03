using DrasiWake.LocalEnvironment;
using Microsoft.Extensions.Configuration;

public sealed class ComposeEnvironmentValidatorTests
{
    [Fact]
    public void Reads_default_published_ports_from_compose_yaml_without_reading_env_file()
    {
        using var fixture = new EnvironmentFixture();
        var composeFile = Path.Combine(fixture.Root, "docker-compose.yml");
        File.WriteAllText(composeFile,
            "services:\n  api:\n    ports:\n      - '${DRASI_API_PORT:-8080}:8080'\n      - '127.0.0.1:${DB_PORT:-5432}:5432'\n  tls:\n    profiles:\n      - with-tls\n    ports:\n      - '80:80'\n      - '443:443'\n");
        File.WriteAllText(Path.Combine(fixture.Root, ".env"), "DRASI_API_PORT=12345\nDB_PORT=23456\n");

        var ports = ComposeFileInspector.GetDefaultHostPorts(composeFile);

        Assert.Equal(new HashSet<int> { 8080, 5432 }, ports);
    }

    [Fact]
    public void Uses_sibling_defaults_and_resolves_relative_paths_from_repository_root()
    {
        using var fixture = new EnvironmentFixture();
        var options = ComposeEnvironmentOptions.FromConfiguration(
            CreateConfiguration(new Dictionary<string, string?>
            {
                ["DrasiWake:DevEnvironment:DrasiRepositoryPath"] = "external/drasi",
                ["DrasiWake:DevEnvironment:OpenClawRepositoryPath"] = null,
                ["DrasiWake:DevEnvironment:OpenClaw:ModelProviderKey"] = "provider-secret",
                ["DrasiWake:DevEnvironment:OpenClaw:AuthToken"] = "gateway-secret"
            }),
            fixture.Root);

        Assert.Equal(Path.Combine(fixture.Root, "external", "drasi"), options.DrasiRepositoryPath);
        Assert.Equal(
            Path.GetFullPath(Path.Combine(fixture.Root, "..", "openclaw.net")),
            options.OpenClawRepositoryPath);
    }

    [Fact]
    public void Uses_repository_compose_copies_and_preserves_sibling_project_directories()
    {
        using var fixture = new EnvironmentFixture();
        var options = new ComposeEnvironmentOptions(
            fixture.Root,
            fixture.OpenClawPath,
            fixture.OpenClawPath,
            "provider-key",
            "gateway-token");
        var stacks = ComposeStackDefinition.Create(
            options,
            new ComposeStackResource("drasi-compose"),
            new ComposeStackResource("openclaw-compose"));

        Assert.Equal(options.DrasiComposeFilePath, stacks[0].ComposeFilePath);
        Assert.Equal(options.DrasiRepositoryPath, stacks[0].RepositoryPath);
        Assert.Equal(options.OpenClawComposeFilePath, stacks[1].ComposeFilePath);
        Assert.Equal(options.OpenClawRepositoryPath, stacks[1].RepositoryPath);
    }

    [Fact]
    public void Maps_llm_environment_variables_to_openclaw_compose_without_passing_them_to_drasi()
    {
        using var fixture = new EnvironmentFixture();
        var options = ComposeEnvironmentOptions.FromConfiguration(
            CreateConfiguration(new Dictionary<string, string?>
            {
                ["LLM_API_KEY"] = "test-api-key",
                ["LLM_BASE_URL"] = "https://api.minimax.cn/v1",
                ["LLM_MODEL_NAME"] = "test-model",
                ["DrasiWake:DevEnvironment:OpenClaw:AuthToken"] = "gateway-secret"
            }),
            fixture.Root);
        var stacks = ComposeStackDefinition.Create(
            options,
            new ComposeStackResource("drasi-compose"),
            new ComposeStackResource("openclaw-compose"));

        Assert.Equal("test-api-key", stacks[1].Environment["MODEL_PROVIDER_KEY"]);
        Assert.Equal("https://api.minimax.cn/v1", stacks[1].Environment["MODEL_PROVIDER_ENDPOINT"]);
        Assert.Equal("test-model", stacks[1].Environment["OPENCLAW_MODEL"]);
        Assert.Single(stacks[0].Environment);
        Assert.Equal(options.DrasiFixtureConfigPath, stacks[0].Environment["DRASIWAKE_FIXTURE_CONFIG_PATH"]);
        Assert.Contains("LLM_API_KEY", stacks[0].RemovedEnvironmentVariables);
        Assert.Contains("LLM_BASE_URL", stacks[0].RemovedEnvironmentVariables);
        Assert.Contains("LLM_MODEL_NAME", stacks[0].RemovedEnvironmentVariables);
    }

    [Fact]
    public void Maps_model_provider_environment_variables_to_openclaw_compose()
    {
        using var fixture = new EnvironmentFixture();
        var options = ComposeEnvironmentOptions.FromConfiguration(
            CreateConfiguration(new Dictionary<string, string?>
            {
                ["MODEL_PROVIDER_KEY"] = "provider-api-key",
                ["MODEL_PROVIDER_ENDPOINT"] = "https://api.minimax.cn/v1",
                ["MODEL_PROVIDER_MODEL"] = "provider-model",
                ["DrasiWake:DevEnvironment:OpenClaw:AuthToken"] = "gateway-secret"
            }),
            fixture.Root);
        var stacks = ComposeStackDefinition.Create(
            options,
            new ComposeStackResource("drasi-compose"),
            new ComposeStackResource("openclaw-compose"));

        Assert.Equal("provider-api-key", stacks[1].Environment["MODEL_PROVIDER_KEY"]);
        Assert.Equal("https://api.minimax.cn/v1", stacks[1].Environment["MODEL_PROVIDER_ENDPOINT"]);
        Assert.Equal("provider-model", stacks[1].Environment["OPENCLAW_MODEL"]);
        Assert.Contains("MODEL_PROVIDER_KEY", stacks[0].RemovedEnvironmentVariables);
        Assert.Contains("MODEL_PROVIDER_ENDPOINT", stacks[0].RemovedEnvironmentVariables);
        Assert.Contains("MODEL_PROVIDER_MODEL", stacks[0].RemovedEnvironmentVariables);
    }

    [Fact]
    public void Validates_repository_paths_and_required_secrets_without_disclosing_secret_values()
    {
        using var fixture = new EnvironmentFixture();
        var options = ComposeEnvironmentOptions.FromConfiguration(
            CreateConfiguration(new Dictionary<string, string?>
            {
                ["DrasiWake:DevEnvironment:DrasiRepositoryPath"] = Path.Combine(fixture.Root, "missing-drasi"),
                ["DrasiWake:DevEnvironment:OpenClawRepositoryPath"] = fixture.OpenClawPath,
                ["DrasiWake:DevEnvironment:OpenClaw:ModelProviderKey"] = "provider-secret-value",
                ["DrasiWake:DevEnvironment:OpenClaw:AuthToken"] = null
            }),
            fixture.Root);

        var errors = new ComposeEnvironmentValidator(options).Validate();
        var diagnostic = string.Join(Environment.NewLine, errors);

        Assert.Contains(errors, error => error.Contains("DrasiRepositoryPath", StringComparison.Ordinal));
        Assert.Contains(errors, error => error.Contains("AuthToken", StringComparison.Ordinal));
        Assert.DoesNotContain("provider-secret-value", diagnostic, StringComparison.Ordinal);
        Assert.DoesNotContain("gateway-secret-value", diagnostic, StringComparison.Ordinal);
        Assert.DoesNotContain("provider-secret-value", options.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void Validates_default_and_absolute_repository_paths()
    {
        using var fixture = new EnvironmentFixture();
        var defaults = ComposeEnvironmentOptions.FromConfiguration(
            CreateConfiguration(new Dictionary<string, string?>
            {
                ["DrasiWake:DevEnvironment:DrasiRepositoryPath"] = null,
                ["DrasiWake:DevEnvironment:OpenClawRepositoryPath"] = null,
                ["DrasiWake:DevEnvironment:OpenClaw:ModelProviderKey"] = "key",
                ["DrasiWake:DevEnvironment:OpenClaw:AuthToken"] = "token"
            }),
            fixture.Root);
        var absoluteDrasiPath = Path.Combine(fixture.Root, "absolute-drasi");
        var absoluteOpenClawPath = Path.Combine(fixture.Root, "absolute-openclaw");
        Directory.CreateDirectory(absoluteDrasiPath);
        Directory.CreateDirectory(absoluteOpenClawPath);
        File.WriteAllText(Path.Combine(absoluteDrasiPath, "docker-compose.yml"), "services: {}");
        File.WriteAllText(Path.Combine(absoluteOpenClawPath, "docker-compose.yml"), "services: {}");
        var absolute = ComposeEnvironmentOptions.FromConfiguration(
            CreateConfiguration(new Dictionary<string, string?>
            {
                ["DrasiWake:DevEnvironment:DrasiRepositoryPath"] = absoluteDrasiPath,
                ["DrasiWake:DevEnvironment:OpenClawRepositoryPath"] = absoluteOpenClawPath,
                ["DrasiWake:DevEnvironment:OpenClaw:ModelProviderKey"] = "key",
                ["DrasiWake:DevEnvironment:OpenClaw:AuthToken"] = "token"
            }),
            fixture.Root);

        Assert.Equal(Path.GetFullPath(Path.Combine(fixture.Root, "..", "drasi-server")), defaults.DrasiRepositoryPath);
        Assert.Equal(Path.GetFullPath(absoluteDrasiPath), absolute.DrasiRepositoryPath);
        Assert.Empty(new ComposeEnvironmentValidator(absolute).Validate());
    }

    [Fact]
    public void Selects_aspire_sensor_registry_only_when_end_to_end_mode_is_enabled()
    {
        using var fixture = new EnvironmentFixture();
        var defaults = ComposeEnvironmentOptions.FromConfiguration(
            CreateConfiguration(new Dictionary<string, string?>
            {
                ["DRASIWAKE_RUN_REAL_ASPIRE_SMOKE"] = "1",
                ["DRASIWAKE_RUN_REAL_DRASI_CONTRACT_TESTS"] = "1",
                ["DRASIWAKE_RUN_REAL_GATEWAY_CONTRACT_TESTS"] = "1"
            }),
            fixture.Root);
        var enabled = ComposeEnvironmentOptions.FromConfiguration(
            CreateConfiguration(new Dictionary<string, string?>
            {
                ["DrasiWake:DevEnvironment:EnableAspireEndToEnd"] = "true"
            }),
            fixture.Root);

        Assert.Null(defaults.AspireRegistryPathOverride);
        Assert.Equal(
            Path.Combine(fixture.Root, "src", "DrasiWake.Host", "contracts", "aspire-sensor-binding.yaml"),
            enabled.AspireRegistryPathOverride);
    }

    [Fact]
    public void Validates_repository_local_fixture_files()
    {
        using var fixture = new EnvironmentFixture();
        var options = new ComposeEnvironmentOptions(
            fixture.Root,
            fixture.OpenClawPath,
            fixture.OpenClawPath,
            "provider-key",
            "gateway-token");
        File.Delete(options.DrasiServerConfigPath);

        var errors = new ComposeEnvironmentValidator(options).Validate();

        Assert.Contains(errors, error => error.Contains("Drasi server config", StringComparison.Ordinal));
    }

    private static IConfiguration CreateConfiguration(IReadOnlyDictionary<string, string?> values) =>
        new ConfigurationBuilder().AddInMemoryCollection(values).Build();

    private sealed class EnvironmentFixture : IDisposable
    {
        private readonly DirectoryInfo _directory = Directory.CreateTempSubdirectory("drasiwake-local-env-");

        public EnvironmentFixture()
        {
            var options = new ComposeEnvironmentOptions(Root, Root, Root, "", "");
            Directory.CreateDirectory(options.DrasiFixtureConfigPath);
            Directory.CreateDirectory(Path.GetDirectoryName(options.OpenClawMetaSkillPath)!);
            File.WriteAllText(options.DrasiComposeFilePath, "services: {}\n");
            File.WriteAllText(options.DrasiComposeOverridePath, "services: {}\n");
            File.WriteAllText(options.DrasiServerConfigPath, "apiVersion: drasi.io/v1\n");
            File.WriteAllText(options.OpenClawComposeFilePath, "services: {}\n");
            File.WriteAllText(options.OpenClawMetaSkillPath, "---\nname: test\nkind: meta\n---\n");
        }

        public string Root => _directory.FullName;

        public string OpenClawPath
        {
            get
            {
                var path = Path.Combine(Root, "openclaw");
                Directory.CreateDirectory(path);
                File.WriteAllText(Path.Combine(path, "docker-compose.yml"), "services: {}");
                return path;
            }
        }

        public void Dispose() => _directory.Delete(recursive: true);
    }
}