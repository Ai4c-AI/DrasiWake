using DrasiWake.Host;
using DrasiWake.Core.Contracts;
using DrasiWake.Core.Domain;
using DrasiWake.Core.Pipeline;
using DrasiWake.LocalEnvironment;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.Text.Json.Nodes;

namespace DrasiWake.IntegrationTests;

public sealed class HostStartupTests
{
    [Fact]
    public async Task Invalid_registry_fails_host_startup()
    {
        using var host = DrasiWakeHostBuilder.CreateHost(CreateConfiguration(new Dictionary<string, string?>
        {
            ["DrasiWake:Registry:Path"] = Path.Combine(Path.GetTempPath(), $"missing-{Guid.NewGuid():N}.yaml")
        }));

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => host.StartAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public void Named_target_environment_keys_bind_host_gateway_and_token()
    {
        var configuration = CreateConfiguration(new Dictionary<string, string?>
        {
            ["DrasiWake:Drasi:ServerUri"] = "http://127.0.0.1:45123/",
            ["DrasiWake:OpenClaw:Targets:sample-gateway:BaseAddress"] = "http://127.0.0.1:45678/",
            ["DrasiWake:OpenClaw:Targets:sample-gateway:BearerToken"] = "gateway-test-token"
        });

        var settings = DrasiWakeHostSettings.FromConfiguration(configuration);

        Assert.Equal(new Uri("http://127.0.0.1:45123/"), settings.Drasi.ServerUri);
        Assert.Equal(new Uri("http://127.0.0.1:45678/"), settings.OpenClawTargets["sample-gateway"].BaseAddress);
        Assert.Equal("gateway-test-token", settings.OpenClawTargets["sample-gateway"].BearerToken);
    }

    [Fact]
    public void Named_gateway_target_settings_bind_by_logical_name()
    {
        var configuration = CreateConfiguration(new Dictionary<string, string?>
        {
            ["DrasiWake:OpenClaw:Targets:sensor-gateway:BaseAddress"] = "https://sensor-gateway.test/",
            ["DrasiWake:OpenClaw:Targets:sensor-gateway:BearerToken"] = "sensor-token",
            ["DrasiWake:OpenClaw:Targets:sensor-gateway:GatewayIdempotencyRetention"] = "15.00:00:00"
        });

        var settings = DrasiWakeHostSettings.FromConfiguration(configuration);
        var targetsProperty = settings.GetType().GetProperty("OpenClawTargets");
        Assert.NotNull(targetsProperty);
        var targets = Assert.IsAssignableFrom<System.Collections.IDictionary>(targetsProperty.GetValue(settings));
        var target = targets["sensor-gateway"];
        Assert.NotNull(target);
        var targetType = target.GetType();
        Assert.Equal(
            new Uri("https://sensor-gateway.test/"),
            targetType.GetProperty("BaseAddress")!.GetValue(target));
        Assert.Equal("sensor-token", targetType.GetProperty("BearerToken")!.GetValue(target));
        Assert.Equal(
            TimeSpan.FromDays(15),
            targetType.GetProperty("GatewayIdempotencyRetention")!.GetValue(target));
    }

    [Theory]
    [InlineData("ftp://gateway.test/")]
    [InlineData("/relative")]
    public void Invalid_named_target_address_is_rejected(string address)
    {
        var exception = Assert.Throws<InvalidOperationException>(() =>
            DrasiWakeHostSettings.FromConfiguration(CreateConfiguration(new Dictionary<string, string?>
            {
                ["DrasiWake:OpenClaw:Targets:sample-gateway:BaseAddress"] = address
            })));

        Assert.Contains("DrasiWake:OpenClaw:Targets:sample-gateway:BaseAddress", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Malformed_named_target_credential_is_rejected_without_echoing_secret()
    {
        const string token = "token with spaces";
        var exception = Assert.Throws<InvalidOperationException>(() =>
            DrasiWakeHostSettings.FromConfiguration(CreateConfiguration(new Dictionary<string, string?>
            {
                ["DrasiWake:OpenClaw:Targets:sample-gateway:BearerToken"] = token
            })));

        Assert.Contains("BearerToken", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(token, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Named_target_requires_idempotency_retention_configuration()
    {
        var exception = Assert.Throws<InvalidOperationException>(() =>
            DrasiWakeHostSettings.FromConfiguration(CreateConfiguration(new Dictionary<string, string?>
            {
                ["DrasiWake:OpenClaw:Targets:sample-gateway:GatewayIdempotencyRetention"] = null
            })));

        Assert.Contains("GatewayIdempotencyRetention", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Binding_with_unknown_target_fails_host_startup()
    {
        var (root, registryPath) = await CreateSampleRegistryWithTargetAsync("missing-gateway");
        var databasePath = Path.Combine(root, "database");
        using var host = DrasiWakeHostBuilder.CreateHost(CreateConfiguration(new Dictionary<string, string?>
        {
            ["DrasiWake:Registry:Path"] = registryPath,
            ["DrasiWake:Database:Path"] = databasePath
        }));

        try
        {
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => host.Services.GetRequiredService<HostStartupValidator>()
                    .StartAsync(TestContext.Current.CancellationToken));

            Assert.Contains("missing-gateway", exception.Message, StringComparison.Ordinal);
            Assert.False(Directory.Exists(databasePath));
        }
        finally
        {
            host.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Target_retention_shorter_than_binding_retry_age_fails_host_startup()
    {
        var (root, registryPath) = await CreateSampleRegistryWithTargetAsync("sample-gateway");
        var databasePath = Path.Combine(root, "database");
        using var host = DrasiWakeHostBuilder.CreateHost(CreateConfiguration(new Dictionary<string, string?>
        {
            ["DrasiWake:Registry:Path"] = registryPath,
            ["DrasiWake:Database:Path"] = databasePath,
            ["DrasiWake:OpenClaw:Targets:sample-gateway:GatewayIdempotencyRetention"] = "01:00:00"
        }));

        try
        {
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => host.Services.GetRequiredService<HostStartupValidator>()
                    .StartAsync(TestContext.Current.CancellationToken));

            Assert.Contains("sample-gateway", exception.Message, StringComparison.Ordinal);
            Assert.Contains("retention", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.False(Directory.Exists(databasePath));
        }
        finally
        {
            host.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Aspire_sensor_binding_matches_fixture_and_accepts_sensor_rows()
    {
        var repositoryRoot = RepositoryRootLocator.Find(AppContext.BaseDirectory);
        var registryPath = Path.Combine(
            repositoryRoot,
            "src",
            "DrasiWake.Host",
            "contracts",
            "aspire-sensor-binding.yaml");
        var candidate = await new ContractRegistryLoader().LoadCandidateAsync(
            registryPath,
            TestContext.Current.CancellationToken);

        Assert.True(candidate.IsValid, string.Join("; ", candidate.Errors.Select(error => error.Message)));
        var binding = Assert.Single(candidate.Registry!.Bindings);
        Assert.Equal("drasi-server", binding.Source);
        Assert.Equal(new Uri("http://127.0.0.1:8080/"), binding.Server);
        Assert.Equal("drasiwake-sensor-monitor", binding.InstanceId);
        Assert.Equal("sensor-readings", binding.QueryId);
        Assert.Equal("drasiwake-sensor-reading-summary", binding.MetaSkill);

        var renderer = new WakePayloadRenderer();
        var query = new QueryIdentity(binding.Server, binding.InstanceId, binding.QueryId!);
        var rendered = renderer.Render(
            binding,
            query,
            "aspire-e2e-test",
            [JsonNode.Parse("""{"SensorId":"sensor-1","Temperature":21.5,"Humidity":45.2}""")]);

        Assert.Single((JsonArray)rendered.Input["facts"]!);
        Assert.Throws<WakePayloadRejectedException>(() => renderer.Render(
            binding,
            query,
            "aspire-e2e-test",
            [JsonNode.Parse("""{"SensorId":"sensor-1","Temperature":21.5}""")]));
    }

    [Theory]
    [InlineData("DrasiWake:WorkerCount", "not-a-number")]
    [InlineData("DrasiWake:DispatchPollInterval", "not-a-duration")]
    public void Invalid_numeric_configuration_is_rejected(string key, string value)
    {
        var exception = Assert.Throws<InvalidOperationException>(() =>
            DrasiWakeHostBuilder.CreateHost(CreateConfiguration(new Dictionary<string, string?>
            {
                [key] = value
            })));

        Assert.Contains(key, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Valid_registry_migrates_database_and_database_directory_has_single_owner()
    {
        var root = Path.Combine(Path.GetTempPath(), $"DrasiWake-host-{Guid.NewGuid():N}");
        var registryPath = Path.Combine(root, "contracts", "bindings.yaml");
        Directory.CreateDirectory(Path.GetDirectoryName(registryPath)!);
        await File.WriteAllTextAsync(Path.Combine(root, "contracts", "facts.schema.json"),
            "{\"type\":\"object\"}", TestContext.Current.CancellationToken);
        await File.WriteAllTextAsync(registryPath, SampleRegistry, TestContext.Current.CancellationToken);
        var candidate = await new ContractRegistryLoader().LoadCandidateAsync(registryPath, TestContext.Current.CancellationToken);
        Assert.True(candidate.IsValid, string.Join("; ", candidate.Errors.Select(error => error.Message)));
        var configuration = CreateConfiguration(new Dictionary<string, string?>
        {
            ["DrasiWake:Registry:Path"] = registryPath,
            ["DrasiWake:Database:Path"] = Path.Combine(root, "database")
        });

        var firstHost = DrasiWakeHostBuilder.CreateHost(configuration);
        var secondHost = DrasiWakeHostBuilder.CreateHost(configuration);
        var firstValidator = firstHost.Services.GetRequiredService<HostStartupValidator>();
        var secondValidator = secondHost.Services.GetRequiredService<HostStartupValidator>();
        try
        {
            await firstValidator.StartAsync(TestContext.Current.CancellationToken);
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => secondValidator.StartAsync(TestContext.Current.CancellationToken));
            await firstValidator.StopAsync(CancellationToken.None);
            await secondValidator.StartAsync(TestContext.Current.CancellationToken);
        }
        finally
        {
            firstHost.Dispose();
            secondHost.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }

    private static IConfiguration CreateConfiguration(IReadOnlyDictionary<string, string?> overrides)
    {
        var values = new Dictionary<string, string?>
        {
            ["DrasiWake:Drasi:ServerUri"] = "http://127.0.0.1:8080",
            ["DrasiWake:OpenClaw:Targets:sample-gateway:BaseAddress"] = "http://127.0.0.1:8081",
            ["DrasiWake:OpenClaw:Targets:sample-gateway:GatewayIdempotencyRetention"] = "30.00:00:00",
            ["DrasiWake:Database:Path"] = Path.Combine(Path.GetTempPath(), $"DrasiWake-test-{Guid.NewGuid():N}"),
            ["DrasiWake:Registry:Path"] = Path.Combine(Path.GetTempPath(), $"missing-{Guid.NewGuid():N}.yaml")
        };
        foreach (var entry in overrides)
            values[entry.Key] = entry.Value;
        return new ConfigurationBuilder().AddInMemoryCollection(values).Build();
    }

    private static async Task<(string Root, string RegistryPath)> CreateSampleRegistryWithTargetAsync(string target)
    {
        var repositoryRoot = RepositoryRootLocator.Find(AppContext.BaseDirectory);
        var sourceDirectory = Path.Combine(repositoryRoot, "src", "DrasiWake.Host", "contracts");
        var root = Path.Combine(Path.GetTempPath(), $"DrasiWake-target-startup-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        var registryPath = Path.Combine(root, "bindings.yaml");
        var yaml = await File.ReadAllTextAsync(Path.Combine(sourceDirectory, "sample-binding.yaml"), TestContext.Current.CancellationToken);
        yaml = yaml.Replace("openClawTarget: sample-gateway", $"openClawTarget: {target}", StringComparison.Ordinal);
        await File.WriteAllTextAsync(registryPath, yaml, TestContext.Current.CancellationToken);
        File.Copy(Path.Combine(sourceDirectory, "sample-facts.schema.json"), Path.Combine(root, "sample-facts.schema.json"));
        return (root, registryPath);
    }

    private static readonly string SampleRegistry = string.Join(Environment.NewLine,
    [
        "version: 1.0.0",
        "bindings:",
        "  - id: test-binding",
        "    source: drasi-server",
        "    server: http://127.0.0.1:8080",
        "    instanceId: default",
        "    queryId: orders",
        "    deliveryMode: converge-latest",
        "    sessionScope: singleton",
        "    openClawTarget: sample-gateway",
        "    metaSkill: triage-order",
        "    contractVersion: 1.0.0",
        "    factSchemaPath: facts.schema.json",
        "    maxPayloadBytes: 32768",
        "    retry:",
        "      maxAttempts: 3",
        "      maxAgeSeconds: 86400",
        "    rateLimit:",
        "      permitLimit: 10",
        "      windowMilliseconds: 1000"
    ]);
}