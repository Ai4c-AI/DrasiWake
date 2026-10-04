using DrasiWake.Core.Contracts;

namespace DrasiWake.Core.Tests.Contracts;

public sealed class ContractRegistryTests
{
    [Fact]
    public async Task Invalid_candidate_keeps_previous_registry_active()
    {
        var loader = new ContractRegistryLoader();
        var manager = new ContractRegistryManager();

        var validCandidate = await loader.LoadCandidateAsync(Fixture("valid-default.yaml"), TestContext.Current.CancellationToken);
        Assert.Empty(validCandidate.Errors);
        Assert.NotNull(validCandidate.Registry);
        var binding = Assert.Single(validCandidate.Registry.Bindings);
        Assert.Equal("per-query", binding.SessionScope);
        Assert.True(Path.IsPathFullyQualified(binding.Contract.FactSchemaPath));
        Assert.True(File.Exists(binding.Contract.FactSchemaPath));
        Assert.True(manager.TryActivate(validCandidate));
        var previous = manager.Active;

        var candidate = await loader.LoadCandidateAsync(Fixture("invalid-shared-identity.yaml"), TestContext.Current.CancellationToken);

        Assert.Contains(candidate.Errors, error => error.Code == "identity.required");
        Assert.False(manager.TryActivate(candidate));
        Assert.Same(previous, manager.Active);
    }

    [Fact]
    public async Task Trigger_phrase_conflict_rejects_candidate_without_changing_active_registry()
    {
        var loader = new ContractRegistryLoader();
        var manager = new ContractRegistryManager();
        var previous = manager.Active;

        var candidate = await loader.LoadCandidateAsync(Fixture("invalid-trigger-conflict.yaml"), TestContext.Current.CancellationToken);

        Assert.Contains(candidate.Errors, error => error.Code == "trigger.conflict");
        Assert.False(manager.TryActivate(candidate));
        Assert.Same(previous, manager.Active);
    }

    [Fact]
    public async Task Binding_without_openclaw_target_is_rejected()
    {
        var candidate = await LoadCandidateWithTargetAsync(null);

        Assert.Contains(candidate.Errors, error => error.Code == "routing.target_required");
    }

    [Fact]
    public async Task Binding_with_blank_openclaw_target_is_rejected()
    {
        var candidate = await LoadCandidateWithTargetAsync("   ");

        Assert.Contains(candidate.Errors, error => error.Code == "routing.target_required");
    }

    [Fact]
    public async Task Binding_target_is_exposed_on_loaded_binding()
    {
        var candidate = await LoadCandidateWithTargetAsync("sample-gateway");

        Assert.True(candidate.IsValid, string.Join("; ", candidate.Errors.Select(error => error.Code)));
        var binding = Assert.Single(candidate.Registry!.Bindings);
        var targetProperty = typeof(BridgeBinding).GetProperty("OpenClawTarget");
        Assert.NotNull(targetProperty);
        Assert.Equal("sample-gateway", targetProperty.GetValue(binding));
    }

    private static string Fixture(string name) =>
        Path.Combine(AppContext.BaseDirectory, "Contracts", "Fixtures", name);

    private static async Task<ContractRegistryCandidate> LoadCandidateWithTargetAsync(string? target)
    {
        var source = await File.ReadAllTextAsync(Fixture("valid-default.yaml"), TestContext.Current.CancellationToken);
        var yaml = target is null
            ? source.Replace("    openClawTarget: sample-gateway", string.Empty, StringComparison.Ordinal)
            : source.Replace(
                "    openClawTarget: sample-gateway",
                $"    openClawTarget: \"{target}\"",
                StringComparison.Ordinal);
        var root = Path.Combine(Path.GetTempPath(), $"DrasiWake-contract-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "valid-binding.yaml"), yaml, TestContext.Current.CancellationToken);
            File.Copy(Fixture("order.schema.json"), Path.Combine(root, "order.schema.json"));
            return await new ContractRegistryLoader().LoadCandidateAsync(
                Path.Combine(root, "valid-binding.yaml"),
                TestContext.Current.CancellationToken);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }
}