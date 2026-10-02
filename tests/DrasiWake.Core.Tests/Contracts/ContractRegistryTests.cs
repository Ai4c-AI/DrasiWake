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
        Assert.Equal("per-query", Assert.Single(validCandidate.Registry.Bindings).SessionScope);
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

    private static string Fixture(string name) =>
        Path.Combine(AppContext.BaseDirectory, "Contracts", "Fixtures", name);
}