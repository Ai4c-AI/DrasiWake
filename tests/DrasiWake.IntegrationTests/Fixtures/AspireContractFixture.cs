using Aspire.Hosting.Testing;
using Aspire.Hosting;
using DrasiWake.LocalEnvironment;
using Microsoft.Extensions.DependencyInjection;

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class AspireContractCollection : ICollectionFixture<AspireContractFixture>
{
    public const string Name = "Aspire real service contracts";
}

public sealed class AspireContractFixture : IAsyncLifetime
{
    private DistributedApplication? _application;
    private IComposeEnvironmentRuntime? _runtime;

    public ComposeEnvironmentState? State { get; private set; }

    public ComposeEnvironmentOptions? Options { get; private set; }

    public async ValueTask InitializeAsync()
    {
        var runDrasi = Environment.GetEnvironmentVariable("DRASIWAKE_RUN_REAL_DRASI_CONTRACT_TESTS") == "1";
        var runGateway = Environment.GetEnvironmentVariable("DRASIWAKE_RUN_REAL_GATEWAY_CONTRACT_TESTS") == "1";
        var runSmoke = Environment.GetEnvironmentVariable("DRASIWAKE_RUN_REAL_ASPIRE_SMOKE") == "1";
        if (!runDrasi && !runGateway && !runSmoke)
        {
            return;
        }

        const string authTokenVariable = "DrasiWake__DevEnvironment__OpenClaw__AuthToken";
        var previousAuthToken = Environment.GetEnvironmentVariable(authTokenVariable);
        var useTemporarySmokeToken = runSmoke && !runDrasi && !runGateway &&
            string.IsNullOrWhiteSpace(previousAuthToken);
        if (useTemporarySmokeToken)
        {
            Environment.SetEnvironmentVariable(
                authTokenVariable,
                Convert.ToHexString(System.Security.Cryptography.RandomNumberGenerator.GetBytes(32)));
        }

        try
        {
            var builder = await DistributedApplicationTestingBuilder
                .CreateAsync<Projects.DrasiWake_AppHost>(TestContext.Current.CancellationToken);
            _application = await builder.BuildAsync();
            _runtime = _application.Services.GetRequiredService<IComposeEnvironmentRuntime>();
            await AppHostRunGuard.RunWithCleanupAsync(
                () => _application.StartAsync(TestContext.Current.CancellationToken),
                cancellationToken => _application.StopAsync(cancellationToken),
                cancellationToken => _runtime.StopAsync(cancellationToken));

            State = _application.Services.GetRequiredService<ComposeEnvironmentState>();
            Options = _application.Services.GetRequiredService<ComposeEnvironmentOptions>();
        }
        finally
        {
            if (useTemporarySmokeToken)
            {
                Environment.SetEnvironmentVariable(authTokenVariable, previousAuthToken);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_application is null)
        {
            return;
        }

        try
        {
            await _application.StopAsync(CancellationToken.None);
        }
        finally
        {
            try
            {
                if (_runtime is not null)
                {
                    await _runtime.StopAsync(CancellationToken.None);
                }
            }
            finally
            {
                await _application.DisposeAsync();
            }
        }
    }
}