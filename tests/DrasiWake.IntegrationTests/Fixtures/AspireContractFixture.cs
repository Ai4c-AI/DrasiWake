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

    public ComposeEnvironmentState? State { get; private set; }

    public ComposeEnvironmentOptions? Options { get; private set; }

    public async ValueTask InitializeAsync()
    {
        var runDrasi = Environment.GetEnvironmentVariable("DRASIWAKE_RUN_REAL_DRASI_CONTRACT_TESTS") == "1";
        var runGateway = Environment.GetEnvironmentVariable("DRASIWAKE_RUN_REAL_GATEWAY_CONTRACT_TESTS") == "1";
        if (!runDrasi && !runGateway)
        {
            return;
        }

        var builder = await DistributedApplicationTestingBuilder
            .CreateAsync<Projects.DrasiWake_AppHost>(TestContext.Current.CancellationToken);
        _application = await builder.BuildAsync();
        await AppHostRunGuard.RunWithCleanupAsync(
            () => _application.StartAsync(TestContext.Current.CancellationToken),
            cancellationToken => _application.StopAsync(cancellationToken),
            cancellationToken => _application.Services
                .GetRequiredService<IComposeEnvironmentRuntime>()
                .StopAsync(cancellationToken));

        State = _application.Services.GetRequiredService<ComposeEnvironmentState>();
        Options = _application.Services.GetRequiredService<ComposeEnvironmentOptions>();
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
                await _application.Services.GetRequiredService<IComposeEnvironmentRuntime>()
                    .StopAsync(CancellationToken.None);
            }
            finally
            {
                await _application.DisposeAsync();
            }
        }
    }
}