using Microsoft.Extensions.Hosting;
using DrasiWake.Persistence.SonnetDB;
using Microsoft.EntityFrameworkCore;

namespace DrasiWake.Host;

public sealed class HostStartupValidator(
    DrasiWakeHostSettings settings,
    DrasiWake.Core.Contracts.ContractRegistryLoader registryLoader,
    DrasiWake.Core.Contracts.ContractRegistryManager registryManager,
    IDbContextFactory<BridgeDbContext> contextFactory) : IHostedService, IDisposable
{
    private FileStream? databaseLease;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        settings.Validate();
        var candidate = await registryLoader.LoadCandidateAsync(settings.RegistryPath, cancellationToken);
        if (!candidate.IsValid || candidate.Registry is null)
        {
            var errors = string.Join("; ", candidate.Errors.Select(error => error.Code));
            throw new InvalidOperationException($"Contract registry is invalid: {errors}");
        }

        var maximumBindingRetryAge = candidate.Registry.Bindings.Max(binding => binding.Retry.MaxAge);
        if (settings.OpenClaw.GatewayIdempotencyRetention < maximumBindingRetryAge)
        {
            throw new InvalidOperationException("Gateway idempotency retention must cover every binding's maximum outbox retry age.");
        }

        if (!registryManager.TryActivate(candidate))
            throw new InvalidOperationException("Contract registry activation failed.");

        Directory.CreateDirectory(settings.DatabasePath);
        var leasePath = Path.Combine(settings.DatabasePath, ".drasiwake.owner.lock");
        try
        {
            databaseLease = new FileStream(leasePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        }
        catch (IOException exception)
        {
            throw new InvalidOperationException("Another active DrasiWake host owns this database directory.", exception);
        }

        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken);
        await context.Database.MigrateAsync(cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        Dispose();
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        databaseLease?.Dispose();
        databaseLease = null;
    }
}