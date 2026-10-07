using Microsoft.Extensions.Hosting;
using DrasiWake.Persistence.SonnetDB;
using Microsoft.EntityFrameworkCore;

namespace DrasiWake.Host;

public sealed class HostStartupValidator(
    DrasiWakeHostSettings settings,
    DrasiWake.Core.Contracts.ContractRegistryLoader registryLoader,
    DrasiWake.Core.Contracts.ContractRegistryManager registryManager,
    IDbContextFactory<BridgeDbContext> contextFactory) : IHostedLifecycleService, IDisposable
{
    private readonly SemaphoreSlim startupGate = new(1, 1);
    private FileStream? databaseLease;
    private bool started;

    public Task StartingAsync(CancellationToken cancellationToken) => StartAsync(cancellationToken);

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await startupGate.WaitAsync(cancellationToken);
        try
        {
            if (started)
                return;

            settings.Validate();
            if (settings.Cluster.Mode == RaftClusterMode.Cluster)
            {
                using var certificate = settings.Cluster.LoadServerCertificate();
                settings.Cluster.ValidateServerCertificateNames(certificate);
            }

            var candidate = await registryLoader.LoadCandidateAsync(settings.RegistryPath, cancellationToken);
            if (!candidate.IsValid || candidate.Registry is null)
            {
                var errors = string.Join("; ", candidate.Errors.Select(error => error.Code));
                throw new InvalidOperationException($"Contract registry is invalid: {errors}");
            }

            foreach (var binding in candidate.Registry.Bindings)
            {
                if (!settings.OpenClawTargets.TryGetValue(binding.OpenClawTarget, out var target))
                {
                    throw new InvalidOperationException(
                        $"Binding '{binding.Id}' references unknown OpenClaw target '{binding.OpenClawTarget}'.");
                }

                if (target.GatewayIdempotencyRetention < binding.Retry.MaxAge)
                {
                    throw new InvalidOperationException(
                        $"OpenClaw target '{binding.OpenClawTarget}' idempotency retention must cover its bindings' maximum outbox retry age.");
                }
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
            started = true;
        }
        catch
        {
            databaseLease?.Dispose();
            databaseLease = null;
            throw;
        }
        finally
        {
            startupGate.Release();
        }
    }

    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

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