using Microsoft.Extensions.Hosting;

namespace DrasiWake.LocalEnvironment;

public sealed class ComposeEnvironmentLifecycleService(
    IComposeEnvironmentRuntime runtime) : IHostedLifecycleService
{
    private bool _started;

    public async Task StartingAsync(CancellationToken cancellationToken)
    {
        try
        {
            await runtime.StartAsync(cancellationToken);
            _started = true;
        }
        catch (Exception startException)
        {
            try
            {
                await runtime.StopAsync(CancellationToken.None);
            }
            catch (Exception rollbackException)
            {
                throw new AggregateException(
                    "Compose startup failed and rollback also failed.",
                    startException,
                    rollbackException);
            }

            throw;
        }
    }

    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public async Task StoppedAsync(CancellationToken cancellationToken)
    {
        if (!_started)
        {
            return;
        }

        try
        {
            await runtime.StopAsync(CancellationToken.None);
        }
        finally
        {
            _started = false;
        }
    }
}