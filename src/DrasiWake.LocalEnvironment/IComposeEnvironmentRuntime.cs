namespace DrasiWake.LocalEnvironment;

public interface IComposeEnvironmentRuntime
{
    Task StartAsync(CancellationToken cancellationToken);

    Task StopAsync(CancellationToken cancellationToken);
}