namespace DrasiWake.Host.Health;

/// <summary>
/// Process-wide readiness marker for leader-epoch recovery. The scoped
/// <see cref="DrasiWake.Core.Pipeline.RecoveryCoordinator"/> instance only exists inside a leader
/// epoch scope, so the runtime reports recovery completion through this singleton instead.
/// </summary>
public sealed class RecoveryReadiness
{
    private int initialized;

    public bool IsInitialized => Volatile.Read(ref initialized) != 0;

    public void MarkInitialized(bool value) => Volatile.Write(ref initialized, value ? 1 : 0);
}
