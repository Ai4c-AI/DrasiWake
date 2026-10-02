namespace DrasiWake.Core.Contracts;

public sealed class ContractRegistryManager
{
    private ContractRegistry _active = ContractRegistry.Empty;

    public ContractRegistry Active => Volatile.Read(ref _active);

    public bool TryActivate(ContractRegistryCandidate candidate)
    {
        if (!candidate.IsValid || candidate.Registry is null)
        {
            return false;
        }

        Interlocked.Exchange(ref _active, candidate.Registry);
        return true;
    }
}