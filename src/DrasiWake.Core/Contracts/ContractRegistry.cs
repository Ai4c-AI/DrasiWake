namespace DrasiWake.Core.Contracts;

public sealed record ContractRegistry(string Version, IReadOnlyList<BridgeBinding> Bindings)
{
    public static ContractRegistry Empty { get; } = new("0.0.0", Array.Empty<BridgeBinding>());
}

public sealed record ContractValidationError(string Code, string Message, string? BindingId = null);

public sealed record ContractRegistryCandidate(
    ContractRegistry? Registry,
    IReadOnlyList<ContractValidationError> Errors)
{
    public bool IsValid => Registry is not null && Errors.Count == 0;
}