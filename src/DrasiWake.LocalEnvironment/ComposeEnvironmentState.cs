namespace DrasiWake.LocalEnvironment;

public sealed class ComposeEnvironmentState
{
    public Uri? DrasiServerUri { get; private set; }

    public Uri? OpenClawBaseAddress { get; private set; }

    public void SetAddresses(Uri drasiServerUri, Uri openClawBaseAddress)
    {
        DrasiServerUri = drasiServerUri ?? throw new ArgumentNullException(nameof(drasiServerUri));
        OpenClawBaseAddress = openClawBaseAddress ?? throw new ArgumentNullException(nameof(openClawBaseAddress));
    }

    public void Clear()
    {
        DrasiServerUri = null;
        OpenClawBaseAddress = null;
    }
}