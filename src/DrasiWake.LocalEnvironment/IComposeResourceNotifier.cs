namespace DrasiWake.LocalEnvironment;

public interface IComposeResourceNotifier
{
    Task MarkReadyAsync(ComposeStackResource resource);

    Task MarkStoppedAsync(ComposeStackResource resource);
}