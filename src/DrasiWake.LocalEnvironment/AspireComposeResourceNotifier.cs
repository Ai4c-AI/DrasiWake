using Aspire.Hosting.ApplicationModel;

namespace DrasiWake.LocalEnvironment;

public sealed class AspireComposeResourceNotifier(ResourceNotificationService notifications)
    : IComposeResourceNotifier
{
    public Task MarkReadyAsync(ComposeStackResource resource) =>
        notifications.PublishUpdateAsync(resource, snapshot => snapshot with
        {
            State = KnownResourceStates.Running,
            StartTimeStamp = DateTime.UtcNow
        });

    public Task MarkStoppedAsync(ComposeStackResource resource) =>
        notifications.PublishUpdateAsync(resource, snapshot => snapshot with
        {
            State = KnownResourceStates.Finished,
            StopTimeStamp = DateTime.UtcNow
        });
}