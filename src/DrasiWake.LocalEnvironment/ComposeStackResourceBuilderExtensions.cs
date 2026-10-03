using Aspire.Hosting.ApplicationModel;
using DrasiWake.LocalEnvironment;

namespace Aspire.Hosting;

public static class ComposeStackResourceBuilderExtensions
{
    public static IResourceBuilder<ComposeStackResource> AddComposeStack(
        this IDistributedApplicationBuilder builder,
        string name)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        return builder.AddResource(new ComposeStackResource(name))
            .ExcludeFromManifest()
            .WithInitialState(new CustomResourceSnapshot
            {
                ResourceType = "Docker Compose stack",
                CreationTimeStamp = DateTime.UtcNow,
                State = KnownResourceStates.NotStarted,
                Properties = []
            });
    }
}