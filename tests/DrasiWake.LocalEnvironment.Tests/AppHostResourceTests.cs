using Aspire.Hosting.ApplicationModel;

public sealed class AppHostResourceTests
{
    [Fact]
    public async Task AppHost_models_both_external_compose_stacks()
    {
        var builder = await DistributedApplicationTestingBuilder
            .CreateAsync<Projects.DrasiWake_AppHost>(TestContext.Current.CancellationToken);

        var drasi = Assert.Single(builder.Resources, resource => resource.Name == "drasi-compose");
        var openClaw = Assert.Single(builder.Resources, resource => resource.Name == "openclaw-compose");
        var host = Assert.Single(builder.Resources, resource => resource.Name == "drasiwake-host");

        var dependencies = host.Annotations.OfType<WaitAnnotation>().Select(annotation => annotation.Resource.Name);
        Assert.Equal([drasi.Name, openClaw.Name], dependencies);
    }
}