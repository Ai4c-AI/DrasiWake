using DrasiWake.LocalEnvironment;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

public sealed class ComposeEnvironmentLifecycleTests
{
    [Fact]
    public async Task Compose_starts_before_resources_and_stops_after_them()
    {
        var events = new List<string>();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<IHostedService>(
            new ComposeEnvironmentLifecycleService(new RecordingComposeRuntime(events)));
        builder.Services.AddSingleton<IHostedService>(new RecordingHostedService(events));
        using var host = builder.Build();

        await host.StartAsync(TestContext.Current.CancellationToken);
        await host.StopAsync(TestContext.Current.CancellationToken);

        Assert.Equal(
            ["compose-start", "resource-start", "resource-stop", "compose-stop"],
            events);
    }

    [Fact]
    public async Task Compose_start_failure_rolls_back_before_resource_start()
    {
        var events = new List<string>();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<IHostedService>(
            new ComposeEnvironmentLifecycleService(new RecordingComposeRuntime(events, failStart: true)));
        builder.Services.AddSingleton<IHostedService>(new RecordingHostedService(events));
        using var host = builder.Build();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => host.StartAsync(TestContext.Current.CancellationToken));

        Assert.Equal(["compose-start", "compose-stop"], events);
    }

    [Fact]
    public async Task Canceled_compose_start_rolls_back()
    {
        var events = new List<string>();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<IHostedService>(
            new ComposeEnvironmentLifecycleService(new RecordingComposeRuntime(events, cancelStart: true)));
        using var host = builder.Build();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => host.StartAsync(CancellationToken.None));

        Assert.Equal(["compose-start", "compose-stop"], events);
    }

    [Fact]
    public async Task Later_host_start_failure_stops_compose_started_by_lifecycle_hook()
    {
        var events = new List<string>();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<IHostedService>(
            new ComposeEnvironmentLifecycleService(new RecordingComposeRuntime(events)));
        builder.Services.AddSingleton<IHostedService>(new FailingHostedService(events));
        using var host = builder.Build();

        var runtime = new RecordingComposeRuntime(events);
        await Assert.ThrowsAsync<InvalidOperationException>(() => AppHostRunGuard.RunWithCleanupAsync(
            () => host.RunAsync(),
            cancellationToken => host.StopAsync(cancellationToken),
            cancellationToken => runtime.StopAsync(cancellationToken)));

        Assert.Equal(["compose-start", "host-start-failed", "compose-stop"], events);
    }

    [Fact]
    public async Task Host_stop_cancellation_does_not_hide_startup_error_or_skip_fallback_cleanup()
    {
        var fallbackCalled = false;

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            AppHostRunGuard.RunWithCleanupAsync(
                () => Task.FromException(new InvalidOperationException("startup failed")),
                _ => Task.FromException(new TaskCanceledException()),
                _ =>
                {
                    fallbackCalled = true;
                    return Task.CompletedTask;
                }));

        Assert.Equal("startup failed", exception.Message);
        Assert.True(fallbackCalled);
    }

    private sealed class RecordingComposeRuntime(
        List<string> events,
        bool failStart = false,
        bool cancelStart = false) : IComposeEnvironmentRuntime
    {
        private bool _started;

        public Task StartAsync(CancellationToken cancellationToken)
        {
            events.Add("compose-start");
            _started = true;
            if (cancelStart)
            {
                return Task.FromCanceled(new CancellationToken(canceled: true));
            }

            return failStart
                ? Task.FromException(new InvalidOperationException("compose start failed"))
                : Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            if (_started)
            {
                events.Add("compose-stop");
                _started = false;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class RecordingHostedService(List<string> events) : IHostedService
    {
        public Task StartAsync(CancellationToken cancellationToken)
        {
            events.Add("resource-start");
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            events.Add("resource-stop");
            return Task.CompletedTask;
        }
    }

    private sealed class FailingHostedService(List<string> events) : IHostedService
    {
        public Task StartAsync(CancellationToken cancellationToken)
        {
            events.Add("host-start-failed");
            return Task.FromException(new InvalidOperationException("host start failed"));
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}