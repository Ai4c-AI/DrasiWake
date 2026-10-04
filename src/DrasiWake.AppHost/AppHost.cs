using Aspire.Hosting.ApplicationModel;
using DrasiWake.LocalEnvironment;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = DistributedApplication.CreateBuilder(args);

var repositoryRoot = RepositoryRootLocator.Find(AppContext.BaseDirectory);
var options = ComposeEnvironmentOptions.FromConfiguration(builder.Configuration, repositoryRoot);
var drasi = builder.AddComposeStack("drasi-compose");
var openClaw = builder.AddComposeStack("openclaw-compose");
var environmentState = new ComposeEnvironmentState();
var stacks = ComposeStackDefinition.Create(options, drasi.Resource, openClaw.Resource);

builder.Services.AddSingleton(environmentState);
builder.Services.AddSingleton(options);
builder.Services.AddSingleton<IComposeCommandExecutor, ComposeCommandExecutor>();
builder.Services.AddSingleton<IComposeResourceNotifier, AspireComposeResourceNotifier>();
builder.Services.AddSingleton<IComposeEnvironmentRuntime>(services => new ComposeEnvironmentCoordinator(
	options,
	stacks,
	services.GetRequiredService<IComposeCommandExecutor>(),
	services.GetRequiredService<IComposeResourceNotifier>(),
	environmentState));
builder.Services.AddSingleton<IHostedService>(services => new ComposeEnvironmentLifecycleService(
	services.GetRequiredService<IComposeEnvironmentRuntime>()));

var authToken = builder.AddParameter("openclaw-auth-token", () => options.AuthToken, secret: true);

builder.AddProject<Projects.DrasiWake_Host>("drasiwake-host")
	.WaitFor(drasi)
	.WaitFor(openClaw)
	.WithEnvironment(context =>
	{
		context.EnvironmentVariables["DrasiWake__Drasi__ServerUri"] =
			environmentState.DrasiServerUri?.ToString()
			?? throw new InvalidOperationException("Drasi address was not discovered before Host startup.");
		context.EnvironmentVariables["DrasiWake__OpenClaw__Targets__sensor-gateway__BaseAddress"] =
			environmentState.OpenClawBaseAddress?.ToString()
			?? throw new InvalidOperationException("OpenClaw address was not discovered before Host startup.");
		context.EnvironmentVariables["DrasiWake__OpenClaw__Targets__sensor-gateway__GatewayIdempotencyRetention"] =
			"30.00:00:00";
		if (options.AspireRegistryPathOverride is { } registryPath)
		{
			context.EnvironmentVariables["DrasiWake__Registry__Path"] = registryPath;
		}
	})
	.WithEnvironment("DrasiWake__OpenClaw__Targets__sensor-gateway__BearerToken", authToken);

await using var app = builder.Build();
var composeRuntime = app.Services.GetRequiredService<IComposeEnvironmentRuntime>();
await AppHostRunGuard.RunWithCleanupAsync(
	() => app.RunAsync(),
	cancellationToken => app.StopAsync(cancellationToken),
	cancellationToken => composeRuntime.StopAsync(cancellationToken));
