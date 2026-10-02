using DrasiWake.Host;
using Microsoft.Extensions.Hosting;

using var host = DrasiWakeHostBuilder.CreateHost(args);
await host.RunAsync();
