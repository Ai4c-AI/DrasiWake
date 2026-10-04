using System.Collections.Concurrent;
using DrasiWake.Adapters.OpenClaw;
using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Domain;

namespace DrasiWake.Host;

public sealed class TargetRoutedWakeSink : IWakeSink, IDisposable
{
    private readonly IHttpClientFactory httpClientFactory;
    private readonly OpenClawOptions options;
    private readonly IReadOnlyDictionary<string, OpenClawTargetOptions> targets;
    private readonly ConcurrentDictionary<string, Lazy<OpenClawMetaInvocationClient>> clients =
        new(StringComparer.Ordinal);

    public TargetRoutedWakeSink(
        IHttpClientFactory httpClientFactory,
        OpenClawOptions options,
        IReadOnlyDictionary<string, OpenClawTargetOptions> targets)
    {
        ArgumentNullException.ThrowIfNull(httpClientFactory);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(targets);

        this.httpClientFactory = httpClientFactory;
        this.options = options;
        this.options.Validate();
        var targetCopy = new Dictionary<string, OpenClawTargetOptions>(StringComparer.Ordinal);
        foreach (var (targetName, targetOptions) in targets)
        {
            targetOptions.Validate(targetName);
            targetCopy.Add(targetName, targetOptions);
        }
        this.targets = targetCopy;
    }

    public ValueTask<WakeAcceptance> InvokeAsync(WakeRequest request, CancellationToken cancellationToken)
        => Resolve(request).InvokeAsync(request, cancellationToken);

    public ValueTask<WakeExecutionStatus?> GetStatusAsync(WakeRequest request, CancellationToken cancellationToken)
        => Resolve(request).GetStatusAsync(request, cancellationToken);

    public void Dispose()
    {
        foreach (var client in clients.Values)
        {
            if (client.IsValueCreated)
                client.Value.Dispose();
        }
    }

    private OpenClawMetaInvocationClient Resolve(WakeRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        var targetName = request.OpenClawTarget;
        if (string.IsNullOrWhiteSpace(targetName) || !targets.TryGetValue(targetName, out var targetOptions))
        {
            throw new InvalidOperationException(
                $"Wake request '{request.IdempotencyKey}' references unknown OpenClaw target '{targetName}'.");
        }

        return clients.GetOrAdd(
            targetName,
            name => new Lazy<OpenClawMetaInvocationClient>(
                () => new OpenClawMetaInvocationClient(
                    httpClientFactory.CreateClient(name),
                    options,
                    targetOptions),
                LazyThreadSafetyMode.ExecutionAndPublication)).Value;
    }
}
