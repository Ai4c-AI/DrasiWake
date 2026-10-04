using DrasiWake.Adapters.OpenClaw;
using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Domain;

namespace DrasiWake.Host;

public sealed class TargetRoutedWakeSink : IWakeSink
{
    private readonly IHttpClientFactory httpClientFactory;
    private readonly OpenClawOptions options;
    private readonly IReadOnlyDictionary<string, OpenClawTargetOptions> targets;

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

    public async ValueTask<WakeAcceptance> InvokeAsync(WakeRequest request, CancellationToken cancellationToken)
    {
        using var client = Resolve(request);
        return await client.InvokeAsync(request, cancellationToken);
    }

    public async ValueTask<WakeExecutionStatus?> GetStatusAsync(
        WakeRequest request,
        CancellationToken cancellationToken)
    {
        using var client = Resolve(request);
        return await client.GetStatusAsync(request, cancellationToken);
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

        return new OpenClawMetaInvocationClient(
            httpClientFactory.CreateClient(targetName),
            options,
            targetOptions);
    }
}
