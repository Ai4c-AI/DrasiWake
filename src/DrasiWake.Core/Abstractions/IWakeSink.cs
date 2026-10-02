using DrasiWake.Core.Domain;

namespace DrasiWake.Core.Abstractions;

public interface IWakeSink
{
    ValueTask<WakeAcceptance> InvokeAsync(WakeRequest request, CancellationToken cancellationToken);
    ValueTask<WakeExecutionStatus?> GetStatusAsync(string invocationId, CancellationToken cancellationToken);
}