using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Contracts;
using DrasiWake.Core.Domain;

namespace DrasiWake.Core.Pipeline;

public sealed class OutboxDispatcher(
    IBridgeStore store,
    IWakeSink wakeSink,
    ContractRegistryManager registry,
    TimeProvider? timeProvider = null)
{
    private readonly TimeProvider timeProvider = timeProvider ?? TimeProvider.System;

    public async Task DispatchOneAsync(WakeOutboxItem item, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(item);
        var binding = registry.Active.Bindings.FirstOrDefault(candidate => candidate.Id == item.BindingId);
        if (binding is null)
        {
            await store.MarkDeadLetterAsync(item.Id, "binding.not_found", cancellationToken);
            return;
        }

        var request = new WakeRequest(
            item.BindingId,
            item.SessionId,
            item.Skill,
            item.Input,
            item.IdempotencyKey,
            item.ContractVersion,
            item.TraceId);

        WakeAcceptance acceptance;
        try
        {
            acceptance = await wakeSink.InvokeAsync(request, cancellationToken);
        }
        catch (WakeSinkException exception)
        {
            var reason = exception.Kind switch
            {
                WakeSinkFailureKind.ContractConflict => "gateway.idempotency_conflict",
                WakeSinkFailureKind.OutcomeUncertain => "gateway.invocation_uncertain",
                _ => "gateway.contract_failure"
            };
            await store.MarkDeadLetterAsync(item.Id, reason, cancellationToken);
            return;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            var attemptCount = item.AttemptCount + 1;
            var now = timeProvider.GetUtcNow();
            var retryExpired = now - item.CreatedAtUtc >= binding.Retry.MaxAge;
            if (attemptCount >= binding.Retry.MaxAttempts || retryExpired)
            {
                await store.MarkDeadLetterAsync(item.Id, "dispatch.retry_exhausted", cancellationToken);
                return;
            }

            var delaySeconds = Math.Min(Math.Pow(2, Math.Min(attemptCount - 1, 20)), binding.Retry.MaxAge.TotalSeconds);
            await store.MarkRetryScheduledAsync(
                item.Id,
                attemptCount,
                now.AddSeconds(delaySeconds),
                "dispatch.transient_failure",
                cancellationToken);
            return;
        }

        var checkpoint = new SnapshotCheckpoint(
            item.BindingId,
            item.SessionId,
            item.SnapshotFingerprint,
            acceptance.AcceptedAtUtc);
        try
        {
            await store.MarkAcceptedWithCheckpointAsync(item.Id, acceptance, checkpoint, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            var attemptCount = item.AttemptCount + 1;
            var now = timeProvider.GetUtcNow();
            var retryExpired = now - item.CreatedAtUtc >= binding.Retry.MaxAge;
            if (attemptCount >= binding.Retry.MaxAttempts || retryExpired)
            {
                await store.MarkDeadLetterAsync(item.Id, "dispatch.retry_exhausted", cancellationToken);
                return;
            }

            var delaySeconds = Math.Min(Math.Pow(2, Math.Min(attemptCount - 1, 20)), binding.Retry.MaxAge.TotalSeconds);
            await store.MarkRetryScheduledAsync(
                item.Id,
                attemptCount,
                now.AddSeconds(delaySeconds),
                "dispatch.transient_failure",
                cancellationToken);
            return;
        }

        var executionStatus = await wakeSink.GetStatusAsync(request, cancellationToken);
        if (executionStatus is not null)
            await store.UpdateExecutionStatusAsync(executionStatus, cancellationToken);
    }
}