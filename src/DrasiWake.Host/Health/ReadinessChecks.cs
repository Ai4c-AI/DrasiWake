using System.Diagnostics;
using DrasiWake.Adapters.DrasiServer;
using DrasiWake.Core.Contracts;
using DrasiWake.Core.Pipeline;
using DrasiWake.Persistence.SonnetDB;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;

namespace DrasiWake.Host.Health;

/// <summary>Liveness check: healthy once the host application has started.</summary>
public sealed class HostAliveCheck(IHostApplicationLifetime lifetime) : IHealthCheck
{
    private readonly IHostApplicationLifetime _lifetime = lifetime ?? throw new ArgumentNullException(nameof(lifetime));

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_lifetime.ApplicationStarted.IsCancellationRequested
            ? HealthCheckResult.Healthy("Host application has started.")
            : HealthCheckResult.Unhealthy("Host application has not started yet."));
    }
}

/// <summary>
/// Raft readiness: unhealthy without quorum; degraded when a follower keeps trailing its
/// commit index for longer than <c>DrasiWake:Admin:CommitLagDegradedAfter</c>.
/// </summary>
public sealed class RaftHealthCheck(
    IRaftLeadership leadership,
    DrasiWakeHostSettings settings,
    TimeProvider timeProvider) : IHealthCheck
{
    private readonly IRaftLeadership _leadership = leadership ?? throw new ArgumentNullException(nameof(leadership));
    private readonly DrasiWakeHostSettings _settings = settings ?? throw new ArgumentNullException(nameof(settings));
    private readonly TimeProvider _timeProvider = timeProvider ?? TimeProvider.System;
    private readonly object gate = new();
    private long lagStartedAt;
    private long lagCommittedIndex;

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (!_leadership.HasQuorum)
        {
            ResetLag();
            return Completed(HealthCheckResult.Unhealthy("Raft member has no quorum."));
        }

        if (_leadership is DotNextRaftLeadership concrete)
        {
            var observation = concrete.ObserveNode();
            if (!observation.IsLeader && observation.CommittedIndex > observation.AppliedIndex)
            {
                lock (gate)
                {
                    if (lagStartedAt == 0 || lagCommittedIndex != observation.CommittedIndex)
                    {
                        lagStartedAt = Stopwatch.GetTimestamp();
                        lagCommittedIndex = observation.CommittedIndex;
                    }
                    else if (Stopwatch.GetElapsedTime(lagStartedAt) > _settings.Admin.CommitLagDegradedAfter)
                    {
                        return Completed(HealthCheckResult.Degraded(
                            $"Follower still trails its commit index by {observation.CommittedIndex - observation.AppliedIndex} entries."));
                    }
                }

                return Completed(HealthCheckResult.Healthy(
                    $"Follower is catching up ({observation.CommittedIndex - observation.AppliedIndex} entries behind)."));
            }
        }

        ResetLag();
        return Completed(HealthCheckResult.Healthy(_leadership.IsLeader
            ? "Member is the leader with quorum."
            : "Member follows a leader with quorum."));
    }

    private void ResetLag()
    {
        lock (gate)
        {
            lagStartedAt = 0;
            lagCommittedIndex = 0;
        }
    }

    private static Task<HealthCheckResult> Completed(HealthCheckResult result) => Task.FromResult(result);
}

/// <summary>Registry readiness: unhealthy until a valid contract registry has been activated.</summary>
public sealed class RegistryHealthCheck(ContractRegistryManager registryManager) : IHealthCheck
{
    private readonly ContractRegistryManager _registryManager = registryManager ?? throw new ArgumentNullException(nameof(registryManager));

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var active = _registryManager.Active;
        return Task.FromResult(ReferenceEquals(active, ContractRegistry.Empty)
            ? HealthCheckResult.Unhealthy("Contract registry has not been activated.")
            : HealthCheckResult.Healthy($"Contract registry {active.Version} active with {active.Bindings.Count} binding(s)."));
    }
}

/// <summary>Database readiness: unhealthy when the projection store cannot be read.</summary>
public sealed class DatabaseHealthCheck(IDbContextFactory<BridgeDbContext> contextFactory) : IHealthCheck
{
    private readonly IDbContextFactory<BridgeDbContext> _contextFactory = contextFactory ?? throw new ArgumentNullException(nameof(contextFactory));

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var dbContext = await _contextFactory.CreateDbContextAsync(cancellationToken).ConfigureAwait(false);
            var state = await dbContext.RaftProjectionStates
                .AsNoTracking()
                .SingleOrDefaultAsync(item => item.Id == Persistence.SonnetDB.Entities.RaftProjectionState.SingletonId, cancellationToken)
                .ConfigureAwait(false);
            return HealthCheckResult.Healthy(state is null
                ? "Projection store is readable and empty."
                : $"Projection store is readable at applied index {state.LastAppliedIndex}.");
        }
        catch (Exception exception)
        {
            return HealthCheckResult.Unhealthy("Projection store could not be read.", exception);
        }
    }
}

/// <summary>Recovery readiness: unhealthy until the current leader epoch finished recovery.</summary>
public sealed class RecoveryHealthCheck(RecoveryReadiness recoveryReadiness) : IHealthCheck
{
    private readonly RecoveryReadiness _recoveryReadiness = recoveryReadiness ?? throw new ArgumentNullException(nameof(recoveryReadiness));

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_recoveryReadiness.IsInitialized
            ? HealthCheckResult.Healthy("Leader epoch recovery completed.")
            : HealthCheckResult.Unhealthy("Recovery has not completed for the active leader epoch yet."));
    }
}

/// <summary>
/// Drasi readiness based on live SSE subscription counters. Standby (non-leader) members do not
/// consume Drasi events, so they report healthy with a standby description.
/// </summary>
public sealed class DrasiHealthCheck(DrasiChangeSource changeSource, IRaftLeadership leadership) : IHealthCheck
{
    private readonly DrasiChangeSource _changeSource = changeSource ?? throw new ArgumentNullException(nameof(changeSource));
    private readonly IRaftLeadership _leadership = leadership ?? throw new ArgumentNullException(nameof(leadership));

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (!_leadership.IsLeader)
        {
            return Task.FromResult(HealthCheckResult.Healthy(
                "Standby member: Drasi event consumption runs on the leader only."));
        }

        var visible = _changeSource.VisibleQueries;
        var connected = _changeSource.ConnectedQueries;
        if (visible == 0 || connected == 0)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy(
                "No Drasi query streams are connected."));
        }

        if (connected < visible)
        {
            return Task.FromResult(HealthCheckResult.Degraded(
                $"{connected}/{visible} Drasi query streams connected; the rest are reconnecting."));
        }

        return Task.FromResult(HealthCheckResult.Healthy($"{connected} Drasi query stream(s) connected."));
    }
}

/// <summary>Per-target OpenClaw readiness from the downstream probe cache. Unreachable targets degrade.</summary>
public sealed class OpenClawTargetHealthCheck(string targetName, DownstreamProbeService probeService) : IHealthCheck
{
    private readonly string _targetName = targetName ?? throw new ArgumentNullException(nameof(targetName));
    private readonly DownstreamProbeService _probeService = probeService ?? throw new ArgumentNullException(nameof(probeService));

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var result = _probeService.GetResult(_targetName);
        if (result is null)
        {
            return Task.FromResult(HealthCheckResult.Degraded(
                $"OpenClaw target '{_targetName}' has not been probed yet."));
        }

        if (!result.Reachable)
        {
            return Task.FromResult(HealthCheckResult.Degraded(
                $"OpenClaw target '{_targetName}' is unreachable ({result.ErrorClass})."));
        }

        if (result.ErrorClass is not null)
        {
            return Task.FromResult(HealthCheckResult.Degraded(
                $"OpenClaw target '{_targetName}' responded with {result.ErrorClass}."));
        }

        return Task.FromResult(HealthCheckResult.Healthy(
            $"OpenClaw target '{_targetName}' is reachable ({result.LatencyMs} ms)."));
    }
}

/// <summary>
/// Aggregate OpenClaw readiness: unhealthy only when every configured target is unreachable;
/// individual target failures are carried by the per-target checks as degraded.
/// </summary>
public sealed class OpenClawAggregateHealthCheck(DrasiWakeHostSettings settings, DownstreamProbeService probeService) : IHealthCheck
{
    private readonly DrasiWakeHostSettings _settings = settings ?? throw new ArgumentNullException(nameof(settings));
    private readonly DownstreamProbeService _probeService = probeService ?? throw new ArgumentNullException(nameof(probeService));

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var targetNames = _settings.OpenClawTargets.Keys.ToArray();
        if (targetNames.Length == 0)
            return Task.FromResult(HealthCheckResult.Healthy("No OpenClaw targets are configured."));

        var results = targetNames.Select(_probeService.GetResult).ToArray();
        if (results.All(result => result is not null && !result.Reachable))
        {
            return Task.FromResult(HealthCheckResult.Unhealthy(
                $"All {targetNames.Length} OpenClaw target(s) are unreachable."));
        }

        var reachableCount = results.Count(result => result?.Reachable == true);
        return Task.FromResult(HealthCheckResult.Healthy(
            $"{reachableCount}/{targetNames.Length} OpenClaw target(s) reachable."));
    }
}
