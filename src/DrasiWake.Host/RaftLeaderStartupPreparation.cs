using System.Security.Cryptography;
using System.Text.Json;
using DrasiWake.Adapters.OpenClaw;
using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Contracts;
using DrasiWake.Persistence.Raft;
using DrasiWake.Persistence.SonnetDB.Replication;

namespace DrasiWake.Host;

public interface IRaftLeaderStartupPreparation
{
    /// <summary>
    /// Validates or commits the cluster business-configuration fingerprint and repairs
    /// replicated target metadata before recovery or any other leader worker begins.
    /// </summary>
    /// <remarks>
    /// The leadership-epoch coordinator must call this only after Raft state restoration
    /// and leadership with quorum are established, and before RecoveryCoordinator.RecoverAsync.
    /// </remarks>
    Task PrepareBeforeRecoveryAsync(CancellationToken cancellationToken);
}

public sealed class RaftLeaderStartupPreparation(
    DrasiWakeHostSettings settings,
    ContractRegistryManager registryManager,
    IRaftCommandExecutor commandExecutor,
    IRaftBridgeProjection projection,
    IBridgeStore bridgeStore) : IRaftLeaderStartupPreparation
{
    private readonly DrasiWakeHostSettings _settings = settings ?? throw new ArgumentNullException(nameof(settings));
    private readonly ContractRegistryManager _registryManager =
        registryManager ?? throw new ArgumentNullException(nameof(registryManager));
    private readonly IRaftCommandExecutor _commandExecutor =
        commandExecutor ?? throw new ArgumentNullException(nameof(commandExecutor));
    private readonly IRaftBridgeProjection _projection =
        projection ?? throw new ArgumentNullException(nameof(projection));
    private readonly IBridgeStore _bridgeStore = bridgeStore ?? throw new ArgumentNullException(nameof(bridgeStore));

    public async Task PrepareBeforeRecoveryAsync(CancellationToken cancellationToken)
    {
        if (!_commandExecutor.IsLeader || !_commandExecutor.HasQuorum)
            throw new InvalidOperationException("Leader startup preparation requires Raft leadership and quorum.");

        var registry = _registryManager.Active;
        if (ReferenceEquals(registry, ContractRegistry.Empty))
            throw new InvalidOperationException("The validated contract registry is not active.");

        var expectedFingerprint = ClusterBusinessConfigurationFingerprint.Compute(_settings, registry);
        var currentFingerprint = (await _projection.ExportSnapshotAsync(cancellationToken).ConfigureAwait(false))
            .ConfigurationFingerprint;
        if (currentFingerprint is null)
        {
            var command = ReplicatedBridgeCommand.Create(
                BridgeCommandKind.SetConfigurationFingerprint,
                new SetConfigurationFingerprintPayload(expectedFingerprint));
            await _commandExecutor.ReplicateAsync(command, cancellationToken).ConfigureAwait(false);
            currentFingerprint = (await _projection.ExportSnapshotAsync(cancellationToken).ConfigureAwait(false))
                .ConfigurationFingerprint;
        }

        if (!string.Equals(currentFingerprint, expectedFingerprint, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "This node's business configuration does not match the fingerprint committed by the Raft cluster.");
        }

        var targetByBindingId = registry.Bindings.ToDictionary(
            binding => binding.Id,
            binding => binding.OpenClawTarget,
            StringComparer.Ordinal);
        var configuredTargetNames = _settings.OpenClawTargets.Keys.ToHashSet(StringComparer.Ordinal);
        var maximumRetryAgeByBindingId = registry.Bindings.ToDictionary(
            binding => binding.Id,
            binding => binding.Retry.MaxAge,
            StringComparer.Ordinal);
        var idempotencyRetentionByTarget = _settings.OpenClawTargets.ToDictionary(
            pair => pair.Key,
            pair => pair.Value.GatewayIdempotencyRetention,
            StringComparer.Ordinal);

        await _bridgeStore.EnsureOpenClawTargetsAsync(
            targetByBindingId,
            configuredTargetNames,
            maximumRetryAgeByBindingId,
            idempotencyRetentionByTarget,
            cancellationToken).ConfigureAwait(false);
    }
}

public static class ClusterBusinessConfigurationFingerprint
{
    public static string Compute(DrasiWakeHostSettings settings, ContractRegistry registry)
    {
        ArgumentNullException.ThrowIfNull(settings);
        ArgumentNullException.ThrowIfNull(registry);

        var bindingConfiguration = registry.Bindings
            .OrderBy(binding => binding.Id, StringComparer.Ordinal)
            .Select(binding => new
            {
                binding.Id,
                binding.Source,
                Server = binding.Server.AbsoluteUri,
                binding.InstanceId,
                binding.QueryId,
                binding.DeliveryMode,
                binding.SessionScope,
                binding.AggregateKeyPointer,
                binding.CanonicalIdentity,
                binding.OpenClawTarget,
                binding.MetaSkill,
                ContractVersion = binding.Contract.Version,
                FactSchemaHash = Convert.ToHexString(
                    SHA256.HashData(File.ReadAllBytes(binding.Contract.FactSchemaPath))),
                binding.MaxPayloadBytes,
                TriggerPhrases = binding.TriggerPhrases.Order(StringComparer.Ordinal),
                binding.Retry,
                binding.RateLimit
            })
            .ToArray();
        var targetConfiguration = settings.OpenClawTargets
            .OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .Select(pair => new
            {
                Name = pair.Key,
                BaseAddress = pair.Value.BaseAddress.AbsoluteUri,
                pair.Value.GatewayIdempotencyRetention
            })
            .ToArray();
        var canonicalConfiguration = new
        {
            SchemaVersion = 1,
            RegistryVersion = registry.Version,
            Bindings = bindingConfiguration,
            DrasiServerUri = settings.Drasi.ServerUri.AbsoluteUri,
            settings.Drasi.InitialReconnectDelay,
            settings.Drasi.MaxReconnectDelay,
            settings.OpenClaw.MaxRetryAttempts,
            settings.OpenClaw.RetryBaseDelay,
            settings.OpenClaw.MaxRetryDelay,
            settings.SignalCapacity,
            settings.WorkerCount,
            settings.ReconciliationInterval,
            settings.DispatchPollInterval,
            Targets = targetConfiguration
        };
        var canonicalBytes = JsonSerializer.SerializeToUtf8Bytes(canonicalConfiguration);
        return Convert.ToHexString(SHA256.HashData(canonicalBytes)).ToLowerInvariant();
    }
}
