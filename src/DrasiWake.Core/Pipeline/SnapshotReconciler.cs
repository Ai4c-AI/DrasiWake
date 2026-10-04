using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Contracts;
using DrasiWake.Core.Domain;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;

namespace DrasiWake.Core.Pipeline;

public sealed class SnapshotReconciler(
    IChangeSource changeSource,
    IBridgeStore store,
    ContractRegistryManager registry,
    SessionIdentityResolver? identityResolver = null,
    WakePayloadRenderer? payloadRenderer = null,
    TimeProvider? timeProvider = null)
{
    private static readonly ConcurrentDictionary<string, SemaphoreSlim> QueryLocks = new(StringComparer.Ordinal);
    private readonly SessionIdentityResolver identityResolver = identityResolver ?? new SessionIdentityResolver();
    private readonly WakePayloadRenderer payloadRenderer = payloadRenderer ?? new WakePayloadRenderer();
    private readonly TimeProvider timeProvider = timeProvider ?? TimeProvider.System;

    public async Task ReconcileAsync(QueryIdentity query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        using var activity = BridgeTelemetry.StartReconciliation(query);
        BridgeTelemetry.RecordReconciliation(query);

        var bindings = registry.Active.Bindings
            .Where(binding => Matches(binding, query))
            .ToArray();
        if (bindings.Length == 0)
        {
            BridgeTelemetry.RecordRoute(query, null, "unmatched");
            return;
        }

        foreach (var binding in bindings)
            BridgeTelemetry.RecordRoute(query, binding.Id, "matched", binding.Contract.Version);

        var lockKey = GetQueryKey(query);
        var queryLock = QueryLocks.GetOrAdd(lockKey, static _ => new SemaphoreSlim(1, 1));
        await queryLock.WaitAsync(cancellationToken);
        try
        {
            QuerySnapshot snapshot;
            try
            {
                snapshot = await changeSource.ReadSnapshotAsync(query, cancellationToken);
            }
            catch
            {
                BridgeTelemetry.RecordSnapshotFailure(query);
                throw;
            }
            var recoveryState = await store.LoadRecoveryStateAsync(cancellationToken);
            var rows = SnapshotFingerprint.CanonicalizeRows(snapshot.Rows);

            foreach (var binding in bindings)
            {
                var rowsBySession = new Dictionary<string, List<System.Text.Json.Nodes.JsonNode?>>(StringComparer.Ordinal);
                string? identityRejection = null;
                foreach (var row in rows)
                {
                    if (row is not System.Text.Json.Nodes.JsonObject rowObject)
                    {
                        identityRejection = "identity.row_invalid";
                        break;
                    }

                    var resolution = identityResolver.Resolve(binding, rowObject);
                    if (!resolution.IsSuccess)
                    {
                        identityRejection = resolution.ErrorCode!;
                        break;
                    }

                    var sessionId = resolution.Identity!.Value;
                    if (!rowsBySession.TryGetValue(sessionId, out var sessionRows))
                    {
                        sessionRows = [];
                        rowsBySession.Add(sessionId, sessionRows);
                    }
                    sessionRows.Add(row);
                }

                if (identityRejection is not null)
                {
                    var snapshotFingerprint = SnapshotFingerprint.Compute(snapshot);
                    var auditSessionId = $"rejected-query:{GetQueryKey(query)}";
                    await store.RecordRejectedWakeAsync(
                        CreateRejectedWake(binding, auditSessionId, snapshotFingerprint, identityRejection),
                        identityRejection,
                        cancellationToken);
                    continue;
                }

                if (string.Equals(binding.SessionScope, "singleton", StringComparison.OrdinalIgnoreCase))
                    rowsBySession.TryAdd("singleton", []);

                foreach (var sessionId in recoveryState.Checkpoints
                             .Where(item => item.BindingId == binding.Id)
                             .Select(item => item.SessionId)
                             .Concat(recoveryState.DispatchableItems
                                 .Where(item => item.BindingId == binding.Id)
                                 .Select(item => item.SessionId)))
                {
                    rowsBySession.TryAdd(sessionId, []);
                }

                foreach (var (sessionId, sessionRows) in rowsBySession)
                {
                    var fingerprint = SnapshotFingerprint.Compute(new QuerySnapshot(query, sessionRows));
                    JsonObject input;
                    try
                    {
                        (input, fingerprint) = payloadRenderer.Render(binding, query, sessionId, sessionRows);
                    }
                    catch (WakePayloadRejectedException exception)
                    {
                        await store.SupersedePendingWakesAsync(binding.Id, sessionId, fingerprint, cancellationToken);
                        await store.RecordRejectedWakeAsync(
                            CreateRejectedWake(binding, sessionId, fingerprint, exception.ErrorCode),
                            exception.ErrorCode,
                            cancellationToken);
                        continue;
                    }

                    await store.SupersedePendingWakesAsync(binding.Id, sessionId, fingerprint, cancellationToken);
                    var checkpoint = recoveryState.Checkpoints.FirstOrDefault(item =>
                        item.BindingId == binding.Id && item.SessionId == sessionId);
                    if (checkpoint?.Fingerprint == fingerprint)
                    {
                        BridgeTelemetry.RecordDuplicateFingerprint(binding.Id);
                        continue;
                    }

                    if (recoveryState.DispatchableItems.Any(item =>
                            item.BindingId == binding.Id && item.SessionId == sessionId &&
                            item.SnapshotFingerprint == fingerprint &&
                            item.Status is WakeOutboxStatus.Pending or WakeOutboxStatus.RetryScheduled))
                    {
                        BridgeTelemetry.RecordDuplicateFingerprint(binding.Id);
                        continue;
                    }

                    var id = Guid.NewGuid();
                    var item = new WakeOutboxItem(
                        id,
                        binding.Id,
                        sessionId,
                        fingerprint,
                        binding.MetaSkill,
                        input,
                        binding.Contract.Version,
                        $"drasiwake:{id:N}",
                        0,
                        timeProvider.GetUtcNow(),
                        timeProvider.GetUtcNow(),
                        WakeOutboxStatus.Pending,
                        null,
                        null,
                        binding.OpenClawTarget);
                    _ = await store.CreateOrUpdatePendingWakeAsync(item, cancellationToken);
                    BridgeTelemetry.RecordOutboxCreated(item);
                }
            }
        }
        finally
        {
            queryLock.Release();
        }
    }

    private static bool Matches(BridgeBinding binding, QueryIdentity query)
        => string.Equals(binding.Source, "drasi-server", StringComparison.OrdinalIgnoreCase)
            && binding.Server == query.Server
            && string.Equals(binding.InstanceId, query.InstanceId, StringComparison.Ordinal)
            && string.Equals(binding.QueryId, query.QueryId, StringComparison.Ordinal);

    private static string GetQueryKey(QueryIdentity query)
        => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(
            $"{query.Server.AbsoluteUri}\n{query.InstanceId}\n{query.QueryId}")));

    private WakeOutboxItem CreateRejectedWake(
        BridgeBinding binding,
        string sessionId,
        string fingerprint,
        string reasonCode)
    {
        var stableIdentity = $"{binding.Id}\n{sessionId}\n{fingerprint}\n{reasonCode}";
        var idBytes = SHA256.HashData(Encoding.UTF8.GetBytes(stableIdentity));
        var id = new Guid(idBytes.AsSpan(0, 16));
        var now = timeProvider.GetUtcNow();
        return new WakeOutboxItem(
            id,
            binding.Id,
            sessionId,
            fingerprint,
            binding.MetaSkill,
            new JsonObject
            {
                ["bindingId"] = binding.Id,
                ["sessionId"] = sessionId,
                ["snapshotFingerprint"] = fingerprint,
                ["rejectionCode"] = reasonCode
            },
            binding.Contract.Version,
            $"drasiwake:rejected:{id:N}",
            0,
            now,
            now,
            WakeOutboxStatus.DeadLetter,
            null,
            null,
            binding.OpenClawTarget);
    }
}