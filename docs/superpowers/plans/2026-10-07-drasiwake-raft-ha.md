# DrasiWake Raft HA Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable three-node DrasiWake high availability with DotNext Raft, per-node SonnetDB projections, leader-only work, failover recovery, and protected runtime membership management.

**Architecture:** A new Raft persistence adapter replicates versioned bridge-store commands through DotNext; the state machine applies committed commands and snapshots to each node's independent SonnetDB projection. The Host starts Drasi and outbox workers only for a caught-up leader, and exposes a TLS-protected, Bearer-authenticated gRPC control plane for membership changes.

**Tech Stack:** .NET 10, DotNext.AspNetCore.Cluster 6.9.0, EF Core 10 with SonnetDB.EntityFrameworkCore 4.0.0, protobuf-net.Grpc.AspNetCore 1.3.14, Grpc.Net.Client 2.84.0, xUnit v3, existing Microsoft.Testing.Platform runner.

## Global Constraints

- Use three Raft voting nodes and tolerate one node failure.
- Every node owns an independent SonnetDB directory and independent Raft data directory; never share database files between replicas.
- Raft committed bridge commands are authoritative; local SonnetDB is a rebuildable projection.
- Only the current leader starts Drasi receive, reconciliation, and outbox dispatch workers.
- No majority means no new durable business progress; do not fall back to independent local writes.
- Preserve the existing single-node startup configuration and run it through the same Raft state-machine path.
- `Accepted` and its matching snapshot checkpoint are one committed command.
- Gateway side effects remain recoverable at-least-once operations using the existing stable idempotency key and retention validation; do not claim exactly-once.
- Snapshot contents must reconstruct every persisted bridge table and the projection's last applied Raft index.
- Raft and management gRPC use TLS; management requests additionally require a cluster management Bearer Token that is injected through a secret provider and never logged.
- Every cluster member must have the same effective binding/Drasi/Gateway behavior fingerprint and a compatible application command version.
- Keep NodeId, local paths, certificates, and tokens out of replicated business commands.

---

## File Map

| File | Responsibility |
| --- | --- |
| `Directory.Packages.props` | Centrally pin DotNext.AspNetCore.Cluster 6.9.0, Grpc.Net.Client 2.84.0, and protobuf-net.Grpc.AspNetCore 1.3.14. |
| `DrasiWake.sln` | Include the new Raft persistence and test projects. |
| `src/DrasiWake.Persistence.SonnetDB/BridgeDbContext.cs`, `Entities/RaftProjectionState.cs`, `Migrations/` | Persist the last applied Raft index and replicated cluster metadata. |
| `src/DrasiWake.Persistence.SonnetDB/Replication/ReplicatedBridgeCommand.cs`, `BridgeStoreSnapshot.cs` | Define versioned command wire data and the complete, versioned projection snapshot, including the last applied command ID. |
| `src/DrasiWake.Persistence.SonnetDB/SonnetBridgeStore.cs` | Apply commands and indexes transactionally; export/restore snapshots; expose a non-mutating dispatch-candidate query for Raft claims. |
| `src/DrasiWake.Persistence.Raft/DrasiWake.Persistence.Raft.csproj` | Isolate DotNext state-machine and command-replication dependencies. |
| `src/DrasiWake.Persistence.Raft/RaftBridgeStateMachine.cs` | Apply each committed DotNext log entry to the local SonnetDB projection and implement snapshot persistence/restoration. |
| `src/DrasiWake.Persistence.Raft/IRaftCommandExecutor.cs`, `DotNextRaftCommandExecutor.cs` | Abstract command submission for tests and implement it with the local DotNext cluster. |
| `src/DrasiWake.Persistence.Raft/RaftBridgeStore.cs` | Implement `IBridgeStore` by submitting mutations as Raft commands and reading committed results from the local projection. |
| `src/DrasiWake.Host/DrasiWakeHostSettings.cs`, `RaftClusterSettings.cs` | Parse and validate topology, node identity, local directories, TLS, management credentials, and snapshot settings. |
| `src/DrasiWake.Host/DrasiWakeHostBuilder.cs`, `DrasiWake.Host.csproj`, `appsettings.json` | Host DotNext HTTP/2 endpoints, register the state machine and Raft store, and retain the single-node configuration. |
| `src/DrasiWake.Host/HostStartupValidator.cs` | Keep static validation, local directory ownership, and migrations before cluster startup; remove replicated writes from pre-consensus startup. |
| `src/DrasiWake.Host/RaftLeaderHostedService.cs` | Track leadership and start/stop a fresh scoped bridge worker runtime for each leadership epoch. |
| `src/DrasiWake.Host/BridgeHostedService.cs`, `ReconciliationHostedService.cs`, `DrasiWakeHostBuilder.cs` | Make current one-shot workers scoped/recreatable and register them only under the leader coordinator. |
| `src/DrasiWake.Host/ClusterMembershipGrpcService.cs`, `RaftMembershipContracts.cs`, `ClusterConfigurationFingerprint.cs` | Authenticate membership RPCs, compare joining-node compatibility, and forward/commit Add/Remove through DotNext. |
| `src/DrasiWake.Core/Pipeline/RecoveryCoordinator.cs`, `BridgeTelemetry.cs` | Support leadership-epoch recovery and report HA role, quorum, lag, membership, and recovery metrics. |
| `tests/DrasiWake.Persistence.SonnetDB.Tests/ReplicatedProjectionTests.cs` | Test atomic applied-index updates, idempotent replay, and complete snapshot round trips. |
| `tests/DrasiWake.Persistence.Raft.Tests/` | Test command encoding, state-machine application, and Raft adapter semantics. |
| `tests/DrasiWake.IntegrationTests/Fixtures/RaftClusterFixture.cs`, `RaftFailoverTests.cs`, `HostStartupTests.cs` | Run real multi-node DotNext tests and prove leadership gating, quorum behavior, failover, catch-up, and config rejection. |
| `docs/bridge-core-v1-operations.md`, `README.md` | Document node configuration, secure operation, membership changes, failure behavior, and single-node compatibility. |

The pieces are one interdependent HA release rather than separately shippable projects: replicated state, leader gating, and membership management all rely on the same persisted Raft topology. Keep the responsibilities in the listed project boundaries.

## Task 1: Add Cluster Settings, Dependency Pins, and Validation

**Files:**

- Modify: `Directory.Packages.props`
- Modify: `src/DrasiWake.Host/DrasiWake.Host.csproj`
- Modify: `src/DrasiWake.Host/DrasiWakeHostSettings.cs`
- Create: `src/DrasiWake.Host/RaftClusterSettings.cs`
- Modify: `tests/DrasiWake.IntegrationTests/HostStartupTests.cs`
- Modify: `src/DrasiWake.Host/appsettings.json`

**Interfaces:**

- Produces `RaftClusterSettings.FromConfiguration(IConfiguration, string databasePath)` and `RaftClusterSettings.Validate()`.
- `RaftClusterSettings` carries `Mode`, `NodeId`, `ListenAddress`, `RaftDataPath`, `InitialMembers`, `CertificatePath`, `CertificatePassword`, `ManagementAddress`, `ManagementBearerToken`, and `SnapshotFrequency`.
- `Mode` is `SingleNode` or `Cluster`; `SingleNode` derives a stable local identity, a loopback endpoint and one local member. `Cluster` requires a stable NodeId, an HTTPS endpoint, independent Raft path, certificate configuration, management HTTPS endpoint/token, positive snapshot frequency, and valid initial member URIs.
- Host settings retain existing values and expose the validated cluster settings as `DrasiWakeHostSettings.Cluster`.

- [ ] **Step 1: Add failing configuration tests.** Add tests to `HostStartupTests` for default single-node settings, valid three-member settings, blank NodeId, duplicate initial member URI, non-HTTPS cluster endpoint, shared Raft/DB path, missing management token, invalid member URI, and non-positive snapshot frequency. Test duplicate NodeId rejection at membership preflight, where the full persisted membership is available.

  The missing-management-token case applies to `Cluster` mode. `SingleNode` mode keeps the checked-in configuration secret-free and does not expose runtime membership management.

  Example assertion:

  ```csharp
  [Fact]
  public void Cluster_mode_requires_management_token()
  {
      var configuration = CreateConfiguration(new Dictionary<string, string?>
      {
          ["DrasiWake:Cluster:Mode"] = "Cluster",
          ["DrasiWake:Cluster:Management:BearerToken"] = null
      });

      var exception = Assert.Throws<InvalidOperationException>(
          () => DrasiWakeHostSettings.FromConfiguration(configuration));

      Assert.Contains("DrasiWake:Cluster:Management:BearerToken", exception.Message, StringComparison.Ordinal);
  }
  ```

- [ ] **Step 2: Run the focused test class and verify the new validation tests fail.**

  Run: `dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.HostStartupTests`

  Expected: the new tests fail because cluster settings are not yet parsed or validated. Do not accept a zero-test run.

- [ ] **Step 3: Pin and reference the packages.** Add central versions matching SlikCache: `DotNext.AspNetCore.Cluster` 6.9.0, `Grpc.Net.Client` 2.84.0, and `protobuf-net.Grpc.AspNetCore` 1.3.14. Add ASP.NET Core framework support to Host and add required project/package references only where the code uses those APIs.

- [ ] **Step 4: Implement cluster settings parsing and validation.** Parse the `DrasiWake:Cluster` section; default only the legacy single-node path to local loopback settings. In Cluster mode reject missing/invalid values with errors naming the configuration key. Normalize member URIs, reject duplicates, require the local endpoint to appear in initial membership for bootstrap, and require the Raft path to differ from the SonnetDB path.

- [ ] **Step 5: Add non-secret sample configuration.** Add a `Cluster` sample using single-node mode to `appsettings.json`; do not add real certificate passwords or management tokens. Keep multi-node secrets environment/secret-provider-only and disable membership gRPC in single-node mode.

- [ ] **Step 6: Run the focused Host startup tests and commit.**

  Run: `dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.HostStartupTests`

  Expected: all HostStartupTests pass, including every new invalid-configuration case.

  ```text
  git add Directory.Packages.props src/DrasiWake.Host tests/DrasiWake.IntegrationTests/HostStartupTests.cs
  git commit -m "feat: add Raft cluster settings"
  ```

## Task 2: Make the SonnetDB Projection Replay-Safe and Snapshot-Capable

**Files:**

- Modify: `src/DrasiWake.Persistence.SonnetDB/BridgeDbContext.cs`
- Create: `src/DrasiWake.Persistence.SonnetDB/Entities/RaftProjectionState.cs`
- Create: `src/DrasiWake.Persistence.SonnetDB/Replication/ReplicatedBridgeCommand.cs`
- Create: `src/DrasiWake.Persistence.SonnetDB/Replication/BridgeStoreSnapshot.cs`
- Modify: `src/DrasiWake.Persistence.SonnetDB/SonnetBridgeStore.cs`
- Generate: SonnetDB migration `AddRaftProjectionState` with EF Core tooling under `src/DrasiWake.Persistence.SonnetDB/Migrations/`
- Create: `tests/DrasiWake.Persistence.SonnetDB.Tests/ReplicatedProjectionTests.cs`

**Interfaces:**

- `ReplicatedBridgeCommand` is an immutable envelope with `SchemaVersion`, `CommandId`, `Kind`, and serialized payload. `CommandId` is the SHA-256 hex digest of the command kind and canonical payload, so an uncertain caller retry with the same values has a stable ID. `Kind` covers all existing `IBridgeStore` mutations, dispatch claim, interrupted-dispatch recovery, target backfill, and cluster metadata updates.
- `BridgeStoreSnapshot` carries `SchemaVersion`, `LastAppliedIndex`, `LastAppliedCommandId`, cluster configuration fingerprint, and complete ordered row collections for `Subscriptions`, `SnapshotCheckpoints`, `KeyMappings`, and `WakeOutbox`.
- Add to `SonnetBridgeStore`:

  ```csharp
  public ValueTask<long> GetLastAppliedIndexAsync(CancellationToken cancellationToken);
  public ValueTask ApplyReplicatedCommandAsync(
      ReplicatedBridgeCommand command,
      long raftIndex,
      CancellationToken cancellationToken);
  public ValueTask<BridgeStoreSnapshot> ExportSnapshotAsync(CancellationToken cancellationToken);
  public ValueTask RestoreSnapshotAsync(BridgeStoreSnapshot snapshot, CancellationToken cancellationToken);
  public ValueTask<IReadOnlyList<WakeOutboxItem>> LoadDispatchableCandidatesAsync(
      DateTimeOffset nowUtc,
      int limit,
      CancellationToken cancellationToken);
  public ValueTask<RecoveryState> ReadRecoveryStateAsync(CancellationToken cancellationToken);
  ```

- `RaftProjectionState` stores the last applied index, the command ID for that index, and committed cluster configuration fingerprint in a singleton keyed row.

- [ ] **Step 1: Add failing projection tests for index atomicity, duplicate replay, and gaps.** Use the existing `CreateOptions`/`TestContextFactory` patterns in `AtomicAcceptanceTests`, but keep new test helpers in `ReplicatedProjectionTests`.

  ```csharp
  [Fact]
  public async Task Reapplying_the_same_index_does_not_duplicate_or_advance_state()
  {
      var command = CreatePendingWakeCommand();
      await store.ApplyReplicatedCommandAsync(command, 1, cancellationToken);
      await store.ApplyReplicatedCommandAsync(command, 1, cancellationToken);

      Assert.Equal(1, await store.GetLastAppliedIndexAsync(cancellationToken));
      Assert.Single(await ReadOutboxAsync());
  }
  ```

  Also assert that index `3` is rejected when the current projection index is `1`, and an injected failure leaves both the business rows and applied index unchanged.

- [ ] **Step 2: Run the focused persistence tests and verify failure.**

  Run: `dotnet test --project tests\DrasiWake.Persistence.SonnetDB.Tests\DrasiWake.Persistence.SonnetDB.Tests.csproj -c Release --filter-class DrasiWake.Persistence.SonnetDB.Tests.ReplicatedProjectionTests`

  Expected: compilation/test failure because replicated projection APIs and metadata do not exist.

- [ ] **Step 3: Add the projection-state entity and migration.** Add a singleton primary key, non-negative `LastAppliedIndex`, `LastAppliedCommandId`, and nullable-then-initialized committed configuration fingerprint. Generate the migration with `dotnet ef migrations add AddRaftProjectionState --project src\DrasiWake.Persistence.SonnetDB\DrasiWake.Persistence.SonnetDB.csproj --startup-project src\DrasiWake.Host\DrasiWake.Host.csproj`; update `BridgeDbContextModelSnapshot` through the migration command rather than editing generated metadata by hand.

- [ ] **Step 4: Implement the replicated command envelope and transactional applier.** Refactor `SonnetBridgeStore` mutations into helpers that accept the same `BridgeDbContext`. `ApplyReplicatedCommandAsync` begins one EF transaction, skips entries older than the local projection index during replay, treats a duplicate of the current index as a no-op only when its command ID matches `LastAppliedCommandId`, rejects an index gap, applies exactly one typed operation, updates both applied index and command ID, and commits. Preserve `MarkAcceptedWithCheckpointAsync` outbox/checkpoint atomicity inside that same transaction.

- [ ] **Step 5: Separate candidate reads from dispatch claims.** Add a read-only `LoadDispatchableCandidatesAsync` using the current deterministic ordering; implement `ClaimDispatchable` as a replicated command containing the selected outbox IDs and claim time. Split recovery into a replicated `RecoverInterruptedDispatches` command and read-only `ReadRecoveryStateAsync`.

- [ ] **Step 6: Implement complete snapshot export/restore.** Export all four existing tables, including currently empty `Subscriptions` and `KeyMappings`, in deterministic key order. Restore validates schema version, duplicate keys, required fields, and index before opening the replacement transaction; replace table state and projection metadata atomically. A failed validation or transaction must leave the previous projection intact.

- [ ] **Step 7: Add snapshot round-trip and failure tests.** Include outbox statuses, nullable fields, JSON input, checkpoint timestamps, key mappings, subscriptions, and committed index in the round trip. Assert snapshots with unsupported versions or duplicate primary keys are rejected without altering existing rows.

- [ ] **Step 8: Run persistence tests and commit.**

  Run: `dotnet test --project tests\DrasiWake.Persistence.SonnetDB.Tests\DrasiWake.Persistence.SonnetDB.Tests.csproj -c Release`

  Expected: all persistence tests pass, including existing atomic acceptance, database reopen, and ownership-lock tests.

  ```text
  git add src/DrasiWake.Persistence.SonnetDB tests/DrasiWake.Persistence.SonnetDB.Tests
  git commit -m "feat: add replay-safe SonnetDB projection"
  ```

## Task 3: Implement the DotNext State Machine and Raft Command Executor

**Files:**

- Create: `src/DrasiWake.Persistence.Raft/DrasiWake.Persistence.Raft.csproj`
- Create: `src/DrasiWake.Persistence.Raft/RaftBridgeStateMachine.cs`
- Create: `src/DrasiWake.Persistence.Raft/IRaftCommandExecutor.cs`
- Create: `src/DrasiWake.Persistence.Raft/DotNextRaftCommandExecutor.cs`
- Create: `tests/DrasiWake.Persistence.Raft.Tests/DrasiWake.Persistence.Raft.Tests.csproj`
- Create: `tests/DrasiWake.Persistence.Raft.Tests/RaftBridgeStateMachineTests.cs`
- Modify: `DrasiWake.sln`

**Interfaces:**

- `RaftBridgeStateMachine` derives from DotNext `SimpleStateMachine` and receives the local `SonnetBridgeStore`, snapshot frequency, and logger.
- `IRaftCommandExecutor` exposes:

  ```csharp
  public interface IRaftCommandExecutor
  {
      ValueTask ReplicateAsync(ReplicatedBridgeCommand command, CancellationToken cancellationToken);
      bool IsLeader { get; }
      bool HasQuorum { get; }
  }
  ```

- `DotNextRaftCommandExecutor` serializes the envelope with the shared versioned `JsonSerializerOptions`, awaits `IRaftCluster.ReplicateAsync`, and returns only after DotNext reports successful consensus replication. It rejects submissions when the local node is not leader or the cluster has no writable quorum; it never writes directly to the local SonnetDB.
- The state machine deserializes only supported command versions, applies `LogEntry.Index` to the local projection, and persists/restores `BridgeStoreSnapshot` through DotNext's binary reader/writer APIs.

- [ ] **Step 1: Add failing command serialization tests.** Assert every command kind round-trips its typed payload, `SchemaVersion` is explicit, the same canonical payload produces the same command ID, a changed timestamp/payload produces a different ID, malformed/unknown versions are rejected, and serialization never includes node secrets or file paths.

- [ ] **Step 2: Run the new focused Raft tests and verify failure.**

  Run: `dotnet test --project tests\DrasiWake.Persistence.Raft.Tests\DrasiWake.Persistence.Raft.Tests.csproj -c Release --filter-class DrasiWake.Persistence.Raft.Tests.RaftBridgeStateMachineTests`

  Expected: project/test compilation fails because the Raft project and state machine are not present.

- [ ] **Step 3: Add the Raft project and package references.** Reference `DrasiWake.Core` and `DrasiWake.Persistence.SonnetDB`; reference centrally pinned DotNext.AspNetCore.Cluster 6.9.0. Add the test project with the existing .NET 10, xUnit v3, and runner settings.

- [ ] **Step 4: Implement the executor and state machine.** Use SlikCache's 6.9.0 `SimpleStateMachine`, `IRaftCluster.ReplicateAsync`, `WriteAheadLog.Options`, snapshot reader/writer, and `UseStateMachine<T>` usage as API references. `ApplyAsync` must persist each committed log index atomically with its local projection; snapshot creation must serialize the complete projection and its index; restore must replace a validated snapshot and then allow later log entries to apply.

- [ ] **Step 5: Test state-machine replay and snapshot callbacks.** Exercise one single-node DotNext host with a temporary Raft directory; submit two commands, stop/reopen the node, and assert the projection and applied index restore. Include malformed log payload and projection transaction failure cases; the node must not report itself caught up after apply fails.

- [ ] **Step 6: Run new and persistence tests, then commit.**

  Run: `dotnet test --project tests\DrasiWake.Persistence.Raft.Tests\DrasiWake.Persistence.Raft.Tests.csproj -c Release`

  Expected: command, state-machine, and snapshot tests pass.

  ```text
  git add DrasiWake.sln src/DrasiWake.Persistence.Raft tests/DrasiWake.Persistence.Raft.Tests
  git commit -m "feat: add DotNext bridge state machine"
  ```

## Task 4: Route Every `IBridgeStore` Mutation Through Raft

**Files:**

- Create: `src/DrasiWake.Persistence.Raft/RaftBridgeStore.cs`
- Create: `tests/DrasiWake.Persistence.Raft.Tests/RaftBridgeStoreTests.cs`
- Modify: `src/DrasiWake.Persistence.SonnetDB/Replication/ReplicatedBridgeCommand.cs`
- Modify: `src/DrasiWake.Persistence.SonnetDB/SonnetBridgeStore.cs`

**Interfaces:**

- `RaftBridgeStore : IBridgeStore` receives `IRaftCommandExecutor` and local `SonnetBridgeStore`.
- Each mutation maps to one `ReplicatedBridgeCommand`; the method awaits committed application before reading its result from the local projection.
- `LoadDispatchableAsync` reads ordered candidates, submits one claim command containing their IDs and `nowUtc`, and returns only those committed as `Dispatching`.
- `LoadRecoveryStateAsync` submits the recovery command with its leader-supplied UTC timestamp, then calls read-only `ReadRecoveryStateAsync`.
- `EnsureOpenClawTargetsAsync` is a replicated command; static target/retention checks remain in `HostStartupValidator`.

- [ ] **Step 1: Add failing adapter tests using a capturing executor.** The test executor records the envelope, applies it to a temporary projection with the next index, and completes without a return value. Assert command kind and payload for every `IBridgeStore` mutation, and assert a failed executor leaves the projection unchanged.

- [ ] **Step 2: Verify all operation cases fail before implementation.**

  Run: `dotnet test --project tests\DrasiWake.Persistence.Raft.Tests\DrasiWake.Persistence.Raft.Tests.csproj -c Release --filter-class DrasiWake.Persistence.Raft.Tests.RaftBridgeStoreTests`

- [ ] **Step 3: Implement the adapter method by method.** Cover create/update pending wake, rejected wake, supersede, dispatch claim, atomic acceptance/checkpoint, execution status, retry, dead-letter, interrupted-dispatch recovery, and target backfill. Supply all timestamps and IDs in the command so followers never use local wall clocks or generate local IDs. Compute `CommandId` as lowercase SHA-256 over the operation kind and deterministic JSON payload; sort dictionary keys before serialization.

- [ ] **Step 4: Test key invariants.** Assert claim failure causes no Gateway-ready result, repeated acceptance preserves the original invocation/checkpoint, recovery uses the same idempotency key, and `Accepted` cannot be observed without the matching checkpoint on any projection.

- [ ] **Step 5: Run Raft and SonnetDB focused tests and commit.**

  Run: `dotnet test --project tests\DrasiWake.Persistence.Raft.Tests\DrasiWake.Persistence.Raft.Tests.csproj -c Release`

  Run: `dotnet test --project tests\DrasiWake.Persistence.SonnetDB.Tests\DrasiWake.Persistence.SonnetDB.Tests.csproj -c Release`

  Expected: both projects pass.

  ```text
  git add src/DrasiWake.Persistence.Raft src/DrasiWake.Persistence.SonnetDB tests/DrasiWake.Persistence.Raft.Tests
  git commit -m "feat: replicate bridge store mutations"
  ```

## Task 5: Host a Cluster Without Breaking Single-Node Startup

**Files:**

- Modify: `src/DrasiWake.Host/DrasiWake.Host.csproj`
- Modify: `src/DrasiWake.Host/DrasiWakeHostBuilder.cs`
- Modify: `src/DrasiWake.Host/HostStartupValidator.cs`
- Modify: `src/DrasiWake.Host/appsettings.json`
- Modify: `src/DrasiWake.Host/DrasiWakeHostSettings.cs`
- Modify: `tests/DrasiWake.IntegrationTests/HostStartupTests.cs`
- Modify: `DrasiWake.sln`

**Interfaces:**

- Register the local `SonnetBridgeStore` as the Raft state-machine projection and register `RaftBridgeStore` as the production `IBridgeStore`.
- Host startup order is: validate static settings and registry; acquire this node's local database ownership; migrate its database; start Kestrel/Raft and restore the projection; verify/commit cluster business fingerprint; only then allow leader worker coordination.
- Move the mutating `EnsureOpenClawTargetsAsync` call out of `HostStartupValidator`; the elected leader submits it through `RaftBridgeStore` before `RecoveryCoordinator.RecoverAsync`.

- [ ] **Step 1: Add failing Host lifecycle tests.** Assert configured single-node mode starts a Raft-backed store; multi-node mode starts a Kestrel endpoint with the configured TLS certificate; two hosts with the same local DB still fail the existing owner-lock test; and migrations run before cluster workers.

- [ ] **Step 2: Run the focused Host startup class and verify the new assertions fail.**

  Run: `dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.HostStartupTests`

- [ ] **Step 3: Add Host references and cluster hosting.** Reference `DrasiWake.Persistence.Raft`, DotNext.AspNetCore.Cluster, Grpc.Net.Client, and protobuf-net.Grpc.AspNetCore; add `Microsoft.AspNetCore.App` as a framework reference to the console Host. Change both `DrasiWakeHostBuilder.CreateHost` overloads to `Host.CreateDefaultBuilder` plus `ConfigureServices`/`ConfigureAppConfiguration`, then use `ConfigureWebHostDefaults`, DotNext's HTTP/2 protocol middleware, persistent cluster configuration, and `JoinCluster`; bind only configured endpoints. Validate production certificates and cluster TLS trust before starting Kestrel.

- [ ] **Step 4: Reorder service registration and startup.** Preserve the old `HostStartupValidator` work that is safe before Raft (settings, contracts, lock, migration, static target retention checks). Remove its direct `IBridgeStore` mutation. Start cluster/state-machine hosting before any leader controller. After leadership, commit target backfill/validation and recovery before worker readiness.

- [ ] **Step 5: Update `appsettings.json` and all Host test configuration helpers.** Keep secrets absent; configure the single-node local topology and document environment-key names. Assert old settings still resolve to the single-node defaults and use the Raft adapter.

- [ ] **Step 6: Run Host startup tests and a Release Host build.**

  Run: `dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.HostStartupTests`

  Run: `dotnet build src\DrasiWake.Host\DrasiWake.Host.csproj -c Release`

  Expected: both succeed; `Valid_registry_migrates_database_and_database_directory_has_single_owner` continues to pass.

  ```text
  git add DrasiWake.sln src/DrasiWake.Host tests/DrasiWake.IntegrationTests/HostStartupTests.cs
  git commit -m "feat: host DrasiWake Raft cluster"
  ```

## Task 6: Gate and Recreate Bridge Workers by Leadership Epoch

**Files:**

- Create: `src/DrasiWake.Host/RaftLeaderHostedService.cs`
- Create: `src/DrasiWake.Host/LeaderWorkerRuntime.cs`
- Modify: `src/DrasiWake.Host/DrasiWakeHostBuilder.cs`
- Modify: `src/DrasiWake.Host/BridgeHostedService.cs`
- Modify: `src/DrasiWake.Host/ReconciliationHostedService.cs`
- Modify: `src/DrasiWake.Core/Pipeline/RecoveryCoordinator.cs`
- Modify: `tests/DrasiWake.IntegrationTests/HostStartupTests.cs`
- Create: `tests/DrasiWake.IntegrationTests/LeaderWorkerLifecycleTests.cs`

**Interfaces:**

- `IRaftLeadership` exposes `bool IsLeader`, `bool HasQuorum`, `long Term`, and `IAsyncEnumerable<RaftLeadershipChange> WatchLeadershipAsync(CancellationToken)`.
- Define `RaftLeadershipChange` as `public sealed record RaftLeadershipChange(long Term, bool IsLeader, bool HasQuorum);`.
- `RaftLeaderHostedService` consumes `IRaftLeadership` and an `IServiceScopeFactory`; on a leader/quorum transition it starts one scoped `LeaderWorkerRuntime`; on term change, quorum loss, cancellation, or disposal it cancels and awaits that runtime before observing another epoch.
- `LeaderWorkerRuntime.StartAsync(CancellationToken)` runs recovery and periodic reconciliation before bridge receive/dispatch becomes active; `StopAsync(CancellationToken)` cancels all epoch tasks and disposes the scope.
- The scope creates fresh `SignalInbox`, `SessionPartitioner`, `SnapshotReconciler`, `RecoveryCoordinator`, `BridgeHostedService`, and `ReconciliationHostedService` per term. The current one-shot inbox/partitioner instances must not be reused after stop.

- [ ] **Step 1: Add fake-leadership lifecycle tests.** Verify follower startup creates no Drasi watch or Gateway calls; one leader epoch creates exactly one worker runtime; quorum loss cancels the runtime; a later leader term creates a new runtime; and a slow prior shutdown completes before the new runtime starts.

- [ ] **Step 2: Run the focused lifecycle test class and verify failure.**

  Run: `dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.LeaderWorkerLifecycleTests`

- [ ] **Step 3: Implement the leader coordinator and scoped runtime.** Adapt DotNext's leader notification/message-bus state to `IRaftLeadership`. Use a linked cancellation token for each term. Do not report a leadership epoch as active until the local projection is caught up and the cluster reports a writable majority.

- [ ] **Step 4: Make recovery safe to repeat.** Each runtime calls the committed interrupted-dispatch recovery command, enumerates all visible Drasi queries, reconciles authoritative snapshots, then starts dispatch polling and signal reception. Cancellation on leadership loss must not swallow non-cancellation faults; unexpected worker failures stop the epoch and are logged as fixed error categories.

- [ ] **Step 5: Run Host lifecycle and existing recovery tests.**

  Run: `dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.LeaderWorkerLifecycleTests`

  Run: `dotnet test --project tests\DrasiWake.Core.Tests\DrasiWake.Core.Tests.csproj -c Release --filter-class DrasiWake.Core.Tests.Pipeline.RecoveryCoordinatorTests`

  Expected: only the current leader starts work, and existing recovery semantics remain unchanged.

  ```text
  git add src/DrasiWake.Host src/DrasiWake.Core/Pipeline/RecoveryCoordinator.cs tests/DrasiWake.IntegrationTests
  git commit -m "feat: gate bridge workers on Raft leadership"
  ```

## Task 7: Add Authenticated Runtime Membership and Configuration Compatibility

**Files:**

- Create: `src/DrasiWake.Host/RaftMembershipContracts.cs`
- Create: `src/DrasiWake.Host/ClusterMembershipGrpcService.cs`
- Create: `src/DrasiWake.Host/ClusterConfigurationFingerprint.cs`
- Modify: `src/DrasiWake.Host/DrasiWakeHostSettings.cs`
- Modify: `src/DrasiWake.Host/DrasiWakeHostBuilder.cs`
- Modify: `src/DrasiWake.Host/HostStartupValidator.cs`
- Create: `tests/DrasiWake.IntegrationTests/ClusterMembershipTests.cs`
- Modify: `tests/DrasiWake.IntegrationTests/HostStartupTests.cs`

**Interfaces:**

- `IRaftMembershipManager` exposes:

  ```csharp
  Task AddMemberAsync(Uri endpoint, CancellationToken cancellationToken);
  Task RemoveMemberAsync(Uri endpoint, CancellationToken cancellationToken);
  ```

- `ClusterCompatibility` is `public sealed record ClusterCompatibility(string NodeId, string ApplicationVersion, string ConfigurationFingerprint);`.
- `IClusterCompatibilityProbe` exposes `Task<ClusterCompatibility> GetCompatibilityAsync(Uri endpoint, CancellationToken cancellationToken)`.
- Code-first gRPC contracts define `GetCompatibility`, `Add`, and `Remove`; all three methods require the configured Bearer Token on every request.
- `ClusterConfigurationFingerprint.Create(ContractRegistry registry, Uri drasiServerUri, IReadOnlyDictionary<string, OpenClawTargetOptions> targets, OpenClawOptions retryOptions)` returns a SHA-256 hex string over canonical validated registry content, Drasi server identity, Gateway logical names/endpoints/retention and retry behavior; it excludes secrets, node identity, listen addresses and local data paths.
- Add probes the candidate over TLS, compares application command version and configuration fingerprint with the committed cluster metadata, and only then invokes DotNext membership change. Remove routes to the leader and requires a quorum-committed change.

- [ ] **Step 1: Add failing gRPC authorization and membership tests.** Assert missing, malformed, and incorrect tokens are rejected without echoing token text; valid requests call the membership manager once; follower requests forward the operation to the current leader exactly once; no-quorum requests fail; a config/version mismatch never reaches `AddMemberAsync`.

- [ ] **Step 2: Run the focused membership tests and verify failure.**

  Run: `dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.ClusterMembershipTests`

- [ ] **Step 3: Implement code-first gRPC contracts and per-call authorization.** Compare the supplied Bearer Token using a constant-time byte comparison. Require HTTPS for both the local admin listener and candidate compatibility requests. Return fixed error codes; never return exception messages or secrets.

- [ ] **Step 4: Implement fingerprint persistence and compatibility preflight.** On first cluster bootstrap, commit the business fingerprint as cluster metadata. On restart, reject a mismatch before leader work. Before Add, call the candidate compatibility endpoint and reject a NodeId already present in membership, mismatched fingerprints, or incompatible command versions. Do not copy registry bodies or credentials through management RPC.

- [ ] **Step 5: Implement leader-routed Add/Remove and safe validation.** Use DotNext's persistent cluster configuration and `IRaftHttpCluster.AddMemberAsync`/`RemoveMemberAsync`. Validate endpoint scheme/address, duplicates, existing membership, last-member removal, current leader and quorum. Log only operation kind, stable member ID, result code and timestamp.

- [ ] **Step 6: Run membership, Host startup, and telemetry-redaction tests; commit.**

  Run: `dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.ClusterMembershipTests`

  Run: `dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.TelemetryRedactionTests`

  Expected: unauthorized or incompatible membership changes have no effect; credentials never appear in logs or error responses.

  ```text
  git add src/DrasiWake.Host tests/DrasiWake.IntegrationTests
  git commit -m "feat: secure Raft membership management"
  ```

## Task 8: Prove Three-Node Replication, Quorum Loss, and Failover

**Files:**

- Create: `tests/DrasiWake.IntegrationTests/Fixtures/RaftClusterFixture.cs`
- Create: `tests/DrasiWake.IntegrationTests/RaftFailoverTests.cs`
- Modify: `tests/DrasiWake.IntegrationTests/DrasiWake.IntegrationTests.csproj`
- Modify: `tests/DrasiWake.IntegrationTests/HostStartupTests.cs`

**Interfaces:**

- `RaftClusterFixture.CreateAsync(int nodeCount, CancellationToken)` starts exactly three real DotNext nodes with distinct free ports, test TLS certificates, Raft directories, SonnetDB directories, a common management token, and identical business configuration fingerprints.
- Fixture exposes `WaitForLeaderAsync`, `StopNodeWithoutMembershipChangeAsync`, `RestartNodeAsync`, `ReadProjectionAsync`, and `DisposeAsync`.
- Each node uses a fake Drasi source and idempotent fake Gateway, but real DotNext consensus, persistent cluster configuration, state machine, Raft adapter, and SonnetDB projection.

- [ ] **Step 1: Add failing real-cluster tests for replication and persisted restart.** Submit one `CreateOrUpdatePendingWake` command through the leader; wait until all three projections reach the same Raft index; stop and restart one follower with the same local directories; assert it restores from snapshot/log and reaches the same outbox/checkpoint state before it is marked ready.

- [ ] **Step 2: Add failing leader failover test.** Submit a pending wake, stop the current leader's transport without invoking membership removal, wait for one of the two surviving nodes to become leader, and assert it recovers and dispatches the wake.

  ```csharp
  [Fact]
  public async Task New_leader_replays_gateway_acceptance_with_the_same_idempotency_key()
  {
      await using var cluster = await RaftClusterFixture.CreateAsync(3, TestContext.Current.CancellationToken);
      var oldLeader = await cluster.WaitForLeaderAsync(TestContext.Current.CancellationToken);
      var wake = await cluster.CreatePendingWakeAsync(oldLeader);
      cluster.Gateway.HoldAcceptedResponse();
      var dispatch = cluster.DispatchOnceAsync(oldLeader, wake.Id);
      await cluster.Gateway.FirstRequestReceived.WaitAsync(TestContext.Current.CancellationToken);
      await cluster.StopNodeWithoutMembershipChangeAsync(oldLeader);
      cluster.Gateway.ReleaseFirstResponseAsTimeout();
      await Assert.ThrowsAnyAsync<OperationCanceledException>(() => dispatch);

      var newLeader = await cluster.WaitForLeaderAsync(TestContext.Current.CancellationToken);
      await cluster.WaitForStatusAsync(newLeader, wake.Id, WakeOutboxStatus.Completed);

      Assert.Single(cluster.Gateway.LogicalInvocations);
      Assert.All(cluster.Gateway.Requests, request => Assert.Equal(wake.IdempotencyKey, request.IdempotencyKey));
      await cluster.AssertAllAvailableProjectionsEqualAsync();
  }
  ```

- [ ] **Step 3: Add failing quorum-loss test.** Stop two nodes without membership removal; assert the remaining node cannot commit a command, cannot call the Gateway for a new claim, and leaves outbox state unchanged. Restart one stopped node; assert quorum returns, a leader is elected, and committed state is consistent.

- [ ] **Step 4: Add failing membership integration tests.** Add a fourth node with an independent local directory after compatibility preflight; wait for it to catch up; remove it through authenticated gRPC; assert remaining members continue committing and the removed node no longer receives entries.

- [ ] **Step 5: Implement the fixture and run the focused failover suite.** Reserve ports dynamically, use disposable per-node TLS certificates, disable graceful membership removal in the abrupt-stop test path, and clean each named test directory in fixture disposal. Do not use external Drasi/Gateway services or fixed sleeps; wait for observable term/index/status transitions with bounded timeouts.

  Run: `dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.RaftFailoverTests`

  Expected: all real-node replication, snapshot, leader change, quorum, idempotency, and membership assertions pass.

- [ ] **Step 6: Run the full integration tests and commit.**

  Run: `dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release`

  Expected: all local integration tests pass; the opt-in real service tests remain opt-in.

  ```text
  git add tests/DrasiWake.IntegrationTests
  git commit -m "test: verify Raft failover and quorum behavior"
  ```

## Task 9: Add HA Telemetry and Operational Documentation

**Files:**

- Modify: `src/DrasiWake.Core/Pipeline/BridgeTelemetry.cs`
- Modify: `src/DrasiWake.Host/RaftLeaderHostedService.cs`
- Modify: `src/DrasiWake.Host/ClusterMembershipGrpcService.cs`
- Modify: `docs/bridge-core-v1-operations.md`
- Modify: `README.md`
- Modify: `src/DrasiWake.Host/appsettings.json`
- Modify: `tests/DrasiWake.IntegrationTests/TelemetryRedactionTests.cs`

**Interfaces:**

- Add low-cardinality metrics for node role, quorum/writable state, leader transitions, commit/apply lag, snapshot export/restore, follower catch-up, membership result, leadership recovery duration, and failover duration.
- Add activities for leader recovery and membership operations. Hash member endpoints before telemetry; use fixed result/error codes.
- Document the three-node environment variables and directories, TLS certificate/trust configuration, shared management-token injection, safe Add/Remove sequence, no-quorum behavior, restart/recovery, and the unchanged single-node developer invocation.

- [ ] **Step 1: Add failing telemetry redaction assertions.** Verify Raft member URLs, management token, certificate password, payload facts, and exception text are absent from emitted logs/metric tags.
- [ ] **Step 2: Implement bounded-cardinality instruments and safe logs.** Reuse the existing `BridgeTelemetry` ActivitySource/Meter; do not add raw NodeId, endpoint, or arbitrary exception text as metric dimensions.
- [ ] **Step 3: Update the operations guide and README.** State that every member has its own SonnetDB/Raft directories; describe three-node quorum and recovery ordering; include safe secret-provider keys but no credentials; warn that only majority-backed work continues and Gateway delivery is at least once.
- [ ] **Step 4: Verify the documented commands and configuration keys against Host settings tests.** Include one three-node sample with distinct node IDs/paths/endpoints, TLS certificate references, and a secret-provider token name; keep the checked-in `appsettings.json` single-node and secret-free.
- [ ] **Step 5: Run focused redaction tests and full Release verification.**

  Run: `dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.TelemetryRedactionTests`

  Run: `dotnet test --solution DrasiWake.sln --configuration Release`

  Run: `dotnet publish src\DrasiWake.Host\DrasiWake.Host.csproj -c Release -o artifacts\publish`

  Expected: all required tests pass; existing opt-in real-service tests remain skipped unless explicitly enabled; the Host publishes with all required DotNext and ASP.NET Core dependencies.

  ```text
  git add README.md docs/bridge-core-v1-operations.md src/DrasiWake.Core/Pipeline/BridgeTelemetry.cs src/DrasiWake.Host tests/DrasiWake.IntegrationTests/TelemetryRedactionTests.cs
  git commit -m "docs: document DrasiWake Raft HA operations"
  ```

## Final Acceptance Checklist

- [ ] Three real DotNext voting nodes converge on every committed command and complete snapshot restoration.
- [ ] A single node failure preserves quorum; the new leader resumes only after local projection catch-up and Drasi snapshot recovery.
- [ ] Loss of quorum prevents new claims and Gateway calls for newly unclaimed work.
- [ ] Gateway acceptance ambiguity replays only with the original idempotency key and produces one logical invocation.
- [ ] `Accepted` and its checkpoint remain atomic across all replicas and restart.
- [ ] Runtime Add/Remove is TLS-protected, Bearer-authenticated, compatibility-checked, quorum-committed, and audited without secrets.
- [ ] A new member with mismatched application version/config fingerprint is rejected before membership change.
- [ ] Legacy single-node configuration and all existing tests remain functional through the Raft state-machine path.
- [ ] Operations docs describe independent storage, quorum limits, TLS, token injection, membership, recovery, and at-least-once semantics.
