# OpenClaw Target Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将必填的 binding 逻辑 target 持久化到 wake outbox，并让 DrasiWake Host 按该 target 将调用和状态查询路由到各自配置的 OpenClaw Gateway。

**Architecture:** Core binding 契约声明 `OpenClawTarget`，Host 在 `DrasiWake:OpenClaw:Targets` 中解析具名 Gateway 配置，并在启动时验证 registry、重试年龄和持久 outbox 引用。Snapshot reconciler 将 target 写入新 wake，SonnetDB migration 为现存未完成记录添加可回填的 target；Host 启动恢复时仅能从当前同 ID binding 回填旧记录。Target-aware `IWakeSink` 通过每个 target 的 endpoint 和可选凭据创建 OpenClaw client，并让调用与状态查询使用同一逻辑 target。

**Tech Stack:** .NET 10、现有 Core / Host / OpenClaw adapter / SonnetDB persistence 项目、YamlDotNet、EF Core 与 SonnetDB、xUnit v3 和 Microsoft.Testing.Platform。

## Global Constraints

- 每个 binding 必须显式指定逻辑 target；不提供隐式 `default` target 或回退路由。
- Gateway 地址和凭据保存在 Host 配置中，不放入契约 YAML。
- 将选定的逻辑 target 持久化到 outbox，避免 binding 修改后静默改变已排队任务的路由。
- 派发前验证 target 配置和 Gateway 幂等键保留期。
- `BaseAddress`：绝对 HTTP 或 HTTPS Gateway URL。
- `BearerToken`：Gateway 专属凭据；当 Gateway 要求 bearer 认证时配置，并通过环境变量、User Secrets 或等效 secret provider 提供。无需认证的 Gateway 可以省略此项。
- `GatewayIdempotencyRetention`：该 Gateway 的幂等键保留时长。
- 现有共享客户端重试设置继续由 Host 全局管理，除非实现过程中证据表明它们必须按 target 区分。
- 使用 binding 的 `Retry.MaxAge` 计算每个 target 所关联 binding 的最大重试期；该 target 配置的幂等保留时长必须不短于此值。
- binding 引用的 target 必须在启动时完整配置且有效。不得从全局 OpenClaw 配置隐式继承 URI 或凭据。
- Target 名称是稳定的逻辑身份；有意修改某名称对应的 endpoint，会将之后按该名称派发的任务导向新 endpoint。
- 只要持久化任务仍引用某 target，运维方就必须保留该 target 配置。
- 若可派发或仍需跟踪的 outbox work 引用了未配置的 target，启动校验必须给出明确失败，不得静默选择其他 target。
- 迁移前创建、因而尚无 target 的记录，由启动流程根据当前加载的、ID 相同的 binding 执行一次性回填。
- 如果旧的未完成记录没有匹配 binding，或该 binding 没有有效配置的 target，则启动失败。
- 无法解析 target 的 outbox item 不得发送到其他 Gateway。
- 网络或 Gateway 调用失败继续使用现有重试和 dead-letter 机制，并始终针对已选定的 target 执行。
- 除选择 Gateway 外，不改变 MetaSkill 调用负载、重试调度、速率限制或投递语义。

---

## 文件布局

| 文件 | 职责 |
| --- | --- |
| `src/DrasiWake.Core/Contracts/BridgeBinding.cs`、`ContractRegistryLoader.cs` | 将 `openClawTarget` 纳入强类型 binding 与 YAML 加载验证。 |
| `src/DrasiWake.Host/DrasiWakeHostSettings.cs`、`HostStartupValidator.cs`、`DrasiWakeHostBuilder.cs` | 读取具名 target 配置、验证 registry 和持久 work，并注册 target-aware sink。 |
| `src/DrasiWake.Adapters.OpenClaw/OpenClawOptions.cs`、`OpenClawMetaInvocationClient.cs` | 保留共享 HTTP 重试策略，将 endpoint、凭据与保留期作为 target 专属选项。 |
| `src/DrasiWake.Core/Domain/WakeRequest.cs`、`WakeOutboxItem.cs`、`Pipeline/SnapshotReconciler.cs`、`Pipeline/OutboxDispatcher.cs` | 将选中的逻辑 target 从 binding 传递到 outbox 和派发请求。 |
| `src/DrasiWake.Persistence.SonnetDB/Entities/WakeOutbox.cs`、`BridgeDbContext.cs`、`SonnetBridgeStore.cs`、`Migrations/` | 保存 target，并支持旧的未完成 outbox 记录启动回填。 |
| `src/DrasiWake.Host/TargetRoutedWakeSink.cs` | 按 `WakeRequest.OpenClawTarget` 选择 OpenClaw client；不做默认路由。 |
| `src/DrasiWake.Host/appsettings.json`、`contracts/*.yaml`、测试 YAML 与 inline registry | 将每个现有 binding 和 Host 示例配置迁移到显式 target。 |
| `tests/DrasiWake.Core.Tests/`、`tests/DrasiWake.Adapters.OpenClaw.Tests/`、`tests/DrasiWake.Persistence.SonnetDB.Tests/`、`tests/DrasiWake.IntegrationTests/` | 验证契约、端到端路由、迁移、重启回填与启动拒绝行为。 |
| `src/DrasiWake.AppHost/AppHost.cs`、`README.md`、`docs/bridge-core-v1-operations.md`、`docs/development/aspire-local-environment.md` | 将 Aspire 地址/secret 注入新 target 配置，并说明新的运行配置方式。 |

## Task 1: Require a Target in Every Binding

**Files:**

- Modify: `src/DrasiWake.Core/Contracts/BridgeBinding.cs`
- Modify: `src/DrasiWake.Core/Contracts/ContractRegistryLoader.cs`
- Modify: `tests/DrasiWake.Core.Tests/Contracts/ContractRegistryTests.cs`
- Modify: `tests/DrasiWake.Core.Tests/Contracts/Fixtures/valid-default.yaml`
- Modify: `tests/DrasiWake.Core.Tests/Contracts/Fixtures/invalid-shared-identity.yaml`
- Modify: `tests/DrasiWake.Core.Tests/Contracts/Fixtures/invalid-trigger-conflict.yaml`
- Modify: `tests/DrasiWake.Core.Tests/Pipeline/SessionPartitionerTests.cs`
- Modify: `tests/DrasiWake.Core.Tests/Pipeline/RecoveryCoordinatorTests.cs`
- Modify: `tests/DrasiWake.Core.Tests/Domain/SessionIdentityResolverTests.cs`
- Modify: `tests/DrasiWake.Core.Tests/Pipeline/SnapshotReconcilerTests.cs`
- Modify: `tests/DrasiWake.Core.Tests/Pipeline/OutboxDispatcherTests.cs`
- Modify: `src/DrasiWake.Host/contracts/sample-binding.yaml`
- Modify: `src/DrasiWake.Host/contracts/aspire-sensor-binding.yaml`
- Modify: `tests/DrasiWake.IntegrationTests/Fixtures/BridgeTestFixture.cs`
- Modify: `tests/DrasiWake.IntegrationTests/HostStartupTests.cs`

**Interfaces:**

- Consumes: Existing YAML key naming via YamlDotNet `CamelCaseNamingConvention`.
- Produces: `BridgeBinding.OpenClawTarget : string`, populated from required registry key `openClawTarget`.

- [x] **Step 1: Add failing loader tests for missing, whitespace-only, and valid targets.** Assert invalid candidates contain `routing.target_required`, while a trimmed, non-empty target is available as `binding.OpenClawTarget`. Keep other validation errors out of the way by using the existing valid fixture and schema.

- [x] **Step 2: Run the focused contract tests and verify the new cases fail.**

  Run: `dotnet test --project tests\DrasiWake.Core.Tests\DrasiWake.Core.Tests.csproj -c Release --filter-class DrasiWake.Core.Tests.Contracts.ContractRegistryTests`

  Expected: the new test cases fail because `BridgeBinding` and loader validation do not yet expose/require the target.

- [x] **Step 3: Add the binding property and loader validation.** Add `string OpenClawTarget` to `BridgeBinding`; add `string? OpenClawTarget` to the loader's private `BindingDocument`; reject `null`, empty, or whitespace values with error code `routing.target_required`; trim the valid value and pass it into the constructed `BridgeBinding`. Do not infer any value from `MetaSkill`, a global setting, or a default.

- [x] **Step 4: Add an explicit target to every existing binding source.** Use `sample-gateway` in the generic/sample/Core/integration registry fixtures and `sensor-gateway` in `aspire-sensor-binding.yaml`. Also add `sample-gateway` to both bindings in `invalid-trigger-conflict.yaml` so that fixture continues to test its trigger conflict rather than failing earlier for a missing target. Update every direct `new BridgeBinding(...)` call in `SessionPartitionerTests` and `RecoveryCoordinatorTests` to pass `sample-gateway`.

- [x] **Step 5: Run Core and integration contract/startup tests.**

  Run: `dotnet test --project tests\DrasiWake.Core.Tests\DrasiWake.Core.Tests.csproj -c Release`

  Expected: PASS; all pre-existing contract checks retain their original validation purpose, and missing/blank targets are rejected.

- [x] **Step 6: Commit the contract change.**

  ```powershell
  git add src/DrasiWake.Core/Contracts/BridgeBinding.cs src/DrasiWake.Core/Contracts/ContractRegistryLoader.cs tests/DrasiWake.Core.Tests/Contracts src/DrasiWake.Host/contracts tests/DrasiWake.IntegrationTests/Fixtures/BridgeTestFixture.cs tests/DrasiWake.IntegrationTests/HostStartupTests.cs
  git commit -m "feat: require OpenClaw target in bindings" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
  ```

## Task 2: Configure and Validate Named Gateway Targets

**Files:**

- Create: `src/DrasiWake.Adapters.OpenClaw/OpenClawTargetOptions.cs`
- Modify: `src/DrasiWake.Host/DrasiWakeHostSettings.cs`
- Modify: `src/DrasiWake.Host/HostStartupValidator.cs`
- Modify: `src/DrasiWake.Host/appsettings.json`
- Modify: `tests/DrasiWake.IntegrationTests/HostStartupTests.cs`

**Interfaces:**

- Consumes: `BridgeBinding.OpenClawTarget` and `BridgeBinding.Retry.MaxAge` from Task 1.
- Produces:
  - `OpenClawTargetOptions(Uri BaseAddress, string? BearerToken, TimeSpan GatewayIdempotencyRetention)`.
  - `DrasiWakeHostSettings.OpenClawTargets : IReadOnlyDictionary<string, OpenClawTargetOptions>`.
  - Startup validation of each binding's target and per-target idempotency retention.

- [x] **Step 1: Add startup tests for target option parsing and validation.** Cover a valid HTTP URL with no token, a valid HTTPS URL with a token, relative/unsupported URLs, whitespace-only and malformed non-empty bearer credentials, unknown binding target names, and a target whose retention is shorter than the maximum `Retry.MaxAge` of its referencing bindings. Assert errors identify the target/configuration key, never the credential value.

- [x] **Step 2: Run the focused startup and options tests to establish the failing baseline.**

  Run: `dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.HostStartupTests`

  Expected: the target-specific configuration cases fail; existing startup tests still compile after fixtures are updated in Task 1.

- [x] **Step 3: Introduce and validate target-specific options.** Define:

  ```csharp
  public sealed record OpenClawTargetOptions(
      Uri BaseAddress,
      string? BearerToken,
      TimeSpan GatewayIdempotencyRetention);
  ```

  Validate that `BaseAddress` is absolute HTTP/HTTPS, retention is positive, and a supplied `BearerToken` matches RFC 6750 `b64token` syntax before constructing `AuthenticationHeaderValue` (reject whitespace and invalid header characters without including the token in diagnostics). Keep the current global client fields in place temporarily so the existing single-client registration continues working until Task 5; startup validation must use only `OpenClawTargets` and must not treat those legacy fields as a target fallback.

- [x] **Step 4: Bind target children under `DrasiWake:OpenClaw:Targets`.** Parse each child as one named target, use ordinal target-name matching for binding references, and fail on malformed target data. In `HostStartupValidator`, verify every loaded binding references an existing target and compare that target's `GatewayIdempotencyRetention` with the maximum `Retry.MaxAge` among bindings referencing that target. Do not compare against a global retry-age setting or use an unconfigured fallback.

- [x] **Step 5: Add an explicit `sample-gateway` target to Host config.** Use the existing sample Gateway URL, omit a credential, and configure its retention as `30.00:00:00`. Keep the old global endpoint fields only for the existing client registration until Task 5; no binding validation may inherit from those fields.

- [x] **Step 6: Run focused startup, options, and registry tests.**

  Run: `dotnet test --project tests\DrasiWake.Adapters.OpenClaw.Tests\DrasiWake.Adapters.OpenClaw.Tests.csproj -c Release --filter-class DrasiWake.Adapters.OpenClaw.Tests.OpenClawOptionsTests; dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.HostStartupTests`

  Expected: PASS; malformed/unknown targets and insufficient target retention fail before normal Host startup, while a valid named configuration succeeds.

- [x] **Step 7: Commit named target configuration and validation.**

  ```powershell
  git add src/DrasiWake.Adapters.OpenClaw/OpenClawTargetOptions.cs src/DrasiWake.Host/DrasiWakeHostSettings.cs src/DrasiWake.Host/HostStartupValidator.cs src/DrasiWake.Host/appsettings.json tests/DrasiWake.IntegrationTests/HostStartupTests.cs
  git commit -m "feat: configure named OpenClaw targets" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
  ```

## Task 3: Persist Target on Wake and Create the SonnetDB Migration

**Files:**

- Modify: `src/DrasiWake.Core/Domain/WakeRequest.cs`
- Modify: `src/DrasiWake.Core/Domain/WakeOutboxItem.cs`
- Modify: `src/DrasiWake.Core/Pipeline/SnapshotReconciler.cs`
- Modify: `src/DrasiWake.Core/Pipeline/OutboxDispatcher.cs`
- Modify: `src/DrasiWake.Persistence.SonnetDB/Entities/WakeOutbox.cs`
- Modify: `src/DrasiWake.Persistence.SonnetDB/BridgeDbContext.cs`
- Modify: `src/DrasiWake.Persistence.SonnetDB/SonnetBridgeStore.cs`
- Create: `src/DrasiWake.Persistence.SonnetDB/Migrations/20261004005951_AddOpenClawTargetToWakeOutbox.cs`
- Create: `src/DrasiWake.Persistence.SonnetDB/Migrations/20261004005951_AddOpenClawTargetToWakeOutbox.Designer.cs`
- Modify: `src/DrasiWake.Persistence.SonnetDB/Migrations/BridgeDbContextModelSnapshot.cs`
- Modify: `tests/DrasiWake.Core.Tests/Pipeline/SnapshotReconcilerTests.cs`
- Modify: `tests/DrasiWake.Core.Tests/Pipeline/RecoveryCoordinatorTests.cs`
- Modify: `tests/DrasiWake.Core.Tests/Pipeline/SessionPartitionerTests.cs`
- Modify: `tests/DrasiWake.Core.Tests/Pipeline/OutboxDispatcherTests.cs`
- Modify: `tests/DrasiWake.IntegrationTests/TelemetryRedactionTests.cs`
- Modify: `tests/DrasiWake.IntegrationTests/RealServiceContractTests.cs`
- Modify: `tests/DrasiWake.IntegrationTests/GatewayIdempotencyTests.cs`
- Modify: `tests/DrasiWake.IntegrationTests/CrashRecoveryTests.cs`
- Modify: `tests/DrasiWake.IntegrationTests/Fixtures/BridgeTestFixture.cs`
- Modify: `tests/DrasiWake.Adapters.OpenClaw.Tests/MetaInvocationRequestTests.cs`
- Modify: `tests/DrasiWake.Persistence.SonnetDB.Tests/AtomicAcceptanceTests.cs`

**Interfaces:**

- Consumes: Required `BridgeBinding.OpenClawTarget`.
- Produces:
  - `WakeOutboxItem.OpenClawTarget : string`.
  - `WakeRequest.OpenClawTarget : string`.
  - New pending wakes persist the binding's target alongside `Skill`.
  - The migration adds a nullable database column so pre-migration rows can be populated from the loaded binding before the store materializes a domain item.

- [x] **Step 1: Add failing assertions proving the target survives reconciliation and database reopen.** In `SnapshotReconcilerTests`, assert the created outbox item's target equals the binding target. In integration recovery tests, assert the reloaded wake has the same target after reopening the database, even if the current registry target is later changed.

- [x] **Step 2: Run the focused tests and verify target assertions fail before implementation.**

  Run: `dotnet test --project tests\DrasiWake.Core.Tests\DrasiWake.Core.Tests.csproj -c Release --filter-class DrasiWake.Core.Tests.Pipeline.SnapshotReconcilerTests; dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.CrashRecoveryTests`

  Expected: the assertions fail because no target is currently represented in wake domain/persistence models.

- [x] **Step 3: Add `OpenClawTarget` to both domain records and propagate it when reconciling and dispatching.** Set it directly from `binding.OpenClawTarget` when `SnapshotReconciler` creates a `WakeOutboxItem`; copy `item.OpenClawTarget` into `WakeRequest` in `OutboxDispatcher`. Update every constructor call site reported by `rg "new WakeRequest\\(|new WakeOutboxItem\\(" src tests` with explicit target values from its binding/test scenario.

- [x] **Step 4: Add target persistence and generate the migration.** Add `OpenClawTarget` to the persistence entity and EF model; make the migration's new column nullable for existing rows. Update `ToEntity`, `ToDomain`, and `Copy` in `SonnetBridgeStore`; `ToDomain` must fail explicitly if a dispatchable record still has a null/blank target rather than producing a routable default. Generate the migration with the repository's EF tooling and update the model snapshot. Update the migration-count assertion in `AtomicAcceptanceTests` from one migration to the existing initial migration plus this new migration.

- [x] **Step 5: Run Core and persistence tests, including migration application and atomic acceptance regression coverage.**

  Run: `dotnet test --project tests\DrasiWake.Core.Tests\DrasiWake.Core.Tests.csproj -c Release; dotnet test --project tests\DrasiWake.Persistence.SonnetDB.Tests\DrasiWake.Persistence.SonnetDB.Tests.csproj -c Release`

  Expected: PASS; fresh databases migrate and existing acceptance/checkpoint transaction behavior remains atomic.

- [x] **Step 6: Run integration recovery tests and verify an updated binding does not mutate persisted work.** Enqueue a wake with target `sample-gateway`, activate a replacement binding with another target name, reopen the database, and assert the queued row still contains `sample-gateway`.

  Run: `dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.CrashRecoveryTests`

  Expected: PASS; both target and idempotency key remain the values written when the outbox row was created.

- [x] **Step 7: Commit domain and persistence changes.**

  ```powershell
  git add src/DrasiWake.Core/Domain src/DrasiWake.Core/Pipeline/SnapshotReconciler.cs src/DrasiWake.Core/Pipeline/OutboxDispatcher.cs src/DrasiWake.Persistence.SonnetDB tests/DrasiWake.Core.Tests/Pipeline tests/DrasiWake.IntegrationTests/TelemetryRedactionTests.cs tests/DrasiWake.IntegrationTests/RealServiceContractTests.cs tests/DrasiWake.IntegrationTests/GatewayIdempotencyTests.cs tests/DrasiWake.IntegrationTests/CrashRecoveryTests.cs tests/DrasiWake.IntegrationTests/Fixtures/BridgeTestFixture.cs tests/DrasiWake.Persistence.SonnetDB.Tests/AtomicAcceptanceTests.cs
  git commit -m "feat: persist OpenClaw target with wake outbox" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
  ```

## Task 4: Backfill and Validate Persisted Targets Before Dispatch

**Files:**

- Modify: `src/DrasiWake.Core/Abstractions/IBridgeStore.cs`
- Modify: `src/DrasiWake.Persistence.SonnetDB/SonnetBridgeStore.cs`
- Modify: `src/DrasiWake.Host/HostStartupValidator.cs`
- Modify: `tests/DrasiWake.Persistence.SonnetDB.Tests/AtomicAcceptanceTests.cs`
- Modify: `tests/DrasiWake.IntegrationTests/HostStartupTests.cs`
- Create: `tests/DrasiWake.IntegrationTests/OutboxTargetRecoveryTests.cs`

**Interfaces:**

- Consumes: Migrated nullable database column, active registry, and configured target names from Tasks 2–3.
- Produces:

  ```csharp
  ValueTask EnsureOpenClawTargetsAsync(
      IReadOnlyDictionary<string, string> targetByBindingId,
      IReadOnlySet<string> configuredTargetNames,
      CancellationToken cancellationToken);
  ```

- [x] **Step 1: Add failing startup-recovery tests for legacy rows.** Cover (a) an old pending row with a matching binding being backfilled, (b) an old pending row whose binding ID is absent failing startup with the row/binding identified, (c) a persisted active row referring to a removed target failing startup, and (d) a completed historical row with no target not preventing startup.

- [x] **Step 2: Run the focused tests to establish the missing migration recovery behavior.**

  Run: `dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.OutboxTargetRecoveryTests`

  Expected: the missing-target records are currently neither backfilled nor validated.

- [x] **Step 3: Implement `EnsureOpenClawTargetsAsync` in `SonnetBridgeStore`.** In one database transaction, inspect records in `Pending`, `Dispatching`, `RetryScheduled`, `Accepted`, or `Executing` state. For a null target, resolve only `BindingId` in `targetByBindingId`, assign that exact binding target, and save. Reject missing binding mappings, blank values, or any active row whose target is absent from `configuredTargetNames`; include safe binding/outbox identifiers in the error and never include credentials. Leave terminal `Completed`, `DeadLetter`, and `Superseded` rows untouched.

- [x] **Step 4: Call the store check after migration and before Host startup completes.** Build the `targetByBindingId` mapping from the successfully loaded candidate, use the configured target key set, and run the check after `context.Database.MigrateAsync` while the existing database ownership lease is held. Do not invoke dispatch or recovery if the check fails.

- [x] **Step 5: Verify old-schema migration behavior.** Add a persistence regression test that migrates a temporary database only through `20261002145302_InitialBridgeState`, inserts a legacy pending row, applies the new migration, runs `EnsureOpenClawTargetsAsync`, then asserts the stored target. Also assert that the migration itself leaves the new column nullable until startup has registry context.

- [x] **Step 6: Run persistence and startup recovery tests.**

  Run: `dotnet test --project tests\DrasiWake.Persistence.SonnetDB.Tests\DrasiWake.Persistence.SonnetDB.Tests.csproj -c Release; dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.HostStartupTests; dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.OutboxTargetRecoveryTests`

  Expected: PASS; legacy dispatchable records recover only from their current binding, invalid active references fail startup, and terminal historical rows do not need a Gateway configuration.

- [x] **Step 7: Commit startup backfill and validation.**

  ```powershell
  git add src/DrasiWake.Core/Abstractions/IBridgeStore.cs src/DrasiWake.Persistence.SonnetDB/SonnetBridgeStore.cs src/DrasiWake.Host/HostStartupValidator.cs tests/DrasiWake.Persistence.SonnetDB.Tests/AtomicAcceptanceTests.cs tests/DrasiWake.IntegrationTests/HostStartupTests.cs tests/DrasiWake.IntegrationTests/OutboxTargetRecoveryTests.cs
  git commit -m "fix: validate and backfill persisted OpenClaw targets" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
  ```

## Task 5: Route Invocation and Status Through the Persisted Target

**Files:**

- Modify: `src/DrasiWake.Adapters.OpenClaw/OpenClawMetaInvocationClient.cs`
- Modify: `src/DrasiWake.Adapters.OpenClaw/OpenClawOptions.cs`
- Modify: `src/DrasiWake.Host/DrasiWakeHostSettings.cs`
- Modify: `src/DrasiWake.Host/appsettings.json`
- Create: `src/DrasiWake.Host/TargetRoutedWakeSink.cs`
- Modify: `src/DrasiWake.Host/DrasiWakeHostBuilder.cs`
- Modify: `tests/DrasiWake.Adapters.OpenClaw.Tests/MetaInvocationRequestTests.cs`
- Modify: `tests/DrasiWake.Adapters.OpenClaw.Tests/OpenClawOptionsTests.cs`
- Modify: `tests/DrasiWake.IntegrationTests/HostStartupTests.cs`
- Create: `tests/DrasiWake.IntegrationTests/TargetRoutedWakeSinkTests.cs`

**Interfaces:**

- Consumes: `WakeRequest.OpenClawTarget`, `OpenClawOptions` shared retry settings, and the validated `DrasiWakeHostSettings.OpenClawTargets` map.
- Produces:
  - `OpenClawMetaInvocationClient(HttpClient httpClient, OpenClawOptions options, OpenClawTargetOptions targetOptions)`.
  - `TargetRoutedWakeSink : IWakeSink`, selecting an OpenClaw client by exact target name for both `InvokeAsync(WakeRequest, CancellationToken)` and `GetStatusAsync(WakeRequest, CancellationToken)`.

- [x] **Step 1: Add failing route tests with two target-specific handlers.** Send one `WakeRequest` to each configured target and assert the two exact request URLs and bearer headers. Assert neither token appears on the other handler, both calls retain the original `skill`, `input`, `sessionId`, and `Idempotency-Key`, and status replay goes to the same target as invocation. Assert an unknown target throws explicitly without making an HTTP request.

- [x] **Step 2: Run the focused route tests and verify they fail before routing exists.**

  Run: `dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.TargetRoutedWakeSinkTests`

  Expected: routing tests fail because the current Host registers one global `OpenClawMetaInvocationClient`.

- [x] **Step 3: Pass target options to the OpenClaw adapter and remove legacy global endpoint settings.** Change the client constructor to accept `OpenClawOptions` shared retry settings and `OpenClawTargetOptions`; set `HttpClient.BaseAddress` from the target and attach only that target's optional bearer token. Remove the legacy global `BaseAddress`, `BearerToken`, `GatewayIdempotencyRetention`, and `MaximumOutboxRetryAge` fields from `OpenClawOptions`, `DrasiWakeHostSettings.FromConfiguration`, and `appsettings.json`. Preserve the current POST path, JSON contract, response interpretation, and retry behavior.

- [x] **Step 4: Implement `TargetRoutedWakeSink` and register it in Host DI.** Resolve only the exact `request.OpenClawTarget` key; cache/reuse clients by target with each client bound to that target's URI/token. Do not construct a default client for absent, null, or unknown target names. Both `InvokeAsync` and `GetStatusAsync` must use the same resolver.

- [x] **Step 5: Run adapter and routing tests.**

  Run: `dotnet test --project tests\DrasiWake.Adapters.OpenClaw.Tests\DrasiWake.Adapters.OpenClaw.Tests.csproj -c Release; dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.TargetRoutedWakeSinkTests`

  Expected: PASS; each call/status replay uses only the selected Gateway while existing MetaSkill request and idempotency behavior stays unchanged.

- [x] **Step 6: Run Core dispatch regression tests.**

  Run: `dotnet test --project tests\DrasiWake.Core.Tests\DrasiWake.Core.Tests.csproj -c Release --filter-class DrasiWake.Core.Tests.Pipeline.OutboxDispatcherTests`

  Expected: PASS; dispatch retry and dead-letter behavior is unchanged, and generated `WakeRequest` retains the outbox target.

- [x] **Step 7: Commit target-aware invocation routing.**

  ```powershell
  git add src/DrasiWake.Adapters.OpenClaw/OpenClawOptions.cs src/DrasiWake.Adapters.OpenClaw/OpenClawMetaInvocationClient.cs src/DrasiWake.Host/TargetRoutedWakeSink.cs src/DrasiWake.Host/DrasiWakeHostBuilder.cs tests/DrasiWake.Adapters.OpenClaw.Tests/MetaInvocationRequestTests.cs tests/DrasiWake.IntegrationTests/TargetRoutedWakeSinkTests.cs
  git commit -m "feat: route OpenClaw calls by persisted target" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
  ```

## Task 6: Update Aspire Wiring, Examples, Operations Documentation, and Full Regression Tests

**Files:**

- Modify: `src/DrasiWake.AppHost/AppHost.cs`
- Modify: `src/DrasiWake.Host/appsettings.json`
- Modify: `src/DrasiWake.Host/contracts/sample-binding.yaml`
- Modify: `src/DrasiWake.Host/contracts/aspire-sensor-binding.yaml`
- Modify: `tests/DrasiWake.IntegrationTests/HostStartupTests.cs`
- Modify: `README.md`
- Modify: `docs/bridge-core-v1-operations.md`
- Modify: `docs/development/aspire-local-environment.md`

**Interfaces:**

- Consumes: The sample binding's `sample-gateway` and Aspire sensor binding's `sensor-gateway`; target-aware settings and sink from Tasks 1–5.
- Produces: Aspire injects its discovered Gateway URL and secret into `DrasiWake:OpenClaw:Targets:sensor-gateway:*`, never into a global OpenClaw endpoint or credential setting.

- [x] **Step 1: Update AppHost environment injection.** Set `DrasiWake__OpenClaw__Targets__sensor-gateway__BaseAddress` from `ComposeEnvironmentState.OpenClawBaseAddress`; pass the Aspire auth parameter only as `DrasiWake__OpenClaw__Targets__sensor-gateway__BearerToken`; set that target's `GatewayIdempotencyRetention` to `30.00:00:00`. Keep the existing explicit registry override for the sensor E2E binding.

- [x] **Step 2: Add focused configuration coverage for the Aspire target keys.** Extend `HostStartupTests` to load the same `Targets:sensor-gateway` hierarchy and verify endpoint, token, and retention bind to that named target; retain tests asserting invalid targets fail before migrations or dispatch.

- [x] **Step 3: Update operational documentation.** Replace deprecated root-level Gateway endpoint and credential references in `README.md`, `docs/bridge-core-v1-operations.md`, and `docs/development/aspire-local-environment.md` with the named-target configuration keys. Explain that registry bindings must name a configured target, credentials are target-specific secret-provider values, and removing a target still used by active outbox work prevents startup.

- [x] **Step 4: Search for stale global endpoint/token references and binding omissions.**

  Run: `rg -n "OpenClaw(__|:)(Targets(__|:))?.*(BaseAddress|BearerToken)|metaSkill:" README.md docs src tests`

  Expected: no obsolete global endpoint/token references remain; every YAML/inline registry binding includes `openClawTarget`.

- [x] **Step 5: Run the full test suite and Host release build.**

  Run: `dotnet test --solution DrasiWake.sln --configuration Release; dotnet build src\DrasiWake.Host\DrasiWake.Host.csproj -c Release`

  Expected: PASS. Real-service/Aspire tests remain subject to their existing opt-in environment variables; do not enable them unless the local Compose prerequisites and credentials are intentionally configured.

- [x] **Step 6: Commit the Aspire and documentation updates.**

  ```powershell
  git add src/DrasiWake.AppHost/AppHost.cs src/DrasiWake.Host/appsettings.json src/DrasiWake.Host/contracts README.md docs/bridge-core-v1-operations.md docs/development/aspire-local-environment.md tests/DrasiWake.IntegrationTests/HostStartupTests.cs
  git commit -m "docs: document named OpenClaw target configuration" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
  ```

## Self-Review Checklist

- **Spec coverage:** Binding validation and no fallback are covered by Tasks 1–2; target-specific URL/token/retention validation by Task 2; outbox persistence and binding-change stability by Task 3; schema migration, legacy backfill, missing binding/config failure, and terminal historical rows by Task 4; per-target endpoint/token isolation and same-target status replay by Task 5; Aspire, sample, and operations documentation by Task 6.
- **Failure behavior:** Invalid/unknown target configuration fails startup; active persisted rows cannot silently route elsewhere; runtime target resolution has no default; transient Gateway/network failures keep existing retry/dead-letter policy.
- **Completeness scan:** All tasks name concrete repository paths, test projects, expected outcomes, and target/configuration contracts; no generic unfinished steps remain.
- **Type consistency:** The binding, outbox item, wake request, target options map, store recovery method, OpenClaw adapter constructor, and routed sink use the same `OpenClawTarget`/`OpenClawTargetOptions` names throughout.
