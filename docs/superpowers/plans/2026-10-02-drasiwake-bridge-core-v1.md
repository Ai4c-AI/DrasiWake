# DrasiWake Bridge Core V1 实施计划

> **执行说明：** 实施人员必须逐任务执行本计划，使用 `superpowers:subagent-driven-development`（推荐）或 `superpowers:executing-plans`。任务步骤使用 `- [ ]` 勾选格式。

**目标：** 交付一个单实例 .NET 10 Bridge，将 Drasi 查询快照最终收敛为显式路由、可持久重试的 OpenClaw.NET MetaSkill 调用。

**架构：** 使用 Generic Host 组合与传输无关的 Core、Drasi Server HTTP/SSE adapter、OpenClaw.NET integration adapter，以及基于单个嵌入式 SonnetDB 目录的 EF Core 持久化 adapter。Drasi `attach` frame 仅作为对账提示，权威 `results` 快照生成按会话分区的持久 outbox 工作项。Gateway 受理与已受理快照 checkpoint 在同一事务中提交。各边界分别进行契约测试，最终门禁覆盖进程重启、受理结果不确定和幂等重放。

**技术栈：** .NET 10；Microsoft.Extensions.Hosting 10.0.12、`BackgroundService`；`System.Threading.Channels`；`TimeProvider`、`PeriodicTimer`；`System.Threading.RateLimiting`；`Microsoft.Extensions.Http.Resilience` 10.10.0；System.Text.Json；JsonSchema.Net 9.4.0；YamlDotNet 18.1.0；EF Core、Microsoft.EntityFrameworkCore.Design 10.0.12；SonnetDB.EntityFrameworkCore 4.0.0；OpenTelemetry.Extensions.Hosting 1.19.1；`xunit.v3` 4.0.1。

## 全局约束

- 目标框架为 `net10.0`。V1 为独立、单进程、单活动实例部署；NativeAOT 不属于 V1 门禁。
- Drasi 查询 `attach` SSE 只是提示；查询 `results` 是权威当前快照。不得把查询 `events/stream` 当作结果变化流。
- 保证对可读取的当前快照最终收敛，并重试已持久化的 outbox；不保证每个中间状态都产生唤醒，也不承诺任意介质故障下零丢失。
- Drasi attach 不提供可恢复游标或停机期间重放保证；必须拒绝要求保留每个中间变化的绑定。
- 启动、attach 连接/重连、Drasi 恢复、周期计时器及队列溢出都触发对账。有界内存 Channel 满时标记对应查询待对账；这不代表提示已持久接收。
- 快照读取失败时不得推进已接受指纹或 checkpoint。
- HTTP 注入前先持久化 wake。Gateway 受理与 checkpoint 更新必须在一个 EF Core 事务内提交。
- 只调用 `POST /api/integration/meta-invocations`，显式传入目标 MetaSkill、`input`、`sessionId` 和稳定的 `Idempotency-Key`。不得改用 `/messages`、`/workflows`、coding backend stdin 或 trigger phrase 匹配。
- 对不确定/未确认调用使用原幂等键重试。Gateway 幂等记录保留期必须不短于 outbox 最长重试年龄。HTTP 受理不等于 MetaSkill 执行完成。
- 同一解析会话身份的工作严格串行，不同身份可并行；所有内存 Channel 均有界。
- 默认按查询隔离会话。跨查询共享必须显式提供 `(ontology/context, entity type, entity ID)` 规范身份。缺失或非法键必须拒绝，不得静默映射到 singleton。
- 契约注册表以 Git 为事实源；整批候选校验通过后原子激活。候选无效时保留上一活动版本。
- 同一 SonnetDB 目录只允许一个活动 Bridge 实例持有。验证 EF migration、事务、回滚、关闭重开及 outbox/checkpoint 一致性；不得套用 SQLite 行为假设。
- 事实载荷、凭据及敏感键不得写入日志或指标标签。已完成 outbox 采用有限且可配置的保留期；死信在操作人员明确处理或归档前保留。
- V1 不包含内嵌 Gateway、多副本协调/HA、事件溯源、逐个中间快照送达保证、测量前硬性性能 SLO 或 NativeAOT 验收。

---

## 文件布局

从仓库根目录建立 solution。`Core` 管理传输无关契约、确定性裁决和编排；adapters 管理 HTTP/SSE DTO 与传输策略；persistence 管理 EF 实体、migration 和原子状态迁移；Host 管理配置、DI、生命周期及 telemetry。

```text
DrasiWake.sln
global.json
Directory.Build.props
Directory.Packages.props
src/DrasiWake.Core/Contracts/{BridgeBinding,BridgeContract,ContractRegistry,ContractRegistryLoader,ContractRegistryManager}.cs
src/DrasiWake.Core/Domain/{ChangeSignal,QueryIdentity,CanonicalIdentity,QuerySnapshot,SnapshotFingerprint,WakeRequest,WakeAcceptance,WakeExecutionStatus,WakeOutboxItem,SnapshotCheckpoint,RecoveryState}.cs
src/DrasiWake.Core/Abstractions/{IChangeSource,IWakeSink,IBridgeStore}.cs
src/DrasiWake.Core/Pipeline/{BridgeCoordinator,SnapshotReconciler,WakePayloadRenderer,OutboxDispatcher,SignalInbox,SessionPartitioner,RecoveryCoordinator}.cs
src/DrasiWake.Adapters.DrasiServer/{DrasiServerOptions,DrasiServerClient,DrasiSseReader,DrasiChangeSource}.cs
src/DrasiWake.Adapters.OpenClaw/{OpenClawOptions,OpenClawMetaInvocationClient}.cs
src/DrasiWake.Persistence.SonnetDB/BridgeDbContext.cs
src/DrasiWake.Persistence.SonnetDB/Entities/{SubscriptionState,SnapshotCheckpoint,KeyMapping,WakeOutbox}.cs
src/DrasiWake.Persistence.SonnetDB/SonnetBridgeStore.cs
src/DrasiWake.Host/{Program,BridgeHostedService,ReconciliationHostedService}.cs
src/DrasiWake.Host/appsettings.json
src/DrasiWake.Host/contracts/sample-binding.yaml
tests/DrasiWake.Core.Tests/
tests/DrasiWake.Adapters.DrasiServer.Tests/
tests/DrasiWake.Adapters.OpenClaw.Tests/
tests/DrasiWake.Persistence.SonnetDB.Tests/
tests/DrasiWake.IntegrationTests/
```

项目引用只能向内依赖 Core，Host 是组合根。建立四个单元测试项目（每个代码边界一个）及 `DrasiWake.IntegrationTests`。不得增加第二种部署形态或事件历史子系统。

## 实施任务

### 任务 1：建立 .NET solution 和 Core 边界契约

**文件：** 新建 `DrasiWake.sln`、`global.json`、`Directory.Build.props`、`Directory.Packages.props`；项目 `src/DrasiWake.Core/DrasiWake.Core.csproj`、`src/DrasiWake.Adapters.DrasiServer/DrasiWake.Adapters.DrasiServer.csproj`、`src/DrasiWake.Adapters.OpenClaw/DrasiWake.Adapters.OpenClaw.csproj`、`src/DrasiWake.Persistence.SonnetDB/DrasiWake.Persistence.SonnetDB.csproj`、`src/DrasiWake.Host/DrasiWake.Host.csproj`；四个单元测试项目及 `tests/DrasiWake.IntegrationTests/DrasiWake.IntegrationTests.csproj`；Core 的 `Abstractions/IChangeSource.cs`、`IWakeSink.cs`、`IBridgeStore.cs`、`Domain/` 传输无关模型及 `tests/DrasiWake.Core.Tests/BoundaryContractTests.cs`。`global.json` 选择 `Microsoft.Testing.Platform` runner，以便 xunit.v3 在 .NET 10 下使用新式 `dotnet test` 模式。

**接口：** 本任务定义以下精确签名和返回模型；任务 3 再实现模型的确定性行为。Gateway 执行状态查询的路由和 schema 必须查实际 API 契约，不得猜测。只有 `MarkAcceptedWithCheckpointAsync` 可以推进 checkpoint。

```csharp
public interface IChangeSource
{
    IAsyncEnumerable<ChangeSignal> WatchAsync(CancellationToken cancellationToken);
    ValueTask<IReadOnlyList<QueryIdentity>> EnumerateQueriesAsync(CancellationToken cancellationToken);
    ValueTask<QuerySnapshot> ReadSnapshotAsync(QueryIdentity query, CancellationToken cancellationToken);
}

public interface IWakeSink
{
    ValueTask<WakeAcceptance> InvokeAsync(WakeRequest request, CancellationToken cancellationToken);
    ValueTask<WakeExecutionStatus?> GetStatusAsync(string invocationId, CancellationToken cancellationToken);
}

public interface IBridgeStore
{
    ValueTask<WakeOutboxItem> CreateOrUpdatePendingWakeAsync(WakeOutboxItem item, CancellationToken cancellationToken);
    ValueTask<IReadOnlyList<WakeOutboxItem>> LoadDispatchableAsync(DateTimeOffset nowUtc, int limit, CancellationToken cancellationToken);
    ValueTask MarkAcceptedWithCheckpointAsync(Guid outboxId, SnapshotCheckpoint checkpoint, CancellationToken cancellationToken);
    ValueTask MarkRetryScheduledAsync(Guid outboxId, int attemptCount, DateTimeOffset nextAttemptUtc, string reasonCode, CancellationToken cancellationToken);
    ValueTask MarkDeadLetterAsync(Guid outboxId, string reasonCode, CancellationToken cancellationToken);
    ValueTask<RecoveryState> LoadRecoveryStateAsync(CancellationToken cancellationToken);
}
```

- [x] **步骤 1：先写一个失败的边界测试**

```csharp
[Fact]
public void Change_source_exposes_no_http_or_drasi_types()
{
    var signature = typeof(IChangeSource).GetMethods().SelectMany(m => m.GetParameters())
        .Select(p => p.ParameterType);
    Assert.DoesNotContain(signature, type => type == typeof(HttpRequestMessage));
}
```

- [x] **步骤 2：运行定向测试，确认因 Core 接口不存在而失败**

运行：`dotnet test --project tests/DrasiWake.Core.Tests/DrasiWake.Core.Tests.csproj --filter-class DrasiWake.Core.Tests.BoundaryContractTests`
预期：项目尚无 `IChangeSource`，编译因接口缺失而失败。

- [x] **步骤 3：创建 projects、共享构建属性、Core 接口及其模型**

目标框架设为 `net10.0`，启用 nullable 和 implicit usings，并通过中央包管理固定技术栈中列出的版本。创建不可变 `ChangeSignal`、`QueryIdentity`、`CanonicalIdentity`、`QuerySnapshot`、`WakeRequest`、`WakeAcceptance`、`WakeExecutionStatus`、`WakeOutboxItem`、`SnapshotCheckpoint`、`RecoveryState` 模型。Core 不引用 adapter DTO、`HttpRequestMessage` 或 EF 实体。

- [x] **步骤 4：重跑定向测试并还原/构建 solution**

运行：`dotnet test --project tests/DrasiWake.Core.Tests/DrasiWake.Core.Tests.csproj --filter-class DrasiWake.Core.Tests.BoundaryContractTests`
预期：测试通过，项目引用无循环。

- [ ] **步骤 5：提交** `git add DrasiWake.sln Directory.Build.props Directory.Packages.props src tests; git commit -m "build: establish DrasiWake solution boundaries"`
- [x] **步骤 5：提交** `git add DrasiWake.sln global.json Directory.Build.props Directory.Packages.props src tests; git commit -m "build: establish DrasiWake solution boundaries"`

### 任务 2：校验并原子激活契约注册表

**文件：** 新建 `src/DrasiWake.Core/Contracts/{BridgeBinding,BridgeContract,ContractRegistry,ContractRegistryLoader,ContractRegistryManager}.cs`、`tests/DrasiWake.Core.Tests/Contracts/ContractRegistryTests.cs` 和有效/无效 YAML fixtures。

**接口：** `BridgeBinding` 声明 source/server/可选 instance/query、delivery mode、`SessionScope`、aggregate-key JSON Pointer、显式 MetaSkill 名、fact schema 路径、载荷上限、重试及速率策略。`CanonicalIdentity` 包含非空 ontology/context、entity type 和 entity ID。`LoadCandidateAsync(path, ct)` 返回整份不可变有效注册表或校验错误；`TryActivate(candidate)` 仅在全量校验通过后原子交换活动引用。

- [x] **步骤 1：添加共享身份缺失和整批拒绝测试**

```csharp
[Fact]
public async Task Invalid_candidate_keeps_previous_registry_active()
{
    var previous = manager.Active;
    var candidate = await loader.LoadCandidateAsync("Fixtures/invalid-shared-identity.yaml", default);
    Assert.Contains(candidate.Errors, e => e.Code == "identity.required");
    Assert.False(manager.TryActivate(candidate));
    Assert.Same(previous, manager.Active);
}
```

- [x] **步骤 2：运行 `dotnet test --project tests/DrasiWake.Core.Tests/DrasiWake.Core.Tests.csproj --filter-class DrasiWake.Core.Tests.Contracts.ContractRegistryTests`，确认预期失败。**

- [x] **步骤 3：实现 YamlDotNet 18.1.0 加载和 JsonSchema.Net 9.4.0 校验。** 校验整个候选集：source 能力与 delivery mode 兼容（Drasi 拒绝 retain-every-change）、MetaSkill 名非空、JSON Pointer 合法、schema 存在、载荷限制有效、query binding/trigger phrase 无冲突、共享 scope 提供完整规范身份。候选加载不得修改活动状态；只有 `TryActivate` 可以切换注册表。

- [x] **步骤 4：补充有效默认 scope、trigger 冲突和活动引用不变测试并重跑。** 预期：有效注册表整体激活；任何无效候选都保留旧活动引用。

- [x] **步骤 5：提交** `git add src/DrasiWake.Core/Contracts tests/DrasiWake.Core.Tests/Contracts; git commit -m "feat: validate and atomically activate bridge contracts"`

### 任务 3：实现快照规范化、指纹与会话身份

**文件：** 完善 `src/DrasiWake.Core/Domain/QueryIdentity.cs`、`QuerySnapshot.cs`、`CanonicalIdentity.cs`；新建 `SnapshotFingerprint.cs`、`SessionIdentityResolver.cs` 和对应 Core domain tests。

**接口：** `QueryIdentity` 为 `(Uri Server, string? InstanceId, string QueryId)`，使用规范化后的 URI。`QuerySnapshot` 只含 query identity 与 JSON rows。`SessionIdentityResolver.Resolve(binding, row)` 返回会话身份或类型化拒绝结果。默认身份包含 query identity 和 aggregate key；共享 scope 使用完整规范身份。

- [x] **步骤 1：先写 JSON 属性/行顺序不变性及缺键测试**

```csharp
[Fact]
public void Fingerprint_ignores_property_and_result_row_order() =>
    Assert.Equal(Hash("[{\"id\":2},{\"id\":1}]"), Hash("[{\"id\":1},{\"id\":2}]"));

[Fact]
public void Missing_key_is_rejected_not_singleton() =>
    Assert.Equal("identity.key_missing", resolver.Resolve(binding, JsonNode.Parse("{}")!.AsObject()).ErrorCode);
```

- [x] **步骤 2：分别运行** `dotnet test --project tests/DrasiWake.Core.Tests/DrasiWake.Core.Tests.csproj --filter-class DrasiWake.Core.Tests.Domain.SnapshotFingerprintTests` 和 `dotnet test --project tests/DrasiWake.Core.Tests/DrasiWake.Core.Tests.csproj --filter-class DrasiWake.Core.Tests.Domain.SessionIdentityResolverTests`；预期失败。

- [x] **步骤 3：实现规范化。** 对象属性按 ordinal 排序，保留嵌套数组顺序，将最外层结果行按规范化 UTF-8 JSON 排序，再计算 SHA-256。按 RFC 6901 JSON Pointer 读取配置键；缺失、null、空值或格式错误均拒绝。共享身份必须验证三个字段，不得因原始键相同而推断共享。

- [x] **步骤 4：补充事实变化、跨 query 隔离和不完整共享身份测试并重跑。** 预期：仅表示顺序不同的快照指纹相等，事实变化会产生不同指纹，默认会话按 query 隔离。

- [x] **步骤 5：提交** `git add src/DrasiWake.Core/Domain tests/DrasiWake.Core.Tests/Domain; git commit -m "feat: canonicalize snapshots and resolve session identities"`

### 任务 4：实现 Drasi Server HTTP/SSE adapter

**文件：** 新建 `src/DrasiWake.Adapters.DrasiServer/{DrasiServerOptions,DrasiServerClient,DrasiSseReader,DrasiChangeSource}.cs`、adapter tests 和 SSE/results fixtures。

**接口：** 实现 Core `IChangeSource`。多实例路由为 `/api/v1/instances/{instanceId}/queries/{queryId}/attach` 与 `/results`；默认实例路由为 `/api/v1/queries/{queryId}/attach` 与 `/results`。

- [x] **步骤 1：添加失败的 SSE 测试。** 将 `data:` JSON frame 解析为提示；忽略注释/keep-alive；不得把生命周期事件当作查询结果。
- [x] **步骤 2：运行** `dotnet test --project tests/DrasiWake.Adapters.DrasiServer.Tests/DrasiWake.Adapters.DrasiServer.Tests.csproj --filter-class DrasiWake.Adapters.DrasiServer.Tests.DrasiSseReaderTests`；预期因 parser 缺失失败。
- [x] **步骤 3：实现路由、Drasi instances/queries API 枚举、results 解析、SSE 解析、取消、可见性状态及有界指数退避重连。** 按规范化 `(server, instance, query)` 复用单条 attach 连接，即使多个 binding 指向同一 query。每次连接/重连成功都触发快照对账。Drasi DTO 仅存在于 adapter；不得调用 `events/stream` 获取查询变化。
- [x] **步骤 4：测试两种路由、results 权威性、取消、连接复用、重连对账、畸形 frame，以及生命周期事件不会发出 query change。** 运行 `dotnet test --project tests/DrasiWake.Adapters.DrasiServer.Tests/DrasiWake.Adapters.DrasiServer.Tests.csproj`；预期全部通过，且不假定离线重放。
- [x] **步骤 5：提交** `git add src/DrasiWake.Adapters.DrasiServer tests/DrasiWake.Adapters.DrasiServer.Tests; git commit -m "feat: add Drasi Server attach and snapshot adapter"`

### 任务 5：持久化运行态并验证 SonnetDB 事务

**文件：** 新建 `src/DrasiWake.Persistence.SonnetDB/BridgeDbContext.cs`、四个实体 `SubscriptionState`、`SnapshotCheckpoint`、`KeyMapping`、`WakeOutbox`、`SonnetBridgeStore.cs`、EF migration 和持久化测试。

**接口：** 使用 EF Core 10.0.12、Microsoft.EntityFrameworkCore.Design 10.0.12 和 SonnetDB.EntityFrameworkCore 4.0.0（`UseSonnetDB`）实现 `IBridgeStore` 并生成 migration。Checkpoint 唯一键为 binding + session identity；mapping 唯一键为 contract scope + canonical identity。Outbox 保存渲染载荷、契约版本、稳定幂等键、尝试次数、下次重试时间、trace ID、受理/执行状态和保留时间戳。

- [x] **步骤 1：为 acceptance/checkpoint 联合事务添加失败回滚测试**

```csharp
[Fact]
public async Task Failed_transaction_changes_neither_outbox_nor_checkpoint()
{
    await store.SeedAsync(pendingWake, oldCheckpoint);
    store.FailAfterOutboxUpdateForTest = true;
    await Assert.ThrowsAsync<InjectedStoreFailure>(() =>
        store.MarkAcceptedWithCheckpointAsync(pendingWake.Id, newCheckpoint, default));
    Assert.Equal(OutboxState.Pending, await store.ReadStateAsync(pendingWake.Id));
    Assert.Equal(oldCheckpoint.Fingerprint, await store.ReadCheckpointAsync(bindingId, sessionId));
}
```

- [x] **步骤 2：运行 `dotnet test --project tests/DrasiWake.Persistence.SonnetDB.Tests/DrasiWake.Persistence.SonnetDB.Tests.csproj --filter-class DrasiWake.Persistence.SonnetDB.Tests.AtomicAcceptanceTests`，确认实现前失败。**
- [x] **步骤 3：添加 EF mapping、唯一索引、migration 和事务存储方法。** `MarkAcceptedWithCheckpointAsync` 在一个 EF transaction 中更新 outbox、upsert checkpoint、保存并提交；任何失败都回滚并抛出。用乐观并发控制防止多个 dispatch loop claim 同一记录；按 `(nextAttemptUtc, createdUtc, id)` 确定性排序。
- [x] **步骤 4：在临时嵌入式 SonnetDB 目录中测试 migration、提交/回滚、关闭重开恢复及第二个活动 owner 被拒绝。** 运行整个持久化测试项目；不得用 EF InMemory 或 SQLite 替代。使用固定 provider 文档中描述的嵌入式启动方式。
- [x] **步骤 5：提交** `git add src/DrasiWake.Persistence.SonnetDB tests/DrasiWake.Persistence.SonnetDB.Tests; git commit -m "feat: persist bridge state atomically in SonnetDB"`

### 任务 6：实现显式 OpenClaw.NET MetaSkill 调用

**文件：** 新建 `src/DrasiWake.Adapters.OpenClaw/{OpenClawOptions,OpenClawMetaInvocationClient}.cs`、请求/重试契约测试和 accepted-response fixture。

**接口：** 使用 `POST /api/integration/meta-invocations` 实现 `IWakeSink`；JSON 字段为 `skill`、`input`、`sessionId`，幂等键放在 `Idempotency-Key` header。受理状态与执行状态分开映射。

- [x] **步骤 1：添加显式路由请求失败测试**

```csharp
[Fact]
public async Task Sends_explicit_skill_session_input_and_existing_key()
{
    await client.InvokeAsync(request with { IdempotencyKey = "drasiwake:outbox-42" }, default);
    Assert.Equal("/api/integration/meta-invocations", handler.LastPath);
    Assert.Equal("drasiwake:outbox-42", handler.LastIdempotencyKey);
    Assert.Equal("triage-order", handler.LastBody.GetProperty("skill").GetString());
}
```

- [x] **步骤 2：运行** `dotnet test --project tests/DrasiWake.Adapters.OpenClaw.Tests/DrasiWake.Adapters.OpenClaw.Tests.csproj --filter-class DrasiWake.Adapters.OpenClaw.Tests.MetaInvocationRequestTests`；预期因路由/映射缺失而失败。
- [x] **步骤 3：实现请求/响应映射和瞬态故障 resilience。** 精确遵循 Gateway 契约中的 DTO。超时、重试和进程重启后都保留 outbox 中原幂等键；不得在 resilience handler 中生成新键。HTTP 409 指纹冲突属于契约错误，不可重试。执行状态查询必须使用实际 Gateway 文档中的路由/schema，并用测试固定下来。
- [x] **步骤 4：测试 Gateway 已受理后的超时、同键重放、并发重复抑制、409 冲突和保留期配置。** 预期：受理映射为 `Accepted` 而非 `Completed`；Gateway 幂等保留期短于 outbox 最长重试年龄时 Host 配置校验失败。
- [x] **步骤 5：提交** `git add src/DrasiWake.Adapters.OpenClaw tests/DrasiWake.Adapters.OpenClaw.Tests; git commit -m "feat: invoke explicitly routed MetaSkills with idempotency"`

### 任务 7：实现快照对账和持久化分发

**文件：** 新建 `src/DrasiWake.Core/Pipeline/{BridgeCoordinator,SnapshotReconciler,WakePayloadRenderer,OutboxDispatcher}.cs` 和 `tests/DrasiWake.Core.Tests/Pipeline/{SnapshotReconcilerTests,OutboxDispatcherTests}.cs`；复用任务 1 的 Core `Domain/` 模型。

**接口：** `SnapshotReconciler.ReconcileAsync(query, ct)` 读取当前快照，按 binding/session 校验和分区、比较指纹并持久化 pending outbox。`OutboxDispatcher.DispatchOneAsync(item, ct)` 使用已保存的幂等键调用 sink，收到 Gateway 受理后调用唯一的 acceptance/checkpoint 事务方法。

- [x] **步骤 1：添加快照读取失败不得改 checkpoint 的测试。** Stub `ReadSnapshotAsync` 抛出异常，并断言 checkpoint 前后完全一致。
- [x] **步骤 2：添加重复观察去重和先持久化再调用 sink 的失败测试。** 相同 pending 指纹只生成一个 outbox 项；sink spy 调用时能从 store 查到 pending 项。
- [x] **步骤 3：实现契约/schema/大小校验后的 JSON 渲染和分发。** 幂等键由持久化 outbox ID 派生，例如 `drasiwake:<outbox-guid-N>`，不能由可变载荷生成。HTTP 前必须持久化。新快照只能 supersede 同身份下尚未分发的 pending 工作，不得改写 accepted/executing 历史。键/schema/大小无效时记录可审计拒绝原因，不得回退到其他路由。
- [x] **步骤 4：测试受理与 checkpoint 更新顺序、本地事务失败、原键重试及 execution receipt 独立更新。** 分别运行 `dotnet test --project tests/DrasiWake.Core.Tests/DrasiWake.Core.Tests.csproj --filter-class DrasiWake.Core.Tests.Pipeline.SnapshotReconcilerTests` 和 `dotnet test --project tests/DrasiWake.Core.Tests/DrasiWake.Core.Tests.csproj --filter-class DrasiWake.Core.Tests.Pipeline.OutboxDispatcherTests`；预期通过。
- [x] **步骤 5：提交** `git add src/DrasiWake.Core/Domain src/DrasiWake.Core/Pipeline tests/DrasiWake.Core.Tests/Pipeline; git commit -m "feat: reconcile snapshots into durable wake outbox"`

### 任务 8：实现有界接收、会话串行和恢复

**文件：** 新建 `src/DrasiWake.Core/Pipeline/{SignalInbox,SessionPartitioner,RecoveryCoordinator}.cs` 和对应定向测试。

**接口：** `SignalInbox` 使用有界 `Channel<ChangeSignal>`，`TryWrite` 失败时将 query 标记为 dirty。`SessionPartitioner` 保证每个 `SessionIdentity` FIFO，同时限制不同身份的并行 worker 数，不为每个身份永久创建线程。`RecoveryCoordinator` 在启动时重载 outbox/state，重连后先读新快照。

- [x] **步骤 1：添加并发测试，阻塞会话 A 的第一次 sink 调用，断言 A 的第二次等待而会话 B 可继续。**
- [x] **步骤 2：添加 channel 满测试，断言入队失败会标记 query dirty，且 SSE 接收循环不执行快照 I/O。**
- [x] **步骤 3：实现有界 keyed FIFO worker、按 binding 的 `RateLimiter`、注入式 `TimeProvider` 和可取消停机。** 限流中的工作必须保留在持久 outbox 或等待，不得丢弃；用可控测试时钟验证重试/速率窗口。停机时停止接收、取消网络读取、保留未受理工作，并在配置的超时内等待 worker；出队不代表受理。
- [x] **步骤 4：测试限流等待仍保留 outbox、虚拟时间下的重试计划、重启后复用原幂等键，以及新快照完成前不将重连 query 标记为健康。** 分别运行 `dotnet test --project tests/DrasiWake.Core.Tests/DrasiWake.Core.Tests.csproj --filter-class DrasiWake.Core.Tests.Pipeline.SessionPartitionerTests` 和 `dotnet test --project tests/DrasiWake.Core.Tests/DrasiWake.Core.Tests.csproj --filter-class DrasiWake.Core.Tests.Pipeline.RecoveryCoordinatorTests`；预期通过。
- [x] **步骤 5：提交** `git add src/DrasiWake.Core/Pipeline tests/DrasiWake.Core.Tests/Pipeline; git commit -m "feat: serialize wakes per session and recover pending work"`

### 任务 9：组合 Host、对账调度和 telemetry

**文件：** 新建 `src/DrasiWake.Host/{Program,BridgeHostedService,ReconciliationHostedService}.cs`、`appsettings.json`、`contracts/sample-binding.yaml` 及 Host/telemetry 集成测试。

**接口：** Host 注册 Core、adapters 和 persistence；各服务共享 `TimeProvider`。`ReconciliationHostedService` 使用 `PeriodicTimer`；启动、重连/恢复和周期计时器触发对账，且不阻塞 SSE 读取。

- [ ] **步骤 1：添加无效 registry 和 Gateway 保留期短于最大重试年龄时启动失败的测试。**
- [ ] **步骤 2：从标准 .NET configuration 绑定 endpoint/auth、数据库目录、重试/保留期、channel 容量、worker 数和注册表路径。** 凭据从环境变量或 secret provider 读取，不写入已提交的 `appsettings.json`。拒绝第二个活动实例持有同一 DB 目录；配置无效时必须在启动订阅前失败。
- [ ] **步骤 3：添加 OpenTelemetry activities、counters 和结构化日志。** 记录提示至受理延迟、队列深度/年龄、对账差异、可见性、快照读取失败、重复指纹、重试/死信、契约版本及路由结果。脱敏事实/凭据/键，只使用非敏感稳定 ID 关联。
- [ ] **步骤 4：测试启动校验、停机保留 pending 工作，以及日志/指标脱敏。** 分别运行 `dotnet test --project tests/DrasiWake.IntegrationTests/DrasiWake.IntegrationTests.csproj --filter-class DrasiWake.IntegrationTests.HostStartupTests` 和 `dotnet test --project tests/DrasiWake.IntegrationTests/DrasiWake.IntegrationTests.csproj --filter-class DrasiWake.IntegrationTests.TelemetryRedactionTests`；预期通过。
- [ ] **步骤 5：提交** `git add src/DrasiWake.Host tests/DrasiWake.IntegrationTests; git commit -m "feat: compose standalone bridge host and telemetry"`

### 任务 10：验证端到端恢复并编写运行手册

**文件：** 新建 `tests/DrasiWake.IntegrationTests/{DrasiConvergenceTests,GatewayIdempotencyTests,CrashRecoveryTests}.cs` 和 Drasi fixtures；新建 `docs/bridge-core-v1-operations.md`；修改 `README.md`。

**接口：** 使用确定性的本地 HTTP 测试服务验证 adapter 契约。宣称 V1 ready 前，还必须运行单独配置的真实 Drasi/OpenClaw 契约测试。

- [ ] **步骤 1：测试提示重复、乱序、丢失后最终收敛。** 断言最终 checkpoint/wake 与 `results` 一致，不断言每个中间状态都唤醒。
- [ ] **步骤 2：测试重连、channel 溢出和进程重启。** 每种恢复路径都重新读取 `results`；读取失败不改变 checkpoint。
- [ ] **步骤 3：在 outbox 持久化后、Gateway 受理后、本地 acceptance 事务提交前后及 execution receipt 前注入崩溃。** 重开数据库后断言使用原幂等键、同一 caller/key/request 不会启动第二个 DAG，且 acceptance 不会被报告为完成。
- [ ] **步骤 4：运行真实 Gateway 契约测试。** 验证专用 POST endpoint、MetaSkill/session/input 映射、同键重放、并发重复抑制以及幂等保留期不短于配置的最大 outbox 重试年龄；记录 Gateway build/config。任一断言不通过都保持 V1-ready 门禁关闭，不得回退到 `/messages`、`/workflows` 或 trigger matching。
- [ ] **步骤 5：运行真实 Drasi 契约测试和 SonnetDB 重开测试。** 验证 attach/results 路由、SSE JSON frame 解析、query 枚举、重连快照及不使用 `events/stream`；对全新临时目录应用 migration，停止 Host 后重开同一目录。验证普通 .NET 10 Release 发布，不增加 AOT 门禁。
- [ ] **步骤 6：运行发布门禁**

```powershell
dotnet test --solution DrasiWake.sln
dotnet publish src/DrasiWake.Host/DrasiWake.Host.csproj -c Release -o artifacts/publish
```

预期：自动化测试全部通过，publish 退出码为 0。单独记录真实 Drasi 和 Gateway 契约测试结果；mock 测试不能代替 V1-ready 证据。

- [ ] **步骤 7：检查排除范围并提交。** 确认没有加入内嵌 Gateway adapter、MCP Reaction、多副本协调、事件溯源、逐个中间变化保证或 AOT 门禁。执行 `git add README.md docs/bridge-core-v1-operations.md tests/DrasiWake.IntegrationTests; git commit -m "test: verify Bridge V1 recovery and document operations"`。

## 规格覆盖自审

| 规格要求 | 对应任务 |
| --- | --- |
| .NET 10 独立宿主、单 SonnetDB owner、V1 不要求 AOT/HA | 1、5、9、10 |
| Drasi attach 提示、results 权威、两类路由、枚举、不用 events/stream | 4、7、10 |
| 启动/重连/恢复/周期/溢出对账及有界 Channel | 4、8、9、10 |
| 最终状态语义及拒绝不支持的逐次保留模式 | 2、4、7、10 |
| 快照失败不推进 checkpoint | 5、7、10 |
| Git registry、整批校验和原子激活 | 2、9 |
| 默认 query 隔离及显式规范共享身份 | 2、3 |
| 四类运行态实体、EF migration/事务/回滚/重开 | 5 |
| 先持久化再分发、受理与 checkpoint 原子更新、稳定重试键 | 5、7、8、10 |
| 显式 MetaSkill endpoint、Gateway 幂等及保留期 | 6、9、10 |
| 受理与执行回执分离 | 6、7、10 |
| 同会话 FIFO、跨会话并行、背压和限流 | 8 |
| 重试/死信与敏感数据脱敏 | 5、7、9、10 |
| 故障注入及真实 V1-ready 证据 | 10 |

**占位符扫描：** 不含 `TBD`、`TODO` 或未指派实现项。Gateway 执行状态路由必须从实际 API 契约读取并通过测试固定，不得猜测。

**类型一致性：** 任务 1 定义唯一跨项目 ports；任务 2-3 定义契约、query/session、快照和指纹模型；任务 4-6 实现 ports；任务 7-9 消费相同签名。Checkpoint 只能通过统一的 `MarkAcceptedWithCheckpointAsync` 操作推进。

**范围审查：** 保留单一计划，因为交付目标是一个 V1 runtime，其正确性依赖快照、持久 outbox/checkpoint 事务与 Gateway 幂等链的整体衔接。Adapter 和 persistence 各有独立测试，但单独交付都不能满足此规格。
