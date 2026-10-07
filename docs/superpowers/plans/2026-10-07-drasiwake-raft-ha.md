# DrasiWake Raft 高可用实施计划

> **供自动化执行者使用：** 必须使用 `superpowers:subagent-driven-development`（推荐）或 `superpowers:executing-plans`，逐项执行本计划。使用复选框（`- [ ]`）跟踪步骤。

**目标：** 通过 DotNext Raft 实现 DrasiWake 三节点高可用，包括每节点 SonnetDB 投影、仅 Leader 运行工作、故障切换恢复和受保护的运行时成员管理。

**架构：** 新增 Raft 持久化适配器，通过 DotNext 复制带版本的 Bridge Store 命令；各节点状态机将已提交命令和完整快照应用到独立 SonnetDB 投影。Host 仅允许已追平且有多数派的 Leader 启动 Drasi 和 outbox worker，并提供使用 TLS 与 Bearer Token 保护的 gRPC 管理面。

**技术栈：** .NET 10、DotNext.AspNetCore.Cluster 6.9.0、EF Core 10、SonnetDB.EntityFrameworkCore 4.0.0、protobuf-net.Grpc.AspNetCore 1.3.14、Grpc.Net.Client 2.84.0、xUnit v3、现有 Microsoft.Testing.Platform 测试运行器。

## 全局约束

- 使用三个 Raft 投票节点，容忍一个节点故障。
- 每个节点使用独立的 SonnetDB 目录和独立 Raft 数据目录；副本之间不得共享数据库文件。
- Raft 已提交的 Bridge 命令是权威状态；本地 SonnetDB 是可从 Raft 重建的投影。
- 仅当前 Leader 可以启动 Drasi 接收、快照对账和 outbox 分发 worker。
- 没有多数派时不得推进新的持久化业务状态；不得回退为各节点独立写入。
- 保留现有单节点启动配置，并让其使用同一 Raft 状态机路径。
- `Accepted` 与对应的 snapshot checkpoint 必须由同一条已提交命令原子写入。
- Gateway 副作用使用现有稳定幂等键和保留时长校验，语义为可恢复的至少一次投递；不得宣称 exactly-once。
- Raft snapshot 必须能重建每张持久化业务表以及本地投影已应用的最后 Raft 索引。
- Raft 和管理 gRPC 使用 TLS；管理请求还必须验证通过 secret provider 注入且绝不写入日志的集群管理 Bearer Token。
- 集群所有节点必须使用一致的 binding、Drasi、Gateway 功能配置指纹和兼容的应用命令版本。
- NodeId、本地路径、证书和令牌不得进入业务 Raft 命令。

---

## 文件职责图

| 文件 | 职责 |
| --- | --- |
| `Directory.Packages.props` | 集中固定 DotNext.AspNetCore.Cluster 6.9.0、Grpc.Net.Client 2.84.0 和 protobuf-net.Grpc.AspNetCore 1.3.14。 |
| `DrasiWake.sln` | 纳入新的 Raft 持久化项目和测试项目。 |
| `src/DrasiWake.Persistence.SonnetDB/BridgeDbContext.cs`、`Entities/RaftProjectionState.cs`、`Migrations/` | 保存本地投影已应用的最后 Raft 索引及复制的集群元数据。 |
| `src/DrasiWake.Persistence.SonnetDB/Replication/ReplicatedBridgeCommand.cs`、`BridgeStoreSnapshot.cs` | 定义有版本的命令载荷和完整投影快照；快照包含最后应用的命令 ID。 |
| `src/DrasiWake.Persistence.SonnetDB/SonnetBridgeStore.cs` | 在事务中应用命令及索引、导出/恢复快照，并提供只读分发候选项查询。 |
| `src/DrasiWake.Persistence.Raft/DrasiWake.Persistence.Raft.csproj` | 隔离 DotNext 状态机和命令复制依赖。 |
| `src/DrasiWake.Persistence.Raft/RaftBridgeStateMachine.cs` | 将每条已提交 DotNext 日志项应用到本地 SonnetDB，并负责快照保存与恢复。 |
| `src/DrasiWake.Persistence.Raft/IRaftCommandExecutor.cs`、`DotNextRaftCommandExecutor.cs` | 抽象命令提交以便测试，并通过本地 DotNext 集群实现。 |
| `src/DrasiWake.Persistence.Raft/RaftBridgeStore.cs` | 实现 `IBridgeStore`：写操作提交 Raft 命令，提交后从本地投影读取结果。 |
| `src/DrasiWake.Host/DrasiWakeHostSettings.cs`、`RaftClusterSettings.cs` | 解析并验证拓扑、节点身份、本地目录、TLS、管理凭据和快照设置。 |
| `src/DrasiWake.Host/DrasiWakeHostBuilder.cs`、`DrasiWake.Host.csproj`、`appsettings.json` | 托管 DotNext HTTP/2 端点、注册状态机和 Raft Store，并保留单节点配置。 |
| `src/DrasiWake.Host/HostStartupValidator.cs` | 在集群启动前完成静态校验、本地目录所有权和 migration；移除共识启动前的复制状态写入。 |
| `src/DrasiWake.Host/RaftLeaderHostedService.cs` | 监控领导权，并在每个 leadership epoch 启停新的 Bridge worker scope。 |
| `src/DrasiWake.Host/BridgeHostedService.cs`、`ReconciliationHostedService.cs`、`DrasiWakeHostBuilder.cs` | 将一次性 worker 改为可按领导权重新创建的服务，并由 Leader coordinator 管理。 |
| `src/DrasiWake.Host/ClusterMembershipGrpcService.cs`、`RaftMembershipContracts.cs`、`ClusterConfigurationFingerprint.cs` | 验证成员管理 RPC、比较候选节点兼容性，并通过 DotNext 转发/提交 Add/Remove。 |
| `src/DrasiWake.Core/Pipeline/RecoveryCoordinator.cs`、`BridgeTelemetry.cs` | 支持按领导权 epoch 恢复，并记录 HA 角色、quorum、索引延迟、成员变更和恢复指标。 |
| `tests/DrasiWake.Persistence.SonnetDB.Tests/ReplicatedProjectionTests.cs` | 验证已应用索引原子性、重复应用幂等性和完整快照往返。 |
| `tests/DrasiWake.Persistence.Raft.Tests/` | 验证命令编码、状态机应用和 Raft Store 适配器语义。 |
| `tests/DrasiWake.IntegrationTests/Fixtures/RaftClusterFixture.cs`、`RaftFailoverTests.cs`、`HostStartupTests.cs` | 启动真实 DotNext 多节点，验证 Leader 门控、quorum、故障切换、追平和配置拒绝。 |
| `docs/bridge-core-v1-operations.md`、`README.md` | 记录节点配置、安全运行、成员变更、故障行为和单节点兼容性。 |

这些组成部分属于同一项相互依赖的 HA 交付：状态复制、Leader 门控和成员管理都依赖同一套持久化 Raft 拓扑。按上表的项目边界实现，不拆成互不兼容的独立交付。

## 任务 1：添加集群设置、依赖版本和配置校验

**文件：**

- 修改：`Directory.Packages.props`
- 修改：`src/DrasiWake.Host/DrasiWake.Host.csproj`
- 修改：`src/DrasiWake.Host/DrasiWakeHostSettings.cs`
- 新建：`src/DrasiWake.Host/RaftClusterSettings.cs`
- 修改：`tests/DrasiWake.IntegrationTests/HostStartupTests.cs`
- 修改：`src/DrasiWake.Host/appsettings.json`

**接口：**

- 新增 `RaftClusterSettings.FromConfiguration(IConfiguration, string databasePath)` 和 `RaftClusterSettings.Validate()`。
- `RaftClusterSettings` 包含 `Mode`、`NodeId`、`ListenAddress`、`RaftDataPath`、`InitialMembers`、`CertificatePath`、`CertificatePassword`、`ManagementAddress`、`ManagementBearerToken` 和 `SnapshotFrequency`。
- `Mode` 取 `SingleNode` 或 `Cluster`。`SingleNode` 派生稳定的本地节点身份、loopback endpoint 和一个本地成员；`Cluster` 必须提供稳定 NodeId、HTTPS 地址、独立 Raft 路径、证书、管理 HTTPS 地址/令牌、正数快照频率及有效初始成员 URI。
- `DrasiWakeHostSettings` 保留既有设置，并公开已经校验的 `Cluster` 设置。

- [x] **步骤 1：先写失败的配置测试。** 在 `HostStartupTests` 增加默认单节点、有效三成员配置、NodeId 为空、初始成员 URI 重复、集群地址非 HTTPS、Raft 与数据库路径相同、管理令牌缺失、成员 URI 无效、快照频率非正数等测试。重复 NodeId 应在成员预检阶段测试，因为届时才有已持久化的完整成员列表。

  管理令牌缺失只在 `Cluster` 模式下报错。`SingleNode` 模式保持已签入配置不含密钥，也不暴露运行时成员管理接口。

  示例断言：

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

- [x] **步骤 2：运行指定测试类，确认新增测试先失败。**

  运行：`dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.HostStartupTests`

  预期：新增测试因尚无集群配置解析/校验而失败；确认测试筛选器确实执行了测试，不能接受零测试结果。

- [x] **步骤 3：固定并引用所需 NuGet 包。** 按 SlikCache 的版本在中央清单增加 `DotNext.AspNetCore.Cluster` 6.9.0、`Grpc.Net.Client` 2.84.0、`protobuf-net.Grpc.AspNetCore` 1.3.14。Host 增加 ASP.NET Core framework reference；各项目只引用实际使用的包和项目。

- [x] **步骤 4：实现设置解析和校验。** 解析 `DrasiWake:Cluster`；仅当沿用旧单节点配置时默认生成 loopback 单节点设置。`Cluster` 模式下缺失/无效值必须显式报错并指出配置键。规范化成员 URI、拒绝重复地址、要求本地地址出现在初始成员中，并拒绝 Raft 与 SonnetDB 使用相同目录。

- [x] **步骤 5：补充不含密钥的示例配置。** 在 `appsettings.json` 添加单节点 `Cluster` 设置；不得写入真实证书密码或管理令牌。多节点配置的密钥只由环境变量/secret provider 注入，且单节点模式关闭成员管理 gRPC（当前尚无成员管理 gRPC 服务）。

- [x] **步骤 6：运行 Host 启动测试并提交。**

  运行：`dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.HostStartupTests`

  预期：所有 `HostStartupTests` 通过，包括新增的配置无效情形。

  ```text
  git add Directory.Packages.props src/DrasiWake.Host tests/DrasiWake.IntegrationTests/HostStartupTests.cs
  git commit -m "feat: add Raft cluster settings"
  ```

## 任务 2：使 SonnetDB 投影可安全重放并支持快照

**文件：**

- 修改：`src/DrasiWake.Persistence.SonnetDB/BridgeDbContext.cs`
- 新建：`src/DrasiWake.Persistence.SonnetDB/Entities/RaftProjectionState.cs`
- 新建：`src/DrasiWake.Persistence.SonnetDB/Replication/ReplicatedBridgeCommand.cs`
- 新建：`src/DrasiWake.Persistence.SonnetDB/Replication/BridgeStoreSnapshot.cs`
- 修改：`src/DrasiWake.Persistence.SonnetDB/SonnetBridgeStore.cs`
- 生成：EF Core migration `AddRaftProjectionState`，位于 `src/DrasiWake.Persistence.SonnetDB/Migrations/`
- 新建：`tests/DrasiWake.Persistence.SonnetDB.Tests/ReplicatedProjectionTests.cs`

**接口：**

- `ReplicatedBridgeCommand` 是不可变 envelope，包含 `SchemaVersion`、`CommandId`、`Kind` 和序列化 payload。`CommandId` 是命令类型与规范 payload 的 SHA-256 十六进制摘要，因此同一内容的不确定重试具有相同 ID。`Kind` 覆盖所有现有 `IBridgeStore` 写操作、分发 claim、中断分发恢复、target 回填及集群元数据更新。
- `BridgeStoreSnapshot` 包含 `SchemaVersion`、`LastAppliedIndex`、`LastAppliedCommandId`、集群配置指纹，以及按确定顺序排列的 `Subscriptions`、`SnapshotCheckpoints`、`KeyMappings`、`WakeOutbox` 全部记录。
- 在 `SonnetBridgeStore` 增加：

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

- `RaftProjectionState` 使用单例主键行保存最后应用的索引、该索引的命令 ID 和已提交的集群配置指纹。

- [ ] **步骤 1：先写索引原子性、重复重放和索引缺口测试。** 参考 `AtomicAcceptanceTests` 中的 `CreateOptions`/`TestContextFactory` 模式，在新文件中自建测试辅助代码。

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

  另需断言：当前索引为 1 时拒绝索引 3；注入数据库故障时业务记录和已应用索引都不变。

- [ ] **步骤 2：运行持久化测试类，确认测试先失败。**

  运行：`dotnet test --project tests\DrasiWake.Persistence.SonnetDB.Tests\DrasiWake.Persistence.SonnetDB.Tests.csproj -c Release --filter-class DrasiWake.Persistence.SonnetDB.Tests.ReplicatedProjectionTests`

  预期：因复制投影 API 和元数据尚不存在而编译失败/测试失败。

- [ ] **步骤 3：增加投影状态实体并生成 migration。** 实体包含单例主键、非负 `LastAppliedIndex`、`LastAppliedCommandId` 和可初始化的集群配置指纹。运行：

  ```text
  dotnet ef migrations add AddRaftProjectionState --project src\DrasiWake.Persistence.SonnetDB\DrasiWake.Persistence.SonnetDB.csproj --startup-project src\DrasiWake.Host\DrasiWake.Host.csproj
  ```

  通过 EF 工具更新 `BridgeDbContextModelSnapshot`，不得手工编辑生成的 migration metadata。

- [ ] **步骤 4：实现命令 envelope 和事务性应用。** 将 `SonnetBridgeStore` 写操作重构为可接收同一个 `BridgeDbContext` 的 helper。`ApplyReplicatedCommandAsync` 在单一 EF 事务内处理：重放时跳过小于本地索引的历史日志项；索引等于本地索引时，仅当命令 ID 等于 `LastAppliedCommandId` 才作为重复命令忽略；拒绝索引缺口；应用一个类型明确的命令；更新索引与命令 ID；提交事务。`MarkAcceptedWithCheckpointAsync` 的 outbox 与 checkpoint 更新也必须包含在同一事务中。

- [ ] **步骤 5：将候选项读取与 dispatch claim 分离。** `LoadDispatchableCandidatesAsync` 沿用既有稳定排序但不修改数据；`ClaimDispatchable` 复制命令带上准确的 outbox ID 列表和 claim 时间。将 recovery 拆成复制命令 `RecoverInterruptedDispatches` 和只读 `ReadRecoveryStateAsync`。

- [ ] **步骤 6：实现完整快照导出和恢复。** 以确定性主键顺序导出四张既有表，包括当前可能为空的 `Subscriptions` 和 `KeyMappings`。在开事务前校验快照版本、重复主键、必填字段和索引；随后在一个事务中替换业务表和投影元数据。校验或事务失败必须保留原投影。

- [ ] **步骤 7：增加快照往返和失败测试。** 覆盖 outbox 状态、可空字段、JSON 输入、checkpoint 时间、key mapping、subscription 和提交索引。断言不支持的版本或重复主键会被拒绝，且不改变既有记录。

- [ ] **步骤 8：运行持久化测试并提交。**

  运行：`dotnet test --project tests\DrasiWake.Persistence.SonnetDB.Tests\DrasiWake.Persistence.SonnetDB.Tests.csproj -c Release`

  预期：既有原子接受、数据库重开、目录所有权锁测试和新增投影测试全部通过。

  ```text
  git add src/DrasiWake.Persistence.SonnetDB tests/DrasiWake.Persistence.SonnetDB.Tests
  git commit -m "feat: add replay-safe SonnetDB projection"
  ```

## 任务 3：实现 DotNext 状态机和 Raft 命令执行器

**文件：**

- 新建：`src/DrasiWake.Persistence.Raft/DrasiWake.Persistence.Raft.csproj`
- 新建：`src/DrasiWake.Persistence.Raft/RaftBridgeStateMachine.cs`
- 新建：`src/DrasiWake.Persistence.Raft/IRaftCommandExecutor.cs`
- 新建：`src/DrasiWake.Persistence.Raft/DotNextRaftCommandExecutor.cs`
- 新建：`tests/DrasiWake.Persistence.Raft.Tests/DrasiWake.Persistence.Raft.Tests.csproj`
- 新建：`tests/DrasiWake.Persistence.Raft.Tests/RaftBridgeStateMachineTests.cs`
- 修改：`DrasiWake.sln`

**接口：**

- `RaftBridgeStateMachine` 继承 DotNext `SimpleStateMachine`，依赖本节点 `SonnetBridgeStore`、快照频率和 logger。
- `IRaftCommandExecutor` 定义：

  ```csharp
  public interface IRaftCommandExecutor
  {
      ValueTask ReplicateAsync(ReplicatedBridgeCommand command, CancellationToken cancellationToken);
      bool IsLeader { get; }
      bool HasQuorum { get; }
  }
  ```

- `DotNextRaftCommandExecutor` 使用统一的有版本 `JsonSerializerOptions` 序列化 envelope，等待 `IRaftCluster.ReplicateAsync` 共识复制成功；非 Leader 或无可写多数派时拒绝提交，不得直接写入本地 SonnetDB。
- 状态机只接受受支持版本的命令，将 `LogEntry.Index` 传给本地投影；通过 DotNext binary reader/writer API 保存/恢复 `BridgeStoreSnapshot`。

- [ ] **步骤 1：先写命令序列化失败测试。** 验证每种命令类型及 payload 往返一致、`SchemaVersion` 明确、相同规范 payload 生成相同 command ID、时间戳或 payload 改变后 ID 不同、错误/未知版本被拒绝，并确认序列化内容不含节点密钥或本地路径。

- [ ] **步骤 2：运行 Raft 测试类，确认新增测试先失败。**

  运行：`dotnet test --project tests\DrasiWake.Persistence.Raft.Tests\DrasiWake.Persistence.Raft.Tests.csproj -c Release --filter-class DrasiWake.Persistence.Raft.Tests.RaftBridgeStateMachineTests`

  预期：先因 Raft 项目和状态机尚不存在而失败；步骤 1 同时创建测试项目和测试文件。

- [ ] **步骤 3：创建 Raft 与测试项目。** Raft 项目引用 `DrasiWake.Core`、`DrasiWake.Persistence.SonnetDB` 和中央固定版本的 DotNext.AspNetCore.Cluster 6.9.0。测试项目采用现有 .NET 10、xUnit v3 和测试运行器配置。

- [ ] **步骤 4：实现 executor 和状态机。** 以 SlikCache 6.9.0 中的 `SimpleStateMachine`、`IRaftCluster.ReplicateAsync`、`WriteAheadLog.Options`、snapshot reader/writer 和 `UseStateMachine<T>` 为 API 参考。`ApplyAsync` 必须把每个已提交索引与本地投影原子保存；snapshot 必须包含完整投影和索引；恢复先校验快照，再应用后续日志。

- [ ] **步骤 5：测试状态机重放和快照回调。** 使用临时 Raft 目录启动单节点 DotNext Host，提交两条命令、关闭并重新打开节点，验证投影和索引恢复。另测畸形日志 payload 和投影事务失败；应用失败的节点不得报告追平。

- [ ] **步骤 6：运行 Raft 与持久化测试并提交。**

  运行：`dotnet test --project tests\DrasiWake.Persistence.Raft.Tests\DrasiWake.Persistence.Raft.Tests.csproj -c Release`

  预期：命令序列化、状态机和 snapshot 测试通过。

  ```text
  git add DrasiWake.sln src/DrasiWake.Persistence.Raft tests/DrasiWake.Persistence.Raft.Tests
  git commit -m "feat: add DotNext bridge state machine"
  ```

## 任务 4：将所有 `IBridgeStore` 写操作改为经 Raft 提交

**文件：**

- 新建：`src/DrasiWake.Persistence.Raft/RaftBridgeStore.cs`
- 新建：`tests/DrasiWake.Persistence.Raft.Tests/RaftBridgeStoreTests.cs`
- 修改：`src/DrasiWake.Persistence.SonnetDB/Replication/ReplicatedBridgeCommand.cs`
- 修改：`src/DrasiWake.Persistence.SonnetDB/SonnetBridgeStore.cs`

**接口：**

- `RaftBridgeStore : IBridgeStore` 依赖 `IRaftCommandExecutor` 和本地 `SonnetBridgeStore`。
- 每个写方法映射为一条 `ReplicatedBridgeCommand`；等待共识提交和本地应用后，才从本地投影读取结果。
- `LoadDispatchableAsync` 先读取候选项，再复制包含准确 ID 和 `nowUtc` 的 claim 命令；只返回已提交为 `Dispatching` 的记录。
- `LoadRecoveryStateAsync` 先用 Leader 提供的 UTC 时间提交 recovery 命令，再调用只读 `ReadRecoveryStateAsync`。
- `EnsureOpenClawTargetsAsync` 是复制命令；静态 target/保留时长校验仍由 `HostStartupValidator` 执行。

- [ ] **步骤 1：用 capturing executor 先写失败测试。** 测试 executor 记录 envelope、以本地投影的下一个索引应用命令，并以无返回值的 `ValueTask` 完成。对每个 `IBridgeStore` 写操作断言命令类型及 payload；executor 失败时，投影不得改变。

- [ ] **步骤 2：运行适配器测试，确认测试先失败。**

  运行：`dotnet test --project tests\DrasiWake.Persistence.Raft.Tests\DrasiWake.Persistence.Raft.Tests.csproj -c Release --filter-class DrasiWake.Persistence.Raft.Tests.RaftBridgeStoreTests`

- [ ] **步骤 3：逐方法实现适配器。** 覆盖创建/更新 pending wake、拒绝记录、supersede、dispatch claim、原子 acceptance/checkpoint、执行状态、retry、dead-letter、中断分发恢复和 target 回填。命令需包含所有时间戳与标识符，followers 不得自行读取墙上时钟或生成本地 ID。使用命令类型和规范化 JSON payload 的 SHA-256 小写十六进制摘要生成 `CommandId`；序列化前按键排序字典。

- [ ] **步骤 4：验证关键不变量。** claim 提交失败不得产生可发给 Gateway 的结果；重复 acceptance 不得替换原 invocation/checkpoint；恢复必须保留同一个幂等键；任何副本不得观察到缺少匹配 checkpoint 的 `Accepted` 状态。

- [ ] **步骤 5：运行 Raft 和 SonnetDB 测试并提交。**

  运行：`dotnet test --project tests\DrasiWake.Persistence.Raft.Tests\DrasiWake.Persistence.Raft.Tests.csproj -c Release`

  运行：`dotnet test --project tests\DrasiWake.Persistence.SonnetDB.Tests\DrasiWake.Persistence.SonnetDB.Tests.csproj -c Release`

  预期：两个测试项目全部通过。

  ```text
  git add src/DrasiWake.Persistence.Raft src/DrasiWake.Persistence.SonnetDB tests/DrasiWake.Persistence.Raft.Tests
  git commit -m "feat: replicate bridge store mutations"
  ```

## 任务 5：托管集群并保持单节点启动兼容

**文件：**

- 修改：`src/DrasiWake.Host/DrasiWake.Host.csproj`
- 修改：`src/DrasiWake.Host/DrasiWakeHostBuilder.cs`
- 修改：`src/DrasiWake.Host/HostStartupValidator.cs`
- 修改：`src/DrasiWake.Host/appsettings.json`
- 修改：`src/DrasiWake.Host/DrasiWakeHostSettings.cs`
- 修改：`tests/DrasiWake.IntegrationTests/HostStartupTests.cs`
- 修改：`DrasiWake.sln`

**接口：**

- 本地 `SonnetBridgeStore` 注册为 Raft 状态机投影，生产环境 `IBridgeStore` 注册为 `RaftBridgeStore`。
- Host 启动顺序：校验静态设置和注册表；获取该节点数据库目录所有权；执行数据库 migration；启动 Kestrel/Raft 并恢复投影；验证/提交集群业务配置指纹；最后才允许 Leader worker coordinator 启动。
- 从 `HostStartupValidator` 移除会修改持久状态的 `EnsureOpenClawTargetsAsync`；当选 Leader 通过 `RaftBridgeStore` 提交该操作，并在 `RecoveryCoordinator.RecoverAsync` 前完成。

- [ ] **步骤 1：先写失败的 Host 生命周期测试。** 验证单节点模式注册 Raft-backed store；多节点模式以设置的 TLS 证书启动 Kestrel；同一数据库目录仍只能由一个 Host 持有；migration 必须在 worker 启动前完成。

- [ ] **步骤 2：运行 Host 启动测试类，确认新增断言先失败。**

  运行：`dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.HostStartupTests`

- [ ] **步骤 3：添加 Host 引用并托管集群。** 引用 `DrasiWake.Persistence.Raft`、DotNext.AspNetCore.Cluster、Grpc.Net.Client 和 protobuf-net.Grpc.AspNetCore；为 console Host 添加 `Microsoft.AspNetCore.App` framework reference。将 `DrasiWakeHostBuilder.CreateHost` 两个 overload 改用 `Host.CreateDefaultBuilder` 与 `ConfigureServices`/`ConfigureAppConfiguration`，再使用 `ConfigureWebHostDefaults`、DotNext HTTP/2 协议 middleware、持久化集群配置和 `JoinCluster`。仅监听配置的地址；Kestrel 启动前校验证书和 TLS 信任。

- [ ] **步骤 4：调整服务注册和启动次序。** 保留 `HostStartupValidator` 中可在 Raft 前执行的工作（设置、契约、目录锁、migration、静态 target 保留时长）；移除其直接调用 `IBridgeStore` 写入状态的行为。先启动集群/状态机，再启动 Leader controller。当选 Leader 后，先经 Raft 提交 target 回填/校验，再恢复业务 worker。

- [ ] **步骤 5：更新 `appsettings.json` 和 Host 测试配置辅助方法。** 保持配置文件不含密钥，示例配置单节点本地拓扑并记录环境变量键名。验证旧配置仍默认启动单节点模式且生产使用 Raft Store。

- [ ] **步骤 6：运行 Host 启动测试和 Release build。**

  运行：`dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.HostStartupTests`

  运行：`dotnet build src\DrasiWake.Host\DrasiWake.Host.csproj -c Release`

  预期：两个命令成功；既有 `Valid_registry_migrates_database_and_database_directory_has_single_owner` 测试继续通过。

  ```text
  git add DrasiWake.sln src/DrasiWake.Host tests/DrasiWake.IntegrationTests/HostStartupTests.cs
  git commit -m "feat: host DrasiWake Raft cluster"
  ```

## 任务 6：按领导权 epoch 门控并重新创建 Bridge worker

**文件：**

- 新建：`src/DrasiWake.Host/RaftLeaderHostedService.cs`
- 新建：`src/DrasiWake.Host/LeaderWorkerRuntime.cs`
- 修改：`src/DrasiWake.Host/DrasiWakeHostBuilder.cs`
- 修改：`src/DrasiWake.Host/BridgeHostedService.cs`
- 修改：`src/DrasiWake.Host/ReconciliationHostedService.cs`
- 修改：`src/DrasiWake.Core/Pipeline/RecoveryCoordinator.cs`
- 修改：`tests/DrasiWake.IntegrationTests/HostStartupTests.cs`
- 新建：`tests/DrasiWake.IntegrationTests/LeaderWorkerLifecycleTests.cs`

**接口：**

- `IRaftLeadership` 暴露 `bool IsLeader`、`bool HasQuorum`、`long Term` 和 `IAsyncEnumerable<RaftLeadershipChange> WatchLeadershipAsync(CancellationToken)`。
- `RaftLeadershipChange` 定义为 `public sealed record RaftLeadershipChange(long Term, bool IsLeader, bool HasQuorum);`。
- `RaftLeaderHostedService` 依赖 `IRaftLeadership` 和 `IServiceScopeFactory`；领导权/quorum 改变时创建一个 scoped `LeaderWorkerRuntime`；term 改变、失去 quorum、取消或释放时先取消并等待 runtime 完成，再观察下一 epoch。
- `LeaderWorkerRuntime.StartAsync(CancellationToken)` 先执行恢复和周期对账，再启用 Bridge 接收/分发；`StopAsync(CancellationToken)` 取消该 epoch 的任务并释放 scope。
- 每个 epoch 的 scope 创建新的 `SignalInbox`、`SessionPartitioner`、`SnapshotReconciler`、`RecoveryCoordinator`、`BridgeHostedService` 和 `ReconciliationHostedService`。停止后不得复用一次性 inbox/partitioner。

- [ ] **步骤 1：先写 fake-leadership 生命周期测试。** 验证 follower 不建立 Drasi watch 或调用 Gateway；一个 Leader epoch 只创建一个 runtime；失去 quorum 后 runtime 取消；新 term 创建新 runtime；上一 runtime 完成慢速关闭前不得启动下一 runtime。

- [ ] **步骤 2：运行生命周期测试类，确认测试先失败。**

  运行：`dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.LeaderWorkerLifecycleTests`

- [ ] **步骤 3：实现 Leader coordinator 和 scoped runtime。** 将 DotNext Leader 通知/消息总线状态适配为 `IRaftLeadership`。每个 term 使用独立 linked cancellation token。只有本地投影追平且集群具有可写多数派时，才可将 epoch 标记为活动。

- [ ] **步骤 4：使恢复可重复执行。** 每个 runtime 先提交中断分发恢复命令，再枚举所有可见 Drasi 查询并对权威 snapshot 对账；完成后才启动 dispatch polling 和信号接收。领导权丢失时取消必须传播；非取消异常不得被吞掉。unexpected worker failure 终止该 epoch，并使用固定错误类别记录。

- [ ] **步骤 5：运行生命周期及既有恢复测试。**

  运行：`dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.LeaderWorkerLifecycleTests`

  运行：`dotnet test --project tests\DrasiWake.Core.Tests\DrasiWake.Core.Tests.csproj -c Release --filter-class DrasiWake.Core.Tests.Pipeline.RecoveryCoordinatorTests`

  预期：只有当前 Leader 运行工作，且原恢复语义不变。

  ```text
  git add src/DrasiWake.Host src/DrasiWake.Core/Pipeline/RecoveryCoordinator.cs tests/DrasiWake.IntegrationTests
  git commit -m "feat: gate bridge workers on Raft leadership"
  ```

## 任务 7：增加认证成员管理和配置兼容性检查

**文件：**

- 新建：`src/DrasiWake.Host/RaftMembershipContracts.cs`
- 新建：`src/DrasiWake.Host/ClusterMembershipGrpcService.cs`
- 新建：`src/DrasiWake.Host/ClusterConfigurationFingerprint.cs`
- 修改：`src/DrasiWake.Host/DrasiWakeHostSettings.cs`
- 修改：`src/DrasiWake.Host/DrasiWakeHostBuilder.cs`
- 修改：`src/DrasiWake.Host/HostStartupValidator.cs`
- 新建：`tests/DrasiWake.IntegrationTests/ClusterMembershipTests.cs`
- 修改：`tests/DrasiWake.IntegrationTests/HostStartupTests.cs`

**接口：**

- `IRaftMembershipManager` 暴露：

  ```csharp
  Task AddMemberAsync(Uri endpoint, CancellationToken cancellationToken);
  Task RemoveMemberAsync(Uri endpoint, CancellationToken cancellationToken);
  ```

- `ClusterCompatibility` 定义为 `public sealed record ClusterCompatibility(string NodeId, string ApplicationVersion, string ConfigurationFingerprint);`。
- `IClusterCompatibilityProbe` 暴露 `Task<ClusterCompatibility> GetCompatibilityAsync(Uri endpoint, CancellationToken cancellationToken)`。
- code-first gRPC 契约定义 `GetCompatibility`、`Add` 和 `Remove`；每个方法都必须验证配置的 Bearer Token。
- `ClusterConfigurationFingerprint.Create(ContractRegistry registry, Uri drasiServerUri, IReadOnlyDictionary<string, OpenClawTargetOptions> targets, OpenClawOptions retryOptions)` 对规范化后的注册表、Drasi 身份、Gateway 逻辑名称/地址/保留时长和 retry 行为计算 SHA-256 十六进制摘要；排除密钥、节点身份、监听地址和本地目录。
- Add 前先通过 TLS 查询候选节点；若 NodeId 已存在、配置指纹不一致或应用命令版本不兼容，则不得执行 DotNext 成员变更。Remove 由 Leader 转发并要求 quorum 提交。

- [ ] **步骤 1：先写 gRPC 鉴权和成员管理失败测试。** 验证缺失、格式错误和错误令牌均被拒绝且响应不回显令牌；合法调用只调用 membership manager 一次；follower 将请求恰好转发给 Leader 一次；无 quorum 时失败；配置/版本不匹配时不调用 `AddMemberAsync`。

- [ ] **步骤 2：运行成员管理测试类，确认测试先失败。**

  运行：`dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.ClusterMembershipTests`

- [ ] **步骤 3：实现 code-first gRPC 契约和逐请求授权。** 使用 constant-time 字节比较校验 Bearer Token。管理 listener 和候选节点兼容查询都必须使用 HTTPS。返回固定错误代码，不返回异常消息或秘密。

- [ ] **步骤 4：实现指纹持久化和兼容性预检。** 初次 bootstrap 时将业务配置指纹作为集群元数据提交；重启时在 Leader 工作启动前检查指纹；Add 前调用候选节点兼容性接口，拒绝重复 NodeId、指纹不一致或命令版本不兼容。管理 RPC 不传递注册表正文或凭据。

- [ ] **步骤 5：实现经 Leader 路由的 Add/Remove 和安全校验。** 使用 DotNext 持久集群配置与 `IRaftHttpCluster.AddMemberAsync`/`RemoveMemberAsync`。验证 endpoint scheme/address、重复/现存成员、移除最后一个成员、当前 Leader 和 quorum。日志只记录操作类型、成员稳定 ID、结果代码和时间。

- [ ] **步骤 6：运行成员管理、Host 启动和遥测脱敏测试并提交。**

  运行：`dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.ClusterMembershipTests`

  运行：`dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.TelemetryRedactionTests`

  预期：未授权或不兼容的成员变更不产生任何效果；日志和错误响应中无凭据。

  ```text
  git add src/DrasiWake.Host tests/DrasiWake.IntegrationTests
  git commit -m "feat: secure Raft membership management"
  ```

## 任务 8：验证三节点复制、quorum 丢失和故障切换

**文件：**

- 新建：`tests/DrasiWake.IntegrationTests/Fixtures/RaftClusterFixture.cs`
- 新建：`tests/DrasiWake.IntegrationTests/RaftFailoverTests.cs`
- 修改：`tests/DrasiWake.IntegrationTests/DrasiWake.IntegrationTests.csproj`
- 修改：`tests/DrasiWake.IntegrationTests/HostStartupTests.cs`

**接口：**

- `RaftClusterFixture.CreateAsync(int nodeCount, CancellationToken)` 启动三个真实 DotNext 节点，使用不同空闲端口、测试 TLS 证书、Raft 目录、SonnetDB 目录、相同管理令牌和相同业务配置指纹。
- Fixture 暴露 `WaitForLeaderAsync`、`StopNodeWithoutMembershipChangeAsync`、`RestartNodeAsync`、`ReadProjectionAsync` 和 `DisposeAsync`。
- 每个节点使用 fake Drasi source 和具幂等能力的 fake Gateway，但使用真实 DotNext consensus、持久集群配置、状态机、Raft Store 和 SonnetDB 投影。

- [ ] **步骤 1：先写命令复制与持久重启失败测试。** 经 Leader 提交一条 `CreateOrUpdatePendingWake`；等待三个投影到达相同 Raft 索引；停止并重启一个 follower，沿用原本地目录；断言它从 snapshot/日志恢复，并在标记 ready 前得到相同 outbox/checkpoint 状态。

- [ ] **步骤 2：先写 Leader 故障切换测试。** 提交 pending wake，在不调用成员移除的情况下停止 Leader 的传输；等待另外两个节点选出新 Leader；断言新 Leader 完成恢复并分发 wake。

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

- [ ] **步骤 3：先写失去 quorum 测试。** 不做成员移除而停止两个节点；断言剩余节点不能提交命令、不能为新 claim 调 Gateway、outbox 状态保持不变。恢复一个节点后断言 quorum 恢复、Leader 重新选出、已提交状态一致。

- [ ] **步骤 4：先写成员变更集成测试。** 通过兼容性预检添加第四个节点，使用独立本地目录；等待其追平后经认证 gRPC 移除；断言剩余成员继续提交且被移除节点不再接收日志项。

- [ ] **步骤 5：实现 Fixture 并运行故障测试。** 动态分配端口、使用临时 TLS 证书、在 abrupt-stop 路径禁用优雅成员移除，并在 Fixture 释放时清理每个命名测试目录。不得依赖外部 Drasi/Gateway 或固定 sleep；使用带超时的 term、索引和状态变更断言。

  运行：`dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.RaftFailoverTests`

  预期：真实节点复制、快照、Leader 切换、quorum、幂等性和成员管理断言全部通过。

- [ ] **步骤 6：运行所有集成测试并提交。**

  运行：`dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release`

  预期：本地集成测试全部通过；真实服务测试仍保持 opt-in。

  ```text
  git add tests/DrasiWake.IntegrationTests
  git commit -m "test: verify Raft failover and quorum behavior"
  ```

## 任务 9：添加 HA 遥测和运维文档

**文件：**

- 修改：`src/DrasiWake.Core/Pipeline/BridgeTelemetry.cs`
- 修改：`src/DrasiWake.Host/RaftLeaderHostedService.cs`
- 修改：`src/DrasiWake.Host/ClusterMembershipGrpcService.cs`
- 修改：`docs/bridge-core-v1-operations.md`
- 修改：`README.md`
- 修改：`src/DrasiWake.Host/appsettings.json`
- 修改：`tests/DrasiWake.IntegrationTests/TelemetryRedactionTests.cs`

**接口：**

- 增加低基数指标：节点角色、quorum/可写状态、Leader 变化、commit/apply 索引差、snapshot 导出/恢复、follower 追平、成员变更结果、领导权恢复时间和故障切换时间。
- 为 Leader 恢复和成员管理添加 activity；遥测中的成员 endpoint 必须哈希，结果/错误使用固定代码。
- 文档说明三节点环境变量和目录、TLS 证书/信任配置、共享管理令牌注入、Add/Remove 安全顺序、无 quorum 行为、重启恢复和不变的单节点开发启动方式。

- [ ] **步骤 1：先写遥测脱敏失败断言。** 验证 Raft 成员 URL、管理令牌、证书密码、事实载荷和异常文本不出现在日志或 metric tag 中。
- [ ] **步骤 2：实现有限基数指标和安全日志。** 复用 `BridgeTelemetry` 的 ActivitySource/Meter；不得将原始 NodeId、endpoint 或任意异常文本作为指标维度。
- [ ] **步骤 3：更新运维手册和 README。** 说明每个成员使用自己的 SonnetDB/Raft 目录、三节点 quorum 与恢复顺序；展示安全 secret-provider 键名但不提供凭据；说明仅多数派可以推进处理且 Gateway 语义是至少一次。
- [ ] **步骤 4：用 Host 设置测试核对文档命令和配置键。** 提供三个节点的示例，节点 ID、路径、地址各不相同，包含 TLS 证书引用和 secret-provider 令牌键名；已签入 `appsettings.json` 继续使用无密钥单节点配置。
- [ ] **步骤 5：运行脱敏测试和 Release 全量验证。**

  运行：`dotnet test --project tests\DrasiWake.IntegrationTests\DrasiWake.IntegrationTests.csproj -c Release --filter-class DrasiWake.IntegrationTests.TelemetryRedactionTests`

  运行：`dotnet test --solution DrasiWake.sln --configuration Release`

  运行：`dotnet publish src\DrasiWake.Host\DrasiWake.Host.csproj -c Release -o artifacts\publish`

  预期：必需测试全部通过；opt-in 真实服务测试在未显式启用时继续跳过；Host 发布产物包含 DotNext 和 ASP.NET Core 所需依赖。

  ```text
  git add README.md docs/bridge-core-v1-operations.md src/DrasiWake.Core/Pipeline/BridgeTelemetry.cs src/DrasiWake.Host tests/DrasiWake.IntegrationTests/TelemetryRedactionTests.cs
  git commit -m "docs: document DrasiWake Raft HA operations"
  ```

## 最终验收清单

- [ ] 三个真实 DotNext 投票节点对每条已提交命令收敛一致，并可恢复完整快照。
- [ ] 单节点故障时仍有 quorum；新 Leader 只有在本地投影追平并完成 Drasi snapshot 对账后才能恢复工作。
- [ ] quorum 丢失时禁止新 claim 和新 Gateway 调用。
- [ ] Gateway 接受结果不确定时，只能以原幂等键重放，且最终只创建一个逻辑 invocation。
- [ ] `Accepted` 与 checkpoint 在所有副本及重启后仍保持原子性。
- [ ] 运行时 Add/Remove 使用 TLS、Bearer Token、兼容性检查、quorum 提交和无秘密审计。
- [ ] 应用版本或配置指纹不匹配的新成员在成员变更前被拒绝。
- [ ] 原单节点配置和既有测试仍通过相同 Raft 状态机路径运行。
- [ ] 运维文档说明独立存储、quorum 限制、TLS、令牌注入、成员管理、恢复和至少一次语义。
