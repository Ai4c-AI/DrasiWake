# DrasiWake Bridge Core V1 运维手册

## 范围与运行模型

Drasi attach 通知只是提示，不是持久化事件日志。Drasi `results` 才是权威数据：启动恢复、attach/重连通知、定期枚举和 Channel 溢出恢复都会重新读取当前结果集。Bridge 会收敛到最新的受支持快照，但不保证每个中间变化都触发一次唤醒。

Host 会在分发前持久化每个待处理唤醒。Gateway 受理结果及对应的快照 checkpoint 会在同一个 SonnetDB 事务中提交。执行状态通过后续回执更新：`Accepted` 或 `Executing` 不等于 `Completed`。

V1 要求每个 SonnetDB 目录只能由一个活动 Host 持有。目录所有权锁会阻止第二个进程使用同一目录启动；这不是多副本租约或高可用（HA）协议。

## 配置与启动

仓库中的默认配置位于 `src/DrasiWake.Host/appsettings.json`，示例注册表和 schema 位于 `src/DrasiWake.Host/contracts/`。请以示例绑定为基础进行配置，填入实际的 Drasi 查询和 schema，并确保 `factSchemaPath` 使用相对于注册表文件的路径。

相关环境变量遵循标准 .NET 双下划线命名约定：

| 配置项 | 环境变量示例 | 说明 |
| --- | --- | --- |
| Drasi 服务器 | `DrasiWake__Drasi__ServerUri` | 绝对 HTTP 或 HTTPS URL。 |
| OpenClaw Gateway | `DrasiWake__OpenClaw__BaseAddress` | 专用集成端点的服务基础 URL。 |
| Gateway 凭据 | `DrasiWake__OpenClaw__BearerToken` | 通过环境变量或密钥提供程序传入；切勿提交到版本库。 |
| SonnetDB 目录 | `DrasiWake__Database__Path` | 每个目录只能由一个活动 Host 持有。 |
| 绑定注册表 | `DrasiWake__Registry__Path` | YAML 注册表路径。 |
| 最大重试时长 | `DrasiWake__Outbox__MaximumRetryAge` | Gateway 幂等记录的保留时间不得短于此值，也必须覆盖所有绑定的重试时长。 |
| 信号队列容量 | `DrasiWake__ChannelCapacity` | 有界内存提示队列；队列溢出时会将查询标记为待对账。 |
| Worker 数量 | `DrasiWake__WorkerCount` | 并发会话 Worker 的最大数量。 |

请在 Host 项目目录中运行，以便正确发现 `appsettings.json`，并按示例解析数据库和注册表的相对路径：

```powershell
Push-Location src/DrasiWake.Host
dotnet run
Pop-Location
```

启动时会校验配置和完整注册表、检查 Gateway 幂等记录保留时间、获取数据库目录所有权，并在启动 Drasi 接收器前应用 EF migration。配置无效或目录已被其他活动实例持有时，Host 将无法启动。

## 恢复与状态

- Host 会在普通 outbox 轮询开始前枚举 Drasi 查询，并在启动期间对账 `results`。
- 收到重连信号后会读取快照。SSE 断开后会按有界指数退避策略重新连接。
- 周期性对账器会枚举查询并处理已标记为待对账的查询；即使信号 Channel 已满，SSE 读取也不会被快照 I/O 阻塞。
- 待处理工作会一直持久化，直到 Gateway 受理结果提交。如果进程在调用期间退出，重启后会复用已持久化的幂等键。
- Gateway 冲突或结果不确定的调用不会被默认为成功。排查死信时，请检查 outbox 状态和固定的 `LastErrorCode` 值。

outbox 状态说明：

| 状态 | 含义 |
| --- | --- |
| `Pending`、`RetryScheduled`、`Dispatching` | 尚未被持久化地确认受理；到达计划时间后可进行分发或恢复。 |
| `Accepted` | Gateway 受理结果和快照 checkpoint 已提交；执行是否完成仍未知。 |
| `Executing` | Gateway 报告该调用正在执行。 |
| `Completed` | Gateway 执行回执报告任务已完成。 |
| `DeadLetter` | 契约或载荷永久失败、调用结果不确定，或重试次数已耗尽；需要操作人员检查。 |
| `Superseded` | 更新的权威快照替代了这个尚未被受理的唤醒。 |

## 遥测与隐私

Host 注册了 `DrasiWake.Bridge` ActivitySource 和 Meter。遥测指标覆盖信号接收/溢出/队列深度、可见查询及路由结果、对账和快照读取失败、重复指纹、outbox 创建数量/等待时长、受理延迟、重试、死信以及执行状态查询失败。Activity 覆盖快照对账和 outbox 分发。

遥测中的标识符是由 SHA-256 派生的稳定短 ID。事实数据、Bearer 凭据、幂等键以及原始会话/查询标识符都不会作为 tag 输出。Host 日志记录固定错误类别和经过哈希处理的查询标识符，不记录异常消息或载荷内容。

默认 Host 配置不会添加 exporter 或 collector 端点。请在部署组合中配置 OpenTelemetry exporter，将这些指标发送到遥测后端；仅使用示例配置不会导出任何指标。

## 验证

在仓库根目录运行本地自动化测试套件，并执行 Release 发布：

```powershell
dotnet test --solution DrasiWake.sln
dotnet publish src/DrasiWake.Host/DrasiWake.Host.csproj -c Release -o artifacts/publish
```

运行发布产物时，请将 `artifacts/publish` 设为工作目录，以便程序能找到复制过去的配置和 `contracts/` 文件：

```powershell
Push-Location artifacts/publish
.\DrasiWake.Host.exe
Pop-Location
```

确定性集成测试使用真实 adapter 和 SonnetDB provider，并通过本地 HTTP 契约处理器模拟服务端。测试会验证收敛、幂等重放、数据库重开和崩溃窗口，但不能据此认证已部署的 Drasi 或 OpenClaw Gateway 版本。

真实服务检查需显式启用；未设置运行标志时会跳过。Drasi 检查需要设置 `DRASIWAKE_RUN_REAL_DRASI_CONTRACT_TESTS=1`、`DRASIWAKE_REAL_DRASI_URL`、`DRASIWAKE_REAL_DRASI_INSTANCE_ID` 和 `DRASIWAKE_REAL_DRASI_QUERY_ID`。该检查会枚举查询、读取 results 并发起 attach 请求。当前 adapter 尚未配置 Drasi 身份验证。

Gateway 检查可能会执行 MetaSkill DAG。请只对专用且安全的契约测试 MetaSkill 和 Gateway 运行。需要设置 `DRASIWAKE_RUN_REAL_GATEWAY_CONTRACT_TESTS=1`、`DRASIWAKE_REAL_GATEWAY_URL`、`DRASIWAKE_REAL_GATEWAY_TOKEN`、`DRASIWAKE_REAL_GATEWAY_TEST_SKILL`、`DRASIWAKE_REAL_GATEWAY_BUILD`、`DRASIWAKE_REAL_GATEWAY_IDEMPOTENCY_RETENTION` 和 `DRASIWAKE_REAL_MAX_OUTBOX_RETRY_AGE`。测试会创建唯一会话和幂等键，并发发送相同请求，检查重放是否返回同一个 invocation ID，并验证声明的保留时间不短于最大重试时长。测试输出会记录 Gateway build 和保留时间，但不会打印凭据。

例如，在当前 PowerShell 终端设置好所需变量后，可通过以下命令显式启用 Gateway 检查：

```powershell
$env:DRASIWAKE_RUN_REAL_GATEWAY_CONTRACT_TESTS = '1'
dotnet test --project tests/DrasiWake.IntegrationTests/DrasiWake.IntegrationTests.csproj --filter-class DrasiWake.IntegrationTests.RealGatewayContractTests
```

在目标部署版本和配置上通过两项真实服务检查，并记录相应证据之前，请保持 V1-ready 门禁关闭。本地模拟服务和单元测试不能替代这些检查。