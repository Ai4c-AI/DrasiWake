# DrasiWake Bridge Core V1 运维手册

## 范围与运行模型

Drasi attach 通知只是提示，不是持久化事件日志。Drasi `results` 才是权威数据：启动恢复、attach/重连通知、定期枚举和 Channel 溢出恢复都会重新读取当前结果集。Bridge 会收敛到最新的受支持快照，但不保证每个中间变化都触发一次唤醒。

Host 会在分发前通过 Raft 提交每个待处理唤醒。Raft 命令是状态权威，各节点的 SonnetDB 是独立、可重建的本地投影。Gateway 受理结果及对应的快照 checkpoint 在同一条 Raft 命令中提交，并在每个投影的同一个 SonnetDB 事务中应用。执行状态通过后续回执更新：`Accepted` 或 `Executing` 不等于 `Completed`。

每个 SonnetDB 目录只能由一个活动 Host 持有。目录所有权锁仅防止同目录双开；HA 由 DotNext Raft 提供，不共享数据库目录，也不以共享文件锁代替共识。只有当前且具有多数派 quorum 的 Leader 推进接收、对账、恢复和分发 Worker；Follower 不推进业务工作。任何写入失败都不得回退为本地写入。

Gateway 是 **at-least-once** 分发，不是 exactly-once。调用已被 Gateway 受理但 Raft 确认尚未提交的崩溃窗口可能导致重发；新 Leader 复用持久化的稳定幂等键。Gateway 必须保持幂等记录至少覆盖所有绑定的最大重试窗口。

## 配置与启动

仓库中的默认配置位于 `src/DrasiWake.Host/appsettings.json`，示例注册表和 schema 位于 `src/DrasiWake.Host/contracts/`。请以示例绑定为基础进行配置，填入实际的 Drasi 查询和 schema，并确保 `factSchemaPath` 使用相对于注册表文件的路径。

相关环境变量遵循标准 .NET 双下划线命名约定：

| 配置项 | 环境变量示例 | 说明 |
| --- | --- | --- |
| Drasi 服务器 | `DrasiWake__Drasi__ServerUri` | 绝对 HTTP 或 HTTPS URL。 |
| Gateway URL | `DrasiWake__OpenClaw__Targets__<target>__BaseAddress` | 每个命名目标一个专用集成端点的服务基础 URL。 |
| Gateway 凭据 | `DrasiWake__OpenClaw__Targets__<target>__BearerToken` | 目标专属可选凭据；通过环境变量或密钥提供程序传入，切勿提交到版本库。 |
| Gateway 幂等保留时间 | `DrasiWake__OpenClaw__Targets__<target>__GatewayIdempotencyRetention` | 每个目标的保留时长必须覆盖注册表中引用该目标的所有绑定的最大 `retry.maxAgeSeconds`。 |
| SonnetDB 目录 | `DrasiWake__Database__Path` | 每个目录只能由一个活动 Host 持有。 |
| 绑定注册表 | `DrasiWake__Registry__Path` | YAML 注册表路径。 |
| 信号队列容量 | `DrasiWake__ChannelCapacity` | 有界内存提示队列；队列溢出时会将查询标记为待对账。 |
| Worker 数量 | `DrasiWake__WorkerCount` | 并发会话 Worker 的最大数量。 |

每个注册表绑定都必须包含 `openClawTarget`，且其值必须与配置的目标名称完全匹配。Host 启动时会逐目标验证幂等保留时长。目标凭据仅属于对应目标，建议由环境变量、User Secrets 或部署 secret provider 注入。

请在 Host 项目目录中运行，以便正确发现 `appsettings.json`，并按示例解析数据库和注册表的相对路径：

```powershell
Push-Location src/DrasiWake.Host
dotnet run
Pop-Location
```

启动时会校验配置和完整注册表、检查每个目标的 Gateway 幂等记录保留时间、获取数据库目录所有权，并在启动 Drasi 接收器前应用 EF migration。迁移后，Host 会从当前注册表绑定回填尚未结束且缺少目标的旧 outbox 记录；若记录的绑定已不存在，或活动记录仍引用未配置的目标，Host 将拒绝启动。移除仍被活动 outbox 工作引用的目标前，应先处理这些记录；已完成、死信或已取代的历史记录不阻止移除目标。

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

## 三节点 HA 部署

保持默认 `appsettings.json` 为无密钥的 `SingleNode`；上面的 `dotnet run` 开发启动不变，单节点也使用 Raft 命令而不是另一条本地写入路径。部署示例：

- [node-a.json](examples/raft/node-a.json)：`node-a`，Raft `https://node-a.example.test:5101/`，管理 `https://node-a.example.test:5102/`，目录 `C:\DrasiWake\node-a\sonnetdb` / `C:\DrasiWake\node-a\raft`。
- [node-b.json](examples/raft/node-b.json)：`node-b`，Raft `https://node-b.example.test:5101/`，管理 `https://node-b.example.test:5102/`，目录 `C:\DrasiWake\node-b\sonnetdb` / `C:\DrasiWake\node-b\raft`。
- [node-c.json](examples/raft/node-c.json)：`node-c`，Raft `https://node-c.example.test:5101/`，管理 `https://node-c.example.test:5102/`，目录 `C:\DrasiWake\node-c\sonnetdb` / `C:\DrasiWake\node-c\raft`。

这些是可由 Host settings 校验的配置示例，不是开箱即用的部署：替换 DNS、Drasi/Gateway 服务地址、注册表和证书路径；确保三个注册表、schema、应用/命令/envelope 版本和非密钥配置指纹一致。`production-gateway` 必须与注册表的 `openClawTarget` 一致。将每个示例复制为对应节点发布目录的 `appsettings.json`，或由部署配置提供程序加载等价键值；Host 不会自动读取 `docs\examples`。每台节点的 DNS 必须解析到本机可绑定地址，Raft 和管理端口分别允许节点间 HTTP/2 TLS 通信。

| 配置键 | 约束 / 注入方式 |
| --- | --- |
| `DrasiWake:Cluster:Mode` | `Cluster`；开发默认 `SingleNode`。 |
| `DrasiWake:Cluster:NodeId` | 三节点分别为 `node-a` / `node-b` / `node-c`，集群内唯一。 |
| `DrasiWake:Cluster:ListenAddress` | 无凭据、query、fragment 或非根路径的 HTTPS URI。 |
| `DrasiWake:Cluster:InitialMembers:0..2` | 同一初始成员集合，含本节点 ListenAddress；持久化成员配置建立后不能用编辑种子列表代替 Add/Remove。 |
| `DrasiWake:Database:Path` | 每节点独立 SonnetDB 目录；不可使用同一个网络共享目录。 |
| `DrasiWake:Cluster:RaftDataPath` | 每节点独立 Raft 目录，不能等于数据库目录；包含 `log`、`snapshots` 和 `cluster-configuration`。 |
| `DrasiWake:Cluster:Certificate:Path` | 本节点带私钥的 PFX/PKCS#12，示例不携带证书。 |
| `DrasiWake:Cluster:Certificate:Password` | 必需；secret provider 注入，环境键 `DrasiWake__Cluster__Certificate__Password`。 |
| `DrasiWake:Cluster:Management:Address` | HTTPS，TCP 端口必须与本节点 Raft ListenAddress 不同。 |
| `DrasiWake:Cluster:Management:BearerToken` | 所有节点共享的必需管理凭据，仅 secret provider 注入，环境键 `DrasiWake__Cluster__Management__BearerToken`。 |
| `DrasiWake:Cluster:SnapshotFrequency` | 必需正整数，示例 `1000`；应用此数量业务命令后导出完整投影快照。 |

密钥由环境变量、.NET User Secrets（开发）或部署 secret provider 提供。上述键名不含真实凭据；不要在示例 JSON、注册表、命令行参数或日志中放密码/token。按命名 Gateway 的键 `DrasiWake__OpenClaw__Targets__production-gateway__BearerToken` 独立注入其凭据。共享管理 token 不是客户端证书认证：管理 RPC 要求 TLS 和 Bearer，Raft 网络应额外由网络边界保护。

证书须在有效期内、含私钥、允许 server authentication，SAN 覆盖本节点 Raft 和 Management DNS（使用 IP 时覆盖相应 IP）。每个节点及管理客户端的 OS 信任库应信任签发 CA 和完整证书链；默认 HTTP 客户端校验名称和链，不禁用证书校验。证书、目录和 secret provider 访问权限仅授予运行账户；先配置所有节点信任，再轮换证书/token，避免破坏多数派通信。

### 安全 Add / Remove 顺序

1. 初次部署使用上述相同三节点 seed 集合和空的独立目录；按 ListenAddress 字典序最小的节点（这里 `node-a`）是冷启动 bootstrap，其余节点加入，必须达到多数派后才能写入。无需对初始三个成员逐个 Add。
2. 扩容/替换时先部署候选节点：新 NodeId、HTTPS 根 endpoint、独立空目录、正确证书/trust、共享管理 token，以及与集群兼容的配置。`InitialMembers` 包含本节点和可达集群成员，并显式设置候选的 `DrasiWake:Cluster:BootstrapMembers:0..N` 为已有的 bootstrap 成员集合（均须在 InitialMembers 中），**不包含候选自身**，避免在 Add 提交前把候选提前种入投票配置。此覆盖仅用于空目录初始配置，不能重配置已有集群。**不要让候选 endpoint 的字典序小于已有 bootstrap endpoint**，以免将空节点冷启动成独立集群。
3. 使用 HTTPS code-first gRPC `IClusterMembershipService`，在请求头提供 `authorization: Bearer <secret-provider-value>`，调用 `Add`，`ClusterMemberRequest.Endpoint` 是候选 **Raft** 地址（不是管理地址）。所有节点须使用相同管理 TCP 端口（示例为 `5102`）；兼容性探测/转发由远端 Raft 主机名与调用节点配置的管理端口构建 HTTPS 管理地址，不计算端口偏移。Follower 可转发到当前 Leader；成功必须是成员配置已提交，不是请求已发送。
4. 等待候选的快照/log 应用追上当前 committed index、成员可达且多数派稳定。一次只做一项成员变更，先 Add 并验证替代成员，再 Remove 旧成员；四节点过渡期多数派为三，尤其不能提前停止旧节点。
5. 调用 `Remove`，同样传旧节点 Raft endpoint。等成员配置提交并确认 quorum 之后才停止/下线旧进程。不要并发 Add/Remove，不要移除最后一个成员；优先移除 Follower，若必须替换 Leader，等待新 Leader 和恢复完成后继续。
6. `Aborted` 表示 leadership/term 变化，重新查询后重试；`Unavailable` 表示不能提交（例如无 quorum），恢复网络/成员而不是绕过校验。认证、证书或兼容性失败须先修复部署配置。管理日志/活动仅显示 endpoint 哈希与固定类别。

### Quorum 丢失与恢复

三节点可容忍一个节点不可用，失去两个节点则不能提交命令或成员变更；接收/对账/分发 epoch 被取消，不以 SonnetDB 本地写入维持假进度。已经发出的 Gateway 请求仍可能被受理，不能据此推断 Raft 已确认或执行 exactly-once。恢复足够成员/网络后，当前有 quorum 的 Leader 先校验配置、恢复 outbox/对账，再启动 Worker。

同一 term 的 Leader 启动恢复遇到明确的暂时性上游故障（HTTP 408/429/502/503/504、已分类的连接/DNS/响应结束错误、HTTP 超时，以及 `HttpIOException(ResponseEnded)` 响应体提前 EOF）时，会以 1、2、4、5 秒封顶的可取消退避重新创建 scope 并执行恢复；此前 epoch 必须先取消并停止。日志只记录固定类别 `leader-recovery-transient`，每次恢复保留原有结果遥测。失去 leadership/quorum 或停止 Host 会中止重试；配置校验/指纹不一致、损坏持久化状态、无效响应数据和其他启动/Worker 错误不采用此重试路径，保持 fail-closed。普通 `IOException`（包括未归类的连接重置）不因此自动重试，以免把存储错误掩盖为网络故障。

节点正常重启须保留自己的 Raft 数据和 SonnetDB。Host 启动先恢复持久化快照，再重放快照之后的已提交 log；Follower 接收 Leader 的 log/快照追赶。SonnetDB 投影可以在**节点停止且有可用 Raft 快照/log**时隔离原目录并从 Raft 重建，但不得把别的运行节点的数据库或单独的 Raft 文件直接复制进来。备份/恢复须一致保留该节点完整 Raft 目录（含成员配置、log、snapshots），先停止节点或采用一致的存储快照。丢失 Raft 数据的节点应作为新成员经 Add 重新加入；不要删除多数派 Raft 数据来强制重建 quorum。集群整体灾难恢复需独立的经过演练的恢复流程，不能把空目录当现有集群权威。

## 遥测与隐私

Host 注册了 `DrasiWake.Bridge` ActivitySource 和 Meter。遥测指标覆盖信号接收/溢出/队列深度、可见查询及路由结果、对账和快照读取失败、重复指纹、outbox 创建数量/等待时长、受理延迟、重试、死信以及执行状态查询失败。Activity 覆盖快照对账和 outbox 分发。

HA 指标均使用相同 Meter，索引差单位为 entry，时长单位为 ms：

| 指标 | 含义 / 固定维度 |
| --- | --- |
| `drasiwake.raft.node.role` | 值 1，`role=leader/follower/electing`；electing 表示尚无已知 Leader，不断言内部 Candidate 状态。 |
| `drasiwake.raft.quorum` / `drasiwake.raft.writable` | 0/1；quorum 是本地 consensus token 与可达多数派，writable 还要求当前 Leadership token 有效，不表示 Gateway 已可用。 |
| `drasiwake.raft.leader.changes` | 本节点开始领导新 term 的次数（不是全局选举次数；恢复同一 term 的 quorum 不重复计数）。 |
| `drasiwake.raft.commit.lag` | 本地 last log index - committed index，非负。 |
| `drasiwake.raft.apply.lag` | 本地 committed index - WAL applied index，非负。 |
| `drasiwake.raft.follower.catchup.lag` | Follower/electing 本地 committed - applied，Leader 为 0；不代表 Leader 尚未传来的未知 log。 |
| `drasiwake.raft.follower.catchup.duration` | 100ms leadership 观察循环看到 Follower apply backlog 从正数归零的耗时；短于采样间隔的追赶可能不形成 duration 样本。 |
| `drasiwake.raft.apply` / `drasiwake.raft.apply.duration` | 真正状态机 apply/no-payload 回调的次数与耗时，`result=succeeded/failed/cancelled`。 |
| `drasiwake.raft.snapshots` / `drasiwake.raft.snapshot.duration` | 实际快照回调，`operation=export/restore` 和 `result=succeeded/failed/cancelled`，包含读写/序列化/投影恢复时间。 |
| `drasiwake.raft.membership` | Add/Remove RPC 的结果次数，`operation=add/remove` 和固定 `result` 类别；无 member 标签。 |
| `drasiwake.raft.leader.recovery.duration` | epoch 开始到恢复/Worker 启动完成（或失败/取消）的耗时，固定 `result`。 |
| `drasiwake.raft.failover.duration` | 本节点从观察到不可写（含初始 Follower 等待）到再次作为有 quorum 的 Leader 完成恢复的耗时；不是客户端或集群全局 SLA。 |

`raft.leader.recovery` Activity 包含恢复结果；`raft.membership` Activity 包含 `operation`、`result` 和稳定哈希 `member.id`。成员结果的固定集合为 `succeeded`、`failed`、`cancelled`、`unauthenticated`、`disabled`、`invalid-endpoint`、`invalid-management-endpoint`、`stale-leader`、`no-quorum`、`duplicate-endpoint`、`duplicate-node-id`、`member-not-found`、`last-member`、`incompatible-member`、`tls-trust-failure`、`remote-auth-failure`、`remote-incompatible`、`compatibility-unavailable`、`leader-unavailable`、`membership-not-committed`、`membership-change-failed`；未知类别归为 `failed`。原始 NodeId、URL、token、证书密码、fact 和异常文本不进入应用日志、metric 标签或上述 Activity。建议按部署的 resource 属性区分节点，而不是把 NodeId/endpoint 变成 metric 维度。观察注册在 leadership watcher 结束时释放，重启不会留下旧节点 gauge。

告警建议：有 Leader 但 writable=0 / quorum=0 持续出现时先检查多数派网络和 TLS；apply/commit lag 持续增长时检查存储/投影与节点通信；快照或恢复 `failed`、成员操作固定失败类别和恢复时长增加需要运维介入。Follower 无业务进度是正常行为，不应因其不分发而告警。若额外启用框架/DotNext/HTTP 日志或自动 URL tracing，请独立配置过滤和脱敏；应用的隐私保证不自动覆盖第三方 instrumentation。

遥测中的标识符是由 SHA-256 派生的稳定短 ID。事实数据、Bearer 凭据、幂等键以及原始会话/查询标识符都不会作为 tag 输出。Host 日志记录固定错误类别和经过哈希处理的查询标识符，不记录异常消息或载荷内容。

默认 Host 配置不会添加 exporter 或 collector 端点。请在部署组合中配置 OpenTelemetry exporter，将这些指标发送到遥测后端；仅使用示例配置不会导出任何指标。

## 验证

在仓库根目录运行本地自动化测试套件，并执行 Release 发布：

```powershell
dotnet test --solution DrasiWake.sln --configuration Release
dotnet publish src\DrasiWake.Host\DrasiWake.Host.csproj -c Release -o artifacts\publish
```

运行发布产物时，请将 `artifacts/publish` 设为工作目录，以便程序能找到复制过去的配置和 `contracts/` 文件：

```powershell
Push-Location artifacts/publish
.\DrasiWake.Host.exe
Pop-Location
```

确定性集成测试使用真实 adapter 和 SonnetDB provider，并通过本地 HTTP 契约处理器模拟服务端。测试会验证收敛、幂等重放、数据库重开和崩溃窗口，但不能据此认证已部署的 Drasi 或 OpenClaw Gateway 版本。

三个真实 Aspire 检查默认跳过。需要运行时，请配置 Aspire 本地环境所需的模型提供方密钥和 Gateway token，并按 [Aspire 本地环境指南](development/aspire-local-environment.md) 显式打开 smoke、Drasi contract 和 Gateway contract 开关。指南包含运行完整解决方案（含三个真实检查）的命令及单项过滤方式。真实 Drasi contract 会枚举查询、读取 results 并 attach；真实 Gateway contract 会通过 `/apps/chat` 建立唯一测试会话，再并发发送相同 MetaInvocation 请求，检查 invocation ID 幂等，并验证 Gateway 幂等保留时间不短于 outbox 最大重试时长。Gateway contract 会调用真实模型服务，可能产生费用；请只对专用测试环境运行。测试不会打印凭据。

2026-10-03：完整解决方案测试通过（109/109），包括 Aspire smoke、Drasi contract 和 Gateway contract。本次真实服务验证针对本地 Aspire fixture 栈，不等同于目标部署版本和配置的契约认证。在目标环境通过三个真实检查并记录结果之前，请保持 V1-ready 门禁关闭。