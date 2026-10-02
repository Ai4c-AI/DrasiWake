# DrasiWake Bridge Core 设计规格

- 日期：2026-10-02
- 状态：设计提案；V1 核心决策已在 brainstorming 中确认；本次逐章补充稿待审阅
- 范围：Bridge Core 架构与 V1 交付方案

## 1. 背景与目标

Drasi 通过持续查询感知数据变化，OpenClaw.NET 执行 MetaSkill 工作流。Bridge Core 将变更源信号转换为路由正确、顺序明确且可恢复的唤醒请求。

本设计保留共享内核方向，同时区分首个交付版本与长期部署目标。V1 验证独立桥及其边界契约，不要求内嵌 Gateway 集成，也不要求多副本运行。

## 2. V1 范围与部署形态

V1 是单实例独立桥：

- 北向边界：Drasi Server HTTP API。多实例路由使用 `/api/v1/instances/{instanceId}/queries/{queryId}/attach` 接收 SSE 查询结果，使用 `/api/v1/instances/{instanceId}/queries/{queryId}/results` 读取当前快照；默认实例可使用 `/api/v1/queries/{queryId}/...` 便捷路由。不依赖 MCP Reaction。
- 内核：归一化、快照对账、指纹仲裁、基于契约的映射、按会话串行化、载荷渲染与持久化分发。
- 南向边界：OpenClaw.NET integration HTTP adapter。OpenClaw.NET 现已提供专用的 `POST /api/integration/meta-invocations` endpoint，支持请求中显式指定 MetaSkill，并在 Gateway 端持久化幂等记录。Bridge 必须使用该 endpoint；仍需通过集成验证确认鉴权、请求映射、幂等重放，以及保留期覆盖 outbox 最长重试窗口。
- 运行态：一个本地 SonnetDB 数据库目录，通过 `SonnetDB.EntityFrameworkCore` 和 EF Core 迁移访问。

内嵌 Gateway 形态作为后续部署目标。Core 应依赖边界抽象，以便未来宿主提供其他运行态适配器；V1 不实现该适配器。

V1 不要求多副本高可用（HA）或 NativeAOT 发布。NativeAOT 是未来内嵌形态的兼容性目标；SonnetDB EF Core provider 未声明 NativeAOT 兼容性。未来 AOT 路径必须单独验证，也可能采用其他持久化适配器。

## 3. 交付语义

V1 默认采用**最终状态收敛**。变更源通知只是读取当前查询快照的提示，不是需要持久逐条处理的业务事件。重复、乱序或遗漏的通知通过快照指纹与对账处理。中间状态可以合并，不保证每个状态都产生独立唤醒。

只有当变更源提供稳定事件标识、可恢复游标，以及足以在停机后恢复的重放与保留能力时，绑定才能要求**逐次保留变化**。V1 Drasi Server `attach` SSE 接口未提供可恢复游标或停机期间重放保证，因此适配器不承诺具备此能力；必须拒绝要求逐次保留变化的绑定。

相同会话身份的唤醒按序处理。尚未提交的待处理唤醒可以被更新快照吸收；已经提交的唤醒不撤销，新快照排在其后。

可靠性承诺有明确边界：Bridge Core 对当前快照进行对账，并重试已持久化的唤醒项。快照模式下不保证观察到每个中间状态，也不承诺在任意操作系统、存储设备或介质故障下零数据丢失。

## 4. 契约、路由与会话身份

每个绑定契约声明：

- 查询与变更源身份。
- 交付模式：默认最终状态收敛；只有变更源契约允许时才能逐次保留变化。
- 会话作用域：默认按查询隔离；使用相同领域本体的绑定可显式声明按规范实体身份共享会话。
- 规范键定义、事实 Schema、载荷限制、策略限制及契约版本。
- 预期 MetaSkill 路由与触发短语元数据。

默认映射键为 `(queryId, aggregateKey)`。实体作用域共享必须使用显式规范身份，其中同时包含本体/上下文、实体类型和实体 ID。原始键值相同并不意味着可以共享记忆。缺少键或无法归一化的键应拒绝该绑定或事件，不得静默退化为单例会话。

OpenClaw.NET 已提供可显式指定 MetaSkill 的专用 invocation endpoint。V1 Bridge 必须使用该 endpoint，并验证目标技能名映射和端到端路由结果；不得将 trigger phrase 当作确定性地址或回退路由。Bridge 集成通过验证前，不得宣称端到端路由契约已通过。

契约注册表以 Git 为事实源。启动时加载并校验完整注册表；文件/GitOps 更新生成候选注册表，对整体校验通过后再原子切换活动版本。候选版本无效时继续使用上一有效版本。注册表是 Bridge 绑定和 MetaSkill 路由元数据的事实源；审批流程不属于桥的职责。

## 5. 运行时流水线与恢复

七个概念阶段保持不变：接收、归一化、读取快照、指纹仲裁、映射/串行化、渲染、注入/确认。

V1 实现遵循以下规则：

1. Drasi Server north adapter 使用查询 `attach` SSE 接收 JSON 查询结果变化，并使用查询 `results` API 获取权威当前结果；组件生命周期 `events/stream` 不作为查询结果变更流。当前 drasi-server 实现会为每条 attach 连接创建临时 `ApplicationReaction` 并在连接关闭时清理；适配器按 `(server, instance, query)` 复用单条连接并退避重连，避免按绑定重复连接。这不依赖 MCP Reaction 或新增 Drasi 插件。
2. 有界内存 Channel 只用于 SSE 提示的接纳/分发优化，不是持久化存储。提示无法入队时，将对应查询标记为需要对账；SSE 接收不得因快照 I/O 阻塞，桥也不得声称已可靠持久接收每条变化。
3. 启动、SSE 连接或重连、Drasi Server 恢复及周期计时器都会触发查询枚举和快照对账。重连后必须读取当前结果快照，不依赖 attach 流补发断线期间的变化。
4. 快照读取失败时不得推进已接纳指纹。失败应重试，并按变更源隔离。
5. 快照变化时，先创建或更新持久化 `WakeOutbox` 项，再进行 HTTP 注入。对相同待处理指纹的重复观察应去重；尚未提交的待处理工作可以合并。
6. 注入使用稳定的幂等键。未确认项目在重启后使用相同幂等键重试。
7. Gateway 确认受理后，通过一个 EF Core 事务同时更新 outbox 状态与已接纳快照指纹。执行回执用于更新执行状态，不取代受理确认。
8. Gateway 已受理但本地事务提交失败属于结果不确定。OpenClaw.NET 的专用 MetaSkill invocation endpoint 已提供持久化幂等去重；Bridge 必须使用同一幂等键重试，并确认 Gateway 幂等记录的保留期不短于 outbox 最长重试窗口。

Drasi Server north adapter 负责维护 attach SSE 连接并报告服务/查询可见性。SSE 断线或桥重启后的最终状态收敛由 `results` 快照对账保证；attach 不提供逐条事件的持久游标或断线重放保证。

## 6. 运行态与持久化

EF Core 模型包含四类逻辑状态：

- `SubscriptionState`：attach SSE 健康状态、最近一次成功对账时间及 Drasi Server/查询可见性状态。
- `SnapshotCheckpoint`：已接纳指纹及对应的绑定/会话身份。
- `KeyMapping`：契约作用域、规范键、会话引用及 active/cooling/tombstone 生命周期状态。
- `WakeOutbox`：渲染后的载荷、契约版本、幂等键、重试计划、trace 上下文及受理/执行状态。

SonnetDB EF Core provider 基于 SonnetDB ADO.NET package，支持 EF Core CRUD 与迁移。SonnetDB 的 ADO.NET 文档说明支持开始、提交和回滚事务。V1 使用 EF Core 持久化；集成契约测试必须验证 EF Core 事务 API 能否原子覆盖 outbox/checkpoint 更新，并确认回滚及重新打开后的行为正确。

数据库目录同一时间只允许一个活动桥实例拥有。V1 不协调多个桥进程。已受理/已完成 outbox 行保留可配置的有限期限。死信行保留至操作人员明确处理或归档。Gateway 幂等记录的保留期不得短于桥重试 outbox 项的最长周期。运行审计/遥测不能替代事件存储。

## 7. 故障处理

- Drasi Server 不可达、attach SSE 断连或快照读取失败：将服务/查询标记为降级，退避重试，并在恢复后读取当前快照对账；读取失败时不推进已接纳指纹。
- 内存提示 Channel 已满：将受影响的查询标记为需要对账，并发出告警/指标。
- Gateway 不可用或受理结果不确定：保留 outbox 项，并使用原幂等键重试。
- 契约校验失败：继续使用先前活动契约，并报告候选版本失败原因。
- 规范键或载荷契约无效：以可审计原因隔离/拒绝相关绑定或项目；不得静默路由到其他会话。
- 重试耗尽：将项目保留在终态/死信状态，等待操作人员处理；不得自动删除，也不得报告为成功唤醒。

## 8. 可观测性与 V1 验证

V1 不设置硬性吞吐量或延迟验收门槛，而是记录基线数据供后续容量规划。

运行信号包括端到端 SSE 提示至受理延迟、队列深度、对账差异、重试与死信数量、Drasi Server/查询可见性、活动契约版本及路由结果。Trace 上下文贯穿从 SSE 提示到 Gateway 受理及执行状态的完整链路。事实载荷和凭据不得写入指标标签或未脱敏日志。

必须完成以下验证：

- Drasi Server attach SSE 提示重复、乱序或丢失后，系统最终收敛到 `results` API 返回的当前快照。
- SSE 断线重连、桥重启或提示因队列已满未入队后，系统通过重新读取当前快照恢复最终状态；不验证或宣称断线期间每个中间变化均可重放。
- north adapter 使用查询 `attach` SSE 和查询 `results` 快照 API；不得将查询生命周期 `events/stream` 误用为结果变化流。
- 快照读取失败不会改变已接纳 checkpoint。
- EF Core 回滚能够保持 outbox/checkpoint 原子性。
- 在持久化、Gateway 受理和执行回执边界注入崩溃并重开后，系统使用原幂等键恢复。
- 同一会话的唤醒严格串行；不同会话可以并行推进。
- 无效契约激活不会替换上一有效版本。
- 通过集成契约测试确认 Bridge 使用专用 endpoint 显式指定 MetaSkill，并验证幂等重放、并发重复抑制及保留期覆盖 outbox 最长重试窗口。Gateway 能力已提供；Bridge 集成通过验证前，不得宣称端到端契约已通过。
- 在支持的普通 .NET 发布路径中，验证 SonnetDB EF Core 迁移以及嵌入式单实例打开/重新打开行为。

## 9. V1 非目标

- 内嵌 Gateway 部署和进程内运行时调用。
- 多副本协调或高可用。
- 保证处理每个中间快照变化。
- 事件溯源或长期历史存储。
- 试点测量前设定硬性性能 SLO。
- 保证 EF Core 持久化路径支持 NativeAOT。

## 附录 A. 原设计文档逐章承接与处置

本附录逐章对应 `docs/DrasiWake_Bridge_Core_设计文档.md`。主规格第 1–9 节是 V1 的规范性决策；本附录补齐原文中仍兼容的设计细节，并明确哪些内容已被新决策替换或推迟。原文中的技术探索记录仅作为背景，不自动构成 V1 承诺。

| 原文章节 | 处置 | 本规格位置与说明 |
| --- | --- | --- |
| 1. 背景与设计目标 | 保留并修订 | 第 1 节；保留 Ambient Agent 目标，收窄“绝不丢失”承诺。 |
| 2. 总体定位与两种部署形态 | 更新；一形态延期 | 第 2 节；V1 独立桥接 Drasi Server；内嵌 drasi-dotnet 形态延期；K8s MCP 形态不适用。 |
| 3. Bridge Core 职责边界 | 保留 | 附录 A.1；明确 Core 与两侧 adapter 的职责。 |
| 4. 七阶段流水线规格 | 保留并更新 | 第 5 节、附录 A.2；北向改为 SSE 提示加查询快照。 |
| 5. 核心数据模型 | 保留并适配 | 第 6 节、附录 A.3；沿用逻辑模型，名称及状态按 V1 outbox/checkpoint 语义调整。 |
| 6. 状态机设计 | 保留并适配 | 附录 A.3；订阅状态改为 Drasi Server SSE/查询可见性状态。 |
| 7. 并发模型 | 保留并细化 | 附录 A.4；同身份串行、不同身份并行，明确有界队列不等于持久化。 |
| 8. 边界接口语义规格 | 北向替换，南向 endpoint 已支持 | 第 2、5、8 节及附录 A.5；MCP 换为 Drasi Server HTTP API；Bridge 仍须验证端到端集成契约。 |
| 9. 契约注册表与生效模型 | 保留；双写延期 | 第 4 节、附录 A.6；Git 事实源及原子激活保留，major 双写不列入 V1。 |
| 10. 唤醒契约与 MetaSkill triggers | 保留并明确使用显式路由 API | 第 4 节、附录 A.6；OpenClaw.NET 已支持显式 MetaSkill 定向，Bridge 必须通过集成测试证明正确使用。 |
| 11. Query 键空间到 Agent 会话空间映射 | 保留 V1 子集；其余延期 | 第 4 节、附录 A.7；默认按查询隔离，跨查询共享必须显式声明规范身份。 |
| 12. 失败语义矩阵 | 保留并修订 | 第 7 节、附录 A.8；快照最终收敛不等于每个中间变化绝不丢失。 |
| 13. 可观测性规格 | 保留并聚焦基线 | 第 8 节、附录 A.9；V1 采集指标，不预设硬性 SLO。 |
| 14. 可测试性设计 | 保留 | 第 8 节、附录 A.10；确定性测试、虚拟时钟和契约测试纳入验证。 |
| 15. .NET 10 组件选型 | 更新 | 附录 A.11；SonnetDB 替换 SQLite；NativeAOT 和内嵌方案延期。 |
| 16. 流水线编排选型论证 | 保留为实现基线 | 附录 A.12；优先采用 Channels 与纯异步变换段，避免引入重型 broker/actor。 |
| 17. 风险登记册 | 更新 | 附录 A.13；移除已不适用的 MCP 风险，加入 SSE 缺少断线重放、SonnetDB 事务及 Gateway 幂等风险。 |
| 18. 术语表 | 保留并更新 | 附录 A.14；MCP Reaction 等术语标为被替换的历史方案。 |

### A.1 Bridge Core 职责边界

Core 的唯一使命是把变更源信号转换成语义、顺序和路由均受契约约束的唤醒请求。Core 内部应以确定性逻辑为主：给定相同的已归一输入、契约版本和持久化状态，应得到相同的裁决与唤醒载荷。时钟、ID 生成、网络、存储和执行状态等非确定性依赖通过边界抽象注入。

| Core 负责 | Core 不负责 |
| --- | --- |
| 信号归一、去抖、快照指纹和最终状态对账 | Drasi Server 内部如何采集数据或运行查询 |
| 查询键到会话身份的映射及映射生命周期 | 会话的创建、LLM/工具执行和业务处置逻辑 |
| 契约解析、校验、版本仲裁及唤醒载荷生成 | 契约审批和组织治理流程 |
| 同身份串行、背压、持久化分发和失败重试 | SSE/HTTP 的具体传输实现细节 |
| IntegrationBackend 受理与执行状态的关联 | Gateway 内部的 MetaSkill DAG 实现 |

North adapter 不得向 Core 暴露 Drasi Server URL 路由、SSE frame 或 HTTP DTO；south adapter 不得把 Gateway 的传输错误混同为业务执行失败。

### A.2 七阶段流水线细化

| 阶段 | 输入 → 输出 | V1 规则与失败语义 |
| --- | --- | --- |
| 1. 接收 | Drasi attach SSE frame 或对账触发 → `ChangeSignal` | 只登记查询身份并入有界队列，不在接收循环执行快照 I/O。队列满时标记查询待对账并告警，不声称已持久接收该提示。 |
| 2. 归一 | `ChangeSignal` → 标准变更提示 | 隐藏 Drasi DTO/SSE 差异，附关联 ID 和单调的桥内处理序号。无法解析的提示隔离并记录；随后仍可由快照对账恢复最终状态。 |
| 3. 快照获取 | 查询身份 → 当前结果集 | 调用查询 `results` API；失败时退避重试，不推进已接纳指纹，并按查询隔离故障。快照是 V1 状态裁决的依据。 |
| 4. 指纹仲裁 | 当前快照 → 转发、丢弃或合并裁决 | 对规范化快照计算稳定指纹；相同已接纳指纹不重复唤醒。去抖窗和待处理 outbox 合并不得撤销已确认唤醒。 |
| 5. 映射与串行化 | 裁决 → 会话身份及队列项 | 按绑定契约解析规范键，进入同身份串行队列。映射存储失败时暂停相关身份，不静默退化成共享单例。 |
| 6. 载荷渲染 | 快照事实、契约版本 → 唤醒载荷 | 按路由、事实、语义和映射契约渲染并校验 Schema/大小限制。契约错误时隔离该绑定或项目，不路由到未声明的技能。 |
| 7. 注入与确认 | WakeOutbox 项 → Gateway 受理/执行状态 | 使用稳定幂等键调用 IntegrationBackend；受理确认与执行回执分别记录。结果不确定时保留 outbox 并以同一幂等键重试。 |

阶段间使用不可变值对象有利于单测和故障复现；这不代表生产运行会长期保存所有原始事件或支持对 Drasi 中间状态做事件溯源。

### A.3 核心数据模型与状态机

静态配置至少包含：

| 实体 | 关键字段 |
| --- | --- |
| `Binding` | Drasi Server/instance/query 身份、交付模式、会话策略、路由目标、去抖/合并策略、速率策略及优先级 |
| `Contract` | routing、fact、semantic、mapping 四层声明及显式版本 |
| `ContractRegistry` | 契约集合、当前生效版本及候选版本校验结果；Git 为事实源 |

运行态逻辑模型如下：

| 实体 | 内容与持久化语义 |
| --- | --- |
| `SubscriptionState` | attach SSE 连接健康、Drasi Server/查询可见性及最近成功对账时间 |
| `SnapshotCheckpoint` | 按绑定和会话身份记录已接纳快照指纹；只在对应唤醒满足受理确认语义后推进 |
| `KeyMapping` | 契约作用域、规范键、Gateway 会话引用及 active/cooling/tombstone 状态 |
| `WakeOutbox` | 已渲染载荷、契约版本、幂等键、重试计划、trace 上下文及受理/执行状态 |
| `SerialQueue` | 按会话身份派生的排队视图，可由持久化 outbox 重建，不单独作为事实存储 |

WakeOutbox 生命周期为 `Pending → Dispatching → Accepted → Executing → Completed`，临时失败回到 `RetryScheduled`，达到策略上限后进入 `DeadLetter`。新快照可吸收尚未提交的同身份待处理工作，标记为 `Superseded`；不得改写已接受或已执行项目的历史状态。受理与 checkpoint 更新在同一 EF Core 事务内提交；执行回执只更新执行状态，不替代受理确认。

北向连接状态为 `Disconnected → Connecting → Active → Degraded → Reconnecting`；恢复时先建立 attach 流，再获取当前快照并对账。键映射生命周期沿用 `Active → Cooling → Tombstone/Archived` 的概念；自动归档、跨会话迁移及记忆沉淀需有 Gateway 能力支撑，不构成 V1 的默认动作。状态迁移应产出结构化审计记录，但不因此要求构建长期事件存储。

### A.4 并发、背压与速率整形

- 并行分区单位是最终会话身份。不同身份可以并行，同一身份严格串行，防止旧快照与新快照并发注入造成语义倒序。
- 接收队列、按身份串行队列和注入/速率队列是三个逻辑阶段；不要求为每个键永久创建操作系统线程或无限容量 Channel。
- 内存 Channel 必须有界。满队列只触发待对账标记、指标和恢复快照读取，不能把内存队列描述成持久化保证。
- 去抖/合并处理通知噪声；串行队列维护已持久化唤醒的顺序，两者语义不同。已接受工作不可被后来的快照吸收。
- 绑定可声明速率上限和优先级；限流不能静默丢弃快照差异，超限工作须留在持久化 outbox 或等待后续快照合并。具体默认容量及试点阈值通过基线测量确定。
- 全内核使用单一时钟抽象，去抖、TTL 和退避统一依赖 `TimeProvider`，以支持虚拟时间测试。

### A.5 边界接口语义

#### 北向：`IChangeSource`

| 能力 | V1 Drasi Server 适配语义 |
| --- | --- |
| 变化提示 | 多实例使用 `GET /api/v1/instances/{instanceId}/queries/{queryId}/attach`；默认实例可用 `/api/v1/queries/{queryId}/attach`。响应是 SSE，frame data 携带序列化的查询结果变化。 |
| 当前快照 | 对应 `GET /api/v1/instances/{instanceId}/queries/{queryId}/results` 或默认实例 `/api/v1/queries/{queryId}/results`。 |
| 枚举与可见性 | 使用 instances/queries API 枚举配置并确认服务、实例和查询状态；配置 snapshot 是组件配置快照，不可误当作查询结果快照。 |
| 生命周期 | SSE 断开、HTTP 错误和查询不可见均上报 adapter 状态；按退避策略重连。连接成功后重新读取结果快照。 |
| 重放保证 | 当前 attach 路由无可恢复游标参数或断线事件重放契约；不宣称至少一次逐事件交付，不支持要求保留每个中间变化的绑定。 |

当前 Drasi Server 实现为每条 attach HTTP 连接创建临时 `ApplicationReaction`，流关闭时清理。adapter 按 `(server, instance, query)` 复用单条连接、限制重连风暴，并按查询对账。查询 `events/stream` 是组件生命周期事件，不是结果变化流。

#### 南向：`IWakeSink`

- 已核实 `POST /api/integration/backends/{id}/sessions/{sessionId}/input` 是 coding backend 的 stdin 输入，不是 Agent/MetaSkill 唤醒 API。
- 已核实 `POST /api/integration/messages` 将普通文本入队为 `InboundMessage`，可指定 channel、sender、session；可选 `messageId` 被传递下去，但 endpoint 没有 Gateway 端持久去重。HTTP 202 只表示消息已受理入队。
- 已核实 `POST /api/integration/workflows/{workflowId}/runs` 中的 `workflowId` 选择配置的 workflow backend；当前 backend kind 为 `maf-durable-http`，并将请求转发给配置的外部 `WorkflowName`。请求包含 input/payload 和可选 session/channel/sender/metadata，没有显式 MetaSkill 目标或专用幂等键；每次调用都会向外部 workflow 发起 POST。GET run 路由可查询外部返回的 run snapshot，但 Gateway 代码没有建立请求去重契约。外部 workflow 可能自行调用 MetaSkill 或实现去重，这两点尚未验证。
- OpenClaw.NET 现已提供专用 `POST /api/integration/meta-invocations` endpoint，显式接收 `skill`、`input` 和 `sessionId`，并使用 `Idempotency-Key` 在 Gateway 端持久化请求指纹及调用状态。相同调用方、幂等键和请求会返回原调用记录/终态结果；同一键对应不同请求返回 HTTP 409；相同请求并发时不会启动第二次 DAG 执行；重启时遗留的运行中调用标记为不确定状态，不会自动重跑。幂等记录保留期可配置。
- Bridge 必须使用该专用 endpoint，而不是用 `/messages` 或 `/workflows` 代替显式 MetaSkill 定向。集成测试仍需验证技能名和输入映射正确、重试沿用原幂等键、持久化去重行为正确，且保留期覆盖桥的最长重试窗口。任何 202 响应都不能单独证明 MetaSkill 已完成。
- 在 Bridge 集成通过上述验证前，不得宣称端到端路由和重复安全重试契约已通过，也不得将该集成标记为 V1 ready。

### A.6 契约注册表与 MetaSkill 路由

契约采用四层结构：

| 层 | 声明 | 主要风险 |
| --- | --- | --- |
| routing | 目标 MetaSkill、允许的 trigger phrases 和路由方式 | 自然语言匹配漂移、错误路由或落空 |
| fact | 事实载荷 Schema、字段类型、基数、枚举、最大尺寸 | DAG 参数提取或首步执行失败 |
| semantic | JSON-LD `@context`、限界上下文、实体类型及规范键本体 | 跨查询身份不一致、历史记忆指向失效实体 |
| mapping | 键字段、粒度、会话作用域、TTL 和生命周期策略 | 会话记忆串扰或知识碎片化 |

Git 是注册表事实源。启动时全量校验；更新形成候选版本，完成 Schema、目标路由、trigger 冲突及语义引用校验后原子切换。候选失败继续使用上一有效版本，禁止半生效。运行时可对照注册表、Drasi 实际查询和 KeyMapping 检测漂移。

事实层增加可选字段可按 minor 版本演进；删除/改义字段、枚举变化、路由目标或关键语义本体变化按破坏式版本处理，并提供迁移策略。原文提出的 major 双写期不纳入 V1；需要时另行设计迁移及重复注入语义。

Gateway 的 trigger phrase matching 是自然语言匹配，不是精确寻址。OpenClaw.NET 现已支持显式 MetaSkill 定向，Bridge 必须调用专用 invocation endpoint 并传入目标技能名，不得退回仅靠 trigger phrase 的路由方式。集成测试应验证技能名映射及实际路由结果；trigger 短语冲突检查和路由 Canary 可继续作为契约校验的一部分，但不能替代显式路由 API。

### A.7 Query 键空间与会话映射

会话是记忆边界：映射太粗会导致无关上下文串扰，太细会分散同一领域实体的处置历史。V1 支持范围如下：

| 策略 | V1 处置 |
| --- | --- |
| `perQuery` | 默认；映射键为 `(queryId, aggregateKey)`，查询间隔离。 |
| `perKey` | 可显式启用；跨查询共享必须使用包含 ontology/context、entity type、entity ID 的 canonical identity。 |
| `singleton` | 不作为缺键时的静默回退；如有系统级场景须由独立契约显式声明。 |
| `perKeyHierarchy`、`timeBucketed`、`new` | 非 V1 默认能力，延期评估。 |

聚合键可以运行时扩张；首次见到合法键时创建并持久化 KeyMapping，再按 IntegrationBackend 的会话能力解析/创建 session。缺键或无法规范化时拒绝该绑定/项目，不得随机创建新会话或落到全局单例。

冷键 TTL 回收、键删除 tombstone 和会话存档可作为生命周期策略，但 V1 不假设 Fractal Memory 沉淀接口存在。键迁移（聚合拆分/合并、摘要导入及映射原子切换）、跨查询聚合收件箱和自动关闭 Gateway 会话均延期到相应南向能力验证后。

### A.8 故障语义矩阵

| 故障 | V1 行为 | 正确性依据/限制 |
| --- | --- | --- |
| Drasi Server 或 attach 断连 | 标记相关实例/查询降级，退避重连；恢复时读取当前结果快照 | 最终状态对账；不补发断线期间所有中间变化 |
| 查询快照读取持续失败 | 不推进 checkpoint，按查询隔离、重试并告警 | 只有成功读取的快照可参与指纹裁决 |
| attach 提示重复、乱序或丢失 | 重复由指纹去重；丢失由启动/重连/周期对账补当前状态 | 中间快照不保证逐次唤醒 |
| 接收 Channel 满 | 标记待对账并告警，继续接收能力按 adapter 实际背压处理 | Channel 不是 durable queue，不宣称事件已持久接收 |
| OpenClaw.NET 不可达 | WakeOutbox 保持可重试状态，使用原幂等键退避重试 | 使用专用 endpoint 的持久化幂等能力；确认记录保留期覆盖最长重试窗口 |
| Gateway 已受理但本地提交失败 | 记录结果不确定，重试原幂等键 | 只有 Gateway 持久去重才能避免二次受理 |
| 契约候选无效 | 保持旧版本活动，记录校验失败 | 禁止部分切换 |
| 键映射/存储故障 | 暂停受影响身份，其他分区可继续 | 不得静默改用另一个会话身份 |
| 重试耗尽 | 保留为 DeadLetter，等待人工处理 | 不自动删除，不报告为成功 |

原文“宁可延迟、不可错误、绝不丢失”修订为可验证边界：V1 对当前可读取快照最终收敛、对已持久化 outbox 尽力按幂等键重试；不保证每个瞬时中间状态、任意介质故障下零丢失或 Gateway 未提供的重放能力。

### A.9 可观测性

V1 先建立容量与时延基线，不设置未测量的硬性 SLO。应提供：

- 黄金信号：SSE 提示至 Gateway 受理延迟、队列深度/年龄、快照对账差异、Drasi 服务/查询可见性、同会话唤醒次数及会话复用率。
- 阶段指标：七阶段的输入/输出/失败计数，快照读取失败和耗时，重复指纹丢弃数，去抖/合并数，限流等待，重试与死信数。
- 审计记录：契约版本激活、键映射创建/状态迁移、快照裁决、outbox 状态变化及其原因。审计保留策略需有限且可配置，不作为业务事件历史库。
- Trace：从 Drasi SSE hint 或对账触发，贯穿快照、映射、Gateway 受理及执行回执；允许 SSE 断线后新 trace 与原查询/绑定关联。
- 数据保护：事实载荷、凭据和敏感键不得写入指标标签或未脱敏日志；用稳定的非敏感 ID 关联日志和 trace。

### A.10 可测试性与验证

1. 纯函数测试覆盖归一、指纹、契约映射、载荷渲染和相同输入/状态下的确定性裁决。
2. 使用可控 `TimeProvider` 测试去抖、TTL、退避和速率窗口，无需等待真实时间。
3. 用标准化提示及快照夹具重放正常、重复、乱序、断连和队列满序列；断言相同输入状态产生相同最终唤醒，不断言每个源端中间变化都有唤醒。
4. Drasi Server adapter 集成测试覆盖 attach SSE JSON frame 解析、results 快照解析、连接取消/清理、重连触发对账，以及生命周期 events 不被当作 query result。
5. EF Core/SonnetDB 契约测试覆盖事务原子性、回滚、进程关闭后重新打开及 outbox/checkpoint 一致性。
6. Gateway 契约测试验证 Bridge 对专用 endpoint 的显式路由、受理与执行状态分离、幂等键去重及保留期配置；任一项未验证不得通过相应 V1 ready gate。
7. 故障注入覆盖持久化前后、Gateway 受理前后、执行回执前后崩溃；验证重启后的原幂等键恢复、同身份串行和不同身份并发。
8. 契约配置测试覆盖整批校验、trigger 冲突、无效候选不切换以及 Schema/路由 Canary。

### A.11 .NET 10 组件选型与探索结论

下表是 V1 实现基线/候选，具体 package 版本在实施计划中锁定；NativeAOT 结果不作为 V1 发布门槛。

| 层 | V1 选择或基线 | 处置说明 |
| --- | --- | --- |
| 宿主与后台服务 | .NET Generic Host、`BackgroundService` | 适用于单实例独立桥。 |
| 通道与分区 | `System.Threading.Channels`、自建按会话身份分区器 | 有界容量、背压显式、避免引入重型 actor。 |
| 时间与周期任务 | `TimeProvider`、`PeriodicTimer` | 便于虚拟时钟测试，无需集群调度器。 |
| 弹性与速率 | `Microsoft.Extensions.Resilience`/Polly、`System.Threading.RateLimiting` | 退避须受可控时钟和幂等策略约束。 |
| JSON | `System.Text.Json`；动态事实使用 `JsonNode` | 若目标发布形态需要 AOT，再要求 source generation。 |
| Schema | JsonSchema.Net | 原文记录的 9.4.0 AOT spike 属未来兼容性证据，不代表 V1 必须 AOT。 |
| YAML | YamlDotNet 静态生成路径 | 原文记录的反射路径 AOT 问题只影响未来 AOT 目标；V1 普通 .NET 发布不因此设阻断项。 |
| 北向 | Drasi Server HTTP/SSE adapter | 替换 MCP C# SDK；不加载 K8s MCP Reaction。 |
| 南向 | OpenClaw.NET integration HTTP adapter | 使用已支持显式 MetaSkill 定向和 Gateway 持久幂等的 `POST /api/integration/meta-invocations`；`/messages`、`/workflows` 与 coding backend stdin 路由不满足该调用契约。 |
| 持久化 | `SonnetDB.EntityFrameworkCore` + EF Core migrations | 替换 SQLite；事务行为需按第 6、8 节验证。 |
| 可观测性 | OpenTelemetry、`ILogger`、`ActivitySource` | 和执行状态/trace 关联，不记录敏感载荷。 |

以下原文探索结论的处置：

- MCP C# SDK 的 SSE 重连、Last-Event-ID、session resume 和订阅重放结论：**被替换**。V1 改用 Drasi Server attach API，按当前 API 不支持游标/断线重放设计。
- drasi-dotnet callback 的 Tokio worker、sync-over-async、FFI 和 checkpoint 背压分析：**延期**至内嵌 drasi-dotnet 形态；原分析指出回调在 Tokio worker 上同步等待 .NET Task，未来 adapter 回调必须只做微秒级非阻塞入队，不能在回调中等待 IO/LLM。此约束不适用于 V1 HTTP adapter。
- JsonSchema.Net 9.4.0 与 YamlDotNet 18.1.0 的 NativeAOT spike：**保留为未来 AOT 参考**，不是 V1 验收门槛。原测试中 JsonSchema.Net 的 FromText/Evaluate 可运行且无 AOT 警告，但 v9 Evaluate 使用 `JsonElement`；YamlDotNet 反射反序列化路径有 IL3050/运行时失败，静态路径需要 analyzer source generator，原文记录采用 Vecc 发布的 generator，并受类型转换器、`OrderedDictionary` 和 StaticContext 命名空间等限制。未来启用 AOT 前需按实际锁定版本重测。
- SQLite + EF Core 编译模型：**被 SonnetDB EF Core provider 替换**；不能直接沿用 SQLite 行为假设。

### A.12 流水线编排选择

保留原文的推荐方向：Channels 承载有界接收、按身份串行和持久化 outbox 工作循环；归一、快照结果标准化及指纹比较等纯变换段可用 `IAsyncEnumerable` 组合，或保持显式方法以便独立测试。映射、持久化和注入属于有状态/副作用边界，不应藏在无法观察的长 LINQ 链中。

不在 V1 引入 TPL Dataflow、Akka.NET/Orleans、MassTransit/broker 或 DurableTask：当前单实例最终状态收敛模型不需要额外的 actor 集群、broker 运维或 saga 执行时。上游关闭、取消及宿主优雅停机时必须显式传播取消/完成信号，并在退出前按策略排空或持久化待处理工作。

### A.13 更新后的风险登记册

| 风险 | 等级 | 缓解或门槛 |
| --- | --- | --- |
| Bridge 未正确使用显式 MetaSkill endpoint | 中 | OpenClaw.NET 已提供专用 invocation API；通过集成测试确认 Bridge 传入准确的技能名并路由到对应 MetaSkill，禁止回退到 `/messages` 或 `/workflows`。 |
| attach SSE 断线窗口内变化不可重放 | 高 | 明确只承诺最终状态收敛；连接恢复后快照对账；逐次保留变化不进入 V1。 |
| Gateway 幂等保留期短于桥的最长重试窗口 | 中 | 专用 MetaSkill endpoint 已支持持久化幂等；配置并验证保留期不短于 WakeOutbox 最长重试周期，且通过重启后的重复提交测试。 |
| SonnetDB EF Core 事务未能原子提交 outbox/checkpoint | 高 | 集成契约测试验证提交、回滚、关闭重开；失败时不宣称崩溃恢复成立。 |
| 规范身份设计不当导致会话串扰/碎片化 | 中 | 默认按 query 隔离；共享需完整 canonical identity 和契约测试。 |
| `@context` 或领域本体演进导致历史会话记忆引用失效 | 中 | 语义层变更按破坏式契约变化处理；共享身份前要求迁移方案和兼容性测试。 |
| 动态聚合键导致 KeyMapping 膨胀 | 中 | 按契约配置 TTL/容量指标；归档/删除策略不得绕过 tombstone 与审计，Fractal Memory 自动沉淀延期。 |
| 查询结果或载荷膨胀 | 中 | 契约设置载荷上限、指标记录大小；超限隔离而非截断事实。 |
| 变更风暴使南向过载 | 中 | 有界队列、同身份合并、速率整形及队列年龄告警；不得静默丢弃持久工作。 |
| attach 按绑定重复建立导致临时 Reaction 膨胀 | 中 | 连接按 `(server, instance, query)` 复用；限制并发连接并退避重连。 |
| 旧的 drasi-dotnet / K8s MCP 方案与 V1 混淆 | 低 | 在部署章节及术语表标记延期/替换；V1 不实现这些 adapter。 |
| 未来内嵌形态与 Gateway 共用故障域 | 中（延期风险） | 内嵌部署不属于 V1；进入该阶段前需设计宿主优雅停机顺序并复核 drasi-dotnet 成熟度。 |

### A.14 术语表

| 术语 | 本规格含义 |
| --- | --- |
| Ambient Agent | 平时静默、由数据状态变化提示唤醒的 Agent 形态。 |
| Drasi Server | 本项目 V1 的感知层服务，通过 REST、SSE 和持续查询结果 API 接入。 |
| `attach` SSE | Drasi Server 按查询建立的实时结果流；不是持久游标或历史事件日志。 |
| `results` snapshot | 某查询当前结果集，是 V1 最终状态裁决依据。 |
| MetaSkill | OpenClaw.NET 中声明 trigger 和 DAG 的技能工作流。 |
| WakeOutbox | 持久化待注入、重试、受理及执行状态的桥侧工作项。 |
| SnapshotCheckpoint | 已受理快照指纹及其绑定/会话身份，用于重启后对账。 |
| MCP Reaction | 原文中 K8s Drasi 专属的候选接入方式；V1 不使用。 |
| Bridge Core | 形态无关的确定性唤醒内核；V1 通过 Drasi Server 与 IntegrationBackend adapters 运行。 |
| `IChangeSource` / `IWakeSink` | Core 的北向变更源和南向唤醒汇边界抽象。 |
