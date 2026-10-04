# OpenClaw 多目标路由设计

## 状态

设计方案已获认可，规格说明待用户审阅；尚未开始实现。

## 问题

当前 DrasiWake Host 全局配置一个 OpenClaw endpoint 和一个可选 bearer token。每个 `BridgeBinding` 可以选择 `MetaSkill`，但不能选择接收调用的 OpenClaw Gateway。因此，单个 Host 无法把不同 binding 路由到不同 OpenClaw 实例，binding 也没有完整表达调用目标。

## 目标

- 允许单个 Host 将不同 binding 路由到不同 OpenClaw Gateway。
- 每个 binding 必须显式指定逻辑 target；不提供隐式 `default` target 或回退路由。
- Gateway 地址和凭据保存在 Host 配置中，不放入契约 YAML。
- 将选定的逻辑 target 持久化到 outbox，避免 binding 修改后静默改变已排队任务的路由。
- 派发前验证 target 配置和 Gateway 幂等键保留期。

## 非目标

- 由 DrasiWake 配置或创建 OpenClaw Gateway。
- 在 binding 中加入 endpoint URL、凭据或模型提供方配置。
- 除选择 Gateway 外，改变 MetaSkill 调用负载、重试调度、速率限制或投递语义。

## 设计

### Binding 契约

在 `BridgeBinding` 中增加必填的 `OpenClawTarget` 属性，在 registry YAML 中序列化为 `openClawTarget`。每个 binding 必须命名一个已配置的逻辑 target。现有 `MetaSkill` 仍表示 skill 标识，与 Gateway 选择相互独立。

示例：

```yaml
id: aspire-sensor-readings
source: drasi-server
server: http://127.0.0.1:8080
instanceId: drasiwake-sensor-monitor
queryId: sensor-readings
openClawTarget: sensor-gateway
metaSkill: drasiwake-sensor-reading-summary
```

Registry loader 拒绝缺失或全为空白的 target。Host 启动校验器拒绝 target 不在配置映射中的 binding。现有 sample、Aspire 和测试 binding 文件都必须补上显式 target，不设置隐式回退。

### Host target 配置

在 `DrasiWake:OpenClaw:Targets` 下配置具名 target 映射。每个条目包含：

- `BaseAddress`：绝对 HTTP 或 HTTPS Gateway URL。
- `BearerToken`：Gateway 专属凭据；当 Gateway 要求 bearer 认证时配置，并通过环境变量、User Secrets 或等效 secret provider 提供。无需认证的 Gateway 可以省略此项。
- `GatewayIdempotencyRetention`：该 Gateway 的幂等键保留时长。

现有共享客户端重试设置继续由 Host 全局管理，除非实现过程中证据表明它们必须按 target 区分。使用 binding 的 `Retry.MaxAge` 计算每个 target 所关联 binding 的最大重试期；该 target 配置的幂等保留时长必须不短于此值。

binding 引用的 target 必须在启动时完整配置且有效。不得从全局 OpenClaw 配置隐式继承 URI 或凭据。

### 路由与持久化 outbox

`SnapshotReconciler` 创建 wake 时，将匹配到的 binding 中的 `OpenClawTarget` 和 `MetaSkill` 一并复制到 outbox item。`WakeRequest` 在派发过程中携带 target。此改动需要为 `WakeOutboxItem` 增加持久化字段和数据库迁移。

Host 提供支持 target 路由的 `IWakeSink`。它按 outbox item 的 target 选择对应配置的 `OpenClawMetaInvocationClient`，并将调用及状态查询都委派给同一 target。每个客户端有自己的 base address 和可选 bearer token。请求中的 skill、input、session ID、幂等键及响应行为保持不变。

持久化逻辑 target 可避免后续 binding 修改导致已排队任务被改道。Target 名称是稳定的逻辑身份；有意修改某名称对应的 endpoint，会将之后按该名称派发的任务导向新 endpoint。只要持久化任务仍引用某 target，运维方就必须保留该 target 配置。若可派发或仍需跟踪的 outbox work 引用了未配置的 target，启动校验必须给出明确失败，不得静默选择其他 target。

迁移前创建、因而尚无 target 的记录，由启动流程根据当前加载的、ID 相同的 binding 执行一次性回填。如果旧的未完成记录没有匹配 binding，或该 binding 没有有效配置的 target，则启动失败。旧 schema 未记录 Gateway 选择，这是唯一可恢复的路由依据。

### 失败行为

- Binding 缺少 target、target 配置格式错误、target 未知，或该 target 的幂等保留期不足时，Host 启动失败。
- 无法解析 target 的 outbox item 不得发送到其他 Gateway。若启动时能发现未完成记录引用了未配置 target，Host 必须在派发前校验失败。
- 网络或 Gateway 调用失败继续使用现有重试和 dead-letter 机制，并始终针对已选定的 target 执行。

## 测试

- 契约加载测试验证缺失或空白 `openClawTarget` 会被拒绝，非空值可通过。
- 启动校验测试验证未知 target、无效 URL、格式错误的非空凭据，以及 target 幂等保留期短于所关联 binding 最大重试期时会失败。
- 路由测试验证调用和状态查询使用所选 target 的 endpoint 及其已配置的 bearer token（不同 target 的凭据不会串用），同时保持 skill 和幂等键不变。
- Outbox 测试验证 target 被持久化，并确认修改 binding target 不会改变已排队任务的 target。
- 启动恢复测试验证旧 outbox 记录从当前 binding 回填；缺少有效 binding 时明确失败；移除仍被未完成任务引用的 target 不会导致任务改道。
- 更新所有现有 registry fixture，为每个 binding 指定 target，并在 Host 测试中提供对应 target 配置。

## 范围与迁移

这是一项跨层功能，涉及 Core 契约与领域模型、registry 加载和启动校验、OpenClaw adapter 路由、SonnetDB 持久化与迁移、Host 依赖注入与配置，以及对应测试。Registry 公共契约有意收紧：所有 binding 都必须声明 target。不增加源码兼容用的默认路由。
