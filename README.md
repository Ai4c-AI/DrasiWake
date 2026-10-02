# DrasiWake
在"环境式 Agent"（Ambient Agent）架构中，Agent 平时静默，数据变化时被唤醒。Drasi 负责感知层（持续查询定义"什么变化值得关注"），OpenClaw.NET 负责执行层（MetaSkill DAG 定义"唤醒后做什么"）。两者之间需要一个桥接组件，把"查询结果集变化"翻译为"Agent 会话唤醒"

## 启动 Bridge

独立的 .NET 10 Host 默认读取 `src/DrasiWake.Host/appsettings.json`，并使用 `src/DrasiWake.Host/contracts/sample-binding.yaml` 中的绑定注册表。请通过环境变量或密钥提供程序设置 `DrasiWake__OpenClaw__BearerToken`；不要将凭据写入 appsettings 或注册表。

在仓库根目录运行以下命令启动 Host：

```powershell
Push-Location src/DrasiWake.Host
dotnet run
Pop-Location
```

配置、恢复行为、遥测、测试以及外部服务契约门禁详见 [Bridge Core V1 运维手册](docs/bridge-core-v1-operations.md)。
