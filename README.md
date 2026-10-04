# DrasiWake
在"环境式 Agent"（Ambient Agent）架构中，Agent 平时静默，数据变化时被唤醒。Drasi 负责感知层（持续查询定义"什么变化值得关注"），OpenClaw.NET 负责执行层（MetaSkill DAG 定义"唤醒后做什么"）。两者之间需要一个桥接组件，把"查询结果集变化"翻译为"Agent 会话唤醒"

## Aspire 本地开发环境

在本仓库同级放置 `drasi-server` 与 `openclaw.net` 后，可用 Aspire 一次启动两组 Compose 依赖和 DrasiWake Host。真实服务测试用的 Drasi 配置与 OpenClaw MetaSkill 均位于本仓库 `dev/fixtures`。配置密钥、路径覆盖、冲突处理与清理语义见 [Aspire 本地环境指南](docs/development/aspire-local-environment.md)。

```powershell
dotnet run --project src/DrasiWake.AppHost/DrasiWake.AppHost.csproj
```

## 启动 Bridge

独立的 .NET 10 Host 默认读取 `src/DrasiWake.Host/appsettings.json`，并使用 `src/DrasiWake.Host/contracts/sample-binding.yaml` 中的绑定注册表。每个绑定都必须通过 `openClawTarget` 指向一个已配置的 Gateway；凭据应通过对应目标的环境变量或密钥提供程序（例如 `DrasiWake__OpenClaw__Targets__sample-gateway__BearerToken`）提供，不要将凭据写入 appsettings 或注册表。

在仓库根目录运行以下命令启动 Host：

```powershell
Push-Location src/DrasiWake.Host
dotnet run
Pop-Location
```

配置、恢复行为、遥测、测试以及外部服务契约门禁详见 [Bridge Core V1 运维手册](docs/bridge-core-v1-operations.md)。
