# Aspire 本地开发环境

Aspire AppHost 将本仓库的 DrasiWake Host 与两个 Compose 项目统一启动。它只用于本地开发和集成测试；生产仍直接部署 `src/DrasiWake.Host`。本仓库在 `dev/fixtures` 保存 Aspire 使用的 Drasi 与 OpenClaw Compose 副本、专用 Drasi 测试配置、Compose override 和 OpenClaw workspace。Drasi Server `0.2.3` 与 OpenClaw.NET 都从对应 sibling repo 的 Dockerfile 和构建上下文构建；Compose 相对路径仍以对应 sibling repo 为项目目录解析。

## 前置条件

- .NET 10 SDK。
- Docker Desktop 已启动，`docker info` 可连接 daemon。
- Docker Compose 支持 `up --wait --wait-timeout`。
- Drasi Server 与 OpenClaw.NET 仓库可访问。

默认仓库布局：

```text
<workspace-parent>/
  DrasiWake/
  drasi-server/
  openclaw.net/
```

AppHost 从 `DrasiWake.sln` 定位本仓库根目录。路径缺省时使用同级目录；相对覆盖路径也以本仓库根目录为基准，而不是当前终端目录。可用标准 .NET 配置提供程序（User Secrets 或进程环境变量）覆盖：

| 配置键 | 用途 |
| --- | --- |
| `DrasiWake:DevEnvironment:DrasiRepositoryPath` | Drasi Server 仓库路径 |
| `DrasiWake:DevEnvironment:OpenClawRepositoryPath` | OpenClaw.NET 仓库路径 |
| `DrasiWake:DevEnvironment:OpenClaw:ModelProviderKey` | 必需的模型提供方密钥 |
| `DrasiWake:DevEnvironment:OpenClaw:AuthToken` | 必需的 Gateway 认证令牌 |
| `DrasiWake:DevEnvironment:OpenClaw:ModelProviderEndpoint` | OpenAI-compatible API base URL |
| `DrasiWake:DevEnvironment:OpenClaw:ModelName` | 模型名称 |
| `DrasiWake:DevEnvironment:EnableAspireEndToEnd` | 显式启用 Aspire 传感器端到端 binding，默认 `false` |

本地 Compose 与契约 fixture 由本仓库管理：`dev/fixtures/drasi/docker-compose.yml` 和 `dev/fixtures/openclaw/docker-compose.yml` 是对应 sibling repo Compose 文件的副本；OpenClaw 副本额外设置 `OpenClaw__Canvas__Enabled=false`，因为其 `0.0.0.0` bind 下 Gateway 会拒绝启用 Canvas 命令转发。Drasi override 从 sibling 源码构建 `drasi-server:0.2.3`，并只读挂载 fixture 配置；插件目录设为容器内可写的 `/app/plugins`。配置将 `source/mock` 固定为 `0.2.12`、`reaction/log` 固定为 `0.2.9`，两者的插件 SDK 元数据均为 `0.11.3`，与 server 使用的 `0.11.2` 兼容。`dev/fixtures/openclaw/workspace/skills/drasiwake-sensor-reading-summary/SKILL.md` 定义 Gateway MetaSkill。AppHost 将本地 Compose 副本传给 Docker Compose，并保留 sibling repo 作为 project directory，以解析 `.env`、相对挂载路径和两种镜像的构建上下文。修改源 Compose 文件后，需同步更新本仓库副本并保留本地覆盖。

PowerShell 环境变量形式使用双下划线分隔配置层级，例如 `DrasiWake__DevEnvironment__OpenClaw__AuthToken`。也可直接设置 `MODEL_PROVIDER_KEY`、`MODEL_PROVIDER_ENDPOINT` 和 `MODEL_PROVIDER_MODEL`；`MODEL_PROVIDER_MODEL` 会作为 `OPENCLAW_MODEL` 提供给 Compose。`LLM_API_KEY`、`LLM_BASE_URL` 和 `LLM_MODEL_NAME` 仍作为兼容 fallback。优先级为显式 `DrasiWake:DevEnvironment:OpenClaw:*` 配置、`MODEL_PROVIDER_*`、再到 `LLM_*`。密钥不要写入仓库文件、AppHost 参数或 Compose 命令行；AppHost 只把模型配置和 `OPENCLAW_AUTH_TOKEN` 作为 OpenClaw Compose 子进程环境变量，并将认证令牌通过 Aspire secret parameter 传给 Host。Drasi Compose 不继承这些模型变量；它仍按原有 Compose 行为从其仓库加载 `.env`，AppHost 不读取、复制或打印该文件。

## 启动

在仓库根目录运行：

```powershell
dotnet run --project src/DrasiWake.AppHost/DrasiWake.AppHost.csproj
```

也可通过以下环境变量覆盖外部仓库路径：

```powershell
$env:DrasiWake__DevEnvironment__DrasiRepositoryPath = 'E:\GitHub\drasi-server'
$env:DrasiWake__DevEnvironment__OpenClawRepositoryPath = 'E:\GitHub\openclaw.net'
```

AppHost 在启动 Host 前会检查配置、Compose 文件、Docker daemon、`--wait` 能力、固定容器名和 Compose YAML 声明的默认宿主机端口；随后并行启动两个栈并等待 Compose 健康检查通过。它用 `docker compose port` 发现实际发布地址，再注入 `DrasiWake:Drasi:ServerUri`，以及 `DrasiWake:OpenClaw:Targets:sensor-gateway:*` 下的 Gateway 地址、Bearer token 和 `30.00:00:00` 幂等保留时长。token 通过 Aspire secret parameter 传入，不写入配置文件或命令行。

默认情况下 Host 继续加载通用 `contracts/sample-binding.yaml`。只有显式设置 `DrasiWake__DevEnvironment__EnableAspireEndToEnd=true`，AppHost 才会将 Host registry 覆盖为 `contracts/aspire-sensor-binding.yaml`；该 binding 对应 fixture 的 `drasiwake-sensor-monitor` / `sensor-readings`、`SensorId` / `Temperature` / `Humidity` 和 `drasiwake-sensor-reading-summary`。fixture 每 3 秒生成一次传感器更新，启用此模式并配置真实模型提供方时可能持续产生模型调用和费用。普通启动及三个独立的真实服务 contract 测试开关都不会启用此模式。

需要手动运行完整 Aspire 传感器端到端流程时，在启动 AppHost 的 PowerShell 进程中显式设置：

```powershell
$env:DrasiWake__DevEnvironment__EnableAspireEndToEnd = 'true'
dotnet run --project src/DrasiWake.AppHost/DrasiWake.AppHost.csproj
```

结束后可运行 `Remove-Item Env:DrasiWake__DevEnvironment__EnableAspireEndToEnd` 关闭该进程环境变量。

如果 Compose 启动、端口发现或后续 Host 进程启动失败，AppHost 会显式执行回滚；不会将未能确认属于本次运行的容器纳入清理。

## 冲突与清理

已有固定容器名、默认端口占用或 Docker/Compose 不可用时，AppHost 会在启动前失败，不会接管或清理现存资源。正常退出时先停止 Host，再按启动的逆序对本次运行拥有的 Compose project 执行 `down`。清理不带 `--volumes` 或 `-v`，因此保留 `drasi_postgres_data` 与 OpenClaw memory volume。

强制结束 AppHost 可能留下容器。下次启动会因固定容器名冲突而拒绝接管；确认资源归属后再由开发者手动清理。不要为了清理而删除持久化 volume。

初版不启用 OpenClaw 的 `with-tls` profile。现有 Compose 文件可能将发布端口绑定到所有宿主机网卡，而非仅回环地址；请将该开发环境视为可被本机网络访问。AppHost 不修改外部 Compose 文件。

## 外部契约测试

三个真实 Aspire 测试共用同一个 AppHost fixture：smoke 检查 Compose 服务启动和地址发现；Drasi contract 检查查询枚举、results 快照和 attach；Gateway contract 检查相同幂等键的并发重放及保留时长。启用任一运行开关后，fixture 启动 AppHost 一次；测试地址取自 Compose 实际发布端口，测试结束时按正常 Aspire 生命周期停止本次栈。

Drasi contract 默认使用 fixture 的 `drasiwake-sensor-monitor` instance 和 `sensor-readings` query；可通过 `DRASIWAKE_REAL_DRASI_INSTANCE_ID` / `DRASIWAKE_REAL_DRASI_QUERY_ID` 覆盖。Gateway contract 默认调用本仓库的 `drasiwake-sensor-reading-summary` skill，可通过 `DRASIWAKE_REAL_GATEWAY_TEST_SKILL` 覆盖。模型提供方密钥与 Gateway token 是不同凭据：前者用于真实模型调用，后者用于 Gateway 认证；通过 AppHost User Secrets 或本地进程环境配置，不要放进测试命令参数。

运行完整解决方案测试并显式启用三个真实 Aspire 检查：

```powershell
$env:DRASIWAKE_RUN_REAL_ASPIRE_SMOKE = '1'
$env:DRASIWAKE_RUN_REAL_DRASI_CONTRACT_TESTS = '1'
$env:DRASIWAKE_RUN_REAL_GATEWAY_CONTRACT_TESTS = '1'
dotnet test --solution DrasiWake.sln --configuration Release
```

最近验证记录（2026-10-03）：以上述三个开关运行完整解决方案，109 项通过、0 项失败、0 项跳过；Aspire Compose 服务在测试结束后停止，持久化数据卷保留。

真实 contract 同时运行时必须提供 Gateway token。若只运行 smoke 且没有配置 token，fixture 会生成仅用于该测试进程的随机临时 token；不要依赖此行为运行 Drasi 或 Gateway contract。Gateway contract 会先通过 `/apps/chat` 建立测试 session，再并发提交两次相同的 MetaInvocation 请求；会话准备和 MetaSkill 执行都会调用模型，可能产生模型服务费用。

只运行某一项时，设置对应开关，并使用 xUnit v3 / Microsoft Testing Platform 的 `--filter-class` 过滤器。例如：

```powershell
$env:DRASIWAKE_RUN_REAL_GATEWAY_CONTRACT_TESTS = '1'
dotnet test --project tests/DrasiWake.IntegrationTests/DrasiWake.IntegrationTests.csproj --configuration Release --filter-class DrasiWake.IntegrationTests.RealGatewayContractTests
```

未设置任一运行开关时，三个真实 Aspire 测试会跳过且不会启动 Aspire 或 Docker 服务。
