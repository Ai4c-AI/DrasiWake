# DrasiWake
在"环境式 Agent"（Ambient Agent）架构中，Agent 平时静默，数据变化时被唤醒。Drasi 负责感知层（持续查询定义"什么变化值得关注"），OpenClaw.NET 负责执行层（MetaSkill DAG 定义"唤醒后做什么"）。两者之间需要一个桥接组件，把"查询结果集变化"翻译为"Agent 会话唤醒"
