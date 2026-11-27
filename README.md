# MyGo-Bot

这是一个用 Rust 编写的极简常驻 Agent 运行框架。开箱是一个带有内嵌 WebUI、OpenAPI 文档、SQLite 只增账本与 QuickJS 工具沙箱的单进程服务；多台机器用邀请码组群后，能自动通过 `iroh` QUIC 同步状态、跨节点调度能力并实现控制面与执行面分离。

---

## 快速上手（3 步跑起来）

1. **启动服务**：
   - 临时试跑：`mygo daemon`（后台运行，当前目录下生成 `mygo.sqlite3` 与 `./mygo.log`）或 `mygo daemon --foreground`（前台直接看日志）；
   - 安装成开机自启服务（macOS / Linux）：
     ```bash
     mygo install
     mygo service install
     mygo service start
     ```
2. **获取地址与 Token**：默认监听 `127.0.0.1:26946`（端口被占会顺延）。执行 `mygo service status` 可看当前监听地址与自动生成的一次性 Bearer Token（未传 `--token` 时不会落盘）。
3. **打开网页开聊**：浏览器访问 `http://127.0.0.1:26946/` 输入 Token（或走免密 Unix Socket），先在左下角 **Models** 添加一个兼容 OpenAI Responses API 的供应商和模型，即可新建会话开始对话；回环地址下访问 `http://127.0.0.1:26946/docs` 可直接浏览交互式 API 文档。

---

## 常用命令行与可选参数

- 服务维护：`mygo service status | stop | restart | logs [-n 行数] [-f] | uninstall`（系统级管理与卸载加 `--system`；`mygo uninstall` 一并卸载二进制）。
- `mygo daemon` 常用选项：
  - `--foreground`：前台运行，日志打到 `stderr`；
  - `--listen <IP:PORT>`：固定 HTTP/SSE 监听地址（端口填 `0` 自动分配）；
  - `--token <TOKEN>`：指定 TCP 访问密码（Bearer Token）；
  - `--js-threads <COUNT>`：设置 JS 沙箱工人线程数（默认按 CPU 核数最多 4 条）；
  - `--unix-socket <PATH>` / `--no-unix-socket`：自定义或禁用免 Token 本地 Unix Socket（仅 Unix）。
- **私有集群参数（默认全部可省略）**：
  - 单机默认**按需延迟绑定** QUIC 端点——平时不开额外 UDP 端口，直到在 WebUI / API 生成邀请码或加入集群才启动；
  - `--cluster-local`：禁用公共中继与 DNS 发现，仅走局域网或本机直连；
  - `--cluster-bind <IP:PORT>`：显式固定 `iroh` QUIC 集群 UDP 端口；
  - `--cluster-role <voter|edge>`：预设单机初始投票角色（默认 `voter`；笔记本等常离线设备可在单机启动时设或组群后在线切为 `edge`）。

---

## 工作原理文档（How MyGo-Bot Works）

想深入了解内部状态机、单线程 SQLite 合批记账、QuickJS 原语、MCP OAuth 与无冲突集群设计，请从 [架构文档总览](how-it-work/README.md) 开始读（共 9 篇，由浅入深）：

1. [01 · 核心抽象与 Agent 循环](how-it-work/01-agent-loop.md)
2. [02 · 底层底座：单记账员、合批落盘、流式读写与防重](how-it-work/02-storage-and-concurrency.md)
3. [03 · 模型目录、多模态附件与精确计费](how-it-work/03-catalog-media-and-billing.md)
4. [04 · Codemode 上篇：最小完备原语、独立车间与宿主能力](how-it-work/04-codemode-sandbox.md)
5. [05 · Codemode 下篇：前后台分流、原地等信、实时透视与租期恢复](how-it-work/05-codemode-lifecycle.md)
6. [06 · 外部工具扩展：MCP 协议、双传输与 OAuth 2.1](how-it-work/06-mcp-and-oauth.md)
7. [07 · 实时流、HTTP API、内置文档与 WebUI](how-it-work/07-realtime-api-and-webui.md)
8. [08 · 私有集群：无冲突同步与多数派执行控制](how-it-work/08-cluster-sync-and-execution.md)
9. [09 · 部署运维、压测验证、瘦身发布与评测体系](how-it-work/09-deployment-testing-and-tooling.md)

---

## 主要技术栈

- Rust 1.100.0（`axum` 提供 HTTP/SSE、`reqwest` 对接 OpenAI Responses API、`rquickjs` 驱动 JS 车间）
- 私有 P2P 集群（`iroh` QUIC `mygo/{cluster,exec,cap,join}/1`、半格律 CRDT 与上下文 DAG、BLAKE3 鉴权与分块校验）
- SQLite（`rusqlite`，独占记账与全链路流式读写）与零膨胀 Preact WebUI

## 许可证

MIT
