# MyGo-Bot

这是一个用 Rust 编写的极简 Agent 运行框架。

## 常用命令

可执行文件叫 `mygo`。运行 `mygo daemon` 在后台启动，或用 `mygo daemon --foreground` 在终端看日志。TCP 默认从 `127.0.0.1:26946` 开始监听。

单节点启动天然构成一个 1 节点私有集群（纯单机默认延迟到生成邀请码或加入集群时才按需绑定 `iroh` QUIC 端点，平时零额外 UDP 端口与网络开销）；可通过 WebUI 或 `/cluster/*` 接口生成一次性邀请码把多台设备/实例接入同一集群自动同步状态。加入同一集群的多节点不仅通过 `iroh` QUIC 对等暴露统一节点寻址的能力接口（模型在 `codemode` 中通过 `cluster.nodes()` 与 `cluster.node(target)` 即可无差别调度任意在线节点上的 `fs`、`proc`、`http`、`mcp` 与 `mygo` 能力），还通过专门的 `mygo/exec/1` 通道实现了**控制面与执行面分离**——任何节点都可作为入口触发（`PUT /sessions/{sid}/run` 支持指定 `{ "node": "..." }`）、插话、撤回、暂停、查停后台任务并实时中继远端运行中的 SSE 流。局域网或本机多实例联调可按需传入：

- `--cluster-local`：禁用公共中继与 DNS 发现，仅走本地直连集群模式；
- `--cluster-bind <IP:PORT>`：把 `iroh` QUIC 集群端点固定绑定到指定 UDP 地址；
- `--cluster-role <voter|edge>`：设置节点在集群执行多数派租约中的投票角色（默认 `voter` 计入多数派门槛；`edge` 作为边缘节点自由上下线、不计入多数派门槛但仍参与在线占有探测与能力/执行调度）。

macOS 和 Linux 可用 `mygo install` 安装到 `~/.local/bin`。用 `mygo service install` 注册登录自启，`mygo service start` 立即启动，`mygo service status` 查看状态。系统级安装和服务命令加 `--system`。

## 主要技术栈

- Rust 1.100.0（`axum` 提供 HTTP 和 SSE 接口，`reqwest` 对接 OpenAI Responses API）
- P2P 集群与一致性同步（`iroh` QUIC 私有组网、基于半格律 CRDT（目录逐字段 HLC LWW + 因果向量）与上下文 DAG 的无冲突增量同步，以及 `blake3` 鉴权与附件内容完整性校验）
- SQLite（`rusqlite`）

## 许可证

MIT
