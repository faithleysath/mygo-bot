# MyGo-Bot

这是一个用 Rust 编写的极简 Agent 运行框架。

## 常用命令

可执行文件叫 `mygo`。运行 `mygo daemon` 在后台启动，或用 `mygo daemon --foreground` 在终端看日志。TCP 默认从 `127.0.0.1:26946` 开始监听。

单节点启动天然构成一个 1 节点私有集群；可通过 WebUI 或 `/cluster/*` 接口生成一次性邀请码把多台设备/实例接入同一集群自动同步状态。局域网或本机多实例联调可按需传入：

- `--cluster-local`：禁用公共中继与 DNS 发现，仅走本地直连集群模式；
- `--cluster-bind <IP:PORT>`：把 `iroh` QUIC 集群端点固定绑定到指定 UDP 地址。

macOS 和 Linux 可用 `mygo install` 安装到 `~/.local/bin`。用 `mygo service install` 注册登录自启，`mygo service start` 立即启动，`mygo service status` 查看状态。系统级安装和服务命令加 `--system`。

## 主要技术栈

- Rust 1.100.0（`axum` 提供 HTTP 和 SSE 接口，`reqwest` 对接 OpenAI Responses API）
- P2P 集群与一致性同步（`iroh` QUIC 私有组网、`blake3` 哈希链同步账本与附件内容完整性校验）
- SQLite（`rusqlite`）

## 许可证

MIT
