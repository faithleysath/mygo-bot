# MyGo-Bot

这是一个用 Rust 编写的极简 Agent 运行框架。

## 常用命令

可执行文件叫 `mygo`。运行 `mygo daemon` 在后台启动，或用 `mygo daemon --foreground` 在终端看日志。TCP 默认从 `127.0.0.1:26946` 开始监听。

macOS 和 Linux 可用 `mygo install` 安装到 `~/.local/bin`。用 `mygo service install` 注册登录自启，`mygo service start` 立即启动，`mygo service status` 查看状态。系统级安装和服务命令加 `--system`。

## 主要技术栈

- Rust 1.100.0（`axum` 提供 HTTP 和 SSE 接口，`reqwest` 对接 OpenAI Responses API）
- SQLite（`rusqlite`）

## 许可证

MIT
