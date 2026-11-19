# mygo-bot

一个用 Go 写的报时 agent。它通过 Responses API 调用模型，提供 `current_time` 工具。配置、原始日志、队列和上下文都在当前目录的 `mygo-bot.sqlite` 中。

## 跑一次报时

使用 Go 1.27.4 和 C 编译器（SQLite 驱动需要）。在项目目录运行 `go run .`，服务监听 `127.0.0.1:8080`。在另一个终端执行下面的请求。示例假设 `MAGPIE_KEY` 已设为本机网关密钥。

```sh
api=http://127.0.0.1:8080
curl -sS -X PUT "$api/config" --json "{\"base_url\":\"http://127.0.0.1:3425/v1\",\"api_key\":\"$MAGPIE_KEY\",\"model\":\"deepseek/deepseek-flash\",\"instructions\":\"询问时间时调用 current_time，回答时带上时区。\"}"
curl -sS "$api/sessions" --json '{"reasoning_effort":"low"}'
# 以下以新库第一次创建的会话 1 为例。
curl -sSN "$api/sessions/1/messages" -H 'Accept: text/event-stream' --json '{"text":"我电脑上现在几点？"}'
curl -sS "$api/sessions/1/items"
curl -sS "$api/sessions/1/context"
```

## 读代码

从 `agent.go` 的 `run` 开始。每次循环先取队列，再调用模型。`accept` 收到完整助手消息时，在同一事务中写入日志和上下文。若随后断流，重试会带上这条消息，模型可能再回答一次。收到 `response.completed` 后，`complete` 决定会话继续、暂停还是回到空闲。每个活跃会话有自己的循环。

Go 收到完整的工具调用就写日志，并启动一个独立任务执行它，收流循环可以继续读下一帧。工具一返回，结果就写进 `items`。假设先调用 A、再调用 B，B 可以先落库。当前请求收尾时，`run` 等全部工具退出，`store.go` 的 `publishTools` 再按 A、B 的顺序把调用和结果成对加入视图。之后才会重试或请求下一次模型响应。

空请求恢复会话时，同一个事务先读取 `last_error`，发布已有结果，为真正缺结果的调用补上中断原因，然后清空 `last_error`。用户暂停、daemon 重启和正常关机各有对应提示；其他情况只记录结果未知。正常工具报错会直接成为对应调用的结果。

`schema.sql` 定义四张表。`store.go` 的 `pending` 描述哪些用户消息还可以撤回。`responses.go` 负责发送历史和读取 SSE，`api.go` 提供 HTTP 入口。运行 `go test -race ./...` 可以复现并行工具、断流、超时和暂停。

## HTTP 接口

| 方法和路径 | 用途 |
| --- | --- |
| `GET /config`、`PUT /config` | 读取或整体设置配置，读取时隐藏密钥。可设 `default_reasoning_effort`。 |
| `POST /sessions`、`GET /sessions` | 创建或列出会话。推理强度可取 `low`、`high`、`max` 或 `null`。 |
| `GET /sessions/{id}`、`PATCH /sessions/{id}` | 查看状态和停止原因，或修改 `reasoning_effort`。 |
| `POST /sessions/{id}/messages` | 提交 `{"text":"…"}`。空闲时启动，已运行或已暂停时返回 `202` 和消息 ID。 |
| `GET /sessions/{id}/queue`、`DELETE /sessions/{id}/queue/{item}` | 查看或撤回排队消息。已经进入视图的消息返回 `409`。 |
| `POST /sessions/{id}/pause` | 空请求体为软暂停，当前模型请求和它的工具收尾后暂停。`{"mode":"hard"}` 立即取消，也能升级已经请求的软暂停。 |
| `POST /sessions/{id}/resume` | 空请求体，只唤醒会话。重启后遗留的工作处于 `paused`，需显式恢复。 |
| `GET /sessions/{id}/items`、`GET /sessions/{id}/context` | 分别查看原始日志和当前可发送的上下文。 |

列表接受 `limit`（默认 50，最多 200）和 `after`。上下文按 `position` 翻页，其余按 `id` 翻页。

启动执行的请求默认等待 JSON，带 `Accept: text/event-stream` 可收到 `text.delta`、`tool.called` 和最终的 `turn.completed`、`turn.paused` 或 `turn.failed`。`attempt.retry` 标明哪次模型请求中断并触发了重试。只有文字片段的消息不会写入数据库。调用方断开连接后 daemon 会继续工作。发完请求体后最多等 30 秒响应头。收到响应头后，等待下一个完整 SSE 事件或心跳的期限为 60 秒。瞬时故障最多连续重试 3 次，等待间隔依次为 250 毫秒、500 毫秒、1 秒。只有当前模型响应正常完成才重置计数。耗尽后会话转入 `paused`，可从 `last_error` 查看原因，通过 `/resume` 重新开始。正常工具往返次数不限。

收到 SIGINT 或 SIGTERM 后，daemon 停止接收新工作，给活跃会话 5 秒软暂停收尾时间，到期取消仍在运行的请求和工具。工具需响应取消信号。HTTP 处理和会话任务全部退出后才关闭数据库。

## 数据库启动规则

首次启动建库。已有数据库必须与内嵌的 `schema.sql` 匹配，结构有差异就退出。数据库只供一个 daemon 使用。为保证单文件运行，回滚日志放在内存中；写事务途中断电可能损坏数据库。API 密钥以明文存入本机数据库。
