# 07 · 实时流、HTTP API、内置文档与 WebUI

前面六篇把后端的账房、循环、沙箱和外部扩展全立起来了。可真把系统摊到桌面上让人用，还得解决三件事：

1. **屏幕怎么既能看到模型实时吐字（打字机流），又能保证几十个浏览器窗口围观时不漏字、慢窗口还不拖垮主循环？**
2. **整套 HTTP 接口到底怎么分区暴露、在哪道门验 Token、在哪道门放行？**
3. **不想另外架一套笨重的前后端工程，怎么只靠同一个二进制直接端出轻量、零闪屏、弱网不覆盖的 WebUI 与 API 文档？**

---

## 两级实时 SSE 流：围观再多也不堵门

早期架构只推写进 SQLite 的成品 `item`，人要等整段话说完才看得见。为了做到实时跳字又不污染只增不删的账本，[src/events.rs](../src/events.rs) 和 [src/model.rs](../src/model.rs) 搭了一套**“两级广播 + 全局递增序号”**中心（`events::Hub`）。

### 1. 全局流与单会话双通道

- **全局流 `GET /events`（容量 256）**：连上先发 `ready`（`{}`），之后广播 `session.created` / `updated` / `status` / `deleted` 以及同步落库触发的 `catalog.updated` / `mcp.updated` 供侧边栏和设置页刷新；掉队者收到 `resync`。
- **单会话流 `GET /sessions/{sid}/events`**：同时接两条广播通道：
  - **持久通道（记账员拥有，容量 64）**：只在事务 `COMMIT` 后发送成品事件（`item.saved`、`session.status`、`session.updated`、`session.reset`、`attempt.retry`），零订阅者时经 `Notice::publish_serializable` 直接短路连 JSON 都不拼，有订阅者时一份 `Arc<str>` 全员共享引用；慢窗口积压满 64 条立刻发携带最新 `view` 的 `resync` 让前端自己补课，绝不回头等它。
  - **临时草稿通道（`Hub` 拥有，容量 256）**：上游吐字经过 `DeltaBatcher`（满 **30 毫秒** 或 **256 字节**打包一次）发 `item.started`、`item.delta`、`stream.clear`。内存里另留一份每字段封顶 **256 KiB** 的 `ActiveDraft` 影子草稿——刷新页面或半路点进来时，紧跟着 `ready` 就收到 **`stream.snapshot`** 立刻把半截话续上。

### 2. 防串页视图令牌 `X-History-View` 与分支同步

- `/history`、`/context` 与 `/events` 的响应头均携带当前 **`X-History-View`**（`<instance_id>:<view_rev>`），`/events` 首包 `ready` 和掉队 `resync` 也会携带 `view`。
- 用户切叶节点 `active_head`、或远端同步引发分支切换 / 乱序插旧项（`out_of_order_view`）时，后端递增 `view_rev` 并广播 `session.reset`（若远端 `CatalogSync` 切走了本机正在跑的分支，立刻由 `abort_active_session_worker` 停掉旧 Worker）；翻页接口带上 `?view=<token>` 一旦对不上当场返 **`409 Conflict`**。

### 3. 跨节点实时流中继（`ALPN b"mygo/exec/1"`）

- 单会话流核心封装在可复用的 `SessionEventStream`（同时服务本地 HTTP SSE 和远端 `ExecServer::handle_subscribe_events`，关闭时顺手 `remove_idle_stream` 回收空闲槽位）。
- 浏览器连上**非执行节点**的 `GET /sessions/{sid}/events` 时，协程接上 `PresenceState::watch_remote_runner(sid)` 自动走 `SubscribeSessionEvents` 中继（最多 **64 路**并发许可；连纯单机未组群前提前打开的连接，也会监听 `subscribe_cluster_active()` 在入群后无缝切入中继而不重复吐快照）。
- 出口处维护容量 **256 条**的 **`SeenSavedItems`** 去重窗口（防同条记录既走中继又走同步落库被吐两遍）；远端断开时自动补发携带当前 `view` 的 `resync`，标签页关闭则由 `ActiveRelay::drop` 级联释放远端流。

### 4. 怎么保证“碎片不跑到成品后面”？——全局 `NEXT_SEQ`

两股通道各自并发，为了防成品 `item.saved` 上屏后又冒出迟到的 `item.delta`，全进程共享一枚原子序号 **`NEXT_SEQ`**（[src/events.rs](../src/events.rs)）：
1. 任一事件进槽前先领一个唯一 `seq`；连入 SSE 记下此刻快照覆盖水位 `min_seq`，更老的碎片直接丢弃；
2. 推送协程每吐一条持久事件 `d`，先调 `take_older_deltas` 把临时通道里所有 `seq < d.seq` 的碎片排干再吐 `d`；
3. **两处掉队补救**：持久通道掉队则清手里草稿、发带当前 `view` 的 `resync` 并在需要时补发最新 `stream.snapshot` / `stream.clear`；临时通道掉队先排完持久队列，再重拍快照推进 `min_seq` 并补发快照/清盘事件，一字不缺也不重复。

---

## HTTP API 全景、三条路由面与内置 OpenAPI 文档

整套系统共暴露 **34 条路径、49 个操作**，完整 OpenAPI `3.1.0` 契约与 **55 个 Schema**（单行压缩 JSON 约 **84.0 KiB / 86,030 字节**）编译期嵌在 [src/openapi.json](../src/openapi.json) 里：
- 会话模型涵盖 `runner_node_id`、`active_head`、`branches`（`BranchHead[]`）；支持 `/history` 与 `/context` 的 `?view=` 校验与 `X-History-View`（失配返 `409`）；
- `PUT /sessions/{sid}/run` 支持可选请求体 `{ "node" }`（指定目标直连节点但对方离线时返 `503`；若群内已有节点占用则返 `409`；完全断网时本机可直接开跑）；
- `POST /cluster/join` 在邀请作废或换群未传 `force` 时返 **`409 Conflict`**（不可达返 `503`，允许带活跃会话入群）；`DELETE /cluster/peers/{node_id}` 直接清理离线或在线同伴并断开连接；`POST /cluster/sync` 支持 `404` / `503` / `504`。

| 模块 | 路由一览 | 核心职责 |
| :--- | :--- | :--- |
| **供应商与模型**（12） | `GET/POST /providers`，`GET/PATCH/DELETE /providers/{id}`，`POST .../restore`<br>`GET/POST /models`，`GET/PATCH/DELETE /models/{id}`，`POST .../restore` | 维护多上游地址/密钥（脱敏）、模态/档位/价目表及软归档恢复 |
| **会话与账本**（7） | `GET/POST /sessions`，`GET/PATCH/DELETE /sessions/{sid}`<br>`GET /sessions/{sid}/history`，`GET /sessions/{sid}/context` | 管理会话生命周期、DAG 叶节点分支切换与基于快照 + `view` 的流式分页 |
| **排队消息**（3） | `GET/POST /sessions/{sid}/messages`，`DELETE /sessions/{sid}/messages/{mid}` | 投信（`Idempotency-Key` 幂等）、查排队、撤回未进上下文输入 |
| **运行、流与任务**（6） | `GET /events`，`GET /sessions/{sid}/events`<br>`PUT/DELETE /sessions/{sid}/run`，`GET .../tasks`，`DELETE .../tasks/{tid}` | 点火/软硬暂停（`?mode=soft\|hard`）、订阅两级 SSE、查停后台 JS 任务 |
| **媒体附件**（2） | `POST /attachments`，`GET /attachments/{id}` | 上传不超过 50 MiB 多模态原文件（限流返 `429`）与跨节点分块流式下载 |
| **调用与计费**（3） | `GET /calls`，`GET /calls/summary`，`GET /calls/{id}` | 查询每轮上游调用明细、token、最新上下文长度与 `cost_nano_usd` 费用 |
| **MCP 与 OAuth**（10） | `GET/POST /mcp/servers`，`GET/PATCH/DELETE /mcp/servers/{id}`，`POST .../test`<br>`GET/DELETE .../oauth`，`POST .../oauth/login`，`POST .../oauth/callback` | 注册传输与 `shared` 开关、并发 4 体检探测、启动/手动/撤销 OAuth 2.1 |
| **集群与同步**（6） | `GET /cluster`，`PATCH /cluster/self`，`POST /cluster/invite`，`POST /cluster/join`<br>`DELETE /cluster/peers/{node_id}`，`POST /cluster/sync` | 查集群、改节点名称/描述、经 `mygo/join/1` 准入加入（`409`/`503`）、随时移出同伴与触发同步 |

### 三条不同的路由出口（[src/main.rs](../src/main.rs)、[src/api.rs](../src/api.rs)）

| 入口 | 业务 API (`/sessions` 等) | WebUI (`/`、`/ui/*`) | `/openapi.json` | `/docs` 页面 | `/_runtime` |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **TCP 监听** | **必须带 Bearer Token**（拒绝多头、恒定时间比对 `ConstantTimeEq`） | 放行（JS 自己弹登录门禁） | **仅回环地址免 Token 可见**；非回环直接 404 | 同左，仅回环可见 | 不挂载（均 404） |
| **Unix Socket**（限 Unix） | **免 Token 放行**（靠文件权限 `0600` 鉴权） | 放行 | 不挂载 | 不挂载 | **挂载免 Token**（返 `{ pid, listen, token }`，`no-store`） |
| **进程内 `mygo.request`** | **免 Token 直调**（不经网络栈） | 不挂载 | **挂载免 Token** | 不挂载 | 不挂载 |

`/docs` 页面（[src/docs.rs](../src/docs.rs)）是一个极简 HTML 加载页，以 SRI 校验（`credentials: 'omit'`）从 jsDelivr（8 秒超时自动回退 npmmirror 20 秒）加载 Scalar UI 渲染 `/openapi.json`，禁用遥测。

---

## 零膨胀内嵌的 Preact WebUI

前端代码全在 `web/` 下：**运行时唯一依赖只有 `preact@11`，开发期只有严格配置下的 `typescript`**（无笨重状态库）。

### 1. 构建、压缩与强 ETag 嵌入（`web/build.mjs` & `src/webui.rs`）

执行 `bun run build` 先跑 `tsc --noEmit` 全量类型检查，随后输出三份静态资源编译期通过 `include_bytes!` 焊进二进制：
- `bun` 打包 + `gzip -9` → **`src/webui/app.js.gz`（约 45.3 KiB / 46,382 字节）**；
- 状态机压缩 CSS + gzip → **`src/webui/app.css.gz`（约 6.6 KiB / 6,736 字节）**；
- 内联无闪屏主题探测脚本 → **`src/webui/index.html`（898 字节）**。

服务端对静态资源按 64 位 FNV-1a 算出强 ETag（命中 `If-None-Match` 直接回 `304`），`.js` / `.css` 直接附带 `Content-Encoding: gzip` + `Vary: Accept-Encoding` 输出。

### 2. 鉴权门禁与手写 `fetch` SSE 客户端（`web/src/api.ts`、`web/src/main.tsx`）

- **一次性 URL Token 与免密自动绕过**：打开网页先检查 `#token=...` 并存入 `localStorage.mygo_token`（立刻 `history.replaceState` 抹掉 URL 痕迹），再探一下 `GET /cluster`——若通过 Unix Socket 打开空 Token 直接返回 `200` 就跳过登录框，返回 `401` 则弹 `TokenGate`。
- **手写 SSE & 缩略图缓存**：浏览器原生 `EventSource` 不允许加 `Authorization` 请求头，所以前端用 `fetch` 逐行解码 `ReadableStream`（支持 `500ms -> 8s` 指数退避重连）；附件缩略图进 48 项 LRU 缓存并自动调用 `URL.revokeObjectURL` 释放。

### 3. 前端状态同步的四大防错机关（`web/src/state.ts`）

| 机关 | 它只管哪件事、省掉会出什么乱子 |
| :--- | :--- |
| **1. 全局快照与增量缓冲**<br>（`activeBuffer` + `latestSeq`） | 拉 `GET /sessions` 半路时收到 SSE 或刚做完的本地改动，先记进缓冲并在全量回来后重放叠加；省掉它，列表刷新瞬间会把刚建好的会话或新标题抹回旧值。 |
| **2. 单会话版本与墓碑集**<br>（`baseVersion` + `deletedIds`） | `PATCH /sessions/{sid}` 返回前若版本已被 SSE 推高或会话已入 `deletedIds`，严禁回包覆盖或复活已删会话；冲突时退避 `1s..8s` 重拉单会话权威状态（`404` 转删除）。 |
| **3. 首屏游标、视图校验与重拉**<br>（`knownView` + `gapOpen` + `onReset`） | • 切进会话先拉 `?tail=true&limit=50` 记下 `syncedThroughId` 与 **`knownView`**；<br>• 翻老历史和补缺口均附带 `?view=${knownView}` 与 `AbortController`；`ready`/`resync` 若 `view` 失配当场触发 `onReset()`；<br>• 连上期间开 `gapOpen` 锁游标先渲染新 `item.saved`，补齐缺口再推进；<br>• 一旦遇到 `409`、`session.reset` 或本机切完 `active_head`，`onReset()` 立即取消在途分页、递增 `resetGen`、清游标重拉最新一页并整表替换（用量只按屏幕 `llm_call_id` 合并拉取）。 |
| **4. 草稿防幽灵重现**<br>（`closedCalls` / `closedItems`） | 某项一旦 `item.saved` 或收到 `stream.clear` / `attempt.retry`，立刻清理草稿并封禁不晚于该单调 `CallId` 的旧碎片（重连 `stream.snapshot` 同样过滤），防止同一句话闪回屏幕两次。 |

### 4. 四块界面功能速览

- **侧边栏（`Sidebar.tsx`）**：按 `updated_at` 倒序排列并带实时状态圆点；运行中、暂停中或带有 `runner_node_id` 占用的会话禁止删除（删除需 3 秒二次确认）；窄屏自动收起，底部常驻 `Models` / `MCP` / `Cluster` / `Usage` 导航。
- **时间线（`Timeline.tsx`、`markdown.tsx`）**：撤回消息自动折叠并给原消息画删除线；成对工具卡片语法高亮 `codemode` 脚本并展示返回媒体；自研零依赖 JSX Markdown 解析器（**无 `dangerouslySetInnerHTML`**，单趟线性扫描配对括号与 `new URL` 校验放行 `http/https/mailto`，屏蔽外链图片防跟踪像素）。
- **输入栏与顶栏（`Composer.tsx`）**：
  - 按会话保存文本草稿与粘贴/拖拽附件；发送锁定快照并生成 `Idempotency-Key`（4xx 重置、5xx/网络报错保留防重投，且仅在发送期间没继续打字才清空框）；
  - 多节点时右下角出现**执行节点选择器**（可指定在线节点跑下一轮）；
  - 运行中支持 **`Pause`**（软暂停） / **`Stop`**（硬暂停）、上方折叠任务栏每 2 秒拉取后台终端尾部并支持 `Kill`；
  - 顶栏集成 `Chat` / `Trace` / `Context` 三视图、多分支 **`Branch` 切换下拉框**、`@runner` 徽标、标题编辑、模型/推理档位切换以及调用/缓存命中率/费用/上下文占比环。
- **设置弹窗（`Settings.tsx`）**：`Models`（供应商与模型管理，含密钥编辑与价目 JSON）、`MCP`（含 `shared` 全集群同步开关与告警、OAuth 弹窗登录与跨机手动粘贴 `Submit Callback`）、`Cluster`（本机改名与描述、生成邀请码/加入或带确认 `Force Switch Cluster` 换群、一键同步与成员管理）、`Usage` 与 `General`（深浅主题、`/docs`、登出）。

---

**上一篇**：[06 · 外部工具扩展：MCP 协议、双传输与 OAuth 2.1](06-mcp-and-oauth.md) · **返回目录**：[总览](README.md) · **下一篇**：[08 · 私有集群：无冲突同步与连通图 Gossip 协作](08-cluster-sync-and-execution.md)
