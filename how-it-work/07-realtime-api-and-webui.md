# 07 · 实时流、HTTP API、内置文档与 WebUI

前面六篇把后端的账房、循环、沙箱和外部扩展全立起来了。可真把系统摊到桌面上让人用，还得解决三件事：

1. **屏幕怎么既能看到模型一个字一个字往外吐（打字机流），又能保证几十个浏览器窗口一起围观时一条不漏、慢窗口还不拖垮主循环？**
2. **整套 HTTP 接口到底怎么分区暴露、在哪道门验 Token、在哪道门放行？**
3. **不想另外架一套笨重的前后端工程，怎么只靠同一个二进制，直接给用户端出轻量、零闪屏、不会因弱网乱序覆盖的 WebUI 和 API 文档？**

---

## 两级实时 SSE 流：围观再多也不堵门

早期架构只把写进 SQLite 的成品 `item` 往外推，人坐在屏幕前要干等模型整段话吐完才能看见内容。为了让界面跟聊天软件一样实时跳字，同时又绝不污染只增不删的账本，[src/events.rs](../src/events.rs) 和 [src/model.rs](../src/model.rs#L106-L154) 搭了一套**“两级广播 + 全局统一编号”**的实时流中心（`events::Hub`）：

1. **先分两股流接口**：
   - **全局流 `GET /events`**：容量 **256** 的广播通道。一连上先推 `ready`（`{}`），之后整个进程任何会话新建（`session.created`）、改配置/改标题/换分支（`session.updated`）、切状态（`session.status`）或删除（`session.deleted`），外加集群同步拉来新目录/MCP 时触发的 **`catalog.updated`** 和 **`mcp.updated`**，侧边栏与设置页全靠它实时刷新；谁掉队了收到 `resync`。
   - **单会话流 `GET /sessions/{sid}/events`**：每个新连上的订阅者，都会同时接上**两条独立广播通道**：
     - **持久通道（记账员拥有，容量 64）**：只在事务 `COMMIT` 之后发成品事件——`item.saved`（完整账目）、`session.status`、`session.updated`、`session.reset`（当用户调 `PATCH /sessions/{sid}` 切换叶节点 `active_head` 或 `SessionSync` / `CatalogSync` 重新对齐发生分支切换或清空时递增 `view_rev` 并发出 `{"view": "<token>"}` 通知前端重置游标重拉），外加不进数据库的 `attempt.retry`。并且同步落盘的 `publish_session_merge` 还会先把非活跃分支上的项滤出 `item.saved` 队列，如果发现分支切换或乱序补进了比当前更小的可见 `item_id`（`out_of_order_view`），照样推进 `view_rev`；若远端 `CatalogSync` 切走了正在本机运行会话的分支，`publish_catalog_merge` 更会立刻调 `abort_active_session_worker` 停掉旧本地 Worker 再推 `session.reset`。每次连上 `/sessions/{sid}/history`、`/sessions/{sid}/context` 和 `/sessions/{sid}/events` 时响应头都会带着当前的 **`X-History-View`**（`<instance_id>:<view_rev>`）；连接 `/events` 首包的 `ready` 会带上最新的完整 `Session` 视图快照，后续掉队发出的 `resync` 则同样带着 `{"view": "<token>"}`；而翻页接口 `/history` 与 `/context` 也支持可选查询参数 **`?view=<token>`**，只要分支切换、乱序插入旧项或服务重启导致视图令牌对不上，当场返 **`409 Conflict`** 拦下跨分支混页！**压根没人订阅时，记账员连回查数据库组装 JSON 的活都直接省掉**；有人听时，一笔新账只组装一份 `Arc<str>` 丢进槽里大家共享引用；要是某个慢窗口积压超过 64 条，广播槽直接冲它扔一条携带最新 `view` 的 **`resync`** 让它自己去翻 `/history` 补课，绝不回头等它。
     - **临时草稿通道（`Hub` 拥有，容量 256）**：模型在上游刚吐字时，经过一个 **`DeltaBatcher`**（每攒满 **30 毫秒**或者满 **256 字节**就打包推一次，不按单字符轰炸广播槽）发来 `item.started`、`item.delta`（区分 `text`/`reasoning`/`arguments`）和收尾清空用的 `stream.clear`。同时内存里维持着一个每字段封顶 **256 KiB** 的 `ActiveDraft` 影子草稿——所以你半路刷新页面或刚点进一个正在生成的会话，一连上紧跟着 `ready` 就会收到一份 **`stream.snapshot`**，立刻把屏幕上已经吐到一半的话续上，不用等这轮说完！
   - **跨节点实时流中继与组群前订阅升级（`ALPN b"mygo/exec/2"`）**：单会话流核心逻辑被抽成了可复用的 `SessionEventStream`（同时服务本地 HTTP SSE 与远端 `ExecServer::handle_subscribe_events`；当无订阅者监听时，`Store::item_saved`、`notify_status` 与 `session.created` 通过 `Notice::publish_serializable` 直接短路，省去全部 SQLite 回查与 `Value` 构造；并在连接对象 `Drop` 时顺手调用 `remove_idle_stream` 回收 `Hub` 中的空闲会话槽位）。当浏览器连到**非执行节点**的 `GET /sessions/{sid}/events` 时，接入协程会直接把 `PresenceState::watch_remote_runner(sid)` 接进协调循环；甚至**早在单机尚未组群前就打开的 SSE 连接**，也会在内部同一层 `tokio::select!` 里静默监听 `PresenceState::subscribe_cluster_active()`——只要后续有同伴加入集群（`known_peer_count > 0`），就当场触发 `ensure_session_watched(sid)` 并无缝升级进集群中继循环（且不再重复补发初始 `stream.snapshot`），确保组群后由远端 Runner 跑起来的 `item.started`、`item.delta`、`stream.snapshot`、`stream.clear`、`attempt.retry` 和 `item.saved` 一条不漏地经 `SubscribeSessionEvents` 中继给浏览器。为了防止同一条已完成项既走中继提前露面、又随集群同步提交在本地再次落库触发第二次 `item.saved`，中继出口维护了一圈容量 **256 条**的 **`SeenSavedItems`** 去重窗口；远端中继断开时会自动补推携带当前 `view` 的 **`resync`**（`{"view": "..."}`）；浏览器一旦关闭标签页，借助 `ActiveRelay::drop` -> `recv.stop` -> `send.stopped()` 三端联动立即退出协程并归还远端 SSE 并发许可（上限 `64` 路）。

2. **两股通道怎么保证“碎片不跑到成品后面”？**
   既然打字机碎片走 `Hub` 的 256 通道、成品 `item.saved` 走记账员的 64 通道，万一两路消息在 `tokio::select!` 里抢跑，用户刚看到完整的 `item.saved` 落盘卡片，下一毫秒一条迟到的 `item.delta` 才冒出来怎么办？

   解决招数非常漂亮——**全进程共抢一个原子递增序号 [`NEXT_SEQ`](../src/events.rs#L33-L67)**（[推送协调循环](../src/events.rs#L504-L716)）：
   - 每一条发往持久通道和临时通道的通知，在放进槽里的那一瞬间都领一个唯一的 `seq`；
   - 连入 SSE 时顺手记下此刻快照已经覆盖到的线 **`min_seq`**，所有比 `min_seq` 老的碎片直接丢弃，免得快照和碎片重复叠字；
   - 推送协程每收到一条持久事件 `d` 时，先把 `take_older_deltas` 排干——把临时通道里所有序号 `seq < d.seq` 的碎片先吐干净，**然后再吐 `d`**！如果慢窗口在**持久通道**掉队了，清掉缓存在手里的草稿、在全部 4 处掉队分支统一吐出带当前 `view` 的 `resync` 让前端补拉对齐，同时顺带触发一次草稿恢复（继续补发最新的 `stream.snapshot` 或 `stream.clear`），保证已落盘或已清盘状态不与界面草稿脱节；如果在**临时通道**掉队，同样先把已经排进持久通道的事件送完保证因果顺序，再拿着 `Hub` 锁重拍一次最新快照、把 `min_seq` 往前推，并补发 `stream.snapshot`（若该轮调用刚结束清盘，则补发 `stream.clear`）把实时草稿接回来，既不缺字、也不用把整条长对话重拉一遍。

---

## HTTP API 全景、三条路由面与内置 OpenAPI 文档

整套系统一共对外暴露 **34 条路径、49 个操作**（完整契约及 **56 个 Schema** 以单行压缩 JSON 格式编译期嵌在 [src/openapi.json](../src/openapi.json) 里，约 **85.8 KiB / 87,844 字节**，OpenAPI `3.1.0`；契约内涵盖 **`Session.runner_node_id`**、**`Session.active_head`**、**`Session.branches`**（`BranchHead[]`）、支持切换叶节点分支的 **`SessionPatch.active_head`**、`/history` 与 `/context` 的可选视图校验参数 **`view`** 与响应头 **`X-History-View`**（失配返 `409 Conflict`）、可选请求体 **`SessionRunRequest`**（`PUT /sessions/{sid}/run` 可传 `{ "node": "<id-or-name>", "force_offline": false }` 路由执行节点或显式跳过 `Voter` 多数派在线门控，目标离线或集群失多数派默认返回 `503`）、**`NodeRole`**（`voter`/`edge`）及 **`ClusterSyncSummary`**（`peers_queried` / `catalog_rows_updated` / `sessions_updated`），并完整声明了按需 Watch 拉取、切换非叶或未就绪分支、集群占有权拦截以及 **`POST /cluster/join`** 准入校验（作废邀请、存在活跃会话或后台任务、或无法核准入群返 **`409 Conflict`**，对端不可达返 **`503 Service Unavailable`**）相关的响应，以及 **`PATCH /cluster/self`** / **`DELETE /cluster/peers/{node_id}`** 联合多数派不可用时的 **`503 Service Unavailable`** 与 **`POST /cluster/sync`** 的 **`404` / `503` / `504`**）：

| 模块 | 路由一览 | 核心职责 |
| :--- | :--- | :--- |
| **供应商与模型**（12） | `GET/POST /providers`，`GET/PATCH/DELETE /providers/{id}`，`POST .../restore`<br>`GET/POST /models`，`GET/PATCH/DELETE /models/{id}`，`POST .../restore` | 维护多上游地址/密钥（脱敏）、模态/档位/价目表及软归档恢复 |
| **会话与账本**（7） | `GET/POST /sessions`，`GET/PATCH/DELETE /sessions/{sid}`<br>`GET /sessions/{sid}/history`，`GET /sessions/{sid}/context` | 管理会话生命周期、DAG 叶节点活跃分支切换以及基于不可变快照 + `X-History-View`/`?view=` 校验的 `items`/`active_context` 分块流式分页 |
| **排队消息**（3） | `GET/POST /sessions/{sid}/messages`，`DELETE /sessions/{sid}/messages/{mid}` | 投信（`Idempotency-Key` 幂等）、查排队状态、撤回未进上下文输入 |
| **运行、流与任务**（6） | `GET /events`，`GET /sessions/{sid}/events`<br>`PUT/DELETE /sessions/{sid}/run`，`GET .../tasks`，`DELETE .../tasks/{tid}` | 点火/软硬暂停（`?mode=soft\|hard`）、订阅两级 SSE（附带 `X-History-View`）、查停后台 JS 任务 |
| **媒体附件**（2） | `POST /attachments`，`GET /attachments/{id}` | 上传不超过 50 MiB 多模态原文件（并发与字节预算限流，满额返 `429`）及按需跨节点分块拉取、增量流式防嗅探下载 |
| **调用与计费**（3） | `GET /calls`，`GET /calls/summary`，`GET /calls/{id}` | 查询每轮上游调用明细、token、最新上下文长度与 `cost_nano_usd` 费用 |
| **MCP 与 OAuth**（10） | `GET/POST /mcp/servers`，`GET/PATCH/DELETE /mcp/servers/{id}`，`POST .../test`<br>`GET/DELETE .../oauth`，`POST .../oauth/login`，`POST .../oauth/callback` | 注册传输与 `shared` 同步开关、并发 4 体检探测、启动/手动完成/撤销 OAuth 2.1 |
| **集群与同步**（6） | `GET /cluster`，`PATCH /cluster/self`，`POST /cluster/invite`，`POST /cluster/join`<br>`DELETE /cluster/peers/{node_id}`，`POST /cluster/sync` | 查询集群状态与成员、改本机名称/描述/两阶段联合多数派调角色（`voter`/`edge`，不可用返 `503`）、邀请/经 `JOIN_ALPN`（`mygo/join/1`）串行准入校验后加入（作废或拒接返 `409`，不可达返 `503`）/移出节点、触发手动同步（`404`/`503`/`504`） |

### 三条不同的路由出口（[src/main.rs](../src/main.rs#L612-L650)、[src/api.rs](../src/api.rs#L54-L85)）

虽然核心业务逻辑都在同一套 `api::routes` 里，但程序故意把**外网/本机 TCP**、**本机 Unix Socket** 和 **Codemode 内部自省**三条入口的权限与页面切得清清楚楚：

| 入口 | 业务 API (`/sessions` 等) | WebUI (`/`、`/ui/*`) | `/openapi.json` | `/docs` 页面 | `/_runtime`（运行时地址与 Token） |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **TCP 监听** | **必须带 Bearer Token**（`authorize_tcp` 拒绝多 `Authorization` 头，大小写不敏感 `Bearer` + `subtle::ConstantTimeEq` 恒定时间比对） | 放行（免 Token，因为页面 JS 自己弹输入门禁） | **仅当监听在回环地址**（如 `127.0.0.1`）时挂出且免 Token；非回环地址直接 404 | 同左，仅回环地址可见 | 不挂载（无论有无 Token 均 404） |
| **Unix Socket**（限 Unix） | **免 Token 放行**（靠文件权限 `0600` 拦人） | 放行 | 不挂载 | 不挂载 | **挂载且免 Token**（带 `Cache-Control: no-store` 返回 `{ pid, listen, token }`，供 `service status` 查询） |
| **进程内 `mygo.request`** | **免 Token 直调**（不经网络栈） | 不挂载 | **挂载且免 Token**（供脚本查全部接口） | 不挂载 | 不挂载 |

`/docs` 页面（[src/docs.rs](../src/docs.rs)）是一个极简 HTML 加载页，以 SRI 子资源完整性校验（`credentials: 'omit'`）从 jsDelivr（8 秒超时自动回退 npmmirror 20 秒）加载 Scalar API Reference UI 渲染 `/openapi.json`，同时禁用所有开发者工具与遥测上报。

---

## 零膨胀内嵌的 Preact WebUI

MyGo-Bot 的前端代码全在 `web/` 下，用到了极致克制的技术栈：**运行时唯一依赖只有 `preact@11`，开发期只有严格配置下的 `typescript`**（无多余 UI 框架和第三方状态库）。

### 1. 构建、压缩与强 ETag 嵌入（`web/build.mjs` & `src/webui.rs`）

执行 `bun run build` 先跑一遍 `tsc --noEmit` 全量类型检查，随后：
- Bun 把 `web/src/main.tsx` 打包压缩成 ESM，再用 `gzip -9` 压进 **`src/webui/app.js.gz`（约 45.4 KiB / 46,538 字节）**；
- 自定义状态机压缩 CSS（保留引号内容、去注释、去尾分号），压进 **`src/webui/app.css.gz`（约 6.6 KiB / 6,736 字节）**；
- `index.html` 折叠标签间空白生成 **`src/webui/index.html`（898 字节）**，开头直接内联了一小段同步 `<script>`，在浏览器首帧绘制前读取 `localStorage.mygo_theme` 和系统深浅色偏好把 `dataset.theme` 设好，彻底杜绝深色模式下切白闪屏（FOUC）。

这三份静态资源在 Rust 编译时通过 `include_bytes!` 直接焊进最终二进制里。`GET /`、`GET /ui/app.js` 和 `GET /ui/app.css` 每次按 64 位 FNV-1a 算法算好强 ETag（`"16位十六进制"`），客户端带 `If-None-Match` 命中直接回 `304 Not Modified`；`.js` 和 `.css` 直接附带 `Content-Encoding: gzip` + `Vary: Accept-Encoding` 原样吐出压缩字节。

### 2. 鉴权门禁与 `fetch` 驱动的 SSE 客户端（`web/src/api.ts`、`web/src/main.tsx`）

- **自动绕过与一次性 URL Token**：浏览器一打开页面，先把地址栏里的 `/#token=...`（如果有的话）捞进 `localStorage.mygo_token` 并立刻用 `history.replaceState` 擦掉 URL 痕迹，接着拿着手头的 Token 去试探 `GET /cluster`。如果是通过 Unix Socket 或免鉴权反代打开的，空 Token 也能直接返回 `200`、自动跳过密码门禁进主界面；如果返回 `401` 就弹出 `TokenGate` 登录框；运行途中任何接口或 SSE 遇到 `401` 立刻清掉本地 Token 退回门禁。
- **为什么手写 SSE 客户端**：浏览器原生的 `EventSource` 规范不允许自定义 `Authorization: Bearer` 请求头，所以前端用 `fetch` 自己读取 `ReadableStream` 逐行解码 `event:` 与多行 `data:`，自带 `500ms -> 8s` 指数退避重连。附件缩略图则走带 Token 的 `fetch` 转成 Blob URL，放进容量 **48 项的 LRU 缓存**里，淘汰时自动调 `URL.revokeObjectURL` 释放内存。

### 3. 前端状态同步的四大防错机关（`web/src/state.ts`）

别看前端只有几百行状态管理，它专门针对异步并发和断线重连修好了最常见的四类 UI 幻觉：

| 机关 | 它只管哪件事、省掉会出什么乱子 |
| :--- | :--- |
| **1. 全局快照与增量缓冲（`activeBuffer` + `latestSeq`）** | 全量拉取 `GET /sessions` 还在半路时，全局 SSE 推来的 `session.created`/`status` 以及本地已经确认成功的 `onLocalPatch`/`runRefresh` 更新，都会先就地生效并记进临时缓冲，等全量快照回来再按序（本地改动走 `applyLocalPatch`）重放盖上去。省掉它，拉列表那几百毫秒里刚发生的事件或刚改好的标题/模型就会被旧快照抹掉。 |
| **2. 单会话版本号 + 永久墓碑 `deletedIds` + 回拉退避** | 每次调 `PATCH /sessions/{sid}` 前先拍下该会话此刻的版本号 `baseVersion`。如果请求飞在半路时有更新的 SSE 事件把版本号推高了，或者该会话已经被删进 `deletedIds` 墓碑集里——回包绝对不允许直接盖上去把旧数据覆盖回来、更不许把删掉的会话“复活”！冲突时自动改调一次带 `1s..8s` 退避的 `GET /sessions/{sid}` 对齐权威状态（`404` 立刻转为删除）。 |
| **3. 立即首屏、`knownView` 连续性校验与全量重置（`start` + `gapOpen` + `loadOlder` + `onReset`）** | 切进会话不等 SSE `ready`，`SessionSync.start()` 立刻先拉最新 50 条（`?tail=true&limit=50`）并同时记下已同步游标 `syncedThroughId` 与响应头里的 **`knownView`**（`X-History-View`；`connectSse.onOpen` 也会顺手把 `X-History-View` 交给 `onReady(view)`）；向上翻页（`loadOlder` -> `fetchBefore`）与缺口补拉（`fetchAfter`）每次都会带上 **`?view=${knownView}`** 交给服务端校验，统一配着 `AbortController` 管理。只要连接上 `ready` 或收到 `resync`，先核对随信带来的 `view` 是否与 `knownView` 一致（不一致当场 `onReset()`），再把 `gapOpen` 置 `true` 锁住游标：期间新到达的 `item.saved` 先渲染上屏但不推高 `syncedThroughId`，等 `?after=...&limit=200&view=...` 补齐缺口再合拢。只要遇到任何分页返回的 `view` 缺失/失配、后端返回 **`409 Conflict`**、后端推来 **`session.reset`**，或本机刚调 `PATCH /sessions/{sid}` 切换完 `active_head`——`createSessionSync` 就会立刻调 `onReset()` 取消在途翻页请求、递增 `resetGen`（防在途旧翻页结果回写）、清零 `syncedThroughId` 与 `knownView`、置 `replaceNext = true` 重新从 `?tail=true` 拉取最新一页并调用 `onReplaceItems` 整表替换屏幕列表，彻底抹去已切出分支或乱序混页的旧消息；同时同一会话的 sidecar 刷新合并为最多“一个在途 + 一个尾随”，用量只按屏幕上的 `llm_call_id` 按需单条拉取。 |
| **4. 草稿防幽灵重现（`closedCalls` / `closedItems`）** | 当某条流式回答或工具调用已经被 `item.saved` 正式落盘、或者收到 `stream.clear` / `attempt.retry` 时，不仅立刻清掉屏幕上的流式草稿，还把对应的 `llm_call_id` 或 `call_id:output_index` 记进关闭集合；收到 `stream.clear` 时更会借助 UUIDv7 `CallId` 的单调递增序把所有不晚于该 ID 的旧草稿一并清盘并封禁，且对重连收到的 `stream.snapshot` 按 `closedItems` 逐项过滤，把任何迟到的增量碎片或旧快照彻底拦在门外，防止同一句话闪回屏幕两次。 |

### 4. 四块界面能力速览

- **侧边栏（`Sidebar.tsx`）**：按 `updated_at` 倒序排列，状态圆点一目了然（`idle`/`running` 呼吸灯/`pausing`/`paused`/带 `last_error` 红灯），运行中、暂停中以及带有远端/后台占有权（`runner_node_id != null`）的会话一律禁用/隐藏删除按钮（附带占用提示），删会话需 3 秒内二次确认；窄屏自动折叠为抽屉；底部保留 **Cluster** 入口，可随时查看节点角色与集群成员。
- **时间线（`Timeline.tsx`、`markdown.tsx`）**：把 `withdrawal` 折叠并直接给对应用户消息画删除线；把同一组 `function_call` 与 `function_call_output` 配对成可展开工具卡片（专门把 `codemode` 的 `code`/`trigger_code` 高亮展示，也支持展示工具返回的图片音视频附件）；自研零依赖 Preact JSX Markdown 解析器——**全程零 `dangerouslySetInnerHTML`**，所有链接在行内通过单趟线性栈扫描配对括号提取目标地址并严格经 `new URL` 校验只放行显式 `http://`、`https://`、`mailto:`（彻底杜绝流式未闭合长链接引发正则回溯与平方级扫描卡死），且故意屏蔽 `![img](...)` 外链防跟踪像素；每条回复卡片和底部汇总栏统一按后端 `uncached + cache_read + cache_write` 口径计算总输入与缓存命中率。
- **输入框与顶栏（`Composer.tsx`、`TopBar`）**：按会话独立保存未发草稿与粘贴/拖拽上传的待发附件；当集群存在多台节点时，输入框工具条直接出现**执行节点选择器**（可按当前集群在线成员指定由哪台机器跑下一轮，点击 Run 携带 `{ node }` 发出；当会话已由某节点占有时也会显示当前占有者，只读展示或按需切回）；由于后端强制要求每场会话都带有有效模型，新建会话优先取 `localStorage`（`mygo_last_model_id` / `mygo_last_reasoning_effort`）里仍未归档的模型、否则自动回落到首个可用模型（若完全没有可用模型则提示并直接打开 `Settings -> Models`）；提交前锁定内容快照复用或生成 `Idempotency-Key`（4xx 报错重置 Key，网络/5xx 报错保留 Key 防重投），而且只有当用户在请求飞出期间**没继续打字改文本框（`textVersion` 未变）**时才清空输入，绝不吞掉用户刚续写的新字；上方只要会话在跑、在暂停或 `runner_node_id` 非空就持续轮询并展示可折叠的任务栏（每 2 秒经控制面/执行面转发拉取，带 `[node]` 前缀展示 `running_commands` 实时终端尾部与 `Kill` 按钮）；顶栏在 `runner_node_id` 非空时展示 `@node` 实时运行徽标，**当会话在 DAG 上存在多条并行分支（`(session.branches?.length ?? 0) > 1`）时还会弹出 Branch 分支切换下拉框**（按 `#序号 · 深度 · 节点 · node_id 前缀` 展示，运行中/暂停中禁用，切换时调 `PATCH /sessions/{sid}` 传入 `active_head` 并触发历史重拉），旁边集中放会话改名、模型切换（去掉“无模型”选项，切模型主动清零 `reasoning_effort`）、推理档位选择和基于 `last_input_tokens / context_window_tokens` 计算的上下文用量与花费统计。
- **设置弹窗（`Settings.tsx`）**：分 `Models`（去掉全局默认开关，保留显示归档、供应商密钥安全编辑、模型能力与定价 JSON）、`MCP`（包含 **`shared` 集群同步开关**与共享角标，编辑时屏蔽未改动的查询串/Headers/Args/Env/OAuth 防误覆盖且无改动不发 PATCH；关闭同步前显式提醒将克隆为新本地服务并清理他机旧共享副本；OAuth 既提供一键弹窗打开授权，也贴心留了粘贴远程回调 URL 的 `Submit Callback` 框方便跨机部署）、`Cluster`（修改本机节点名称与描述、分段切换 **`Voter` / `Edge`** 节点角色、复制 Node ID、一键全量同步、生成邀请码或输入邀请码加入集群——若已有同伴触发 `409` 会弹出带警告的 **`Force Switch Cluster`** 按钮二次确认以 `force: true` 切换、查看同伴在线/离线状态、`Voter`/`Edge` 角色徽标及移除成员）、`Usage`（按会话筛调用费和历史流水）与 `General`（主题、`/docs` 链接、登出）。

---

**上一篇**：[06 · 外部工具扩展：MCP 协议、双传输与 OAuth 2.1](06-mcp-and-oauth.md) · **返回目录**：[总览](README.md) · **下一篇**：[08 · 部署运维、压测验证、瘦身发布与评测体系](08-deployment-testing-and-tooling.md)
