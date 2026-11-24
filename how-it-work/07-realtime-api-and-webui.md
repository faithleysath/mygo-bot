# 07 · 实时流、HTTP API、内置文档与 WebUI

前面六篇把后端的账房、循环、沙箱和外部扩展全立起来了。可真把系统摊到桌面上让人用，还得解决三件事：

1. **屏幕怎么既能看到模型一个字一个字往外吐（打字机流），又能保证几十个浏览器窗口一起围观时一条不漏、慢窗口还不拖垮主循环？**
2. **整套 HTTP 接口到底怎么分区暴露、在哪道门验 Token、在哪道门放行？**
3. **不想另外架一套笨重的前后端工程，怎么只靠同一个二进制，直接给用户端出轻量、零闪屏、不会因弱网乱序覆盖的 WebUI 和 API 文档？**

---

## 两级实时 SSE 流：围观再多也不堵门

早期架构只把写进 SQLite 的成品 `item` 往外推，人坐在屏幕前要干等模型整段话吐完才能看见内容。为了让界面跟聊天软件一样实时跳字，同时又绝不污染只增不删的账本，[src/events.rs](../src/events.rs) 和 [src/model.rs](../src/model.rs#L79-L127) 搭了一套**“两级广播 + 全局统一编号”**的实时流中心（`events::Hub`）：

1. **先分两股流接口**：
   - **全局流 `GET /events`**：容量 **256** 的广播通道。一连上先推 `ready`（`{}`），之后整个进程任何会话新建（`session.created`）、改配置/改标题（`session.updated`）、切状态（`session.status`）或删除（`session.deleted`），侧边栏靠它实时刷新；谁掉队了收到 `resync`。
   - **单会话流 `GET /sessions/{sid}/events`**：每个新连上的订阅者，都会同时接上**两条独立广播通道**：
     - **持久通道（记账员拥有，容量 64）**：只在事务 `COMMIT` 之后发成品事件——`item.saved`（完整账目）、`session.status`、`session.updated`，外加不进数据库的 `attempt.retry`。**压根没人订阅时，记账员连回查数据库组装 JSON 的活都直接省掉**；有人听时，一笔新账只组装一份 `Arc<str>` 丢进槽里大家共享引用；要是某个慢窗口积压超过 64 条，广播槽直接冲它扔一条 **`resync`** 让它自己去翻 `/history` 补课，绝不回头等它。
     - **临时草稿通道（`Hub` 拥有，容量 256）**：模型在上游刚吐字时，经过一个 **`DeltaBatcher`**（每攒满 **30 毫秒**或者满 **256 字节**就打包推一次，不按单字符轰炸广播槽）发来 `item.started`、`item.delta`（区分 `text`/`reasoning`/`arguments`）和收尾清空用的 `stream.clear`。同时内存里维持着一个每字段封顶 **256 KiB** 的 `ActiveDraft` 影子草稿——所以你半路刷新页面或刚点进一个正在生成的会话，一连上紧跟着 `ready` 就会收到一份 **`stream.snapshot`**，立刻把屏幕上已经吐到一半的话续上，不用等这轮说完！

2. **两股通道怎么保证“碎片不跑到成品后面”？**
   既然打字机碎片走 `Hub` 的 256 通道、成品 `item.saved` 走记账员的 64 通道，万一两路消息在 `tokio::select!` 里抢跑，用户刚看到完整的 `item.saved` 落盘卡片，下一毫秒一条迟到的 `item.delta` 才冒出来怎么办？

   解决招数非常漂亮——**全进程共抢一个原子递增序号 [`NEXT_SEQ`](../src/events.rs#L25-L43)**（[推送协调循环](../src/events.rs#L313-L436)）：
   - 每一条发往持久通道和临时通道的通知，在放进槽里的那一瞬间都领一个唯一的 `seq`；
   - 连入 SSE 时顺手记下此刻快照已经覆盖到的线 **`min_seq`**，所有比 `min_seq` 老的碎片直接丢弃，免得快照和碎片重复叠字；
   - 推送协程每收到一条持久事件 `d` 时，先把 `take_older_deltas` 排干——把临时通道里所有序号 `seq < d.seq` 的碎片先吐干净，**然后再吐 `d`**！如果慢窗口在**持久通道**掉队了，清掉缓存在手里的草稿、吐一条 `resync` 让前端去拉历史重对齐；如果在**临时通道**掉队，静默跳过旧草稿继续收新的即可，绝不耽误主账。

---

## HTTP API 全景、三条路由面与内置 OpenAPI 文档

整套系统一共对外暴露 **29 条路径、45 个操作**（完整契约编译期嵌在 [src/openapi.json](../src/openapi.json) 里，OpenAPI `3.1.0`）：

| 模块 | 路由一览 | 核心职责 |
| :--- | :--- | :--- |
| **全局配置**（2） | `GET/PATCH /config` | 读写新建会话继承的默认模型与推理强度 |
| **供应商与模型**（12） | `GET/POST /providers`，`GET/PATCH/DELETE /providers/{id}`，`POST .../restore`<br>`GET/POST /models`，`GET/PATCH/DELETE /models/{id}`，`POST .../restore` | 维护多上游地址/密钥（脱敏）、模态/档位/价目表及软归档恢复 |
| **会话与账本**（7） | `GET/POST /sessions`，`GET/PATCH/DELETE /sessions/{sid}`<br>`GET /sessions/{sid}/history`，`GET /sessions/{sid}/context` | 管理会话生命周期与 `items`/`context_items` 分块流式分页 |
| **排队消息**（3） | `GET/POST /sessions/{sid}/messages`，`DELETE /sessions/{sid}/messages/{mid}` | 投信（`Idempotency-Key` 幂等）、查排队状态、撤回未进上下文输入 |
| **运行、流与任务**（6） | `GET /events`，`GET /sessions/{sid}/events`<br>`PUT/DELETE /sessions/{sid}/run`，`GET .../tasks`，`DELETE .../tasks/{tid}` | 点火/软硬暂停（`?mode=soft\|hard`）、订阅两级 SSE、查停后台 JS 任务 |
| **媒体附件**（2） | `POST /attachments`，`GET /attachments/{id}` | 上传不超过 50 MiB 多模态原文件及防嗅探下载 |
| **调用与计费**（3） | `GET /calls`，`GET /calls/summary`，`GET /calls/{id}` | 查询每轮上游调用明细、token、最新上下文长度与 `cost_nano_usd` 费用 |
| **MCP 与 OAuth**（10） | `GET/POST /mcp/servers`，`GET/PATCH/DELETE /mcp/servers/{id}`，`POST .../test`<br>`GET/DELETE .../oauth`，`POST .../oauth/login`，`POST .../oauth/callback` | 注册传输配置、并发 4 体检探测、启动/手动完成/撤销 OAuth 2.1 |

### 三条不同的路由出口（[src/main.rs](../src/main.rs#L503-L541)、[src/api.rs](../src/api.rs#L51-L78)）

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
- Bun 把 `web/src/main.tsx` 打包压缩成 ESM，再用 `gzip -9` 压进 **`src/webui/app.js.gz`（约 24.7 KiB）**；
- 自定义状态机压缩 CSS（保留引号内容、去注释、去尾分号），压进 **`src/webui/app.css.gz`（约 3.5 KiB）**；
- `index.html` 折叠标签间空白生成 **`src/webui/index.html`（851 字节）**，开头直接内联了一小段同步 `<script>`，在浏览器首帧绘制前读取 `localStorage.mygo_theme` 和系统深浅色偏好把 `dataset.theme` 设好，彻底杜绝深色模式下切白闪屏（FOUC）。

这三份静态资源在 Rust 编译时通过 `include_bytes!` 直接焊进最终二进制里。`GET /`、`GET /ui/app.js` 和 `GET /ui/app.css` 每次按 64 位 FNV-1a 算法算好强 ETag（`"16位十六进制"`），客户端带 `If-None-Match` 命中直接回 `304 Not Modified`；`.js` 和 `.css` 直接附带 `Content-Encoding: gzip` + `Vary: Accept-Encoding` 原样吐出压缩字节。

### 2. 鉴权门禁与 `fetch` 驱动的 SSE 客户端（`web/src/api.ts`、`web/src/main.tsx`）

- **自动绕过与一次性 URL Token**：浏览器一打开页面，先把地址栏里的 `/#token=...`（如果有的话）捞进 `localStorage.mygo_token` 并立刻用 `history.replaceState` 擦掉 URL 痕迹，接着拿着手头的 Token 去试探 `GET /config`。如果是通过 Unix Socket 或免鉴权反代打开的，空 Token 也能直接返回 `200`、自动跳过密码门禁进主界面；如果返回 `401` 就弹出 `TokenGate` 登录框；运行途中任何接口或 SSE 遇到 `401` 立刻清掉本地 Token 退回门禁。
- **为什么手写 SSE 客户端**：浏览器原生的 `EventSource` 规范不允许自定义 `Authorization: Bearer` 请求头，所以前端用 `fetch` 自己读取 `ReadableStream` 逐行解码 `event:` 与多行 `data:`，自带 `500ms -> 8s` 指数退避重连。附件缩略图则走带 Token 的 `fetch` 转成 Blob URL，放进容量 **48 项的 LRU 缓存**里，淘汰时自动调 `URL.revokeObjectURL` 释放内存。

### 3. 前端状态同步的四大防错机关（`web/src/state.ts`）

别看前端只有几百行状态管理，它专门针对异步并发和断线重连修好了最常见的四类 UI 幻觉：

| 机关 | 它只管哪件事、省掉会出什么乱子 |
| :--- | :--- |
| **1. 全局快照与增量缓冲（`activeBuffer` + `latestSeq`）** | 全量拉取 `GET /sessions` 还在半路时，全局 SSE 推来的 `session.created`/`status` 先一边就地更新一边记进临时缓冲，等全量快照回来再把缓冲按序重放盖上去。省掉它，拉列表那几百毫秒里刚发生的事件就会被旧快照抹掉。 |
| **2. 单会话版本号 + 永久墓碑 `deletedIds` + 回拉退避** | 每次调 `PATCH /sessions/{sid}` 前先拍下该会话此刻的版本号 `baseVersion`。如果请求飞在半路时有更新的 SSE 事件把版本号推高了，或者该会话已经被删进 `deletedIds` 墓碑集里——回包绝对不允许直接盖上去把旧数据覆盖回来、更不许把删掉的会话“复活”！冲突时自动改调一次带 `1s..8s` 退避的 `GET /sessions/{sid}` 对齐权威状态（`404` 立刻转为删除）。 |
| **3. 历史缺口闭合锁（`gapOpen`）** | 刚切进会话先拉最新 50 条（`?tail=true&limit=50`），记下已同步游标 `syncedThroughId`。一旦断线重连 `ready` 或收到 `resync`，立刻把 `gapOpen` 置 `true` 锁住游标：期间新到达的 `item.saved` 可以先渲染上屏给人看，但**绝不推高 `syncedThroughId`**，一直等到 `?after=...&limit=200` 把断线期间漏掉的所有历史一页页补齐合并，才把缺口合拢！ |
| **4. 草稿防幽灵重现（`closedCalls` / `closedItems`）** | 当某条流式回答或工具调用已经被 `item.saved` 正式落盘、或者收到 `stream.clear` / `attempt.retry` 时，不仅立刻清掉屏幕上的流式草稿，还把对应的 `llm_call_id` 或 `call_id:output_index` 记进关闭集合，把任何迟到的增量碎片或旧快照彻底拦在门外，防止同一句话闪回屏幕两次。 |

### 4. 四块界面能力速览

- **侧边栏（`Sidebar.tsx`）**：按 `updated_at` 倒序排列，状态圆点一目了然（`idle`/`running` 呼吸灯/`pausing`/`paused`/带 `last_error` 红灯），运行中和暂停中隐藏删除按钮，删会话需 3 秒内二次确认；窄屏自动折叠为抽屉。
- **时间线（`Timeline.tsx`、`markdown.tsx`）**：把 `withdrawal` 折叠并直接给对应用户消息画删除线；把同一组 `function_call` 与 `function_call_output` 配对成可展开工具卡片（专门把 `codemode` 的 `code`/`trigger_code` 高亮展示，也支持展示工具返回的图片音视频附件）；自研零依赖 Preact JSX Markdown 解析器——**全程零 `dangerouslySetInnerHTML`**，所有链接严格用 `new URL` 校验只放行显式 `http://`、`https://`、`mailto:`，且故意屏蔽 `![img](...)` 外链防跟踪像素。
- **输入框与顶栏（`Composer.tsx`、`TopBar`）**：按会话独立保存未发草稿与粘贴/拖拽上传的待发附件；提交前锁定内容快照复用或生成 `Idempotency-Key`（4xx 报错重置 Key，网络/5xx 报错保留 Key 防重投），而且只有当用户在请求飞出期间**没继续打字改文本框（`textVersion` 未变）**时才清空输入，绝不吞掉用户刚续写的新字；上方在有活跃任务时展示可折叠的任务栏（每 2 秒轮询，直接展示 `running_commands` 实时终端尾部与 `Kill` 按钮）；顶栏集中放会话改名、模型切换（切模型主动清零 `reasoning_effort`）、推理档位选择和基于 `last_input_tokens / context_window_tokens` 计算的上下文用量与花费统计。
- **设置弹窗（`Settings.tsx`）**：分 `Models`（全局默认、显示归档、供应商密钥安全编辑、模型能力与定价 JSON）、`MCP`（编辑时屏蔽未改动的查询串/Headers/Args/Env/OAuth 防误覆盖且无改动不发 PATCH；OAuth 既提供一键弹窗打开授权，也贴心留了粘贴远程回调 URL 的 `Submit Callback` 框方便跨机部署）、`Usage`（按会话筛调用费和历史流水）与 `General`（主题、`/docs` 链接、登出）。

---

**上一篇**：[06 · 外部工具扩展：MCP 协议、双传输与 OAuth 2.1](06-mcp-and-oauth.md) · **返回目录**：[总览](README.md) · **下一篇**：[08 · 部署运维、压测验证、瘦身发布与评测体系](08-deployment-testing-and-tooling.md)
