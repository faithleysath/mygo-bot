# 06 · 外部工具扩展：MCP 协议、双传输与 OAuth 2.1

模型手头始终只有那 6 个 `codemode*` 工具，那如果想接入联网搜索、数据库查询、内部工单系统这类外部 MCP（Model Context Protocol）服务，难道要把每个外部工具都塞进模型工具列表里吗？

如果真那么干，挂上五个 MCP 服务、上百个工具的 schema 就先把上下文挤爆了；而且模型调一个远端工具拿回几兆结果再写回本地，全得经大模型吐字中转，慢得要命。MyGo-Bot 的做法完全顺着第 4 篇的逻辑：**把 `mcp.*` 也做成递进 QuickJS 车间里的基础原语。**

---

## 先看脚本里怎么找到并调用远端工具

拿一次实际的联网搜索来说。用户问了 SQLite 3.50.0 的发布日期和两个新函数的名字。模型手里仍是那 6 个工具，它先调 `codemode`，在脚本里问一句 `await mcp.servers()`。宿主直接从 SQLite 的 `mcp_servers` 表读出已启用服务的名字和用途，这一步根本不用联网。模型看到 `exa` 能搜网页，才用 `await mcp.tools('exa')` 看它有哪些工具、每个工具要填什么参数（目录大时可以传 `{ query: 'search' }` 筛选，或把上页 `nextCursor` 作为 `cursor` 传回去继续翻）。

Exa 的 `web_search_exa` 要求脚本用 `query` 写想找什么，用 `objective` 写希望从结果里核实什么。摸清参数后，模型在脚本里直接调用：

```js
const result = await mcp.call('exa', 'web_search_exa', {
    query: 'SQLite 3.50.0 official release notes Unicode escape functions',
    objective: 'Find the release date and both new SQL function names in the official notes.',
    numResults: 5,
})
if (result.isError) throw new Error(result.content.map(block => block.text).join('\n'))
return result.structuredContent ?? result.content
```

Exa 返回的第一条就是 SQLite 官方发布说明，模型据此答出 2025 年 5 月 29 日、`unistr()` 和 `unistr_quote()`。所有中间查询、筛选和容错全在车间里就地消化完，最后只挑干货交还给外层的 `codemode` 账本。

看懂了表面用法，接下来从底往上把整套 MCP 引擎（`src/mcp/` 下的全部 14 个模块）拆开看透。

---

## 配置保护与按会话懒建的连接池

### 配置模型与脱敏守门（`src/mcp/config.rs`）

通过 `POST /mcp/servers` 和 `PATCH /mcp/servers/{id}` 登记外部服务，存进 `mcp_servers` 表：
- **两大传输模式**：`streamable-http`（配 `url`、可选静态请求头 `headers`、可选 `oauth` 配置）和 `stdio`（配本地 `command`、`args`、`env`、`cwd`）。两种模式的字段互斥，切换 `transport` 时程序会自动清空旧模式下的全部传输字段；
- **公共控制字段**：`name`（全局唯一、新建后**不可改名**）、`description`、`enabled`（禁用后就算脚本背出了服务名和工具名也发不起新调用）、`shared`（布尔开关，默认 `false`，决定该服务是否同步到集群其他节点）、`allowed_tools`（`null` 全放行、`[]` 全禁止，或精确工具名白名单）、`timeout_ms`（默认 **60 秒**，合法范围 1 毫秒到 24 小时）、单调递增的 **`revision`**；
- **三道凭据防护**：
  1. **只写不读的投影**：`GET /mcp/servers` 返回的 `McpServerInfo` 绝不回显静态 `headers` 内容、`oauth` 凭据、stdio 的 `args`/`env` 明文，连 `url` 里的 `?query` 都会剥掉只留 `has_url_query: true`（防 URL 参数里带 API Key 泄密）；
  2. **禁覆盖协议头**：HTTP 模式禁止在静态 `headers` 里乱填 `mcp-*`、`accept`、`content-type`、`host`、`content-length`、`transfer-encoding`、`connection`、`last-event-id`；一旦启用 OAuth，也不许同时写死静态 `Authorization` 头；
  3. **防换目标偷走旧密钥**：在 `PATCH` 时只要改动了 HTTP `url` 的 origin（协议+域名+端口）却没显式重设原本存在的 `headers` 或 `oauth`，直接 `400` 拦下，防旧凭据被意外发往新主机。

### 懒连接、配置共用与会话隔离池（`src/mcp/mod.rs`）

**配置全进程共用，连接按会话单独建。** 登记服务时程序压根不连网；直到某个会话里的脚本第一次调 `mcp.tools` 或 `mcp.call` 等操作，记账员线程管理的 **`Connections` 连接池**才按 **`(session_id, server_id)`** 现开一条懒连接。同一会话后面的新脚本复用这条连接，别的会话则另开自己的连接。

连接池全进程上限 **64 条**（`MAX_CONNECTIONS`）：满了优先驱逐最近最少使用且无人持有的空闲槽位；如果 64 条全在忙着干活，直接返回 `409 Conflict`（`"MCP connection limit reached"`）。后台还有一条每 **60 秒**巡检一次的 `reap_idle` 协程，把空闲满 **10 分钟**或已坏掉的连接清掉。

修改配置、禁用、删服务或 OAuth 状态变动时，记账员当场调用 `forget(id)` 把老槽位标成 retired 并从池子里摘掉——**已经在途的调用按原配置收完尾，新进来的调用立刻按新配置另建连接**；删除会话或服务优雅退出时也会顺手清理相关槽位。

---

## 双传输链路：先共通，再分叉

不管是远程 HTTP 服务还是本地命令行子进程，对上都统一封装成一套 `Client` 抽象——**上层拿连接、建握手、挂超时和防刷屏缓存走同一条路，只在最底下“怎么把一条 JSON-RPC 帧送过去并拿回来”这一步分叉**：

| 维度 | `streamable-http`（[src/mcp/client.rs](../src/mcp/client.rs)） | `stdio`（[src/mcp/stdio.rs](../src/mcp/stdio.rs)） |
| :--- | :--- | :--- |
| **建连与环境隔离** | 独立 `reqwest` POST，不跟重定向、不自动重试 | `spawn_blocking` 启动子进程；Windows **拒 `.cmd`/`.bat`**；先 `env_clear()` 只继承最少必要系统变量（如 `PATH`/`HOME` 等），`stderr` 直接接**空设备**防日志泄密和写满管道死锁 |
| **协议握手协商** | **两阶段握手**：先试现代 **`server/discover`**（支持 `2026-07-28` 等 5 个版本，遇 `-32022` 按服务端列表降级）；不认则回退经典 **`initialize` + `notifications/initialized`** | 经典 `initialize` + `notifications/initialized`（或按协议版本协商） |
| **会话与协议头** | 跟踪服务端返回的 `Mcp-Session-Id`（遇 `404` 判会话过期立刻淘汰连接）；退场发 HTTP `DELETE`（`Drop` 最多等 5 秒，`close` 等 1 秒）。`2026-07-28+` 协议自动带 `_meta`、`Mcp-Method`、`Mcp-Name`，还能按工具 `inputSchema` 声明的 **`x-mcp-header`** 抽出参数塞进 `Mcp-Param-*` 请求头（RFC `tchar` 校验 + `=?base64?...?=` 编码，缓存 512 个工具 schema） | 无 HTTP 头；通过 **Unix 进程组（`process_group(0)` + `waitid(NOWAIT)`）** 或 **Windows `JobObject`（`CREATE_SUSPENDED` 挂起绑作业再恢复线程防孙子逃逸）** 关进笼子 |
| **消息收发与背压** | 每个 `POST` 可回普通 `application/json` 或 **`text/event-stream` SSE**（16 MiB 线限额）；流里自动答复服务端 `ping`，收到 `list_changed` 通知刷新缓存纪元；不建单独 `GET` 监听流 | 换行分隔 JSON-RPC（16 MiB 单行上限）；单写协程配 **128 容量通道**。发请求前原子**预占 2 个槽位**：1 槽写请求、1 槽留给 **`PendingGuard`**——就算 stdin 被堵满，超时或取消时析构函数也能立刻把 `notifications/cancelled` 稳稳排在原请求后面发出去！ |
| **列表缓存与退场** | 仅当没开 `resources` 且不会漏掉通知的 `listChanged` 时，用最多 **4 条、5~30 秒 TTL** 的短缓存缓存列表（`cacheScope == "private"` 或 `ttlMs == 0` 跳过） | 能实时收到 `notifications/*/list_changed` 推送通知；退场顺序：关停写协程（150ms）-> 等领头进程（150ms）-> 杀光整组/作业 -> 统一用 `TransportClosed` 唤醒挂起请求 |

---

## 全量能力、内容打包、错误脱敏与体检

### 1. 八个 JS 接口与翻页保护

车间里的 `mcp` 对象不止能调工具，覆盖 MCP 三大原语的全部 8 个方法（[src/mcp/capabilities.rs](../src/mcp/capabilities.rs)）：
- `mcp.servers()`、`mcp.tools(server, { query?, cursor? })`、`mcp.call(server, tool, args?)`
- `mcp.resources(server, { cursor? })`、`mcp.resourceTemplates(server, { cursor? })`、`mcp.readResource(server, uri)`
- `mcp.prompts(server, { cursor? })`、`mcp.getPrompt(server, name, args?)`（参数值要求全是字符串）

其中 `mcp.tools` 在宿主层做了一次强防御：单次最多自动翻 **64 页**、单页超过 **4096 个**原始工具直接报错、用哈希集合记下看过的 `cursor`（最大 16 KiB）防止死循环、先按 `allowed_tools` 白名单再按 `query` 模糊过滤、并顺手把工具 `inputSchema` 的顶层属性名记进 `slot.public_fields` 白名单。

### 2. 二进制打包与不支持扩展（`src/mcp/content.rs`）

当远端返回图片、音频、视频或二进制资源时，宿主先拦截掉 MyGo 暂不支持的 `input_required` 和服务端后台 `task` 扩展（`resultType` 必须是缺省或 `"complete"`）。然后把响应里所有解码出来的 Base64 字节紧凑拼成一整块连续缓冲区 **`BinaryData`**，再通过 `bootstrap.js` 里的 `mcpResult` 精准地只把受信任内容槽位里的 `[start, end]` 偏移量替换成 `bytes.subarray(start, end)`（`structuredContent` 和 `resource_link` 绝不篡改）。

- 具体合法的 `image/*`、`audio/*`、`video/*` 块会变成跟第 4 篇完全同款的块，脚本可以直接 `return` 或 `emitEvent` 喂给多模态模型；
- 其余 MIME 类型（如 PDF 或 `application/octet-stream`）变成 **`{ type: 'blob', mime_type, data: Uint8Array }`**，供脚本就地分析或用 `fs.write` 落盘，禁止直接扔进模型输出；
- 整套解码受限于 **16 MiB** 线协议总长（`WIRE_LIMIT`）、**2 MiB** 元数据 JSON 壳（`BYTE_LIMIT`）、单块 **50 MiB** 与单次回复 **52 MiB** 预算。

### 3. 错误三层分治、脱敏与“结果未知”语义（`src/mcp/errors.rs`）

外部 MCP 报错绝不瞎处理，分成三层看待：
1. **工具业务失败（`isError: true`）**：通信正常完成，服务明确说这单业务失败，原因全在 `result.content` 里，交给脚本自己 `if (result.isError)` 决定怎么处理；
2. **协议报错脱敏**：绝对不直接把远端 JSON-RPC 返回的原始报错文字原封不动传给模型——因为有些烂服务会把请求头、Token 或内网 URL 吐进错误详情里！宿主先把错误信息截到 1024 字节，只抽取结构化的“缺少/非法字段名”，而且**只有字段名属于 8 个常规固定名或刚才在 `tools/list` 里声明过的公开属性名（`public_fields`）才允许带出去**，其余一律换成按错误码映射的安全提示；
3. **写操作断线绝不自动重发**：整次操作包在服务配置的 `timeout_ms` 超时里（硬暂停也会立刻取消本地等待）。程序用了一个布尔标志 **`sent`**：只有当 `mcp.call` 确认跳过本地校验并把请求发出去那一刻才把 `sent` 置为 `true`。一旦在这之后遇上超时、断线或响应超限，MyGo 会明确报错 **`"; result unknown; request was not replayed"`**——比如远端可能已经把工单建好了、只是回信丢在半路，MyGo 绝不会擅自重放请求，提醒模型先去远端核对状态再决定。

### 4. 一键诊断体检 `POST /mcp/servers/{id}/test`（`src/mcp/diagnostics.rs`）

配置完服务（哪怕当前设了 `enabled: false` 还没启用），可以调体检接口试连通。它受全进程信号量 `mcp_probes` 限流（最多 **4 个**同时探测，满额返回 `429`），现开一条不进连接池的短连接，依次走完握手 -> `tools/list` -> `resources/list` + `resources/templates/list` -> `prompts/list`，把协商出的协议版本、每项列表条目数、是否还有下一页以及停在哪一步返回来，最后 `close(1s)` 干净收尾。

---

## OAuth 2.1 全链路：防 SSRF、并发刷新与世代号

企业级远端 HTTP MCP 服务往往不能配死静态 Key，而是要走标准的 **MCP OAuth 2.1**。MyGo-Bot 在 `src/mcp/oauth/` 下实现了一套非常硬核的完整客户端：

```
  1. 启动登录 POST /oauth/login（检查当前未授权 -> 写入唯一 generation 世代号）
       ├─ 有静态头或 URL 带 query 时，先用 app.client 探测拿 WWW-Authenticate
       ├─ 经 MetadataResolver 防 SSRF 网关，发现 RFC 9728 (PRM) -> RFC 8414 / OIDC (ASM)
       ├─ 未配 client_id 时走 RFC 7591 动态注册（验证 issuer / 防 HTTPS 降级）
       └─ 生成 PKCE(S256) + state + resource(RFC 8707)，返回 authorization_url
            │
            ▼ 用户浏览器完成授权（有效期 600 秒）
  2. 回调完成（本地免配时开 127.0.0.1:0/callback 验 Host；远程部署粘贴 URL 调 POST /oauth/callback）
       └─ 校验 state/redirect_uri/RFC 9207 iss -> 消费 pending -> 换 Token 存入 credentials + binding
            │
            ▼ 运行期每次发 MCP HTTP 请求
  3. HttpClient::acquire_token
       ├─ 同 generation 共享一把互斥锁 gate
       ├─ 401 标 rejected 或快过期（< min(30s, ttl/2)）-> 在【脱离的 tokio::spawn(45s)】里刷新
       └─ 若遇服务端轮换 refresh_token，哪怕外层 MCP 调用刚好超时取消，新令牌仍能稳稳写回库！
```

其中藏着四个一不小心就会踩坑的保命设计：

1. **SSRF 与重定向防火墙（`MetadataResolver`）**：发现元数据和动态注册都要按服务端吐出来的 URL 去发请求，绝不能被恶意服务端引去扫内网或云厂商元数据（`metadata.google.internal` 等）。所以自研 DNS 解析器会把所有 IP（包括 IPv4-mapped IPv6、NAT64、6to4 等变形地址）分类：链路本地/保留段一律死封；私网和回环地址**只有当管理员最初配的 MCP 地址本身就是私网字面 IP 或回环地址时才放行对应网段**；发现过程最多跟 10 跳**同源**重定向且严禁 HTTPS 降级为 HTTP。
2. **用 `generation` 世代号和触发器锁死状态归属（`mcp_oauth` 表）**：每次发起新登录都发一块全新的 48 位十六进制随机 `generation` 牌子，且只有 `login` 接口允许往 `mcp_oauth` 插行，所有后续回调、刷新、标拒全都改成带 `WHERE server_id=? AND generation=?` 的 `UPDATE`——绝不允许一个半小时前超时掉队的回调或刷新任务把用户刚注销的账号悄悄插回数据库里！同时 `schema.sql` 里的触发器 **`invalidate_mcp_oauth`** 盯紧 `mcp_servers`：改 `description`/`enabled`/`allowed_tools`/`timeout_ms` 不影响已登录的 OAuth，但只要改了 `transport` 或 `config_json`（端点 URL、静态头、OAuth 配置），数据库当场自动删掉旧 OAuth 凭证。
3. **跨取消与冲突中保住轮换令牌（`oauth/transport.rs`、`oauth/storage.rs`）**：很多 OAuth 服务启用了 **Refresh Token Rotation（每次刷新都把旧 `refresh_token` 作废、发个新的）**。要是把刷新 HTTP 请求直接跑在 MCP 工具的异步任务里，刚收到新令牌、还没来得及写进 SQLite，外层 MCP 调用恰巧超时被取消，新旧 `refresh_token` 就全毁了！所以 `acquire_token` 拿到同世代的 `gate` 锁之后，特意把 `acquire_valid_token` 放进一个**独立的 `tokio::spawn`（设 45 秒超时）**里跑完——哪怕外面那次 MCP 调用已经超时或被硬暂停取消，后台这趟刷新照样把轮换来的新令牌稳稳存进数据库，甚至就算期间有人顺手改了服务描述导致 `revision` 涨了，`save_credentials` 依然允许把轮换令牌存下，同时返回 `false` 通知老连接退场让新连接继承这枚珍贵的新令牌。更进一步，万一网络刷新飞在半路时该共享 MCP 流恰好撞上集群分叉冲突，直接返 `409` 回滚就会白白吞掉单次有效的 `refresh_token`；此时 `save_credentials` 会先把轮换新凭据落进 **`mcp_oauth.staged_credentials`**（保留冲突冻结态），等后续选择保留本地（`resolve_conflict_local`）时再自动把暂存新凭据晋升成正式凭据发出，刷新绝不丢单。
4. **登出与远程撤销（`DELETE /oauth`、`oauth/revoke.rs`）**：点登出时，先确认该共享服务没有未决同步冲突（有冲突则返 `409` 不动库），再按最多 5 秒等一把可能正在途的 `gate` 刷新任务，然后带 `generation` 校验删光本地 `mcp_oauth` 行并踢掉全部相关 MCP 连接；接着再走 Worker 任务按 **RFC 7009** 并行去授权服务器撤销 `refresh_token` 和 `access_token`（只认跟 `issuer` 或 `token_endpoint` 同源的撤销地址）。就算远端撤销失败或刚好撞上刷新超时，**绝不回滚本地已经完成的登出清理**，而是直接返回 `502 Bad Gateway` 提醒用户“本地凭据已清，请去授权方手动撤销授权”。

---

## 单服务集群同步开关（`shared`）与跨节点 OAuth 刷新协调

不是所有 MCP 工具都适合丢到集群每一台机器上同步（比如只绑定了本机某个目录或硬件的 `stdio` 脚本就该安安静静留在那台机子上）。MyGo-Bot 在单服务粒度用 **`shared`** 字段把“本地私有”和“全集群共享”拆得一清二楚：

1. **双态控制与 `pending` 登录态隔离（`src/cluster/ledger.rs`、`src/mcp/config.rs`）**：
   - `shared = false` 时，服务配置、修改与 OAuth 登录全部留在本机，不写任何集群提交；
   - `shared = true` 时，服务配置和已经进入终态（`credentials IS NOT NULL` 或 `rejected = 1`）的 `mcp_oauth` 记录一起打包进 `mcp_upsert` 提交实时同步全集群。**唯独进行中的 `pending` 登录态绝对不同步**——一次性 PKCE 挑战码和本地回调地址只属于发起登录的那一台机器。
2. **共享切回私有、删服的高 `epoch` 墓碑与清密（`true -> false` / `DELETE`）**：
   - 把一台共享服务关掉同步或彻底删掉时，如果只追加一笔普通删除提交，旧 `sync_commits` 里依然残留着带 `url` 参数、`headers`、`env` 或 OAuth 凭据的历史快照，不但占着本地库，还会继续扩散给新上线节点。因此无论 `shared` 从 `true` 切回 `false` 还是直接删共享 MCP：服务端先把旧配置克隆为一枚**全新的本地 `McpServerId`**（`shared = 0`、`revision = 1`，**仅复制已授权/已拒的终态 OAuth，绝不带走 `pending` 登录态并清除暂存凭据**），同时把旧 `McpServerId` 流推进到**更高 `epoch`**，写入永久 **`mcp_delete` 墓碑（`deleted = 1, retired = 1`）并就地洗清旧 `sync_commits`**；在线与日后重连的同伴收到高 `epoch` 墓碑后，也会一并删净旧配置和旧历史提交里的全部敏感凭据，并锁死该 `stream_id` 永不复活。
3. **回放次序与连接池精确热重置**：
   - 远端回放 `mcp_upsert` 时，程序总是**先 upsert `mcp_servers`、再按需写入 `mcp_oauth`**——因为前面说过 `schema.sql` 挂了 `invalidate_mcp_oauth` 触发器，如果顺手先写了 OAuth 再更新服务传输配置，触发器就会把刚到手的有效凭据洗光；每次同步变更落盘后，记账员调用 `Connections::apply_sync_change(McpChange)` 清掉旧连接槽位，并**只在凭据或绑定确实发生变化（`cancel_login = true`）时**才取消本机登录监听，避免无辜元数据更新打断本机正在扫码/回调的登录窗口。
4. **跨节点 OAuth 刷新协调（`coordinate_mcp_oauth_refresh`，`src/cluster/sync.rs`、`src/mcp/oauth/transport.rs`）**：
   - 前面讲过很多 OAuth 服务会开启 Refresh Token Rotation：多台节点手里拿着同一份同步来的 `refresh_token`，如果同一秒各自拿它去换新令牌，慢半拍的节点一定把令牌撞废！
   - 当共享服务的令牌临近过期或遇 401 准备刷新时（若流本身已有冲突则当场返 `409` 阻止盲刷），本机先群发 `RpcReq::GetHeads` 询问所有在线同伴谁拿着这条非墓碑 `mcp_*` 流，然后按**字典序挑出在线最小 `node_id` 作为唯一协调者**：
     - 若最小节点就是本机（或没有其他在线同伴持有），直接本机刷新并写入同步提交；
     - 否则向协调者发一趟 `RpcReq::OAuthRefresh`——除了 `(server_id, generation)`，还顺手带上调用方此刻的 **`(expected_epoch, expected_seq, expected_hash)`** 与 **`rejected_token_hash`**（被远端 401 拒掉的访问令牌 BLAKE3 摘要）。协调者抢住同世代 `gate` 锁后，先比对流前缀祖先是否一致（分叉直接返回 `Conflicted`，落后则让调用方先对账）、再看协调者手里是不是早已有更新过的有效访问令牌（若 `rejected_token_hash` 已经不是协调者当前令牌，说明同伴刚顺手刷好了，直接返回 `UpToDate` 和缺失提交、根本不再多余请求授权方）；真要刷才单点刷新并把最新流头和提交分页打回调用方；调用方收齐验证收敛到目标头才放行。万一协调者返回需要重登录（`ReauthRequired`），调用方直接返 `AuthRequired` 停止重刷；若协调者半途失联，调用方会**先跟在线同伴同步一轮确认对方没把单次令牌偷刷走**，确认没推进后才在本机兜底刷新。

---

**上一篇**：[05 · Codemode 下篇：前后台分流、原地等信、实时透视与租期恢复](05-codemode-lifecycle.md) · **返回目录**：[总览](README.md) · **下一篇**：[07 · 实时流、HTTP API、内置文档与 WebUI](07-realtime-api-and-webui.md)
