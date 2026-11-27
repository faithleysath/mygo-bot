# 03 · 模型目录、多模态附件与精确计费

一个 Agent 框架一旦接上多个模型供应商、允许发图片音频视频、还打算长期统计每次调用的开销，立刻就会冒出三类最烦人的烂账：

1. **改配置把老会话搞崩了**：模型删了某个推理档位或者去掉了图片支持，结果老会话上下文里还躺着几张图片，一运行才炸；或者改了模型的上游地址/代号，回头一查花费报表全张冠李戴。
2. **多模态大文件把账本撑爆了**：每发一次图片就把几兆 Base64 塞进对话流水账，翻个历史列表都要搬几十兆文字。
3. **算钱算成一笔糊涂账**：供应商中途调价、分峰谷时段、缓存写入有的算进普通输入有的单独计费，再叠加浮点数 `0.1 + 0.2` 舍入误差，对账永远差几分钱。

MyGo-Bot 用“目录四道铁门 + 附件分表实时展开 + 开跑冻快照、纯整数按十亿分之一美元算账”把这三摊事清清楚楚地管了起来。

---

## 供应商与模型目录：软归档与四道护账铁门

所有模型信息都分成两级存在 SQLite 里（[schema.sql](../schema.sql)、[src/catalog.rs](../src/catalog.rs)）：

- **`llm_providers`（供应商）**：登记 `name`、`base_url` 和可选 `api_key`。其中 `base_url` 必须是纯正的 `http`/`https` 根地址，禁止夹带用户名、密码、query 或 `#fragment`；`api_key` 传空串视为清空，查询投影 `ProviderInfo` 只返回 `has_api_key: bool`、绝不回显明文；集群同步时，`base_url` 与 `api_key` 共用同一个原子 HLC 时间戳 `endpoint_ts`，防跨机拼错地址与密钥。
- **`llm_models`（模型）**：挂在某个 `provider_id` 下，记录 `model_key`、可选正整数 `context_window_tokens`、`input_modalities`（非空去重数组，只能取 `text`、`image`、`audio`、`video`）、`reasoning_efforts`（去重非空、无首尾空白的字符串数组）和可选的 `pricing` 价目表。

### 软归档与名字复用

用过的供应商和模型从来不能物理硬删——因为 `llm_calls` 账单和老会话里还指着它们。程序采用 `archived_at` 时间戳来做软归档（`DELETE /providers/{id}`、`DELETE /models/{id}`，支持用 `POST .../restore` 恢复）。

妙在两个部分索引配合应用层事务检查：
- `llm_providers_active_name ON llm_providers(name) WHERE archived_at IS NULL`
- `llm_models_active_key ON llm_models(provider_id, model_key) WHERE archived_at IS NULL`

这两条索引刻意建成**非唯一部分索引**。这样哪怕两台设备离线并发创建重名目录，跨节点 CRDT 合流时也不会被 SQL `UNIQUE` 报错卡死；而在本地常规的新建、修改与恢复写事务里，应用层会顺着这两条索引严格拦截同活跃名称碰撞并返回 `409 Conflict`。也就是说：**只要旧条目归档了，你就可以立刻用一模一样的名字或 `model_key` 新建一条配置**；只有试图恢复一条“跟当前活跃条目重名”的旧记录时才会被拦下。

判断一个模型“能不能用”也不用业务代码四处拼条件，全库统一查 **`model_catalog` 视图**——只有模型自己没归档 **且** 所属供应商也没归档（`m.archived_at IS NULL AND p.archived_at IS NULL`），`available` 才为 `1`。两张目录表的所有增改和归档/恢复都会记下逐字段 HLC 时间戳，并递增持久化在高水位 `local_node.catalog_rev` 上的行级 `sync_rev`，经 `CatalogSync` 在集群内自动追更。其中 [src/cluster/ledger.rs](../src/cluster/ledger.rs) 的 `merge_provider_wire` 通过 `lww_take_pair` 把供应商的 `base_url` 与 `api_key` 捆进同一枚 **`endpoint_ts`** 一并取新——省掉这枚绑定、拆成两个时间戳，A 改地址而 B 换密钥后合流就会把新密钥错发去老地址。

### 护账与护会话的四道铁门

为了不让后面的修改把前面的账和会话撕烂，程序卡死了四道检查：

| 铁门 | 在哪拦截 | 它只管哪件事、省掉会漏掉什么 |
| :--- | :--- | :--- |
| **1. 跑过一次就锁死身份** | `PATCH /models/{id}`<br>（`Model::check_update`） | 只要 `llm_calls` 里已经记过哪怕一笔调用，该模型的 `provider_id` 和 `model_key` 就永久锁死不可改（返 `409`）。省掉它，改一下 `model_key` 就会把昨天跑旧模型的花费算到新模型头上。 |
| **2. 缩减能力先扫所有相关会话** | `PATCH /models/{id}`<br>（`Model::check_update`） | 想从模型里删掉某个 `reasoning_effort` 或某项 `input_modality`？先扫描所有正在使用该模型的未删除会话：检查 `reasoning_effort` 是否正在使用；若缩减了 `input_modalities`（[src/catalog.rs](../src/catalog.rs)），则先确认该会话活跃分支历史已在本地完整收敛（`session_branch_complete`，不完整直接返 `409` 提示先同步再改模态），再跑 `media::check_session` 逐项验光全部已入账与门外媒体。 |
| **3. 换会话模型先查历史媒体** | `PATCH /sessions/{sid}` | 每场会话必须绑定明确模型（`sessions.model_id NOT NULL`），切换时不允许传 `model_id: null`（直接返 `400`）。更换前会先调 `ensure_session_watched(sid)` 拉齐正文，再跑 `media::check_session` 仅凭 `attachments` 元数据里的 `mime_type` 验光全部现有媒体是否受新模型支持，无需预先下载远端附件原文件。 |
| **4. 归档供应商拒绝挂新模型** | `POST /models` / `PATCH /models/{id}` | 不能往已归档的供应商下面添新模型或把模型挪过去（返 `409`）。同时针对单个会话的 `Selection` 配置，没选模型时一律不准单填 `reasoning_effort`。 |

---

## 多模态附件：内容寻址存门牌、按需分块拉字节、发上游才现包 Base64

图片、音频、视频动辄几兆甚至几十兆，MyGo-Bot 坚决不把原始二进制直接塞进 `items` 表的 JSON 里，也不在集群同步会话时盲目把整包大文件到处预先拷贝，而是用 `attachments` 表单独托管（[src/media.rs](../src/media.rs)、[src/cluster/sync.rs](../src/cluster/sync.rs)）：

1. **上传存门牌（`POST /attachments`）**：
   - Body 直接传原始二进制，单文件上限 **50 MiB**，请求头必须带单个具体、无 `*` 通配符的 `image/*`、`audio/*` 或 `video/*` `Content-Type`；
   - 入口由 `UPLOAD_SLOTS`（最多 **8 路**并发）和 `UPLOAD_BYTES`（全局 **100 MiB** = `2 × 50 MiB` 字节预算，满额返 `429 Too Many Requests`）护住内存。字节许可绑定到 `64 KiB` 分块缓冲区，直到数据库线程写完才释放（客户端中途断连也同步回收）；
   - 接收时流式算出 32 字节 BLAKE3 **`content_hash`** 与 **`byte_len`**，落库先插 `ZeroBlob` 再经 SQLite `blob_open` 分块写入，拿到 `att_<26位后缀>` 主键（[src/media.rs](../src/media.rs)）。
2. **同步元数据，用时按需分块拉字节**：
   - 集群同步会话正文时，`attachments` 只传轻量元数据行（`id`、`mime_type`、`content_hash`、`byte_len`），**`data` 列保持 `NULL`**——没点开、没重跑的大文件一字节都不浪费带宽；
   - 等浏览器调 **`GET /attachments/{id}`**（触发 `ensure_attachment_data`）或某节点点火准备喂模型（触发 `ensure_session_attachments`）发现 `data IS NULL` 时：先顺着 `attachments_by_hash` 索引在本机找同哈希副本直接复用；本机没有，再像 BT 下载一样预占全局 **128 MiB** 临时文件预算（`MAX_ATT_BYTE_PERMITS = 512 × 256 KiB`），向所有在线持有节点发起最多 **4 路**并发的 **256 KiB 分块（`GetAttachmentChunk`）** 接力拉取；
   - 发送方通过 SQLite **`blob_open` 增量读取**只抠出请求的字节区间，收方按序写入 `tempfile` 并在阻塞线程池核对整包 BLAKE3，校验通过后再流式写进 SQLite BLOB。
3. **下载与喂模型时现包 Base64**：
   - 对外下载 `GET /attachments/{id}` 通过 `AttachmentBlob::chunks` 增量读取 BLOB：每次开 `blob_open` 连续读取最多 **4 帧 × 192 KiB**（768 KiB）便立刻松开数据库锁吐给网络，原样附带 `mime_type`，并强制加 `Content-Disposition: attachment` 与 `X-Content-Type-Options: nosniff`，防浏览器把文件当同源脚本执行；
   - 账本里的 `user_message`、`function_call_output` 或 `codemode_event` 只记小指针 `{"type":"image","attachment_id":"att_<26位后缀>"}`。只有准备发上游模型时，`media::wire::model_input`（[src/media/wire.rs](../src/media/wire.rs)）才按本轮钉死的 **`pinned_head`** 逐页拼接 JSON 骨架，在每次回库翻页前重新核对 `active_head == pinned_head`（防分块读期间被并发切分支导致前后半截拼岔），随后放锁按 **192 KiB** 帧（3 的倍数，适合无填充增量 Base64 编码）现读现编：
     - **`image`**：转为 OpenAI Responses API 的 `{"type":"input_image","image_url":"data:...","detail":"auto"}`；
     - **`audio` / `video`**：转为 `{"type":"input_file","filename":"attachment-{id}.{ext}","file_data":"data:..."}`，内部将 `audio/mpeg`、`audio/x-wav`、`audio/mp4`、`video/quicktime` 等准确映射为 `.mp3`、`.wav`、`.m4a`、`.mov`；
     - **`codemode_event`**：在内容数组开头自动插一块身份牌（`"Codemode <event> event from cm_<26位后缀> (sequence 1)."`），再包成 `role: "user"` 消息喂给模型。
4. **精准验光与 `temp.checked_input` 增量前缀缓存**：
   - 点火开跑（`calls::begin`）与换模型时的 `media::check_session` / `media::check_call_input` 只检查真正进模型槽位的三处数据：① 已入 `active_context` 的消息/推理/图像生成结果；② 尚未归档的 `function_call_output`；③ 门外有效 `pending_inputs` 队列——绝不误查工具参数或普通日志字符串；
   - 为避免几千条消息的长对话每轮从头扫库，独占连接上建了连接级临时表 **`temp.checked_input`**（最多缓存 256 个会话，挂了 7 个 `active_context` / `items` / `attachments` 变更触发器，随事务/`savepoint` 同步回滚失效），记住已验过的 `active_context.position` 前缀，后续回合只增量检查新增行与门外队列。
5. **本地引用追踪与孤儿附件定时回收（`item_attachments`、`reap_orphan_attachments`）**：
   - `attachments` 记录上传或登记时间戳 `created_at`；所有消息写入路径统一经 `on_item_inserted` 维护本机专用引用索引表 **`item_attachments(session_id, item_id, attachment_id)`**，删会话时一并清掉该会话的引用行（Fork 出新会话时会共享原 `AttachmentId` 并为新会话登记引用行，零字节拷贝）；
   - 后台任务 [`media::reap_orphan_attachments`](../src/media.rs)（挂在 `app.workers`）每小时以低优先级分批（每批最多 `ATTACHMENT_GC_BATCH = 64` 条，每轮最多 16 批）扫描并清理超过 **24 小时宽限期（`ATTACHMENT_GC_GRACE_MS`）**且在 `item_attachments` 中已无任何引用的孤儿附件，兼顾上传至发送窗口期安全与长期磁盘空间有界。

---

## 每次请求立一张单：`llm_calls` 与 `PriceBook` 精确计费

每当 Agent 循环准备向模型发起**一次**上游 HTTP 请求（注意：**自动重试时的每一次重发也算单独一笔**），记账员 [`calls::begin`](../src/calls.rs) 就在同一事务内钉死三样东西：
- 验证并记录本轮读取的上下文终点 `input_end_position` 与当前分支头 **`input_head`**（核对 `active_context` 末端与 `sessions.active_head` 一致，防分叉或半同步上下文开跑），后续崩溃恢复正靠 `input_head` 精确认出属于当前分支的在途工具调用；
- 往 `llm_calls` 插入一行 `status = 'in_flight'` 账单，并写入 `(origin_node_id, origin_seq)` 因果序；
- 在收尾结算与删会话脱钩（将 `session_id` 设为 `NULL`）时递增源节点 `origin_seq`，随 `CatalogSync` 的 `calls_vector` 因果向量自动对齐全群，保证任意节点都能看见完整花费流水。

### 为什么开跑时就先把价格快照冻进账单？

如果模型单价是按星期、UTC 小时甚至节假日变的，或者管理员明天把 `llm_models.pricing` 改了，半途跑完的那一单到底按什么时候、哪一档价目表算？

所以 `calls::begin` 在发请求**那一刻**（按 `started_at` 毫秒时间戳），就拿着模型的 `PriceBook` 算出当下适用的**`pricing_snapshot`**，当场写进 `llm_calls` 行里冻住（[src/pricing.rs](../src/pricing.rs)）：

```
PriceBook（强制 timezone = "UTC"）
  ├─ calendar（可选：covered_years + holiday_dates YYYY-MM-DD）
  └─ versions[]（支持 RFC3339 或 Unix ms 的 effective_from，按 started_at 挑最新已生效版本）
       ├─ calendar（版本级日历优先于顶层日历）
       ├─ default_tiers[]（默认阶梯价）
       └─ periods[]（分时段规则：ISO 周 1..=7、start_minute..end_minute 0..1440 同日不可重叠、可选 exclude_holidays）
            └─ tiers[]（按 up_to_input_tokens 严格递增，最后一级必为 null 无上限）
                 └─ rates: { input, cache_read, cache_write, output }（十进制字符串：美元 / 百万 token）
```

这里有一个很谨慎的细节：**节假日判断不猜未知年份**。如果命中的时段勾了 `exclude_holidays: true`，但调用当年的年份压根没填进日历的 `covered_years` 里，`is_holiday` 直接返回 `None`，宁可跳过生成快照让这单 `cost_nano_usd` 留空，也绝不胡乱按“非节假日”打折或原价记账。

等这趟 HTTP 请求结束（无论成功、失败、被取消还是被重启动断），`Report::finish` 会写入 `finished_at`、`status`、`upstream_response_id`、`http_status`、`error_summary` 与 `raw_usage`，再用当时冻好的 `pricing_snapshot` 匹配这次输入总 token 数所在阶梯（记录 `matched_tier_index`）并算出费用。

为了防止上游吐出超长脏字段撑爆数据库或阻塞 `CatalogSync` 分页，[src/calls.rs](../src/calls.rs)、`Store::status_checked` 与同步落库路径统一守死上限：`upstream_response_id` 超过 **`MAX_STORED_RESPONSE_ID_BYTES`（1024 字节）**直接置空丢弃，`raw_usage` 超过 **64 KiB** 置空；`error_summary` 与会话的 `last_error` / `local_error` 一律经 `truncate_utf8` 按 UTF-8 字符边界安全截断在 **8192 字节**内。

### 零浮点 `cost_nano_usd` 与双口径缓存结算

钱这东西最怕 IEEE 754 浮点数丢精度，所以数据库里存的金额单位叫 **`cost_nano_usd`**（十亿分之一美元，`1 USD = 10^9 nano-USD`）：
- 价目表里写的是“美元 / 百万 token”，换算成每单个 token 的 `nano-USD`，刚好就是把小数点右移 **3** 位（乘以 `1000`）！
- 计算时把字符串拆成整数部分和小数部分，用 `u128` 累加整数纳美分，小数位超过 3 位的余数放进一个 `Vec<u8>` 逐位做十进制乘法和进位，最后只在十分位满 `5` 时向上四舍五入 `1` 纳美元——**全程零浮点运算**，哪怕几百万 token 也能精确到十亿分之一美元。

在拆解上游返回的 `usage` 时，不同厂商对“提示词缓存”的汇报口径不一，MyGo-Bot 用一套双口径规则防错：
- **没有单独收缓存写入费的模型**（比如 DeepSeek 风格，只报 `input_tokens` 和 `cached_tokens`，缓存写入按普通输入收费）：只要当前阶梯没配正数 `cache_write` 费率，程序就把 `cache_write_tokens` 记为 `0`，稳妥算出 `input_uncached_tokens = input_tokens - cache_read_tokens`；
- **单独配了正数 `cache_write` 费率的模型**（比如按较高/较低写入单价单独计费的厂商）：程序强要求上游响应的 `input_tokens_details.cache_write_tokens` 必须明确给出数值；如果缺了这个字段，程序**宁可让 `cost_nano_usd` 保持 `NULL`**（算入未计价统计），也绝不把缓存写入混成普通输入瞎算一通。同样，只要出现 `cache_read + cache_write > input_tokens` 这种自相矛盾的计数，也置空不记假账。

### 对账与前端用量接口

所有调用账单可以通过三个只读接口查询（`GET /calls` 游标分页可按 `session_id`、`model_id` 筛选；`GET /calls/{id}` 查单笔详情；`GET /calls/summary` 查汇总）。`summary` 不仅聚合总调用数、成功/失败数、四类 token 总量和 `cost_nano_usd` 总额，还专门吐出两个贴心数字：
- **`unpriced_succeeded_count`**：有多少笔成功调用因为没配价目表或日历/缓存口径不全没算出钱（提醒你总花费只是部分统计）；
- **`last_input_tokens`**：最近一次成功调用吃进的总输入 token 数（`input_uncached + cache_read + cache_write`），WebUI 顶栏拿它除以模型的 `context_window_tokens`，正好能算出当前上下文窗口已经填了多少。

---

**上一篇**：[02 · 底层底座：单记账员、合批落盘、流式读写与防重](02-storage-and-concurrency.md) · **返回目录**：[总览](README.md) · **下一篇**：[04 · Codemode 上篇：最小完备原语、独立车间与宿主能力](04-codemode-sandbox.md)
