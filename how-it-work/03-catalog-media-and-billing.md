# 03 · 模型目录、多模态附件与精确计费

一个 Agent 框架一旦接上多个模型供应商、允许发图片音频视频、还打算长期统计每次调用的开销，立刻就会冒出三类最烦人的烂账：

1. **改配置把老会话搞崩了**：模型删了某个推理档位或者去掉了图片支持，结果老会话上下文里还躺着几张图片，一运行才炸；或者改了模型的上游地址/代号，回头一查花费报表全张冠李戴。
2. **多模态大文件把账本撑爆了**：每发一次图片就把几兆 Base64 塞进对话流水账，翻个历史列表都要搬几十兆文字。
3. **算钱算成一笔糊涂账**：供应商中途调价、分峰谷时段、缓存写入有的算进普通输入有的单独计费，再叠加浮点数 `0.1 + 0.2` 舍入误差，对账永远差几分钱。

MyGo-Bot 用“目录四道铁门 + 附件分表实时展开 + 开跑冻快照、纯整数按十亿分之一美元算账”把这三摊事清清楚楚地管了起来。

---

## 供应商与模型目录：软归档与四道护账铁门

所有模型信息都分成两级存在 SQLite 里（[schema.sql](../schema.sql#L1-L23)、[src/catalog.rs](../src/catalog.rs)）：

- **`llm_providers`（供应商）**：登记 `name`、`base_url`（校验必须是 `http` 或 `https`，且不允许夹带用户名、密码、query 或 `#fragment`）以及可选 `api_key`（传空串视为清空；查询接口 `ProviderInfo` **永远不回显密钥明文**，只返回 `has_api_key: bool`）。
- **`llm_models`（模型）**：挂在某个 `provider_id` 下，记录 `model_key`、可选正整数 `context_window_tokens`、`input_modalities`（非空去重数组，只能取 `text`、`image`、`audio`、`video`）、`reasoning_efforts`（去重非空、无首尾空白的字符串数组）和可选的 `pricing` 价目表。

### 软归档与名字复用

用过的供应商和模型从来不能物理硬删——因为 `llm_calls` 账单和老会话里还指着它们。程序采用 `archived_at` 时间戳来做软归档（`DELETE /providers/{id}`、`DELETE /models/{id}`，支持用 `POST .../restore` 恢复）。

妙在两个部分唯一索引：
- `active_provider_name ON llm_providers(name) WHERE archived_at IS NULL`
- `active_model_key ON llm_models(provider_id, model_key) WHERE archived_at IS NULL`

意思是：**只要旧条目归档了，你就可以立刻用一模一样的名字或 `model_key` 新建一条全新配置**；只有当你试图恢复一条“跟当前活条目重名”的旧记录时，才会撞上唯一索引返回 `409 Conflict`。

判断一个模型“能不能用”也不用业务代码四处拼条件，全库统一查 **`model_catalog` 视图**——只有模型自己没归档 **且** 它所属的供应商也没归档（`m.archived_at IS NULL AND p.archived_at IS NULL`），`available` 才为 `1`。当你把某个供应商或模型归档时，记账员会在**同一个数据库事务**里顺手把指向不可用模型的全局 `config.default_model_id` 和 `default_reasoning_effort` 一起清空。

### 护账与护会话的四道铁门

为了不让后面的修改把前面的账和会话撕烂，程序卡死了四道检查：

| 铁门 | 在哪拦截 | 它只管哪件事、省掉会漏掉什么 |
| :--- | :--- | :--- |
| **1. 跑过一次就锁死身份** | `PATCH /models/{id}`<br>（`Model::check_update`） | 只要 `llm_calls` 里已经记过哪怕一笔调用，该模型的 `provider_id` 和 `model_key` 就永久锁死不可改（返 `409`）。省掉它，改一下 `model_key` 就会把昨天跑旧模型的花费算到新模型头上。 |
| **2. 缩减能力先扫全局和所有会话** | `PATCH /models/{id}`<br>（`Model::check_update`） | 想从模型里删掉某个 `reasoning_effort` 或某项 `input_modality`？先查全局 `config` 和每一个正在用该模型的会话——只要有人选了被删的档位，或者会话历史/队列里存了被删的媒体类型，立刻返 `409` 拦下。 |
| **3. 换会话模型先查历史媒体** | `PATCH /sessions/{sid}` | 给会话切换 `model_id` 时必跑 `media::check_session` 验光全部现有媒体是否被新模型支持；而且**只有完全空的会话**（上下文、排队队列、工具调用全为空）才允许把 `model_id` 改回 `null`（传 `model_id: null` 时自动联动清空 `reasoning_effort`）。 |
| **4. 归档供应商拒绝挂新模型** | `POST /models` / `PATCH /models/{id}` | 不能往已归档的供应商下面添新模型或把模型挪过去（返 `409`）。同时无论全局 `config` 还是会话 `Selection`，没选模型一律不准单填 `reasoning_effort`。 |

---

## 多模态附件：账本存门牌号，发上游才现包 Base64

图片、音频、视频动辄几兆甚至几十兆，MyGo-Bot 坚决不把原始二进制直接塞进 `items` 表的 JSON 里，而是用 `attachments` 表单独托管（[src/media.rs](../src/media.rs)）：

1. **上传与下载的防线**：
   - 客户端先调 `POST /attachments`，Body 直接传原始二进制（上限 **50 MiB**），请求头必须带单个具体、无 `*` 通配符的 `image/*`、`audio/*` 或 `video/*` `Content-Type`，落库后拿到 `att_<26位后缀>` 格式的 TypeID。
   - 下载接口 `GET /attachments/{id}` 会原样吐出存储的 `mime_type` 和字节流，但强制加上 `Content-Disposition: attachment` 和 `X-Content-Type-Options: nosniff`，防止浏览器把恶意文件当成同源网页或脚本执行。
2. **轻量账本引用 vs 上游实时展开**：
   - 写进 `user_message`、`function_call_output` 或 `codemode_event` 的媒体块，只留一个干净的小指针：`{"type":"image","attachment_id":"att_<26位后缀>"}`。
   - 只有当第 2 篇讲的 `JsonRows::ModelInput` 接力拼装上游 HTTP 请求体时，`media::wire_message` 才现从 `attachments` 里捞出二进制并编成 `data:{essence};base64,...`：
     - **`image`**：转成 OpenAI Responses API 的 `{"type":"input_image","image_url":"data:...","detail":"auto"}`；
     - **`audio` / `video`**：转成 `{"type":"input_file","filename":"attachment-{id}.{ext}","file_data":"data:..."}`，内部贴心把 `audio/mpeg`、`audio/x-wav`、`audio/mp4`、`video/quicktime` 等分别映射成 `.mp3`、`.wav`、`.m4a`、`.mov`；
     - **`codemode_event`**：在内容数组最前面自动插一块文本身份牌（`"Codemode <event> event from cm_<26位后缀> (sequence 1)."`），再把整条包成 `role: "user"` 消息喂给模型。
3. **精准检查不误伤（`media::check_session`）**：
   - 无论是点火开跑前（`calls::begin`）还是给会话换模型时，`check_session` 只翻**真正会送进模型输入槽位**的三处东西：① 已进 `context_items` 的消息/推理/图像生成结果；② 刚跑完还没归进上下文的 `function_call_output`；③ 门外还没收进来的有效 `pending` 输入。
   - 它**绝不乱翻工具名称、参数或普通 JSON 字符串**——脚本里打印了一句 `"image_url"` 纯文本，不会误判成会话带了图片模态。

---

## 每次请求立一张单：`llm_calls` 与 `PriceBook` 精确计费

每当 Agent 循环准备向模型发起**一次**上游 HTTP 请求（注意：**自动重试时的每一次重发也算单独一笔**！），记账员就会在钉死 `input_end_position` 的同一个事务里，往 `llm_calls` 表插一行 `status = 'in_flight'` 的账单（[src/calls.rs](../src/calls.rs)）。

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

等这趟 HTTP 请求结束（无论成功、失败、被取消还是被重启动断），`Report::finish` 会填好 `finished_at`、`status`、`upstream_response_id`、`http_status`、`error_summary`、`raw_usage`，再用当时冻好的 `pricing_snapshot` 匹配这次实际产生的输入总 token 数所在阶梯（把下标写回快照的 `matched_tier_index`），算出最终费用。

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
