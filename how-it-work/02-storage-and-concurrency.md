# 02 · 底层底座：单记账员、合批落盘、流式读写与防重

[第 1 篇](01-agent-loop.md)讲清了会话怎么靠“两本账、两个防覆盖集合、一趟 `step`”转起来。可真把服务丢进真实并发里——有人发消息、有人翻几千条老历史、同时好几个网页开着 SSE 直播——马上就会撞上一个底层矛盾：

**为了绝不算乱账，整个进程只设了一个“记账员”握笔；可既然只有一个人握笔，为什么几十个会话并发操作却不堵车？**

这一篇就把它的单线程线程模型、合批写盘、切片翻页、应用层防重以及规范墓碑删库全拆明白。

---

## 一条腿等网络，一条腿独占账本

主进程启动后，干活的核心其实只有两条线：

- **网络与调度线**：Tokio 单线程异步运行时（`Builder::new_current_thread()`），接 HTTP、推 SSE、连大模型、做异步 I/O。
- **独占记账线**：一条名叫 `sqlite` 的专用操作系统线程，手里攥着**全进程唯一一条 SQLite 连接**打开当前工作目录的 `mygo.sqlite3`（Unix 下文件权限 `0600`）。外部所有读写都要排进一条容量 **64** 的 `mpsc` 通道（`Db`），由这一个人串行接单。

连接参数故意开了 `PRAGMA journal_mode=WAL`，却又锁死 `PRAGMA locking_mode=EXCLUSIVE`——压根**不搞多读单写连接池**。好处非常实在：
1. **消灭并发读写锁撕裂**：查改库和内存状态表（`active`、`tasks`、`mcp`、`wake_sequences`、`channels`）同在一条线程上按序完成，绝不会出现“库里写完了但内存还没记上”的窗口；
2. **天然防同目录双开**：独占锁一加，同一个工作目录下只允许跑一个 MyGo-Bot 实例。

> 通道容量 `64` 只限制同时排在记账员窗口前等落笔的指令数，不限制能建多少会话或挂多少条 SSE 长连接。

一个人握笔却不堵车的秘密，就一条铁律：**记账员绝不跨着网络 I/O 干活。** 模型思考、等网络吐字、跑外部工具动辄几秒几十秒，全都在数据库锁外面完成；每次进出记账员只花不到一毫秒落笔或切片取数就松手让给下一位。唯一要防的是把 CPU 纯死算塞进网络事件循环（所以第 4 篇会把模型写的 JS 隔离到单独工人线程里）。

---

## 写得猛时不堵门：顺手拼车与独立反悔点

为了断电不丢账，连接打开了最严的 `PRAGMA synchronous=FULL` 和 `PRAGMA fullfsync=ON`。每提交一次 `COMMIT` 都要真刷物理磁盘，单次要几毫秒；高并发一人一刷立刻排起长队。

所以 `Db::write` 实现了一套**“顺手拼车”（Group Commit）**：
1. 记账员从通道口取下一笔写单时，顺眼看后面排着的其他写单，只要互相兼容就全拢进**同一个 SQLite 外层事务**（每车最多 **32 笔**），只做一次 `COMMIT` 和一次 `fsync`；
2. 通道空着就当场提交，**绝不傻等凑人数**；
3. 遇到普通 `Command::Call` 或不兼容的写单就先截断结清这一车，绝不插队乱序。

```
通道口排队的指令： [写单1] [写单2] [写单3(同会话二次点火)] [读单4]
                      │       │             │               │
  第 1 车（1次fsync）：└───────┴── 兼容，拼！  │               │
  不兼容，截断留下一轮 ──────────────────────┘               │
  第 2 车（先结算点火，发布内存 worker）──────┘               │
  单独执行 Call ────────────────────────────────────────────┘
```

### 细节一：同车有人报错怎么办？——`savepoint` 与先提交后发布

拼车最怕“一颗老鼠屎坏一锅粥”。因此外层事务里，**每一笔**进车的写单都会单独套一个 **`savepoint`**：
- 某笔写单自己的业务校验挂了，只回滚它自己的 `savepoint`，其余正常单照样同车刷盘；
- 只有外层物理刷盘彻底失败，整车才一并回退。

更关键的是发布时机：**所有对外回复、SSE 广播和集群同步脏标记通知，一律按 FIFO 严格等外层 `COMMIT` 真正落盘之后才发出。** 每笔写入通过 `stage_catalog()` / `stage_session(sid)` 把受影响标记先暂存在记账员局部缓冲区 `SYNC_STAGED` 里，跟它的 `savepoint` 同生共死——哪笔回滚就丢弃它的脏标记，整车落稳后才交给 `ClusterSignal::notify_changes` 去叫醒同步循环。

### 细节二：谁绝不能拼进同一趟车？——`Write::can_join`

记账规则把写单分成“普通写（包括 `Db::write` 和运行期 `Db::write_for_run`）”与“启动运行写（`start_session`）”两类（[src/store.rs](../src/store.rs)）：
- **同会话两次点火绝不混批**：若把同一个 `sid` 的两次 `/run` 塞进同一外层事务，第一笔还没 `publish` 进内存 `s.active`，第二笔进事务就会误以为还没人跑从而起出重复 Worker！所以同会话点火必须拆批（不同会话的点火照常拼车；优雅退出 `self.closing == true` 时统一拒开返 `503`）。
- **运行世代拦截过期旧 Worker**：每次开跑递增 `Store.session_run_gen` 钉在 `Control` 上，Worker 的每笔落库全走 `Db::write_for_run` 带上 `expected_run_gen`。记账员在 `commit_batch` 严格核对：
  - 若运行世代已被新 Worker 或分支切换/删除动作顶掉，当场拒写返 **`409 Conflict`**；
  - 错误摘要统一用 `truncate_utf8(..., MAX_STORED_ERROR_SUMMARY_BYTES)` 钳在 **8192 字节**内，绝不切断多字节 UTF-8 字符。
- **分叉/删除重置世代 `session_reset_gen`**：仅在切换 `active_head` 或删除会话时递增，供后台 `codemode` 任务在 `task_superseded` 里核对。正常重开新回合不会递增它，免得误吞跨回合后台完工信。

---

## 账本厚时不堵门：不可变主键快照、视图令牌与分块读

当长会话攒了几千条记录或大媒体附件，若一口气查全表拼几十兆 JSON，既爆内存又堵死记账员窗口。程序在分页与喂模型两条路上用同一招解法——**先把主键抓进内存，再一小瓢一小瓢往外舀**（[src/store.rs](../src/store.rs)、[src/media/wire.rs](../src/media/wire.rs)）：

1. **三选一分页与两类游标**：`limit` 统一为 `1..=200`（默认 50）。只有 `/sessions/{sid}/history`（`itm_...`）和 `/sessions/{sid}/context`（`active_context` 从 1 递增的整型 `position`）完整支持 `after`、`before`、`tail=true` 三种互斥游标；其余列表接口仅支持 `after`。
2. **先拍主键快照、再校验 `X-History-View`**：`JsonRows` 在开读瞬间调 `history_snapshot` 或 `context_snapshot`，先把这页命中的最多 200 条主键清单连同 **`X-History-View`**（`<instance_id>:<view_rev>`）抓在手里；单分支会话走 `has_inactive_branch` 零开销直读快路径，有多条并行分支时则只留当前活分支和未入枝的排队/撤回项。调用方传回 `?view=<token>` 时若发现服务重启、切分支或同步乱序插旧项导致视图号变了，直接返 **`409 Conflict`** 催前端整页重拉，彻底防死混页。
3. **分瓢接力释放库锁**：拿到主键清单后，每趟最多捞 **256 行 / 64 KiB**（单条本身超 64 KiB 也完整放行）立刻松开 SQLite 窗口，通过 `async_stream` 边吐边去队列排下一瓢；喂模型时由 `media::wire::model_input` 同样复核 `active_head == pinned_head`，并通过 `AttachmentBlob::chunks` 用 `blob_open` 每批读最多 4 帧 × 192 KiB 实时编码 Base64 往外流，不在库里冗余存 Base64 也不憋大缓冲区。

> **能力边界**：它省下的是本机**峰值内存**和**占锁时长**；最终喂给上游模型的总上下文字符和字节数并不会变少。

---

## 网络断了重发不算重账：无跨表外键与纯应用层防重

为了防止多节点间乱序送达的 CRDT 同步包被 SQL 外键或全局 `UNIQUE` 报死，Schema `v1` **刻意拿掉了跨表 `FOREIGN KEY` 约束以及共享同步表上的 SQL `UNIQUE` 约束**——`items_by_idempotency`、`tools_by_call`、`items_by_target` 全是非唯一部分索引。防重与收敛全部收拢在独占记账员写事务里按编号卡死：

| 场景 | 依据编号与索引 | 处理规则与跨节点收敛 |
| :--- | :--- | :--- |
| **客户端投信**<br>(`POST /sessions/{sid}/messages`) | HTTP 请求头 **`Idempotency-Key`**<br>（限单头，去首尾空格后 1..=256 字节） | 写事务走索引 `items_by_idempotency(session_id, idempotency_key, id)` 按最小 `id` 查规范代表项：<br>• 首次投递 → `201 Created` + 新 `id`；<br>• 同编号同内容重投 → `200 OK` + 原 `id`；<br>• **同编号换了内容** → `409 Conflict` 拦住；<br>• **双机离线同键撞车** → 合流时 `reconcile_idempotency_key` 归入最小 `id` 规范等价类，任一副本已入上下文或撤回则全组出列，绝不重跑；<br>• **故意不带键** → 当用户真想说两遍，正常记两笔 |
| **模型派工具**<br>(`function_call`) | 条目内 **`$.call_id`** | 落库前走 `tools_by_call` 索引（[src/model.rs](../src/model.rs)）：同会话同 `call_id` 同名同参直接幂等跳过；同 `call_id` 改名或改参直接当异常报错拦住 |

---

## 查询、撤回与安全删除会话

最后看日常会话管理的四个操作（[src/api.rs](../src/api.rs)）：

- **切换分支视角 `PATCH /sessions/{sid} { active_head }`**：须先停在非运行态，且目标必须存在于 `session_leaves`（只认完整分支叶节点）；切换后刷新 `active_context`、推高 `view_rev` 并广播 `session.reset`。
- **查排队信件 `GET /sessions/{sid}/messages`**：按同一套 `idempotency_key` 规范等价类规则聚合（每键只展示最小 `id`），结合是否曾进入任何分支 `context_nodes` 或挂上 `withdrawal` 实时算出 `queued` / `included` / `withdrawn` 状态，支持过滤和游标分页。
- **撤回消息 `DELETE /sessions/{sid}/messages/{mid}`**：若正被远端 Runner 占有会先单跳转发；本机事务按等价类核对还没进过任何上下文即可写撤回单（已进上下文返 `409 Conflict`；已撤回过直接幂等返回 `204 No Content`）。
- **彻底删除会话 `DELETE /sessions/{sid}`**：
  1. 若存在集群同伴，先 `ensure_session_watched` 拉齐正文，再经 [src/cluster/exec.rs](../src/cluster/exec.rs) 的 `acquire_delete_gate` 沿着当前连通图多跳排查占位（他机已删返 `404`、连通图内有人占用返 `409`、发现更新进度则先拉齐再复核），拿着临时声明护栏 `DeleteGateGuard` 进最终事务再次验活后释放；
  2. 将 `sessions` 行推向单调不可逆的 **`deleted = 1` 规范墓碑**（标题、时间、状态、配置与 `active_head` 等全部洗净归零并递增 `sync_rev`），随后清空会话正文与本地派生表，绝不让离线同伴连回来把旧会话复活；
  3. **保留花费账单**：把关联 `llm_calls.session_id` 设为 `NULL` 后继续留存；
  4. **摘掉内存残余**：立即清除该会话的 MCP 连接、SSE 槽位、唤醒弦并广播 `session.deleted`。

---

## 从单机记账走向多节点集群

看到这里你会发现，单机这一层几乎所有看似“怪异”的决定——**去掉跨表外键和全局唯一索引、拆出永不上网的 `local_status`、把 `deleted=1` 洗成全零对称墓碑、严格在 `COMMIT` 后才推脏标记**——全都是在为分布式合流铺平地基。

至于把多台 MyGo-Bot 连成一个私人小群后，数据如何按半格律 CRDT 自动合流、执行面又怎么靠路径向量 Gossip 占有与多跳探测防撞车，我们放到专门的 **[第 8 篇 · 私有集群：无冲突同步与连通图 Gossip 协作](08-cluster-sync-and-execution.md)** 里一次讲透。

---

**上一篇**：[01 · 核心抽象与 Agent 循环](01-agent-loop.md) · **返回目录**：[总览](README.md) · **下一篇**：[03 · 模型目录、多模态附件与精确计费](03-catalog-media-and-billing.md)
