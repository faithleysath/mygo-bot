# 02 · 底层底座：单记账员、合批落盘、流式读写与防重

[第 1 篇](01-agent-loop.md)讲清了会话怎么靠“两本账、两个书签、一趟 `step`”转起来。但只要把程序放到真实的多用户并发环境里——一边有人疯狂投信，一边有人翻几千条老历史，一边还有好几个浏览器标签页开着 SSE 直播——马上就会撞上一个底层矛盾：

**为了绝不算乱账，整个进程只设了一个“记账员”握笔；可既然只有一个人握笔，为什么几十个会话同时用却不卡死？**

顺着这个矛盾往下剥，就能把它的线程模型、合批写库、接力读账、并发防重和删除清理全串起来。

---

## 一条腿等网络，一条腿独占账本

程序跑起来后，主进程只拉了两条主线干活：

- **网络与调度线**：Tokio 的**单线程异步运行时**（`Builder::new_current_thread()`），专门接 HTTP 请求、推实时 SSE、连上游大模型、转发异步 I/O。
- **记账线**：一条名叫 `sqlite` 的独立后台操作系统线程，手里攥着**全进程唯一一条 SQLite 连接**，打开当前工作目录下的 `mygo.sqlite3`（Unix 下文件权限严格设为 `0600`）。外部所有读写请求都得排进一条容量 **64** 的 `mpsc` 通道（`Db` 把手），由这一个记账员串行接单。

这里最容易让人看迷糊的是连接参数：它在 `Db::open` 里同时开了 `PRAGMA journal_mode=WAL`，却又锁死了 `PRAGMA locking_mode=EXCLUSIVE`，压根**没搞“多读单写”连接池**！

为什么这么拧着来？有两个实打实的好处：
1. **彻底消灭并发读写怪病和事务锁竞争**：所有查账、改账和内存状态（活动会话 `active`、活动任务 `tasks`、MCP 连接表 `mcp`、唤醒弦 `wake_sequences`、广播槽 `channels`）全都归这一个线程统一对表，数据库事务提交和内存登记在同一个人手里按顺序走，绝不会出现“库里写了但内存还没记上”的撕裂窗口。
2. **顺手看住了单实例大门**：独占锁一加，**同一个工作目录下同时绝对只准跑一个 MyGo-Bot 实例**。哪怕你第二个实例换了监听端口、也换了 Unix socket 路径，只要工作目录没换、指着同一本 `mygo.sqlite3`，连库这一步就会被直接挡回去。

> 通道容量 `64` 只限制“记账员窗口前瞬间排队等待落笔的指令个数”，不限制有多少在线会话，也不限制挂着多少条长连接 SSE。

那只有一个记账员，为什么几十个会话同时跑不堵车？**铁律只有一条：记账员绝不跨着网络 I/O 干活。**

举个例子：B 会话开跑一个 `step` 时，只在开头花不到一毫秒请记账员把排队新信收进上下文、开好 `llm_calls` 账单并钉死上下文终点，事务当场就 `COMMIT` 交掉了；接下来几秒甚至几十秒上传上下文、等上游模型吐字、跑外部工具，全都在数据库窗口外面进行。B 在那头傻等网络回包的空当，记账员早就顺手把 A、C、D 投来的十几封信全都落完盘了。

这套架构唯一怕两件事：
1. **所有人都在等同一块慢盘做 `fsync` 刷盘**（后面用“顺手拼车”缓解）；
2. **有人不小心在单线程异步运行时上塞了纯 CPU 同步死算**，把整个网络循环冻住——这也是为什么第 4 篇要把模型写的 JS 代码彻底隔离到另一组工人线程里去跑。

---

## 写得猛时不堵门：顺手拼车与独立反悔点

记账员为了断电不丢账，开启了最严苛的落盘配置：`PRAGMA synchronous=FULL` 和 `PRAGMA fullfsync=ON`。每提交一次事务，操作系统都得真把数据刷到物理磁盘才肯放行，一次刷盘就要耗掉几毫秒。要是 16 个客户端同时给会话投信，每个人单独跑一次 `COMMIT` 和 `fsync`，记账员窗口立刻排起长队。

于是记账员在写库路径（`Db::write`）里实现了一套**“顺手拼车”（Group Commit）**机制：

1. 记账员收到一笔写单时，顺手瞄一眼通道口：要是后面已经排着别的写单，**并且互相兼容**，就一口气全拢过来（每车最多拉 **32 笔**），塞进**同一个 SQLite 外层事务**，最后只做一次 `COMMIT` 和一次物理刷盘；
2. 要是通道里空空的，这笔单子当场提交走人，**绝不傻等凑人数**；
3. 只要在通道口遇到一笔普通的读单/复合调用（`Command::Call`），或者遇到**不能拼车的写单**，就先把手里这一车结清，把不兼容的那单放进下一轮，**绝不插队乱序**。

```
通道口排队的指令： [写单1] [写单2] [写单3(同会话二次点火)] [读单4]
                      │       │             │               │
  第 1 车（1次fsync）：└───────┴── 兼容，拼！  │               │
  不兼容，截断留下一轮 ──────────────────────┘               │
  第 2 车（先结算点火，发布内存 worker）──────┘               │
  单独执行 Call ────────────────────────────────────────────┘
```

顺手拼车立刻引出两个必须摁死的细节：

### 细节一：同车有人业务报错怎么办？——`savepoint` 独立反悔点

假设 32 封投信拼一锅提交，第 5 封信因为带了模型不支持的图片格式或者幂等键冲突失败了，总不能让同车的另外 31 封无辜信件一起陪葬回滚吧？

所以在外层事务里面，记账员为**每一笔**进车的写单单独套了一层 SQLite **`savepoint`（独立反悔点）**：
- 哪笔单子自己业务校验挂了，或者写到一半出错，就只回滚它自己的 `savepoint`；
- 同车里其余正常成功的写单各自提交自己的 `savepoint`，跟着外层事务照常一次刷盘；
- 只有当外层 `COMMIT` 物理刷盘本身彻底挂了，整车才会一起失败。

并且记账员死守一条发布顺序：**所有对外部的回信、唤醒信号、SSE 广播，以及触发集群同步的脏标记通知，一律按 FIFO 顺序排在外层事务 `COMMIT` 彻底落稳之后才发**（代码里 `apply` 和 `publish` 两段回调就是干这个用的）。每一笔写单写库时，先调 `stage_catalog()` / `stage_session(sid)` 把受影响的脏标记暂存在记账员线程局部缓冲区 `SYNC_STAGED` 里，跟它自己的 `savepoint` 同生共死——哪个 `savepoint` 失败回滚就当场丢弃它暂存的脏标记（外加整批回滚前清理），只有等整车外层事务 `COMMIT` 真正落盘后，记账员才统一收拢交给有界合并信号器 `ClusterSignal::notify_changes` 唤醒后台同步循环广播 `NotifyDirty`。哪怕客户端在 HTTP 返回前突然断线，或者半路有一两单被回滚，既不漏播已落盘的变更，也绝不会把回滚了的脏状态广播给集群同伴。

### 细节二：谁绝不能拼进同一趟车？——`Write::can_join`

看代码 [src/store.rs](../src/store.rs#L49-L64) 里的规则，写单分成“普通写（包括 `Db::write`、`calls::begin` 与收尾均使用的 `Db::write_for_run`）”和“启动运行写（`start_session`）”两类，**它们绝不混批**：
- 普通写（比如建会话、投信、开单 `calls::begin`、记录回复、保存模型）之间可以自由拼车；
- **最关键的是同一个会话被两个客户端同时点 `PUT /run` 点火**：如果两笔针对同一个会话 `sid` 的点火被塞进同一个外层事务里，第一笔虽然在 SQLite 里写下了本机运行叠加态 `local_status='running'`，但外层事务还没 `COMMIT`，它的 `publish` 阶段还没来得及把 worker 登记进内存 `s.active` 字典，紧接着第二笔在同一批里一看 `s.active` 还是空的，就会误以为“还没人跑”，再派一个重复 worker 出来！
- 所以规则写得死死的：**不同会话的点火可以拼车，同一个会话的两次点火绝不能进同一车**——第一车提交并把 worker 登记进 `s.active` 之后，第二车进门一眼看见活动 worker 已经在了，直接跳过开新循环、原样返回当前会话状态。顺便一提，在优雅退出阶段（`self.closing == true`），点火批次还会直接拒绝开新单，统一返回 `503 Service Unavailable`（`"shutting down"`）。
- **还要防着过期老 worker 与失租节点死灰复燃（运行世代、重置世代与租约同查）**：假如会话正跑在半路，突然碰上用户切换分支、删除会话，或者网络分区导致执行租约过期，旧 worker 虽然收到了取消信号，但手里已经攥着一笔算完的工具账单正准备排进写通道。为了既不让“上一任过期打工人”或“失去多数派的旧 Runner”写坏当前分支，又不至于让正常新开的下一回合把上一回合留下的后台任务完成事件吞掉，记账员把世代与租约绑定在了同一道批提交校验里：
  - `Store.session_run_gen`（Worker 身份世代）与二态实时租约检查 `is_local_lease_valid`：每次会话点火都先递增并钉在 `Control` 上，模型 worker 的每一笔写库——包括开单 `calls::begin`、[src/model.rs](../src/model.rs) 里的流式中间推理/回复输出，以及 [src/tools.rs](../src/tools.rs) 配合 `hard.is_cancelled()` 守门的参数校验报错回包——全都要经 `Db::write_for_run` 附带 `expected_run_gen`。记账员在 `commit_batch`（[src/store.rs](../src/store.rs)）一方面遇到 `starting && presence.is_joining()` 会直接以 **`409 Conflict`** 拦下入群期间的新点火，另一方面对在途写入直接核对 `run_gen` 一致且 `is_local_lease_valid(sid)` 为真，只要运行世代已被顶掉或执行租约已经失效就当场拒写返回 **`409 Conflict`**（再无运行期退回等待重排）。同时 `Store::status_checked`、故障收尾的 `wake` 与熔断路径 `fence_local_run` 在落库错误摘要前均统一用 `truncate_utf8(..., MAX_STORED_ERROR_SUMMARY_BYTES)` 把 `last_error`/`local_error` 安全截断在 **8192 字节**以内（不切断多字节 UTF-8 字符）；
  - `Store.session_reset_gen`（纯内存分支切换/删除重置世代）：**只在切换会话活跃分支 `active_head` 或删除会话时递增**，供后台 `codemode` 任务在 `task_superseded` 里核对。因此正常开新回合不会递增 `reset_gen`，上一回合遗留的后台任务完成事件照常送达；只有真正删会话（`abort_active_session_worker`）或切走分支时，才会用递增后的 `reset_gen` 拦下旧任务遗言。

---

## 账本厚时不堵门：不可变主键快照、视图令牌与分块接力读

搞定了写，再看读。假如某个长会话已经在 `items` 和 `context_nodes` 里攒了好几千条记录（甚至夹着几兆的多模态图片附件），这时候前端来翻 `/history` 或 `/context`，或者 Agent 循环准备把整本上下文拼成 JSON 喂给上游模型——如果让记账员一口气全读出来拼成几十兆的字符串再交出去，不仅当场撑爆内存，记账员窗口还会被死死占住好久，别人连一封小信都投不进来。

所以程序在**对外翻页接口**（`/sessions/{sid}/history`、`/sessions/{sid}/context`，每页默认 **50 条**、最多 **200 条**）使用基于**不可变主键快照**的 **`JsonRows`** 流式分块读法（[src/store.rs](../src/store.rs#L1147-L1260)），并在**对上喂模型**时抽出同样按小块接力的独立通道 **`media::wire::model_input`**（[src/media/wire.rs](../src/media/wire.rs#L193)）：

1. **三选一翻页、一次抓齐不可变主键清单与 `X-History-View`**：
   - **先分清两类分页接口**（`limit` 统一限制在 `1..=200`）：只有 `/sessions/{sid}/history` 和 `/sessions/{sid}/context` 完整支持 `after`、`before`、`tail=true` 三种互斥游标模式（最多传一个），其中 `/history` 传 `itm_...` 形式的 **TypeID 字符串**，而 `/context` 查询 **`active_context`** 表、传该会话活跃分支内从 `1` 开始连续递增的整型 `position`（`after >= 0`、`before > 0`）。至于 `/sessions`、`/calls` 和 `/sessions/{sid}/messages` 列表接口，**仅支持向后翻页的 `after` 游标**（分别传 `ses_...`、`cal_...`、`itm_...` TypeID 字符串），不接受 `before` 或 `tail`。
   - **先拍主键快照、再按视图令牌守门**：过去要是只在第一趟记下范围端点 `(start, end)` 而在后续每瓢重新跑范围 SQL，一旦分块输出中途恰好撞上 `PATCH active_head` 或同步切换分支，同一趟 HTTP 响应的前半页和后半页就可能把两条不同分支的记录混拼到一张 JSON 数组里！因此 `JsonRows` 改为在开读第一瞬间调用 [`history_snapshot`](../src/store.rs#L1155) 或 [`context_snapshot`](../src/store.rs#L1199)，一趟先把本次命中（不超过 200 条）的**不可变主键清单**（`Vec<ItemId>` 或 `Vec<(position, ItemId)>`）连同当前会话的 **`X-History-View`** 视图令牌（`<instance_id>:<view_rev>`，在进程重启、用户切换分支、或同步落库发生分支切换/乱序旧项插入时都会改变）一次性抓进内存。调用方还可以把上次拿到的令牌通过可选查询参数 **`?view=<token>`** 传回来：只要跟服务端当下视图对不上，直接返回 **`409 Conflict`** 催客户端从尾重拉，彻底防住跨分支、跨乱序的混页拼缝。
   - **对存在分叉的会话零开销过滤非活跃分支**：`JsonRows::history_snapshot` 会先看一眼 `session_leaves` 确认是否存在非 `active_head` 的平行分支（[`has_inactive_branch`](../src/store.rs#L1137-L1144)）——绝大多数单分支会话走**零过滤快路径**直读 `items`；只有真正发生过分叉且存在其他分支的会话，才自动只保留“当前活跃分支 `active_context` 里的项，以及尚未进过任何分支的项（如排队输入、撤回单）”，把旁支上的历史项干净藏起来（切去旁支后立刻可见）。
2. **拿着主键切片一瓢一瓢往外舀**：
   - 拿到那份最多 200 行的不可变主键清单后，后面每次只请记账员按清单切片查**一小瓢**正文：最多 **256 行**（按 64 KiB 字节上限提前截断），字节攒到大约 **64 KiB** 就立刻打住（如果单条记录本身超过 64 KiB，也会完整放行这一条不卡死）；
   - 舀出这一瓢立刻松开记账员窗口，通过 `async_stream` 交给网络流往下发；发完这瓢再去通道排队舀下一瓢，直到整份主键快照全数吐完才合上末尾括号。
   - **喂模型时的增量流式展开**：对上喂模型的 `media::wire::model_input` 在舀每一瓢 `active_context` 上下文时都会复核 `active_head == pinned_head`，遇到 `user_message`、`function_call_output` 或 `codemode_event`，只先序列化 JSON 骨架并记下附件要插入的精确字节偏移量，随后放开记账员窗口，通过 `AttachmentBlob::chunks` 每次开 SQLite `blob_open` 句柄读取最多 4 帧 × 192 KiB 就立刻释放库锁，边吐出网络流边增量编码 Base64（见 [第 3 篇](03-catalog-media-and-billing.md)），既不提前在库里存冗余 Base64，也不用一口气憋出几十兆的请求体缓冲区。

> 要分清这招的**边界**：它省掉的是本机**单次峰值内存**和**霸占记账员窗口的时间**；每个 `step` 该发给模型的完整上下文依然是一字不少，账本越厚，上游模型吃进的 HTTP 总字节数照样往上涨。

---

## 网络断了重发不算重账：无跨表外键与纯应用层防重

为了防止多节点间乱序送达的 CRDT 同步包在回放时被数据库的级联约束和全局 `UNIQUE` 报错卡死，`v11` 账本**刻意去掉了所有跨表 `FOREIGN KEY` 约束以及共享同步表上的 SQL `UNIQUE` 约束**——`items_by_idempotency`（3 列复合索引 `(session_id, idempotency_key, id) WHERE idempotency_key IS NOT NULL`）、`tools_by_call` 和 `items_by_target` 都是普通的非唯一部分索引。但这绝不等于能乱记重复账：单机内只有独占 SQLite 记账员一条通道写入，防重、冲突校验与跨节点同键收敛全在写事务里精准卡死，用的是同一个朴素哲学——**认编号不认长相**：

| 场景 | 所用编号与位置 | 重复规则与跨节点收敛机制 |
| :--- | :--- | :--- |
| **客户端投信**<br>(`POST /sessions/{sid}/messages`) | HTTP 请求头 **`Idempotency-Key`**<br>（只准带一个头，去首尾空格后 1..=256 字节） | 写事务走 `items_by_idempotency(session_id, idempotency_key, id)` 索引按 **`ORDER BY id ASC LIMIT 1`** 挑出最小规范项（[src/store.rs](../src/store.rs)、[`reconcile_idempotency_key`](../src/cluster/ledger.rs#L909)）：<br>• 首次投递：返回 `201 Created` + 新 `id`<br>• 同编号同正文重发：返回 `200 OK` + 原 `id`，绝不重记<br>• **同编号塞了不同正文**：返回 `409 Conflict` 拦住<br>• **双节点离线同键撞车**：若两台离线节点恰好收过同一个 `Idempotency-Key` 产生两枚 `ItemId`，`SessionSync` 合流时 `reconcile_idempotency_key`、`GET /messages` 和撤回逻辑会把同 `(session_id, idempotency_key)` 归入同一**规范等价类**：只要等价类里任一副本已进上下文或已撤回，整组全部移出 `pending_inputs`；否则仅保留字典序最小的 `id` 留在门外排队，**绝不会重复执行两遍**<br>• **故意不带编号**：就算两句话一模一样，也当用户真想说两遍，照常记两笔 |
| **模型派工具**<br>(`function_call`) | 条目内的 **`$.call_id`** | 落库前走 `tools_by_call` 部分索引查询（[src/model.rs](../src/model.rs#L385-L420)）：<br>• 同会话同 `call_id` 同名同参：已经入账，安静跳过不重派<br>• **同 `call_id` 换了工具名或参数**：直接当异常报错拦住<br>• 换了新 `call_id`：哪怕参数一字不差，也算一次新调用 |

---

## 查询、撤回与安全删除会话

最后补全会话日常管理里的几个小机关（[src/api.rs](../src/api.rs#L225-L640)）：

- **切换分支视角 `PATCH /sessions/{sid} { active_head }`**：必须先停在非运行态，且严格校验目标节点必须存在于派生表 `session_leaves`（只认完整分支叶节点，防退回中间节点导致分叉混乱）；切换后会立刻刷新 `active_context`、递增视图令牌 `view_rev` 并广播 `session.reset`。
- **查排队信件 `GET /sessions/{sid}/messages`**：同 `reconcile_idempotency_key` 保持同一套规范等价类规则——对带 `idempotency_key` 的消息按同键聚合只展示最小 `id` 代表项，并联合检查等价类里是否已有任意副本进过 `context_nodes` 或挂上 `withdrawal` 撤回单，实时算出 `queued`、`included`、`withdrawn` 三种状态（注意：`included` 代表曾在**任意**分支被消费过），支持按 `id` 和 `status` 过滤及游标分页。
- **撤回消息 `DELETE /sessions/{sid}/messages/{mid}`**：如果正被远端 Runner 占有会先经 `mygo/exec/2` 转发过去；本机写事务里按同一套 `idempotency_key` 规范等价类检查——只要该消息（或其跨机同幂等键副本）还没被 `take_pending` 收进 `context_nodes` 即可撤回。如果已经进上下文，直接返回 `409 Conflict`（`"message already included"`）；如果已经撤回过再点撤回，直接幂等返回 `204 No Content`，不会写第二张撤回单（写完 `withdrawal` 会经 `on_item_inserted` 调 `reconcile_idempotency_key` 把整个同键等价类统一从 `pending_inputs` 摘掉）。
- **彻底删除会话 `DELETE /sessions/{sid}`**：
  - **先入 Watch 再抢 `Voter` 多数派删除门控**：入口处先在有集群同伴时调 `ensure_session_watched(sid)` 把会话正文拉齐，随后走复用 [`acquire_session_lease(..., LeasePurpose::Delete)`](../src/cluster/exec.rs#L3139) 的 [`acquire_delete_gate`](../src/cluster/exec.rs#L3857)——向全网全体在线节点发 `CheckOccupancy` 先看 `Occupancy` 回报：如果任何同伴已经记过该会话的 `deleted` 墓碑，立刻同步并退回 **`404`**；如果任何同伴的 `active_head_ts` 或因果向量领先本机，必须先跑 **`sync_session_strict`** 同步到最新状态（RPC 失败或截断则直接 `503`/`504` 拒绝删除）；确认无人占用且至少从 **`Voter` 多数派**拿到授权并取得有效期约 9 秒的本地 `DeleteGateGuard` 隔离租约（分区失多数派直接返 `503`，任何人占着会话或挂着后台任务返 `409`）；进 SQLite 事务后还会**再次核验 `delete_lease.is_valid()` 且无远端占有/已提交租约**，彻底封死预约超期后的并发提交窗口；
  - **写下 `deleted = 1` 规范墓碑并清理正文（[src/cluster/ledger.rs](../src/cluster/ledger.rs#L1151-L1204)）**：在事务内先把 `sessions` 行的 `deleted` 推向单调不可逆的 `1` 并洗成完全对称的**规范墓碑**（标题、`title_ts`、时间、状态、`status_ts`、`local_status`、`local_error`、`model_id`、`selection_ts`、`active_head`、`active_head_ts` 全部清空归零，仅递增本地 `sync_rev`），再顺序清空本地派生表和正文表：`active_context` -> `pending_inputs` -> `session_leaves` -> `turn_settled` -> `invalidated_warnings` -> `context_nodes` -> `items` -> `watched_sessions` -> `sync_vectors`。哪怕离线同伴重连后手里还留着这台已删会话的新老提交，收到 `deleted = 1` 墓碑也会跟着洗成完全一致的规范墓碑、清场正文并永不再收，**绝不会把旧副本“复活”回来**。
  - **留住花费账单并脱钩**：删会话**不会删历史花费账单**！程序先把这批 `llm_calls.session_id` 设为 `NULL`（同时这些 `llm_calls` 会随 `CatalogSync` 的因果向量继续在集群保留），所有模型调用与费用记录照常供全局核算。
  - **清光内存残留**：事务提交后，记账员当场从内存摘掉该会话的 MCP 连接（`s.mcp.forget_session(sid)`）、广播槽、唤醒弦、草稿流缓存，并向全局事件流广播 `session.deleted`。

---

## 多节点私有集群与半格律 CRDT 同步

前面看懂了一台机器里记账员怎么写盘，再往上推一层：如果你有笔记本、台式机、云端服务器好几台设备各自跑着 MyGo-Bot，怎么让它们既能零门槛单机开跑，又能在组网后像同一套系统那样自动同步状态，还绝不会因为网络分区而整出必须人工裁决的死锁账？

### 1. 单机即 1 节点集群：`local_node`、`voter_epoch`、`db_epoch` 与确定性破冰

在 MyGo-Bot 里，**没有“单机模式”和“集群模式”两套代码**：
- 首次开库建表（当前 schema 版本 **`PRAGMA user_version = 11`**，共 20 张表、2 个视图、0 个持久化触发器）时，程序在 `local_node` 表生成本机专属密钥（`iroh` Ed25519 节点身份 `node_id`）、初始成员代数 `membership_gen = 1`、一条 32 字节随机集群密钥 `cluster_secret`、投票角色 `role`（默认 `'voter'`，纯单机时可通过 `--cluster-role edge` 设为边缘节点）、初始持久化成员纪元与选民集（`voter_epoch = 1`、`voter_set_json` 与两阶段暂存字段 `pending_voter_epoch` / `pending_voter_set_json`）、持久化目录高水位 **`catalog_rev = 0`** 以及随机唯一的本地数据库纪元 **`db_epoch`**——此时你就是一台 **1 节点私有集群**；单机启动且未传入 `--cluster-bind` 时连 **`iroh` QUIC 端点都会通过 `OnceCell` 延迟按需绑定**（直到调用 `/cluster/invite` 或 `/cluster/join` 才真正开端口连网），日常写盘会先经 `known_peer_count() == 0` 原子快路径直接跳过集群锁、RPC 与 `ClusterSignal` 唤醒开销，保持与纯单机无异的极速吞吐与零后台网络消耗；一旦 `cluster_peers` 非空，启动时任何离线 `--cluster-role` 篡改角色都会被直接拒绝（必须在线经两阶段联合多数派变更调整，详见 [第 1 篇](01-agent-loop.md)）；
- 通过 `POST /cluster/invite` 生成一次性邀请码（打包了集群密钥与接入地址）；在另一台节点上调 `POST /cluster/join` 填入后：加入方先挂上 **`PresenceState::begin_join`** RAII 进群锁（阻止在入群半路再点新运行），在本地 `db.call` 核验当前没有活跃会话、待唤醒队列或后台任务（任一未停稳直接返回 **`409 Conflict`**）且核对是否已有同伴却切换了不同 `cluster_secret`（未传 `force: true` 同样先返 **`409 Conflict`** 防止误踢原有集群），接着经 **`JOIN_ALPN`**（`b"mygo/join/1"`）向邀请方发起 **`request_admission`** 准入请求（`AuthMsg::Admission`）。邀请方在 `admit_joining_peer` 经双向 HMAC 验明身份后，**在同一个 SQLite `db.call` 事务里先核对 `membership_gen`、`voter_epoch > 0` 与 `voter_set` 非空（同时检查若自己还是 0 同伴单机且手头挂着活跃会话、待唤醒或后台任务也一并拒绝），并立刻调用 `upsert_peer_if_gen` 登记新同伴、提交事务后才把当前 `(voter_epoch, voter_set)` 发回给加入方**——由于 `set_local_role` 降级最后一个孤立 `Voter` 时也走同一个独占 `sqlite` 线程并核对 `cluster_peers` 是否为空，两边彻底串行化，绝不可能出现“刚把节点放进群同时自己降成零 Voter”或“单机孤儿运行态被半路拉入集群”的竞态；密钥已作废或任意一端不满足准入条件时直接返 **`409 Conflict`**（不可达返 **`503`**），绝不让无效邀请污染本地 `cluster_peers`。加入方拿到准入回复后，在同一笔本地写事务内执行 `apply_join` 与 **`identity::adopt_voter_config(conn, epoch, &voters)`**（初次组网直接热切换密钥并保留本地数据；换群则传 **`force: true`** 保留本地数据、清空旧 `cluster_peers` 与 `peer_catalog_cursors`、原子写入准入返回的现行 `voter_epoch`/`voter_set`、写入新同伴、原子递增 `membership_gen` 并先调 `net.switch_membership` 立刻切断旧世代连接，再通知 `sync.on_membership_switched` 并在同步落库前经 `assert_membership_gen` 核对世代，跨世代旧包当场丢弃重取，杜绝换群瞬间还在半路的同步包跨世污染新集群）；另，`--cluster-local --cluster-bind` 在绑定端点前会先 `clear_ip_transports()` 确保不会混用其他默认传输地址；
- **双向 BLAKE3 keyed-MAC 握手 + 带角色的 PEX + 确定性破冰**：连上后双方互相拿 `cluster_secret` + 各自随机数算两轮 BLAKE3 keyed-MAC 验身份（`mygo-auth-srv` / `mygo-auth-cli`），验过身份后通过 **`ExchangePeers`** 做 **PEX（Peer Exchange）** 自动发现全网状同伴，并同步各自的 `role`、`meta_seq` 与 `addrs_ts`（单独区分直连实测地址与间接转播时间戳，避免陈旧转播覆盖新地址）；同一对节点若同毫秒互相拨通，就按 `node_id` 字典序破冰（只保留较小 `node_id` 主动拨出的那条连接），同时每节点同一时刻只允许一个 `DialGuard` 在途拨号。

### 2. 半格律 CRDT 与双轨同步（`CatalogSync` + `SessionSync`）

很多分布式系统靠一根头尾相扣的线性单链强求全网先后完全一致——只要两台机子断网各自改了一笔，链就当场劈叉卡死，只能弹窗逼着用户选边擦除另一边。MyGo-Bot 在 `v11` 选择了一条彻底免去人工选边的数学路径——**把整套同步模型设计成严格满足半格律（Semilattice）的 CRDT**：不管同步包按什么顺序到达（**交换律**）、分几批还是合一批到达（**结合律**）、重复收到多少遍（**幂等律**），任意两台节点合流完之后都能**自动算出完全一模一样的最终状态，永远没有“同步冲突”这个中间状态**！

具体实现拆成两条各司其职的增量轨道（[src/cluster/ledger.rs](../src/cluster/ledger.rs)、[src/cluster/sync.rs](../src/cluster/sync.rs)）：

1. **轨道一：`CatalogSync`（目录表 HLC 逐字段 LWW + `llm_calls` 因果向量）**：
   - **目录表怎么比新旧？——固定宽度 HLC 时间戳逐字段或原子组取大（LWW-Register）**：`llm_providers`、`llm_models`、`mcp_servers`（仅 `shared = 1`）和 `sessions` 元数据都给单个字段或语义绑定字段组配上混合逻辑时钟时间戳——供应商将 `base_url` 与 `api_key` 合进单一原子 **`endpoint_ts`**（防“新钥匙寄往旧地址”错拼），MCP 则使用四时间戳 **`config_ts`、`auth_config_ts`、`oauth_epoch_ts`、`oauth_ts`** 与端点指纹防凭据串用（见 [第 6 篇](06-mcp-and-oauth.md)），会话携带 `title_ts`、`status_ts`、`selection_ts`、`active_head_ts`。格式固定为 **`"{wall_ms:016x}:{logical:08x}:{node_id}"`**，还专门用 **`MAX_HLC_WALL_MS`（`1 << 60`）** 守住十六进制固定宽度上界、并通过单次按表查询的 [`seed_hlc_from_db`](../src/cluster/ledger.rs#L177) 对齐水印，杜绝异常大系统时间撑破定长字典序；正常时间戳直接按字符串字典序大小严格全序可比，哪怕同微秒同逻辑刻撞上也是 `node_id` 全网统一决胜负；删除则一律采用只增不减的 `0 -> 1` 规范墓碑。
   - **同伴怎么知道从哪补拉？——持久化 `local_node.catalog_rev` + `db_epoch` 与两阶段有界差量读取**：本地高水位不再放在重启靠扫描重算的内存计数器里（删会话清表也不会让水位倒退！），而是持久化在 **`local_node.catalog_rev`**，每次变更在写事务内通过 [`next_catalog_rev`](../src/cluster/ledger.rs#L270) 原子递增写入各行 `sync_rev`，绑着开库生成的一次性 **`db_epoch`**。同伴在 `peer_catalog_cursors` 里记住上次拉到哪个 `last_remote_rev` 后，发起 `RpcReq::CatalogSync { peer_db_epoch, since_rev, calls_vector }`。发送方调用 `read_catalog_delta`（[src/cluster/ledger.rs](../src/cluster/ledger.rs)）走**两阶段有界读取**：第一阶段只扫索引行、顺便用 SQLite `length(CAST(... AS BLOB))` 对所有可变字段精确估算字节大小（其中 `sessions` 计入被 `8192` 字节钳制的 `last_error` 与 `reasoning_effort`，`llm_calls` 计入被 `1024` 字节钳制的 `upstream_response_id`、`8192` 字节钳制的 `error_summary`、`64 KiB` 钳制的 `raw_usage`、`reasoning_effort` 与 `origin_node_id`），先算好 **256 行**（`DELTA_PAGE_LIMIT`）与 **1.5 MB**（`MAX_DELTA_BYTES`）限制下的最小安全截断线 **`safe_ceiling`**，第二阶段再只按最终入围的主键去读完整行（并在读写与入站合流两侧同步执行字段钳制），既防翻漏页又防单批大字段撑爆预算或拖住共享 SQLite 线程。
   - **`llm_calls` 捎带在 `CatalogSync` 里按因果向量追更**：调用花费账单本身是只增（或由 `origin_node_id` 就地结算后递增 `origin_seq`）的记录，因此在 `sync_vectors(scope = "calls")` 里记下每个来源节点见过的最高 `origin_seq`，随 `CatalogSync` 的剩余字节预算一并增量补齐。
2. **轨道二：`SessionSync`（上下文 DAG、因果向量差量与超大负载分块接力）**：
   - 会话正文包含四张只增或增量合并的因果表：**`items`**、**`context_nodes`**、**`turn_settled`** 和 **`invalidated_warnings`**，每行都刻着永久身份 `(origin_node_id, origin_seq)`。
   - 订阅了某场会话（登记进 `watched_sessions`）的节点在 **`RpcReq::SessionSync { session_id, vector }`** 里递上自己本地的 **`sync_vectors(scope = "session:<sid>")`** 向量（记住每个来源节点我已拿到第几号）；对方经 [`read_session_delta`](../src/cluster/ledger.rs#L3116) 同样按**两阶段有界读取**用 `length(CAST(payload AS BLOB))` 先挑切片——凡是单条正文不超过 **1,000,000 字节**（`MAX_INLINE_ITEM_PAYLOAD_BYTES`）的普通 `ItemWire` 直接内联带回；一旦遇上超过 1,000,000 字节的超长 `item`（比如巨型代码或工具输出），delta 里只发 **`payload = ""` 占位**外加 **`payload_len` 与 32 字节 BLAKE3 `payload_hash`**，接收方落库前专门发起 **`RpcReq::GetItemPayloadChunk`** 按 **256 KiB** 分块接力拉齐（上限 **32 MiB** `MAX_ITEM_PAYLOAD_BYTES`），验过整包 BLAKE3 与 UTF-8 后再进事务落库。
   - 接收方在 [`apply_session_delta`](../src/cluster/ledger.rs#L2415) 内先严密交叉核对 **`delta.session_id == sid`** 及每行 `session_id`，全部落库改用**定点主键/唯一索引的 `ON CONFLICT(...) DO NOTHING`** 替代笼统的 `INSERT OR IGNORE`（防止把本该暴露的 `CHECK` 或类型异常静默吞掉！），顺手增量刷新 `pending_inputs`（按规范等价类收敛同 `idempotency_key`）、`session_leaves` 与新附件元数据的去重别名挂载。`session_branch_complete`（[src/cluster/ledger.rs](../src/cluster/ledger.rs#L692)）先走 **O(1) `active_context` 尖端与 `position=1` 快路径**，未命中才回溯全链；`reconcile_active_context` 若检测到活跃分支切换或清零会设 `reset_view = true`，若老的无分支草稿被插进旁支也会设 `reset_view = true`，而**一旦新收到的可见项比原先屏幕上已可见的最大 `item_id` 更小（乱序补到旧项）**则会标 `out_of_order_view = true`——两类情况都会递增 `session_view_rev`，且发给 SSE 的 `new_items` 只保留属于活跃分支或未入枝的可见项（并在 `publish_catalog_merge` 检测到远端切分支时通过 [`abort_active_session_worker`](../src/cluster/sync.rs#L2323) 立刻中止旧本机 Runner、推带新 `view` 的 `session.reset`）。
3. **细粒度 peer 锁、高并发扇出、严格强同步与 30 秒反熵**：
   - 本地写盘提交后由 `NotifyDirty { catalog, sessions }` 主动叫醒同伴拉取；同步引擎去掉了会跨网络 RPC 持久化的全局目录大锁，改用**按 peer 粒度的 `catalog_locks: LockTable<String>`** 与**按 `(sid, peer_id)` 粒度的 `session_locks`**，搭配 **`buffer_unordered(4)`** 并发扇出拉取。
   - 远端入站队列 `InboundState` 由 **4 个固定工人协程** + 退避重试队列处理，待拉条目封顶 **4096**（`MAX_INBOUND_QUEUE`）并对同 `(mgen, sid)` 合并记录所有脏同伴——[`pop_inbound_work`](../src/cluster/sync.rs#L528) 每次弹出一位 `peer_id` 若队里还剩其他待拉同伴会把该会话放回队尾继续排，绝不因某一个慢/断连 peer 连累丢掉另一位 peer 发来的同一会话更新。
   - 正常背景反熵保持 **30 秒周期**（`ANTI_ENTROPY_INTERVAL`）；而在执行开跑或删会话前走的 [`sync_session_strict`](../src/cluster/sync.rs#L1497) 则绝不吞错——任一在线同伴 RPC 失败返 **`503`**、连续拉满 1024 页仍没追平（`!caught_up`）返 **`504`**；而 [`sync_now`](../src/cluster/sync.rs#L1587) 与 [`ensure_session_watched`](../src/cluster/sync.rs#L1756) 只有在查明该会话确已存在且 `deleted = 0` 时才写入 `watched_sessions`（不存在或已墓碑返回 **`404`**，同伴全失联或翻页截断返回 `503`/`504`，绝不把垃圾假 ID 写脏本地表）。网络 RPC 依然严格受限于 2 MiB 请求头 / **12 MiB** 响应头 / 2 MiB 体上限、32 MiB 全局帧字节预算，超过 **64 KiB** 的 JSON 编解码自动卸载到最多 4 路的阻塞线程池。

### 3. 四档分级自动同步策略

为了既能随时随地查看目录，又不把几百兆没看过的对话正文和媒体字节盲目塞满每台机器，集群按资源重要性和体量分了四档策略：

| 资源类型 | 同步节奏与机制 | 省掉会出什么问题 |
| :--- | :--- | :--- |
| **供应商、模型、会话目录头、`llm_calls`** | **即时全量 `CatalogSync`**：目录字段按 HLC LWW 取新（`endpoint_ts`、`selection_ts` 等）、`llm_calls` 按 `calls` 因果向量追更，借 `local_node.catalog_rev` 两阶段有界增量拉取，每次变动主动发 `NotifyDirty` 给在线同伴 | 不同步目录，别的节点 WebUI 看不到全集群有哪些模型、哪些会话、当前 `active_head` 与总费用账单 |
| **会话正文**（`items`、`context_nodes`、`turn_settled`、`invalidated_warnings`） | **按需 Watch 懒同步（`watched_sessions` + `SessionSync`）**：新会话默认只在 `sessions` 表挂着轻目录头；直到确认会话未删且存在并进入查看详情、历史、上下文、订阅 SSE 或投信/撤回/点火/暂停/改删会话时自动 `ensure_session_watched` 记进 `watched_sessions` 并发起 `SessionSync` 按因果向量拉齐、超大 `item` 走 `GetItemPayloadChunk` 分块接力，从此持续追更 | 全量预推所有历史对话正文会瞬间撑爆小设备存储和同步带宽 |
| **多模态附件**（`attachments`） | 仅元数据随 `SessionSync` 同步；`data` 字节默认置空，下载或发模型前先在本地库查同 `content_hash` 复用，没有则通过 SQLite **`blob_open` 增量零拷贝读取**向在线持有者发起 **256 KiB 多节点并发分块（BT 风格）** 接力拉取，并在阻塞池校验 BLAKE3（详见 [第 3 篇](03-catalog-media-and-billing.md)） | 提前广播大图片/音视频会拖垮日常消息与控制信令 |
| **MCP 服务与 OAuth**（`mcp_servers` / `mcp_oauth`） | 由单服务 **`shared`** 开关控制，授权/拒绝凭证带 `config_ts`/`auth_config_ts`/`oauth_epoch_ts`/`oauth_ts` 与端点指纹 `auth_fingerprint` 随 `CatalogSync` 同步，进行中 `pending` / `pending_refresh` 严格留在本地；取消同步或删除共享服务时写入单调 `deleted = 1` 墓碑并洗净 `config_json` 与 `mcp_oauth` 凭据，且跨节点刷新走 `OAuthRefresh` 单点协调（详见 [第 6 篇](06-mcp-and-oauth.md)） | 一刀切同步会把纯本机命令行 MCP 或半途一次性的登录回调乱推到全集群 |

### 4. 离线并发怎么办？——无冲突自动合流与 DAG 分支并存

如果节点 A 和节点 B 在断网离线期间各自做了操作，重连后会发生什么？因为整套底座从设计上就满足半格律，**再也没有“冲突告警、锁表 409、弹窗等人选一边覆盖另一边”这套麻烦**：

- **改了目录与设置（供应商 / 模型 / 共享 MCP / 会话标题或选模）**：每组字段各自带着精确的 HLC 时间戳 `*_ts`，逐字段或原子组（如 `endpoint_ts`、`auth_config_ts`）挑出最新写的那份保留；改标题和换模型并发不互相打架，删资源则靠单调 `0 -> 1` 对称规范 `deleted` 墓碑全网统一落定。
- **离线向同一会话投信或用 `force_offline: true` 同时开跑**：所有聊天流水账 `items` 按 `(origin_node_id, origin_seq)` 无损并集全部收齐；即便两机曾为同一 `Idempotency-Key` 写入两条同键消息，也会自动归进最小 `id` 规范等价类绝不重复跑两次；各自追加的 `context_nodes` 顺着共同祖先 `parent_id` 自然长成**两条并存的 DAG 对话分支**，两边叶节点全都稳稳登记在 `session_leaves` 与 `Session.branches` 里！`sessions.active_head` 会按照 `active_head_ts` 自动选出一个一致的默认活跃视角，另一边走过的内容也没有丢一个字——你随时都能在 WebUI 顶栏或调 `PATCH /sessions/{sid} { "active_head": "ctx_..." }` 一秒切回另一条叶节点分支继续看或接着往下聊。

---

**上一篇**：[01 · 核心抽象与 Agent 循环](01-agent-loop.md) · **返回目录**：[总览](README.md) · **下一篇**：[03 · 模型目录、多模态附件与精确计费](03-catalog-media-and-billing.md)
