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

并且记账员死守一条发布顺序：**所有对外部的回信、唤醒信号、SSE 广播，以及触发集群同步的流变动通知，一律按 FIFO 顺序排在外层事务 `COMMIT` 彻底落稳之后才发**（代码里 `apply` 和 `publish` 两段回调就是干这个用的）。每一笔写单提交哈希链变更时，先把涉及的 `stream_id` 暂存在记账员线程局部缓冲区 `SYNC_STAGED` 里，跟它自己的 `savepoint` 同生共死——哪个 `savepoint` 失败回滚就当场丢弃它暂存的流 ID，只有等整车外层事务 `COMMIT` 真正落盘后，记账员才统一收拢交给有界合并信号器 `ClusterSignal::notify_streams` 唤醒后台同步循环。哪怕客户端在 HTTP 返回前突然断线，或者半路有一两单被回滚，既不漏播已落盘的提交，也绝不会把回滚了的脏状态广播给集群同伴。

### 细节二：谁绝不能拼进同一趟车？——`Write::can_join`

看代码 [src/store.rs](../src/store.rs#L37-L44) 里的规则，写单分成“普通写”和“启动运行写（`start_session`）”两类，**它们绝不混批**：
- 普通写（比如投信、建目录、保存模型）只能和普通写拼车；
- **最关键的是同一个会话被两个客户端同时点 `PUT /run` 点火**：如果两笔针对同一个会话 `sid` 的点火被塞进同一个外层事务里，第一笔虽然在 SQLite 里改了 `status='running'`，但外层事务还没 `COMMIT`，它的 `publish` 阶段还没来得及把 worker 登记进内存 `s.active` 字典，紧接着第二笔在同一批里一看 `s.active` 还是空的，就会误以为“还没人跑”，再派一个重复 worker 出来！
- 所以规则写得死死的：**不同会话的点火可以拼车，同一个会话的两次点火绝不能进同一车**——第一车提交并把 worker 登记进 `s.active` 之后，第二车进门一眼看见活动 worker 已经在了，直接跳过开新循环、原样返回当前会话状态。顺便一提，在优雅退出阶段（`self.closing == true`），点火批次还会直接拒绝开新单，统一返回 `503 Service Unavailable`（`"shutting down"`）。
- **还要防着过期老 worker 死灰复燃（运行世代栅栏）**：假如会话正跑在半路，突然碰上集群全量覆盖或删除会话，旧 worker 虽然收到了取消信号，但手里已经攥着一笔算完的工具账单正准备排进写通道。为了绝不让“上一任过期打工人”写坏新历史、错删后来新点火 worker 的 `s.active` 登记，记账员维护了一本 `Store.session_run_gen` 运行世代账：每次会话点火都先递增世代号 `run_gen` 并钉在 `Control` 上，该 worker 后续每一笔工具落账与终态写库都经 `Db::write_for_run` 附带 `expected_run_gen`，记账员在 `commit_batch`（[src/store.rs](../src/store.rs#L390)）里先核对与当前 `session_run_gen` 一致才放行，否则当场拒写；同时集群覆盖或删会话时会调 `abort_active_session_worker`（硬取消当前 worker、停掉名下 codemode 任务、从 `s.active` 摘除并递增 `run_gen`），`clear_session_body_rows` 也会把滞留的 `'running'`/`'pausing'` 状态先归位为 `'paused'` 再重放胜方快照——哪怕是在同 `epoch` 里选 `cluster` 覆盖，过期 worker 也既写不进新账，也不会把会话卡死在 `running`。

---

## 账本厚时不堵门：钉死页尾，分块接力读

搞定了写，再看读。假如某个长会话已经在 `items` 和 `context_items` 里攒了好几千条记录（甚至夹着几兆的多模态图片附件），这时候前端来翻 `/history` 或 `/context`，或者 Agent 循环准备把整本上下文拼成 JSON 喂给上游模型——如果让记账员一口气全读出来拼成几十兆的字符串再交出去，不仅当场撑爆内存，记账员窗口还会被死死占住好久，别人连一封小信都投不进来。

所以程序在**对外翻页接口**（`/sessions/{sid}/history`、`/sessions/{sid}/context`，每页默认 **50 条**、最多 **200 条**）和**对上喂模型**（`JsonRows::ModelInput`）上，直接复用了同一套 **`JsonRows` 流式分块读法**（[src/store.rs](../src/store.rs#L797-L955)）：

1. **三选一翻页与先钉死终点**：
   - **先分清两类分页接口**（`limit` 统一限制在 `1..=200`）：只有 `/sessions/{sid}/history` 和 `/sessions/{sid}/context` 完整支持 `after`、`before`、`tail=true` 三种互斥游标模式（最多传一个），其中 `/history` 传 `itm_...` 形式的 **TypeID 字符串**，而 `/context` 传该会话内从 `1` 开始连续递增的整型 `position`（`after >= 0`、`before > 0`）。至于 `/sessions`、`/calls` 和 `/sessions/{sid}/messages` 列表接口，**仅支持向后翻页的 `after` 游标**（分别传 `ses_...`、`cal_...`、`itm_...` TypeID 字符串），不接受 `before` 或 `tail`。
   - 真正取数据前，记账员先花一瞬间的读操作把本次范围 `(start, end)` **钉死**：查流水账 `/history` 钉的是 `items.id`，查上下文 `/context` 和喂模型钉的是 `context_items.position`。（注意两边游标**绝不能混用**，因为并发工具归进上下文的 `position` 顺序和原 `items.id` 顺序可能不同。）终点一钉死，哪怕下一毫秒门外又新落盘了几十条记录，编号也全都落在 `end` 外面，留给下次再翻。
2. **一瓢一瓢往外舀**：
   - 接着从 `start` 到 `end`，每次只请记账员舀**一小瓢**：最多 **256 行**，或者字节攒到大约 **64 KiB** 就立刻打住（如果单条记录本身超过 64 KiB，也会完整放行这一条不卡死）；
   - 舀出这一瓢立刻松开记账员窗口，通过 `async_stream` 交给网络流往下发；发完这瓢再去通道排队舀下一瓢，直到抵达终点 `end` 才合上末尾括号。
   - **喂模型时的动态展开**：如果是 `ModelInput` 通道，在舀每一瓢的时候遇到 `user_message`、`function_call_output` 或 `codemode_event`，才当场去 `attachments` 表里捞出对应媒体附件转成 Base64（见 [第 3 篇](03-catalog-media-and-billing.md)），既不提前在库里存冗余 Base64，也不用一口气憋出几十兆的请求体缓冲区。

> 要分清这招的**边界**：它省掉的是本机**单次峰值内存**和**霸占记账员窗口的时间**；每个 `step` 该发给模型的完整上下文依然是一字不少，账本越厚，上游模型吃进的 HTTP 总字节数照样往上涨。

---

## 网络断了重发不算重账：信封编号与调用编号

落盘算清了，还得防一种日常事故：**账其实已经记上了，但回执在半路断网了，发起方以为没收到就重发一遍。** MyGo-Bot 在客户端投信和模型派工具这两端，用的都是同一个朴素哲学——**认编号不认长相**：

| 场景 | 所用编号与位置 | 重复规则与冲突判定 |
| :--- | :--- | :--- |
| **客户端投信**<br>(`POST /sessions/{sid}/messages`) | HTTP 请求头 **`Idempotency-Key`**<br>（只准带一个头，去首尾空格后 1..=256 字节） | 借 `items(session_id, idempotency_key)` 部分唯一索引：<br>• 首次投递：返回 `201 Created` + 新 `id`<br>• 同编号同正文重发：返回 `200 OK` + 原 `id`，绝不重记<br>• **同编号塞了不同正文**：返回 `409 Conflict` 拦住<br>• **故意不带编号**：就算两句话一模一样，也当用户真想说两遍，照常记两笔 |
| **模型派工具**<br>(`function_call`) | 条目内的 **`$.call_id`** | 借 `items(session_id, json_extract(payload,'$.call_id'))` 部分唯一索引：<br>• 同会话同 `call_id` 同名同参：已经入账，安静跳过不重派<br>• **同 `call_id` 换了工具名或参数**：直接当异常报错拦住<br>• 换了新 `call_id`：哪怕参数一字不差，也算一次新调用 |

---

## 查询、撤回与安全删除会话

最后补全会话日常管理里的几个小机关（[src/api.rs](../src/api.rs#L241-L510)）：

- **查排队信件 `GET /sessions/{sid}/messages`**：通过左连接撤回单 `withdrawal` 和上下文 `context_items`，实时把每条 `user_message` 算出 `queued`、`included`、`withdrawn` 三种状态，支持按 `id` 和 `status` 过滤及游标分页。
- **撤回消息 `DELETE /sessions/{sid}/messages/{mid}`**：先确保会话已进入 Watch，然后在独立 `Db::write` 事务里检查与写入——只能撤回还没被 `take_pending` 收进上下文的 `user_message`。如果已经进上下文，直接返回 `409 Conflict`（`"message already included"`）；如果流处于分叉冲突中，savepoint 回滚并返回 `409`，不留半截记录；如果已经撤回过再点撤回，直接幂等返回 `204 No Content`，不会写第二张撤回单（`UNIQUE(kind, target_id)` 也兜底锁死了这一点）。
- **彻底删除会话 `DELETE /sessions/{sid}`**：
  - **先入 Watch 再查禁区**：入口处先调用 `ensure_session_watched(sid)` 把会话正文拉齐（免得在只有半份目录头的状态下误删），进事务后再检查不能处于冲突未裁决态（`409`）；只要会话正在 `running`/`pausing`，或者 `s.active` 还在跑，或者名下还有活着的 `codemode` 任务，一律返回 `409 Conflict` 拒删——必须先停下来。
  - **外键有序清表**：在事务里严格按外键依赖依次清理 `context_items` -> `invalidated_warnings` -> 带 `target_id` 或 `codemode_call_item_id` 的衍生 `items` -> 其余 `items`。
  - **留住花费账单并记录脱钩**：删会话**不会删历史花费账单**！程序先把这批 `llm_calls.session_id` 设为 `NULL`（同时把这几笔脱离关联的 `llm_calls` 各自记进同步提交，并把 `detached_call_ids` 一起写入 `session_delete` 提交载荷，确保远端按终态墓碑删除时也不会卡在外键上），保留所有模型调用和费用记录供全局核算，然后才删 `sessions` 行，并在该会话流上写入一笔永久 `session_delete` 墓碑（`deleted = 1`），保证离线节点重新上线后也会跟着彻底删掉它、绝不会把旧副本“复活”回来。
  - **清光内存残留**：事务提交后，记账员当场从内存摘掉该会话的 MCP 连接（`s.mcp.forget_session(sid)`）、广播槽、唤醒弦、草稿流缓存，并向全局事件流广播 `session.deleted`。

---

## 多节点私有集群与哈希链同步（Stage 1）

前面看懂了一台机器里记账员怎么写盘，再往上推一层：如果你有笔记本、台式机、云端服务器好几台设备各自跑着 MyGo-Bot，怎么让它们既能零门槛单机开跑，又能在组网后像同一套系统那样自动同步状态，还绝不算乱账？

### 1. 单机即 1 节点集群：`local_node`、成员代数与确定性破冰

在 MyGo-Bot 里，**没有“单机模式”和“集群模式”两套代码**：
- 首次开库建表（当前 schema 版本 `PRAGMA user_version = 5`）时，程序在 `local_node` 表生成本机专属密钥（`iroh` Ed25519 节点身份 `node_id`）、初始成员代数 `membership_gen = 1` 和一条 32 字节随机集群密钥 `cluster_secret`——此时你就是一台 **1 节点私有集群**；
- 通过 `POST /cluster/invite` 生成一次性邀请码（打包了集群密钥与接入地址）；在另一台节点上调 `POST /cluster/join` 填入后：如果本机还没有任何集群同伴，会直接热切换密钥并合并保留本地数据接入；如果本机**已经存在 `cluster_peers`** 且邀请码更换了 `cluster_secret`，默认返回 **`409 Conflict`** 防止误踢原有集群，必须明确传入 **`force: true`** 才执行切换（保留本地数据、清空旧 `cluster_peers` 与 `sync_staging_commits` 暂存表、写入新同伴、原子递增 `membership_gen` 并经 `switch_membership` 立刻切断所有旧世代连接），同时 `sync.rs` 里每一笔远端应用与暂存写库都在 SQLite 事务内校验 `identity::assert_membership_gen`，杜绝切换瞬间还在半路的旧世代同步包跨世污染新集群；另，`--cluster-local --cluster-bind` 在绑定端点前会先 `clear_ip_transports()` 确保不会混用其他默认传输地址；
- **双向 BLAKE3 keyed-MAC 握手 + 区分来源的 PEX + 确定性破冰**：连上后双方互相拿 `cluster_secret` + 各自随机数算两轮 BLAKE3 keyed-MAC 验身份（`mygo-auth-srv` / `mygo-auth-cli`），验过身份后做 **PEX（Peer Exchange）** 自动发现全网状同伴，并单独区分直连实测地址与间接 PEX 转发时间戳（`addrs_ts`），避免陈旧转播覆盖新地址；同一对节点若同毫秒互相拨通，就按 `node_id` 字典序破冰（只保留较小 `node_id` 主动拨出的那条连接），同时每节点同一时刻只允许一个 `DialGuard` 在途拨号。

### 2. `(epoch, seq)` 哈希链、有界 RPC 与 8 阶段外键安全同步

跟很多框架整库拷贝 SQLite 文件不同，MyGo-Bot 把每条需要同步的业务实体（供应商、模型、共享 MCP、会话、调用账单）拆成各自独立的一条**可同步流（`sync_streams`）**，所有写变动追加进 **`sync_commits`**：
- **提交链、“已拉齐指针”、持久订阅意图与增量校验**：每一笔提交编号为 `(epoch, seq)`，`commit_hash` 用 BLAKE3 把 `stream_id`、`kind`、`epoch`、`seq`、`parent_hash`、`author_node_id`、`op_type`、`payload` 和时间戳全部锁在一起；`sync_streams` 表把远端通告的最新头 `(epoch, head_seq, head_hash)`、本机真正验证并写完的正文前沿 `(synced_epoch, synced_seq, synced_hash, checkpoint_seq, retired)`，以及专管全量重写版本的 **`reset_gen`**（每逢会话正文全量覆盖就单调自增，供 SSE `history_rev` 对齐）分开记账——目录节点虽然看见远端 `head_seq` 涨了，但只要 `synced_seq` 没跟上、`checkpoint_seq != 1` 或局部校验不过，[record_commit_with_epoch](../src/cluster/ledger.rs) 一律拒绝盲目在残缺链上继续追加（彻底杜绝制造缺前缀“伪完整链”）；同时 **`watched` 只记录持续订阅意图**（跨多页追更中间页或刷新目标头时绝不清零 `watched`，正文到底齐不齐统一由 `is_body_complete()` 对比 `(synced_epoch, synced_seq, synced_hash)` 判定）。热路径上的高频会话追加也不再把整本历史读回内存重算 O(N²) 哈希，改由 `verify_local_tip_commit` 以 O(log N) 代价只核验 `seq = 1` 与当前头提交哈希，并基于上一份 `head_snapshot` 做 `required_modalities` 增量并集（`media::item_required_modalities`）；逐行流式全链核验 `verify_local_prefix` 仅保留给启动恢复和 `resolve_conflict_local` 等低频重操作；
- **出站合并、有界通道、大提交分片与严格 8 阶段外键回放**：本地流变更经有界 `ClusterSignal` 合并去重；远端入站队列 `InboundState` 由 **4 个固定工人协程** + 退避重试队列处理，全局总排队条数严格封顶为 **4096**（`MAX_INBOUND_QUEUE`）并对重复 `(from_peer, stream_id)` 合并去重。所有网络 RPC 严格受限于 2 MiB 请求头 / 6 MiB 响应头 / 2 MiB 体上限、32 MiB 全局帧字节预算与全局/单同伴并发信号量，大响应序列化的阈值也按字节数（**64 KiB** `OFFLOAD_BYTES_THRESHOLD`）而非提交条数决定是否卸载到 `spawn_blocking`；当单条合法提交（例如模型一次性吐出 7 MiB 长文本）超过 **1.5 MB**（`MAX_INLINE_COMMIT_PAYLOAD_BYTES`）内联限制时，`read_commits_page` 会返回 `oversized_next` 元数据，转由 `RpcReq::GetCommitChunk` / `RpcResp::CommitChunk` 按 **512 KiB**（`COMMIT_CHUNK_BYTES`）分片流式拉取并在拼装后校验 BLAKE3 承诺哈希（首次 Watch、日常追更、冲突分支补全与 `cluster` 覆盖共用同一条分片通路）。系统通过 **30 秒周期的分页清单（`GetManifest`）反熵**对齐状态（未 Watch 目录头 `apply_unwatched_session_head` 推进时必须验证明确通过同纪元祖先证明 `verified_ancestor_same_epoch == Some(true)`，且优先向完整正文持有者 `full_holders` 核对哈希），回放依赖严格分为 **8 个阶段**：
  `0: Provider -> 1: Model -> 2: Mcp -> 3: SessionHeads（轻量目录头与分叉校验）-> 4: AttachedCalls -> 5: Terminal Session Deletes -> 6: DetachedCalls -> 7: Watched Session Bodies`。
  整纪元大跨度替换会先写入 `sync_staging_commits` 暂存表，等整链哈希连续验证通过后再于单个 SQLite 事务内原子切换正表，并将规范化的 `SessionSnapshot`（含 `required_modalities`）一并锁定进提交与 `head_snapshot`。

### 3. 四档分级自动同步策略

为了既能随时随地查看目录，又不把几百兆没看过的对话正文和媒体字节盲目塞满每台机器，集群按资源重要性和体量分了四档策略：

| 资源类型 | 同步节奏与机制 | 省掉会出什么问题 |
| :--- | :--- | :--- |
| **供应商、模型、会话目录头、`llm_calls`** | **即时全量自动同步**：每次变动主动推头给在线同伴 | 不同步目录，别的节点 WebUI 看不到全集群有哪些模型、哪些会话与总费用账单 |
| **会话正文**（`items`、`context_items`、`invalidated_warnings`） | **按需 Watch 懒同步**：新会话初始在 `sync_streams` 只存轻量 `head_snapshot`，设 **`watched = 0, synced_seq = 0`**；直到某节点查看详情、历史、上下文、订阅 SSE 或进行投信/撤回/点火/暂停/改删会话时自动 `ensure_session_watched`，把 `watched` 置 `1` 拉齐历史并从此自动追更 | 全量预推所有历史对话会瞬间撑爆小设备存储和同步带宽 |
| **多模态附件**（`attachments`） | 仅元数据随 Watch 同步；`data` 字节默认置空，下载或发模型前先在本地库查同 `content_hash` 复用，没有则通过 SQLite **`blob_open` 增量零拷贝读取**向在线持有者发起 **256 KiB 多节点并发分块（BT 风格）** 接力拉取，并在阻塞池校验 BLAKE3（详见 [第 3 篇](03-catalog-media-and-billing.md)） | 提前广播大图片/音视频会拖垮日常消息与控制信令 |
| **MCP 服务与 OAuth**（`mcp_servers` / `mcp_oauth`） | 由单服务 **`shared`** 开关控制，授权/拒绝凭证同步、进行中 `pending` 态不同步；取消同步或删除共享服务时写入高 `epoch` 永久墓碑并清净旧历史含密提交（详见 [第 6 篇](06-mcp-and-oauth.md)） | 一刀切同步会把纯本机命令行 MCP 或半途一次性的登录回调乱推到全集群 |

### 4. 分叉冲突检测、凭据脱敏与双向人工裁决

要是节点 A 和节点 B 在断网离线期间同时改了同一条流（同一个 `epoch` 里沿着相同的历史长出了不同的 `(seq, commit_hash)`，或者独立新建了同名的供应商/模型/共享 MCP），系统绝不自作主张盲目覆盖或半吊子合并：
1. 同步引擎检测到分叉（包括多候选之间通过 `GetCommitHash` 校验出的共同祖先分歧，以及同序号不同哈希碰撞）后，把内部重放数据保存在库内、向 HTTP **`GET /cluster/conflicts` 只吐出脱敏后的白名单摘要（`remote_summary` + `related_local_id`）** 绝不泄露 `api_key` / `headers` / `env` / OAuth 凭据，同时向全局 SSE 推送 **`sync.conflict`**，并**立刻用 `409 Conflict` + savepoint 回滚锁住该流在本机的写操作**，防止在分叉之上越写越多或留下半截脏写；
2. 人工只在 WebUI 或 `/cluster/conflicts/{stream_id}/resolve` 选两个干脆方向之一（带 CAS 冲突快照校验防并发误裁）：
   - **`local`（保留本地强推集群）**：先在本机事务内验证持有完整连续前缀与目标头哈希；如果本机是只有 `watched = 0` 目录头的会话节点，必须先向持有该本地分支正文的在线同伴拉齐完整分支落进正表（`commit_staged_branch_preserving_conflict`）——**拉不齐就返回 `409` 绝不发布空正文高 `epoch` 抹除集群历史**！确认完整后才把 **`epoch` 推到 `max(local_epoch, remote_epoch) + 1`** 打出完整快照提交并广播覆盖同伴；
   - **`cluster`（采用集群覆盖本地）**：清掉本地分叉链与冲突投影，向验证过与冲突记录一致（或其直系后代）的持有者拉齐并完整重放覆盖（同时支持**同高度编辑 vs 删除**相撞时接受删除：`apply_terminal_session_delete` / `apply_terminal_mcp_delete` 在核对远端匹配 `expected_conflict` 后直接放行，并在事务内先无条件删除 `sync_conflicts` 再合并通知标记，绝不漏清残留冲突）；若是**非删除类**的会话覆盖，只清空旧正文行而**保留既有 `llm_calls.session_id` 关联**；若撞上唯一名字冲突，则在重放时自动归档或重命名本地碰撞项；
3. **离线裁决回执表（`sync_resolutions`）防滞留卡死**：人工裁决完不仅广播实时通知，还会把胜方凭证记进有界表 **`sync_resolutions`**（按时间最多保留最新 **2048** 条 `ResolutionReceipt`）。如果有同伴裁决当时恰好离线没接到通知，等它重连恢复、且发现本地 `sync_conflicts` 仍有未决项时，同步循环会在常规清单对账前先调 **`RpcReq::GetResolutions` / `RpcResp::Resolutions`** 拉取裁决回执，核对本地头对齐胜方后自动清理滞留冲突。

---

**上一篇**：[01 · 核心抽象与 Agent 循环](01-agent-loop.md) · **返回目录**：[总览](README.md) · **下一篇**：[03 · 模型目录、多模态附件与精确计费](03-catalog-media-and-billing.md)
