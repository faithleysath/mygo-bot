# 05 · Codemode 下篇：前后台分流、原地等信、实时透视与租期恢复

[第 4 篇](04-codemode-sandbox.md)搭好了安全隔离的 QuickJS 车间。可紧接着就会撞上第二道更棘手的坎：

**在 [第 1 篇](01-agent-loop.md) 的循环规矩里，一个 `step` 必须等所有派出去的工具都交齐结果才能收尾进下一轮；可一旦脚本跑起耗时好几分钟甚至半小时的编译、安装或监控，死等就把整场会话卡死了。要是先放行转去后台，模型手头没别的活时怎么忍住不去起新脚本瞎轮询？怀疑命令卡在后台了怎么隔窗偷看进度？会话早就睡进 `idle` 了谁来重新点火？脚本写死循环霸占工人又怎么收场？**

顺着这几个问题从底往上拆，六个工具、前后台分流、门外双信箱、原地等信与透视窗、限时租期和暂停恢复就能全串成一条线。

---

## 六把模型工具与两只 HTTP 遥控器

模型在每一轮里能看到的工具一共只有这 6 个（定义在 [src/tools.rs](../src/tools.rs)），另外 HTTP API 和前端界面还额外配了两个管理接口：

| 入口 | 归属 | 作用 | 参数 / 约束亮点 |
| :--- | :--- | :--- | :--- |
| **`codemode`** | 模型工具 | 启动一段异步 JS 脚本 | `code` 必填（<= 256 KiB），可选 `background`（默认 `false`）、`label`（<= 256 B） |
| **`codemode_wait`** | 模型工具 | 在当前 `step` 原地等动静，或派一次性探针赛跑 | 传 `{}` 纯等信/等任务；可选 `trigger_code`（<= 256 KiB） |
| **`codemode_task_list`** | 模型工具 | 列出本会话仍在跑（含正在停止）的任务 | 无参数 `{}`，按原调用 `id` 升序返回 |
| **`codemode_task_status`** | 模型工具 | 查单个任务的实时透视快照或已完工摘要 | 必填 `id`（严格格式 `cm_<26位小写 Crockford Base32 后缀>`，共 29 字符） |
| **`codemode_task_renew`** | 模型工具 | 把活任务剩余租期从现在起重置为指定秒数 | 必填 `id` 和 `duration_seconds`（**1 到 86400 秒**） |
| **`codemode_task_kill`** | 模型工具 | 停掉任务及其受管进程组，最多原地等 **3 秒**遗言 | 必填 `id` |
| `GET /sessions/{sid}/tasks` | HTTP API | 供 WebUI 任务栏每 2 秒拉取本会话活跃任务全量快照 | 绕过“无变化去重”，总是返回最新完整文本尾部 |
| `DELETE /sessions/{sid}/tasks/{tid}` | HTTP API | 人工从页面点 Stop 掐断指定任务 | 等同触发停止（超时标 `expired`，否则 `killed`），返回 `stopping` |

每个脚本开跑时，都会从它那笔 `function_call` 在 `items` 表里的 TypeID 主键 `origin`（形如 `itm_<26位后缀>`）直接换前缀命名，任务编号就是 **`cm_<26位后缀>`**。为了护住机器内存和 JS 线程池，系统锁死了两道并发配额：**全进程最多同时跑 256 个虚拟机任务，单个会话最多 32 个**——不仅记账员的 `s.tasks` 算这个数，连后面 `codemode_wait` 临时拉起来的 `trigger_code` 探针也会占一个 `VmSlot`（直到工人线程把进程组完全清场释放才摘名额）。

---

## 快活当场结清，慢活打欠条转后台

车间跑起来后，第一件事是跟当前 `step` 对账。模型发起一次 `codemode` 之后，在**怎么交还 `function_call_output`** 这一步一分为二（[src/codemode/mod.rs](../src/codemode/mod.rs#L759-L880)）：

- **快活走前台**：默认（`background: false`）在当前 `step` 里先等等看。只要 **90 秒**（`FRONT_WAIT`）内跑完，最终输出直接写进配对的 `function_call_output`（此时 `codemode_call_item_id` 留空 `NULL`），当前 `step` 拿着完整答卷直接进下一轮，跟普通同步工具一模一样。
- **慢活打欠条转后台**：只要满足“模型显式传了 `background: true`”或者“前台等满 **90 秒**还没交卷（且当前会话没收到硬暂停、进程没在关闭）”，程序就不再让当前 `step` 干陪着了——当场先写一张**后台收据**顶上 `function_call_output` 的坑位（告诉模型 `cm_<26位后缀>` 已转后台，状态 `background`，可以先干别的独立工作，要等结果就传 `{}` 调 `codemode_wait`），同时在这条结果行上盖一个特殊印章：**`codemode_call_item_id = "itm_<26位后缀>"`**。

**`function_call_output` 上的 `codemode_call_item_id` 只管一件事：区分这笔工具结果交的是“最终答卷”，还是只打了张“后台欠条”。** 前台结清时它是 `NULL`；转后台打欠条时它指回原调用 `itm_<26位后缀>`。省掉这个印章，后面收到迟到结果或者崩溃重启翻账本时，光看见“已经有了一条 `function_call_output`”，程序根本分不清这单活是早就办完了，还是只打过欠条、还欠着真正的完工答卷！

既然每个 `function_call` 唯一的 `function_call_output` 坑位已经被欠条占住了，退居后台的任务再想跟模型说话，就不能回头篡改旧收据，而是往 `items` 流水账里新记一种条目，叫 **`codemode_event`**（[schema.sql](../schema.sql#L102-L129)）。每笔事件用 `codemode_call_item_id` 指回原调用，带着递增序号 `sequence`（`1, 2, 3…`）和唯一索引防重，一共只记三类事：

1. **`message`**：脚本中途调 `emitEvent(...)` 主动报信。为防死循环刷爆信箱，带了双重背压限流：单任务一个**令牌桶**（初始和上限 **8 个**令牌，每秒回充 **1 个**，且事务 `COMMIT` 成功才扣令牌），同时整场会话门外排队未读的 `message` **最多 128 条**。
2. **`lease_warning`**：租期快到时的催单提醒。
3. **`completed`**：后台任务彻底收工（`succeeded` / `failed` / `expired` / `killed` / `interrupted`）交回的最终答卷，带上任务总耗时 **`duration_ms`**；数据库用部分唯一索引 `codemode_completed` 死锁每个任务最多只能有一笔 `completed`。

> 注意一条状态边界：转入后台前如果任务正巧触发过提醒或发过消息，流水账允许提前写事件；但一旦任务已经以**前台**方式结清交付，或者已经写过 `completed`，账本层（`ledger::event`）就会坚决禁止再写入任何新事件。

---

## 同一个门外信箱，两处分叉：`input_queue_cursor` 与自动 `wake`

后台任务塞进来的 `codemode_event` 怎么送进模型视野？拿它跟 [第 1 篇](01-agent-loop.md) 里的用户消息 `user_message` 放一起比对最清楚——**它俩前半截走同一条道，到后半截分叉。**

**共通的前半截**：
- 都不去打断半路正在流式输出的 `step`，而是老老实实落进 `items` 流水账、排在 `input_queue_cursor` 后面等着；
- 等当前 `step` 收尾开下一轮时，由 `take_pending` 从 `pending` 视图按 `items.id` 发生先后一视同仁地收进 `context_items` 上下文；
- 在 [第 3 篇](03-catalog-media-and-billing.md) 的 `wire_message` 里，`codemode_event` 会在内容开头自动插一块身份牌（`"Codemode <event> event from cm_<26位后缀> (sequence N)."`），然后包装成一条 `role: "user"` 消息送去上游模型；
- 要是门外剩下的全是被撤回的消息或作废的提醒、`has_pending` 检查一圈发现没活可取，也会顺手把 `input_queue_cursor` 往前推过这批废单，省得次次重扫。

**从哪一步开始分叉？分叉在两处：**

1. **第一处分叉在“会话睡着时点不点火”**：用户往停着的会话发消息，“投信”和“点火”是分开的，得人工调 `PUT /sessions/{sid}/run` 才开工；可后台脚本半夜跑完交卷时，模型早就结束回合睡进 `idle` 了，外面没人替它点开关。所以只要新落盘的 `codemode_event` 带着 `notify = true`（且进程没关闭、会话没有活动 worker），宿主就会自动调 **`app.wake(sid)`** 把会话拉起来开跑下一轮。**但 `wake` 比手动 `/run` 克制得多**：要是会话正停在 `paused`（比如用户手动暂停或上次报了错），事件只安静进队排队，**绝不越权替用户解开暂停**；反过来，要是会话原本在 `idle`、自动唤醒时却发现当时选的模型已经被归档了，[src/agent.rs](../src/agent.rs#L37-L59) 会把会话切到 `paused` 并写明原因，免得事件卡在 `idle` 里装死。
2. **第二处分叉在“门外未读的信怎么作废”**：用户消息靠 `withdrawal` 撤回，过期租期提醒靠下面要讲的 `invalidated_warnings` 作废表跳过。

---

## 原地等信与探针赛跑：`codemode_wait`

后台收据里明明劝了模型“没事就先交卷等自动唤醒”，可实战里还是会撞上一堵墙：大模型在训练里把“不调工具纯说话”等同于“向用户最终交卷”。后台脚本还在装依赖，模型手头又没别的代码好写，它不敢直接回复用户，就会一轮又一轮反复新起 `codemode` 去 `ps` / `sleep` 查日志——白烧几十轮上下文和算力。

**`codemode_wait` 给的就是一个“不用交卷结束回合、就在当前 `step` 里原地等动静”的挡位。** 工具不收超时参数，内部兜底最长等 **60 分钟（3600 秒）**。

拿它跟前面“前台 `codemode` 默认等 90 秒”并排对比：
- **前半截完全共通**：都是在当前 `step` 里先扣住一条 `function_call_output` 不交账。只要它还没交账，整个 Agent 循环（`run_loop`）就稳稳停在当前 `step` 原地不动；会话状态照旧维持 `running`——**根本不需要给 `sessions.status` 硬造第五个 `waiting` 状态**，因为等待期间用户插话照样在门外排队、点暂停照样能停、崩溃重启照样按未完工具收残局，所有转移规矩跟 `running` 严丝合缝！
- **它真正分叉在“进门查什么”和“什么信号能叫醒它”**（[src/codemode/mod.rs](../src/codemode/mod.rs#L503-L692)）：

```
进入 Wait::poll（在 SQLite 记账线程上原子判定，严格按以下优先级）：
  1. 收到硬暂停？         ──▶ 直接结束不写输出（交给 publish_tools 补中断）
  2. 收到软暂停或停机中？   ──▶ 立刻回 interrupted ("session pausing")
  3. 门外信箱已有信？       ──▶ 0 毫秒立刻返回 woke（按实际积压区分 task_event / user_message / pending_input）
  4. 核对本会话活跃任务     ──▶ 刚才还有任务现在全退光了？返回 woke (no_active_tasks)
     （含同一步刚派的兄弟） ──▶ 一开始就空车间且没带探针？当场报 error ("no active codemode tasks")
  5. 等满 60 分钟兜底？     ──▶ 返回 timeout（附 active_tasks 数）
  6. 探针脚本跑完出结果了？  ──▶ 返回 woke ("trigger") 或 error ("trigger_failed")
  7. 以上全无               ──▶ 挂上唤醒弦进入监听（若带 trigger_code 则拉起探针赛跑）
```

这里面藏着三个非常精妙的设计：

1. **先看信箱再数任务，还认同一步的“兄弟”**：假如模型思考这几百毫秒里，后台任务刚好跑完并把 `completed` 塞进了门外信箱，如果先数在跑任务就会误报“没有活跃任务”；先看 `pending_reason`，就能 **0 毫秒当场唤醒**并指出该收哪种信，下一轮立刻喂进上下文！而要是模型在同一轮里并排同时调了 `codemode(...)` 和 `codemode_wait({})`，SQLite 里的 `ledger::starting_sibling` 会查出同一 `llm_call_id` 下还有刚落盘、尚未交收据的兄弟 `codemode` 调用，同样算作“有活跃任务”，绝不会因并发派发次序撞车误报空车间。
2. **一根唤醒弦盯所有动静**：记账员在内存的 `wake_sequences` 里按需给正在等的会话拉一根单调递增的 `watch` 序号（没人等时自动清理回收）。纯传 `{}` 时只监听这根弦，**完全不占 QuickJS 工人线程**；只要用户投信、后台报信、任务启停或发生暂停，记账员顺手拨动这根弦，等待器立刻醒来重跑上面 1~6 步。
3. **一次性探针 `trigger_code`**：当用户想在空车间等外部状态，或者等某个后台命令日志里出现 `"READY"` / `"ERR"` 时，可以传一段 `trigger_code`。它和普通任务一样在 `VmSlot` 和 QuickJS 里跑（占用 256/32 配额），但有四点严格区别：
   - **没有 `Store::tasks` 登记**（`task.list` 看不见、不能续租、不收租期提醒、崩溃重启也绝不恢复重跑）；
   - **禁止调 `emitEvent`**，`mygo.taskId === null`；
   - **它是一次性脚本而不是周期回调**：自己写 `while` + `await sleep(500)` 轮询，只要 `return` 任何值（包括 `null`）或脚本跑到底就立刻算唤醒；
   - **只准做无副作用检查**：一旦等待期间门外进信、原来在跑的任务全退光、收到软暂停或到 60 分钟，1~5 步优先级高于第 6 步，等待器会立刻拉闸取消探针、等它把自己的子进程组收干净，然后**直接丢弃探针输出**。

---

## 车间透视窗 `LiveState` 与防刷屏机关

等后台脚本半天没回音，模型心里犯嘀咕：是不是卡死在哪个 `apt install` 或者编译卡住了？

光看 `codemode_task_list` 只能知道“还在跑”，光看 `console.log` 也会扑空——因为脚本一旦停在 `await proc.exec(...)` 等子进程退出，JS 代码根本还没往下走到打印那一行，`console` 里一个字都没有。

所以每个任务诞生时，虚拟机、`io::drain` 管道读取器和 `Task` 台账之间共用一块内存实时板 **`LiveState`**（[src/codemode/live.rs](../src/codemode/live.rs)）：
- **`console` 环形尾部**：攒着最新 **256 KiB** 日志，支持 `console.clear()` 推进 `console_epoch`。如果日志绕圈截断了，输出文本前会先跳过开头的 0~3 个 UTF-8 续字节（`b & 0xc0 == 0x80`），防止半截汉字乱码；
- **`CommandGuard` 子进程透视**：每启动一个 `proc.exec`，RAII 守卫立刻在 `LiveState` 挂牌（保存命令名、最多 **16 个**截断至 256 字节的参数预览、`cwd`、`started_at`），每次从管道读到新字节，**先于 1 MiB 返回上限**实时抄送一份到各 **64 KiB** 的 `stdout`/`stderr` 环形尾部里，命令一退不管成功超时必摘牌并累加 `completed_commands`；
- **`McpGuard` 阶段透视**：调 `mcp.*` 时只记操作名、服务器名、可选工具名（截断至 256B）和阶段（`looking_up` -> `connecting` -> `discovering` / `requesting`）及耗时——**刻意不记参数、URI 和游标**，防监控快照把敏感凭据泄漏出去。

为了防止模型拿着 `codemode_task_status` 反复瞎轮询把上下文填满，模型工具专用的 `status` 藏了两招**防刷屏机关**（注：JS 内的 `task.status` 和 HTTP 的 `GET /sessions/{sid}/tasks` 绕过这两招，总是拿完整快照）：

1. **状态指纹去重（`ProgressFingerprint`）**：每次模型调 `codemode_task_status`，程序会对比这次的指纹（含 `stopping`/`background`/`deadline`、`console_epoch` 与总字节数、`completed_commands`、在跑命令各自的 `stdout`/`stderr` 字节数，外加 **`last_mcp_id`** 专门捕捉两次轮询间隙一闪而过的短 MCP 调用）。要是跟上一次一模一样，快照会故意**省略 `console.output` 和在跑命令的 `stdout`/`stderr` 大段正文**，并贴上 `"unchanged_since_last_status": true` 劝它改用 `codemode_wait`。
2. **门外完工信折叠（`fold_queued`）**：要是后台任务刚刚跑完摘牌、`completed` 完工信正躺在门外队列里还没被收进上下文，这时候调 `codemode_task_status` 不会把长达几十 KB 的答卷原文再塞一遍工具返回值，而是只读头块、把报错截断至 512 字节，返回 `delivery: "queued"` 提醒“下个 `step` 门外信就送到了”；只有前台结清或已消费进上下文的完工任务，才吐出最多 **64 KiB** 的正文预览和附件数量统计。

---

## 限时工位、续命催单与“三秒等遗言”

允许脚本常驻后台，就必须防它写个死循环永远赖在车间里。所以每个 `codemode` 任务出生自带 **2 小时（7200 秒）** 租期（[src/codemode/mod.rs](../src/codemode/mod.rs#L38-L44)）：

- **双档催单提醒**：看门协程的 `tick()` 盯着到期表，离到期只剩 **1 小时（3600s）** 和 **15 分钟（900s）** 时各往门外投一封 `lease_warning`（带着触发的 `threshold_seconds`）叫醒模型：
  - 模型觉得还需要跑，就调 **`codemode_task_renew`** 把剩余租期从当下重置成 **1 秒到 24 小时（86400s）**（系统自动按新剩余时长重新计算下一个提醒档位）；
  - 不需要了就调 **`codemode_task_kill`** 掐掉；
  - 置之不理，到点就自动按 `expired` 断电。
- **门外作废表 `invalidated_warnings`**：如果剩 1 小时的提醒刚排进门外信箱、模型还没来得及读，任务自己跑完了或者模型刚续过命，下一轮把过期的“快到期了”喂给模型只会干扰判断。但流水账又只增不删、进过上下文的更不能改——所以程序单设了一张 **`invalidated_warnings`** 表（[schema.sql](../schema.sql#L131-L133)）。**它只管把“还在门外（`id > input_queue_cursor`）、没进过上下文的 `lease_warning`”标成作废。** 任务一续租、一结清或重启，门外未读提醒就进这张表，`pending` 视图自动绕过它，就像绕过被 `withdrawal` 撤回的用户消息一样。

**`codemode_task_kill` 里也藏着一个防止回声的小机关（“三秒等遗言”）**：
模型下令 `kill` 停掉一个任务时，工具拉下取消闸门后不会扭头就走，而是原地最多等 **3 秒（`KILL_WAIT`）**：
- **3 秒到了子进程还在收尾**：`kill` 先返回 `{"status":"stopping"}` 解放当前 `step`；等后面真退干净了，监督协程再按正常后台路数补发一封 `notify = true` 的 `completed` 事件叫模型收尸。
- **3 秒内痛快退干净了**：`kill` 直接把任务最终输出塞进自己的工具返回值里当场带回！但如果这个任务之前已经转过后台、留过欠条，流水账上欠条必须配一笔 `completed` 才算平账（否则崩溃恢复会误以为没完工！）。于是程序照样写一笔 `completed` 事件，但把里面的 **`notify` 标成 `false`**：只平账留底（也让 `task_status` 能查到），**不进 `pending` 队列、不进上下文、也不再唤醒模型**。省掉 `notify` 这个开关，模型刚在 `kill` 返回值里看过一遍遗言，下一轮又会收到一封一模一样的完工信。

---

## 拼回老积木：暂停、优雅退出与崩溃恢复

把整套 `codemode` 任务生命周期接回 [第 1 篇](01-agent-loop.md) 的积木里，所有边界都能严丝合缝对上账：

1. **软暂停**：只拦下一步不打断当前 `step`。正在等的前台 `codemode` 照旧等（满 90 秒照样打欠条转后台），后台任务更完全不受打扰（即使途中报信也只进队列排队，绝不唤醒 `paused` 会话）。但 **`codemode_wait` 一听到软暂停立刻收工**返回 `"session pausing"`；若挂着 `trigger_code` 探针，先拉闸等它把子进程清净、丢弃输出再交账。
2. **硬暂停**：一边掐断模型流，一边把该会话名下全部前后台任务统一按 `killed` 拉闸，同时 `can_detach` 锁死，**绝不让刚好踩到 90 秒线的前台任务趁乱转后台**。前台 `codemode` 与 `codemode_wait` 当场松手让 `run_loop` 由 `publish_tools(..., recovering=true)` 补上 `"Execution interrupted; result unknown."` 停进 `paused`；等虚拟机在后台慢吞吞把子进程组拆干净回来交差时，`complete()` 检查 `original_pending` 发现原调用早被结清了，就安静摘牌退出，绝不写重账。
3. **优雅退出**：第一步前 5 秒给活动会话发软暂停（正卡在 `codemode_wait` 的会话立刻脱身），同时**先把所有后台 `codemode` 任务按 `interrupted` 停掉**（前台脚本留 5 秒看能不能跟着当前 `step` 收完尾）；如果哪个原本睡在 `idle` 的会话因为断了后台任务受影响，顺手把它标成 `paused`（`"background task interrupted by shutdown"`）留痕；5 秒还没退完切第二步，剩下的前台任务和会话全部按硬暂停掐断再等 5 秒。
4. **崩溃恢复（`ledger::recover`）**：强杀或断电重启后，内存里的所有车间和进程全没了，启动事务一口气收三摊残局：
   - **第一摊**：把全库所有门外未读的 `lease_warning` 全塞进 `invalidated_warnings` 作废——因为重启后没有哪个租期还活着；
   - **第二摊**：顺着 `codemode_receipts` 索引找出所有打过后台欠条（`codemode_call_item_id IS NOT NULL`）却还没等来 `completed` 事件的任务，挨个补写一笔 `"Daemon restarted; result unknown."`（`status: "interrupted"`）的 `completed` 事件，并把对应会话标成 `paused`——跟普通工具一样，绝不擅自重跑可能已产生的副作用；
   - **第三摊**：万一崩溃刚好卡在“后台事件刚写库提交，但 `app.wake` 还没来得及把 `idle` 会话拉起来”的那一微秒，就把这些门外压着未读 `codemode_event` 却仍停在 `idle` 的会话也统一标成 `paused`（`"daemon restarted with pending codemode events"`），等你下次点 `/run` 接手，绝不让事件沉底装死。

---

**上一篇**：[04 · Codemode 上篇：最小完备原语、独立车间与宿主能力](04-codemode-sandbox.md) · **返回目录**：[总览](README.md) · **下一篇**：[06 · 外部工具扩展：MCP 协议、双传输与 OAuth 2.1](06-mcp-and-oauth.md)
