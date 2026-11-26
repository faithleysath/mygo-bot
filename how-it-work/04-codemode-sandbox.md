# 04 · Codemode 上篇：最小完备原语、独立车间与宿主能力

在很多 Agent 框架里，你会看到模型工具列表越堆越长：`read_file`、`write_file`、`edit_file`、`glob`、`grep`、`bash`、`curl`、`search_mcp_tool`……模型每看一个文件、筛一次日志、调一个接口都要往返一轮模型推理，既慢又容易被海量原始输出灌满上下文。

MyGo-Bot 死守一条**最小完备原语原则**：不给模型发一大堆杂乱包装工具，而是**只把最基础、正交可组合的原语递进一个 JavaScript 沙箱车间里**。并且为了彻底抹平“本机”和“远端机器”的特权差别，全局只保留与节点无关的编排原语（`cluster`、`task.{list, status}`、`mygo.{sessionId, taskId}`、`sleep`、`emitEvent`、`console`、`TextEncoder`、`TextDecoder`、`Date`），**所有文件、命令、HTTP、MCP 与 MyGo API 全部收敛进 `cluster.node(target)` 返回的冻结节点句柄里**。模型想跨机器同时查十个文件、把节点 A 的文件直接流式写到节点 B、再并发调节点 C 上的 MCP 汇总结果，只要写一段 JS 脚本跑一次 `codemode`，在车间里一步加工完再把干货带出来。

但这立刻撞上第一道工程难关：[第 2 篇](02-storage-and-concurrency.md)立住了“单线程异步运行时配一个独占记账员”的底座——**脚本要是写了个 `while (true) {}` 死循环，或者通过命令行生出一串孙子进程，怎么做到既不冻住主进程的所有网络会话，取消时又不留下一地孤儿进程？**

---

## 独立车间与 JS 工人线程池

因为主运行时只有一条单线程事件循环，JavaScript **绝不能**直接跑在它上面。

所以程序启动时（[src/main.rs](../src/main.rs#L499-L502)），会专门单拉一排固定数量的后台操作系统线程，取名叫 `codemode-js-0`、`codemode-js-1`……数量由 `--js-threads` 控制，默认按宿主机逻辑 CPU 核数最多开 **4 条**（每条线程配 **4 MiB** OS 栈，内部跑一个**不带 I/O 和定时器驱动的裸单线程 Tokio `LocalSet`**）。

每次模型调 `codemode`（代码最多 **256 KiB**，可选 `label` 最多 **256 字节**；每次 `codemode_wait` 带的探针脚本同理）：

1. **负载均衡派工**：调度器从轮转起点出发，在没关闭的 JS 工人里挑当前手头在跑虚拟机数量（`active`）最少的那位（[src/codemode/vm.rs](../src/codemode/vm.rs)）。同一条线程上的多个车间靠 `LocalSet` 多路复用。
2. **现开一间干净小车间**：每个任务现场新建一套干净的 QuickJS `AsyncRuntime` 和上下文——**内存封顶 96 MiB、调用栈封顶 256 KiB**。
3. **挂紧箍咒与公平调度**：
   - 虚拟机挂着一个**中断钩子**（`set_interrupt_handler`），每次检测到任务取消或进程退出就返回中断信号，哪怕脚本里写死了纯同步 `while (true) {}` 也能被立刻抢占停掉；
   - 脚本被包进 `(async () => {code\n})()` 里执行（注意末尾特意加了一行换行 `\n`，防止用户代码最后一行写了个 `//` 行注释把后面的 `}})()` 吞掉），天然支持顶层 `await` 和 `return`；
   - 驱动循环每次最多连续跑 **64 个** QuickJS 微任务就主动 `yield_now()` 让出控制权，免得同一条 JS 线程上的一个忙碌微任务循环把邻近车间饿死；
   - 挂给 JS 对象的原生回调只抓 `Weak<Resolvers>` 弱引用，彻底打断 Rust `Rc` 和 QuickJS 垃圾回收器之间的循环引用内存泄漏；
   - 一旦最外层根 `Promise` 结算，程序立刻把 `scope.accepting` 设成 `false` **然后再**去读返回值——专门防返回值对象里藏着恶作剧 `getter` 在交卷瞬间偷偷再调宿主 API 或写 `console.log`。
4. **重活外包给主运行时**：JS 工人线程自己没有网络和定时器轮子，脚本每调一次异步宿主能力（比如读写文件、发 HTTP、跑命令），VM 就通过 `JoinSet::spawn_on(..., &host.executor)` 把这个异步任务扔回主进程的 Tokio 运行时执行，等结果回来后再在 JS 线程唤醒对应的 `Promise`。
5. **内建文本编解码（[src/codemode/decoder.js](../src/codemode/decoder.js)）**：QuickJS 本身不带 Web 标准的 `TextEncoder` / `TextDecoder`，程序启动车间时先装好一套带品牌校验和原型缓存（提前锁住 `TypedArray`、`DataView`、`ArrayBuffer` 原型 getter，防脚本造假对象欺骗长度/偏移）的 UTF-8 编解码器（release 构建由 [build.rs](../build.rs) 调 `bun` 运行 [tools/minify-codemode.mjs](../tools/minify-codemode.mjs) 压缩 `bootstrap.js` 和 `decoder.js` 后再从 `OUT_DIR` 嵌入二进制）。每次按 **64 KiB** 分块处理（单次总上限 **8 MiB**），支持流式 `{ stream: true }` 暂存跨块劈开的合法多字节 UTF-8 残缺前缀、自动剥离首部 BOM、并把孤立 UTF-16 代理对安全转成替换符 `U+FFFD`。

---

## 全套云化节点原语、跨节点流句柄与进程笼子

在全局层，车间只提供 `cluster.nodes()`（返回按 `(name, id)` 排序的 `[{ id, name, description, os, arch, online }]`，**刻意不暴露哪个节点是当前脚本执行者**）和 `const node = cluster.node(target)`（按精确 `id` 或唯一 `name` 取出目标节点句柄，报错带 `ENODENOTFOUND` / `ENODEAMBIGUOUS` / `ENODEUNREACH`）。

每次调用 `node.fs.*`、`node.proc.exec`、`node.http.request`、`node.mcp.*` 或 `node.mygo.request` 时，请求统一步入 `src/codemode/io.rs`：
- **本地零开销直调**：目标解析到本机 `node_id` 时直接走本机执行路径；
- **跨节点 QUIC 透明转发（`ALPN b"mygo/cap/1"`，[src/cluster/cap.rs](../src/cluster/cap.rs)）**：目标在远端在线同伴时，在复用并经 `mygo-cap-srv`/`mygo-cap-cli` BLAKE3 keyed-MAC 双向认证、64 MiB 全局字节预算和并发配额保护的 QUIC 连接上开一条双向流转发；任何一方断连或取消，流关闭立刻联动取消远端任务。

为了防单个失控脚本把宿主进程拖垮，车间门口站着两道流量闸门：
- **单车间闸门（`HOST_CALLS`）**：一个虚拟机同时最多挂 **16 个**在途宿主调用（或打开的流句柄），并且刻意在**调用入口**和**读完 JS 参数之后**连检两次，堵死参数 `getter` 重入偷塞调用的漏洞。
- **全进程闸门（`IO_SLOTS`）**：除了 `sleep` 和 `emitEvent` 之外，所有异步 I/O 操作在进程级共享 **32 个**并发槽位。更有意思的是，拿到的槽位许可（Permit）会跟着 `HostResult` 一直陪着直到 JS 线程真正唤醒 `Promise` 才释放——专门防止脚本发了一堆请求却卡住不读结果、把已完成的大响应体全憋在内存里。
- **参数与缓冲上限**：单次传进宿主的参数 JSON 信封上限 **256 KiB**；所有缓冲型文本或二进制输入/输出一律上限 **8 MiB**；文件系统与操作系统出错时，JS 异常对象上会自动贴好 POSIX 风格 `error.code`（如 `ENOENT`、`EACCES`、`EFBIG`、`EXDEV`、`ENOTSUP` 等）。

### 1. 统一流手柄与跨节点流适配器（`src/codemode/streams.rs`）

当文件或网络响应超过 8 MiB 时，就不能整块憋在内存里读写，而要换成流（`stream: true`）。`Streams` 表控制全局最多同时打开 **128 个**流句柄，单任务最多 **16 个**（并严格**先占好本地和全局两个许可**才去开文件、连远端流或发外部 HTTP 请求）。
- 读流和写流每次切片上限 **64 KiB**；
- JS 侧的 `reader` 包装成**单次消费**的异步可迭代对象（并发同时 `next()` 会直接抛 `TypeError`），`for await` 中途 `break`、抛错或显式调用 `await body.return()` 都会立刻关掉底层句柄（以及远端 QUIC 流）释放名额；
- 对远端流还提供了 `RemoteByteReader`、`RemoteByteWriter` 和 `RemoteDirReader` 适配器，使 **`await dst.fs.write(dstPath, await src.fs.read(srcPath, { stream: true }))`** 这类跨机器直接流式传输自动享受同等的 64 KiB 背压控制与近零额外内存占用。
- 流式上传使用容量为 **1** 的 `mpsc` 通道（而且 `stream.write` 会先克隆 sender、**松开互斥锁再去等通道消费**，既不锁死句柄关闭，又把上传背压控制在 1 个 64 KiB 块以内）。

### 2. 文件系统 `node.fs.*`（`src/codemode/files.rs`）

直接以目标节点服务进程权限操作该节点本地文件（相对路径统一解析到目标节点服务用户的 `HOME` 目录），即使跑在没装 Unix Shell 工具链的精简容器或 Windows 上也能照常工作：
- `node.fs.read(path, { encoding?, stream? })`：返回 `Uint8Array`、`encoding: 'utf8'` 字符串（与 `stream: true` 互斥），或 64 KiB 块的异步字节流（流式读取不限 8 MiB 体积）。
- `node.fs.write(path, textOrBytesOrStream, { append? })`：支持字符串、`Uint8Array` 或异步字节流（流式写入不限体积，可直接对接另一节点的读流），返回实际写入字节数。
- `node.fs.readDir(path)`、`node.fs.stat(path, { followSymlinks? })`（返回 `{ type, size, mtimeMs }`，支持 1970 年前负时间戳）、`node.fs.mkdir(path, { recursive? })`、`node.fs.rename(from, to)`、`node.fs.remove(path, { recursive? })`（专门规范化路径去掉末尾 `/` 或 `/.`，保证删除指向目录的符号链接本身而不顺着删进目标目录）。

**`fs` 模块里藏得最深的是防卡死与 TOCTOU（检查与打开时间差）防线**：如果模型拿 `node.fs.read` 去打开 `/dev/zero`、设备节点或阻塞的命名管道，一读就把工人线程甚至阻塞池彻底挂死了。所以 `open_regular` 在 `spawn_blocking` 里连设三道卡：
1. **Windows 事前拦截**：直接拒绝 `\\.\` 设备命名空间、`\\?\` 原始前缀及 UNC 里的 `pipe`/`mailslot` 共享路径；
2. **Unix 防挂死打开标**：带 `O_NONBLOCK | O_NOCTTY` 打开，保证哪怕检查完一瞬间路径被掉包成命名管道或终端设备也不会阻塞或抢占控制终端；
3. **无截断先验身再清空**：覆盖写（`Overwrite`）时**不带 `O_TRUNC` 打开**，先核实拿在手里的真实句柄确实是个普通文件（Windows 调 `GetFileType == FILE_TYPE_DISK`，全平台再验 `metadata().is_file()`），验明正身之后才调用 `set_len(0)` 截断——绝不允许对着 FIFO 管道或设备文件发截断操作。

### 3. 网络请求 `node.http.request`（`src/codemode/io.rs`）

- 底层用目标节点专用的 `reqwest::Client`（独立连接池，30 秒连接超时，**不自动重试、不自动跟重定向**），与模型流请求隔离，请求真实从目标节点所在网络发出。
- 传 `{ url, method, headers, body, timeout_ms, stream }`（只允许 `http:` / `https:`；支持字符串/`Uint8Array` 或异步流作为请求体）。
- 返回 `{ status, headers, body }`：缓冲模式下 `body` 是 `Uint8Array`（最多 8 MiB），并贴心地附带同步 `.text()` 和 `.json()` 方法；传 `stream: true` 则一拿到响应头就立刻返回，让脚本自己拿 `body` 边拉边解码或直接流式转存到任意节点的 `fs.write`。

### 4. 受管命令与进程笼 `node.proc.exec`（`process.rs` & `windows.rs`）

`node.proc.exec(program, args, { cwd, env, stdin, timeout_ms })` 在目标节点直接拉起可执行文件（默认 `cwd` 统一解析到目标节点服务用户的 `HOME` 目录，**不经过 Shell**；Windows 节点上 `.bat`/`.cmd` 需显式经 `cmd.exe` 调用）。返回 `{ exit_code, signal, timed_out, stdout, stderr, truncated }`，其中返回的 `stdout` 和 `stderr` 各留前 **1 MiB**，超出置 `truncated: true`。

但外部命令最危险的是**拉起一串孙子进程**：要是脚本超时或被取消时只杀最外层领头进程，孙子进程还会留在后台野跑，甚至死死攥着输出管道写端不放，把等待管道关闭的代码活活拖死。所以 MyGo-Bot 在两个平台上分别搭了一座**关住整棵进程树的笼子**：

| 平台 | 建笼子方式 | 防逃逸与退场机制 |
| :--- | :--- | :--- |
| **Unix**<br>（`process.rs`） | 在 `spawn_blocking` 里事先订好 `SIGCHLD`，以 `process_group(0)` 启动进程，让子进程成为新进程组组长（`pgid = pid`）。 | 用 `waitid(WaitId::Pid(pid), EXITED \| NOWAIT \| NOHANG)` 监听退出——**`NOWAIT` 故意保留僵尸领头进程占住 PID 坑位**，防止操作系统把同一个 PID 重新分给别人；收网时（`reap_group`）先用 `kill_process_group(pgid, SIGKILL)` 把整组子孙连根拔掉，最后才 `child.wait()` 回收坑位。 |
| **Windows**<br>（`windows.rs`） | 创建带 `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` 的 `JobObject`，按 MSVC CRT 规则转义命令行，按 Windows 大小写无关语义合并环境变量，建唯一命名管道（输入非阻塞、输出异步）。 | 调用 `CreateProcessW` 时通过 `STARTUPINFOEXW` 扩展属性列表，在进程诞生的**同一瞬间**把 `PROC_THREAD_ATTRIBUTE_JOB_LIST` 焊死在作业上，并用 `HANDLE_LIST` 白名单锁死仅继承 `[stdin, stdout, stderr]` 三个句柄，孙子进程毫无机会逃出笼子；通过 Windows 线程池等待通知退出，退场时先 `TerminateJobObject` 再轮询 `ActiveProcesses == 0`。 |

无论是正常退出、`timeout_ms` 到点（**注意**：超时触发时会杀光整组并**照常返回 `timed_out: true` 连同已经收到的前 1 MiB 输出**，不会直接扔个空报错把现场抹掉！），还是任务被强行取消：
- `io::drain` 从主进程退出起最多再给 **250 毫秒**管道排空宽限期，读完还没关就果断截断并标 `truncated = true`；
- 面对跨节点调用，执行方拉响取消后会发送 `CapClientMsg::Cancel`，并在 **3 秒**（`CAP_PROC_REAP_TIMEOUT`）内等待远端节点完成进程组 / `JobObject` 清场收尸；
- 在虚拟机里收尾时（`vm::run`），即使任务已被取消、根 Promise 已经落袋，VM 也**绝不直接腰斩**正在跑的宿主异步任务，而是拉响 `scope.cancel` 闸门后老老实实 `join_next()` 等所有在途 `proc.exec`（包括远端同步清场）全部收干净，再在汇报完成前 `drop(scope)` 释放掉全部文件与流句柄。

### 5. 自省与节点内网接口 `mygo` & `node.mygo.request`（`bootstrap.js` + `io.rs` + `api.rs`）

脚本全局保留只读的当前任务身份 `mygo.sessionId` 与 `mygo.taskId`（`cm_<26位后缀>`，在 `codemode_wait` 探针中为 `null`）；而调任意节点的管理接口则挂载在具体节点句柄 **`node.mygo.request({ path, method, headers, body, timeout_ms, stream })`** 上：
- **直接在目标节点通过 `tower::ServiceExt::oneshot` 走内部路由 `api::internal_routes(app)`**（若目标是远端节点则先经 `mygo/cap/1` 送达该节点后再 `oneshot`）。它**不走目标节点外部 Bearer Token 鉴权**，只要求 `path` 是合法的站内路径（`/` 开头、非 `//`、无控制字符/空格）。传普通 JS 对象会自动序列化并补上 `Content-Type: application/json`；调 SSE 接口时强制要求传 `stream: true` 且流级读取同样受 `timeout_ms` 约束。而且内部路由专门并入了 **`GET /openapi.json`**——模型忘了某个管理接口长什么样，通过任意节点 `node.mygo.request({ path: '/openapi.json' })` 就能当场翻完整说明书。

### 6. 其余原生能力速览

- `sleep(ms)`：可取消的非阻塞延时（不占全局 `IO_SLOTS`，占单 VM `HOST_CALLS`）。
- `console.{log,info,debug,warn,error,assert,clear}`：同步写入内存实时板，内部用 `JSON.stringify` replacer 把各种二进制视图（`Uint8Array`/`DataView` 等）优雅渲染成 `Uint8Array(1024)` 而不是展开成几万个数字下标。
- `emitEvent(textOrBlocks)`：后台运行期间向会话主动投一条事件（带令牌桶限流，探针脚本禁用，详见 [第 5 篇](05-codemode-lifecycle.md)）。
- `task.list()` 与 `task.status(id)`：脚本内部查本会话任务快照，共享至少 **100ms** 的节流间隔防止死循环把记账员打爆；内部调用 `full_status` 总是返回最新全量输出。
- `node.mcp.*`（8 个接口）：列出与调用目标节点上已启用的共享或本地 Streamable HTTP / Stdio 外部 MCP 工具、资源和提示词，详见 [第 6 篇](06-mcp-and-oauth.md)。

---

## 两路合流交账与 `savepoint` 多模态落盘

脚本跑到最后，怎么把战果安全地交回给大模型？这里顺着人写 JS 的直觉做了**两路合流**（[src/codemode/vm.rs](../src/codemode/vm.rs#L514-L669)、[src/codemode/output.rs](../src/codemode/output.rs)）：

1. **`return` 返回值**：
   - `undefined` 或纯空白文本记空；
   - 普通字符串或任意可 JSON 序列化的 JS 值（比如直接 `return await node.proc.exec(...)`），宿主自动格式化为文本块；
   - 单个或一组（**1 到 1024 个**）内容块：支持 `{type: 'text', text}`，以及通过 `attachment_id` 引用老附件、或直接附带不超过 **50 MiB** `Uint8Array` 原始字节的 `image`、`audio`、`video` 媒体块（字段名严格精准，多一个少一个都不行）。
   - **两刀精准拦截**：要是脚本粗心直接 `return bytes`（裸 `Uint8Array` / `ArrayBuffer` / `DataView`，哪怕嵌在深层普通对象里），或者把媒体块字段写残了，或者试图把 MCP 通用二进制 `{type: 'blob', ...}` 直接丢给模型，虚拟机当场抛 `TypeError` 拦住——**这一刀只防两件事：不让几兆原始字节被 `JSON.stringify` 展开成上千万字的下标字典，也不让写坏的媒体块静默混成普通文本。**
2. **`console` 尾部合并**：无论脚本成功还是抛错、走前台还是转后台，实时板里攒下的最新 **256 KiB** `console` 日志都会跟着结果一起带回——没写 `return` 就拿日志当正文，写了 `return` 就拼在后面标上 `Console output:`。

为了防止多模态大附件把内存撑爆，每块数据必须同时过**两道预算闸门**：每块额外加计 **128 字节**元开销，单次输出/事件总预算封顶 **52 MiB**（`TASK_BYTES`），全进程所有在途未落盘的输出共享 **192 MiB** 全局预算（`OUTPUT_BUDGET` 信号量，结果一消费或 `Drop` 立刻归还）。

最后记账员把这包 `Blocks` 真正写进 SQLite 时，在事务里先立一个 **`SAVEPOINT codemode_blocks`**（[src/codemode/ledger.rs](../src/codemode/ledger.rs#L331-L368)）：
1. 把每个内联 `Part::Bytes` 存进 `attachments` 表拿到新 `attachment_id`；
2. 查出该会话所选模型声明的 `input_modalities`（哪怕模型之后被归档了，仍按它记录的模态核对），校验这批块是不是模型吃得下的（比如纯文本模型不收图片）；
3. **要是任何一步失败**：当场 `ROLLBACK TO codemode_blocks`，把刚才临时插进去的 `attachments` 行抹得干干净净，**绝不在库里留半个孤儿附件**；若是客户端错误（如模态不支持或空结果），外层事务再顺势转成纯文本的 `"failed"` 工具回复收尾。

---

**上一篇**：[03 · 模型目录、多模态附件与精确计费](03-catalog-media-and-billing.md) · **返回目录**：[总览](README.md) · **下一篇**：[05 · Codemode 下篇：前后台分流、原地等信、实时透视与租期恢复](05-codemode-lifecycle.md)
