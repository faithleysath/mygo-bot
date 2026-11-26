# 08 · 部署运维、压测验证、瘦身发布与评测体系

把程序的核心机制写完只是完成了一半，另一半决定了它能不能稳稳落地跑在生产机器上、能不能放心大改而不算糊涂账、以及跟同类前沿 Agent 放在同一道考题里到底几斤几两。这一篇把 MyGo-Bot 从启动命令、原生服务托管、极限瘦身构建，到 26 场景并发压测与 59 题横向评测一次讲透。

---

## 命令行启动、握手管道与双入口守卫

可执行文件名叫 `mygo`。直接跑 `mygo` 只看帮助；命令行参数解析失败退出码为 `2`，运行期失败退出码为 `1`。

### 1. 为什么后台启动会顿一下才还你终端？（`src/daemon.rs`）

执行 `mygo daemon` 时，默认会脱离终端在后台跑（可用 `--foreground` 留在前台把日志直接打到 `stderr`）。

父进程拉起后台子进程（附带环境变量 `MYGO_DAEMON_CHILD=1`，Unix 下子进程先调 `setsid()` 脱离控制终端，Windows 下以 `DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP` 启动）之前，会先打开当前目录的 `./mygo.log` 接住子进程的 `stderr`——注意打开日志用的是极严的 `open_log`（Unix 下 `0600` + `O_NOFOLLOW | O_NONBLOCK`，且核验必须是普通文件，防止顺着符号链接写坏别的文件或被命名管道卡死）。

然后父进程并没有立刻甩手退出，而是**守在子进程的 `stdout` 管道口等一行 `Ready` JSON 回信**：
- 只有等子进程真正把 TCP 端口绑稳、把 Unix Socket 建好、拿着独占锁打开了 `mygo.sqlite3`、并起好了 `--js-threads` 条 JS 工人线程（默认 `min(逻辑核数, 4)`），才往管道吐出 `{ listen, socket, token }`；
- 收到这行就绪回信后，父进程把 PID、监听地址和一次性随机生成的 **64 位十六进制 Bearer Token**（未传 `--token` 时由 `getrandom` 生成 32 字节随机数）打印到终端然后退出；
- 而**子进程一写出管道，立刻把内存里的 `self.token` 抹成 `None`** 再往 `mygo.log` 写启动日志——并且整个系统**绝不把任何 Token 写进 `tracing` 日志**。哪怕是在 `--foreground` 模式下（没有父进程管道中转），也**只有当 `stderr` 连着交互式终端（`IsTerminal`）时**，才会直接往终端打一次 `TCP Bearer token: ...`；只要被重定向进日志文件或者跑在 `systemd`/`launchd` 托管服务里，随机 Token 就绝不会落入日志或磁盘（而若在启动参数里固定传了 `--token`，则永远不会自动输出）。
- 万一端口全被占或同目录已经有另一个实例占死了数据库锁，子进程还没来得及还终端就会把错误顺着管道吐回来，父进程当场报错退出。

### 2. TCP 探测、Unix Socket 与集群网络选项（`src/main.rs`、`src/socket.rs`、`src/cluster/net.rs`）

- **TCP 监听**：默认从 `127.0.0.1:26946` 开始试，撞上 `AddrInUse` 就往后顺延试 **100 个端口**（最多到 `27045`）；显式传 `--listen IP:PORT` 只试指定地址（端口填 `0` 让系统挑空闲端口）。
- **私有集群参数**：`iroh` QUIC 集群端点默认按需延迟绑定——纯单机启动且未显式指定绑定地址时不开启任何 UDP 端口与中继连接，直到数据库里已有同伴（`cluster_peers` 非空）、或用户调 `POST /cluster/invite` / `POST /cluster/join` 组群时才按需拉起；可通过 **`--cluster-local`** 关闭公共中继与 DNS 发现（纯局域网/本机直连），用 **`--cluster-bind <IP:PORT>`** 在启动时显式绑定固定的 UDP 地址（端口填 `0` 时自动挑选可用 UDP 端口），以及用 **`--cluster-role <voter|edge>`** 指定单机初始投票角色（默认 `voter` 并写入 `local_node.role`；设成 `edge` 的边缘节点可自由离线不拖垮多数派门槛，同时仍参与占有探测与能力/执行调度——但注意它**只管还没进群前的初始落户**：一旦 `cluster_peers` 已经非空，重启时再想离线靠 `--cluster-role` 硬改角色会被当场拒绝，必须连群后走 `PATCH /cluster/self` 的两阶段联合多数派变更）。
- **Unix Socket（仅 Unix 平台编译，Windows 仅提供 TCP）**：默认路径改在了**系统临时目录下的按用户隔离目录** `<temp_dir>/mygo-<euid>/mygo.sock`（`--unix-socket` 可改路径，支持非 UTF-8 路径；`--no-unix-socket` 可关）：
  - 先以 `0700` 创建父目录，再用 `symlink_metadata` 严格核实确实是目录、属主是当前用户且同组/其他人无任何权限（防恶意抢占临时目录）；
  - 创建监听 Socket 前，在单线程初始化阶段临时把进程 `umask` 切到 `0o177` 原子创建出 `0600` 权限的 socket 文件再恢复 `umask`，杜绝权限窗口竞态；
  - 启动时**故意不自动删同名旧文件**——否则要是有活着的实例正用着同路径就会被覆盖误伤（`SIGKILL` 遗留的死文件报错退回人工处理）；正常退出清理时会再次比对 `(dev, ino)` 确认确实是自己创建的那枚 inode 才删。
- 想在**同一台机器同时跑两个实例**：只要各自换不同工作目录（分开 `mygo.sqlite3`），再给第二个实例换个 `--unix-socket` 路径（或加 `--no-unix-socket`）；如果要让同机多实例快速组建本地测试集群，还可以加上 `--cluster-local` 与固定 `--cluster-bind`。

---

## 原子安装与原生系统服务托管

在 macOS 和 Linux 上（Windows 目前不支持，调相关子命令会直接明确报错退出码 `2`），`mygo` 内置了完整的可执行文件安装与系统服务托管命令（`src/install.rs`、`src/service/*`）：

### 1. 原子装卸（`mygo install` / `mygo uninstall`）

- **用户级 vs 系统级**：默认用户级装进 `$HOME/.local/bin/mygo`，加 `--system` 装进 `/usr/local/bin/mygo`。程序带防呆权限检查：用户级禁止套着 `sudo` 误跑，系统级要求必须 root；如果已经是同一份文件（比对 `(dev, ino)`）就直接跳过，否则给出是否需加 `PATH` 或重启服务的提示。
- **原子替换正在运行的二进制**：哪怕旧版 `mygo` 正在后台跑，升级时也直接同目录创建 `.mygo.install.<pid>.<n>.tmp` 临时文件写入、`chmod 0o755`、`sync_all` 刷盘，再用 `rename` 原子替换目标路径（Linux/Unix 下旧进程继续用原 inode 跑直到下次重启，新进程立刻吃到新版）。
- **卸载先停服**：跑 `mygo uninstall` 会先停掉并注销对应的系统服务，再删除可执行文件（**始终保留数据库和日志文件**，不误删用户数据）。

### 2. macOS `launchd` 与 Linux `systemd` 对照（`mygo service <action>`）

支持 `install`、`start`、`stop`、`restart`、`status`、`logs [-f] [-n COUNT]`、`uninstall`。其中 `status` 和 `logs` 允许普通用户直接查询 `--system` 服务（碰到权限不足时会主动提示加上 `sudo`）；当服务处于运行态时，`status` 会拿着服务管理器给出的 PID 通过对应私有 Unix Socket 查询 `/_runtime`（刚启动绑定中最多每 50ms 重试一次、共 10 次），核对 PID 一致后实时打印当前 `Listen` 地址端口和 Bearer `Token`（不把 Token 写入磁盘；若普通用户无权连系统级 `0600` socket 则提示加 `sudo`）：

| 对比项 | macOS (`launchd`) | Linux (`systemd`) |
| :--- | :--- | :--- |
| **注册方式** | 用户：`~/Library/LaunchAgents/io.mygo.daemon.plist`<br>系统：`/Library/LaunchDaemons/io.mygo.daemon.plist` | 用户：`${XDG_CONFIG_HOME:-~/.config}/systemd/user/mygo.service`<br>系统：`/etc/systemd/system/mygo.service` |
| **工作/数据目录**（`0700`） | 用户：`~/Library/Application Support/mygo`<br>系统：`/Library/Application Support/mygo` | 用户：`${XDG_DATA_HOME:-~/.local/share}/mygo`<br>系统：`/var/lib/mygo` |
| **可预测 Unix Socket** | 用户：数据目录 `/mygo.sock`<br>系统：`/var/run/mygo.sock` | 运行时目录 `%t/mygo/mygo.sock`（`RuntimeDirectory=mygo`, `0700`） |
| **日志去向** | `~/Library/Logs/mygo/mygo.log` 或 `/var/log/mygo/mygo.log`（`0600`），`logs` 调 `/usr/bin/tail` | `journald`，`logs` 调 `journalctl [--user] -u mygo.service` |
| **拉起与保活** | `mygo daemon --foreground --unix-socket ...`<br>`KeepAlive: SuccessfulExit=false`（非零崩溃才重启）<br>`ExitTimeOut: 20s` | `mygo daemon --foreground --unix-socket ...`<br>`Restart=on-failure`, `RestartSec=3`, `UMask=0077`<br>`TimeoutStopSec=15` |
| **特有细节** | `bootout` 后每 **100ms** 轮询等待最多 **20 秒**彻底卸干净；PLIST 对 XML 控制字符严格拦截转义 | 安装前先用 `systemctl show` 预检管理器可用；用户级安装若未开启 `Linger` 会提示 `sudo loginctl enable-linger` |

安装服务时，如果目标位置还没装可执行文件，会用 `RenameFlags::NOREPLACE` 自动把当前二进制安全补装过去；若目标处已经躺着另一份不同版本的二进制，则会打一行警告保留原地不动，由你决定是否显式覆盖。另外需要留意：虽然临时文件写失败会自动清理，但一旦 plist/unit 文件已经成功写入磁盘而后面 `launchctl`/`systemctl` 启用失败，程序**不会自动删除已写好的配置和数据目录**，方便你排查现场。

---

## 版本元数据与极限瘦身发布

- **版本自动烙印（[build.rs](../build.rs)）**：默认直接读当前 Git 提交时间戳（按 Howard Hinnant 算法转为 UTC `YYYYMMDD`）、7 位短 SHA 和工作区脏标记，拼成 `0.1.0+YYYYMMDD.<sha7>[.dirty]` 写进 `MYGO_VERSION`（也支持用 `MYGO_BUILD` 环境变量显式指定或置空覆盖）；在 release 构建下还会调 `bun` 运行 [tools/minify-codemode.mjs](../tools/minify-codemode.mjs) 压缩内嵌的 `bootstrap.js` 和 `decoder.js` 脚本后再写进 `OUT_DIR`（找不到 `bun` 时回退原文件并给编译警告）。在 Windows 下则跳过内嵌 C 编译，直接动态链接系统自带的 `winsqlite3.dll`（32 位 x86 因 stdcall 调用约定回退到 `sqlite3`）。
- **极限体积瘦身**：[Cargo.toml](../Cargo.toml) 的 `[profile.release]` 打开了 `opt-level = "z"`、`lto = "fat"`、`panic = "abort"`、`codegen-units = 1`、`strip = "symbols"`，并关闭不必要的 crate 默认特性（如禁用 `tempfile` 默认特性以支撑有界附件流式临时文件）与 `rquickjs` 断言，同时给 `tracing` 启用 `release_max_level_info` 裁掉发布版调试日志分支（即便引入 `iroh` 与 `blake3` 实现 P2P 集群组网和半格律 CRDT 无冲突同步，`iroh` 也仅保留 `tls-ring` 与 `portmapper` 最小特性，当前 release 二进制仍维持约 `6.4 MiB`）。发布脚本 [tools/release.py](../tools/release.py)（通过 `mise run release` 调用）在 macOS/Linux 上进一步注入 `-C force-unwind-tables=no` 剥除异常展开表，随后用 `upx --best` 极限压缩（macOS 加 `--force-macos`）并跑 `upx -t` 校验，按平台输出三套发布包到 `target/<target>/release/dist/`：
  - **Linux**：UPX 压缩版 `mygo-<target>` + 基于压缩前 ELF `objdump -p` 抽取的依赖清单 `mygo-<target>.requirements.json`；
  - **macOS**：UPX 压缩版 `mygo-<target>.upx` + 用 `upx -d` 还原、逐字节与原二进制比对一致后打进 HFS+ LZMA 镜像的 `mygo-<target>.dmg`（确保挂载就能直接合法执行）；
  - **Windows**：UPX 压缩版 `mygo-<target>.exe`。

---

## 26 场景并发压测与 SQLite 铁证核验

为了证明第 1、2 篇吹下的“绝不算乱账、不丢记录、不堵车”不是纸上谈兵，仓库在 `examples/stress/` 下自带了一整套端到端压测夹具，通过 `mise run stress` 直接拉起 Release 二进制、独立临时目录、Unix Socket 与进程内模拟大模型进行对撞测试：

```sh
mise run stress --scenarios hot-enqueue,duplicate-run,idempotency,long-context,fanout --concurrency 1,16 --iterations 2
```

### 覆盖全部死角的 26 个压测场景

内置三档模板：`smoke`（跑齐全部 **26 个**场景，4 并发）、`full`（**25 个**场景，刻意剔除高并发下会耗尽本机临时端口的 `sse-churn`，跑 `1,16,64,256` 四级并发）、`soak`（5 个核心场景在 64 并发下长跑 300 秒）：

| 类别 | 场景名称 | 专门撞向哪个机关 |
| :--- | :--- | :--- |
| **基础与投信**（9） | `read`、`sessions`、`enqueue`、`hot-enqueue`、`large-enqueue`、`mixed`、`idempotency`、`withdraw`、`invalid` | 单会话热点 32 笔顺手拼车、64 KiB 大消息、同 `Idempotency-Key` 并发重投与 409 冲突、幂等撤回、边界 400/404 |
| **单轮与多轮循环**（8） | `turn`、`slow-turn`、`chunked-turn`、`multi-turn`、`enqueue-running`、`held-sse`、`long-context`、`tools` | 100ms 延迟回合、跨 SSE 帧半截断包、`steer` 运行中插话、长驻大量 SSE 连接广播、128 KiB 长上下文流式喂入与 `/history`/`/context` 分页对比、并行 2 个 `codemode`（含 UTC/本地时区日期） |
| **并发与广播**（3） | `duplicate-run`、`fanout`、`sse-churn` | 同一会话 4 并发 `/run` 拆批防重跑、8 路持久订阅扇出、`resync`/断线补课 |
| **容错、暂停与崩坏**（6） | `retry-http`、`retry-eof`、`retry-exhausted`、`pause-soft`、`pause-hard`、`restart` | `429`/`503` 重试、半路断流重试、连败 4 次耗尽暂停、软暂停收尾、硬暂停立刻停、`SIGKILL` 强杀重启后批量恢复与内存对比 |

### 怎么抓出藏起来的延迟与糊涂账？

- **闭环并发 vs 开环定速（`--rate`）**：默认固定并发度是“每个虚拟客户端等上一单做完才发下一单”，真遇到服务端卡顿发压会自动减速掩盖延迟（协同遗漏问题）。加上 `--rate` 后改成严格按时钟排定到达时刻硬塞，延迟从**理论计划出发点**算起——要是调度器本身迟到超过 **5 毫秒**直接记 `missed_schedule`，并发槽全满就记 `dropped_capacity`，而且把原始微秒用对数直方图精确落桶汇报 P50 / P95 / P99，配合每 **100ms** 一次的真实进程 RSS 采样。
- **收尾直接开库查铁证**：每轮测完优雅停机后，夹具不只是看 HTTP 有没有返回 200，而是**以只读模式直接打开 `mygo.sqlite3`**，核验 `PRAGMA quick_check == "ok"`、`pragma_foreign_key_check == 0`、每一笔 `user_message` / `assistant_message` / `function_call` / `function_call_output` / `withdrawal` 条数和发压账本一模一样，所有不该挂起的会话全回 `idle`——**只要盘上多记或少记了一条，照样当场判失败。**

---

## 真实环境端到端对比评测（`tools/frontier-eval/`）

压测测的是 MyGo 自己的调度和记账开销；真正放进真实 Linux 编程和运维任务里好不好使，还得跟同类前沿 Agent 真刀真枪比一比。

`mise run eval` 驱动了一套跑在 **Harbor 0.24** 上的自动化对比评测流水线，把 59 道固定镜像摘要和任务包摘要的 `terminal-bench` 实战题（`tasks.tsv`）送进隔离的 `linux/amd64` Docker 容器里，用完全相同的模型、推理档位、上下文配置和资源限制，让 **MyGo-Bot** 和基准对照 **Stock Pi（`pi-coding-agent@1.4.0`）** 同台作答：

- **可移植隔离打包（`pack-mygo.sh`）**：为了不被不同题目的老旧 Linux 基础镜像里的 glibc 或 OpenSSL 版本卡住，脚本在 `rust:1.100.0-bookworm` 容器里编好 `mygo`，顺着 `ldd` 把所有动态库连同 `ld-linux-x86-64.so.2` 加载器一起打进 `mygo.tgz`；
- **无污染考场运行**：考场容器里用独立的临时 `/tmp/mygo-frontier-*` 状态目录和 Unix Socket 启动 `mygo`，只留一个符号链接 `mygo.sqlite3 -> <state>/state.db` 跑测试，交卷后把账本 `items.jsonl`、全部费用流水 `llm_calls.jsonl` 捞出来并拆干净符号链接；
- **离线轨迹对照报告（`compare.py`）**：严密按 `(task, attempt)` 对齐生成单页交互式 `comparison.html`（大段 Base64 自动脱敏截断），直接把两边得分、失败测试点、镜像拉取异常、每阶段耗时、每步工具参数与结果以及完整 token 消耗摆在一起对比。

---

**上一篇**：[07 · 实时流、HTTP API、内置文档与 WebUI](07-realtime-api-and-webui.md) · **返回目录**：[总览](README.md)
