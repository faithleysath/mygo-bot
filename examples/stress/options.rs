use crate::{Result, check, scenarios::CASES};
use std::{
    env,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub(super) const HELP: &str = r#"MyGo-Bot 压测 · macOS / Linux
  mise run stress                         快速验证全部场景，4 个客户端
  mise run stress --profile full          常规场景，1、16、64、256 并发，每档发压 5 秒
  mise run stress --profile soak          64 并发，混合读写、短回合、慢回合、工具和 SSE 各 300 秒
  mise run stress --scenarios turn,enqueue --concurrency 1,16,64 --duration 10 --repeat 3
  mise run stress --scenarios slow-turn --rate 200 --concurrency 16 --duration 10

  mise run stress --scenarios sse-churn --rate 20 --concurrency 16 --duration 10

  --list                     列出场景及每次操作的内容
  --profile smoke|full|soak   选择预设，显式参数优先
  --scenarios NAME,...        选择场景
  --concurrency N,...         客户端数，定速模式下表示在途操作上限
  --iterations N             每个客户端运行 N 次，适用于固定并发
  --duration SECONDS         发压时长，结束后等在途操作完成
  --warmup N                 每客户端预热次数，smoke 默认为 0，其余为 1
  --repeat N                 重复次数，每个场景、并发档和轮次各用新进程与新数据库
  --rate OPS/S               每秒计划到达的操作数，记录容量不足和调度迟到的次数
  --timeout SECONDS          单次操作或恢复阶段的超时，默认 60 秒
  --max-p95-ms N             操作 P95 高于此值时失败
  --min-ops N                每秒成功操作数低于此值时失败
  --fail-on-drop             定速发压遇到容量丢弃或漏发时失败
  --output FILE              新建 JSONL 报告，默认保存在系统临时目录

每个进程使用系统分配的本机端口、独立 SQLite 库和本地模拟模型。
operation_ms 统计整个操作，enqueue_to_idle_ms 从发第一条消息计到最后一次收到 idle。
分位数只取成功样本，按微秒向上取整后记入直方图，桶宽最大为所在量级的 1.57%。
吞吐计入等待在途操作结束的时间。定速模式从计划到达时刻计时，调度迟到超过 5 ms 记为漏发。
每 100 ms 采样 Agent 进程 RSS，单位 KiB。报告另记可执行文件的字节数。
restart 每档中断并恢复整个进程一次，记录批次恢复耗时。held-sse 将所有订阅保持 --duration 秒后广播。
sse-churn 反复新建和断开 TCP 连接，可触及本机临时端口上限。smoke 包含此场景，持续压测时单独选择。
restart 和 held-sse 按并发档和重复次数运行。迭代数、到达率、预热、性能阈值适用于其余负载场景。
操作、数据校验或指定的性能阈值失败时，返回非零退出码。
"#;

#[derive(serde::Serialize)]
pub(super) struct Options {
    pub(super) profile: String,
    pub(super) cases: Vec<String>,
    pub(super) clients: Vec<usize>,
    pub(super) iterations: Option<u64>,
    pub(super) seconds: u64,
    pub(super) warmup: u64,
    pub(super) repeat: usize,
    pub(super) rate: Option<u64>,
    pub(super) timeout_secs: u64,
    pub(super) p95: Option<f64>,
    pub(super) min_ops: Option<f64>,
    pub(super) fail_on_drop: bool,
    pub(super) output: PathBuf,
}
fn positive<T: std::str::FromStr + Default + PartialOrd>(value: &str, flag: &str) -> Result<T> {
    value
        .parse()
        .ok()
        .filter(|n| *n > T::default())
        .ok_or_else(|| format!("{flag} requires a positive number").into())
}
impl Options {
    pub(super) fn parse(args: &[String]) -> Result<Self> {
        let mut profile = "smoke".to_owned();
        let (mut cases, mut clients, mut iterations, mut seconds) = (None, None, None, None);
        let (mut warmup, mut repeat, mut rate, mut timeout_secs, mut p95, mut min_ops) =
            (None, 1, None, 60, None, None);
        let mut fail_on_drop = false;
        let mut output = None;
        let mut args = args.iter();
        while let Some(flag) = args.next() {
            if flag == "--fail-on-drop" {
                fail_on_drop = true;
                continue;
            }
            let value = args
                .next()
                .ok_or_else(|| format!("missing value for {flag}"))?;
            match flag.as_str() {
                "--profile" => profile = value.clone(),
                "--scenarios" => {
                    cases = Some(value.split(',').map(str::to_owned).collect::<Vec<_>>())
                }
                "--concurrency" => {
                    clients = Some(
                        value
                            .split(',')
                            .map(|v| positive(v, flag))
                            .collect::<Result<Vec<_>>>()?,
                    )
                }
                "--iterations" => iterations = Some(positive(value, flag)?),
                "--duration" => seconds = Some(positive(value, flag)?),
                "--warmup" => warmup = Some(value.parse::<u64>()?),
                "--repeat" => repeat = positive(value, flag)?,
                "--rate" => rate = Some(positive(value, flag)?),
                "--timeout" => timeout_secs = positive(value, flag)?,
                "--max-p95-ms" => p95 = Some(positive::<f64>(value, flag)?),
                "--min-ops" => min_ops = Some(positive::<f64>(value, flag)?),
                "--output" => output = Some(PathBuf::from(value)),
                _ => return Err(format!("unknown option {flag}").into()),
            }
        }
        check(
            matches!(profile.as_str(), "smoke" | "full" | "soak"),
            "unknown profile",
        )?;
        check(
            iterations.is_none() || (rate.is_none() && seconds.is_none()),
            "--iterations conflicts with --duration and --rate",
        )?;
        check(
            !fail_on_drop || rate.is_some(),
            "--fail-on-drop requires --rate",
        )?;
        check(
            p95.into_iter().chain(min_ops).all(f64::is_finite),
            "threshold must be finite",
        )?;
        let cases = cases.unwrap_or_else(|| {
            if profile == "soak" {
                ["mixed", "turn", "slow-turn", "tools", "fanout"]
                    .map(str::to_owned)
                    .to_vec()
            } else {
                CASES
                    .iter()
                    .filter(|(name, _)| profile == "smoke" || *name != "sse-churn")
                    .map(|(name, _)| name.to_string())
                    .collect()
            }
        });
        for name in &cases {
            check(
                CASES.iter().any(|(id, _)| id == name),
                format!("unknown scenario {name}"),
            )?;
        }
        if profile == "smoke" && seconds.is_none() && rate.is_none() && iterations.is_none() {
            iterations = Some(1);
        }
        let clients = clients.unwrap_or_else(|| match profile.as_str() {
            "smoke" => vec![4],
            "full" => vec![1, 16, 64, 256],
            _ => vec![64],
        });
        let seconds = seconds.unwrap_or(if profile == "soak" { 300 } else { 5 });
        let warmup = warmup.unwrap_or(u64::from(profile != "smoke"));
        let output = output.unwrap_or_else(|| {
            env::temp_dir().join(format!(
                "mygo-stress-{}-{}.jsonl",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ))
        });
        Ok(Self {
            profile,
            cases,
            clients,
            iterations,
            seconds,
            warmup,
            repeat,
            rate,
            timeout_secs,
            p95,
            min_ops,
            fail_on_drop,
            output,
        })
    }
    pub(super) fn limit(&self) -> Duration {
        Duration::from_secs(self.timeout_secs)
    }
}
