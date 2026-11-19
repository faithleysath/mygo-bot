mod client;
mod fixture;
mod load;
mod model;
mod options;
mod scenarios;

use client::Api;
use fixture::{Agent, Directory, rss_kib};
use model::Model;
use options::{HELP, Options};
use reqwest::Method;
use scenarios::{CASES, Work};
use serde_json::{Value, json};
use std::{
    env,
    fs::{self, File},
    io::Write,
    path::Path,
    process::Command,
    time::Duration,
};
use tokio::{
    task::JoinSet,
    time::{Instant, timeout},
};

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;
fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut detail = error.to_string();
    let mut cause = error.source();
    while let Some(err) = cause {
        detail.push_str(": ");
        detail.push_str(&err.to_string());
        cause = err.source();
    }
    detail
}

fn check(condition: bool, msg: impl Into<String>) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(msg.into().into())
    }
}

async fn restart(
    agent: &mut Agent,
    bin: &Path,
    dir: &Path,
    work: &Work,
    c: usize,
    limit: Duration,
) -> Result<Value> {
    let mut setup = JoinSet::new();
    for _ in 0..c {
        let w = work.clone();
        setup.spawn(async move {
            let (path, key, ticket) = w.prepare_turn().await?;
            w.api.enqueue(&path, &key).await?;
            w.api.start(&path).await?;
            ticket.control.entered.notified().await;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((path, ticket))
        });
    }
    let mut in_flight = vec![];
    timeout(limit, async {
        while let Some(result) = setup.join_next().await {
            in_flight.push(result??);
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await??;
    let before = rss_kib(agent.pid())?;
    let start = Instant::now();
    agent.crash()?;
    for (_, ticket) in &in_flight {
        ticket.control.release.notify_one();
    }
    *agent = Agent::launch(bin, dir).await?;
    let mut resume = JoinSet::new();
    for (path, ticket) in in_flight {
        let api = work.api.clone();
        resume.spawn(async move {
            let state = api.get(&path).await?;
            check(
                state["status"] == "paused" && state["last_error"] == "daemon restarted",
                "running session not recovered as paused",
            )?;
            let mut events = api.listen(&path).await?;
            api.start(&path).await?;
            let seen = events.terminal("idle").await?;
            check(
                seen.answers == 1 && ticket.control.attempts() == 2,
                "wrong answer or call count after restart",
            )
        });
    }
    timeout(limit, async {
        while let Some(result) = resume.join_next().await {
            result??;
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await??;
    Ok(
        json!({"resumed":c,"batch_recovery_ms":start.elapsed().as_secs_f64()*1000.0,"rss_before_kib":before,"rss_after_kib":rss_kib(agent.pid())?}),
    )
}

async fn load_stage(opts: &Options, work: &Work, clients: usize) -> Result<(Value, u64, u64)> {
    let duration = Duration::from_secs(opts.seconds);
    if work.case == "held-sse" {
        let result = work
            .held_subscribers(clients, duration, opts.limit())
            .await?;
        return Ok((json!({"passed":true,"subscribers":result}), 1, 0));
    }
    let warm = load::closed(work, clients, Some(opts.warmup), duration, opts.limit()).await?;
    check(warm.errors == 0, format!("warmup failed: {}", warm.json()))?;
    let measured = match opts.rate {
        Some(rate) => load::open(work, clients, rate, duration, opts.limit()).await?,
        None => load::closed(work, clients, opts.iterations, duration, opts.limit()).await?,
    };
    let failures = measured.failures(opts);
    Ok((
        json!({"passed":failures.is_empty(), "failures":failures, "warmup_ops":warm.success, "load":measured.json()}),
        warm.users + measured.users,
        warm.answers + measured.answers,
    ))
}

async fn stage(
    opts: &Options,
    bin: &Path,
    model: &Model,
    url: &str,
    case: &str,
    c: usize,
) -> Result<Value> {
    let dir = Directory::new()?;
    let mut agent = Agent::launch(bin, &dir.0).await?;
    let idle = rss_kib(agent.pid())?;
    let api = Api::new(opts.limit())?;
    api.json(
        Method::PATCH,
        "/config",
        Some(json!({"base_url":url,"model":case})),
        200,
    )
    .await?;
    let work = Work::new(api, model.clone(), case, c).await?;
    let (mut row, users, answers) = if case == "restart" {
        let recovery = restart(&mut agent, bin, &dir.0, &work, c, opts.limit()).await?;
        (
            json!({"passed":true,"recovery":recovery}),
            c as u64,
            c as u64,
        )
    } else {
        let ((mut row, users, answers), memory) =
            fixture::with_rss(agent.pid(), idle, load_stage(opts, &work, c)).await?;
        row.as_object_mut()
            .unwrap()
            .extend(memory.as_object().unwrap().clone());
        (row, users, answers)
    };
    drop(work);
    agent.stop().await?;
    match fixture::audit(&dir.0, case, users, answers) {
        Ok(audit) => row["database"] = audit,
        Err(err) => {
            row["passed"] = json!(false);
            row["audit_error"] = json!(err.to_string());
        }
    }
    row["rss_idle_kib"] = json!(idle);
    Ok(row)
}

fn line(file: &mut File, value: &Value) -> Result<()> {
    serde_json::to_writer(&mut *file, value)?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(())
}

async fn suite(opts: &Options) -> Result<()> {
    let exe = env::current_exe()?;
    let bin = exe
        .parent()
        .and_then(Path::parent)
        .ok_or("missing release directory")?
        .join("mygo-bot");
    let size = fs::metadata(&bin)
        .map_err(|e| format!("{}: {e}; run via mise run stress", bin.display()))?
        .len();
    let mut file = File::create_new(&opts.output)?;
    println!("Report: {}", opts.output.display());
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string());
    line(
        &mut file,
        &json!({
            "type": "environment", "schema_version": 1, "options": opts, "revision": revision,
            "os": env::consts::OS, "arch": env::consts::ARCH,
            "logical_cpus": std::thread::available_parallelism()?.get(),
            "binary": bin, "binary_bytes": size, "started_at": chrono::Utc::now().to_rfc3339(),
        }),
    )?;
    let model = Model::default();
    let (url, server) = model.serve().await?;
    let mut passed = 0;
    let mut total = 0;
    let mut aborted = None;
    'runs: for rep in 1..=opts.repeat {
        for case in &opts.cases {
            for &c in &opts.clients {
                let result = stage(opts, &bin, &model, &url, case, c).await;
                let mut row = match result {
                    Ok(row) => row,
                    Err(err) => {
                        let message = error_chain(err.as_ref());
                        aborted = Some(err);
                        json!({"passed":false, "error":message})
                    }
                };
                row["type"] = json!("stage");
                row["scenario"] = json!(case);
                row["concurrency"] = json!(c);
                row["repetition"] = json!(rep);
                let ok = row["passed"] == true;
                println!(
                    "{case:16} c={c:<4} rep={rep} {} {}",
                    if ok { "PASS" } else { "FAIL" },
                    if let Some(rate) = row["load"]["ops_s"].as_f64() {
                        format!(
                            "{rate:.1} ops/s · P95 {} ms · errors {} · dropped {} + {}",
                            row["load"]["operation_ms"]["p95"],
                            row["load"]["errors"],
                            row["load"]["dropped_capacity"],
                            row["load"]["missed_schedule"]
                        )
                    } else {
                        row.get("recovery")
                            .or_else(|| row.get("subscribers"))
                            .or_else(|| row.get("error"))
                            .unwrap_or(&Value::Null)
                            .to_string()
                    }
                );
                line(&mut file, &row)?;
                passed += usize::from(ok);
                total += 1;
                if aborted.is_some() {
                    break 'runs;
                }
            }
        }
    }
    server.abort();
    line(
        &mut file,
        &json!({"type":"summary", "stages":total, "passed":passed, "failed":total-passed, "aborted":aborted.is_some()}),
    )?;
    if let Some(error) = aborted {
        return Err(error);
    }
    check(
        passed == total,
        format!(
            "{} stage(s) failed; see {}",
            total - passed,
            opts.output.display()
        ),
    )
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args: Vec<_> = env::args().skip(1).collect();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        print!("{HELP}");
        return Ok(());
    }
    if args.iter().any(|a| a == "--list") {
        for (name, detail) in CASES {
            println!("{name:16} {detail}");
        }
        return Ok(());
    }
    let opts = Options::parse(&args)?;
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let cancelled = async {
        #[cfg(unix)]
        {
            tokio::select! {_=tokio::signal::ctrl_c()=>(),_=terminate.recv()=>()}
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    };
    tokio::select! {result=suite(&opts)=>result,_=cancelled=>Err("interrupted".into())}
}
