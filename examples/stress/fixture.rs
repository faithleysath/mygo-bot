use crate::{Result, check, client::Api};
use rusqlite::Connection;
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::{Instant, sleep};

pub struct Directory(pub PathBuf);
impl Directory {
    pub fn new() -> Result<Self> {
        let dir = std::env::temp_dir().join(format!(
            "mygo-stress-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        fs::create_dir(&dir)?;
        Ok(Self(dir))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

pub struct Agent(Child);
impl Drop for Agent {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
impl Agent {
    pub async fn launch(bin: &Path, dir: &Path, timeout: Duration) -> Result<(Self, Api)> {
        let log = dir.join("agent.log");
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes)?;
        let token = format!("{:032x}", u128::from_ne_bytes(bytes));
        let mut command = Command::new(bin);
        command.args([
            "daemon",
            "--foreground",
            "--listen",
            "127.0.0.1:0",
            "--token",
            &token,
        ]);
        #[cfg(unix)]
        command.args(["--unix-socket", "agent.sock"]);
        let child = command
            .current_dir(dir)
            .env("NO_PROXY", "*")
            .env("no_proxy", "*")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(File::create(&log)?)
            .spawn()?;
        let mut agent = Self(child);
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let message = fs::read_to_string(&log)?;
            if let Some(status) = agent.0.try_wait()? {
                return Err(format!("agent startup {status}: {}", message.trim()).into());
            }
            if let Some(address) = message.split_whitespace().find_map(|field| {
                field
                    .strip_prefix("listen=")
                    .and_then(|value| value.parse().ok())
            }) {
                let api = Api::new(address, &token, timeout)?;
                api.get("/config").await?;
                return Ok((agent, api));
            }
            check(
                Instant::now() < deadline,
                "agent did not announce readiness",
            )?;
            sleep(Duration::from_millis(10)).await;
        }
    }
    pub fn pid(&self) -> u32 {
        self.0.id()
    }
    pub fn crash(&mut self) -> Result<()> {
        self.0.kill()?;
        self.0.wait()?;
        Ok(())
    }
    pub async fn stop(&mut self) -> Result<()> {
        check(
            Command::new("kill")
                .args(["-TERM", &self.pid().to_string()])
                .status()?
                .success(),
            "could not signal agent",
        )?;
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.0.try_wait()? {
                return check(status.success(), format!("agent shutdown {status}"));
            }
            check(Instant::now() < deadline, "agent shutdown timed out")?;
            sleep(Duration::from_millis(10)).await;
        }
    }
}

pub fn rss_kib(pid: u32) -> Result<u64> {
    #[cfg(target_os = "linux")]
    {
        let status = fs::read_to_string(format!("/proc/{pid}/status"))?;
        let line = status
            .lines()
            .find(|line| line.starts_with("VmRSS:"))
            .ok_or("missing VmRSS")?;
        Ok(line
            .split_whitespace()
            .nth(1)
            .ok_or("missing RSS")?
            .parse()?)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let output = Command::new("ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .output()?;
        check(output.status.success(), "ps could not sample child RSS")?;
        Ok(std::str::from_utf8(&output.stdout)?.trim().parse()?)
    }
}

pub async fn with_rss<F, T>(pid: u32, idle: u64, work: F) -> Result<(T, Value)>
where
    F: Future<Output = Result<T>>,
{
    let (stop, mut stopped) = tokio::sync::oneshot::channel();
    let mut sampler = tokio::task::JoinSet::new();
    sampler.spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let start = Instant::now();
        let mut samples = 0;
        let mut peak = idle;
        let mut series = Vec::new();
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                _ = tick.tick() => {
                    let rss = tokio::task::spawn_blocking(move || rss_kib(pid)).await??;
                    peak = peak.max(rss);
                    if samples % 10 == 0 { series.push(json!([start.elapsed().as_secs_f64(), rss])); }
                    samples += 1;
                }
            }
        }
        let end = rss_kib(pid)?;
        Ok::<_,crate::Error>(json!({"rss_peak_kib":peak.max(end),"rss_end_kib":end,"rss_samples":samples,"rss_series_s_kib":series}))
    });
    let result = work.await?;
    let _ = stop.send(());
    let memory = sampler.join_next().await.ok_or("RSS sampler missing")???;
    Ok((result, memory))
}

pub fn audit(dir: &Path, case: &str, users: u64, answers: u64) -> Result<Value> {
    let path = dir.join("mygo.sqlite3");
    // The last connection may leave committed pages in WAL across process exit.
    let main_bytes = fs::metadata(&path)?.len();
    let sidecar_bytes = |suffix: &str| -> std::io::Result<u64> {
        match fs::metadata(dir.join(format!("mygo.sqlite3-{suffix}"))) {
            Ok(meta) => Ok(meta.len()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(err) => Err(err),
        }
    };
    let wal_bytes = sidecar_bytes("wal")?;
    let shm_bytes = sidecar_bytes("shm")?;
    let conn = Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let integrity: String = conn.query_row("PRAGMA quick_check", [], |r| r.get(0))?;
    let foreign_keys: i64 =
        conn.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| {
            r.get(0)
        })?;
    let count = |kind: &str| {
        conn.query_row("SELECT count(*) FROM items WHERE kind=?", [kind], |r| {
            r.get::<_, i64>(0)
        })
    };
    let stored = (count("user_message")?, count("assistant_message")?);
    check(
        integrity == "ok"
            && foreign_keys == 0
            && stored == (i64::try_from(users)?, i64::try_from(answers)?),
        format!(
            "DB audit: quick_check={integrity}, FK errors={foreign_keys}, saved={stored:?}, expected=({users}, {answers})"
        ),
    )?;
    let expected_tools = if case == "tools" { stored.1 * 2 } else { 0 };
    let expected_withdrawals = if case == "withdraw" { stored.0 } else { 0 };
    check(
        count("function_call")? == expected_tools
            && count("function_call_output")? == expected_tools,
        "persisted tool calls and results do not match completed operations",
    )?;
    check(
        count("withdrawal")? == expected_withdrawals,
        "incorrect number of withdrawal records",
    )?;
    let non_idle: i64 = conn.query_row(
        "SELECT count(*) FROM sessions WHERE status != 'idle'",
        [],
        |r| r.get(0),
    )?;
    let expected_paused = if case == "retry-exhausted" {
        stored.0
    } else {
        0
    };
    check(
        non_idle == expected_paused,
        "unexpected unfinished sessions after shutdown",
    )?;
    Ok(json!({
        "quick_check": integrity, "foreign_key_errors": foreign_keys,
        "user_messages": stored.0, "answers": stored.1, "tool_results": expected_tools,
        "withdrawals": expected_withdrawals, "non_idle_sessions": non_idle,
        "database_bytes": main_bytes + wal_bytes + shm_bytes,
        "main_bytes": main_bytes, "wal_bytes": wal_bytes, "shm_bytes": shm_bytes,
    }))
}
