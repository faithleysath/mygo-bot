use crate::{
    Options, Result, check,
    scenarios::{Outcome, Work},
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, time::Duration};
use tokio::{
    task::JoinSet,
    time::{Instant, sleep_until, timeout},
};

// Round up to microseconds, then retain six fractional bits per power of two.
// Histogram storage stays bounded as soak runs accumulate samples.
#[derive(Default)]
struct Latency {
    bins: BTreeMap<u64, u64>,
    n: u64,
    max: u64,
}
impl Latency {
    fn record(&mut self, time: Duration) {
        let us = time.as_nanos().div_ceil(1000).max(1) as u64;
        let width = 1_u64 << us.ilog2().saturating_sub(6);
        *self.bins.entry(us.div_ceil(width) * width).or_default() += 1;
        self.n += 1;
        self.max = self.max.max(us);
    }
    fn percentile(&self, p: u64) -> Option<f64> {
        let rank = (self.n * p).div_ceil(100);
        let mut count = 0;
        for (&upper, &n) in &self.bins {
            count += n;
            if count >= rank {
                return Some(upper.min(self.max) as f64 / 1000.0);
            }
        }
        None
    }
    fn json(&self) -> Value {
        json!({
            "samples": self.n,
            "p50": self.percentile(50),
            "p95": self.percentile(95),
            "p99": self.percentile(99),
            "max": (self.n > 0).then_some(self.max as f64 / 1000.0),
        })
    }
}

#[derive(Default)]
pub struct Stats {
    pub users: u64,
    pub answers: u64,
    pub success: u64,
    pub errors: u64,
    capacity: u64,
    late: u64,
    latency: Latency,
    turn: Latency,
    examples: Vec<String>,
    elapsed: Duration,
}
impl Stats {
    fn accept(&mut self, result: Completion) -> usize {
        let (slot, elapsed, outcome) = result;
        match outcome {
            Ok(out) => {
                self.success += 1;
                self.users += out.users;
                self.answers += out.answers;
                self.latency.record(elapsed);
                if let Some(t) = out.turn_time {
                    self.turn.record(t);
                }
            }
            Err(error) => {
                self.errors += 1;
                if self.examples.len() < 3 {
                    self.examples.push(crate::error_chain(error.as_ref()));
                }
            }
        }
        slot
    }
    pub fn json(&self) -> Value {
        json!({
            "offered": self.success + self.errors + self.capacity + self.late,
            "success": self.success, "errors": self.errors,
            "dropped_capacity": self.capacity, "missed_schedule": self.late,
            "elapsed_s": self.elapsed.as_secs_f64(), "ops_s": self.ops_s(),
            "operation_ms": self.latency.json(), "enqueue_to_idle_ms": self.turn.json(),
            "error_examples": self.examples,
        })
    }
    fn ops_s(&self) -> f64 {
        self.success as f64 / self.elapsed.as_secs_f64().max(f64::EPSILON)
    }
    pub fn failures(&self, opts: &Options) -> Vec<&'static str> {
        let mut failures = Vec::new();
        if self.errors > 0 {
            failures.push("operation errors");
        }
        if self.success == 0 {
            failures.push("no successful operations");
        }
        if opts.fail_on_drop && self.capacity + self.late > 0 {
            failures.push("arrivals dropped");
        }
        if opts.min_ops.is_some_and(|min| self.ops_s() < min) {
            failures.push("throughput below minimum");
        }
        if let (Some(max), Some(p95)) = (opts.p95, self.latency.percentile(95))
            && p95 > max
        {
            failures.push("operation P95 above maximum");
        }
        failures
    }
}

type Completion = (usize, Duration, Result<Outcome>);
fn submit(
    jobs: &mut JoinSet<Completion>,
    work: &Work,
    slot: usize,
    limit: Duration,
    scheduled: Instant,
) {
    let work = work.clone();
    jobs.spawn(async move {
        let outcome = match timeout(limit, work.run(slot)).await {
            Ok(result) => result,
            Err(_) => Err(format!("operation exceeded {limit:?}").into()),
        };
        (slot, scheduled.elapsed(), outcome)
    });
}

pub async fn closed(
    work: &Work,
    c: usize,
    iterations: Option<u64>,
    duration: Duration,
    limit: Duration,
) -> Result<Stats> {
    let mut stats = Stats::default();
    if iterations == Some(0) {
        return Ok(stats);
    }
    let mut jobs = JoinSet::new();
    let mut completed = vec![0; c];
    let start = Instant::now();
    for slot in 0..c {
        submit(&mut jobs, work, slot, limit, start);
    }
    while let Some(result) = jobs.join_next().await {
        let slot = stats.accept(result?);
        completed[slot] += 1;
        if iterations.map_or_else(|| start.elapsed() < duration, |n| completed[slot] < n) {
            submit(&mut jobs, work, slot, limit, Instant::now());
        }
    }
    stats.elapsed = start.elapsed();
    Ok(stats)
}

pub async fn open(
    work: &Work,
    c: usize,
    rate: u64,
    duration: Duration,
    limit: Duration,
) -> Result<Stats> {
    let total = rate
        .checked_mul(duration.as_secs())
        .ok_or("arrival count overflow")?;
    let mut jobs = JoinSet::new();
    let mut available = (0..c).collect::<Vec<_>>();
    let mut stats = Stats::default();
    let start = Instant::now();
    let mut i = 0;
    let offset = |n: u64| Duration::from_secs_f64(n as f64 / rate as f64);
    let slots = |elapsed: Duration| {
        (elapsed.as_nanos() * rate as u128 / 1_000_000_000).min(total as u128) as u64
    };
    while i < total {
        sleep_until(start + offset(i)).await;
        while let Some(result) = jobs.try_join_next() {
            available.push(stats.accept(result?));
        }
        // Several arrivals may fall in one timer tick. Keep their original timestamps.
        // Expire slots older than 5 ms so a stalled generator cannot create a catch-up storm.
        let elapsed = start.elapsed();
        let expired = if elapsed >= duration {
            total
        } else if let Some(stale) = elapsed.checked_sub(Duration::from_millis(5)) {
            slots(stale).saturating_add(1).min(total)
        } else {
            0
        };
        if expired > i {
            stats.late += expired - i;
            i = expired;
        }
        let due = slots(elapsed).saturating_add(1).min(total);
        while i < due {
            let Some(slot) = available.pop() else {
                stats.capacity += due - i;
                i = due;
                break;
            };
            submit(&mut jobs, work, slot, limit, start + offset(i));
            i += 1;
        }
    }
    while let Some(result) = jobs.join_next().await {
        stats.accept(result?);
    }
    sleep_until(start + duration).await;
    stats.elapsed = start.elapsed();
    check(
        stats.success + stats.errors + stats.late + stats.capacity == total,
        "arrival accounting mismatch",
    )?;
    Ok(stats)
}
