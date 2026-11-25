use std::{
    borrow::Cow,
    collections::{BTreeMap, VecDeque},
    sync::Mutex,
};

use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tokio::{sync::Notify, time::Instant};

#[derive(Default)]
struct Tail<const LIMIT: usize> {
    bytes: VecDeque<u8>,
    total: u64,
}

impl<const LIMIT: usize> Tail<LIMIT> {
    fn append(&mut self, chunk: &[u8]) {
        self.total = self.total.saturating_add(chunk.len() as u64);
        let suffix = &chunk[chunk.len().saturating_sub(LIMIT)..];
        let discard = (self.bytes.len() + suffix.len()).saturating_sub(LIMIT);
        self.bytes.drain(..discard);
        self.bytes.extend(suffix);
    }

    fn append_with_dropped(&mut self, dropped: u64, chunk: &[u8]) {
        if dropped > 0 {
            self.total = self.total.saturating_add(dropped);
            self.bytes.clear();
        }
        self.append(chunk);
    }

    fn truncated(&self) -> bool {
        self.total > LIMIT as u64
    }

    fn text(&mut self) -> String {
        let truncated = self.truncated();
        let bytes = self.bytes.make_contiguous();
        // A tail can start in the middle of a UTF-8 character. Keep raw pipe
        // chunks together until this point, including characters split across reads.
        let skip = if truncated {
            bytes
                .iter()
                .take(3)
                .take_while(|b| **b & 0xc0 == 0x80)
                .count()
        } else {
            0
        };
        String::from_utf8_lossy(&bytes[skip..]).into_owned()
    }
}

#[derive(PartialEq, Eq)]
pub(super) struct ProgressFingerprint {
    stopping: bool,
    background: bool,
    deadline: Instant,
    console_epoch: u64,
    console_bytes: u64,
    completed_commands: u64,
    commands: Vec<(u64, u64, u64)>,
    last_mcp_id: u64,
    mcp_calls: Vec<(u64, Cow<'static, str>)>,
}

#[derive(Default)]
pub(crate) struct LiveState {
    console_epoch: u64,
    console: Tail<{ 256 * 1024 }>,
    has_line: bool,
    commands: BTreeMap<u64, Command>,
    next_command: u64,
    completed_commands: u64,
    mcp_calls: BTreeMap<u64, McpCall>,
    next_mcp_call: u64,
}

impl LiveState {
    pub fn log(&mut self, line: Option<&[u8]>) {
        if let Some(line) = line {
            if self.has_line {
                self.console.append(b"\n");
            }
            self.console.append(line);
            self.has_line = true;
        } else {
            self.console_epoch += u64::from(self.has_line);
            self.console = Tail::default();
            self.has_line = false;
        }
    }

    pub fn console_output(&mut self, include_whitespace: bool) -> Option<String> {
        let text = self.console.text();
        if text.is_empty() || (!include_whitespace && text.trim().is_empty()) {
            return None;
        }
        Some(if self.console.truncated() {
            format!("[console truncated to latest 256 KiB]\n{text}")
        } else {
            text
        })
    }

    pub(super) fn fingerprint(
        &self,
        stopping: bool,
        background: bool,
        deadline: Instant,
    ) -> ProgressFingerprint {
        ProgressFingerprint {
            stopping,
            background,
            deadline,
            console_epoch: self.console_epoch,
            console_bytes: self.console.total,
            completed_commands: self.completed_commands,
            commands: self
                .commands
                .iter()
                .map(|(&id, c)| (id, c.stdout.total, c.stderr.total))
                .collect(),
            last_mcp_id: self.next_mcp_call,
            mcp_calls: self
                .mcp_calls
                .iter()
                .map(|(&id, c)| (id, c.phase.clone()))
                .collect(),
        }
    }

    pub fn snapshot(&mut self, info: &mut Value, include_output: bool) {
        info["console"] =
            json!({"total_bytes": self.console.total, "truncated": self.console.truncated()});
        if include_output {
            info["console"]["output"] = json!(self.console.text());
        }
        info["running_mcp_calls"] = Value::Array(self.mcp_calls.iter().map(|(id, call)| json!({
            "id": id, "node": call.node, "server": call.server, "operation": call.operation, "tool": call.tool,
            "phase": call.phase, "elapsed_ms": call.started.elapsed().as_millis(),
        })).collect());
        info["completed_commands"] = json!(self.completed_commands);
        info["running_commands"] = Value::Array(self.commands.iter_mut().map(|(id, c)| {
            let mut command = json!({
                "id": id, "node": c.node, "program": c.program, "cwd": c.cwd,
                "started_at": c.started_at.to_rfc3339(), "elapsed_ms": c.started.elapsed().as_millis(),
                "stdout_bytes": c.stdout.total, "stdout_truncated": c.stdout.truncated(),
                "stderr_bytes": c.stderr.total, "stderr_truncated": c.stderr.truncated(),
            });
            if include_output {
                command["stdout"] = json!(c.stdout.text());
                command["stderr"] = json!(c.stderr.text());
            }
            command
        }).collect());
    }
}

struct McpCall {
    node: String,
    server: Option<String>,
    operation: String,
    tool: Option<String>,
    phase: Cow<'static, str>,
    started: Instant,
}

pub(crate) struct McpGuard<'a> {
    live: &'a Mutex<LiveState>,
    id: u64,
}

impl<'a> McpGuard<'a> {
    pub fn start(live: &'a Mutex<LiveState>, node: &str, operation: &str, args: &Value) -> Self {
        // Resource URIs, prompt arguments, tool inputs and pagination cursors can contain credentials.
        let name = |field: &str| {
            args[field].as_str().map(|s| {
                if s.len() > 256 {
                    "…".to_owned()
                } else {
                    s.to_owned()
                }
            })
        };
        let call = McpCall {
            node: node.to_owned(),
            server: name("server"),
            operation: operation.to_owned(),
            tool: if operation == "mcp.call" {
                name("tool")
            } else {
                None
            },
            phase: Cow::Borrowed("looking_up"),
            started: Instant::now(),
        };
        let mut state = live.lock().unwrap();
        state.next_mcp_call += 1;
        let id = state.next_mcp_call;
        state.mcp_calls.insert(id, call);
        Self { live, id }
    }

    pub fn set_phase(&self, phase: impl Into<Cow<'static, str>>) {
        let mut state = self.live.lock().unwrap();
        state
            .mcp_calls
            .get_mut(&self.id)
            .expect("registered MCP call")
            .phase = phase.into();
    }
}

impl Drop for McpGuard<'_> {
    fn drop(&mut self) {
        self.live.lock().unwrap().mcp_calls.remove(&self.id);
    }
}

struct Command {
    node: String,
    program: String,
    cwd: Option<String>,
    started: Instant,
    started_at: DateTime<Utc>,
    stdout: Tail<{ 64 * 1024 }>,
    stderr: Tail<{ 64 * 1024 }>,
}

pub(crate) enum Stream {
    Stdout,
    Stderr,
}

pub(crate) struct CommandGuard<'a> {
    live: &'a Mutex<LiveState>,
    id: u64,
}

impl<'a> CommandGuard<'a> {
    pub fn start(
        live: &'a Mutex<LiveState>,
        node: &str,
        program: &str,
        cwd: Option<&str>,
    ) -> Self {
        let command = Command {
            node: node.to_owned(),
            program: program.to_owned(),
            cwd: cwd.map(str::to_owned),
            started: Instant::now(),
            started_at: Utc::now(),
            stdout: Tail::default(),
            stderr: Tail::default(),
        };
        let mut state = live.lock().unwrap();
        state.next_command += 1;
        let id = state.next_command;
        state.commands.insert(id, command);
        Self { live, id }
    }

    pub fn set_cwd(&self, cwd: String) {
        let mut live = self.live.lock().unwrap();
        if let Some(command) = live.commands.get_mut(&self.id) {
            command.cwd = Some(cwd);
        }
    }

    pub fn append(&self, stream: &Stream, chunk: &[u8]) {
        self.append_with_dropped(stream, 0, chunk);
    }

    pub fn append_with_dropped(&self, stream: &Stream, dropped: u64, chunk: &[u8]) {
        let mut live = self.live.lock().unwrap();
        let command = live.commands.get_mut(&self.id).expect("registered command");
        match stream {
            Stream::Stdout => command.stdout.append_with_dropped(dropped, chunk),
            Stream::Stderr => command.stderr.append_with_dropped(dropped, chunk),
        }
    }
}

impl Drop for CommandGuard<'_> {
    fn drop(&mut self) {
        let mut live = self.live.lock().unwrap();
        live.commands.remove(&self.id);
        live.completed_commands += 1;
    }
}

#[derive(Default)]
struct PendingStream {
    bytes: VecDeque<u8>,
    dropped: u64,
}

impl PendingStream {
    const LIMIT: usize = 64 * 1024;

    fn push(&mut self, chunk: &[u8]) {
        if chunk.len() >= Self::LIMIT {
            let excess = chunk.len() - Self::LIMIT;
            self.dropped = self
                .dropped
                .saturating_add(self.bytes.len() as u64 + excess as u64);
            self.bytes.clear();
            self.bytes.extend(&chunk[excess..]);
        } else {
            let over = (self.bytes.len() + chunk.len()).saturating_sub(Self::LIMIT);
            if over > 0 {
                self.bytes.drain(..over);
                self.dropped = self.dropped.saturating_add(over as u64);
            }
            self.bytes.extend(chunk);
        }
    }

    fn take(&mut self) -> (u64, Vec<u8>) {
        let dropped = std::mem::take(&mut self.dropped);
        let bytes = self.bytes.drain(..).collect();
        (dropped, bytes)
    }

    fn is_empty(&self) -> bool {
        self.bytes.is_empty() && self.dropped == 0
    }
}

#[derive(Default)]
struct PendingProgress {
    stdout: PendingStream,
    stderr: PendingStream,
}

#[derive(Default)]
pub(crate) struct RemoteCommandProgress {
    pending: Mutex<PendingProgress>,
    notify: Notify,
}

impl RemoteCommandProgress {
    pub fn append(&self, stream: &Stream, chunk: &[u8]) {
        if chunk.is_empty() {
            return;
        }
        {
            let mut pending = self.pending.lock().unwrap();
            match stream {
                Stream::Stdout => pending.stdout.push(chunk),
                Stream::Stderr => pending.stderr.push(chunk),
            }
        }
        self.notify.notify_one();
    }

    pub fn drain(&self) -> (u64, Vec<u8>, u64, Vec<u8>) {
        let mut pending = self.pending.lock().unwrap();
        let (out_d, out_b) = pending.stdout.take();
        let (err_d, err_b) = pending.stderr.take();
        (out_d, out_b, err_d, err_b)
    }

    pub fn has_pending(&self) -> bool {
        let pending = self.pending.lock().unwrap();
        !pending.stdout.is_empty() || !pending.stderr.is_empty()
    }

    pub async fn wait_pending(&self) {
        let notified = self.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.has_pending() {
            notified.await;
        }
    }
}

pub(crate) enum CommandPreview<'a> {
    Local(&'a CommandGuard<'a>),
    Remote(&'a RemoteCommandProgress),
}

impl CommandPreview<'_> {
    pub fn append(&self, stream: &Stream, chunk: &[u8]) {
        match self {
            Self::Local(guard) => guard.append(stream, chunk),
            Self::Remote(remote) => remote.append(stream, chunk),
        }
    }
}
