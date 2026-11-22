use std::{
    collections::{BTreeMap, VecDeque},
    sync::Mutex,
};

use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tokio::time::Instant;

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
    mcp_calls: Vec<(u64, &'static str)>,
}

#[derive(Default)]
pub(super) struct LiveState {
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

    pub fn fingerprint(
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
                .map(|(&id, c)| (id, c.phase))
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
            "id": id, "server": call.server, "operation": call.operation, "tool": call.tool,
            "phase": call.phase, "elapsed_ms": call.started.elapsed().as_millis(),
        })).collect());
        info["completed_commands"] = json!(self.completed_commands);
        info["running_commands"] = Value::Array(self.commands.iter_mut().map(|(id, c)| {
            let mut command = json!({
                "id": id, "program": c.program, "args": c.args, "args_truncated": c.args_truncated, "cwd": c.cwd,
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
    server: Option<String>,
    operation: String,
    tool: Option<String>,
    phase: &'static str,
    started: Instant,
}

pub(super) struct McpGuard<'a> {
    live: &'a Mutex<LiveState>,
    id: u64,
}

impl<'a> McpGuard<'a> {
    pub fn start(live: &'a Mutex<LiveState>, operation: &str, args: &Value) -> Self {
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
            server: name("server"),
            operation: operation.to_owned(),
            tool: if operation == "mcp.call" {
                name("tool")
            } else {
                None
            },
            phase: "looking_up",
            started: Instant::now(),
        };
        let mut state = live.lock().unwrap();
        state.next_mcp_call += 1;
        let id = state.next_mcp_call;
        state.mcp_calls.insert(id, call);
        Self { live, id }
    }

    pub fn set_phase(&self, phase: &'static str) {
        let mut state = self.live.lock().unwrap();
        state
            .mcp_calls
            .get_mut(&self.id)
            .expect("registered MCP call")
            .phase = phase;
    }
}

impl Drop for McpGuard<'_> {
    fn drop(&mut self) {
        self.live.lock().unwrap().mcp_calls.remove(&self.id);
    }
}

struct Command {
    program: String,
    args: Vec<String>,
    args_truncated: bool,
    cwd: Option<String>,
    started: Instant,
    started_at: DateTime<Utc>,
    stdout: Tail<{ 64 * 1024 }>,
    stderr: Tail<{ 64 * 1024 }>,
}

pub(super) enum Stream {
    Stdout,
    Stderr,
}

pub(super) struct CommandGuard<'a> {
    live: &'a Mutex<LiveState>,
    id: u64,
}

impl<'a> CommandGuard<'a> {
    pub fn start(
        live: &'a Mutex<LiveState>,
        program: &str,
        args: &[String],
        cwd: Option<&str>,
    ) -> Self {
        let mut args_truncated = args.len() > 16;
        let preview_args = args
            .iter()
            .take(16)
            .map(|arg| {
                if arg.len() > 256 {
                    args_truncated = true;
                    format!("{}…", &arg[..arg.floor_char_boundary(256)])
                } else {
                    arg.clone()
                }
            })
            .collect();
        let command = Command {
            program: program.to_owned(),
            args: preview_args,
            args_truncated,
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

    pub fn append(&self, stream: &Stream, chunk: &[u8]) {
        let mut live = self.live.lock().unwrap();
        let command = live.commands.get_mut(&self.id).expect("registered command");
        match stream {
            Stream::Stdout => command.stdout.append(chunk),
            Stream::Stderr => command.stderr.append(chunk),
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
