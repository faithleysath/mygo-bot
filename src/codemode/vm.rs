use super::{Blocks, Host, HostError, Job, Outcome, Reply};
use crate::id::AttachmentId;
use rquickjs::{
    AsyncContext, AsyncRuntime, Ctx, Error, Exception, Function, Object, Persistent, Promise,
    Result as JsResult, String as JsString, TypedArray, Value, context::EvalOptions,
};
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    future::{Future, poll_fn},
    io,
    rc::{Rc, Weak},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    thread,
};
use tokio::{
    runtime::Builder,
    sync::{Semaphore, SemaphorePermit, mpsc},
    task::{Id, JoinError, JoinSet, LocalSet},
};
use tokio_util::sync::CancellationToken;

const MIB: usize = 1024 * 1024;
const IO_BYTES: usize = 8 * MIB;
const METADATA_BYTES: usize = 256 * 1024;
const HOST_CALLS: usize = 16;
const JOBS_PER_TURN: usize = 64;
static IO_SLOTS: Semaphore = Semaphore::const_new(32);

struct Worker {
    sender: mpsc::UnboundedSender<Job>,
    active: Arc<AtomicUsize>,
}

pub struct Pool {
    workers: Vec<Worker>,
    next: AtomicUsize,
    stop: CancellationToken,
}

impl Pool {
    pub fn new(threads: usize) -> io::Result<Self> {
        if threads == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "expected at least one JS worker",
            ));
        }
        let mut pool = Self {
            workers: Vec::new(),
            next: AtomicUsize::new(0),
            stop: CancellationToken::new(),
        };
        for index in 0..threads {
            let rt = Builder::new_current_thread().build()?;
            let (sender, mut receiver) = mpsc::unbounded_channel();
            let active = Arc::new(AtomicUsize::new(0));
            let load = active.clone();
            let stop = pool.stop.clone();
            thread::Builder::new()
                .name(format!("codemode-js-{index}"))
                .stack_size(4 * MIB)
                .spawn(move || {
                    let local = LocalSet::new();
                    local.block_on(&rt, async {
                        while let Some(job) = receiver.recv().await {
                            let (load, stop) = (load.clone(), stop.clone());
                            tokio::task::spawn_local(async move {
                                run(job, stop).await;
                                load.fetch_sub(1, Ordering::Relaxed);
                            });
                        }
                    });
                    rt.block_on(local);
                })?;
            pool.workers.push(Worker { sender, active });
        }
        Ok(pool)
    }

    pub fn submit(&self, job: Job) -> Result<(), String> {
        let start = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        let worker = self.workers[start..]
            .iter()
            .chain(&self.workers[..start])
            .filter(|w| !w.sender.is_closed())
            .min_by_key(|w| w.active.load(Ordering::Relaxed))
            .ok_or("JS workers have stopped")?;
        worker.active.fetch_add(1, Ordering::Relaxed);
        worker.sender.send(job).map_err(|_| {
            worker.active.fetch_sub(1, Ordering::Relaxed);
            "JS worker stopped".into()
        })
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

// The permit also bounds completed responses waiting for their turn on a JS worker.
type HostResult = (Result<Reply, HostError>, Option<SemaphorePermit<'static>>);
type Completed = Result<(Id, HostResult), JoinError>;
type Callback = Persistent<Function<'static>>;
type Resolvers = RefCell<HashMap<Id, (Callback, Callback)>>;

struct Scope {
    host: Host,
    cancel: CancellationToken,
    stop: CancellationToken,
    accepting: Cell<bool>,
    jobs: RefCell<JoinSet<HostResult>>,
    output: RefCell<Blocks>,
}

impl Scope {
    fn stopped(&self) -> bool {
        self.cancel.is_cancelled() || self.stop.is_cancelled()
    }

    fn admit(&self, ctx: &Ctx<'_>) -> JsResult<()> {
        if self.stopped() || !self.accepting.get() {
            return Err(Exception::throw_message(ctx, "codemode task stopped"));
        }
        if self.jobs.borrow().len() >= HOST_CALLS {
            return Err(Exception::throw_message(
                ctx,
                "backpressure: too many pending host calls",
            ));
        }
        Ok(())
    }

    fn dispatch<'js>(
        &self,
        ctx: &Ctx<'js>,
        resolvers: &Weak<Resolvers>,
        permit: Option<SemaphorePermit<'static>>,
        future: impl Future<Output = Result<Reply, HostError>> + Send + 'static,
    ) -> JsResult<Promise<'js>> {
        // Reading JavaScript arguments may have reentered another host binding.
        self.admit(ctx)?;
        let resolvers = resolvers
            .upgrade()
            .ok_or_else(|| Exception::throw_message(ctx, "codemode task closed"))?;
        let (promise, yes, no) = Promise::new(ctx)?;
        let task = self
            .jobs
            .borrow_mut()
            .spawn_on(async move { (future.await, permit) }, &self.host.executor);
        resolvers.borrow_mut().insert(
            task.id(),
            (Persistent::save(ctx, yes), Persistent::save(ctx, no)),
        );
        Ok(promise)
    }
}

async fn run(job: Job, stop: CancellationToken) {
    let scope = Rc::new(Scope {
        host: job.host,
        cancel: job.cancel,
        stop,
        accepting: Cell::new(true),
        jobs: RefCell::default(),
        output: RefCell::default(),
    });
    let mut error = drive(&scope, &job.code).await.err();
    scope.cancel.cancel();
    // Never abort a host future. Managed commands finish process-group cleanup inside it.
    let mut jobs = scope.jobs.take();
    while jobs.join_next().await.is_some() {}
    let mut output = scope.output.take();
    let console = scope
        .host
        .live
        .lock()
        .unwrap()
        .console_output(error.is_some());
    if let Some(logs) = console {
        if let Some(err) = &mut error {
            err.push_str("\n\nConsole output:\n");
            err.push_str(&logs);
        } else if let Err(e) = output.append_console(logs) {
            output = Blocks::default();
            error = Some(e);
        }
    }
    drop(scope); // Release unread file and HTTP bodies before reporting completion.
    let _ = job.done.send(Outcome { error, output });
}

async fn drive(scope: &Rc<Scope>, code: &str) -> Result<(), String> {
    if scope.stopped() {
        return Err("codemode task cancelled".into());
    }
    let rt = AsyncRuntime::new().map_err(|e| e.to_string())?;
    rt.set_memory_limit(96 * MIB).await;
    rt.set_max_stack_size(256 * 1024).await;
    let (cancel, stop) = (scope.cancel.clone(), scope.stop.clone());
    rt.set_interrupt_handler(Some(Box::new(move || {
        cancel.is_cancelled() || stop.is_cancelled()
    })))
    .await;
    let context = AsyncContext::full(&rt).await.map_err(|e| e.to_string())?;
    // Only weak references to these JS handles enter native functions owned by the VM.
    let resolvers = Rc::new(Resolvers::default());
    let root = context
        .with(|ctx| {
            let start = || {
                install(&ctx, scope, &resolvers)?;
                let mut options = EvalOptions::default();
                options.filename = Some("codemode.js".into());
                let body = format!("(async () => {{{code}\n}})()");
                ctx.eval_with_options::<Persistent<Promise<'static>>, _>(body, options)
            };
            start().map_err(|e| {
                scope.accepting.set(false);
                describe(&ctx, e)
            })
        })
        .await?;

    let mut ready = None;
    loop {
        let mut idle = false;
        for turn in 0..JOBS_PER_TURN {
            if scope.stopped() {
                return Err("codemode task cancelled".into());
            }
            let finished = context
                .with(|ctx| {
                    let check = || {
                        let promise = root.clone().restore(&ctx)?;
                        let Some(value) = promise.result::<Value>() else {
                            return Ok(false);
                        };
                        // Returning a content block or describing an error can invoke JS getters.
                        scope.accepting.set(false);
                        let value = value?;
                        if !value.is_undefined() {
                            *scope.output.borrow_mut() = blocks(&ctx, value, true)?;
                        }
                        Ok(true)
                    };
                    check().map_err(|e| describe(&ctx, e))
                })
                .await?;
            if finished {
                return Ok(());
            }
            if turn == 0
                && let Some(done) = ready
                    .take()
                    .or_else(|| scope.jobs.borrow_mut().try_join_next_with_id())
            {
                context
                    .with(|ctx| settle(&ctx, &resolvers, done).map_err(|e| describe(&ctx, e)))
                    .await?;
            }
            match rt.execute_pending_job().await {
                Ok(true) => (),
                Ok(false) => {
                    idle = true;
                    break;
                }
                Err(exception) => {
                    return Err(exception
                        .0
                        .with(|ctx| describe(&ctx, Error::Exception))
                        .await);
                }
            }
        }
        if idle {
            // An empty JoinSet means the root can only wake through cancellation.
            ready = tokio::select! {
                biased;
                _ = scope.cancel.cancelled() => return Err("codemode task cancelled".into()),
                _ = scope.stop.cancelled() => return Err("codemode task cancelled".into()),
                done = poll_fn(|cx| match scope.jobs.borrow_mut().poll_join_next_with_id(cx) {
                    Poll::Ready(None) => Poll::Pending,
                    other => other,
                }) => done,
            };
        } else {
            tokio::task::yield_now().await;
        }
    }
}

fn settle<'js>(ctx: &Ctx<'js>, resolvers: &Resolvers, done: Completed) -> JsResult<()> {
    let (id, (reply, _permit)) = match done {
        Ok(done) => done,
        Err(e) => (e.id(), (Err(format!("host task failed: {e}").into()), None)),
    };
    let (yes, no) = resolvers
        .borrow_mut()
        .remove(&id)
        .ok_or_else(|| Exception::throw_message(ctx, "unknown host completion"))?;
    let value = match reply {
        Ok(reply) => reply_value(ctx, reply),
        Err(e) => {
            let error = Exception::from_message(ctx.clone(), e.message())?;
            if let Some(code) = e.code() {
                error.set("code", code)?;
            }
            Err(ctx.throw(error.into_value()))
        }
    };
    match value {
        Ok(value) => yes.restore(ctx)?.call((value,)),
        Err(Error::Exception) => no.restore(ctx)?.call((ctx.catch(),)),
        Err(e) => no
            .restore(ctx)?
            .call((Exception::from_message(ctx.clone(), &e.to_string())?,)),
    }
}

fn reply_value<'js>(ctx: &Ctx<'js>, reply: Reply) -> JsResult<Value<'js>> {
    let value = ctx.json_parse(reply.value.to_string())?;
    let Some(data) = reply.data else {
        return Ok(value);
    };
    let bytes = TypedArray::<u8>::new_copy(ctx.clone(), &data)?;
    if let Some(object) = value.as_object() {
        object.set("body", bytes)?;
        Ok(value)
    } else {
        Ok(bytes.into_value())
    }
}

fn install<'js>(ctx: &Ctx<'js>, scope: &Rc<Scope>, resolvers: &Rc<Resolvers>) -> JsResult<()> {
    let io = {
        let scope = scope.clone();
        let resolvers = Rc::downgrade(resolvers);
        Function::new(
            ctx.clone(),
            move |ctx: Ctx<'js>, op: String, meta: JsString<'js>, data: Value<'js>| {
                scope.admit(&ctx)?;
                let permit = if op == "sleep" {
                    None
                } else {
                    Some(IO_SLOTS.try_acquire().map_err(|_| {
                        Exception::throw_message(&ctx, "backpressure: global I/O limit reached")
                    })?)
                };
                let meta = meta.to_cstring()?;
                if meta.len() > METADATA_BYTES {
                    return Err(Exception::throw_range(
                        &ctx,
                        "host arguments exceed 256 KiB",
                    ));
                }
                let args = serde_json::from_slice(string_bytes(&meta))
                    .map_err(|e| Exception::throw_type(&ctx, &e.to_string()))?;
                let data = input_bytes(&ctx, data)?;
                let host = scope.host.clone();
                scope.dispatch(&ctx, &resolvers, permit, async move {
                    host.io(op, args, data).await
                })
            },
        )?
    };
    let emit = {
        let scope = scope.clone();
        let resolvers = Rc::downgrade(resolvers);
        Function::new(ctx.clone(), move |ctx: Ctx<'js>, value: Value<'js>| {
            scope.admit(&ctx)?;
            let blocks = blocks(&ctx, value, false)?;
            let host = scope.host.clone();
            scope.dispatch(&ctx, &resolvers, None, async move {
                let id = host.emit(blocks).await?;
                Ok(Reply {
                    value: serde_json::json!({"event_id":id}),
                    data: None,
                })
            })
        })?
    };
    let log = {
        let scope = scope.clone();
        Function::new(ctx.clone(), move |line: Option<JsString<'_>>| {
            if !scope.accepting.get() || scope.stopped() {
                return;
            }
            match line {
                Some(line) => {
                    if let Ok(raw) = line.to_cstring() {
                        scope
                            .host
                            .live
                            .lock()
                            .unwrap()
                            .log(Some(string_bytes(&raw)));
                    }
                }
                None => scope.host.live.lock().unwrap().log(None),
            }
        })?
    };
    let streams = scope.host.streams.clone();
    let close = Function::new(ctx.clone(), move |id: u32| streams.close(id))?;
    let decode = Function::new(
        ctx.clone(),
        |ctx: Ctx<'js>, data: TypedArray<'js, u8>, fatal: bool| {
            // SAFETY: decoding finishes before the next JS call.
            let bytes = unsafe { data.as_bytes() }
                .ok_or_else(|| Exception::throw_type(&ctx, "detached Uint8Array"))?;
            if bytes.len() > IO_BYTES {
                return Err(Exception::throw_range(&ctx, "response exceeds 8 MiB"));
            }
            if fatal {
                let text = std::str::from_utf8(bytes)
                    .map_err(|_| Exception::throw_type(&ctx, "invalid UTF-8"))?;
                JsString::from_str(ctx, text)
            } else {
                JsString::from_str(ctx, &String::from_utf8_lossy(bytes))
            }
        },
    )?;
    let encode = Function::new(ctx.clone(), |ctx: Ctx<'js>, text: JsString<'js>| {
        let raw = text.to_cstring()?;
        let bytes = string_bytes(&raw);
        if bytes.len() > IO_BYTES {
            return Err(Exception::throw_range(&ctx, "encoded text exceeds 8 MiB"));
        }
        if std::str::from_utf8(bytes).is_ok() {
            return TypedArray::<u8>::new_copy(ctx, bytes);
        }
        let mut owned = bytes.to_vec();
        wtf8_to_utf8(&mut owned);
        TypedArray::<u8>::new_copy(ctx, &owned)
    })?;
    let decoder: Function = ctx.eval(include_str!("decoder.js"))?;
    decoder.call::<_, ()>((decode.clone(), encode))?;
    let task_id = (!scope.host.trigger).then(|| scope.host.origin.task_id());
    let init: Function = ctx.eval(include_str!("bootstrap.js"))?;
    init.call((
        io,
        emit,
        close,
        decode,
        log,
        scope.host.sid.as_str(),
        task_id,
    ))
}

// QuickJS may encode an unpaired JS surrogate as WTF-8. Validate or decode lossily
// at each boundary before constructing a Rust str.
fn string_bytes<'a>(s: &'a rquickjs::CString<'_>) -> &'a [u8] {
    // SAFETY: CString owns this possibly NUL-containing buffer for the borrow.
    unsafe { std::slice::from_raw_parts(s.as_ptr().cast(), s.len()) }
}

// Replace QuickJS 3-byte WTF-8 lone surrogates (ED A0..BF 80..BF) with UTF-8 U+FFFD.
fn wtf8_to_utf8(bytes: &mut [u8]) {
    let mut i = 0;
    while i + 3 <= bytes.len() {
        if bytes[i] == 0xed
            && (0xa0..=0xbf).contains(&bytes[i + 1])
            && (0x80..=0xbf).contains(&bytes[i + 2])
        {
            bytes[i..i + 3].copy_from_slice(&[0xef, 0xbf, 0xbd]);
            i += 3;
        } else {
            i += 1;
        }
    }
}

fn input_bytes(ctx: &Ctx<'_>, value: Value<'_>) -> JsResult<Option<Vec<u8>>> {
    if value.is_undefined() || value.is_null() {
        return Ok(None);
    }
    let copy = |bytes: &[u8]| {
        if bytes.len() > IO_BYTES {
            return Err(Exception::throw_range(ctx, "host data exceeds 8 MiB"));
        }
        Ok(Some(bytes.to_vec()))
    };
    if let Some(text) = value.as_string() {
        let text = text.clone().to_cstring()?;
        let bytes = string_bytes(&text);
        if std::str::from_utf8(bytes).is_err() {
            return Err(Exception::throw_type(ctx, "expected valid UTF-8 text"));
        }
        return copy(bytes);
    }
    let data = TypedArray::<u8>::from_value(value)?;
    // SAFETY: copy only examines the slice and creates owned Rust data.
    let bytes = unsafe { data.as_bytes() }
        .ok_or_else(|| Exception::throw_type(ctx, "detached Uint8Array"))?;
    copy(bytes)
}

fn raw_binary(value: &Value<'_>) -> bool {
    use rquickjs::qjs;
    let raw = value.as_raw();
    // SAFETY: these predicates only inspect the class of a live JS value.
    unsafe {
        qjs::JS_IsArrayBuffer(raw) || qjs::JS_GetTypedArrayType(raw) >= 0 || qjs::JS_IsDataView(raw)
    }
}

const BINARY_HINT: &str =
    "raw binary data must be wrapped in an image, audio or video block or decoded with TextDecoder";

fn block_candidate(value: &Value<'_>) -> JsResult<bool> {
    let Some(object) = value.as_object() else {
        return Ok(false);
    };
    if value.is_array() || value.is_function() || raw_binary(value) {
        return Ok(false);
    }
    let kind: Value = object.get("type")?;
    if let Some(kind) = kind.as_string()
        && matches!(
            kind.to_string()?.as_str(),
            "text" | "audio" | "image" | "video"
        )
    {
        return Ok(true);
    }
    Ok(object.contains_key("attachment_id")?
        || (object.contains_key("mime_type")? && object.get::<_, Value>("data")?.is_object()))
}

fn blocks<'js>(ctx: &Ctx<'js>, value: Value<'js>, returning: bool) -> JsResult<Blocks> {
    let mut blocks = Blocks::default();
    let text_block = |text: String| {
        let mut blocks = Blocks::default();
        if !returning || !text.trim().is_empty() {
            blocks
                .push_text(text)
                .map_err(|e| Exception::throw_message(ctx, &e))?;
        }
        Ok(blocks)
    };
    if let Some(text) = value.as_string() {
        return text_block(text.to_string()?);
    }
    if raw_binary(&value) {
        return Err(Exception::throw_type(ctx, BINARY_HINT));
    }
    let strict_part = |blocks: &mut Blocks, value: Value<'js>| {
        if raw_binary(&value) {
            return Err(Exception::throw_type(ctx, BINARY_HINT));
        }
        let part = value
            .as_object()
            .ok_or_else(|| Exception::throw_type(ctx, "expected a content block"))?;
        push_part(ctx, blocks, part).map_err(|e| {
            if e.is_from_js() {
                Exception::throw_type(ctx, &e.to_string())
            } else {
                e
            }
        })
    };
    if returning && block_candidate(&value)? {
        strict_part(&mut blocks, value)?;
        return Ok(blocks);
    }
    if let Some(array) = value.as_array() {
        // Array::len assumes a small integer; QuickJS also permits huge sparse arrays.
        let len: f64 = array.as_object().get("length")?;
        if len > (super::output::TASK_BYTES / 2) as f64 {
            return Err(Exception::throw_range(
                ctx,
                "array exceeds task output budget",
            ));
        }
        let len = len as usize;
        let mut strict = !returning;
        if returning {
            for index in 0..len.min(1025) {
                let item: Value = array.get(index)?;
                if raw_binary(&item) || block_candidate(&item)? {
                    strict = true;
                    break;
                }
            }
        }
        if strict {
            if !(1..=1024).contains(&len) {
                return Err(Exception::throw_range(
                    ctx,
                    "expected 1 to 1024 content blocks",
                ));
            }
            for index in 0..len {
                strict_part(&mut blocks, array.get(index)?)?;
            }
            return Ok(blocks);
        }
    } else if !returning {
        return Err(Exception::throw_type(
            ctx,
            "expected text or a content block array",
        ));
    }
    let replacer = Function::new(
        ctx.clone(),
        |ctx: Ctx<'js>, _key: Value<'js>, item: Value<'js>| {
            if raw_binary(&item) {
                return Err(Exception::throw_type(&ctx, BINARY_HINT));
            }
            Ok(item)
        },
    )?;
    let json = ctx
        .json_stringify_replacer(value, replacer)?
        .ok_or_else(|| Exception::throw_type(ctx, "return value cannot be serialized as JSON"))?;
    text_block(json.to_string()?)
}

fn push_part<'js>(ctx: &Ctx<'js>, blocks: &mut Blocks, part: &Object<'js>) -> JsResult<()> {
    let kind: String = part.get("type")?;
    let result = match kind.as_str() {
        "text" => {
            fields(part, &["type", "text"])?;
            blocks.push_text(part.get("text")?)
        }
        "image" | "audio" | "video" => {
            let raw: Value = part.get("attachment_id")?;
            if !raw.is_undefined() && !raw.is_null() {
                fields(part, &["type", "attachment_id"])?;
                let id = raw
                    .as_string()
                    .and_then(|s| s.to_string().ok())
                    .and_then(|s| s.parse::<AttachmentId>().ok())
                    .ok_or_else(|| {
                        Exception::throw_type(ctx, "expected a valid attachment_id or media data")
                    })?;
                blocks.push_attachment(kind, id)
            } else {
                fields(part, &["type", "mime_type", "data"])?;
                let mime = part.get("mime_type")?;
                let data: TypedArray<u8> = part.get("data")?;
                // SAFETY: push_bytes reserves its budget and copies without calling JS.
                let bytes = unsafe { data.as_bytes() }
                    .ok_or_else(|| Exception::throw_type(ctx, "detached Uint8Array"))?;
                blocks.push_bytes(kind, mime, bytes)
            }
        }
        "blob" => {
            return Err(Exception::throw_type(
                ctx,
                "MCP resource blob cannot be sent as model content; use block.data in the script or save it with node.fs.write",
            ));
        }
        _ => {
            return Err(Exception::throw_type(
                ctx,
                "expected a text, image, audio or video block",
            ));
        }
    };
    result.map_err(|e| Exception::throw_type(ctx, &e))
}

fn fields(part: &Object<'_>, expected: &[&str]) -> JsResult<()> {
    let invalid = || {
        Exception::throw_type(
            part.ctx(),
            &format!("expected block fields {}", expected.join(", ")),
        )
    };
    let mut count = 0;
    for key in part.keys::<JsString>() {
        if !expected.contains(&key?.to_string()?.as_str()) {
            return Err(invalid());
        }
        count += 1;
    }
    if count != expected.len() {
        return Err(invalid());
    }
    Ok(())
}

fn describe(ctx: &Ctx<'_>, error: Error) -> String {
    if !matches!(error, Error::Exception) {
        return error.to_string();
    }
    let thrown = ctx.catch();
    let bounded = |s: JsString<'_>| -> Option<String> {
        let s = s.to_cstring().ok()?;
        let bytes = string_bytes(&s);
        Some(String::from_utf8_lossy(&bytes[..bytes.len().min(4096)]).into_owned())
    };
    if let Some(object) = thrown.as_object() {
        let field = |key: &str| object.get::<_, JsString>(key).ok().and_then(bounded);
        let name = field("name").unwrap_or_else(|| "Error".into());
        let message = field("message").unwrap_or_default();
        let stack = field("stack").unwrap_or_default();
        format!("{name}: {message}\n{stack}").trim_end().to_owned()
    } else {
        thrown
            .as_string()
            .cloned()
            .and_then(bounded)
            .or_else(|| thrown.as_number().map(|n| n.to_string()))
            .unwrap_or_else(|| format!("threw {}", thrown.type_name()))
    }
}
