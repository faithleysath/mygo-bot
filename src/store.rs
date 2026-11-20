use std::{collections::HashMap, fs::OpenOptions, path::Path, thread};

use axum::body::Bytes;
use reqwest::Body;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::{Error, Result, agent::Control, events::Notice};

// One owner serializes transactions and the matching in-memory worker transitions.
// A slow disk never blocks the HTTP executor. No job holds SQLite across network I/O.
type Acknowledge = Box<dyn FnOnce(&mut Store, &Result<()>)>;
type ApplyWrite = Box<dyn FnOnce(Result<&Connection>, bool) -> (bool, Acknowledge) + Send>;

struct Write {
    start_session: Option<i64>,
    apply: ApplyWrite,
}

impl Write {
    fn can_join(&self, batch: &[Write]) -> bool {
        let starts = batch[0].start_session.is_some();
        match self.start_session {
            None => !starts,
            // A repeated start must observe the worker published by the previous commit.
            Some(sid) => starts && !batch.iter().any(|write| write.start_session == Some(sid)),
        }
    }
}

enum Command {
    Call(Box<dyn FnOnce(&mut Store) + Send>),
    Write(Write),
}

struct Enqueue {
    sid: i64,
    content: Value,
    key: Option<String>,
}

pub enum Enqueued {
    Created(i64),
    Existing(i64),
}

#[derive(Clone)]
pub struct Db(mpsc::Sender<Command>);

pub struct Store {
    pub conn: Connection,
    pub active: HashMap<i64, Control>,
    channels: HashMap<i64, broadcast::WeakSender<Notice>>,
    pub closing: bool,
}

impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        let mut options = OpenOptions::new();
        options.create(true).write(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options.open(path)?;
        let mut conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA foreign_keys=ON; PRAGMA locking_mode=EXCLUSIVE; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA fullfsync=ON;")?;
        let tx = conn.transaction()?;
        let version: i64 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        match version {
            0 => tx.execute_batch(include_str!("../schema.sql"))?,
            1 if tx.column_exists(None, "llm_models", "archived_at")? => (),
            1 => {
                return Err(Error::internal(
                    "database uses the previous schema; select a new database directory",
                ));
            }
            _ => return Err(Error::internal("unsupported database version")),
        }
        tx.execute("UPDATE llm_calls SET status='interrupted',error_summary='daemon restarted' WHERE status='in_flight'", [])?;
        tx.execute("UPDATE sessions SET status='paused', last_error='daemon restarted' WHERE status IN ('running','pausing')", [])?;
        tx.commit()?;
        let (sender, mut receiver) = mpsc::channel::<Command>(64);
        let runtime = tokio::runtime::Handle::current();
        thread::Builder::new()
            .name("sqlite".into())
            .spawn(move || {
                let _entered = runtime.enter();
                let mut store = Store {
                    conn,
                    active: HashMap::new(),
                    channels: HashMap::new(),
                    closing: false,
                };
                let mut next = None;
                let mut batch = Vec::new();
                while let Some(command) = next.take().or_else(|| receiver.blocking_recv()) {
                    match command {
                        Command::Call(call) => call(&mut store),
                        Command::Write(first) => {
                            batch.push(first);
                            // Drain only compatible writes already waiting at the head of the queue.
                            while batch.len() < 32 {
                                match receiver.try_recv() {
                                    Ok(Command::Write(write)) if write.can_join(&batch) => {
                                        batch.push(write)
                                    }
                                    other => {
                                        next = other.ok();
                                        break;
                                    }
                                }
                            }
                            store.commit_batch(&mut batch);
                        }
                    }
                }
            })?;
        Ok(Self(sender))
    }

    pub async fn call<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Store) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.submit(|reply| {
            Command::Call(Box::new(move |store| {
                let _ = reply.send(f(store));
            }))
        })
        .await
    }

    pub async fn enqueue(&self, sid: i64, content: Value, key: Option<String>) -> Result<Enqueued> {
        self.write(
            move |conn| Enqueue { sid, content, key }.insert(conn),
            move |store, saved| {
                if let Enqueued::Created(id) = saved {
                    store.item_saved(sid, id)?;
                }
                Ok(saved)
            },
        )
        .await
    }

    // The database thread runs apply in FIFO order, then publish in that same order after COMMIT.
    pub async fn write<T: 'static, R: Send + 'static>(
        &self,
        apply: impl FnOnce(&Connection) -> Result<T> + Send + 'static,
        publish: impl FnOnce(&mut Store, T) -> Result<R> + Send + 'static,
    ) -> Result<R> {
        self.grouped_write(None, move |conn, _| apply(conn), publish)
            .await
    }

    pub async fn start_write<T: 'static, R: Send + 'static>(
        &self,
        sid: i64,
        apply: impl FnOnce(&Connection, bool) -> Result<T> + Send + 'static,
        publish: impl FnOnce(&mut Store, T) -> Result<R> + Send + 'static,
    ) -> Result<R> {
        self.grouped_write(Some(sid), apply, publish).await
    }

    async fn grouped_write<T: 'static, R: Send + 'static>(
        &self,
        start_session: Option<i64>,
        apply: impl FnOnce(&Connection, bool) -> Result<T> + Send + 'static,
        publish: impl FnOnce(&mut Store, T) -> Result<R> + Send + 'static,
    ) -> Result<R> {
        self.submit(|reply| {
            Command::Write(Write {
                start_session,
                apply: Box::new(move |conn, active| {
                    let result = conn.and_then(|conn| apply(conn, active));
                    (
                        result.is_ok(),
                        Box::new(move |store, committed| {
                            let result = match committed {
                                Ok(()) => result.and_then(|value| publish(store, value)),
                                Err(error) => Err(error.clone()),
                            };
                            let _ = reply.send(result);
                        }),
                    )
                }),
            })
        })
        .await
    }

    async fn submit<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<T>>) -> Command,
    ) -> Result<T> {
        let (send, recv) = oneshot::channel();
        self.0
            .send(command(send))
            .await
            .map_err(|_| Error::internal("database worker stopped"))?;
        recv.await
            .map_err(|_| Error::internal("database worker stopped"))?
    }
}

impl Enqueue {
    fn insert(&self, conn: &Connection) -> Result<Enqueued> {
        if let Some(key) = &self.key {
            let mut lookup = conn.prepare_cached("SELECT id,json_extract(payload,'$.content') FROM items WHERE session_id=? AND idempotency_key=?")?;
            if let Some((id, content)) = lookup
                .query_row((self.sid, key), |row| {
                    Ok((row.get(0)?, row.get::<_, Value>(1)?))
                })
                .optional()?
            {
                if content != self.content {
                    return Err(Error::conflict(
                        "idempotency key reused with different content",
                    ));
                }
                return Ok(Enqueued::Existing(id));
            }
        }
        let model_id: Option<i64> = conn
            .prepare_cached("SELECT model_id FROM sessions WHERE id=?")?
            .query_row([self.sid], |row| row.get(0))?;
        let model_id = model_id
            .ok_or_else(|| Error::conflict("select a session model before sending messages"))?;
        let (modalities, _) = crate::catalog::available_capabilities(conn, model_id)?;
        crate::media::validate_message(conn, &self.content, &modalities)?;
        conn.prepare_cached("INSERT INTO items(session_id,kind,payload,idempotency_key) VALUES(?1,'user_message',json_object('content',json(?2),'role','user'),?3)")?.execute(params![self.sid, self.content, self.key])?;
        Ok(Enqueued::Created(conn.last_insert_rowid()))
    }
}

impl Store {
    fn commit_batch(&mut self, batch: &mut Vec<Write>) {
        let starting = batch[0].start_session.is_some();
        let mut replies = Vec::with_capacity(batch.len());
        let mut pending = batch.drain(..);
        let committed = (|| -> Result<()> {
            if starting && self.closing {
                return Err(Error(
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    "shutting down".into(),
                ));
            }
            let mut tx = self.conn.transaction()?;
            for write in pending.by_ref() {
                // A failed multi-statement operation must not leave part of its reply in the DB.
                let savepoint = tx.savepoint();
                let active = write
                    .start_session
                    .is_some_and(|id| self.active.contains_key(&id));
                let (ok, reply) = (write.apply)(
                    savepoint
                        .as_deref()
                        .map_err(|e| Error::internal(e.to_string())),
                    active,
                );
                replies.push(reply);
                let savepoint = savepoint?;
                if ok {
                    savepoint.commit()?;
                } else {
                    savepoint.finish()?;
                }
            }
            tx.commit()?;
            Ok(())
        })();
        if let Err(error) = &committed {
            replies.extend(pending.map(|write| (write.apply)(Err(error.clone()), false).1));
        }
        for reply in replies {
            reply(self, &committed);
        }
    }

    pub fn channel(&mut self, sid: i64) -> broadcast::Sender<Notice> {
        if let Some(sender) = self.channels.get(&sid).and_then(|w| w.upgrade()) {
            return sender;
        }
        self.channels.retain(|_, w| w.strong_count() > 0);
        let (sender, _) = broadcast::channel(64);
        self.channels.insert(sid, sender.downgrade());
        sender
    }

    pub fn item_saved(&self, sid: i64, id: i64) -> Result<()> {
        if let Some(channel) = self.channels.get(&sid).and_then(|w| w.upgrade())
            && channel.receiver_count() > 0
        {
            let mut stmt = self.conn.prepare_cached(
                "SELECT id,kind,payload,target_id,llm_call_id FROM items WHERE id=?",
            )?;
            Notice::publish(&channel, "item.saved", stmt.query_row([id], history_row)?);
        }
        Ok(())
    }

    pub fn status(&mut self, sid: i64, state: &str, reason: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE sessions SET status=?2,last_error=?3 WHERE id=?1",
            (sid, state, reason),
        )?;
        self.notify_status(sid, state, reason);
        Ok(())
    }

    pub fn notify_status(&self, sid: i64, state: &str, reason: &str) {
        if let Some(channel) = self.channels.get(&sid).and_then(|w| w.upgrade()) {
            Notice::publish(
                &channel,
                "session.status",
                json!({"status":state,"reason":reason}),
            );
        }
    }
}

pub fn history_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Value> {
    Ok(json!({
        "id": row.get::<_, i64>(0)?, "kind": row.get::<_, String>(1)?,
        "payload": row.get::<_, Value>(2)?, "target_id": row.get::<_, Option<i64>>(3)?, "llm_call_id": row.get::<_, Option<i64>>(4)?
    }))
}

pub fn session(conn: &Connection, sid: i64) -> Result<Value> {
    let mut stmt = conn.prepare_cached(
        "SELECT id,status,reasoning_effort,last_error,model_id FROM sessions WHERE id=?",
    )?;
    Ok(stmt.query_row([sid], session_row)?)
}

pub fn session_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Value> {
    Ok(
        json!({"id":row.get::<_,i64>(0)?,"status":row.get::<_,String>(1)?,"reasoning_effort":row.get::<_,Option<String>>(2)?,"last_error":row.get::<_,String>(3)?,"model_id":row.get::<_,Option<i64>>(4)?}),
    )
}

// This view is shared by queue inspection, consumption and the idle transition.
pub fn has_pending(conn: &Connection, sid: i64) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM pending WHERE session_id=?)",
        [sid],
        |r| r.get(0),
    )?)
}

pub fn take_pending(tx: &Connection, sid: i64) -> Result<()> {
    let added = tx.execute("INSERT INTO context_items(session_id,item_id) SELECT session_id,id FROM pending WHERE session_id=? ORDER BY id",[sid])?;
    if added > 0 {
        tx.execute("UPDATE sessions SET needs_model=1,user_queue_cursor=(SELECT max(id) FROM items WHERE session_id=?1 AND kind='user_message') WHERE id=?1",[sid])?;
    }
    Ok(())
}

pub fn add_item(
    conn: &Connection,
    sid: i64,
    kind: &str,
    payload: &Value,
    target: Option<i64>,
    llm_call_id: Option<i64>,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO items(session_id,kind,payload,target_id,llm_call_id) VALUES(?,?,?,?,?)",
        params![sid, kind, payload, target, llm_call_id],
    )?;
    Ok(conn.last_insert_rowid())
}

pub fn add_context_item(
    tx: &Connection,
    sid: i64,
    kind: &str,
    item: &Value,
    call_id: i64,
) -> Result<i64> {
    let id = add_item(tx, sid, kind, item, None, Some(call_id))?;
    tx.execute(
        "INSERT INTO context_items(session_id,item_id) VALUES(?,?)",
        (sid, id),
    )?;
    Ok(id)
}

// Publish all completed tools in call order. A missing result after interruption
// is recorded explicitly: replaying an already executed tool could repeat its effect.
pub fn publish_tools(tx: &Connection, sid: i64, recovering: bool) -> Result<Vec<i64>> {
    let mut recovered = Vec::new();
    let mut after = 0;
    let mut pending = tx.prepare_cached(
        "SELECT c.id, json_extract(c.payload,'$.call_id'), r.id FROM items c
         LEFT JOIN items r ON r.kind='function_call_output' AND r.target_id=c.id
         WHERE c.session_id=?1 AND c.kind='function_call' AND c.id>?2
           AND NOT EXISTS(SELECT 1 FROM context_items WHERE item_id=c.id)
         ORDER BY c.id LIMIT 1",
    )?;
    while let Some((id, call, output)) = pending
        .query_row((sid, after), |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<i64>>(2)?,
            ))
        })
        .optional()?
    {
        after = id;
        let result = match output {
            Some(id) => id,
            None if recovering => {
                let payload = json!({"type":"function_call_output","call_id":call,"output":"Execution interrupted; result unknown."});
                let result = add_item(tx, sid, "function_call_output", &payload, Some(id), None)?;
                recovered.push(result);
                result
            }
            None => return Err(Error::internal("missing tool result")),
        };
        tx.execute(
            "INSERT INTO context_items(session_id,item_id) VALUES(?1,?2),(?1,?3)",
            (sid, id, result),
        )?;
    }
    Ok(recovered)
}

const BATCH_ITEMS: i64 = 256;
const BATCH_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy)]
pub enum JsonRows {
    History,
    Context,
    ModelInput,
}

impl JsonRows {
    // Both tables are append-only. Pin the last cursor of this page before sending
    // bytes so a concurrent append can't appear halfway through the response.
    pub fn page_end(self, conn: &Connection, sid: i64, after: i64, limit: i64) -> Result<i64> {
        let sql = match self {
            Self::History => {
                "SELECT max(id) FROM (SELECT id FROM items WHERE session_id=? AND id>? ORDER BY id LIMIT ?)"
            }
            Self::Context | Self::ModelInput => {
                "SELECT max(position) FROM (SELECT position FROM context_items WHERE session_id=? AND position>? ORDER BY position LIMIT ?)"
            }
        };
        let end: Option<i64> = conn
            .prepare_cached(sql)?
            .query_row((sid, after, limit), |r| r.get(0))?;
        Ok(end.unwrap_or(after))
    }

    fn batch(
        self,
        conn: &Connection,
        sid: i64,
        after: i64,
        end: i64,
        mut bytes: Vec<u8>,
        mut comma: bool,
    ) -> Result<(Vec<u8>, i64)> {
        let sql = match self {
            Self::ModelInput => "SELECT c.position,i.payload,i.kind FROM context_items c JOIN items i ON i.id=c.item_id
                       WHERE c.session_id=? AND c.position>? AND c.position<=? ORDER BY c.position LIMIT ?",
            Self::Context => "SELECT c.position,json_object('position',c.position,'item_id',i.id,'llm_call_id',i.llm_call_id,'payload',json(i.payload))
                       FROM context_items c JOIN items i ON i.id=c.item_id
                       WHERE c.session_id=? AND c.position>? AND c.position<=? ORDER BY c.position LIMIT ?",
            Self::History => "SELECT id,json_object('id',id,'kind',kind,'payload',json(payload),'target_id',target_id,'llm_call_id',llm_call_id)
                       FROM items WHERE session_id=? AND id>? AND id<=? ORDER BY id LIMIT ?",
        };
        let mut stmt = conn.prepare_cached(sql)?;
        let mut rows = stmt.query((sid, after, end, BATCH_ITEMS))?;
        let mut position = after;
        while let Some(row) = rows.next()? {
            let encoded;
            let raw = if matches!(self, Self::ModelInput)
                && matches!(
                    row.get_ref(2)?.as_str().ok(),
                    Some("user_message" | "function_call_output")
                ) {
                encoded = crate::media::wire_message(conn, row.get(1)?)?.to_string();
                encoded.as_bytes()
            } else {
                row.get_ref(1)?
                    .as_bytes()
                    .map_err(|e| Error::internal(e.to_string()))?
            };
            if position != after && bytes.len() + raw.len() + 1 > BATCH_BYTES {
                break;
            }
            if comma {
                bytes.push(b',');
            }
            bytes.extend_from_slice(raw);
            comma = true;
            position = row.get(0)?;
            if bytes.len() >= BATCH_BYTES {
                break;
            }
        }
        if position == after && after < end {
            return Err(Error::internal("missing row in saved history"));
        }
        Ok((bytes, position))
    }

    pub async fn body(
        self,
        db: Db,
        sid: i64,
        after: i64,
        end: i64,
        prefix: Vec<u8>,
        suffix: &'static [u8],
    ) -> Result<Body> {
        let (mut bytes, mut position) = db
            .call(move |s| self.batch(&s.conn, sid, after, end, prefix, false))
            .await?;
        if position == end {
            bytes.extend_from_slice(suffix);
            return Ok(Body::from(bytes));
        }
        let stream = async_stream::stream! {
            yield Ok::<Bytes, Error>(Bytes::from(bytes));
            while position<end {
                let result = db.call(move |s| self.batch(&s.conn,sid,position,end,Vec::new(),true)).await;
                match result {
                    Ok((mut bytes,next)) => {
                        position=next;
                        if position==end {bytes.extend_from_slice(suffix);}
                        yield Ok(Bytes::from(bytes));
                    }
                    Err(err) => {yield Err(err); break;}
                }
            }
        };
        Ok(Body::wrap_stream(stream))
    }
}
