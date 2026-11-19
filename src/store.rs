use std::{collections::HashMap, fs::OpenOptions, path::Path, thread};

use axum::body::Bytes;
use reqwest::Body;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::{Error, Result, agent::Control, events::Notice};

// One owner serializes transactions and the matching in-memory worker transitions.
// A slow disk never blocks the HTTP executor. No job holds SQLite across network I/O.
type Command = Box<dyn FnOnce(&mut Store) + Send>;
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
            1 => (),
            _ => return Err(Error::internal("unsupported database version")),
        }
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
                while let Some(command) = receiver.blocking_recv() {
                    command(&mut store);
                }
            })?;
        Ok(Self(sender))
    }

    pub async fn call<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Store) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let (send, recv) = oneshot::channel();
        self.0
            .send(Box::new(move |store| {
                let _ = send.send(f(store));
            }))
            .await
            .map_err(|_| Error::internal("database worker stopped"))?;
        recv.await
            .map_err(|_| Error::internal("database worker stopped"))?
    }
}

impl Store {
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
            let mut stmt = self
                .conn
                .prepare_cached("SELECT id,kind,payload,target_id FROM items WHERE id=?")?;
            Notice::publish(&channel, "item.saved", stmt.query_row([id], history_row)?);
        }
        Ok(())
    }

    pub fn status(&mut self, sid: i64, state: &str, reason: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE sessions SET status=?2,last_error=?3 WHERE id=?1",
            (sid, state, reason),
        )?;
        if let Some(channel) = self.channels.get(&sid).and_then(|w| w.upgrade()) {
            Notice::publish(
                &channel,
                "session.status",
                json!({"status":state,"reason":reason}),
            );
        }
        Ok(())
    }
}

pub fn history_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Value> {
    Ok(json!({
        "id": row.get::<_, i64>(0)?, "kind": row.get::<_, String>(1)?,
        "payload": row.get::<_, Value>(2)?, "target_id": row.get::<_, Option<i64>>(3)?
    }))
}

pub fn session(conn: &Connection, sid: i64) -> Result<Value> {
    Ok(conn.query_row(
        "SELECT id,status,reasoning_effort,last_error FROM sessions WHERE id=?",
        [sid],
        session_row,
    )?)
}

pub fn session_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Value> {
    Ok(
        json!({"id":row.get::<_,i64>(0)?,"status":row.get::<_,String>(1)?,"reasoning_effort":row.get::<_,Option<String>>(2)?,"last_error":row.get::<_,String>(3)?}),
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

pub fn take_pending(tx: &Transaction<'_>, sid: i64) -> Result<()> {
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
) -> Result<i64> {
    conn.execute(
        "INSERT INTO items(session_id,kind,payload,target_id) VALUES(?,?,?,?)",
        params![sid, kind, payload, target],
    )?;
    Ok(conn.last_insert_rowid())
}

pub fn add_context_item(tx: &Transaction<'_>, sid: i64, kind: &str, item: &Value) -> Result<i64> {
    let id = add_item(tx, sid, kind, item, None)?;
    tx.execute(
        "INSERT INTO context_items(session_id,item_id) VALUES(?,?)",
        (sid, id),
    )?;
    Ok(id)
}

// Publish all completed tools in call order. A missing result after interruption
// is recorded explicitly: replaying an already executed tool could repeat its effect.
pub fn publish_tools(tx: &Transaction<'_>, sid: i64, recovering: bool) -> Result<Vec<i64>> {
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
                let result = add_item(tx, sid, "function_call_output", &payload, Some(id))?;
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
            Self::ModelInput => "SELECT c.position,i.payload FROM context_items c JOIN items i ON i.id=c.item_id
                       WHERE c.session_id=? AND c.position>? AND c.position<=? ORDER BY c.position LIMIT ?",
            Self::Context => "SELECT c.position,json_object('position',c.position,'item_id',i.id,'payload',json(i.payload))
                       FROM context_items c JOIN items i ON i.id=c.item_id
                       WHERE c.session_id=? AND c.position>? AND c.position<=? ORDER BY c.position LIMIT ?",
            Self::History => "SELECT id,json_object('id',id,'kind',kind,'payload',json(payload),'target_id',target_id)
                       FROM items WHERE session_id=? AND id>? AND id<=? ORDER BY id LIMIT ?",
        };
        let mut stmt = conn.prepare_cached(sql)?;
        let mut rows = stmt.query((sid, after, end, BATCH_ITEMS))?;
        let mut position = after;
        while let Some(row) = rows.next()? {
            let raw = row
                .get_ref(1)?
                .as_bytes()
                .map_err(|e| Error::internal(e.to_string()))?;
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
