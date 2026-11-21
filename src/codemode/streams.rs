use super::{HostError, Reply};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::{
    fs::{File, ReadDir},
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore, SemaphorePermit, mpsc},
};
use tokio_util::sync::CancellationToken;

pub(super) const CHUNK: usize = 64 * 1024;
const PER_TASK: usize = 16;
static HANDLES: Semaphore = Semaphore::const_new(128);

pub(super) enum Resource {
    Reader(Pin<Box<dyn AsyncRead + Send>>),
    Directory(ReadDir),
    Writer(File),
    Upload {
        sender: mpsc::Sender<Option<Vec<u8>>>,
        receiver: Option<mpsc::Receiver<Option<Vec<u8>>>>,
    },
}

struct Entry {
    resource: AsyncMutex<Resource>,
    closed: CancellationToken,
    _permits: (OwnedSemaphorePermit, SemaphorePermit<'static>),
}

#[derive(Default)]
struct Table {
    last_id: u32,
    entries: HashMap<u32, Arc<Entry>>,
}

pub(super) struct Streams {
    table: Mutex<Table>,
    slots: Arc<Semaphore>,
}

impl Default for Streams {
    fn default() -> Self {
        Self {
            table: Mutex::default(),
            slots: Arc::new(Semaphore::new(PER_TASK)),
        }
    }
}

pub(super) struct Reservation<'a> {
    streams: &'a Streams,
    id: u32,
    permits: (OwnedSemaphorePermit, SemaphorePermit<'static>),
}

impl Reservation<'_> {
    pub fn insert(self, resource: Resource) -> u64 {
        let entry = Entry {
            resource: AsyncMutex::new(resource),
            closed: CancellationToken::new(),
            _permits: self.permits,
        };
        self.streams
            .table
            .lock()
            .unwrap()
            .entries
            .insert(self.id, Arc::new(entry));
        self.id.into()
    }
}

impl Streams {
    // Reserve before opening files or issuing HTTP requests with side effects.
    pub fn reserve(&self) -> Result<Reservation<'_>, HostError> {
        let local = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| "task stream handle limit reached")?;
        let global = HANDLES
            .try_acquire()
            .map_err(|_| "global stream handle limit reached")?;
        let mut table = self.table.lock().unwrap();
        let id = table
            .last_id
            .checked_add(1)
            .ok_or("stream ID space exhausted")?;
        table.last_id = id;
        Ok(Reservation {
            streams: self,
            id,
            permits: (local, global),
        })
    }

    fn get(&self, id: &Value) -> Result<Arc<Entry>, HostError> {
        let id = id
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .ok_or("invalid stream ID")?;
        self.table
            .lock()
            .unwrap()
            .entries
            .get(&id)
            .cloned()
            .ok_or_else(|| "stream closed".into())
    }

    pub fn close(&self, id: u32) {
        if let Some(entry) = self.table.lock().unwrap().entries.remove(&id) {
            entry.closed.cancel();
        }
    }

    pub async fn execute(
        &self,
        op: &str,
        id: &Value,
        data: Option<Vec<u8>>,
    ) -> Result<Reply, HostError> {
        if op == "upload.open" {
            let (sender, receiver) = mpsc::channel(1);
            let id = self.reserve()?.insert(Resource::Upload {
                sender,
                receiver: Some(receiver),
            });
            return Ok(Reply::value(json!(id)));
        }
        let entry = self.get(id)?;
        let operation = async {
            let mut resource = entry.resource.lock().await;
            let value = match (op, &mut *resource) {
                ("stream.next", Resource::Reader(reader)) => {
                    let mut chunk = vec![0; CHUNK];
                    let n = reader.read(&mut chunk).await?;
                    if n != 0 {
                        chunk.truncate(n);
                        return Ok(Reply {
                            value: Value::Null,
                            data: Some(chunk),
                        });
                    }
                    Value::Null
                }
                ("stream.next", Resource::Directory(dir)) => match dir.next_entry().await? {
                    Some(entry) => {
                        let name = entry.file_name().into_string().map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "directory entry name is not UTF-8",
                            )
                        })?;
                        json!({"name":name, "type":super::files::file_type(entry.file_type().await?)})
                    }
                    None => Value::Null,
                },
                ("stream.write", Resource::Writer(file)) => {
                    let bytes = chunk(data)?;
                    file.write_all(&bytes).await?;
                    Value::Null
                }
                ("stream.finish", Resource::Writer(file)) => {
                    file.flush().await?;
                    Value::Null
                }
                ("stream.write" | "stream.finish", Resource::Upload { sender, .. }) => {
                    let part = if op == "stream.write" {
                        Some(chunk(data)?)
                    } else {
                        None
                    };
                    let sender = sender.clone();
                    drop(resource);
                    // An early HTTP response can close the body before its producer is done.
                    json!(sender.send(part).await.is_ok())
                }
                _ => return Err("invalid operation for stream".into()),
            };
            Ok(Reply::value(value))
        };
        tokio::select! {
            biased;
            _ = entry.closed.cancelled() => Err("stream closed".into()),
            result = operation => result,
        }
    }

    pub async fn upload(
        &self,
        id: &Value,
        cancel: CancellationToken,
    ) -> Result<(reqwest::Body, CancellationToken), HostError> {
        let entry = self.get(id)?;
        let mut receiver = match &mut *entry.resource.lock().await {
            Resource::Upload { receiver, .. } => {
                receiver.take().ok_or("upload already consumed")?
            }
            _ => return Err("expected an upload stream".into()),
        };
        let closed = entry.closed.clone();
        let body = async_stream::stream! {
            loop {
                let part = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => None,
                    _ = entry.closed.cancelled() => None,
                    part = receiver.recv() => part,
                };
                match part {
                    Some(Some(bytes)) => yield Ok(bytes),
                    Some(None) => break,
                    None => {
                        yield Err(io::Error::new(io::ErrorKind::Interrupted, "upload stopped"));
                        break;
                    }
                }
            }
        };
        Ok((reqwest::Body::wrap_stream(body), closed))
    }
}

fn chunk(data: Option<Vec<u8>>) -> Result<Vec<u8>, HostError> {
    data.filter(|bytes| bytes.len() <= CHUNK)
        .ok_or_else(|| "expected a stream chunk up to 64 KiB".into())
}
