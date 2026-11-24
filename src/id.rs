use std::{
    fmt,
    hash::{Hash, Hasher},
    marker::PhantomData,
    str::FromStr,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use rusqlite::{
    Connection, ToSql,
    types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef},
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::{Error, Result};

const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

const DECODE: [u8; 256] = {
    let mut table = [0xffu8; 256];
    let mut i = 0;
    while i < 32 {
        table[ALPHABET[i] as usize] = i as u8;
        i += 1;
    }
    table
};

struct ClockState {
    last_ms: u64,
    seq: u16,
}

static CLOCK: Mutex<ClockState> = Mutex::new(ClockState { last_ms: 0, seq: 0 });

pub trait Prefix: 'static {
    const PREFIX: &'static [u8; 3];
    const LABEL: &'static str;
}

pub enum Provider {}
impl Prefix for Provider {
    const PREFIX: &'static [u8; 3] = b"prv";
    const LABEL: &'static str = "prv";
}

pub enum Model {}
impl Prefix for Model {
    const PREFIX: &'static [u8; 3] = b"mdl";
    const LABEL: &'static str = "mdl";
}

pub enum McpServer {}
impl Prefix for McpServer {
    const PREFIX: &'static [u8; 3] = b"mcp";
    const LABEL: &'static str = "mcp";
}

pub enum Session {}
impl Prefix for Session {
    const PREFIX: &'static [u8; 3] = b"ses";
    const LABEL: &'static str = "ses";
}

pub enum Call {}
impl Prefix for Call {
    const PREFIX: &'static [u8; 3] = b"cal";
    const LABEL: &'static str = "cal";
}

pub enum Attachment {}
impl Prefix for Attachment {
    const PREFIX: &'static [u8; 3] = b"att";
    const LABEL: &'static str = "att";
}

pub enum Item {}
impl Prefix for Item {
    const PREFIX: &'static [u8; 3] = b"itm";
    const LABEL: &'static str = "itm";
}

pub type ProviderId = Id<Provider>;
pub type ModelId = Id<Model>;
pub type McpServerId = Id<McpServer>;
pub type SessionId = Id<Session>;
pub type CallId = Id<Call>;
pub type AttachmentId = Id<Attachment>;
pub type ItemId = Id<Item>;

pub struct Id<K: Prefix> {
    bytes: [u8; 30],
    _kind: PhantomData<fn() -> K>,
}

impl<K: Prefix> Clone for Id<K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K: Prefix> Copy for Id<K> {}

impl<K: Prefix> PartialEq for Id<K> {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl<K: Prefix> Eq for Id<K> {}

impl<K: Prefix> PartialOrd for Id<K> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: Prefix> Ord for Id<K> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.bytes.cmp(&other.bytes)
    }
}

impl<K: Prefix> Hash for Id<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.bytes.hash(state);
    }
}

fn next_uuidv7() -> Result<[u8; 16]> {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let (ts_ms, seq) = {
        let mut state = CLOCK.lock().unwrap();
        if now_ms > state.last_ms {
            state.last_ms = now_ms;
            state.seq = 0;
        } else if state.seq < 0x0fff {
            state.seq += 1;
        } else {
            state.last_ms = state.last_ms.saturating_add(1);
            state.seq = 0;
        }
        (state.last_ms, state.seq)
    };
    let mut rand = [0u8; 8];
    getrandom::fill(&mut rand).map_err(|_| Error::internal("entropy unavailable"))?;

    let mut raw = [0u8; 16];
    let ts_bytes = ts_ms.to_be_bytes();
    raw[0..6].copy_from_slice(&ts_bytes[2..8]);
    raw[6] = 0x70 | ((seq >> 8) as u8 & 0x0f);
    raw[7] = (seq & 0xff) as u8;
    raw[8] = 0x80 | (rand[0] & 0x3f);
    raw[9..16].copy_from_slice(&rand[1..8]);
    Ok(raw)
}

fn encode_base32(raw: [u8; 16], out: &mut [u8]) {
    let v = u128::from_be_bytes(raw);
    for (i, slot) in out.iter_mut().take(26).enumerate() {
        *slot = ALPHABET[((v >> ((25 - i) * 5)) & 0x1f) as usize];
    }
}

fn valid_suffix(suffix: &[u8]) -> bool {
    suffix.len() == 26
        && DECODE[suffix[0] as usize] < 8
        && suffix[1..].iter().all(|&b| DECODE[b as usize] < 32)
}

fn decode_base32(suffix: &[u8]) -> Option<[u8; 16]> {
    if !valid_suffix(suffix) {
        return None;
    }
    let mut v = 0u128;
    for &b in suffix {
        v = (v << 5) | u128::from(DECODE[b as usize]);
    }
    Some(v.to_be_bytes())
}

fn observe_id_str(id: &str) -> Result<()> {
    let b = id.as_bytes();
    let Some(raw) = (b.len() == 30 && b[3] == b'_')
        .then(|| decode_base32(&b[4..]))
        .flatten()
    else {
        return Err(Error::internal(format!("invalid TypeID in database: {id}")));
    };
    let mut ts_bytes = [0u8; 8];
    ts_bytes[2..8].copy_from_slice(&raw[0..6]);
    let ts_ms = u64::from_be_bytes(ts_bytes);
    let seq = match raw[6] >> 4 {
        7 => (u16::from(raw[6] & 0x0f) << 8) | u16::from(raw[7]),
        8..=15 => 0x0fff,
        _ => 0,
    };
    let mut state = CLOCK.lock().unwrap();
    if (ts_ms, seq) > (state.last_ms, state.seq) {
        state.last_ms = ts_ms;
        state.seq = seq;
    }
    Ok(())
}

/// Advances the monotonic UUIDv7 generator past all TypeIDs and cursors already committed in SQLite.
pub fn seed_from_db(conn: &Connection) -> Result<()> {
    const QUERIES: [&str; 8] = [
        "SELECT max(id) FROM llm_providers",
        "SELECT max(id) FROM llm_models",
        "SELECT max(id) FROM mcp_servers",
        "SELECT max(id) FROM sessions",
        "SELECT max(input_queue_cursor) FROM sessions WHERE input_queue_cursor != ''",
        "SELECT max(id) FROM llm_calls",
        "SELECT max(id) FROM attachments",
        "SELECT max(id) FROM items",
    ];
    for sql in QUERIES {
        let max_id: Option<String> = conn.query_row(sql, [], |r| r.get(0))?;
        if let Some(id) = max_id {
            observe_id_str(&id)?;
        }
    }
    Ok(())
}

impl<K: Prefix> Id<K> {
    pub fn new() -> Result<Self> {
        let raw = next_uuidv7()?;
        let mut bytes = [0u8; 30];
        bytes[..3].copy_from_slice(K::PREFIX);
        bytes[3] = b'_';
        encode_base32(raw, &mut bytes[4..]);
        Ok(Self {
            bytes,
            _kind: PhantomData,
        })
    }

    pub fn as_str(&self) -> &str {
        // SAFETY: bytes only contains ASCII prefix, '_', and ALPHABET characters.
        unsafe { std::str::from_utf8_unchecked(&self.bytes) }
    }

    pub fn suffix(&self) -> &str {
        &self.as_str()[4..]
    }
}

impl ItemId {
    pub fn task_id(self) -> String {
        format!("cm_{}", self.suffix())
    }

    pub fn from_task_id(s: &str) -> std::result::Result<Self, String> {
        let b = s.as_bytes();
        if b.len() != 29 || &b[..3] != b"cm_" || !valid_suffix(&b[3..]) {
            return Err("expected a cm_ task ID".to_owned());
        }
        let mut bytes = [0u8; 30];
        bytes[..4].copy_from_slice(b"itm_");
        bytes[4..].copy_from_slice(&b[3..]);
        Ok(Self {
            bytes,
            _kind: PhantomData,
        })
    }
}

impl<K: Prefix> fmt::Display for Id<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<K: Prefix> fmt::Debug for Id<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<K: Prefix> FromStr for Id<K> {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let b = s.as_bytes();
        if b.len() != 30 || &b[..3] != K::PREFIX || b[3] != b'_' || !valid_suffix(&b[4..]) {
            return Err(format!("expected a valid {}_ ID", K::LABEL));
        }
        let mut bytes = [0u8; 30];
        bytes.copy_from_slice(b);
        Ok(Self {
            bytes,
            _kind: PhantomData,
        })
    }
}

impl<K: Prefix> Serialize for Id<K> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de, K: Prefix> Deserialize<'de> for Id<K> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct IdVisitor<K>(PhantomData<fn() -> K>);

        impl<K: Prefix> de::Visitor<'_> for IdVisitor<K> {
            type Value = Id<K>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "a {}_ ID string", K::LABEL)
            }

            fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<Self::Value, E> {
                v.parse().map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_str(IdVisitor(PhantomData))
    }
}

impl<K: Prefix> ToSql for Id<K> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(ValueRef::Text(&self.bytes)))
    }
}

impl<K: Prefix> FromSql for Id<K> {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        value
            .as_str()?
            .parse::<Self>()
            .map_err(|err| FromSqlError::Other(err.into()))
    }
}
