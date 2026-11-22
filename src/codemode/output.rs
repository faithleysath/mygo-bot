use tokio::sync::Semaphore;

const MIB: usize = 1024 * 1024;
pub(super) const TASK_BYTES: usize = 52 * MIB;
const GLOBAL_BYTES: usize = 192 * MIB;
static OUTPUT_BUDGET: Semaphore = Semaphore::const_new(GLOBAL_BYTES);

pub enum Part {
    Text(String),
    Attachment {
        kind: String,
        id: i64,
    },
    Bytes {
        kind: String,
        mime: String,
        data: Vec<u8>,
    },
}

#[derive(Default)]
pub struct Blocks {
    pub parts: Vec<Part>,
    reserved: usize,
}

impl Drop for Blocks {
    fn drop(&mut self) {
        OUTPUT_BUDGET.add_permits(self.reserved);
    }
}

impl Blocks {
    fn reserve(&mut self, bytes: usize) -> Result<(), String> {
        let bytes = bytes.saturating_add(128);
        // The VM checks the script's block count before appending its console output.
        if bytes > TASK_BYTES - self.reserved {
            return Err("task output budget exceeded".into());
        }
        OUTPUT_BUDGET
            .try_acquire_many(bytes as u32)
            .map_err(|_| "global output budget exceeded".to_owned())?
            .forget();
        self.reserved += bytes;
        Ok(())
    }
    pub fn push_text(&mut self, text: String) -> Result<(), String> {
        if text.trim().is_empty() {
            return Err("expected nonempty text".into());
        }
        self.reserve(text.len())?;
        self.parts.push(Part::Text(text));
        Ok(())
    }
    pub fn append_console(&mut self, logs: String) -> Result<(), String> {
        let prefix = if self.parts.is_empty() {
            ""
        } else {
            "Console output:\n"
        };
        self.reserve(prefix.len() + logs.len())
            .map_err(|e| format!("{e}\n\nConsole output:\n{logs}"))?;
        let text = if prefix.is_empty() {
            logs
        } else {
            format!("{prefix}{logs}")
        };
        self.parts.push(Part::Text(text));
        Ok(())
    }
    pub fn push_attachment(&mut self, kind: String, id: i64) -> Result<(), String> {
        if !matches!(kind.as_str(), "image" | "audio" | "video") || id <= 0 {
            return Err("invalid attachment reference".into());
        }
        self.reserve(kind.len())?;
        self.parts.push(Part::Attachment { kind, id });
        Ok(())
    }
    pub fn push_bytes(&mut self, kind: String, mime: String, bytes: &[u8]) -> Result<(), String> {
        if bytes.is_empty()
            || bytes.len() > 50 * MIB
            || !matches!(kind.as_str(), "image" | "audio" | "video")
            || !mime
                .parse::<mime::Mime>()
                .is_ok_and(|m| m.type_().as_str() == kind && !m.essence_str().contains('*'))
        {
            return Err(
                "invalid media bytes, MIME type or attachment size (maximum 50 MiB)".into(),
            );
        }
        self.reserve(bytes.len() + kind.len() + mime.len())?;
        self.parts.push(Part::Bytes {
            kind,
            mime,
            data: bytes.to_vec(),
        });
        Ok(())
    }
}
