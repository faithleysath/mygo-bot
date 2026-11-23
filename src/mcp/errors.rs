use super::{Slot, client::McpError};

const KEYWORDS: &[&str] = &["missing", "unknown", "unexpected", "invalid", "required"];
const NOUNS: &[&str] = &["argument", "field", "property", "parameter"];

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn starts_word_boundary(s: &str, idx: usize) -> bool {
    idx == 0 || !is_word_byte(s.as_bytes()[idx - 1])
}

fn match_ci_word<'a>(s: &'a str, words: &[&'static str]) -> Option<(&'static str, &'a str)> {
    for &word in words {
        if s.len() >= word.len()
            && s.as_bytes()[..word.len()].eq_ignore_ascii_case(word.as_bytes())
            && !s.as_bytes().get(word.len()).is_some_and(|&b| is_word_byte(b))
        {
            return Some((word, &s[word.len()..]));
        }
    }
    None
}

fn skip_ws(s: &str) -> Option<&str> {
    let trimmed = s.trim_start();
    (trimmed.len() < s.len()).then_some(trimmed)
}

fn skip_ci_token<'a>(s: &'a str, tok: &str) -> Option<&'a str> {
    if s.len() >= tok.len() && s.as_bytes()[..tok.len()].eq_ignore_ascii_case(tok.as_bytes()) {
        skip_ws(&s[tok.len()..])
    } else {
        None
    }
}

fn capture_ident(tail: &str) -> Option<&str> {
    let bytes = tail.as_bytes();
    let &first = bytes.first()?;
    if !(first.is_ascii_alphabetic() || first == b'_') {
        return None;
    }
    let max = bytes.len().min(64);
    let mut end = 1;
    while end < max && (is_word_byte(bytes[end]) || bytes[end] == b'.') {
        end += 1;
    }
    while end > 0 {
        let followed_by_quote = matches!(bytes.get(end), Some(b'`' | b'\'' | b'"'));
        let at_word_boundary = is_word_byte(bytes[end - 1])
            && !bytes.get(end).is_some_and(|&b| is_word_byte(b));
        if followed_by_quote || at_word_boundary {
            return Some(&tail[..end]);
        }
        end -= 1;
    }
    None
}

fn finish_field(after: &str) -> Option<&str> {
    let mut t = after.trim_start();
    if let Some(next) = t.strip_prefix([':', '=']) {
        t = next.trim_start();
    }
    if let Some(next) = t.strip_prefix(['`', '\'', '"']) {
        t = next;
    }
    capture_ident(t)
}

fn try_field_at(s: &str) -> Option<(&'static str, &str)> {
    let (keyword, mut rest) = match_ci_word(s, KEYWORDS)?;
    rest = skip_ws(rest)?;
    if let Some(after_a) = skip_ci_token(rest, "a") {
        rest = after_a;
    }
    if let Some(after_req) = skip_ci_token(rest, "required") {
        rest = after_req;
    }
    let (_, after_noun) = match_ci_word(rest, NOUNS)?;

    if let Some(after_ws) = skip_ws(after_noun)
        && after_ws.len() >= 5
        && after_ws.as_bytes()[..5].eq_ignore_ascii_case(b"named")
        && !after_ws.as_bytes().get(5).is_some_and(|&b| is_word_byte(b))
        && let Some(ident) = finish_field(&after_ws[5..])
    {
        return Some((keyword, ident));
    }
    finish_field(after_noun).map(|ident| (keyword, ident))
}

fn try_required_at(s: &str) -> Option<&str> {
    let after_quote = s.strip_prefix(['`', '\'', '"'])?;
    let bytes = after_quote.as_bytes();
    let &first = bytes.first()?;
    if !(first.is_ascii_alphabetic() || first == b'_') {
        return None;
    }
    let max = bytes.len().min(64);
    let mut len = 1;
    while len < max && (is_word_byte(bytes[len]) || bytes[len] == b'.') {
        len += 1;
    }
    // Check quote right after the identifier (or backtrack if a trailing quote exists inside).
    while len > 0 {
        if matches!(bytes.get(len), Some(b'`' | b'\'' | b'"')) {
            let mut rest = skip_ws(&after_quote[len + 1..])?;
            rest = skip_ci_token(rest, "is")?;
            let after_req = if let Some(after_a) = skip_ci_token(rest, "a")
                && let Some(after_r) = skip_ci_token(after_a, "required")
                && after_r.len() >= 8
                && after_r.as_bytes()[..8].eq_ignore_ascii_case(b"property")
            {
                Some(())
            } else {
                (rest.len() >= 8 && rest.as_bytes()[..8].eq_ignore_ascii_case(b"required"))
                    .then_some(())
            };
            if after_req.is_some() {
                return Some(&after_quote[..len]);
            }
        }
        len -= 1;
    }
    None
}

// Error bodies can contain headers, arguments or URLs. Only extract a concise structural hint.
fn extract_field_hint(message: &str) -> Option<(&'static str, &str)> {
    message
        .char_indices()
        .filter(|&(idx, ch)| ch.is_ascii_alphabetic() && starts_word_boundary(message, idx))
        .find_map(|(idx, _)| try_field_at(&message[idx..]))
}

fn extract_required_hint(message: &str) -> Option<&str> {
    message
        .char_indices()
        .filter(|&(_, ch)| matches!(ch, '`' | '\'' | '"'))
        .find_map(|(idx, _)| try_required_at(&message[idx..]))
}

fn protocol(slot: &Slot, operation: &str, code: i64, message: &str) -> String {
    let message = &message[..message.floor_char_boundary(1024)];
    let hint = if let Some((kind, field)) = extract_field_hint(message)
        && slot.can_report_field(field)
    {
        format!("{kind} field '{field}'")
    } else if let Some(field) = extract_required_hint(message)
        && slot.can_report_field(field)
    {
        format!("missing field '{field}'")
    } else {
        match code {
            -32601 => "method or tool unavailable",
            -32602 => "invalid parameters; check the declared schema",
            -32603 => "remote internal error",
            -32600 => "invalid request",
            -32700 => "remote parse error",
            -32020 => "MCP request header mismatch; inspect the server's current tool schema",
            -32021 => "server requires an unsupported client capability",
            _ => "request rejected by the server",
        }
        .to_owned()
    };
    format!("{operation}: MCP protocol error ({code}): {hint}")
}

fn transport_reason(error: &McpError) -> Option<String> {
    match error {
        McpError::ResponseTooLarge(max_size) => Some(format!("response exceeds {max_size} bytes")),
        McpError::HttpStatus(status) => Some(format!("HTTP {status}")),
        McpError::SessionExpired => Some("HTTP 404; session expired".into()),
        McpError::UnexpectedContentType => Some("unexpected response content type".into()),
        McpError::AuthRequired => Some("HTTP authentication or scope required".into()),
        McpError::ConnectFailed => Some("endpoint connection failed".into()),
        McpError::TimedOut => Some("HTTP request timed out".into()),
        _ => None,
    }
}

pub(super) fn handshake(error: McpError) -> String {
    match &error {
        McpError::Protocol { code, .. } => format!("MCP handshake protocol error ({code})"),
        McpError::NoCompatibleProtocolVersion => {
            "MCP handshake failed: no common protocol version".into()
        }
        e if e.is_auth_required() => {
            "MCP handshake failed: authentication required; check credentials or sign in".into()
        }
        McpError::TransportClosed | McpError::InvalidResponse => {
            "MCP handshake failed: peer closed or returned an invalid handshake".into()
        }
        other => format!(
            "MCP handshake failed: {}",
            transport_reason(other)
                .unwrap_or_else(|| "transport failed; check the endpoint and credentials".into())
        ),
    }
}

pub(super) fn request(error: McpError, slot: &Slot, operation: &str, sent: bool) -> String {
    if let McpError::Protocol { code, message, .. } = error {
        return protocol(slot, operation, code, &message);
    }
    if error.should_retire() {
        slot.retire();
    }
    if error.is_auth_required() {
        return format!("{operation}: authentication required; check credentials or sign in");
    }
    let reason = transport_reason(&error)
        .unwrap_or_else(|| "connection interrupted or response unreadable".into());
    let outcome = if sent {
        "; result unknown; request was not replayed"
    } else {
        ""
    };
    format!("{operation}: {reason}{outcome}")
}

