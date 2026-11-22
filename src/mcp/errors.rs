use super::Slot;
use regex_lite::Regex;
use rmcp::{
    model::ErrorData,
    service::{ClientInitializeError, ServiceError},
    transport::streamable_http_client::StreamableHttpError,
};
use std::{error::Error, sync::LazyLock};

type HttpError = StreamableHttpError<reqwest::Error>;

// Error bodies can contain headers, arguments or URLs. Only extract a concise structural hint.
static FIELD: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?ix)\b(missing|unknown|unexpected|invalid|required)\s+(?:a\s+)?(?:required\s+)?(?:argument|field|property|parameter)\b(?:\s+named)?\s*[:=]?\s*[`'"]?([a-z_][a-z_0-9.]{0,63})(?:[`'"]|\b)"#).unwrap()
});
static REQUIRED: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?i)[`'"]([a-z_][a-z_0-9.]{0,63})[`'"]\s+is\s+(?:a\s+required\s+property|required)"#,
    )
    .unwrap()
});

fn protocol(slot: &Slot, operation: &str, error: &ErrorData) -> String {
    let message = &error.message[..error.message.floor_char_boundary(1024)];
    let hint = if let Some(parts) = FIELD.captures(message)
        && slot.can_report_field(&parts[2])
    {
        format!("{} field '{}'", parts[1].to_ascii_lowercase(), &parts[2])
    } else if let Some(parts) = REQUIRED.captures(message)
        && slot.can_report_field(&parts[1])
    {
        format!("missing field '{}'", &parts[1])
    } else {
        match error.code.0 {
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
    format!("{operation}: MCP protocol error ({}): {hint}", error.code.0)
}
fn transport_reason(error: &(dyn Error + 'static)) -> Option<String> {
    let mut cause = Some(error);
    while let Some(err) = cause {
        if let Some(e) = err.downcast_ref::<HttpError>() {
            let reason = match e {
                HttpError::ResponseBodyTooLarge { max_size } => {
                    format!("response exceeds {max_size} bytes")
                }
                HttpError::HttpStatus(status) => format!("HTTP {}", status.status),
                HttpError::SessionExpired => "HTTP 404; session expired".into(),
                HttpError::UnexpectedContentType(_) => "unexpected response content type".into(),
                HttpError::AuthRequired(_) | HttpError::InsufficientScope(_) => {
                    "HTTP authentication or scope required".into()
                }
                _ => {
                    cause = err.source();
                    continue;
                }
            };
            return Some(reason);
        }
        if let Some(e) = err.downcast_ref::<reqwest::Error>() {
            if let Some(code) = e.status() {
                return Some(format!("HTTP {}", code.as_u16()));
            }
            if e.is_connect() {
                return Some("endpoint connection failed".into());
            }
            if e.is_timeout() {
                return Some("HTTP request timed out".into());
            }
        }
        cause = err.source();
    }
    None
}
pub(super) fn handshake(error: ClientInitializeError) -> String {
    match error {
        ClientInitializeError::LegacyFallbackFailed { fallback, .. } => handshake(*fallback),
        ClientInitializeError::JsonRpcError(data) => {
            format!("MCP handshake protocol error ({})", data.code.0)
        }
        ClientInitializeError::NoCompatibleProtocolVersion { .. } => {
            "MCP handshake failed: no common protocol version".into()
        }
        e if e.is_authorization_required() => {
            "MCP handshake failed: authentication required; check credentials or sign in".into()
        }
        ClientInitializeError::TransportError { error, .. } => format!(
            "MCP handshake failed: {}",
            transport_reason(&error)
                .unwrap_or_else(|| "transport failed; check the endpoint and credentials".into())
        ),
        _ => "MCP handshake failed: peer closed or returned an invalid handshake".into(),
    }
}
pub(super) fn request(error: ServiceError, slot: &Slot, operation: &str, sent: bool) -> String {
    if let ServiceError::McpError(data) = error {
        return protocol(slot, operation, &data);
    }
    if matches!(
        error,
        ServiceError::TransportClosed | ServiceError::TransportSend(_)
    ) {
        slot.retire();
    }
    if error.is_authorization_required() {
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
