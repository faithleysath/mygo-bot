use chrono::{Local, SecondsFormat};
use serde_json::{Map, Value, json};

const CURRENT_TIME: &str = "current_time";
const INVALID_CALL: &str = "Tool error: expected current_time with empty arguments {}.";

pub fn definitions() -> Value {
    json!([{
        "type": "function",
        "name": CURRENT_TIME,
        "description": "Read the current local time, including the UTC offset.",
        "parameters": {"type":"object", "properties":{}, "required":[], "additionalProperties":false}
    }])
}

// Handlers run on the HTTP executor. Await I/O here so dropping the future stops local work.
pub async fn execute(name: &str, arguments: &str) -> String {
    match name {
        CURRENT_TIME => current_time(arguments).await,
        _ => INVALID_CALL.to_owned(),
    }
}

async fn current_time(arguments: &str) -> String {
    if serde_json::from_str::<Map<String, Value>>(arguments).is_ok_and(|args| args.is_empty()) {
        Local::now().to_rfc3339_opts(SecondsFormat::Secs, false)
    } else {
        INVALID_CALL.to_owned()
    }
}
