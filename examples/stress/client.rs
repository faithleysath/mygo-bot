use crate::{Result, check};
use futures_util::TryStreamExt;
use reqwest::{Client, Method, RequestBuilder, header};
use serde_json::{Value, json};
use std::{io, net::SocketAddr, pin::Pin, time::Duration};
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader, Lines};
use tokio_util::io::StreamReader;

#[derive(Clone)]
pub struct Api {
    address: SocketAddr,
    client: Client,
    timeout: Duration,
}

impl Api {
    pub fn new(address: SocketAddr, token: &str, timeout: Duration) -> Result<Self> {
        let mut bearer = header::HeaderValue::from_str(&format!("Bearer {token}"))?;
        bearer.set_sensitive(true);
        let mut headers = header::HeaderMap::new();
        headers.insert(header::AUTHORIZATION, bearer);
        Ok(Self {
            address,
            client: Client::builder()
                .default_headers(headers)
                .no_proxy()
                .http1_only()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(timeout)
                .build()?,
            timeout,
        })
    }

    pub fn request(&self, method: Method, path: &str, body: Option<Value>) -> RequestBuilder {
        let req = self
            .client
            .request(method, format!("http://{}{path}", self.address))
            .timeout(self.timeout);
        match body {
            Some(body) => req
                .header("Content-Type", "application/json")
                .body(body.to_string()),
            None => req,
        }
    }

    pub async fn send(req: RequestBuilder, expected: &[u16]) -> Result<(u16, Value)> {
        let res = req.send().await?;
        let status = res.status().as_u16();
        let bytes = res.bytes().await?;
        if !expected.contains(&status) {
            return Err(format!(
                "HTTP {status}, expected {expected:?}: {}",
                String::from_utf8_lossy(&bytes[..bytes.len().min(240)])
            )
            .into());
        }
        Ok((
            status,
            if bytes.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&bytes)?
            },
        ))
    }

    pub async fn json(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        status: u16,
    ) -> Result<Value> {
        Ok(Self::send(self.request(method, path, body), &[status])
            .await?
            .1)
    }

    pub async fn get(&self, path: &str) -> Result<Value> {
        self.json(Method::GET, path, None, 200).await
    }

    pub async fn session(&self) -> Result<String> {
        let data = self.json(Method::POST, "/sessions", None, 201).await?;
        let id = data["id"].as_u64().ok_or("missing session id")?;
        Ok(format!("/sessions/{id}"))
    }

    pub async fn enqueue(&self, path: &str, text: &str) -> Result<u64> {
        let msg = self
            .json(
                Method::POST,
                &format!("{path}/messages"),
                Some(json!({"text":text})),
                201,
            )
            .await?;
        Ok(msg["id"].as_u64().ok_or("missing message id")?)
    }

    pub async fn start(&self, path: &str) -> Result<()> {
        self.json(Method::PUT, &format!("{path}/run"), None, 200)
            .await?;
        Ok(())
    }

    pub async fn listen(&self, path: &str) -> Result<Events> {
        tokio::time::timeout(self.timeout, self.open_events(path)).await?
    }

    async fn open_events(&self, path: &str) -> Result<Events> {
        let res = self
            .client
            .get(format!("http://{}{path}/events", self.address))
            .send()
            .await?
            .error_for_status()?;
        let stream = res.bytes_stream().map_err(io::Error::other);
        let reader: Pin<Box<dyn AsyncRead + Send>> = Box::pin(StreamReader::new(stream));
        let mut events = Events(BufReader::new(reader).lines());
        let (name, initial) = events.next().await?;
        check(
            name == "ready" && initial["status"].is_string(),
            "missing SSE ready event",
        )?;
        Ok(events)
    }
}

pub struct Events(Lines<BufReader<Pin<Box<dyn AsyncRead + Send>>>>);

#[derive(Default)]
pub struct Notices {
    pub answers: usize,
    pub retries: usize,
    pub tools: usize,
    pub outputs: usize,
    pub reason: String,
}

impl Events {
    pub async fn next(&mut self) -> Result<(String, Value)> {
        let mut name = String::new();
        let mut data = String::new();
        while let Some(line) = self.0.next_line().await? {
            if line.is_empty() && !name.is_empty() {
                return Ok((name, serde_json::from_str(&data)?));
            }
            if let Some(value) = line.strip_prefix("event:") {
                name = value.trim().to_owned();
            }
            if let Some(value) = line.strip_prefix("data:") {
                data.push_str(value.trim_start());
                data.push('\n');
            }
        }
        Err("SSE ended before expected event".into())
    }

    pub async fn terminal(&mut self, expected: &str) -> Result<Notices> {
        let mut seen = Notices::default();
        loop {
            let (event, data) = self.next().await?;
            match event.as_str() {
                "attempt.retry" => seen.retries += 1,
                "resync" => return Err("subscriber missed events".into()),
                "item.saved" => match data["kind"].as_str() {
                    Some("assistant_message") => {
                        check(
                            data["payload"]["content"][0]["text"] == "ok",
                            "wrong assistant answer",
                        )?;
                        seen.answers += 1;
                    }
                    Some("function_call") => seen.tools += 1,
                    Some("function_call_output") => seen.outputs += 1,
                    _ => (),
                },
                "session.status" if data["status"] == "idle" || data["status"] == "paused" => {
                    check(
                        data["status"] == expected,
                        format!("unexpected terminal state: {data}"),
                    )?;
                    seen.reason = data["reason"].as_str().unwrap_or_default().to_owned();
                    return Ok(seen);
                }
                _ => (),
            }
        }
    }
}
