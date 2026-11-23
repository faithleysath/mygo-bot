use axum::{
    Router,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};

static INDEX_HTML: &[u8] = include_bytes!("webui/index.html");
static APP_JS_GZ: &[u8] = include_bytes!("webui/app.js.gz");
static APP_CSS_GZ: &[u8] = include_bytes!("webui/app.css.gz");

fn etag_for(bytes: &[u8]) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("\"{hash:016x}\"")
}

fn serve_asset(
    headers: &HeaderMap,
    content_type: &'static str,
    gzipped: bool,
    body: &'static [u8],
) -> Response {
    let tag = etag_for(body);
    if headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.split(',').any(|part| part.trim() == tag))
    {
        return (
            StatusCode::NOT_MODIFIED,
            [
                (header::ETAG, HeaderValue::from_str(&tag).unwrap()),
                (header::CACHE_CONTROL, HeaderValue::from_static("no-cache")),
            ],
        )
            .into_response();
    }
    let mut resp = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::ETAG, tag);
    if gzipped {
        resp = resp
            .header(header::CONTENT_ENCODING, "gzip")
            .header(header::VARY, "Accept-Encoding");
    }
    resp.body(axum::body::Body::from(body)).unwrap()
}

pub fn routes() -> Router {
    Router::new()
        .route(
            "/",
            get(|headers: HeaderMap| async move {
                serve_asset(&headers, "text/html; charset=utf-8", false, INDEX_HTML)
            }),
        )
        .route(
            "/ui/app.js",
            get(|headers: HeaderMap| async move {
                serve_asset(
                    &headers,
                    "application/javascript; charset=utf-8",
                    true,
                    APP_JS_GZ,
                )
            }),
        )
        .route(
            "/ui/app.css",
            get(|headers: HeaderMap| async move {
                serve_asset(&headers, "text/css; charset=utf-8", true, APP_CSS_GZ)
            }),
        )
}
