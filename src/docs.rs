use axum::{Json, Router, http::header, response::Html, routing::get};
use utoipa::openapi::{
    OpenApi,
    security::{Http, HttpAuthScheme, SecurityRequirement, SecurityScheme},
};

const PAGE: &str = r#"<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>MyGo-Bot API</title>
</head>
<body>
  <div id="app"></div>
  <script src="/docs/scalar.js"></script>
  <script>
    Scalar.createApiReference('#app', {
      url: '/openapi.json',
      servers: [{ url: window.location.origin }],
      withDefaultFonts: false,
      showDeveloperTools: 'never',
      showToolbar: 'never',
      agent: { disabled: true },
      telemetry: false,
    })
  </script>
</body>
</html>"#;

// The npm package's dist/browser/standalone.js with its MIT notice, gzip -9.
const SCALAR_JS: &[u8] = include_bytes!("../vendor/scalar-api-reference-1.82.0.js.gz");

pub fn routes(mut spec: OpenApi) -> Router {
    spec.components.get_or_insert_default().add_security_scheme(
        "bearerAuth",
        SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
    );
    spec.security = Some(vec![SecurityRequirement::new(
        "bearerAuth",
        [] as [&str; 0],
    )]);
    let spec = Json(spec);
    Router::new()
        .route("/docs", get(|| async { Html(PAGE) }))
        .route(
            "/docs/scalar.js",
            get(|| async {
                (
                    [
                        (header::CONTENT_TYPE, "text/javascript"),
                        (header::CONTENT_ENCODING, "gzip"),
                    ],
                    SCALAR_JS,
                )
            }),
        )
        .route("/openapi.json", get(move || async move { spec }))
}
