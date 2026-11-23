use axum::{Router, http::header, response::Html, routing::get};

const OPENAPI: &[u8] = include_bytes!("openapi.json");

const PAGE: &str = r#"<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>MyGo-Bot API</title>
</head>
<body>
  <p id="loading"><span role="status">正在加载接口文档…</span> <a href="/openapi.json">查看 OpenAPI JSON</a></p>
  <div id="app"></div>
  <script type="module">
    const version = '1.82.0';
    const file = 'dist/browser/standalone.js';
    const primary = `https://cdn.jsdelivr.net/npm/@scalar/api-reference@${version}/${file}`;
    const backup = `https://registry.npmmirror.com/@scalar/api-reference/${version}/files/${file}`;
    const integrity = 'sha384-S7zqZOKmB//ONrjKcth/ItE41a3xEDWW4dqHu2nQ/Ggxt06ROPonIfJ03dSBGuXs';

    async function download(url, timeout) {
      const response = await fetch(url, {
        integrity,
        credentials: 'omit',
        signal: AbortSignal.timeout(timeout),
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return response.text();
    }

    try {
      const source = await download(primary, 8000).catch(() => download(backup, 20000));
      const script = document.createElement('script');
      script.textContent = source;
      document.head.append(script);
      script.remove();
      window.Scalar.createApiReference('#app', {
        url: '/openapi.json',
        servers: [{ url: window.location.origin }],
        withDefaultFonts: false,
        showDeveloperTools: 'never',
        showToolbar: 'never',
        agent: { disabled: true },
        telemetry: false,
      });
      document.getElementById('loading').remove();
    } catch {
      document.querySelector('#loading span').textContent = '接口文档页面加载失败，请刷新重试。';
    }
  </script>
</body>
</html>"#;

pub fn routes() -> Router {
    Router::new()
        .route("/docs", get(|| async { Html(PAGE) }))
        .route(
            "/openapi.json",
            get(|| async { ([(header::CONTENT_TYPE, "application/json")], OPENAPI) }),
        )
}
