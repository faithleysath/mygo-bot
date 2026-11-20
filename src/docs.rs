use axum::{Json, Router, response::Html, routing::get};
use utoipa::openapi::{
    OpenApi,
    security::{Http, HttpAuthScheme, SecurityRequirement, SecurityScheme},
};

/// 部分更新沿用完整对象的字段类型与可空性，每个字段均可省略。
/// 处理函数在数据库事务里把传入字段合并到当前值后校验。
pub struct Patch<T>(std::marker::PhantomData<T>);

type Schema = utoipa::openapi::RefOr<utoipa::openapi::Schema>;

impl<T: utoipa::ToSchema> utoipa::PartialSchema for Patch<T> {
    fn schema() -> Schema {
        let mut schema = T::schema();
        if let Schema::T(utoipa::openapi::Schema::Object(object)) = &mut schema {
            object.required.clear();
            object.min_properties = Some(1);
        }
        schema
    }
}

impl<T: utoipa::ToSchema> utoipa::ToSchema for Patch<T> {
    fn name() -> std::borrow::Cow<'static, str> {
        format!("{}Patch", T::name()).into()
    }
    fn schemas(schemas: &mut Vec<(String, Schema)>) {
        T::schemas(schemas);
    }
}

#[derive(utoipa::IntoResponses)]
#[allow(dead_code)]
pub enum ServiceErrors {
    /// 缺少或无效的 Bearer token。
    #[response(status = 401)]
    Unauthorized(crate::ErrorBody),
    /// 服务内部错误。
    #[response(status = 500)]
    Internal(crate::ErrorBody),
}

#[derive(utoipa::IntoResponses)]
#[allow(dead_code)]
pub enum JsonBodyErrors {
    /// 请求体超过大小限制。
    #[response(status = 413, content_type = "text/plain")]
    TooLarge(String),
    /// 请求体需要 application/json。
    #[response(status = 415, content_type = "text/plain")]
    MediaType(String),
    /// JSON 结构与请求对象不匹配。
    #[response(status = 422, content_type = "text/plain")]
    InvalidType(String),
}

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
        .route("/openapi.json", get(move || async move { spec }))
}
