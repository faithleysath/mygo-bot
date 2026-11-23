use axum::{Router, body::Bytes, http::header, response::Html, routing::get};
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

fn json_bytes(mut spec: OpenApi) -> serde_json::Result<Bytes> {
    spec.components.get_or_insert_default().add_security_scheme(
        "bearerAuth",
        SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
    );
    spec.security = Some(vec![SecurityRequirement::new(
        "bearerAuth",
        [] as [&str; 0],
    )]);
    Ok(Bytes::from(serde_json::to_vec(&spec)?))
}

pub fn routes() -> Router {
    use futures_util::FutureExt;

    let openapi = std::sync::Arc::new(std::sync::OnceLock::new());
    Router::new()
        .route("/docs", get(|| async { Html(PAGE) }))
        .route(
            "/openapi.json",
            get(move || async move {
                // The shared future survives a client disconnect while the schema is built.
                let json = openapi
                    .get_or_init(|| {
                        let task =
                            tokio::task::spawn_blocking(|| json_bytes(crate::api::openapi()));
                        async move {
                            match task.await {
                                Ok(Ok(json)) => Ok(json),
                                Ok(Err(error)) => {
                                    tracing::error!(%error, "could not serialize OpenAPI");
                                    Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                                }
                                Err(error) => {
                                    tracing::error!(%error, "could not build OpenAPI");
                                    Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                                }
                            }
                        }
                        .boxed()
                        .shared()
                    })
                    .clone();
                json.await
                    .map(|body| ([(header::CONTENT_TYPE, "application/json")], body))
            }),
        )
}

pub(crate) enum RouteRegistry {
    Documented(Box<utoipa_axum::router::OpenApiRouter<crate::App>>),
    Bare(Router<crate::App>),
}

impl RouteRegistry {
    pub(crate) fn new(documented: bool) -> Self {
        if documented {
            Self::Documented(Box::new(utoipa_axum::router::OpenApiRouter::new()))
        } else {
            Self::Bare(Router::new())
        }
    }

    pub(crate) fn register(self, entry: impl FnOnce(Self) -> Self) -> Self {
        entry(self)
    }

    pub(crate) fn routes(
        self,
        documented: impl FnOnce(
            utoipa_axum::router::OpenApiRouter<crate::App>,
        ) -> utoipa_axum::router::OpenApiRouter<crate::App>,
        bare: impl FnOnce(Router<crate::App>) -> Router<crate::App>,
    ) -> Self {
        match self {
            Self::Documented(router) => Self::Documented(Box::new(documented(*router))),
            Self::Bare(router) => Self::Bare(bare(router)),
        }
    }

    pub(crate) fn merge(self, inner: Self) -> Self {
        match (self, inner) {
            (Self::Documented(outer), Self::Documented(inner)) => {
                Self::Documented(Box::new(outer.merge(*inner)))
            }
            (Self::Bare(outer), Self::Bare(inner)) => Self::Bare(outer.merge(inner)),
            _ => unreachable!("a route registry must use one metadata mode"),
        }
    }

    pub(crate) fn layer(self, body_limit: axum::extract::DefaultBodyLimit) -> Self {
        match self {
            Self::Documented(router) => Self::Documented(Box::new(router.layer(body_limit))),
            Self::Bare(router) => Self::Bare(router.layer(body_limit)),
        }
    }

    pub(crate) fn into_parts(self) -> (Router<crate::App>, Option<OpenApi>) {
        match self {
            Self::Documented(router) => {
                let (router, schema) = router.split_for_parts();
                (router, Some(schema))
            }
            Self::Bare(router) => (router, None),
        }
    }
}

// Utoipa's generated path and method accessors let both router modes use
// the same handler declarations. Build operations and schemas only on demand.
macro_rules! register {
    ($($handler:ident $(::$part:ident)*),+ $(,)?) => {
        |registry: $crate::docs::RouteRegistry| registry.routes(
            |router| router.routes(utoipa_axum::routes!($($handler $(::$part)*),+)),
            |mut router| {
                use utoipa_axum::PathItemExt;
                $(
                    let path = utoipa_axum::routes!(@path [path()] of $handler $(::$part)*);
                    let methods = utoipa_axum::routes!(@path [methods()] of $handler $(::$part)*);
                    let method_router = methods.iter().fold(axum::routing::MethodRouter::new(), |router, method| {
                        router.on(method.to_method_filter(), $handler $(::$part)*)
                    });
                    router = router.route(if path.is_empty() { "/" } else { path.as_str() }, method_router);
                )+
                router
            },
        )
    };
}
pub(crate) use register;
