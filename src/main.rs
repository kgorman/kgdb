//! KGDB — append-only JSON log database (Rust edition)
//!
//! Env vars:
//!   KGDB_DATA        data directory  (default: ./data)
//!   KGDB_PORT        listen port     (default: 8000)
//!   KGDB_BIND        bind address    (default: 127.0.0.1)
//!   KGDB_AUTH_TOKEN  bearer token    (default: disabled)
//!   KGDB_FSYNC       fsync on write  (default: off)
//!   RUST_LOG         tracing filter  (default: info)

use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Path, Query, Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Json, Response},
    routing::{delete, get, post},
    Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::task;

mod engine;
use engine::{validate_name, Engine, EngineError};

type AppState = Arc<Engine>;

// ── error → HTTP ──────────────────────────────────────────────────────────────

struct AppError(EngineError);

impl From<EngineError> for AppError {
    fn from(e: EngineError) -> Self { Self(e) }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, msg) = match &self.0 {
            EngineError::InvalidName(_) => (StatusCode::BAD_REQUEST,       self.0.to_string()),
            EngineError::TooLarge       => (StatusCode::PAYLOAD_TOO_LARGE, self.0.to_string()),
            EngineError::BatchTooLarge  => (StatusCode::PAYLOAD_TOO_LARGE, self.0.to_string()),
            EngineError::OffsetTooLarge => (StatusCode::BAD_REQUEST,       self.0.to_string()),
            // Don't leak filesystem paths or internal details to callers
            EngineError::Io(_)
            | EngineError::Json(_) => {
                tracing::error!(err = %self.0, "internal engine error");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal server error".to_string())
            }
        };
        (status, Json(json!({ "error": msg }))).into_response()
    }
}

// ── auth ──────────────────────────────────────────────────────────────────────

/// Constant-time comparison to prevent token oracle timing attacks.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() { return false; }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

async fn check_auth(token: Arc<Option<String>>, req: Request, next: Next) -> Response {
    if let Some(expected) = token.as_ref() {
        let ok = req
            .headers()
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(|provided| ct_eq(provided.as_bytes(), expected.as_bytes()))
            .unwrap_or(false);

        if !ok {
            return (StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"}))).into_response();
        }
    }
    next.run(req).await
}

// ── query params ──────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct FindParams {
    #[serde(default = "default_n")]
    n: usize,
    #[serde(default)]
    offset: usize,
    /// Field equality filter as a JSON object: {"field":"value",...}
    #[serde(rename = "where")]
    filter: Option<String>,
}

#[derive(Deserialize)]
struct TailParams {
    #[serde(default = "default_n")]
    n: usize,
    /// Field equality filter as a JSON object: {"field":"value",...}
    #[serde(rename = "where")]
    filter: Option<String>,
}

fn default_n() -> usize { 20 }

/// Parse the raw `where` query string into a HashMap. Returns 400 on bad input.
fn parse_filter(raw: Option<String>) -> Result<Option<HashMap<String, Value>>, (StatusCode, Json<Value>)> {
    let Some(s) = raw else { return Ok(None) };
    let v: Value = serde_json::from_str(&s).map_err(|_| (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": "invalid JSON in 'where' parameter" })),
    ))?;
    match v {
        Value::Object(m) if m.is_empty() => Ok(None),
        Value::Object(m) => Ok(Some(m.into_iter().collect())),
        _ => Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "'where' must be a JSON object" })),
        )),
    }
}

// ── handlers ──────────────────────────────────────────────────────────────────

async fn root(State(eng): State<AppState>) -> impl IntoResponse {
    let dbs = eng.list_databases().unwrap_or_default();
    Json(json!({
        "engine":  "KGDB",
        "version": "1.0.0",
        "lang":    "rust",
        "storage": "append-only NDJSON",
        "databases": dbs,
    }))
}

async fn health() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

// ── database ──────────────────────────────────────────────────────────────────

async fn list_collections(
    State(eng): State<AppState>,
    Path(db): Path<String>,
) -> Result<Json<Value>, AppError> {
    let colls = eng.list_collections(&db)?;
    Ok(Json(json!({ "db": db, "collections": colls })))
}

async fn drop_database(
    State(eng): State<AppState>,
    Path(db): Path<String>,
) -> Result<Json<Value>, AppError> {
    let n = eng.drop_database(&db)?;
    Ok(Json(json!({ "db": db, "dropped": true, "collections_removed": n })))
}

// ── collection ────────────────────────────────────────────────────────────────

async fn insert_one(
    State(eng): State<AppState>,
    Path((db, coll)): Path<(String, String)>,
    Json(doc): Json<Value>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    if !doc.is_object() {
        return Ok((StatusCode::BAD_REQUEST, Json(json!({ "error": "body must be a JSON object" }))));
    }
    let id = task::spawn_blocking(move || eng.insert(&db, &coll, doc))
        .await
        .unwrap()?;
    Ok((StatusCode::CREATED, Json(json!({ "inserted": 1, "_id": id }))))
}

async fn insert_many(
    State(eng): State<AppState>,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let docs = match body {
        Value::Array(arr) => arr,
        _ => return Ok((StatusCode::BAD_REQUEST, Json(json!({ "error": "body must be a JSON array" })))),
    };
    if docs.len() > 10_000 {
        return Ok((StatusCode::BAD_REQUEST, Json(json!({ "error": "batch exceeds 10000 document limit" }))));
    }
    let ids = task::spawn_blocking(move || eng.insert_many(&db, &coll, docs))
        .await
        .unwrap()?;
    let n = ids.len();
    Ok((StatusCode::CREATED, Json(json!({ "inserted": n, "_ids": ids }))))
}

async fn find(
    State(eng): State<AppState>,
    Path((db, coll)): Path<(String, String)>,
    Query(p): Query<FindParams>,
) -> Result<Json<Value>, Response> {
    if p.n == 0 || p.n > 100_000 {
        return Ok(Json(json!({ "error": "n must be 1–100000" })));
    }
    let filter = parse_filter(p.filter).map_err(IntoResponse::into_response)?;
    let (db2, coll2) = (db.clone(), coll.clone());
    let (n, offset)  = (p.n, p.offset);
    let docs = task::spawn_blocking(move || eng.find(&db2, &coll2, n, offset, filter.as_ref()))
        .await
        .unwrap()
        .map_err(|e| AppError(e).into_response())?;
    let count = docs.len();
    Ok(Json(json!({ "db": db, "collection": coll, "offset": offset, "count": count, "documents": docs })))
}

async fn tail(
    State(eng): State<AppState>,
    Path((db, coll)): Path<(String, String)>,
    Query(p): Query<TailParams>,
) -> Result<Json<Value>, Response> {
    if p.n == 0 || p.n > 100_000 {
        return Ok(Json(json!({ "error": "n must be 1–100000" })));
    }
    let filter = parse_filter(p.filter).map_err(IntoResponse::into_response)?;
    let (db2, coll2) = (db.clone(), coll.clone());
    let n = p.n;
    let docs = task::spawn_blocking(move || eng.tail(&db2, &coll2, n, filter.as_ref()))
        .await
        .unwrap()
        .map_err(|e| AppError(e).into_response())?;
    let count = docs.len();
    Ok(Json(json!({ "db": db, "collection": coll, "count": count, "documents": docs })))
}

async fn stats(
    State(eng): State<AppState>,
    Path((db, coll)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let (db2, coll2) = (db.clone(), coll.clone());
    let s = task::spawn_blocking(move || eng.stats(&db2, &coll2))
        .await
        .unwrap()?;
    let mut v = serde_json::to_value(s).unwrap();
    v["db"] = json!(db);
    v["collection"] = json!(coll);
    Ok(Json(v))
}

async fn drop_collection(
    State(eng): State<AppState>,
    Path((db, coll)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let (db2, coll2) = (db.clone(), coll.clone());
    let dropped = task::spawn_blocking(move || eng.drop_collection(&db2, &coll2))
        .await
        .unwrap()?;
    Ok(Json(json!({ "db": db, "collection": coll, "dropped": dropped })))
}

// ── index management ──────────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct CreateIndexBody {
    field: String,
}

async fn create_index(
    State(eng): State<AppState>,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<CreateIndexBody>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    validate_name(&body.field).map_err(AppError::from)?;
    let field = body.field;
    let (db2, coll2, field2) = (db.clone(), coll.clone(), field.clone());
    task::spawn_blocking(move || eng.create_index(&db2, &coll2, &field2))
        .await
        .unwrap()?;
    Ok((StatusCode::CREATED, Json(json!({ "db": db, "collection": coll, "field": field, "created": true }))))
}

async fn list_indexes(
    State(eng): State<AppState>,
    Path((db, coll)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let (db2, coll2) = (db.clone(), coll.clone());
    let fields = task::spawn_blocking(move || eng.list_indexes(&db2, &coll2))
        .await
        .unwrap()?;
    Ok(Json(json!({ "db": db, "collection": coll, "indexes": fields })))
}

async fn drop_index(
    State(eng): State<AppState>,
    Path((db, coll, field)): Path<(String, String, String)>,
) -> Result<Json<Value>, AppError> {
    validate_name(&field).map_err(AppError::from)?;
    let field2 = field.clone();
    let existed = task::spawn_blocking(move || eng.drop_index(&db, &coll, &field2))
        .await
        .unwrap()?;
    Ok(Json(json!({ "field": field, "dropped": existed })))
}

// ── main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into())
        )
        .init();

    let data_dir  = std::env::var("KGDB_DATA").unwrap_or_else(|_| "./data".into());
    let port: u16 = std::env::var("KGDB_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8000);
    let bind_addr  = std::env::var("KGDB_BIND").unwrap_or_else(|_| "127.0.0.1".into());
    let auth_token = Arc::new(std::env::var("KGDB_AUTH_TOKEN").ok());

    if auth_token.is_some() {
        tracing::info!("auth: bearer token enabled");
    } else {
        tracing::warn!("auth: disabled — set KGDB_AUTH_TOKEN to require authentication");
    }

    let engine = Arc::new(Engine::new(&data_dir).expect("failed to initialise engine"));
    tracing::info!("KGDB starting  data={data_dir}  port={port}  bind={bind_addr}");

    // /health is intentionally unauthenticated for load-balancer probes
    let protected = {
        let token = Arc::clone(&auth_token);
        Router::new()
            .route("/",                      get(root))
            .route("/v1/:db",                get(list_collections).delete(drop_database))
            .route("/v1/:db/:coll",          post(insert_one).delete(drop_collection))
            .route("/v1/:db/:coll/batch",    post(insert_many))
            .route("/v1/:db/:coll/find",     get(find))
            .route("/v1/:db/:coll/tail",     get(tail))
            .route("/v1/:db/:coll/stats",    get(stats))
            .route("/v1/:db/:coll/index",        post(create_index))
            .route("/v1/:db/:coll/indexes",      get(list_indexes))
            .route("/v1/:db/:coll/index/:field", delete(drop_index))
            .route_layer(axum::middleware::from_fn(move |req, next| {
                check_auth(Arc::clone(&token), req, next)
            }))
    };

    let app = Router::new()
        .route("/health", get(health))
        .merge(protected)
        .layer(axum::extract::DefaultBodyLimit::max(16 * 1024 * 1024))
        .with_state(engine);

    let bind = format!("{bind_addr}:{port}");
    let listener = tokio::net::TcpListener::bind(&bind).await.unwrap();
    tracing::info!("listening on http://{bind}");
    axum::serve(listener, app).await.unwrap();
}
