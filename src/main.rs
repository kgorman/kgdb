//! KGDB — append-only JSON log database (Rust edition)
//!
//! Env vars:
//!   KGDB_DATA        data directory  (default: ./data)
//!   KGDB_PORT        listen port     (default: 8000)
//!   KGDB_BIND        bind address    (default: 127.0.0.1)
//!   KGDB_AUTH_TOKEN           global bearer token    (default: disabled)
//!   KGDB_AUTH_TOKEN_<dbname>  per-db bearer token    (default: disabled)
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

/// Token configuration: one optional global admin token + per-database tokens.
///
/// Global token:    KGDB_AUTH_TOKEN=<token>
/// Per-db tokens:   KGDB_AUTH_TOKEN_<dbname>=<token>
///
/// For a request to /v1/{db}/..., either the global or the db-scoped token is accepted.
/// For routes without a db segment (/), only the global token applies.
/// If no token is configured for a route, access is open.
struct AuthConfig {
    global: Option<String>,
    per_db: HashMap<String, String>,
}

impl AuthConfig {
    /// True when no tokens are configured at all — the documented "auth disabled" mode.
    fn disabled(&self) -> bool {
        self.global.is_none() && self.per_db.is_empty()
    }

    fn is_authorized(&self, provided: Option<&str>, db: Option<&str>) -> bool {
        if self.disabled() {
            return true;
        }
        // Fail closed: once any token is configured, every protected route needs one.
        let Some(tok) = provided else { return false };
        if let Some(g) = &self.global {
            if ct_eq(tok.as_bytes(), g.as_bytes()) { return true; }
        }
        if let Some(d) = db {
            if let Some(t) = self.per_db.get(d) {
                if ct_eq(tok.as_bytes(), t.as_bytes()) { return true; }
            }
        }
        false
    }
}

/// Percent-decode a single path segment, matching what the `Path` extractor hands
/// to handlers. Without this the auth check and the handler can see different
/// database names (e.g. `/v1/%73ecret` vs `/v1/secret`).
fn percent_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                let hex = s.get(i + 1..i + 3)?;
                out.push(u8::from_str_radix(hex, 16).ok()?);
                i += 3;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

/// Extract the decoded database name from a URI path of the form /v1/{db}/...
///
/// Returns `None` if the segment is absent, empty, or not decodable — callers must
/// treat that as "no database matched", never as "no auth required".
fn extract_db(path: &str) -> Option<String> {
    let rest = path.strip_prefix("/v1/")?;
    let db   = rest.split('/').next()?;
    if db.is_empty() { return None; }
    // A decoded '/' would mean the segment spans a route boundary; reject it.
    let decoded = percent_decode(db)?;
    if decoded.is_empty() || decoded.contains('/') { None } else { Some(decoded) }
}

async fn check_auth(auth: Arc<AuthConfig>, req: Request, next: Next) -> Response {
    let db       = extract_db(req.uri().path());
    let provided = req.headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));

    if !auth.is_authorized(provided, db.as_deref()) {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"}))).into_response();
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

// ── POST /find and POST /tail ─────────────────────────────────────────────────
//
// MongoDB-style: the body is a flat JSON object. Reserved keys `n` and `offset`
// are control parameters; every other key is an implicit filter field.
//
//   POST /find   {"role":"admin","n":50,"offset":0}
//   POST /tail   {"level":"error","n":100}

struct PostQuery {
    filter: Option<HashMap<String, Value>>,
    n:      usize,
    offset: usize,
}

/// Extract control params (n, offset) from a flat object; remainder is the filter.
fn parse_post_body(body: Value) -> Result<PostQuery, (StatusCode, Json<Value>)> {
    let Value::Object(mut map) = body else {
        return Err((StatusCode::BAD_REQUEST, Json(json!({ "error": "body must be a JSON object" }))));
    };

    let n = match map.remove("n") {
        None => default_n(),
        Some(Value::Number(num)) => num.as_u64()
            .map(|v| v as usize)
            .ok_or_else(|| (StatusCode::BAD_REQUEST, Json(json!({ "error": "n must be a positive integer" }))))?,
        Some(_) => return Err((StatusCode::BAD_REQUEST, Json(json!({ "error": "n must be a positive integer" })))),
    };

    let offset = match map.remove("offset") {
        None => 0,
        Some(Value::Number(num)) => num.as_u64()
            .map(|v| v as usize)
            .ok_or_else(|| (StatusCode::BAD_REQUEST, Json(json!({ "error": "offset must be a non-negative integer" }))))?,
        Some(_) => return Err((StatusCode::BAD_REQUEST, Json(json!({ "error": "offset must be a non-negative integer" })))),
    };

    let filter = if map.is_empty() {
        None
    } else {
        Some(map.into_iter().collect())
    };

    Ok(PostQuery { filter, n, offset })
}

async fn find_post(
    State(eng): State<AppState>,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, Response> {
    let q = parse_post_body(body).map_err(IntoResponse::into_response)?;
    if q.n == 0 || q.n > 100_000 {
        return Ok(Json(json!({ "error": "n must be 1–100000" })));
    }
    let (db2, coll2) = (db.clone(), coll.clone());
    let (n, offset, filter) = (q.n, q.offset, q.filter);
    let docs = task::spawn_blocking(move || eng.find(&db2, &coll2, n, offset, filter.as_ref()))
        .await
        .unwrap()
        .map_err(|e| AppError(e).into_response())?;
    let count = docs.len();
    Ok(Json(json!({ "db": db, "collection": coll, "offset": offset, "count": count, "documents": docs })))
}

async fn tail_post(
    State(eng): State<AppState>,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, Response> {
    let q = parse_post_body(body).map_err(IntoResponse::into_response)?;
    if q.n == 0 || q.n > 100_000 {
        return Ok(Json(json!({ "error": "n must be 1–100000" })));
    }
    let (db2, coll2) = (db.clone(), coll.clone());
    let (n, filter) = (q.n, q.filter);
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
    let mut per_db: HashMap<String, String> = HashMap::new();
    for (key, val) in std::env::vars() {
        if let Some(db_name) = key.strip_prefix("KGDB_AUTH_TOKEN_") {
            if !db_name.is_empty() {
                per_db.insert(db_name.to_owned(), val);
            }
        }
    }
    let auth = Arc::new(AuthConfig {
        global: std::env::var("KGDB_AUTH_TOKEN").ok(),
        per_db,
    });

    if auth.global.is_some() {
        tracing::info!("auth: global bearer token enabled");
    } else if auth.per_db.is_empty() {
        tracing::warn!("auth: disabled — set KGDB_AUTH_TOKEN to require authentication");
    }
    for db_name in auth.per_db.keys() {
        tracing::info!("auth: per-db token enabled for database '{db_name}'");
    }

    let fsync = std::env::var("KGDB_FSYNC").unwrap_or_default() == "1";
    if fsync { tracing::info!("fsync: enabled (durability mode)"); }

    let engine = Arc::new(Engine::new(&data_dir, fsync).expect("failed to initialise engine"));
    tracing::info!("KGDB starting  data={data_dir}  port={port}  bind={bind_addr}");

    // /health is intentionally unauthenticated for load-balancer probes
    let protected = {
        let auth2 = Arc::clone(&auth);
        Router::new()
            .route("/",                      get(root))
            .route("/v1/:db",                get(list_collections).delete(drop_database))
            .route("/v1/:db/:coll",          post(insert_one).delete(drop_collection))
            .route("/v1/:db/:coll/batch",    post(insert_many))
            .route("/v1/:db/:coll/find",     get(find).post(find_post))
            .route("/v1/:db/:coll/tail",     get(tail).post(tail_post))
            .route("/v1/:db/:coll/stats",    get(stats))
            .route("/v1/:db/:coll/index",        post(create_index))
            .route("/v1/:db/:coll/indexes",      get(list_indexes))
            .route("/v1/:db/:coll/index/:field", delete(drop_index))
            .route_layer(axum::middleware::from_fn(move |req, next| {
                check_auth(Arc::clone(&auth2), req, next)
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
