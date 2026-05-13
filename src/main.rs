//! KGDB — append-only JSON log database (Rust edition)
//!
//! Env vars:
//!   KGDB_DATA    data directory (default: ./data)
//!   KGDB_PORT    listen port    (default: 8000)
//!   KGDB_FSYNC   set to "1" to fsync on every write (default: off)
//!   RUST_LOG     tracing filter (default: info)

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{delete, get, post},
    Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::task;

mod engine;
use engine::{Engine, EngineError};

type AppState = Arc<Engine>;

// ── error → HTTP ──────────────────────────────────────────────────────────────

struct AppError(EngineError);

impl From<EngineError> for AppError {
    fn from(e: EngineError) -> Self { Self(e) }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let status = match &self.0 {
            EngineError::InvalidName(_) => StatusCode::BAD_REQUEST,
            EngineError::TooLarge       => StatusCode::PAYLOAD_TOO_LARGE,
            _                           => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(json!({ "error": self.0.to_string() }))).into_response()
    }
}

// ── query params ──────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct FindParams {
    #[serde(default = "default_n")]
    n: usize,
    #[serde(default)]
    offset: usize,
}

#[derive(Deserialize)]
struct TailParams {
    #[serde(default = "default_n")]
    n: usize,
}

fn default_n() -> usize { 20 }

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
) -> impl IntoResponse {
    let colls = eng.list_collections(&db).unwrap_or_default();
    Json(json!({ "db": db, "collections": colls }))
}

async fn drop_database(
    State(eng): State<AppState>,
    Path(db): Path<String>,
) -> impl IntoResponse {
    let n = eng.drop_database(&db).unwrap_or(0);
    Json(json!({ "db": db, "dropped": true, "collections_removed": n }))
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
) -> Result<Json<Value>, AppError> {
    if p.n == 0 || p.n > 100_000 {
        return Ok(Json(json!({ "error": "n must be 1–100000" })));
    }
    let (db2, coll2) = (db.clone(), coll.clone());
    let docs = task::spawn_blocking(move || eng.find(&db2, &coll2, p.n, p.offset))
        .await
        .unwrap()?;
    let count = docs.len();
    Ok(Json(json!({ "db": db, "collection": coll, "offset": p.offset, "count": count, "documents": docs })))
}

async fn tail(
    State(eng): State<AppState>,
    Path((db, coll)): Path<(String, String)>,
    Query(p): Query<TailParams>,
) -> Result<Json<Value>, AppError> {
    if p.n == 0 || p.n > 100_000 {
        return Ok(Json(json!({ "error": "n must be 1–100000" })));
    }
    let (db2, coll2) = (db.clone(), coll.clone());
    let docs = task::spawn_blocking(move || eng.tail(&db2, &coll2, p.n))
        .await
        .unwrap()?;
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

// ── main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into())
        )
        .init();

    let data_dir = std::env::var("KGDB_DATA").unwrap_or_else(|_| "./data".into());
    let port: u16 = std::env::var("KGDB_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8000);

    let engine = Arc::new(Engine::new(&data_dir).expect("failed to initialise engine"));
    tracing::info!("KGDB starting  data={data_dir}  port={port}");

    let app = Router::new()
        .route("/",                      get(root))
        .route("/health",                get(health))
        .route("/v1/:db",                get(list_collections).delete(drop_database))
        .route("/v1/:db/:coll",          post(insert_one).delete(drop_collection))
        .route("/v1/:db/:coll/batch",    post(insert_many))
        .route("/v1/:db/:coll/find",     get(find))
        .route("/v1/:db/:coll/tail",     get(tail))
        .route("/v1/:db/:coll/stats",    get(stats))
        .with_state(engine);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .unwrap();
    tracing::info!("listening on http://0.0.0.0:{port}");
    axum::serve(listener, app).await.unwrap();
}
