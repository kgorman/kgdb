//! KGDB storage engine — append-only NDJSON log.
//!
//! One file per collection: `{data_dir}/{db}/{coll}.ndjson`
//! File handles kept warm in a DashMap; writes are a single write_all() per batch.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use dashmap::DashMap;
use memchr::memchr_iter;
use serde_json::Value;

const WRITE_BUF: usize = 64  * 1024;   // 64 KB
const READ_BUF:  usize = 128 * 1024;   // 128 KB
const MAX_DOC:   usize = 16  * 1024 * 1024;

// ── ID generation ─────────────────────────────────────────────────────────────
// Format: [8B timestamp_ns hex][2B atomic counter hex][2B random hex] = 24 chars

static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn new_id() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let seq = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed) & 0xFFFF;
    let rnd: [u8; 2] = rand::random();
    format!("{ts:016x}{seq:04x}{:02x}{:02x}", rnd[0], rnd[1])
}

// ── errors ────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("invalid name '{0}': alphanumeric, _ and - only, max 128 chars")]
    InvalidName(String),
    #[error("document exceeds 16 MB limit")]
    TooLarge,
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

// ── engine ────────────────────────────────────────────────────────────────────

type Handle = Arc<Mutex<BufWriter<File>>>;

pub struct Engine {
    root:    PathBuf,
    handles: DashMap<(String, String), Handle>,
}

impl Engine {
    pub fn new(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(Self { root, handles: DashMap::new() })
    }

    // ── internals ─────────────────────────────────────────────────────────────

    fn coll_path(&self, db: &str, coll: &str) -> Result<PathBuf, EngineError> {
        validate_name(db)?;
        validate_name(coll)?;
        let dir = self.root.join(db);
        fs::create_dir_all(&dir)?;
        Ok(dir.join(format!("{coll}.ndjson")))
    }

    fn get_handle(&self, db: &str, coll: &str) -> Result<Handle, EngineError> {
        let key = (db.to_owned(), coll.to_owned());
        if let Some(h) = self.handles.get(&key) {
            return Ok(Arc::clone(&*h));
        }
        let path = self.coll_path(db, coll)?;
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let handle = Arc::new(Mutex::new(BufWriter::with_capacity(WRITE_BUF, file)));
        self.handles.insert(key, Arc::clone(&handle));
        Ok(handle)
    }

    // ── writes ────────────────────────────────────────────────────────────────

    pub fn insert(&self, db: &str, coll: &str, doc: Value) -> Result<String, EngineError> {
        let (id, line) = annotate(doc)?;
        let handle = self.get_handle(db, coll)?;
        let mut w = handle.lock().unwrap();
        w.write_all(line.as_bytes())?;
        w.flush()?;
        Ok(id)
    }

    pub fn insert_many(&self, db: &str, coll: &str, docs: Vec<Value>) -> Result<Vec<String>, EngineError> {
        if docs.is_empty() {
            return Ok(vec![]);
        }
        let mut ids  = Vec::with_capacity(docs.len());
        let mut blob = Vec::new();

        for doc in docs {
            let (id, line) = annotate(doc)?;
            blob.extend_from_slice(line.as_bytes());
            ids.push(id);
        }

        let handle = self.get_handle(db, coll)?;
        let mut w = handle.lock().unwrap();
        w.write_all(&blob)?;
        w.flush()?;
        Ok(ids)
    }

    // ── reads ─────────────────────────────────────────────────────────────────

    pub fn find(&self, db: &str, coll: &str, n: usize, offset: usize) -> Result<Vec<Value>, EngineError> {
        let path = self.coll_path(db, coll)?;
        if !path.exists() { return Ok(vec![]); }
        scan(&path, Some(n), offset)
    }

    pub fn tail(&self, db: &str, coll: &str, n: usize) -> Result<Vec<Value>, EngineError> {
        let path = self.coll_path(db, coll)?;
        if !path.exists() { return Ok(vec![]); }
        let total  = count_lines(&path)?;
        let offset = total.saturating_sub(n);
        scan(&path, Some(n), offset)
    }

    pub fn stats(&self, db: &str, coll: &str) -> Result<CollStats, EngineError> {
        let path = self.coll_path(db, coll)?;
        if !path.exists() {
            return Ok(CollStats { path: path.to_string_lossy().into_owned(), ..Default::default() });
        }
        let meta  = fs::metadata(&path)?;
        let count = count_lines(&path)?;
        Ok(CollStats {
            exists:     true,
            count,
            size_bytes: meta.len(),
            size_mb:    (meta.len() as f64) / 1_048_576.0,
            path:       path.to_string_lossy().into_owned(),
        })
    }

    // ── DDL ───────────────────────────────────────────────────────────────────

    pub fn list_databases(&self) -> io::Result<Vec<String>> {
        let mut out = vec![];
        for e in fs::read_dir(&self.root)? {
            let e = e?;
            if e.file_type()?.is_dir() {
                out.push(e.file_name().to_string_lossy().into_owned());
            }
        }
        out.sort();
        Ok(out)
    }

    pub fn list_collections(&self, db: &str) -> io::Result<Vec<String>> {
        let dir = self.root.join(db);
        if !dir.exists() { return Ok(vec![]); }
        let mut out = vec![];
        for e in fs::read_dir(dir)? {
            let e    = e?;
            let name = e.file_name().to_string_lossy().into_owned();
            if let Some(stem) = name.strip_suffix(".ndjson") {
                out.push(stem.to_owned());
            }
        }
        out.sort();
        Ok(out)
    }

    pub fn drop_collection(&self, db: &str, coll: &str) -> Result<bool, EngineError> {
        let key = (db.to_owned(), coll.to_owned());
        if let Some((_, h)) = self.handles.remove(&key) {
            let _ = h.lock().unwrap().flush();
        }
        let path = self.coll_path(db, coll)?;
        if path.exists() { fs::remove_file(&path)?; return Ok(true); }
        Ok(false)
    }

    pub fn drop_database(&self, db: &str) -> io::Result<usize> {
        let dir = self.root.join(db);
        if !dir.exists() { return Ok(0); }
        self.handles.retain(|(d, _), h| {
            if d == db { let _ = h.lock().unwrap().flush(); false } else { true }
        });
        let mut count = 0;
        for e in fs::read_dir(&dir)? {
            let e = e?;
            if e.file_name().to_string_lossy().ends_with(".ndjson") {
                fs::remove_file(e.path())?;
                count += 1;
            }
        }
        let _ = fs::remove_dir(&dir);
        Ok(count)
    }
}

// ── response types ────────────────────────────────────────────────────────────

#[derive(Default, serde::Serialize)]
pub struct CollStats {
    pub exists:     bool,
    pub count:      usize,
    pub size_bytes: u64,
    pub size_mb:    f64,
    pub path:       String,
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn validate_name(name: &str) -> Result<(), EngineError> {
    if name.is_empty() || name.len() > 128 {
        return Err(EngineError::InvalidName(name.to_owned()));
    }
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-') {
        return Err(EngineError::InvalidName(name.to_owned()));
    }
    Ok(())
}

fn annotate(mut doc: Value) -> Result<(String, String), EngineError> {
    let id = new_id();
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    if let Value::Object(ref mut m) = doc {
        m.insert("_id".into(), Value::String(id.clone()));
        m.insert("_ts".into(), Value::Number(ts.into()));
    }
    let mut s = serde_json::to_string(&doc)?;
    if s.len() > MAX_DOC { return Err(EngineError::TooLarge); }
    s.push('\n');
    Ok((id, s))
}

fn scan(path: &Path, n: Option<usize>, offset: usize) -> Result<Vec<Value>, EngineError> {
    let file   = File::open(path)?;
    let reader = BufReader::with_capacity(READ_BUF, file);
    let mut docs    = Vec::new();
    let mut skipped = 0usize;

    for line in reader.lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() { continue; }
        if skipped < offset { skipped += 1; continue; }
        docs.push(serde_json::from_str(line)?);
        if let Some(limit) = n {
            if docs.len() >= limit { break; }
        }
    }
    Ok(docs)
}

/// Fast newline count using SIMD-accelerated memchr.
fn count_lines(path: &Path) -> io::Result<usize> {
    let mut file  = File::open(path)?;
    let mut buf   = vec![0u8; READ_BUF];
    let mut count = 0usize;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 { break; }
        count += memchr_iter(b'\n', &buf[..n]).count();
    }
    Ok(count)
}
