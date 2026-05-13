//! KGDB storage engine — append-only NDJSON log.
//!
//! One file per collection: `{data_dir}/{db}/{coll}.ndjson`
//! File handles kept warm in a DashMap; writes are a single write_all() per batch.

use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use dashmap::DashMap;
use memchr::memchr_iter;
use serde_json::Value;

const WRITE_BUF:  usize = 64  * 1024;        // 64 KB
const READ_BUF:   usize = 128 * 1024;        // 128 KB
const MAX_DOC:    usize = 16  * 1024 * 1024; // 16 MB per document
const MAX_BATCH:  usize = 128 * 1024 * 1024; // 128 MB aggregate batch
const MAX_OFFSET: usize = 10_000_000;

// ── ID generation ─────────────────────────────────────────────────────────────
// Format: [16 hex timestamp_ns][4 hex counter][16 hex random] = 36 chars

static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn new_id() -> String {
    let ts  = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let seq = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed) & 0xFFFF;
    let rnd: [u8; 8] = rand::random();
    format!("{ts:016x}{seq:04x}{:016x}", u64::from_be_bytes(rnd))
}

// ── errors ────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("invalid name '{0}': alphanumeric, _ and - only, max 128 chars")]
    InvalidName(String),
    #[error("document exceeds 16 MB limit")]
    TooLarge,
    #[error("batch exceeds 128 MB aggregate size limit")]
    BatchTooLarge,
    #[error("offset exceeds maximum of {MAX_OFFSET}")]
    OffsetTooLarge,
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
        // Hot path: read lock only
        if let Some(h) = self.handles.get(&key) {
            return Ok(Arc::clone(&*h));
        }
        // Cold path: entry holds the shard write lock across check + insert, preventing TOCTOU
        let path = self.coll_path(db, coll)?;
        let entry = self.handles.entry(key).or_try_insert_with(|| {
            let file = OpenOptions::new().create(true).append(true).open(&path)?;
            Ok::<Handle, EngineError>(Arc::new(Mutex::new(BufWriter::with_capacity(WRITE_BUF, file))))
        })?;
        Ok(Arc::clone(&*entry))
    }

    // ── writes ────────────────────────────────────────────────────────────────

    pub fn insert(&self, db: &str, coll: &str, doc: Value) -> Result<String, EngineError> {
        let (id, line) = annotate(doc)?;
        let handle = self.get_handle(db, coll)?;
        let mut w = handle.lock().unwrap_or_else(|e| e.into_inner());
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
            if blob.len() > MAX_BATCH {
                return Err(EngineError::BatchTooLarge);
            }
            ids.push(id);
        }

        let handle = self.get_handle(db, coll)?;
        let mut w = handle.lock().unwrap_or_else(|e| e.into_inner());
        w.write_all(&blob)?;
        w.flush()?;
        Ok(ids)
    }

    // ── reads ─────────────────────────────────────────────────────────────────

    pub fn find(
        &self,
        db: &str,
        coll: &str,
        n: usize,
        offset: usize,
        filter: Option<&HashMap<String, Value>>,
    ) -> Result<Vec<Value>, EngineError> {
        if offset > MAX_OFFSET {
            return Err(EngineError::OffsetTooLarge);
        }
        let path = self.coll_path(db, coll)?;
        scan(&path, Some(n), offset, filter)
    }

    pub fn tail(
        &self,
        db: &str,
        coll: &str,
        n: usize,
        filter: Option<&HashMap<String, Value>>,
    ) -> Result<Vec<Value>, EngineError> {
        let path = self.coll_path(db, coll)?;
        if let Some(f) = filter {
            if !f.is_empty() {
                // With a filter, collect all matching docs then return the last n
                let all = scan(&path, None, 0, Some(f))?;
                let start = all.len().saturating_sub(n);
                return Ok(all.into_iter().skip(start).collect());
            }
        }
        let total  = count_lines(&path)?;
        let offset = total.saturating_sub(n);
        scan(&path, Some(n), offset, None)
    }

    pub fn stats(&self, db: &str, coll: &str) -> Result<CollStats, EngineError> {
        let path = self.coll_path(db, coll)?;
        let meta = match fs::metadata(&path) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(CollStats::default()),
            Err(e) => return Err(e.into()),
            Ok(m)  => m,
        };
        let count = count_lines(&path)?;
        Ok(CollStats {
            exists:     true,
            count,
            size_bytes: meta.len(),
            size_mb:    (meta.len() as f64) / 1_048_576.0,
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

    pub fn list_collections(&self, db: &str) -> Result<Vec<String>, EngineError> {
        validate_name(db)?;
        let dir = self.root.join(db);
        let rd = match fs::read_dir(&dir) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(e.into()),
            Ok(rd) => rd,
        };
        let mut out = vec![];
        for e in rd {
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
            let _ = h.lock().unwrap_or_else(|e| e.into_inner()).flush();
        }
        let path = self.coll_path(db, coll)?;
        match fs::remove_file(&path) {
            Ok(())                                         => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e)                                         => Err(e.into()),
        }
    }

    pub fn drop_database(&self, db: &str) -> Result<usize, EngineError> {
        validate_name(db)?;
        let dir = self.root.join(db);
        let rd = match fs::read_dir(&dir) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e.into()),
            Ok(rd) => rd,
        };
        self.handles.retain(|(d, _), h| {
            if d == db {
                let _ = h.lock().unwrap_or_else(|e| e.into_inner()).flush();
                false
            } else {
                true
            }
        });
        let mut count = 0;
        for e in rd {
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
}

// ── helpers ───────────────────────────────────────────────────────────────────

pub fn validate_name(name: &str) -> Result<(), EngineError> {
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
    // _ts stored as string to preserve nanosecond precision past JSON's 2^53 safe integer limit
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .to_string();
    if let Value::Object(ref mut m) = doc {
        m.insert("_id".into(), Value::String(id.clone()));
        m.insert("_ts".into(), Value::String(ts));
    }
    let mut s = serde_json::to_string(&doc)?;
    if s.len() > MAX_DOC { return Err(EngineError::TooLarge); }
    s.push('\n');
    Ok((id, s))
}

/// Full scan with optional field-equality filter (AND semantics).
/// Offset counts only documents that pass the filter.
fn scan(
    path: &Path,
    n: Option<usize>,
    offset: usize,
    filter: Option<&HashMap<String, Value>>,
) -> Result<Vec<Value>, EngineError> {
    let file = match File::open(path) {
        Ok(f)  => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(vec![]),
        Err(e) => return Err(e.into()),
    };
    let reader = BufReader::with_capacity(READ_BUF, file);
    let mut docs    = Vec::new();
    let mut skipped = 0usize;

    for line in reader.lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() { continue; }
        let doc: Value = serde_json::from_str(line)?;
        if let Some(f) = filter {
            if !matches_filter(&doc, f) { continue; }
        }
        if skipped < offset { skipped += 1; continue; }
        docs.push(doc);
        if let Some(limit) = n {
            if docs.len() >= limit { break; }
        }
    }
    Ok(docs)
}

/// Returns true if all filter key=value pairs match the document (AND semantics).
fn matches_filter(doc: &Value, filter: &HashMap<String, Value>) -> bool {
    let Value::Object(map) = doc else { return false };
    filter.iter().all(|(k, v)| map.get(k) == Some(v))
}

fn count_lines(path: &Path) -> io::Result<usize> {
    let mut file = match File::open(path) {
        Ok(f)  => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    let mut buf   = vec![0u8; READ_BUF];
    let mut count = 0usize;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 { break; }
        count += memchr_iter(b'\n', &buf[..n]).count();
    }
    Ok(count)
}
