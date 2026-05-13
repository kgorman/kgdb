//! KGDB storage engine — append-only NDJSON log.
//!
//! One file per collection: `{data_dir}/{db}/{coll}.ndjson`
//! File handles kept warm in a DashMap; writes are a single write_all() per batch.

use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
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

/// Writer state: BufWriter + in-memory byte offset (avoids flush+stat per write).
struct WriterState {
    writer: BufWriter<File>,
    offset: u64,
}

type Handle = Arc<Mutex<WriterState>>;

/// field_name -> value_as_json_string -> sorted Vec<byte_offset>
type FieldIndex = DashMap<String, Vec<u64>>;
/// field_name -> FieldIndex
type CollIndexes = DashMap<String, FieldIndex>;

pub struct Engine {
    root:    PathBuf,
    fsync:   bool,
    handles: DashMap<(String, String), Handle>,
    indexes: DashMap<(String, String), CollIndexes>,
}

impl Engine {
    pub fn new(root: impl Into<PathBuf>, fsync: bool) -> io::Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        let eng = Self { root, fsync, handles: DashMap::new(), indexes: DashMap::new() };
        eng.rebuild_indexes()?;
        Ok(eng)
    }

    // ── internals ─────────────────────────────────────────────────────────────

    fn coll_path(&self, db: &str, coll: &str) -> Result<PathBuf, EngineError> {
        validate_name(db)?;
        validate_name(coll)?;
        let dir = self.root.join(db);
        fs::create_dir_all(&dir)?;
        Ok(dir.join(format!("{coll}.ndjson")))
    }

    fn sidecar_path(&self, db: &str, coll: &str) -> PathBuf {
        self.root.join(db).join(format!("{coll}.index.json"))
    }

    fn save_sidecar(&self, db: &str, coll: &str) -> io::Result<()> {
        let key = (db.to_owned(), coll.to_owned());
        let mut fields: Vec<String> = if let Some(coll_indexes) = self.indexes.get(&key) {
            coll_indexes.iter().map(|e| e.key().clone()).collect()
        } else {
            vec![]
        };
        fields.sort();
        let path = self.sidecar_path(db, coll);
        let json = serde_json::json!({ "fields": fields });
        let s = serde_json::to_string(&json)?;
        // Create parent dir if needed
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        if fields.is_empty() {
            // Remove sidecar if no indexes remain
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        } else {
            fs::write(&path, s)?;
        }
        Ok(())
    }

    fn rebuild_indexes(&self) -> io::Result<()> {
        // Walk data dir looking for *.index.json files
        let rd = match fs::read_dir(&self.root) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
            Ok(rd) => rd,
        };
        for db_entry in rd {
            let db_entry = db_entry?;
            if !db_entry.file_type()?.is_dir() {
                continue;
            }
            let db_name = db_entry.file_name().to_string_lossy().into_owned();
            let db_rd = match fs::read_dir(db_entry.path()) {
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
                Ok(rd) => rd,
            };
            for coll_entry in db_rd {
                let coll_entry = coll_entry?;
                let fname = coll_entry.file_name().to_string_lossy().into_owned();
                let coll_name = match fname.strip_suffix(".index.json") {
                    Some(n) => n.to_owned(),
                    None => continue,
                };
                // Read the sidecar
                let sidecar_path = coll_entry.path();
                let content = match fs::read_to_string(&sidecar_path) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let sidecar: serde_json::Value = match serde_json::from_str(&content) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let fields = match sidecar.get("fields").and_then(|v| v.as_array()) {
                    Some(f) => f.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_owned()))
                        .collect::<Vec<_>>(),
                    None => continue,
                };
                let data_path = self.root.join(&db_name).join(format!("{coll_name}.ndjson"));
                let key = (db_name.clone(), coll_name.clone());
                let coll_indexes: CollIndexes = DashMap::new();
                for field in &fields {
                    match build_field_index(&data_path, field) {
                        Ok(fi) => { coll_indexes.insert(field.clone(), fi); }
                        Err(_) => continue,
                    }
                }
                self.indexes.insert(key, coll_indexes);
            }
        }
        Ok(())
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
            let offset = file.metadata()?.len();
            let state = WriterState { writer: BufWriter::with_capacity(WRITE_BUF, file), offset };
            Ok::<Handle, EngineError>(Arc::new(Mutex::new(state)))
        })?;
        Ok(Arc::clone(&*entry))
    }

    fn update_indexes_for_doc(&self, db: &str, coll: &str, doc: &Value, offset: u64) {
        let key = (db.to_owned(), coll.to_owned());
        if let Some(coll_indexes) = self.indexes.get(&key) {
            for field_entry in coll_indexes.iter() {
                let field = field_entry.key();
                let field_index = field_entry.value();
                if let Some(val) = doc.get(field) {
                    let val_str = serde_json::to_string(val).unwrap_or_default();
                    field_index.entry(val_str).or_default().push(offset);
                }
            }
        }
    }

    // ── writes ────────────────────────────────────────────────────────────────

    pub fn insert(&self, db: &str, coll: &str, doc: Value) -> Result<String, EngineError> {
        let (id, line, annotated_doc) = annotate(doc)?;
        let handle = self.get_handle(db, coll)?;
        let mut st = handle.lock().unwrap_or_else(|e| e.into_inner());
        let offset = st.offset;
        st.writer.write_all(line.as_bytes())?;
        st.offset += line.len() as u64;
        if self.fsync {
            st.writer.flush()?;
            st.writer.get_ref().sync_data()?;
        }
        drop(st);
        self.update_indexes_for_doc(db, coll, &annotated_doc, offset);
        Ok(id)
    }

    pub fn insert_many(&self, db: &str, coll: &str, docs: Vec<Value>) -> Result<Vec<String>, EngineError> {
        if docs.is_empty() {
            return Ok(vec![]);
        }
        let mut ids  = Vec::with_capacity(docs.len());
        let mut blob = Vec::new();
        // Track (annotated_doc, relative_offset_in_blob)
        let mut doc_offsets: Vec<(Value, u64)> = Vec::with_capacity(docs.len());

        for doc in docs {
            let rel_offset = blob.len() as u64;
            let (id, line, annotated_doc) = annotate(doc)?;
            blob.extend_from_slice(line.as_bytes());
            if blob.len() > MAX_BATCH {
                return Err(EngineError::BatchTooLarge);
            }
            ids.push(id);
            doc_offsets.push((annotated_doc, rel_offset));
        }

        let handle = self.get_handle(db, coll)?;
        let mut st = handle.lock().unwrap_or_else(|e| e.into_inner());
        let base = st.offset;
        st.writer.write_all(&blob)?;
        st.offset += blob.len() as u64;
        if self.fsync {
            st.writer.flush()?;
            st.writer.get_ref().sync_data()?;
        }
        drop(st);

        for (doc, rel_offset) in doc_offsets {
            self.update_indexes_for_doc(db, coll, &doc, base + rel_offset);
        }

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

        // Try index-accelerated path
        if let Some(f) = filter {
            if !f.is_empty() {
                let key = (db.to_owned(), coll.to_owned());
                if let Some(coll_indexes) = self.indexes.get(&key) {
                    // Check if ALL filter fields are indexed
                    let all_indexed = f.keys().all(|k| coll_indexes.contains_key(k));
                    if all_indexed {
                        // For each filter field, collect matching offsets
                        let mut offset_sets: Vec<HashSet<u64>> = Vec::new();
                        for (field, val) in f {
                            let val_str = serde_json::to_string(val)?;
                            let offsets_for_field: HashSet<u64> =
                                if let Some(field_index) = coll_indexes.get(field) {
                                    if let Some(offsets) = field_index.get(&val_str) {
                                        offsets.iter().copied().collect()
                                    } else {
                                        HashSet::new()
                                    }
                                } else {
                                    HashSet::new()
                                };
                            offset_sets.push(offsets_for_field);
                        }
                        // Intersect all offset sets
                        let mut intersected: HashSet<u64> = if offset_sets.is_empty() {
                            HashSet::new()
                        } else {
                            offset_sets[0].clone()
                        };
                        for s in &offset_sets[1..] {
                            intersected = intersected.intersection(s).copied().collect();
                        }
                        // Sort offsets
                        let mut sorted_offsets: Vec<u64> = intersected.into_iter().collect();
                        sorted_offsets.sort_unstable();
                        // Apply pagination (offset/n over matching docs)
                        let paginated: Vec<u64> = sorted_offsets
                            .into_iter()
                            .skip(offset)
                            .take(n)
                            .collect();
                        return read_at_offsets(&path, &paginated);
                    }
                }
            }
        }

        // Fall back to full scan
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

    // ── index management ──────────────────────────────────────────────────────

    pub fn create_index(&self, db: &str, coll: &str, field: &str) -> Result<(), EngineError> {
        validate_name(db)?;
        validate_name(coll)?;
        validate_name(field)?;
        let key = (db.to_owned(), coll.to_owned());
        // Check if already indexed
        if let Some(coll_indexes) = self.indexes.get(&key) {
            if coll_indexes.contains_key(field) {
                return Ok(());
            }
        }
        let data_path = self.root.join(db).join(format!("{coll}.ndjson"));
        let fi = build_field_index(&data_path, field)?;
        self.indexes
            .entry(key)
            .or_insert_with(DashMap::new)
            .insert(field.to_owned(), fi);
        self.save_sidecar(db, coll)?;
        Ok(())
    }

    pub fn list_indexes(&self, db: &str, coll: &str) -> Result<Vec<String>, EngineError> {
        validate_name(db)?;
        validate_name(coll)?;
        let key = (db.to_owned(), coll.to_owned());
        let mut fields: Vec<String> = if let Some(coll_indexes) = self.indexes.get(&key) {
            coll_indexes.iter().map(|e| e.key().clone()).collect()
        } else {
            vec![]
        };
        fields.sort();
        Ok(fields)
    }

    pub fn drop_index(&self, db: &str, coll: &str, field: &str) -> Result<bool, EngineError> {
        validate_name(db)?;
        validate_name(coll)?;
        validate_name(field)?;
        let key = (db.to_owned(), coll.to_owned());
        let existed = if let Some(coll_indexes) = self.indexes.get(&key) {
            coll_indexes.remove(field).is_some()
        } else {
            false
        };
        self.save_sidecar(db, coll)?;
        Ok(existed)
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
            let _ = h.lock().unwrap_or_else(|e| e.into_inner()).writer.flush();
        }
        self.indexes.remove(&key);
        let sidecar = self.sidecar_path(db, coll);
        match fs::remove_file(&sidecar) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
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
                let _ = h.lock().unwrap_or_else(|e| e.into_inner()).writer.flush();
                false
            } else {
                true
            }
        });
        self.indexes.retain(|(d, _), _| d != db);
        let mut count = 0;
        for e in rd {
            let e = e?;
            let fname = e.file_name().to_string_lossy().into_owned();
            if fname.ends_with(".ndjson") {
                fs::remove_file(e.path())?;
                count += 1;
            } else if fname.ends_with(".index.json") {
                let _ = fs::remove_file(e.path());
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

/// Returns (id, ndjson_line, annotated_value).
/// Returning the Value avoids a re-parse in callers that need it for index updates.
fn annotate(mut doc: Value) -> Result<(String, String, Value), EngineError> {
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
    Ok((id, s, doc))
}

/// Build a FieldIndex for the given field by scanning the NDJSON file line by line,
/// tracking byte offset of each line.
fn build_field_index(path: &Path, field: &str) -> io::Result<FieldIndex> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(DashMap::new()),
        Err(e) => return Err(e),
    };
    let mut reader = BufReader::with_capacity(READ_BUF, file);
    let index: FieldIndex = DashMap::new();
    let mut offset: u64 = 0;
    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line)?;
        if n == 0 { break; }
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            if let Ok(doc) = serde_json::from_str::<Value>(trimmed) {
                if let Some(val) = doc.get(field) {
                    let val_str = serde_json::to_string(val)
                        .unwrap_or_default();
                    index.entry(val_str).or_default().push(offset);
                }
            }
        }
        offset += n as u64;
    }
    Ok(index)
}

/// Seek to each offset in the file, read one line, and deserialize it.
fn read_at_offsets(path: &Path, offsets: &[u64]) -> Result<Vec<Value>, EngineError> {
    if offsets.is_empty() {
        return Ok(vec![]);
    }
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(vec![]),
        Err(e) => return Err(e.into()),
    };
    let mut reader = BufReader::with_capacity(READ_BUF, file);
    let mut docs = Vec::with_capacity(offsets.len());
    let mut line = String::new();
    for &off in offsets {
        reader.seek(SeekFrom::Start(off))?;
        line.clear();
        let n = reader.read_line(&mut line)?;
        if n == 0 { continue; }
        let trimmed = line.trim();
        if trimmed.is_empty() { continue; }
        let doc: Value = serde_json::from_str(trimmed)?;
        docs.push(doc);
    }
    Ok(docs)
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
