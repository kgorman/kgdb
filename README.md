# KGDB — Append-Only JSON Document Database in Rust

**KGDB** is a lightweight, embeddable JSON document database written in Rust. It stores documents as NDJSON log files, exposes a simple REST API, and is designed for applications that need fast writes, human-readable storage, and zero operational overhead.

> **Single binary. No dependencies. 30,000 inserts/sec.**

---

## Why KGDB?

- **Append-only writes** — sequential disk I/O, no write amplification, crash-safe by design
- **NDJSON on disk** — every collection is a plain `.ndjson` file you can `grep`, `tail`, `wc -l`, or back up with `cp`
- **REST API** — insert, query, and manage data with plain HTTP; no driver, no query language
- **Field equality filters** — `?where={"status":"active"}` with automatic index acceleration
- **In-memory hash indexes** — O(1) point lookups; persist across restarts via sidecar files
- **Batch inserts** — up to 10,000 documents and 128 MB per request
- **Bearer token auth** — single shared secret via `KGDB_AUTH_TOKEN`
- **Written in Rust** — memory-safe, no GC pauses, ~30k req/sec single-doc inserts on commodity hardware

---

## Quick Start

```bash
# Build
cargo build --release

# Run (auth disabled for local dev)
KGDB_DATA=./data ./target/release/kgdb

# Insert a document
curl -X POST http://127.0.0.1:8000/v1/mydb/users \
  -H 'Content-Type: application/json' \
  -d '{"name":"Alice","role":"admin"}'
# → {"inserted":1,"_id":"0196..."}

# Query with a filter
curl -g 'http://127.0.0.1:8000/v1/mydb/users/find?where={"role":"admin"}'

# Tail the last 10 inserts
curl 'http://127.0.0.1:8000/v1/mydb/users/tail?n=10'
```

---

## Features

| Feature | Details |
|---------|---------|
| Storage format | NDJSON (one file per collection) |
| Query | Field equality filter, paginated scan, tail |
| Indexing | In-memory hash index, O(1) point lookup, persisted |
| Writes | Single-doc insert, batch insert (10k docs / 128 MB) |
| Auth | Bearer token (`KGDB_AUTH_TOKEN`) |
| Document limit | 16 MB per document |
| ID format | 36-char hex: timestamp + counter + random |
| Protocol | HTTP/1.1 REST, JSON bodies |
| Runtime deps | None — single static binary |

---

## Build

Requires Rust 1.75+.

```bash
cargo build --release
./target/release/kgdb
```

---

## Configuration

All configuration via environment variables.

| Variable                      | Default     | Description |
|-------------------------------|-------------|-------------|
| `KGDB_DATA`                   | `./data`    | Directory where database files are stored |
| `KGDB_PORT`                   | `8000`      | HTTP listen port |
| `KGDB_BIND`                   | `127.0.0.1` | Bind address (`0.0.0.0` to expose on all interfaces) |
| `KGDB_AUTH_TOKEN`             | _(unset)_   | Global bearer token — required on all requests when set |
| `KGDB_AUTH_TOKEN_<dbname>`    | _(unset)_   | Per-database bearer token — required only for that database |
| `KGDB_FSYNC`                  | `0`         | Set to `1` to fsync on every write |
| `RUST_LOG`                    | `info`      | Log filter (e.g. `debug`, `kgdb=trace`) |

```bash
KGDB_DATA=/var/kgdb \
  KGDB_PORT=9000 \
  KGDB_BIND=0.0.0.0 \
  KGDB_AUTH_TOKEN=$(openssl rand -hex 32) \
  ./kgdb
```

---

## Authentication

### Global token

Set `KGDB_AUTH_TOKEN` to require a bearer token on all requests:

```bash
KGDB_AUTH_TOKEN=$(openssl rand -hex 32) ./kgdb

curl -H 'Authorization: Bearer <token>' \
  http://127.0.0.1:8000/v1/mydb/users/find
```

### Per-database tokens

Set `KGDB_AUTH_TOKEN_<dbname>` to require a token for a specific database only. Other databases remain open unless they have their own token or a global token is set.

```bash
# Only the "logs" database requires auth; everything else is open
KGDB_AUTH_TOKEN_logs=$(openssl rand -hex 32) ./kgdb

curl -H 'Authorization: Bearer <logs-token>' \
  http://127.0.0.1:8000/v1/logs/events/find

# Requests to /v1/metrics/... require no token
curl http://127.0.0.1:8000/v1/metrics/cpu/find
```

Both global and per-db tokens are accepted for a database that has both configured.

Wrong or missing token → `401 {"error":"unauthorized"}`.
`/health` is always unauthenticated for load-balancer probes.

Generate a token: `openssl rand -hex 32`

---

## API Reference

Database and collection names: alphanumeric, `_`, `-`, max 128 chars.

### Server

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Server info + list of databases |
| `GET` | `/health` | Health check (unauthenticated) |

### Database

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/v1/:db` | List collections |
| `DELETE` | `/v1/:db` | Drop database and all collections |

### Collection — Writes

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/v1/:db/:coll` | Insert one document |
| `POST` | `/v1/:db/:coll/batch` | Insert many (JSON array, max 10k docs) |
| `DELETE` | `/v1/:db/:coll` | Drop collection |

### Collection — Reads

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/v1/:db/:coll/find` | Paginated scan with optional filter (query params) |
| `POST` | `/v1/:db/:coll/find` | Paginated scan with filter as JSON body |
| `GET` | `/v1/:db/:coll/tail` | Last N documents with optional filter (query params) |
| `POST` | `/v1/:db/:coll/tail` | Last N documents with filter as JSON body |
| `GET` | `/v1/:db/:coll/stats` | Document count, file size |

#### GET `/find` and `/tail` — query parameters

| Param | Default | Description |
|-------|---------|-------------|
| `n` | `20` | Documents to return (1–100,000) |
| `offset` | `0` | Documents to skip — `/find` only |
| `where` | — | JSON field equality filter: `{"field":"value"}` |

#### POST `/find` and POST `/tail` — JSON body

MongoDB-style: send a flat JSON object. `n` and `offset` are reserved control fields; every other key is an implicit filter field. No wrapper key needed.

```json
{ "role": "admin", "status": "active", "n": 50, "offset": 0 }
```

| Field | Default | Description |
|-------|---------|-------------|
| `n` | `20` | Documents to return (1–100,000) |
| `offset` | `0` | Documents to skip — `/find` only |
| _(any other key)_ | — | Implicit filter — equality match on that field |

### Indexes

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/v1/:db/:coll/index` | Create field index — body: `{"field":"name"}` |
| `GET` | `/v1/:db/:coll/indexes` | List indexed fields |
| `DELETE` | `/v1/:db/:coll/index/:field` | Drop index |

---

## Filtering and Indexing

Filter any read with `?where={"field":"value"}`. Multiple fields use AND semantics.

```bash
# Unindexed — full sequential scan
curl -g 'http://127.0.0.1:8000/v1/mydb/events/find?where={"type":"error"}&n=50'

# Create an index first
curl -X POST http://127.0.0.1:8000/v1/mydb/events/index \
  -H 'Content-Type: application/json' -d '{"field":"type"}'

# Now the same query uses O(1) index lookup
curl -g 'http://127.0.0.1:8000/v1/mydb/events/find?where={"type":"error"}&n=50'
```

Indexes are built in memory at startup from `.index.json` sidecar files. Live inserts update indexes immediately.

---

## Examples

```bash
# Insert one document
curl -X POST http://localhost:8000/v1/mydb/users \
  -H 'Content-Type: application/json' \
  -d '{"name":"Alice","role":"admin","email":"alice@example.com"}'

# Batch insert
curl -X POST http://localhost:8000/v1/mydb/users/batch \
  -H 'Content-Type: application/json' \
  -d '[{"name":"Bob","role":"user"},{"name":"Carol","role":"admin"}]'

# Filter by field value (GET — URL-encoded)
curl -g 'http://localhost:8000/v1/mydb/users/find?where={"role":"admin"}'

# Filter by field value (POST — flat body, MongoDB-style, no URL-encoding)
curl -X POST http://localhost:8000/v1/mydb/users/find \
  -H 'Content-Type: application/json' \
  -d '{"role":"admin","n":50}'

# Save a query to a file and reuse it
echo '{"status":"active","role":"admin","n":100}' > query.json
curl -X POST http://localhost:8000/v1/mydb/users/find \
  -H 'Content-Type: application/json' -d @query.json

# Paginate
curl 'http://localhost:8000/v1/mydb/users/find?n=20&offset=40'

# Last 10 documents
curl 'http://localhost:8000/v1/mydb/users/tail?n=10'

# Last 100 error-level events (POST — flat body)
curl -X POST http://localhost:8000/v1/mydb/events/tail \
  -H 'Content-Type: application/json' \
  -d '{"level":"error","n":100}'

# Stats
curl http://localhost:8000/v1/mydb/users/stats

# Create index
curl -X POST http://localhost:8000/v1/mydb/users/index \
  -H 'Content-Type: application/json' -d '{"field":"role"}'

# Drop collection / database
curl -X DELETE http://localhost:8000/v1/mydb/users
curl -X DELETE http://localhost:8000/v1/mydb
```

---

## Document Format

Every document is stored as a single NDJSON line with two injected fields:

```json
{"name":"Alice","role":"admin","_id":"0196a3f00e2400420000e1b23c9d7f88","_ts":"1715000000000000000"}
```

- `_id` — 36-char hex: `[16 timestamp_ns][4 counter][16 random]` — sortable, unique, unforgeable
- `_ts` — nanosecond Unix timestamp as a string (avoids IEEE 754 precision loss in JS)

---

## Data Files

```
data/
  mydb/
    users.ndjson          ← one line per document
    users.index.json      ← index sidecar (which fields are indexed)
    events.ndjson
```

Inspect with standard Unix tools:

```bash
tail -f data/mydb/events.ndjson
wc -l data/mydb/users.ndjson
grep '"role":"admin"' data/mydb/users.ndjson
```

---

## Performance

Benchmarked on macOS (Apple M-series), release build, loopback, 50 concurrent connections.

| Operation | Throughput | Latency (mean) |
|-----------|------------|----------------|
| Single-doc insert | **~30,000 req/sec** | 0.033 ms |
| Batch insert (10k docs) | **~175,000 docs/sec** | 57 ms/batch |
| Indexed point lookup | **~6,600 req/sec** | 0.15 ms |
| Tail 100 from 100k-doc collection | **~2,400 req/sec** | 0.42 ms |

Writes are sequential appends — no write amplification, no compaction stalls. Indexed reads are O(1) via in-memory hash lookup + file seek. Unindexed reads are O(n) sequential scans.

---

## Compared to Alternatives

| | KGDB | SQLite (JSON) | MongoDB | DuckDB |
|--|------|--------------|---------|--------|
| Storage format | NDJSON plaintext | Binary | BSON | Columnar binary |
| Human-readable data | ✅ | ❌ | ❌ | ❌ |
| Append-only writes | ✅ | ❌ | ❌ | ❌ |
| REST API built-in | ✅ | ❌ | ✅ | ❌ |
| Single binary | ✅ | ✅ | ❌ | ✅ |
| Query language | Equality filter | SQL | MQL | SQL |
| Best for | Logs, events, IoT, AI agent memory | Relational data | General purpose | Analytics |

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).
