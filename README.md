# KGDB

A lightweight, append-only JSON document database written in Rust. Documents are stored as NDJSON log files — one file per collection — making the data human-readable and trivially portable.

## Features

- **Append-only storage** — writes are fast, sequential, and crash-safe
- **NDJSON on disk** — each collection is a plain `.ndjson` file; grep it, tail it, copy it
- **REST API** — simple HTTP interface, no query language to learn
- **Batch inserts** — up to 10,000 documents per request
- **Auto-assigned IDs** — every document gets a `_id` (timestamp + counter + random) and `_ts` (nanosecond epoch)
- **16 MB document limit** — matches familiar conventions
- **Zero dependencies at runtime** — single binary

## Build

Requires Rust 1.75+.

```bash
cargo build --release
./target/release/kgdb
```

## Configuration

All configuration is via environment variables.

| Variable    | Default  | Description                              |
|-------------|----------|------------------------------------------|
| `KGDB_DATA` | `./data` | Directory where database files are stored |
| `KGDB_PORT` | `8000`   | HTTP listen port                         |
| `KGDB_FSYNC`| `0`      | Set to `1` to fsync on every write       |
| `RUST_LOG`  | `info`   | Log filter (e.g. `debug`, `kgdb=trace`)  |

```bash
KGDB_DATA=/var/kgdb KGDB_PORT=9000 ./kgdb
```

## API

All endpoints are under `/v1/{db}/{collection}`. Database and collection names must be alphanumeric with `_` and `-` allowed, max 128 characters.

### Server

| Method | Path      | Description           |
|--------|-----------|-----------------------|
| `GET`  | `/`       | Server info + database list |
| `GET`  | `/health` | Health check          |

### Database

| Method   | Path      | Description                    |
|----------|-----------|--------------------------------|
| `GET`    | `/v1/:db` | List collections in a database |
| `DELETE` | `/v1/:db` | Drop a database and all its collections |

### Collection

| Method   | Path                    | Description                        |
|----------|-------------------------|------------------------------------|
| `POST`   | `/v1/:db/:coll`         | Insert one document                |
| `POST`   | `/v1/:db/:coll/batch`   | Insert many documents (array)      |
| `GET`    | `/v1/:db/:coll/find`    | Scan documents (paginated)         |
| `GET`    | `/v1/:db/:coll/tail`    | Read the last N documents          |
| `GET`    | `/v1/:db/:coll/stats`   | Collection stats                   |
| `DELETE` | `/v1/:db/:coll`         | Drop a collection                  |

#### Query parameters

**`/find`**

| Param    | Default | Description                    |
|----------|---------|--------------------------------|
| `n`      | `20`    | Max documents to return (1–100000) |
| `offset` | `0`     | Number of documents to skip    |

**`/tail`**

| Param | Default | Description                    |
|-------|---------|--------------------------------|
| `n`   | `20`    | Number of documents from the end (1–100000) |

## Examples

```bash
# Insert a document
curl -s -X POST http://localhost:8000/v1/mydb/users \
  -H 'Content-Type: application/json' \
  -d '{"name": "Alice", "email": "alice@example.com"}'

# {"inserted":1,"_id":"0000019612a4b3e0001a7f42"}

# Insert many documents
curl -s -X POST http://localhost:8000/v1/mydb/users/batch \
  -H 'Content-Type: application/json' \
  -d '[{"name":"Bob"},{"name":"Carol"}]'

# Fetch first 5 documents
curl -s 'http://localhost:8000/v1/mydb/users/find?n=5'

# Fetch with offset (page 2)
curl -s 'http://localhost:8000/v1/mydb/users/find?n=20&offset=20'

# Tail the last 10 documents
curl -s 'http://localhost:8000/v1/mydb/users/tail?n=10'

# Collection stats
curl -s http://localhost:8000/v1/mydb/users/stats

# List databases
curl -s http://localhost:8000/

# List collections in a database
curl -s http://localhost:8000/v1/mydb

# Drop a collection
curl -s -X DELETE http://localhost:8000/v1/mydb/users

# Drop a database
curl -s -X DELETE http://localhost:8000/v1/mydb
```

## Document format

Documents are stored as NDJSON with two auto-added fields:

```json
{"name":"Alice","email":"alice@example.com","_id":"0000019612a4b3e0001a7f42","_ts":1715000000000000000}
```

- `_id` — 24-character hex string: `[16 hex timestamp_ns][4 hex counter][4 hex random]`
- `_ts` — Unix timestamp in nanoseconds

These fields are injected at insert time and cannot be overridden.

## Data files

Each collection is stored as a single `.ndjson` file:

```
data/
  mydb/
    users.ndjson
    events.ndjson
```

You can inspect, tail, or back up collections with standard Unix tools:

```bash
tail -f data/mydb/events.ndjson
wc -l data/mydb/users.ndjson
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
