# DB-RAIDr

**DB-RAIDr** is a lock-free, zero-downtime MySQL-to-PostgreSQL migration and Change Data Capture (CDC) tool implemented in Python. 

It implements the [DBLog watermark algorithm](https://arxiv.org/abs/2010.12597) to allow full snapshot backfills of large tables concurrent with live binlog replication—without requiring global read locks, table locks, or long-lived snapshot transactions.

---

## Key Features

- **Lock-Free Chunked Backfill:** Backfills tables chunk-by-chunk using cursor pagination on primary or unique keys.
- **DBLog Watermarking:** Inserts low and high watermark markers directly into a helper table in MySQL to reconcile snapshot rows against in-flight transactions.
- **Continuous CDC (Phase 2):** Seamlessly transitions from historical backfill to real-time binlog streaming.
- **Idempotent Delivery:** PostgreSQL targets receive idempotent writes (`INSERT ... ON CONFLICT DO UPDATE` and key-based `DELETE`), making crash recovery and restarts safe.
- **Composite Key & Binary Key Support:** Supports multi-column composite primary keys and arbitrary data types (including raw binary/UUIDs, JSON, BIT, SET).
- **Graceful Pausing & Resumption:** `Ctrl-C` finishes the current chunk and saves progress to disk. State files allow pausing and resuming without restarting backfills from scratch.
- **Secure Configuration:** Database passwords are read exclusively from environment variables or `.env` files, preventing secrets from being committed in configuration files.

---

## How It Works

DB-RAIDr operates in two distinct phases:

```
                  +----------------------------------------------+
                  |                 MySQL Source                 |
                  +----------------------------------------------+
                     /                                        \
       [Chunked SELECTs]                                   [Binlog Stream]
                   /                                            \
                  v                                              v
      +------------------------+                     +------------------------+
      |     Snapshot Chunk     |                     |   Live Change Events   |
      +------------------------+                     +------------------------+
                  \                                              /
                   \-----> [ Watermark Reconciliation ] <-------/
                                        |
                            (Drop superseded rows)
                                        |
                                        v
                         +-----------------------------+
                         |      PostgreSQL Target      |
                         +-----------------------------+
```

### The Watermark Algorithm (Phase 1: Backfill)

1. **Low Watermark:** DB-RAIDr inserts a unique UUID marker row into `dblog_watermarks`.
2. **Read Chunk:** DB-RAIDr executes a chunked `SELECT` for a batch of rows ordered by primary/unique keys (no locks held).
3. **High Watermark:** DB-RAIDr inserts a second UUID marker row into `dblog_watermarks`.
4. **Reconciliation Window:** While reading the binlog, change events are applied **unconditionally** to PostgreSQL. The binlog events that arrive between the low and high watermarks identify rows modified during the `SELECT`.
5. **Deduplication:** Any row returned in the snapshot chunk that was modified during the window is discarded, because the binlog event already applied contains fresher data.
6. **Upsert:** The remaining snapshot rows are written to PostgreSQL using bulk upserts.

### Continuous CDC (Phase 2: Follow)

Once all tables finish Phase 1, DB-RAIDr streams live `INSERT`, `UPDATE`, and `DELETE` events from the MySQL binlog directly into PostgreSQL indefinitely.

---

## Prerequisites

### 1. Python Environment

DB-RAIDr requires Python 3.9+ and the following packages:

```bash
pip install mysql-replication pymysql psycopg python-dotenv
```

> **Note:** If `psycopg` (v3) fails to locate PostgreSQL client libraries (`libpq`) on your machine, install the pre-compiled binary wheel instead:
> ```bash
> pip install "psycopg[binary]"
> ```

### 2. MySQL Server Configuration

The MySQL source instance must have binary logging enabled with full row images and metadata. Add or verify the following settings in your MySQL configuration (`my.cnf` / `mysqld.cnf`):

```ini
[mysqld]
binlog_format            = ROW
binlog_row_image         = FULL
binlog_row_metadata      = FULL        # Required for MySQL 8.0.14+
binlog_row_value_options = ""          # Must NOT be PARTIAL_JSON
```

Verify these settings dynamically in MySQL:
```sql
SELECT @@GLOBAL.binlog_format,
       @@GLOBAL.binlog_row_image,
       @@GLOBAL.binlog_row_metadata,
       @@GLOBAL.binlog_row_value_options;
```

### 3. Watermark Table

DB-RAIDr requires a small InnoDB table to write low and high watermarks. **This table must exist in the same database/schema as the tables being migrated:**

```sql
CREATE TABLE dblog_watermarks (
    id VARCHAR(64) PRIMARY KEY
) ENGINE=InnoDB;
```

### 4. Table Schema Requirements

- **Primary / Unique Keys:** Every table in the migration list must have `key_columns` configured. These columns must be `NOT NULL` in MySQL and must have a matching unique or primary key index on both MySQL and PostgreSQL.
- **Target Tables:** Target tables in PostgreSQL must already exist with the desired schemas and indexes before starting the backfill.

---

## Configuration

DB-RAIDr uses a dedicated workspace directory for each migration project. The workspace holds the `config.json` file and the persistent state file (`dblog_state.json`).

### Workspace Layout

```
my-migration/
├── .env                  # Optional: contains database passwords
├── config.json           # Migration settings & table mappings
└── dblog_state.json      # Auto-generated runtime state
```

### Environment Variables (`.env`)

Passwords are never placed in `config.json`. Define environment variables corresponding to the keys specified in `password_env`:

```env
SHOP_MYSQL_PW="source_secret_password"
SHOP_PG_PW="target_secret_password"
```

### `config.json` Reference

Create a `config.json` inside your workspace directory:

```json
{
  "mysql": {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "repl",
    "db": "shop",
    "password_env": "SHOP_MYSQL_PW"
  },
  "postgres": {
    "host": "127.0.0.1",
    "port": 5432,
    "user": "repl",
    "db": "shop",
    "password_env": "SHOP_PG_PW"
  },
  "chunk_size": 1000,
  "server_id": 100,
  "watermark_table": "dblog_watermarks",
  "watermark_timeout_seconds": 120,
  "heartbeat_seconds": 10,
  "state_file": "dblog_state.json",
  "tables": [
    {
      "mysql_table": "users",
      "pg_table": "public.users",
      "key_columns": ["id"]
    },
    {
      "mysql_table": "documents",
      "pg_table": "public.documents",
      "key_columns": ["tenant_id", "content_sha256"],
      "skip_columns": ["search_vector"]
    }
  ]
}
```

#### Configuration Options

| Field | Type | Default | Description |
|---|---|---|---|
| `mysql` | `object` | *required* | Source connection parameters (`host`, `port`, `user`, `db`, `password_env`). |
| `postgres` | `object` | *required* | Target connection parameters (`host`, `port`, `user`, `db`, `password_env`). |
| `chunk_size` | `integer` | `1000` | Number of rows to read per snapshot `SELECT` chunk. |
| `server_id` | `integer` | `100` | Unique slave ID presented by the binlog client to MySQL. |
| `watermark_table`| `string` | `"dblog_watermarks"` | Name of the watermark table created in the MySQL schema. |
| `watermark_timeout_seconds` | `number` | `120` | Max seconds of binlog inactivity before a chunk window times out. |
| `heartbeat_seconds` | `number` | `10` | Binlog client heartbeat interval to maintain connection on idle servers. |
| `state_file` | `string` | `"dblog_state.json"` | Name of the checkpoint file written inside the workspace directory. |
| `tables` | `array` | *required* | List of table mapping definitions (see below). |

#### Table Mapping Fields

- `mysql_table`: Name of the source table in MySQL.
- `pg_table`: Fully-qualified name of the target table in PostgreSQL (e.g., `"public.users"`).
- `key_columns`: Array of column names comprising the primary or unique key. Used for ordering chunks and constructing conflict clauses.
- `skip_columns`: *(Optional)* Array of columns to exclude from migration (e.g., generated columns or search vectors).

---

## Usage

Run the migration script by pointing it at your workspace folder:

```bash
python db-raidr.py ./my-migration
```

### Lifecycle and Controls

- **Starting Fresh:** If no state file exists, DB-RAIDr begins by recording the current binlog position, backfills each table in the order specified in `config.json`, and enters continuous replication mode.
- **Graceful Shutdown (`Ctrl-C` once):** Requests the worker to exit cleanly. The current chunk or transaction completes, positions are committed to PostgreSQL, the state file is flushed atomically, and the process exits.
- **Force Exit (`Ctrl-C` twice):** Terminates immediately. Note that state may lag slightly, though subsequent runs will self-heal due to idempotent writes.
- **Resuming:** Re-run the command with the same workspace directory. DB-RAIDr will detect `dblog_state.json` and resume exactly where it stopped.

---

## Type Conversions & Handling

DB-RAIDr inspects MySQL's `information_schema` at runtime to handle tricky type conversions automatically:

| MySQL Type | Handling & PostgreSQL Mapping |
|---|---|
| `BINARY`, `VARBINARY`, `*BLOB` | Converted to raw `bytes` and mapped to PostgreSQL `BYTEA`. In key identity comparisons, raw bytes are hex-encoded. |
| `BIT` | Normalized to integer representation across both snapshot reads and binlog events. |
| `JSON` | Sanitized to canonical JSON text and delivered to PostgreSQL `json` / `jsonb`. |
| `SET` | Decoded and deterministically sorted to maintain consistency regardless of read route. |
| `TIME` (`timedelta`) | Formatted as an ISO-compatible time string. |
| Key Updates | If an `UPDATE` modifies primary key column values, DB-RAIDr issues an explicit `DELETE` for the old key before upserting the new row. |

---

## Limitations & Best Practices

1. **DDL Handling:** Source DDL operations (`CREATE`, `ALTER`, `DROP`, `TRUNCATE`, `RENAME`) encountered during the binlog stream are not replayed and will halt the process with an error. Apply schema changes manually to both engines.
2. **Key Mutability:** Avoid changing `key_columns` in `config.json` after a backfill has started. If key definitions change, delete `dblog_state.json` and restart the migration.
3. **Binlog Retention:** Ensure the MySQL server retains binary logs long enough to cover any planned downtime or slow backfill runs.