#!/usr/bin/env python3
"""
MySQL -> PostgreSQL copy using the DBLog watermark algorithm.

    python db-raidr.py <workspace_dir>

The workspace_dir must contain a config.json (see below). It also doubles as
the script's scratch space: the state file and anything else the script saves
are written inside that same folder.

Chunked SELECTs from the source table are interleaved with the live binlog
stream. Before and after each chunk we insert a marker row ("watermark") into a
small MySQL table. Those markers come back through the binlog, which tells us
which changes happened *while* we were reading the chunk. Any snapshot row
whose key changed inside that window is dropped, because the change event we
already applied is newer. No table locks, no long snapshot transaction.

The rule that keeps this correct: change events are ALWAYS applied. Watermarks
only decide which *snapshot* rows to throw away.

Phase 1 backfills every table, then phase 2 follows the binlog until you stop
it. Ctrl-C finishes the current unit of work, saves position, and exits.
Postgres writes are idempotent (upsert by key, delete by key), so replaying a
few events after a crash is harmless.

Requirements
------------
    pip install mysql-replication pymysql psycopg

    psycopg (v3) is pure Python but loads libpq at runtime; if the import
    fails on this machine, install psycopg[binary] instead, which bundles it.

    binlog_format            = ROW
    binlog_row_image         = FULL
    binlog_row_metadata      = FULL     -- MySQL 8.0.14+
    binlog_row_value_options = ''       -- must not be PARTIAL_JSON

    CREATE TABLE dblog_watermarks (id VARCHAR(64) PRIMARY KEY) ENGINE=InnoDB;

The watermark table must live in the same schema as the migrated tables, or its
rows never reach the stream. Each table's key_columns must be NOT NULL and
backed by a unique index on both sides. NOT NULL is checked at startup; the
unique index is not, but without it rows get merged and pagination skips rows.

Passwords
---------
Passwords are never read from config.json, so the config can be committed.
Each database's password is taken from the environment at startup, from the
environment variable named in that database's "password_env" field:

config.json
-----------
    {
      "mysql": {"host": "127.0.0.1", "port": 3306,
                "user": "repl", "db": "shop",
                "password_env": "SHOP_MYSQL_PW"},
      "postgres": {"host": "127.0.0.1", "port": 5432,
                   "user": "repl", "db": "shop",
                   "password_env": "SHOP_PG_PW"},
      "chunk_size": 1000,
      "tables": [
        {"mysql_table": "documents",
         "pg_table": "public.documents",
         "key_columns": ["tenant_id", "content_sha256"],
         "skip_columns": ["search_vector"]}
      ]
    }

Column types are read from information_schema, so binary/blob/json/bit/set
columns do not need to be declared anywhere.
"""

from __future__ import annotations

import datetime
import decimal
import json
import logging
import os
import signal
import sys
import time
import uuid
from types import FrameType

import pymysql
import psycopg
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)
from pymysqlreplication.event import QueryEvent, XidEvent, HeartbeatLogEvent
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("dblog")

ROW_EVENTS = (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)
DDL_WORDS = ("CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME")
BINARY_TYPES = ("binary", "varbinary", "tinyblob", "blob", "mediumblob",
                "longblob", "geometry")

RowEvent = WriteRowsEvent | UpdateRowsEvent | DeleteRowsEvent
BinlogEvent = RowEvent | QueryEvent | XidEvent | HeartbeatLogEvent
EncodedValue = tuple[str, str]
RowDict = dict[str, object]

stop_requested = False


def read_password(env_var: str) -> str:
    """The password for a database, taken from the environment variable named
    in that database's "password_env" field in config.json.

    Kept out of config.json so the config is safe to commit. An empty value is
    accepted (some setups really do use one); an unset variable is not, since
    that is almost always a forgotten export rather than a deliberate choice.
    """
    password = os.environ.get(env_var)
    if password is None:
        raise RuntimeError(
            f"Failed reading password - environment variable '{env_var}' is not set")
    return password


def install_signal_handlers() -> None:
    """First Ctrl-C asks to stop, a second one forces it."""
    def handler(signum: int, frame: FrameType | None) -> None:
        global stop_requested
        if stop_requested:
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            raise KeyboardInterrupt
        stop_requested = True
        log.warning("interrupt - finishing current chunk, then exiting")
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


class WindowInterrupted(Exception):
    """A chunk's watermark window could not be completed."""


# ---------------------------------------------------------------------------
# Values
#
# The same row reaches us twice by two different routes: once from a SELECT
# (pymysql) and once from the binlog (mysql-replication). The two libraries do
# not return the same Python object for the same column, so every value is put
# into one canonical form based on what information_schema says the column is.
# ---------------------------------------------------------------------------

def to_text(value: object) -> object:
    """Decode bytes/bytearray to str; anything else passes through unchanged."""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8")
    return value


def to_bytes(value: object) -> bytes:
    """Encode a str to bytes; anything else is just coerced to bytes."""
    if isinstance(value, str):
        return value.encode("utf-8")
    return bytes(value)


def bit_to_int(value: object) -> object:
    """A SELECT returns BIT as bytes, the binlog returns it as an int."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (bytes, bytearray)):
        result = 0
        for byte in bytes(value):
            result = (result << 8) | byte
        return result
    return value


def set_to_text(value: object) -> str:
    """MySQL SET: a SELECT gives 'a,b', the binlog gives a Python set.

    Sorting both routes is the point - not MySQL's print order, but an order
    the two routes agree on, so a row does not change value depending on which
    way it arrived.
    """
    if isinstance(value, (bytes, bytearray, str)):
        text = to_text(value)
        members = text.split(",") if text else []
    else:
        members = [to_text(item) for item in value]
    return ",".join(sorted(members))


def clean_json(value: object) -> object:
    """Binlog JSON comes back parsed, and strings inside it can still be bytes."""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8")
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            result[to_text(key)] = clean_json(item)
        return result
    if isinstance(value, list):
        return [clean_json(item) for item in value]
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    return value


def json_to_text(value: object, from_binlog: bool) -> str:
    """A SELECT gives JSON already serialised; the binlog gives it parsed.

    A JSON document can be a bare scalar (42, "hello", true), so passing the
    binlog value straight through sends an int where jsonb is expected, or an
    unquoted hello that is not valid JSON. Either way the row is rejected.
    """
    if not from_binlog:
        return to_text(value)
    return json.dumps(clean_json(value))


def convert(value: object, kind: str, from_binlog: bool) -> object:
    """One MySQL value -> something psycopg can send."""
    if value is None:
        return None
    if kind == "binary":
        return to_bytes(value)                 # psycopg sends bytes as bytea
    if kind == "bit":
        return bit_to_int(value)
    if kind == "json":
        return json_to_text(value, from_binlog)
    if kind == "set":
        return set_to_text(value)
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8")
    if isinstance(value, datetime.timedelta):
        return str(value)                      # MySQL TIME
    return value


def convert_row(row: RowDict, table: Table, from_binlog: bool) -> RowDict:
    """Apply convert() to every column of a row, dropping skipped columns."""
    converted: RowDict = {}
    for column, value in row.items():
        if column not in table.skip_columns:
            converted[column] = convert(value, table.kind(column), from_binlog)
    return converted


# ---------------------------------------------------------------------------
# Keys
#
# This has to be exactly right. If the snapshot copy and the binlog copy of one
# row do not produce the same key, the stale snapshot row is not discarded and
# it overwrites the newer change event. That is silent data loss, and it only
# affects rows written during the backfill.
#
# Binary keys are the dangerous case: random bytes are usually not valid UTF-8,
# so decoding them loses information. We hex them instead, which is reversible.
# The type name is part of the identity, so 5 and "5" never compare equal.
# ---------------------------------------------------------------------------

def normalize_key(value: object, kind: str) -> object:
    """Put a raw key value into the canonical form used for identity
    comparisons, before it is handed to encode_value()."""
    if value is None:
        return None
    if kind == "binary":
        return to_bytes(value)
    if kind == "bit":
        return bit_to_int(value)
    return to_text(value)


def encode_value(value: object) -> EncodedValue:
    """Turn a normalized key value into a (type, text) pair that is both
    JSON-serialisable (for the state file) and hashable/comparable (for the
    in-memory key sets), and that decode_value() can turn back exactly."""
    if value is None:
        return ("null", "")                    # str(None) would collide "None"
    if isinstance(value, (bytes, bytearray)):
        return ("bytes", bytes(value).hex())
    if isinstance(value, bool):
        return ("bool", "1" if value else "0")
    if isinstance(value, int):
        return ("int", str(value))
    if isinstance(value, decimal.Decimal):
        return ("decimal", str(value))
    if isinstance(value, datetime.datetime):
        return ("datetime", value.isoformat())
    if isinstance(value, datetime.date):
        return ("date", value.isoformat())
    if isinstance(value, datetime.time):
        return ("time", value.isoformat())
    if isinstance(value, float):
        return ("float", repr(value))
    return ("str", str(value))


def decode_value(pair: EncodedValue) -> object:
    """Reverse of encode_value() - rebuilds the original typed value from the
    (type, text) pair, e.g. when resuming a backfill from the saved state."""
    name, text = pair
    if name == "null":
        return None
    if name == "bytes":
        return bytes.fromhex(text)
    if name == "bool":
        return text == "1"
    if name == "int":
        return int(text)
    if name == "decimal":
        return decimal.Decimal(text)
    if name == "datetime":
        return datetime.datetime.fromisoformat(text)
    if name == "date":
        return datetime.date.fromisoformat(text)
    if name == "time":
        return datetime.time.fromisoformat(text)
    if name == "float":
        return float(text)
    return text


def row_key(row: RowDict, table: Table) -> tuple[EncodedValue, ...]:
    """The comparable identity of a row, whichever route it came by."""
    parts: list[EncodedValue] = []
    for column in table.key_columns:
        parts.append(encode_value(normalize_key(row[column], table.kind(column))))
    return tuple(parts)


def quote_pg(name: str) -> str:
    """Double-quote a Postgres identifier, escaping embedded quotes. Handles
    "schema.table" by quoting each dot-separated part on its own."""
    return ".".join('"' + p.replace('"', '""') + '"' for p in name.split("."))


def quote_mysql(name: str) -> str:
    """Backtick-quote a MySQL identifier, escaping embedded backticks."""
    return "`" + name.replace("`", "``") + "`"


def row_changes(event: RowEvent) -> list[tuple[RowDict | None, RowDict | None]]:
    """(before, after) pairs. Insert has no before, delete has no after."""
    changes: list[tuple[RowDict | None, RowDict | None]] = []
    for row in event.rows:
        if isinstance(event, UpdateRowsEvent):
            changes.append((row["before_values"], row["after_values"]))
        elif isinstance(event, DeleteRowsEvent):
            changes.append((row["values"], None))
        else:
            changes.append((None, row["values"]))
    return changes


class Table:
    """One table's migration config, plus the column kinds discovered later
    from information_schema (see Migration.load_kinds)."""

    def __init__(self, raw: dict[str, object]) -> None:
        # raw is one entry of the "tables" list from config.json.
        self.mysql_table: str = raw["mysql_table"]
        self.pg_table: str = raw["pg_table"]
        self.key_columns: list[str] = raw["key_columns"]
        self.skip_columns: set[str] = set(raw.get("skip_columns", []))
        self.kinds: dict[str, str] = {}         # filled from information_schema
        if not self.key_columns:
            raise ValueError(self.mysql_table + ": key_columns is empty")
        if set(self.key_columns) & self.skip_columns:
            raise ValueError(self.mysql_table + ": a key column is skipped")

    def kind(self, column: str) -> str:
        """The column's kind for conversion purposes, defaulting to "plain"
        for any column not specially typed (json/bit/set/binary)."""
        return self.kinds.get(column, "plain")


class State:
    """Binlog position plus per-table progress, written atomically."""

    def __init__(self, path: str) -> None:
        # If a state file already exists, load it so a rerun resumes instead
        # of starting the backfill and binlog stream over from scratch.
        self.path: str = path
        self.log_file: str | None = None
        self.log_pos: int | None = None
        self.tables: dict[str, dict[str, object]] = {}
        if os.path.exists(path):
            with open(path) as handle:
                saved = json.load(handle)
            self.log_file = saved["log_file"]
            self.log_pos = saved["log_pos"]
            self.tables = saved["tables"]
            log.info("resuming at %s:%s", self.log_file, self.log_pos)

    def save(self) -> None:
        """Write state to a temp file and rename over the real path, so a
        crash mid-write never leaves a half-written, unreadable state file."""
        with open(self.path + ".tmp", "w") as handle:
            json.dump({"log_file": self.log_file, "log_pos": self.log_pos,
                       "tables": self.tables}, handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(self.path + ".tmp", self.path)

    def progress(self, table: Table) -> dict[str, object]:
        """Get (or create) this table's progress entry. Refuses to reuse a
        saved entry whose key_columns no longer match the config, since the
        saved last_key would then no longer mean the same thing."""
        entry = self.tables.get(table.mysql_table)
        if entry is None:
            entry = {"done": False, "last_key": None,
                     "key_columns": list(table.key_columns)}
            self.tables[table.mysql_table] = entry
        elif entry["key_columns"] != list(table.key_columns):
            raise RuntimeError(table.mysql_table + ": key_columns changed since "
                               "the saved run - delete the state file")
        return entry


# ---------------------------------------------------------------------------

class Migration:
    """Owns both database connections plus all migration state, and drives
    the two phases: backfill() per table, then a live apply_event() loop."""

    def __init__(self, config: dict[str, object], workspace: str) -> None:
        # Opens both connections immediately, so a bad config, a missing
        # password variable or an unreachable database fails at startup
        # instead of mid-run.
        self.config: dict[str, object] = config
        self.workspace: str = workspace
        self.chunk_size: int = int(config.get("chunk_size", 1000))
        self.watermark_table: str = config.get("watermark_table", "dblog_watermarks")
        self.timeout: float = float(config.get("watermark_timeout_seconds", 120))
        self.db: str = config["mysql"]["db"]

        self.connection_settings: dict[str, object] = {
            "host": config["mysql"].get("host", "127.0.0.1"),
            "port": int(config["mysql"].get("port", 3306)),
            "user": config["mysql"]["user"],
            "passwd": read_password(config["mysql"]["password_env"]),
            # Pinned: without it pymysql can return text columns as bytes,
            # which changes the identity of every string key.
            "charset": "utf8mb4",
        }
        # autocommit, so each chunk SELECT takes a fresh read view. Left off,
        # one REPEATABLE READ snapshot spans several statements and a chunk can
        # end up read as of before its own low watermark.
        self.mysql: pymysql.connections.Connection = pymysql.connect(
            db=self.db, cursorclass=pymysql.cursors.DictCursor,
            autocommit=True, **self.connection_settings)
        self.postgres: psycopg.Connection = psycopg.connect(
            host=config["postgres"].get("host", "127.0.0.1"),
            port=int(config["postgres"].get("port", 5432)),
            user=config["postgres"]["user"],
            password=read_password(config["postgres"]["password_env"]),
            dbname=config["postgres"]["db"])

        self.tables: list[Table] = [Table(raw) for raw in config["tables"]]
        self.by_name: dict[str, Table] = {table.mysql_table: table for table in self.tables}
        self.state: State = State(os.path.join(
            self.workspace, config.get("state_file", "dblog_state.json")))
        self.stream: BinLogStreamReader | None = None

    # -- setup --------------------------------------------------------------

    def check_settings(self) -> None:
        """Bad settings fail late and confusingly: MINIMAL row images write rows
        full of NULLs, and PARTIAL_JSON logs JSON diffs that wipe out the rest
        of the document when replayed."""
        wanted = {"binlog_format": "ROW", "binlog_row_image": "FULL",
                  "binlog_row_metadata": "FULL", "binlog_row_value_options": ""}
        with self.mysql.cursor() as cursor:
            for name, expected in wanted.items():
                cursor.execute("SELECT @@GLOBAL." + name + " AS v")
                actual = str(cursor.fetchone()["v"] or "").upper()
                if actual != expected:
                    raise RuntimeError("MySQL " + name + " is '" + actual +
                                       "', must be '" + expected + "'")

    def load_kinds(self, table: Table) -> None:
        """Ask MySQL what each column really is. Trusting a hand-written config
        means one missed BLOB gets decoded as UTF-8 and stored as mojibake."""
        with self.mysql.cursor() as cursor:
            cursor.execute(
                "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_SET_NAME, IS_NULLABLE"
                " FROM information_schema.COLUMNS"
                " WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                (self.db, table.mysql_table))
            rows = cursor.fetchall()
        if not rows:
            raise RuntimeError(table.mysql_table + ": no such table in " + self.db)

        for row in rows:
            name = row["COLUMN_NAME"]
            data_type = str(row["DATA_TYPE"]).lower()
            charset = str(row["CHARACTER_SET_NAME"] or "").lower()
            if data_type in ("json", "bit", "set"):
                table.kinds[name] = data_type
            elif data_type in BINARY_TYPES or charset == "binary":
                table.kinds[name] = "binary"
            else:
                table.kinds[name] = "plain"
            # (key) > (last key) is never true when a key value is NULL, so
            # those rows would be returned by no chunk at all.
            if name in table.key_columns and row["IS_NULLABLE"] == "YES":
                raise RuntimeError(table.mysql_table + ": key column " + name +
                                   " must be NOT NULL")

        for column in table.key_columns:
            if column not in table.kinds:
                raise RuntimeError(table.mysql_table + ": no column " + column)

    def open_stream(self) -> None:
        """blocking=True is required: a non-blocking reader stops as soon as it
        drains, so the loop waiting for a high watermark would exit early.
        Heartbeats keep the iterator waking up on an idle server."""
        if not self.state.log_file:
            with self.mysql.cursor() as cursor:
                try:
                    cursor.execute("SHOW BINARY LOG STATUS")     # MySQL 8.4+
                except pymysql.err.ProgrammingError:
                    cursor.execute("SHOW MASTER STATUS")
                status = cursor.fetchone()
            # Start at the end of the log: the backfill picks up everything
            # already in the tables, so there is no history worth replaying.
            self.state.log_file = status["File"]
            self.state.log_pos = int(status["Position"])
            self.state.save()

        log.info("stream at %s:%s", self.state.log_file, self.state.log_pos)
        self.stream = BinLogStreamReader(
            connection_settings=self.connection_settings,
            server_id=int(self.config.get("server_id", 100)),
            blocking=True, resume_stream=True,
            log_file=self.state.log_file, log_pos=self.state.log_pos,
            only_schemas=[self.db],
            only_tables=list(self.by_name) + [self.watermark_table],
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent,
                         QueryEvent, XidEvent, HeartbeatLogEvent],
            slave_heartbeat=float(self.config.get("heartbeat_seconds", 10)))

    # -- writing ------------------------------------------------------------

    def checkpoint(self) -> None:
        """Commit Postgres first, record the position second, so the saved
        position can only lag the data. Lagging just replays idempotent writes."""
        self.postgres.commit()
        if self.stream is not None:
            self.state.log_file = self.stream.log_file
            self.state.log_pos = self.stream.log_pos
        self.state.save()

    def upsert(self, table: Table, rows: list[RowDict]) -> None:
        """Bulk INSERT ... ON CONFLICT DO UPDATE the given rows into Postgres.
        Idempotent by design (see module docstring), so re-applying the same
        rows after a crash-and-resume is harmless."""
        if not rows:
            return
        columns = sorted(rows[0].keys())
        sets = [quote_pg(c) + " = EXCLUDED." + quote_pg(c)
                for c in columns if c not in table.key_columns]
        action = "DO UPDATE SET " + ", ".join(sets) if sets else "DO NOTHING"
        head = ("INSERT INTO " + quote_pg(table.pg_table) + " (" +
                ", ".join(quote_pg(c) for c in columns) + ") VALUES ")
        tail = (" ON CONFLICT (" +
                ", ".join(quote_pg(c) for c in table.key_columns) + ") " + action)
        holes = "(" + ", ".join(["%s"] * len(columns)) + ")"
        # psycopg has no execute_values, so the multi-row VALUES list is built
        # here. Pages of 100 rows, which is what execute_values defaulted to.
        with self.postgres.cursor() as cursor:
            for start in range(0, len(rows), 100):
                page = rows[start:start + 100]
                cursor.execute(head + ", ".join([holes] * len(page)) + tail,
                               [row.get(c) for row in page for c in columns])

    def delete(self, table: Table, row: RowDict) -> None:
        """DELETE the row matching this table's key columns. Deleting a key
        that is already gone is a no-op, so this is safe to replay too."""
        where = " AND ".join(quote_pg(c) + " = %s" for c in table.key_columns)
        params = [convert(row[c], table.kind(c), True) for c in table.key_columns]
        with self.postgres.cursor() as cursor:
            cursor.execute("DELETE FROM " + quote_pg(table.pg_table) +
                           " WHERE " + where, params)

    # -- events -------------------------------------------------------------

    def marks_in(self, event: BinlogEvent) -> list[str]:
        """The watermark ids inserted by this event, if it is a write to the
        watermark table; otherwise an empty list. Used by follow_to() to spot
        the low/high markers as they come back through the binlog stream."""
        if not isinstance(event, WriteRowsEvent):
            return []
        if event.table != self.watermark_table:
            return []
        return [to_text(row["values"]["id"]) for row in event.rows]

    def apply_event(self, event: BinlogEvent) -> None:
        """Change events are applied unconditionally, window open or not.

        Rows go one at a time, in binlog order. Batching would reorder
        operations on the same key and produce the wrong final state.
        """
        if isinstance(event, XidEvent):
            self.checkpoint()                  # a source commit boundary
            return
        if isinstance(event, QueryEvent):
            query = (event.query or "").strip()
            if query and query.split()[0].upper() in DDL_WORDS:
                raise RuntimeError("source DDL cannot be replayed: " + query[:200])
            return
        if not isinstance(event, ROW_EVENTS):
            return
        table = self.by_name.get(event.table)
        if table is None:
            return                             # watermark rows are signals
        for before, after in row_changes(event):
            if after is None:
                self.delete(table, before)
                continue
            # An update that changes a key moves the row; without the delete
            # the old copy is left behind in Postgres forever.
            if before is not None and row_key(before, table) != row_key(after, table):
                self.delete(table, before)
            self.upsert(table, [convert_row(after, table, True)])

    # -- backfill -----------------------------------------------------------

    def watermark(self) -> str:
        """Insert a fresh, uniquely-identifiable marker row into the
        watermark table and return its id, so it can be recognised later when
        it comes back through the binlog stream."""
        mark = uuid.uuid4().hex
        with self.mysql.cursor() as cursor:
            cursor.execute("INSERT INTO " + quote_mysql(self.watermark_table) +
                           " (id) VALUES (%s)", (mark,))
        return mark

    def fetch_chunk(self, table: Table, after_key: list[object] | None) -> list[RowDict]:
        """Row-constructor comparison, so composite keys of any comparable type
        work and we never assume keys are numeric or contiguous."""
        keys = ", ".join(quote_mysql(c) for c in table.key_columns)
        order = ", ".join(quote_mysql(c) + " ASC" for c in table.key_columns)
        name = quote_mysql(table.mysql_table)
        if after_key is None:
            statement = "SELECT * FROM " + name + " ORDER BY " + order + " LIMIT %s"
            params = [self.chunk_size]
        else:
            holes = ", ".join(["%s"] * len(table.key_columns))
            statement = ("SELECT * FROM " + name + " WHERE (" + keys + ") > (" +
                         holes + ") ORDER BY " + order + " LIMIT %s")
            params = list(after_key) + [self.chunk_size]
        with self.mysql.cursor() as cursor:
            cursor.execute(statement, params)
            return cursor.fetchall()

    def backfill(self, table: Table) -> None:
        """Copy one table from MySQL to Postgres, chunk by chunk, using the
        DBLog watermark algorithm to stay correct against concurrent writes.
        Resumes from entry["last_key"] if a previous run left off partway,
        and returns (without marking the table done) if interrupted."""
        entry = self.state.progress(table)
        if entry["done"]:
            return
        after_key: list[object] | None = None
        if entry["last_key"]:
            after_key = [decode_value(pair) for pair in entry["last_key"]]
        log.info("%s: backfilling", table.mysql_table)
        copied = 0

        while not stop_requested:
            # Keeps the marker table at two rows instead of two per chunk.
            with self.mysql.cursor() as cursor:
                cursor.execute("DELETE FROM " + quote_mysql(self.watermark_table))

            low = self.watermark()
            chunk = self.fetch_chunk(table, after_key)      # no lock held
            if not chunk:
                entry["done"] = True
                self.checkpoint()
                log.info("%s: done (%d rows this run)", table.mysql_table, copied)
                return

            by_key = {row_key(row, table): row for row in chunk}
            next_key = [chunk[-1][c] for c in table.key_columns]
            high = self.watermark()

            try:
                changed = self.follow_to(table, low, high)
            except WindowInterrupted as reason:
                # Without the high watermark we cannot tell which snapshot rows
                # are stale, so the chunk is dropped and re-read next run.
                # Nothing is lost - the change events were applied already.
                log.warning("%s: %s - redoing this chunk", table.mysql_table, reason)
                self.checkpoint()
                return

            # Keep only rows not touched inside the window. For the rest, the
            # change event we already applied is newer than the SELECT.
            fresh = [convert_row(row, table, False)
                     for key, row in by_key.items() if key not in changed]
            self.upsert(table, fresh)

            after_key = next_key
            entry["last_key"] = [list(encode_value(value)) for value in next_key]
            self.checkpoint()
            copied += len(chunk)
            log.info("%s: +%d rows (%d superseded), %d total", table.mysql_table,
                     len(fresh), len(by_key) - len(fresh), copied)

    def follow_to(self, table: Table, low: str, high: str) -> set[tuple[EncodedValue, ...]]:
        """Apply events until the high watermark shows up, returning the keys of
        this table's rows that changed between the two watermarks.

        The timeout measures silence, not elapsed time: a wall-clock deadline
        would starve a busy table, discarding and re-reading the same chunk
        forever.
        """
        inside = False
        changed: set[tuple[EncodedValue, ...]] = set()
        deadline = time.monotonic() + self.timeout

        for event in self.stream:
            if not isinstance(event, HeartbeatLogEvent):
                deadline = time.monotonic() + self.timeout
            marks = self.marks_in(event)

            # Recorded before applying, so a delete's key is captured even
            # though the row will not exist afterwards.
            if inside and isinstance(event, ROW_EVENTS) and event.table == table.mysql_table:
                for before, after in row_changes(event):
                    if before is not None:
                        changed.add(row_key(before, table))
                    if after is not None:
                        changed.add(row_key(after, table))

            self.apply_event(event)

            if low in marks:
                inside = True
            if high in marks:
                return changed
            if stop_requested:
                raise WindowInterrupted("interrupted before the window closed")
            if time.monotonic() > deadline:
                raise WindowInterrupted(
                    "no binlog activity while waiting for a watermark - check "
                    "that " + self.watermark_table + " is in schema " + self.db)

        raise WindowInterrupted("stream ended")

    # -----------------------------------------------------------------------

    def run(self) -> None:
        """Top-level entry point: validate settings, discover column kinds,
        open the binlog stream, backfill every table (phase 1), then follow
        the binlog indefinitely once all tables are done (phase 2). Always
        closes connections and saves state on the way out, whether that is
        because the follow loop was interrupted or because of an error."""
        try:
            self.check_settings()
            for table in self.tables:
                self.load_kinds(table)
            self.open_stream()

            for table in self.tables:
                if stop_requested:
                    break
                self.backfill(table)

            done = [self.state.progress(t)["done"] for t in self.tables]
            if all(done) and not stop_requested:
                log.info("backfill complete - following changes (Ctrl-C to stop)")
                for event in self.stream:
                    self.apply_event(event)
                    if stop_requested:
                        self.checkpoint()
                        return
        finally:
            self.close()

    def close(self) -> None:
        """Best-effort shutdown: commit any pending Postgres work (or roll it
        back if the commit itself fails), close both connections, and persist
        the final state so the next run knows exactly where to resume."""
        try:
            self.postgres.commit()
        except Exception:
            self.postgres.rollback()
        if self.stream is not None:
            self.stream.close()
        self.mysql.close()
        self.postgres.close()
        self.state.save()
        log.info("stopped at %s:%s", self.state.log_file, self.state.log_pos)


def main() -> int:
    """CLI entry point: parse args, load config.json out of the workspace
    folder, and run the migration. Returns a process exit code rather than
    raising, so __main__ can just pass it straight to sys.exit()."""
    logging.basicConfig(level=logging.INFO, datefmt="%H:%M:%S",
                        format="%(asctime)s %(levelname)-7s %(message)s")
    if len(sys.argv) != 2:
        print("usage: " + sys.argv[0] + " <workspace_dir>", file=sys.stderr)
        return 1
    install_signal_handlers()
    workspace = sys.argv[1]
    with open(os.path.join(workspace, "config.json")) as handle:
        config = json.load(handle)
    try:
        Migration(config, workspace).run()
    except RuntimeError as error:
        log.error("%s", error)
        return 2
    except KeyboardInterrupt:
        log.error("forced exit - saved state may be behind actual progress")
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
