from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import html
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import tomllib

from bewley import __version__


PROJECT_DIR = ".bewley"
DB_PATH = Path(PROJECT_DIR) / "index" / "bewley.sqlite"
EVENTS_DIR = Path(PROJECT_DIR) / "events"
OBJECTS_DIR = Path(PROJECT_DIR) / "objects" / "documents"
LOCK_PATH = Path(PROJECT_DIR) / "locks" / "write.lock"
CONFIG_PATH = Path(PROJECT_DIR) / "config.toml"
HEAD_PATH = Path(PROJECT_DIR) / "HEAD"
DEFAULT_QUERY_MODE = "document"
CONTEXT_BYTES = 32
FUZZY_RELOCATION_THRESHOLD = 0.92


class BewleyError(Exception):
    pass


def utcnow() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n"


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        handle.write(content)
        temp_name = handle.name
    os.replace(temp_name, path)


def load_toml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return tomllib.loads(path.read_text(encoding="utf-8"))


def find_project_root(start: Path | None = None) -> Path:
    current = (start or Path.cwd()).resolve()
    for candidate in (current, *current.parents):
        if (candidate / PROJECT_DIR).is_dir():
            return candidate
    raise BewleyError("not inside a bewley project")


def ensure_utf8_bytes(path: Path) -> bytes:
    data = path.read_bytes()
    try:
        data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise BewleyError(f"{path} is not valid UTF-8: {exc}") from exc
    return data


def count_lines(text: str) -> int:
    if not text:
        return 0
    return text.count("\n") + (0 if text.endswith("\n") else 1)


def line_offsets(text: str) -> list[int]:
    starts = [0]
    running = 0
    for line in text.splitlines(keepends=True):
        running += len(line.encode("utf-8"))
        starts.append(running)
    return starts


def byte_to_line_range(text: str, start_byte: int, end_byte: int) -> tuple[int, int]:
    starts = line_offsets(text)
    start_line = 1
    end_line = 1
    for idx, offset in enumerate(starts[:-1], start=1):
        next_offset = starts[idx]
        if offset <= start_byte < next_offset:
            start_line = idx
        if offset < end_byte <= next_offset:
            end_line = idx
            break
        if end_byte == offset and idx > 1:
            end_line = idx - 1
    else:
        if starts:
            end_line = max(1, len(starts) - 1)
    return start_line, max(start_line, end_line)


def lines_to_byte_range(text: str, start_line: int, end_line: int) -> tuple[int, int]:
    if start_line < 1 or end_line < start_line:
        raise BewleyError("invalid line range")
    starts = line_offsets(text)
    last_line = max(1, len(starts) - 1)
    if end_line > last_line:
        raise BewleyError(f"line range exceeds document length ({last_line} lines)")
    start_byte = starts[start_line - 1]
    end_byte = starts[end_line]
    return start_byte, end_byte


def safe_decode(data: bytes) -> str:
    return data.decode("utf-8", errors="replace")


def annotation_overlap(a: sqlite3.Row, b: sqlite3.Row) -> bool:
    if a["scope_type"] == "document" and b["scope_type"] == "document":
        return True
    if a["scope_type"] == "document" or b["scope_type"] == "document":
        return True
    return not (a["end_byte"] <= b["start_byte"] or b["end_byte"] <= a["start_byte"])


class BoolExpr:
    def evaluate(self, names: set[str]) -> bool:
        raise NotImplementedError


@dataclass
class Term(BoolExpr):
    value: str

    def evaluate(self, names: set[str]) -> bool:
        return self.value in names


@dataclass
class Not(BoolExpr):
    expr: BoolExpr

    def evaluate(self, names: set[str]) -> bool:
        return not self.expr.evaluate(names)


@dataclass
class BinOp(BoolExpr):
    left: BoolExpr
    right: BoolExpr
    kind: str

    def evaluate(self, names: set[str]) -> bool:
        if self.kind == "AND":
            return self.left.evaluate(names) and self.right.evaluate(names)
        if self.kind == "OR":
            return self.left.evaluate(names) or self.right.evaluate(names)
        raise ValueError(self.kind)


class ExprParser:
    def __init__(self, text: str) -> None:
        self.tokens = self.tokenize(text)
        self.index = 0

    @staticmethod
    def tokenize(text: str) -> list[str]:
        tokens: list[str] = []
        i = 0
        while i < len(text):
            ch = text[i]
            if ch.isspace():
                i += 1
                continue
            if ch in "()":
                tokens.append(ch)
                i += 1
                continue
            if ch in "\"'":
                quote = ch
                i += 1
                start = i
                while i < len(text) and text[i] != quote:
                    i += 1
                if i >= len(text):
                    raise BewleyError("unterminated quoted token in query")
                tokens.append(text[start:i])
                i += 1
                continue
            start = i
            while i < len(text) and (not text[i].isspace()) and text[i] not in "()":
                i += 1
            tokens.append(text[start:i])
        return tokens

    def current(self) -> str | None:
        if self.index >= len(self.tokens):
            return None
        return self.tokens[self.index]

    def consume(self, expected: str | None = None) -> str:
        token = self.current()
        if token is None:
            raise BewleyError("unexpected end of query")
        if expected is not None and token != expected:
            raise BewleyError(f"expected {expected!r}, got {token!r}")
        self.index += 1
        return token

    def parse(self) -> BoolExpr:
        expr = self.parse_or()
        if self.current() is not None:
            raise BewleyError(f"unexpected token {self.current()!r}")
        return expr

    def parse_or(self) -> BoolExpr:
        left = self.parse_and()
        while (token := self.current()) and token.upper() == "OR":
            self.consume()
            left = BinOp(left=left, right=self.parse_and(), kind="OR")
        return left

    def parse_and(self) -> BoolExpr:
        left = self.parse_not()
        while (token := self.current()) and token.upper() == "AND":
            self.consume()
            left = BinOp(left=left, right=self.parse_not(), kind="AND")
        return left

    def parse_not(self) -> BoolExpr:
        token = self.current()
        if token and token.upper() == "NOT":
            self.consume()
            return Not(self.parse_not())
        return self.parse_primary()

    def parse_primary(self) -> BoolExpr:
        token = self.current()
        if token == "(":
            self.consume("(")
            expr = self.parse_or()
            self.consume(")")
            return expr
        if token is None:
            raise BewleyError("unexpected end of query")
        self.consume()
        return Term(token)


class Project:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.meta = root / PROJECT_DIR

    @classmethod
    def discover(cls) -> "Project":
        return cls(find_project_root())

    @property
    def db_path(self) -> Path:
        return self.root / DB_PATH

    @property
    def config_path(self) -> Path:
        return self.root / CONFIG_PATH

    @property
    def head_path(self) -> Path:
        return self.root / HEAD_PATH

    @property
    def events_dir(self) -> Path:
        return self.root / EVENTS_DIR

    @property
    def objects_dir(self) -> Path:
        return self.root / OBJECTS_DIR

    @property
    def lock_path(self) -> Path:
        return self.root / LOCK_PATH

    def config(self) -> dict[str, Any]:
        return load_toml(self.config_path)

    def actor(self) -> dict[str, str]:
        cfg = self.config()
        actor = cfg.get("actor", {})
        return {
            "name": actor.get("name") or os.environ.get("USER", "unknown"),
            "email": actor.get("email") or "",
        }

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def ensure_db(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS documents (
                  document_id TEXT PRIMARY KEY,
                  current_path TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  archived_at TEXT
                );
                CREATE TABLE IF NOT EXISTS document_revisions (
                  revision_id TEXT PRIMARY KEY,
                  document_id TEXT NOT NULL,
                  content_sha256 TEXT NOT NULL,
                  byte_length INTEGER NOT NULL,
                  line_count INTEGER NOT NULL,
                  created_at TEXT NOT NULL,
                  source_path TEXT NOT NULL,
                  parent_revision_id TEXT,
                  is_current INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS codes (
                  code_id TEXT PRIMARY KEY,
                  canonical_name TEXT NOT NULL UNIQUE,
                  description TEXT,
                  color TEXT,
                  status TEXT NOT NULL,
                  created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS code_aliases (
                  alias_name TEXT PRIMARY KEY,
                  code_id TEXT NOT NULL,
                  created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS annotations (
                  annotation_id TEXT PRIMARY KEY,
                  code_id TEXT NOT NULL,
                  document_id TEXT NOT NULL,
                  document_revision_id TEXT NOT NULL,
                  scope_type TEXT NOT NULL,
                  start_byte INTEGER,
                  end_byte INTEGER,
                  start_line INTEGER,
                  end_line INTEGER,
                  exact_text TEXT,
                  prefix_context TEXT,
                  suffix_context TEXT,
                  anchor_status TEXT NOT NULL,
                  created_by_event_id TEXT NOT NULL,
                  superseded_by_event_id TEXT,
                  memo TEXT,
                  created_at TEXT NOT NULL,
                  is_active INTEGER NOT NULL DEFAULT 1
                );
                CREATE TABLE IF NOT EXISTS events (
                  event_id TEXT PRIMARY KEY,
                  sequence_number INTEGER NOT NULL UNIQUE,
                  event_type TEXT NOT NULL,
                  timestamp TEXT NOT NULL,
                  actor TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_annotations_code_id ON annotations (code_id);
                CREATE INDEX IF NOT EXISTS idx_annotations_document_id ON annotations (document_id);
                CREATE INDEX IF NOT EXISTS idx_annotations_revision_id ON annotations (document_revision_id);
                CREATE INDEX IF NOT EXISTS idx_annotations_anchor_status ON annotations (anchor_status);
                CREATE INDEX IF NOT EXISTS idx_aliases_code_id ON code_aliases (code_id);
                CREATE INDEX IF NOT EXISTS idx_revisions_document_current ON document_revisions (document_id, is_current);
                """
            )

    def init_project(self) -> None:
        if self.meta.exists():
            raise BewleyError("project already initialized")
        for rel in [
            "corpus",
            ".bewley/events",
            ".bewley/objects/documents",
            ".bewley/refs/codes",
            ".bewley/refs/documents",
            ".bewley/index",
            ".bewley/locks",
            ".bewley/logs",
        ]:
            (self.root / rel).mkdir(parents=True, exist_ok=True)
        atomic_write_text(
            self.config_path,
            (
                'format_version = 1\n'
                f'default_query_mode = "{DEFAULT_QUERY_MODE}"\n'
                'text_encoding_policy = "utf-8-only"\n'
                f"relocation_threshold = {FUZZY_RELOCATION_THRESHOLD}\n\n"
                "[actor]\n"
                f'name = "{os.environ.get("USER", "unknown")}"\n'
                'email = ""\n'
            ),
        )
        atomic_write_text(self.head_path, "0\n")
        self.ensure_db()
        self.append_event("project_initialized", {"root": str(self.root.resolve())}, rebuild_projection=True)

    @contextlib.contextmanager
    def write_lock(self) -> Iterable[None]:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise BewleyError("project is locked by another writer") from exc
        try:
            os.write(fd, str(os.getpid()).encode("utf-8"))
            os.close(fd)
            yield
        finally:
            with contextlib.suppress(FileNotFoundError):
                self.lock_path.unlink()

    def next_sequence(self) -> int:
        if not self.head_path.exists():
            return 1
        value = self.head_path.read_text(encoding="utf-8").strip() or "0"
        return int(value) + 1

    def last_event_id(self) -> str | None:
        event_files = sorted(self.events_dir.glob("*.json"))
        if not event_files:
            return None
        payload = json.loads(event_files[-1].read_text(encoding="utf-8"))
        return payload["event_id"]

    def append_event(self, event_type: str, payload: dict[str, Any], rebuild_projection: bool = False) -> dict[str, Any]:
        with self.write_lock():
            sequence_number = self.next_sequence()
            event: dict[str, Any] = {
                "event_id": uuid.uuid4().hex,
                "sequence_number": sequence_number,
                "event_type": event_type,
                "timestamp": utcnow(),
                "actor": self.actor(),
                "tool_version": __version__,
                "payload": payload,
                "parent_event_ids": [eid] if (eid := self.last_event_id()) else [],
            }
            digest_input = dict(event)
            event["event_sha256"] = sha256_text(json.dumps(digest_input, ensure_ascii=False, sort_keys=True))
            event_path = self.events_dir / f"{sequence_number:012d}.json"
            atomic_write_text(event_path, json_dumps(event))
            atomic_write_text(self.head_path, f"{sequence_number}\n")
            self.ensure_db()
            if rebuild_projection:
                self.rebuild_index()
            else:
                with self.connect() as conn:
                    self.apply_event(conn, event)
                    conn.commit()
            return event

    def all_events(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for path in sorted(self.events_dir.glob("*.json")):
            events.append(json.loads(path.read_text(encoding="utf-8")))
        return events

    def rebuild_index(self) -> None:
        temp_db = self.db_path.with_suffix(".sqlite.tmp")
        if temp_db.exists():
            temp_db.unlink()
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        self._init_connection(conn)
        for event in self.all_events():
            self.apply_event(conn, event)
        conn.commit()
        conn.close()
        os.replace(temp_db, self.db_path)
        atomic_write_text(self.root / PROJECT_DIR / "logs" / "rebuild.log", f"{utcnow()} rebuilt index\n")

    def _init_connection(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE documents (
              document_id TEXT PRIMARY KEY,
              current_path TEXT NOT NULL,
              created_at TEXT NOT NULL,
              archived_at TEXT
            );
            CREATE TABLE document_revisions (
              revision_id TEXT PRIMARY KEY,
              document_id TEXT NOT NULL,
              content_sha256 TEXT NOT NULL,
              byte_length INTEGER NOT NULL,
              line_count INTEGER NOT NULL,
              created_at TEXT NOT NULL,
              source_path TEXT NOT NULL,
              parent_revision_id TEXT,
              is_current INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE codes (
              code_id TEXT PRIMARY KEY,
              canonical_name TEXT NOT NULL UNIQUE,
              description TEXT,
              color TEXT,
              status TEXT NOT NULL,
              created_at TEXT NOT NULL
            );
            CREATE TABLE code_aliases (
              alias_name TEXT PRIMARY KEY,
              code_id TEXT NOT NULL,
              created_at TEXT NOT NULL
            );
            CREATE TABLE annotations (
              annotation_id TEXT PRIMARY KEY,
              code_id TEXT NOT NULL,
              document_id TEXT NOT NULL,
              document_revision_id TEXT NOT NULL,
              scope_type TEXT NOT NULL,
              start_byte INTEGER,
              end_byte INTEGER,
              start_line INTEGER,
              end_line INTEGER,
              exact_text TEXT,
              prefix_context TEXT,
              suffix_context TEXT,
              anchor_status TEXT NOT NULL,
              created_by_event_id TEXT NOT NULL,
              superseded_by_event_id TEXT,
              memo TEXT,
              created_at TEXT NOT NULL,
              is_active INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE events (
              event_id TEXT PRIMARY KEY,
              sequence_number INTEGER NOT NULL UNIQUE,
              event_type TEXT NOT NULL,
              timestamp TEXT NOT NULL,
              actor TEXT NOT NULL
            );
            CREATE INDEX idx_annotations_code_id ON annotations (code_id);
            CREATE INDEX idx_annotations_document_id ON annotations (document_id);
            CREATE INDEX idx_annotations_revision_id ON annotations (document_revision_id);
            CREATE INDEX idx_annotations_anchor_status ON annotations (anchor_status);
            CREATE INDEX idx_aliases_code_id ON code_aliases (code_id);
            CREATE INDEX idx_revisions_document_current ON document_revisions (document_id, is_current);
            """
        )

    def apply_event(self, conn: sqlite3.Connection, event: dict[str, Any]) -> None:
        payload = event["payload"]
        conn.execute(
            """
            INSERT OR REPLACE INTO events (event_id, sequence_number, event_type, timestamp, actor)
            VALUES (?, ?, ?, ?, ?)
            """,
            (event["event_id"], event["sequence_number"], event["event_type"], event["timestamp"], json.dumps(event["actor"], ensure_ascii=False)),
        )
        etype = event["event_type"]
        if etype == "project_initialized":
            return
        if etype == "document_added":
            conn.execute(
                "INSERT INTO documents (document_id, current_path, created_at, archived_at) VALUES (?, ?, ?, NULL)",
                (payload["document_id"], payload["current_path"], event["timestamp"]),
            )
            conn.execute(
                """
                INSERT INTO document_revisions (
                  revision_id, document_id, content_sha256, byte_length, line_count, created_at,
                  source_path, parent_revision_id, is_current
                ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 1)
                """,
                (
                    payload["revision_id"],
                    payload["document_id"],
                    payload["content_sha256"],
                    payload["byte_length"],
                    payload["line_count"],
                    event["timestamp"],
                    payload["source_path"],
                ),
            )
            return
        if etype == "document_moved":
            conn.execute(
                "UPDATE documents SET current_path = ? WHERE document_id = ?",
                (payload["current_path"], payload["document_id"]),
            )
            return
        if etype == "document_updated":
            conn.execute(
                "UPDATE documents SET current_path = ? WHERE document_id = ?",
                (payload["current_path"], payload["document_id"]),
            )
            conn.execute(
                "UPDATE document_revisions SET is_current = 0 WHERE document_id = ?",
                (payload["document_id"],),
            )
            conn.execute(
                """
                INSERT INTO document_revisions (
                  revision_id, document_id, content_sha256, byte_length, line_count, created_at,
                  source_path, parent_revision_id, is_current
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    payload["revision_id"],
                    payload["document_id"],
                    payload["content_sha256"],
                    payload["byte_length"],
                    payload["line_count"],
                    event["timestamp"],
                    payload["source_path"],
                    payload["parent_revision_id"],
                ),
            )
            return
        if etype == "code_created":
            conn.execute(
                """
                INSERT INTO codes (code_id, canonical_name, description, color, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (payload["code_id"], payload["canonical_name"], payload.get("description"), payload.get("color"), "active", event["timestamp"]),
            )
            return
        if etype == "code_renamed":
            conn.execute(
                "UPDATE codes SET canonical_name = ? WHERE code_id = ?",
                (payload["new_name"], payload["code_id"]),
            )
            return
        if etype == "code_aliased":
            conn.execute(
                "INSERT INTO code_aliases (alias_name, code_id, created_at) VALUES (?, ?, ?)",
                (payload["alias_name"], payload["code_id"], event["timestamp"]),
            )
            return
        if etype == "code_merged":
            for source_code_id in payload["source_code_ids"]:
                conn.execute("UPDATE codes SET status = 'merged' WHERE code_id = ?", (source_code_id,))
            return
        if etype == "code_split":
            conn.execute(
                """
                INSERT INTO codes (code_id, canonical_name, description, color, status, created_at)
                VALUES (?, ?, ?, ?, 'active', ?)
                """,
                (
                    payload["new_code_id"],
                    payload["new_canonical_name"],
                    payload.get("description"),
                    payload.get("color"),
                    event["timestamp"],
                ),
            )
            for annotation_id in payload["annotation_ids"]:
                conn.execute(
                    "UPDATE annotations SET code_id = ? WHERE annotation_id = ? AND is_active = 1",
                    (payload["new_code_id"], annotation_id),
                )
            return
        if etype == "annotation_added":
            conn.execute(
                """
                INSERT INTO annotations (
                  annotation_id, code_id, document_id, document_revision_id, scope_type, start_byte, end_byte,
                  start_line, end_line, exact_text, prefix_context, suffix_context, anchor_status,
                  created_by_event_id, superseded_by_event_id, memo, created_at, is_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, 1)
                """,
                (
                    payload["annotation_id"],
                    payload["code_id"],
                    payload["document_id"],
                    payload["document_revision_id"],
                    payload["scope_type"],
                    payload.get("start_byte"),
                    payload.get("end_byte"),
                    payload.get("start_line"),
                    payload.get("end_line"),
                    payload.get("exact_text"),
                    payload.get("prefix_context"),
                    payload.get("suffix_context"),
                    payload["anchor_status"],
                    event["event_id"],
                    payload.get("memo"),
                    event["timestamp"],
                ),
            )
            return
        if etype == "annotation_removed":
            conn.execute(
                "UPDATE annotations SET is_active = 0, superseded_by_event_id = ? WHERE annotation_id = ?",
                (event["event_id"], payload["annotation_id"]),
            )
            return
        if etype in {"annotation_reanchored", "annotation_resolved"}:
            conn.execute(
                """
                UPDATE annotations
                SET document_revision_id = ?, start_byte = ?, end_byte = ?, start_line = ?, end_line = ?,
                    exact_text = ?, prefix_context = ?, suffix_context = ?, anchor_status = ?, memo = COALESCE(?, memo)
                WHERE annotation_id = ?
                """,
                (
                    payload["document_revision_id"],
                    payload.get("start_byte"),
                    payload.get("end_byte"),
                    payload.get("start_line"),
                    payload.get("end_line"),
                    payload.get("exact_text"),
                    payload.get("prefix_context"),
                    payload.get("suffix_context"),
                    payload["anchor_status"],
                    payload.get("memo"),
                    payload["annotation_id"],
                ),
            )
            return
        if etype == "annotation_conflicted":
            conn.execute(
                "UPDATE annotations SET anchor_status = 'conflicted', memo = COALESCE(?, memo) WHERE annotation_id = ?",
                (payload.get("memo"), payload["annotation_id"]),
            )
            return
        if etype == "undo_recorded":
            self.apply_undo(conn, payload, event)
            return
        if etype == "index_rebuilt":
            return
        raise BewleyError(f"unsupported event type in projection: {etype}")

    def apply_undo(self, conn: sqlite3.Connection, payload: dict[str, Any], event: dict[str, Any]) -> None:
        undone_type = payload["undone_event_type"]
        original = payload["original_payload"]
        if undone_type == "code_renamed":
            conn.execute("UPDATE codes SET canonical_name = ? WHERE code_id = ?", (original["old_name"], original["code_id"]))
            return
        if undone_type == "annotation_added":
            conn.execute(
                "UPDATE annotations SET is_active = 0, superseded_by_event_id = ? WHERE annotation_id = ?",
                (event["event_id"], original["annotation_id"]),
            )
            return
        raise BewleyError(f"unsupported undo event type in projection: {undone_type}")

    def current_revision(self, conn: sqlite3.Connection, document_id: str) -> sqlite3.Row:
        row = conn.execute(
            "SELECT * FROM document_revisions WHERE document_id = ? AND is_current = 1",
            (document_id,),
        ).fetchone()
        if row is None:
            raise BewleyError("document has no current revision")
        return row

    def resolve_document(self, conn: sqlite3.Connection, ref: str) -> sqlite3.Row:
        exact = conn.execute(
            "SELECT * FROM documents WHERE document_id = ? OR current_path = ?",
            (ref, ref),
        ).fetchall()
        if len(exact) == 1:
            return exact[0]
        if len(exact) > 1:
            raise BewleyError(f"ambiguous document reference: {ref}")
        basename = Path(ref).name
        matches = conn.execute("SELECT * FROM documents WHERE current_path LIKE ?", (f"%{basename}",)).fetchall()
        if len(matches) == 1:
            return matches[0]
        if not matches:
            raise BewleyError(f"unknown document reference: {ref}")
        raise BewleyError(f"ambiguous document reference: {ref}")

    def resolve_code(self, conn: sqlite3.Connection, ref: str) -> sqlite3.Row:
        rows = conn.execute(
            """
            SELECT c.*
            FROM codes c
            LEFT JOIN code_aliases a ON a.code_id = c.code_id
            WHERE c.code_id = ? OR c.canonical_name = ? OR a.alias_name = ?
            """,
            (ref, ref, ref),
        ).fetchall()
        if len(rows) == 1:
            return rows[0]
        if not rows:
            raise BewleyError(f"unknown code reference: {ref}")
        seen = {row["code_id"] for row in rows}
        if len(seen) == 1:
            return rows[0]
        raise BewleyError(f"ambiguous code reference: {ref}")

    def store_revision_object(self, data: bytes) -> str:
        digest = sha256_bytes(data)
        target = self.objects_dir / digest
        if not target.exists():
            target.write_bytes(data)
        return digest

    def add_document(self, path_arg: str) -> dict[str, Any]:
        path = (self.root / path_arg).resolve() if not Path(path_arg).is_absolute() else Path(path_arg)
        try:
            rel = path.relative_to(self.root)
        except ValueError as exc:
            raise BewleyError("document path must be inside the project root") from exc
        if not path.is_file():
            raise BewleyError(f"document not found: {path_arg}")
        with self.connect() as conn:
            existing = conn.execute(
                "SELECT document_id FROM documents WHERE current_path = ?",
                (str(rel),),
            ).fetchone()
            if existing is not None:
                raise BewleyError(f"path is already tracked: {rel}")
        data = ensure_utf8_bytes(path)
        text = data.decode("utf-8")
        digest = self.store_revision_object(data)
        document_id = uuid.uuid4().hex
        revision_id = digest
        return self.append_event(
            "document_added",
            {
                "document_id": document_id,
                "current_path": str(rel),
                "revision_id": revision_id,
                "content_sha256": digest,
                "byte_length": len(data),
                "line_count": count_lines(text),
                "source_path": str(rel),
            },
        )

    def maybe_move_document(self, conn: sqlite3.Connection, document_id: str, current_path: str, new_path: str) -> None:
        if current_path != new_path:
            self.append_event("document_moved", {"document_id": document_id, "current_path": new_path})

    def update_document(self, ref: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            doc = self.resolve_document(conn, ref)
            revision = self.current_revision(conn, doc["document_id"])
        path = self.root / doc["current_path"]
        data = ensure_utf8_bytes(path)
        digest = sha256_bytes(data)
        if digest == revision["content_sha256"]:
            return None
        text = data.decode("utf-8")
        self.store_revision_object(data)
        event = self.append_event(
            "document_updated",
            {
                "document_id": doc["document_id"],
                "current_path": doc["current_path"],
                "revision_id": digest,
                "content_sha256": digest,
                "byte_length": len(data),
                "line_count": count_lines(text),
                "source_path": doc["current_path"],
                "parent_revision_id": revision["revision_id"],
            },
        )
        self.relocate_annotations(doc["document_id"], revision["revision_id"], digest)
        return event

    def relocate_annotations(self, document_id: str, old_revision_id: str, new_revision_id: str) -> None:
        old_bytes = (self.objects_dir / old_revision_id).read_bytes()
        new_bytes = (self.objects_dir / new_revision_id).read_bytes()
        new_text = safe_decode(new_bytes)
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM annotations
                WHERE document_id = ? AND document_revision_id = ? AND is_active = 1
                """,
                (document_id, old_revision_id),
            ).fetchall()
        for row in rows:
            if row["scope_type"] == "document":
                self.append_event(
                    "annotation_reanchored",
                    {
                        "annotation_id": row["annotation_id"],
                        "document_revision_id": new_revision_id,
                        "start_byte": None,
                        "end_byte": None,
                        "start_line": None,
                        "end_line": None,
                        "exact_text": None,
                        "prefix_context": None,
                        "suffix_context": None,
                        "anchor_status": "clean",
                    },
                )
                continue
            start = row["start_byte"]
            end = row["end_byte"]
            exact_text = row["exact_text"] or ""
            if start is None or end is None:
                continue
            if end <= len(new_bytes) and safe_decode(new_bytes[start:end]) == exact_text:
                start_line, end_line = byte_to_line_range(new_text, start, end)
                self.append_event(
                    "annotation_reanchored",
                    self.make_anchor_payload(row["annotation_id"], new_revision_id, new_bytes, start, end, "clean", start_line, end_line),
                )
                continue
            prefix = row["prefix_context"] or ""
            suffix = row["suffix_context"] or ""
            candidates: list[tuple[int, int]] = []
            seek = exact_text.encode("utf-8")
            offset = 0
            while seek and (found := new_bytes.find(seek, offset)) != -1:
                candidates.append((found, found + len(seek)))
                offset = found + 1
            if len(candidates) == 1:
                start, end = candidates[0]
                window_prefix = safe_decode(new_bytes[max(0, start - CONTEXT_BYTES):start])
                window_suffix = safe_decode(new_bytes[end:end + CONTEXT_BYTES])
                similarity = 0.0
                if prefix == window_prefix or suffix == window_suffix:
                    similarity = 1.0
                elif prefix or suffix:
                    matches = 0
                    total = 0
                    if prefix:
                        total += 1
                        matches += int(prefix in window_prefix or window_prefix in prefix)
                    if suffix:
                        total += 1
                        matches += int(suffix in window_suffix or window_suffix in suffix)
                    similarity = matches / total if total else 0.0
                if similarity >= FUZZY_RELOCATION_THRESHOLD or similarity == 1.0 or not (prefix or suffix):
                    start_line, end_line = byte_to_line_range(new_text, start, end)
                    self.append_event(
                        "annotation_reanchored",
                        self.make_anchor_payload(row["annotation_id"], new_revision_id, new_bytes, start, end, "relocated", start_line, end_line),
                    )
                    continue
            self.append_event(
                "annotation_conflicted",
                {
                    "annotation_id": row["annotation_id"],
                    "memo": f"automatic relocation failed when moving from {old_revision_id} to {new_revision_id}",
                },
            )

    def make_anchor_payload(
        self,
        annotation_id: str,
        revision_id: str,
        content: bytes,
        start: int,
        end: int,
        status: str,
        start_line: int,
        end_line: int,
    ) -> dict[str, Any]:
        return {
            "annotation_id": annotation_id,
            "document_revision_id": revision_id,
            "start_byte": start,
            "end_byte": end,
            "start_line": start_line,
            "end_line": end_line,
            "exact_text": safe_decode(content[start:end]),
            "prefix_context": safe_decode(content[max(0, start - CONTEXT_BYTES):start]),
            "suffix_context": safe_decode(content[end:end + CONTEXT_BYTES]),
            "anchor_status": status,
        }

    def code_name_taken(self, conn: sqlite3.Connection, name: str) -> bool:
        row = conn.execute(
            """
            SELECT 1
            FROM codes c
            LEFT JOIN code_aliases a ON a.alias_name = ?
            WHERE c.canonical_name = ? OR a.alias_name = ?
            LIMIT 1
            """,
            (name, name, name),
        ).fetchone()
        return row is not None

    def add_code(self, name: str, description: str | None = None, color: str | None = None) -> dict[str, Any]:
        with self.connect() as conn:
            if self.code_name_taken(conn, name):
                raise BewleyError(f"code name already exists: {name}")
        return self.append_event(
            "code_created",
            {"code_id": uuid.uuid4().hex, "canonical_name": name, "description": description, "color": color},
        )

    def rename_code(self, old_ref: str, new_name: str) -> dict[str, Any]:
        with self.connect() as conn:
            code = self.resolve_code(conn, old_ref)
            if self.code_name_taken(conn, new_name):
                raise BewleyError(f"code name already exists: {new_name}")
        return self.append_event(
            "code_renamed",
            {"code_id": code["code_id"], "old_name": code["canonical_name"], "new_name": new_name},
        )

    def alias_code(self, ref: str, alias_name: str) -> dict[str, Any]:
        with self.connect() as conn:
            code = self.resolve_code(conn, ref)
            if self.code_name_taken(conn, alias_name):
                raise BewleyError(f"alias name already exists: {alias_name}")
        return self.append_event("code_aliased", {"code_id": code["code_id"], "alias_name": alias_name})

    def merge_codes(self, sources: list[str], target_ref: str) -> dict[str, Any]:
        with self.connect() as conn:
            target = self.resolve_code(conn, target_ref)
            resolved = [self.resolve_code(conn, src) for src in sources]
        source_ids = [row["code_id"] for row in resolved if row["code_id"] != target["code_id"]]
        if not source_ids:
            raise BewleyError("merge requires at least one source distinct from target")
        return self.append_event(
            "code_merged",
            {"source_code_ids": source_ids, "target_code_id": target["code_id"]},
        )

    def split_code(
        self,
        source_ref: str,
        new_name: str,
        annotation_ids: list[str],
        description: str | None = None,
        color: str | None = None,
    ) -> dict[str, Any]:
        if not annotation_ids:
            raise BewleyError("split requires at least one --annotation id")
        with self.connect() as conn:
            source = self.resolve_code(conn, source_ref)
            if self.code_name_taken(conn, new_name):
                raise BewleyError(f"code name already exists: {new_name}")
            rows = conn.execute(
                """
                SELECT annotation_id
                FROM annotations
                WHERE code_id = ? AND is_active = 1 AND annotation_id IN ({})
                """.format(",".join("?" for _ in annotation_ids)),
                (source["code_id"], *annotation_ids),
            ).fetchall()
        found_ids = {row["annotation_id"] for row in rows}
        missing = [annotation_id for annotation_id in annotation_ids if annotation_id not in found_ids]
        if missing:
            raise BewleyError(f"annotations not active on source code: {', '.join(missing)}")
        return self.append_event(
            "code_split",
            {
                "source_code_id": source["code_id"],
                "new_code_id": uuid.uuid4().hex,
                "new_canonical_name": new_name,
                "annotation_ids": annotation_ids,
                "description": description,
                "color": color,
            },
        )

    def add_annotation(
        self,
        code_ref: str,
        document_ref: str,
        scope_type: str,
        byte_range: tuple[int, int] | None,
        memo: str | None,
    ) -> dict[str, Any]:
        with self.connect() as conn:
            code = self.resolve_code(conn, code_ref)
            document = self.resolve_document(conn, document_ref)
            revision = self.current_revision(conn, document["document_id"])
        content = (self.objects_dir / revision["revision_id"]).read_bytes()
        text = safe_decode(content)
        payload: dict[str, Any] = {
            "annotation_id": uuid.uuid4().hex,
            "code_id": code["code_id"],
            "document_id": document["document_id"],
            "document_revision_id": revision["revision_id"],
            "scope_type": scope_type,
            "anchor_status": "clean",
            "memo": memo,
        }
        if scope_type == "span":
            if byte_range is None:
                raise BewleyError("span annotation requires byte range")
            start, end = byte_range
            if start < 0 or end <= start or end > len(content):
                raise BewleyError("invalid byte range")
            exact_bytes = content[start:end]
            try:
                exact_text = exact_bytes.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise BewleyError("byte range does not align to UTF-8 boundaries") from exc
            start_line, end_line = byte_to_line_range(text, start, end)
            payload.update(
                {
                    "start_byte": start,
                    "end_byte": end,
                    "start_line": start_line,
                    "end_line": end_line,
                    "exact_text": exact_text,
                    "prefix_context": safe_decode(content[max(0, start - CONTEXT_BYTES):start]),
                    "suffix_context": safe_decode(content[end:end + CONTEXT_BYTES]),
                }
            )
        return self.append_event("annotation_added", payload)

    def remove_annotation(self, annotation_id: str) -> dict[str, Any]:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM annotations WHERE annotation_id = ?", (annotation_id,)).fetchone()
            if row is None:
                raise BewleyError(f"unknown annotation id: {annotation_id}")
            if not row["is_active"]:
                raise BewleyError("annotation already inactive")
        return self.append_event("annotation_removed", {"annotation_id": annotation_id})

    def resolve_annotation(self, annotation_id: str, byte_range: tuple[int, int], memo: str | None) -> dict[str, Any]:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM annotations WHERE annotation_id = ?", (annotation_id,)).fetchone()
            if row is None:
                raise BewleyError(f"unknown annotation id: {annotation_id}")
            doc = conn.execute("SELECT * FROM documents WHERE document_id = ?", (row["document_id"],)).fetchone()
            revision = self.current_revision(conn, row["document_id"])
        content = (self.objects_dir / revision["revision_id"]).read_bytes()
        text = safe_decode(content)
        start, end = byte_range
        if start < 0 or end <= start or end > len(content):
            raise BewleyError("invalid byte range")
        start_line, end_line = byte_to_line_range(text, start, end)
        payload = self.make_anchor_payload(annotation_id, revision["revision_id"], content, start, end, "relocated", start_line, end_line)
        payload["memo"] = memo
        payload["document_id"] = doc["document_id"]
        return self.append_event("annotation_resolved", payload)

    def query_documents(self, expr_text: str) -> list[sqlite3.Row]:
        expr = ExprParser(expr_text).parse()
        with self.connect() as conn:
            docs = conn.execute("SELECT * FROM documents ORDER BY current_path").fetchall()
            matches: list[sqlite3.Row] = []
            for doc in docs:
                names = {
                    row["canonical_name"]
                    for row in conn.execute(
                        """
                        SELECT DISTINCT c.canonical_name
                        FROM annotations a
                        JOIN codes c ON c.code_id = a.code_id
                        WHERE a.document_id = ? AND a.is_active = 1
                        """,
                        (doc["document_id"],),
                    )
                }
                if expr.evaluate(names):
                    matches.append(doc)
            return matches

    def query_annotations(self, expr_text: str) -> list[sqlite3.Row]:
        expr = ExprParser(expr_text).parse()
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT a.*, c.canonical_name, d.current_path
                FROM annotations a
                JOIN codes c ON c.code_id = a.code_id
                JOIN documents d ON d.document_id = a.document_id
                WHERE a.is_active = 1
                ORDER BY d.current_path, COALESCE(a.start_byte, -1), a.annotation_id
                """
            ).fetchall()
            doc_groups: dict[str, list[sqlite3.Row]] = {}
            for row in rows:
                doc_groups.setdefault(row["document_id"], []).append(row)
            matches: list[sqlite3.Row] = []
            for group in doc_groups.values():
                for row in group:
                    comparable = [other for other in group if annotation_overlap(row, other)]
                    names = {item["canonical_name"] for item in comparable}
                    if expr.evaluate(names):
                        matches.append(row)
            return matches

    def fsck(self) -> list[str]:
        problems: list[str] = []
        events = self.all_events()
        seen_sequences: set[int] = set()
        for event in events:
            copied = dict(event)
            event_sha = copied.pop("event_sha256", None)
            expected = sha256_text(json.dumps(copied, ensure_ascii=False, sort_keys=True))
            if event_sha != expected:
                problems.append(f"event hash mismatch: {event['event_id']}")
            seq = event["sequence_number"]
            if seq in seen_sequences:
                problems.append(f"duplicate sequence number: {seq}")
            seen_sequences.add(seq)
            payload = event["payload"]
            if "content_sha256" in payload and not (self.objects_dir / payload["content_sha256"]).exists():
                problems.append(f"missing revision object: {payload['content_sha256']}")
        temp_db = self.db_path.with_suffix(".fsck.sqlite")
        with contextlib.suppress(FileNotFoundError):
            temp_db.unlink()
        conn = sqlite3.connect(temp_db)
        conn.row_factory = sqlite3.Row
        self._init_connection(conn)
        for event in events:
            self.apply_event(conn, event)
        conn.commit()
        with self.connect() as actual:
            for table in ["documents", "document_revisions", "codes", "code_aliases", "annotations", "events"]:
                actual_count = actual.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                rebuilt_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                if actual_count != rebuilt_count:
                    problems.append(f"projection count mismatch for {table}: actual={actual_count} rebuilt={rebuilt_count}")
        conn.close()
        with contextlib.suppress(FileNotFoundError):
            temp_db.unlink()
        return problems

    def history(self, *, document_ref: str | None = None, code_ref: str | None = None, annotation_id: str | None = None) -> list[dict[str, Any]]:
        events = self.all_events()
        if document_ref is None and code_ref is None and annotation_id is None:
            return events
        with self.connect() as conn:
            document_id = self.resolve_document(conn, document_ref)["document_id"] if document_ref else None
            code_id = self.resolve_code(conn, code_ref)["code_id"] if code_ref else None
        filtered = []
        for event in events:
            payload = event["payload"]
            if document_id and payload.get("document_id") == document_id:
                filtered.append(event)
                continue
            if code_id and code_id in {
                payload.get("code_id"),
                payload.get("source_code_id"),
                payload.get("target_code_id"),
                payload.get("new_code_id"),
            }:
                filtered.append(event)
                continue
            if annotation_id and payload.get("annotation_id") == annotation_id:
                filtered.append(event)
        return filtered

    def undo(self, event_id: str) -> dict[str, Any]:
        target = None
        for event in self.all_events():
            if event["event_id"] == event_id:
                target = event
                break
        if target is None:
            raise BewleyError(f"unknown event id: {event_id}")
        if target["event_type"] not in {"code_renamed", "annotation_added"}:
            raise BewleyError(f"undo not supported for event type: {target['event_type']}")
        return self.append_event(
            "undo_recorded",
            {
                "undone_event_id": event_id,
                "undone_event_type": target["event_type"],
                "original_payload": target["payload"],
            },
        )


def parse_byte_range(spec: str) -> tuple[int, int]:
    try:
        start_text, end_text = spec.split(":", 1)
        start = int(start_text)
        end = int(end_text)
    except ValueError as exc:
        raise BewleyError("expected range format START:END") from exc
    return start, end


def print_table(rows: list[tuple[Any, ...]]) -> None:
    for row in rows:
        print("\t".join("" if item is None else str(item) for item in row))


def default_code_color(name: str) -> str:
    digest = sha256_text(name)
    hue = int(digest[:6], 16) % 360
    return f"hsl({hue} 68% 52%)"


def coerce_code_color(color: str | None, name: str) -> str:
    if color and color.strip():
        return color.strip()
    return default_code_color(name)


def soft_color(color: str, alpha: float) -> str:
    value = color.strip()
    if value.startswith("#"):
        hex_part = value[1:]
        if len(hex_part) == 3:
            hex_part = "".join(ch * 2 for ch in hex_part)
        if len(hex_part) == 6:
            red = int(hex_part[0:2], 16)
            green = int(hex_part[2:4], 16)
            blue = int(hex_part[4:6], 16)
            return f"rgba({red}, {green}, {blue}, {alpha})"
    if value.startswith("hsl(") and value.endswith(")"):
        return f"hsl({value[4:-1]} / {alpha})"
    if value.startswith("rgb(") and value.endswith(")"):
        return f"rgb({value[4:-1]} / {alpha})"
    return value


def byte_to_char_index_map(text: str) -> dict[int, int]:
    mapping = {0: 0}
    byte_offset = 0
    for index, char in enumerate(text, start=1):
        byte_offset += len(char.encode("utf-8"))
        mapping[byte_offset] = index
    return mapping


def render_annotated_document_html(text: str, spans: list[dict[str, Any]]) -> str:
    mapping = byte_to_char_index_map(text)
    boundaries = {0, len(text)}
    for span in spans:
        boundaries.add(mapping[span["start_byte"]])
        boundaries.add(mapping[span["end_byte"]])
    ordered = sorted(boundaries)
    pieces: list[str] = []
    for start, end in zip(ordered, ordered[1:]):
        segment = text[start:end]
        covering = [
            span for span in spans
            if mapping[span["start_byte"]] <= start and end <= mapping[span["end_byte"]]
        ]
        escaped = html.escape(segment)
        if not covering:
            pieces.append(escaped)
            continue
        class_names = " ".join(f"code-{span['code_slug']}" for span in covering)
        annotation_ids = ",".join(span["annotation_id"] for span in covering)
        if len(covering) == 1:
            background = covering[0]["highlight_color"]
        else:
            stripes = []
            width = 100 / len(covering)
            for idx, span in enumerate(covering):
                start_pct = idx * width
                end_pct = (idx + 1) * width
                stripes.append(f"{span['highlight_color']} {start_pct:.4f}% {end_pct:.4f}%")
            background = f"linear-gradient(90deg, {', '.join(stripes)})"
        label = " + ".join(span["code_name"] for span in covering)
        tooltip = "\n\n".join(
            "\n".join(
                part
                for part in [
                    span["code_name"],
                    f"Lines {span['start_line']}-{span['end_line']}" if span.get("start_line") is not None else None,
                    span.get("memo"),
                ]
                if part
            )
            for span in covering
        )
        pieces.append(
            f'<mark class="anno-segment {class_names}" data-annotation-ids="{annotation_ids}" '
            f'data-code-names="{html.escape(label)}" title="{html.escape(tooltip)}" '
            f'style="--segment-bg: {background};">{escaped}</mark>'
        )
    return "".join(pieces)


def build_code_explorer_html(payload: dict[str, Any], title: str) -> str:
    safe_title = html.escape(title)
    data_json = json.dumps(payload, ensure_ascii=False)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{safe_title}</title>
  <style>
    :root {{
      --bg: #f4efe5;
      --panel: rgba(255, 250, 242, 0.82);
      --panel-strong: rgba(255, 248, 237, 0.96);
      --ink: #1f1b18;
      --muted: #6f645c;
      --accent: #b74d2c;
      --accent-soft: rgba(183, 77, 44, 0.12);
      --border: rgba(64, 48, 37, 0.14);
      --shadow: 0 18px 60px rgba(61, 42, 24, 0.12);
      --radius: 22px;
      --mono: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
      --sans: "Avenir Next", "Segoe UI", sans-serif;
    }}
    * {{
      box-sizing: border-box;
    }}
    html, body {{
      margin: 0;
      min-height: 100%;
      background:
        radial-gradient(circle at top left, rgba(255, 209, 102, 0.22), transparent 32%),
        radial-gradient(circle at top right, rgba(183, 77, 44, 0.16), transparent 34%),
        linear-gradient(180deg, #f7f0e7 0%, var(--bg) 100%);
      color: var(--ink);
      font-family: var(--sans);
    }}
    body {{
      padding: 32px 18px 48px;
    }}
    .shell {{
      max-width: 1280px;
      margin: 0 auto;
      display: grid;
      gap: 18px;
    }}
    .hero, .controls, .summary, .sidebar, .main {{
      backdrop-filter: blur(18px);
      background: var(--panel);
      border: 1px solid var(--border);
      box-shadow: var(--shadow);
      border-radius: var(--radius);
    }}
    .hero {{
      overflow: hidden;
      position: relative;
      padding: 28px;
    }}
    .hero::after {{
      content: "";
      position: absolute;
      inset: auto -10% -35% 35%;
      height: 220px;
      background: linear-gradient(90deg, rgba(183, 77, 44, 0), rgba(183, 77, 44, 0.28));
      transform: rotate(-8deg);
      pointer-events: none;
    }}
    .eyebrow {{
      margin: 0 0 10px;
      color: var(--accent);
      text-transform: uppercase;
      letter-spacing: 0.12em;
      font-size: 12px;
      font-weight: 700;
    }}
    h1 {{
      margin: 0;
      font-size: clamp(2rem, 5vw, 4.2rem);
      line-height: 0.95;
      max-width: 11ch;
    }}
    .hero p {{
      margin: 16px 0 0;
      max-width: 64ch;
      color: var(--muted);
      font-size: 1rem;
      line-height: 1.5;
    }}
    .summary {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 12px;
      padding: 14px;
    }}
    .stat {{
      background: var(--panel-strong);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 14px;
    }}
    .stat-label {{
      margin: 0;
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .stat-value {{
      margin: 10px 0 0;
      font-size: 1.8rem;
      font-weight: 700;
    }}
    .controls {{
      display: grid;
      gap: 14px;
      padding: 16px;
    }}
    .toolbar {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
      justify-content: space-between;
    }}
    .search {{
      flex: 1 1 280px;
      min-width: 220px;
      display: flex;
      align-items: center;
      gap: 10px;
      background: var(--panel-strong);
      border: 1px solid var(--border);
      border-radius: 999px;
      padding: 0 14px;
    }}
    .search input {{
      width: 100%;
      border: 0;
      background: transparent;
      color: var(--ink);
      padding: 12px 0;
      outline: none;
      font: inherit;
    }}
    .toggle-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
    }}
    button, .scope-pill {{
      border: 1px solid var(--border);
      background: var(--panel-strong);
      color: var(--ink);
      border-radius: 999px;
      padding: 9px 14px;
      font: inherit;
      cursor: pointer;
      transition: 160ms ease;
    }}
    button:hover {{
      transform: translateY(-1px);
      border-color: rgba(64, 48, 37, 0.26);
    }}
    button.is-active {{
      background: var(--accent);
      color: white;
      border-color: transparent;
    }}
    .layout {{
      display: grid;
      grid-template-columns: minmax(280px, 360px) minmax(0, 1fr);
      gap: 18px;
      align-items: start;
    }}
    .sidebar, .main {{
      padding: 16px;
    }}
    .sidebar h2, .main h2 {{
      margin: 0 0 14px;
      font-size: 1rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .code-list {{
      display: grid;
      gap: 10px;
      max-height: 72vh;
      overflow: auto;
      padding-right: 4px;
    }}
    .code-card {{
      border: 1px solid var(--border);
      background: var(--panel-strong);
      border-radius: 18px;
      padding: 14px;
      cursor: pointer;
      transition: 180ms ease;
    }}
    .code-card:hover {{
      transform: translateY(-1px);
      box-shadow: 0 10px 28px rgba(61, 42, 24, 0.1);
    }}
    .code-card.is-selected {{
      border-color: transparent;
      box-shadow: inset 0 0 0 2px var(--accent);
    }}
    .code-top {{
      display: flex;
      gap: 10px;
      align-items: center;
    }}
    .swatch {{
      width: 14px;
      height: 14px;
      border-radius: 999px;
      flex: 0 0 auto;
      box-shadow: inset 0 0 0 1px rgba(0, 0, 0, 0.08);
    }}
    .code-name {{
      font-weight: 700;
      line-height: 1.2;
      word-break: break-word;
    }}
    .code-meta {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 12px;
      color: var(--muted);
      font-size: 0.9rem;
    }}
    .code-description {{
      margin: 10px 0 0;
      color: var(--muted);
      font-size: 0.95rem;
      line-height: 1.45;
    }}
    .snippet-list {{
      display: grid;
      gap: 12px;
    }}
    .snippet {{
      border: 1px solid var(--border);
      border-radius: 20px;
      padding: 16px;
      background: var(--panel-strong);
    }}
    .snippet-head {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      justify-content: space-between;
      align-items: center;
    }}
    .snippet-title {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
      font-weight: 700;
    }}
    .chip {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border-radius: 999px;
      background: var(--accent-soft);
      padding: 5px 10px;
      color: var(--accent);
      font-size: 0.84rem;
      font-weight: 700;
    }}
    .snippet-meta {{
      color: var(--muted);
      font-size: 0.9rem;
    }}
    .snippet pre {{
      margin: 14px 0 0;
      padding: 14px;
      border-radius: 16px;
      background: #201914;
      color: #fff7ee;
      overflow: auto;
      white-space: pre-wrap;
      font-family: var(--mono);
      font-size: 0.92rem;
      line-height: 1.55;
    }}
    .snippet p {{
      margin: 14px 0 0;
      color: var(--muted);
      line-height: 1.5;
    }}
    .empty {{
      border: 1px dashed var(--border);
      border-radius: 18px;
      padding: 24px;
      color: var(--muted);
      text-align: center;
      background: rgba(255, 255, 255, 0.36);
    }}
    .footer {{
      color: var(--muted);
      font-size: 0.9rem;
      text-align: right;
    }}
    @media (max-width: 900px) {{
      .layout {{
        grid-template-columns: 1fr;
      }}
      .code-list {{
        max-height: none;
      }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <p class="eyebrow">Bewley Code Explorer</p>
      <h1>{safe_title}</h1>
      <p>Browse the active codebook, inspect snippet density, filter by document and scope, and search directly across coded text without leaving the project.</p>
    </section>
    <section class="summary" id="summary"></section>
    <section class="controls">
      <div class="toolbar">
        <label class="search" aria-label="Search snippets and code names">
          <span>Search</span>
          <input id="search" type="search" placeholder="code, memo, document, or snippet text">
        </label>
        <div class="toggle-row">
          <button class="is-active" data-scope="all" type="button">All scopes</button>
          <button data-scope="span" type="button">Span only</button>
          <button data-scope="document" type="button">Document only</button>
          <button id="clear-filters" type="button">Clear filters</button>
        </div>
      </div>
    </section>
    <div class="layout">
      <aside class="sidebar">
        <h2>Codes</h2>
        <div class="code-list" id="code-list"></div>
      </aside>
      <main class="main">
        <h2>Snippets</h2>
        <div class="snippet-list" id="snippet-list"></div>
      </main>
    </div>
    <div class="footer" id="footer"></div>
  </div>
  <script>
    const data = {data_json};
    const state = {{
      selectedCode: null,
      scope: "all",
      search: "",
    }};

    const codeListEl = document.getElementById("code-list");
    const snippetListEl = document.getElementById("snippet-list");
    const summaryEl = document.getElementById("summary");
    const footerEl = document.getElementById("footer");
    const searchEl = document.getElementById("search");
    const scopeButtons = Array.from(document.querySelectorAll("[data-scope]"));

    function fmtCount(value, singular, plural) {{
      return `${{value}} ${{value === 1 ? singular : plural}}`;
    }}

    function escapeHtml(value) {{
      return value
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;");
    }}

    function cardForCode(code) {{
      const selected = state.selectedCode === code.code_id;
      const aliases = code.aliases.length ? `Aliases: ${{code.aliases.join(", ")}}` : "No aliases";
      return `
        <article class="code-card ${{selected ? "is-selected" : ""}}" data-code-id="${{code.code_id}}">
          <div class="code-top">
            <span class="swatch" style="background:${{code.display_color}}"></span>
            <div class="code-name">${{escapeHtml(code.name)}}</div>
          </div>
          <div class="code-meta">
            <span>${{fmtCount(code.annotation_count, "annotation", "annotations")}}</span>
            <span>${{fmtCount(code.document_count, "document", "documents")}}</span>
            <span>${{escapeHtml(code.status)}}</span>
          </div>
          <p class="code-description">${{escapeHtml(code.description || aliases)}}</p>
        </article>
      `;
    }}

    function snippetCard(snippet) {{
      const range = snippet.scope_type === "document"
        ? "Whole document"
        : `Lines ${{snippet.start_line}}-${{snippet.end_line}}`;
      const text = snippet.scope_type === "document"
        ? "<document-level annotation>"
        : escapeHtml(snippet.exact_text || "");
      const memo = snippet.memo ? `<p>${{escapeHtml(snippet.memo)}}</p>` : "";
      return `
        <article class="snippet">
          <div class="snippet-head">
            <div class="snippet-title">
              <span class="chip">
                <span class="swatch" style="background:${{snippet.code_color}}"></span>
                ${{escapeHtml(snippet.code_name)}}
              </span>
              <span class="scope-pill">${{escapeHtml(snippet.scope_type)}}</span>
            </div>
            <div class="snippet-meta">${{escapeHtml(snippet.document_path)}} · ${{escapeHtml(range)}} · ${{escapeHtml(snippet.anchor_status)}}</div>
          </div>
          <pre>${{text}}</pre>
          ${{memo}}
        </article>
      `;
    }}

    function matchesSnippet(snippet) {{
      if (state.selectedCode && snippet.code_id !== state.selectedCode) {{
        return false;
      }}
      if (state.scope !== "all" && snippet.scope_type !== state.scope) {{
        return false;
      }}
      if (!state.search) {{
        return true;
      }}
      const haystack = [
        snippet.code_name,
        snippet.document_path,
        snippet.memo || "",
        snippet.exact_text || "",
        snippet.anchor_status,
      ].join("\\n").toLowerCase();
      return haystack.includes(state.search);
    }}

    function renderSummary(snippets) {{
      const activeCodes = state.selectedCode ? 1 : data.codes.length;
      const docs = new Set(snippets.map((snippet) => snippet.document_path));
      const conflicted = snippets.filter((snippet) => snippet.anchor_status === "conflicted").length;
      summaryEl.innerHTML = [
        ["Visible codes", activeCodes],
        ["Visible snippets", snippets.length],
        ["Visible documents", docs.size],
        ["Conflicted anchors", conflicted],
      ].map(([label, value]) => `
        <div class="stat">
          <p class="stat-label">${{label}}</p>
          <p class="stat-value">${{value}}</p>
        </div>
      `).join("");
    }}

    function renderCodes() {{
      codeListEl.innerHTML = data.codes.map(cardForCode).join("");
      for (const node of codeListEl.querySelectorAll(".code-card")) {{
        node.addEventListener("click", () => {{
          const codeId = node.getAttribute("data-code-id");
          state.selectedCode = state.selectedCode === codeId ? null : codeId;
          render();
        }});
      }}
    }}

    function renderSnippets() {{
      const filtered = data.snippets.filter(matchesSnippet);
      if (!filtered.length) {{
        snippetListEl.innerHTML = `<div class="empty">No coded snippets match the current filters.</div>`;
      }} else {{
        snippetListEl.innerHTML = filtered.map(snippetCard).join("");
      }}
      renderSummary(filtered);
      footerEl.textContent = `Generated from ${{data.project_root}} on ${{data.generated_at}}`;
    }}

    function render() {{
      scopeButtons.forEach((button) => {{
        button.classList.toggle("is-active", button.dataset.scope === state.scope);
      }});
      renderCodes();
      renderSnippets();
    }}

    searchEl.addEventListener("input", (event) => {{
      state.search = event.target.value.trim().toLowerCase();
      renderSnippets();
    }});

    for (const button of scopeButtons) {{
      button.addEventListener("click", () => {{
        state.scope = button.dataset.scope;
        render();
      }});
    }}

    document.getElementById("clear-filters").addEventListener("click", () => {{
      state.selectedCode = null;
      state.scope = "all";
      state.search = "";
      searchEl.value = "";
      render();
    }});

    render();
  </script>
</body>
</html>
"""


def build_document_viewer_html(payload: dict[str, Any], title: str) -> str:
    safe_title = html.escape(title)
    data_json = json.dumps(payload, ensure_ascii=False)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{safe_title}</title>
  <style>
    :root {{
      --bg: #f7f2e9;
      --panel: rgba(255, 251, 244, 0.86);
      --panel-strong: rgba(255, 250, 242, 0.98);
      --ink: #221d1a;
      --muted: #6d6259;
      --accent: #8b4a2d;
      --border: rgba(54, 42, 31, 0.14);
      --shadow: 0 20px 60px rgba(58, 39, 22, 0.12);
      --radius: 22px;
      --mono: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
      --sans: "Avenir Next", "Segoe UI", sans-serif;
    }}
    * {{ box-sizing: border-box; }}
    html, body {{
      margin: 0;
      min-height: 100%;
      background:
        radial-gradient(circle at top left, rgba(255, 204, 116, 0.18), transparent 34%),
        radial-gradient(circle at top right, rgba(139, 74, 45, 0.14), transparent 32%),
        linear-gradient(180deg, #fbf6ee 0%, var(--bg) 100%);
      color: var(--ink);
      font-family: var(--sans);
    }}
    body {{ padding: 24px 16px 40px; }}
    .shell {{
      max-width: 1360px;
      margin: 0 auto;
      display: grid;
      gap: 18px;
    }}
    .hero, .controls, .sidebar, .document-panel {{
      backdrop-filter: blur(18px);
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
    }}
    .hero {{
      padding: 26px;
      position: relative;
      overflow: hidden;
    }}
    .hero::after {{
      content: "";
      position: absolute;
      inset: auto -10% -40% 40%;
      height: 220px;
      background: linear-gradient(90deg, rgba(139, 74, 45, 0), rgba(139, 74, 45, 0.25));
      transform: rotate(-8deg);
    }}
    .eyebrow {{
      margin: 0 0 8px;
      color: var(--accent);
      text-transform: uppercase;
      letter-spacing: 0.12em;
      font-size: 12px;
      font-weight: 700;
    }}
    h1 {{
      margin: 0;
      font-size: clamp(1.8rem, 5vw, 3.8rem);
      line-height: 0.96;
      max-width: 12ch;
    }}
    .hero p {{
      margin: 14px 0 0;
      max-width: 70ch;
      color: var(--muted);
      line-height: 1.5;
    }}
    .meta-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 12px;
      margin-top: 18px;
    }}
    .stat {{
      background: var(--panel-strong);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 14px;
    }}
    .stat-label {{
      margin: 0;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-size: 12px;
    }}
    .stat-value {{
      margin: 8px 0 0;
      font-size: 1.7rem;
      font-weight: 700;
    }}
    .controls {{
      padding: 16px;
      display: grid;
      gap: 14px;
    }}
    .toolbar {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
      justify-content: space-between;
    }}
    .search {{
      flex: 1 1 280px;
      min-width: 220px;
      display: flex;
      align-items: center;
      gap: 10px;
      border: 1px solid var(--border);
      background: var(--panel-strong);
      border-radius: 999px;
      padding: 0 14px;
    }}
    .search input {{
      width: 100%;
      border: 0;
      background: transparent;
      color: var(--ink);
      padding: 12px 0;
      outline: none;
      font: inherit;
    }}
    button {{
      border: 1px solid var(--border);
      background: var(--panel-strong);
      color: var(--ink);
      border-radius: 999px;
      padding: 9px 14px;
      font: inherit;
      cursor: pointer;
    }}
    button.is-active {{
      background: var(--accent);
      border-color: transparent;
      color: white;
    }}
    .layout {{
      display: grid;
      grid-template-columns: minmax(280px, 360px) minmax(0, 1fr);
      gap: 18px;
      align-items: start;
    }}
    .sidebar, .document-panel {{
      padding: 16px;
    }}
    .sidebar h2, .document-panel h2 {{
      margin: 0 0 14px;
      font-size: 1rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .legend {{
      display: grid;
      gap: 10px;
      max-height: 72vh;
      overflow: auto;
      padding-right: 4px;
    }}
    .legend-item {{
      border: 1px solid var(--border);
      background: var(--panel-strong);
      border-radius: 18px;
      padding: 14px;
      cursor: pointer;
      transition: 160ms ease;
    }}
    .legend-item.is-selected {{
      box-shadow: inset 0 0 0 2px var(--accent);
      border-color: transparent;
    }}
    .legend-top {{
      display: flex;
      gap: 10px;
      align-items: center;
    }}
    .swatch {{
      width: 14px;
      height: 14px;
      border-radius: 999px;
      flex: 0 0 auto;
      box-shadow: inset 0 0 0 1px rgba(0,0,0,0.08);
    }}
    .legend-name {{
      font-weight: 700;
      word-break: break-word;
    }}
    .legend-meta {{
      margin-top: 10px;
      color: var(--muted);
      font-size: 0.92rem;
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }}
    .doc-frame {{
      border: 1px solid var(--border);
      border-radius: 20px;
      background: #fffdf8;
      overflow: hidden;
    }}
    .doc-header {{
      padding: 14px 16px;
      border-bottom: 1px solid var(--border);
      background: linear-gradient(180deg, rgba(255,255,255,0.7), rgba(255,251,244,0.96));
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      gap: 8px;
      color: var(--muted);
      font-size: 0.92rem;
    }}
    .doc-tags {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 14px;
    }}
    .tag {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px 10px;
      border-radius: 999px;
      background: var(--panel-strong);
      border: 1px solid var(--border);
      font-size: 0.86rem;
      color: var(--muted);
    }}
    .document-text {{
      margin: 0;
      padding: 18px 18px 28px;
      font-family: var(--mono);
      white-space: pre-wrap;
      line-height: 1.72;
      font-size: 0.94rem;
    }}
    .anno-segment {{
      background: var(--segment-bg);
      border-radius: 0.35em;
      box-shadow: inset 0 -1px 0 rgba(0,0,0,0.08);
      transition: opacity 140ms ease, box-shadow 140ms ease;
    }}
    .anno-segment.is-dim {{
      opacity: 0.24;
    }}
    .anno-segment.is-match {{
      box-shadow: inset 0 -1px 0 rgba(0,0,0,0.08), 0 0 0 2px rgba(34, 29, 26, 0.22);
    }}
    .annotation-list {{
      margin-top: 18px;
      display: grid;
      gap: 10px;
    }}
    .annotation-chip {{
      border: 1px solid var(--border);
      background: var(--panel-strong);
      border-radius: 16px;
      padding: 12px;
      font-size: 0.92rem;
      color: var(--muted);
      cursor: pointer;
    }}
    .annotation-chip.is-selected {{
      box-shadow: inset 0 0 0 2px var(--accent);
      border-color: transparent;
    }}
    .annotation-chip strong {{
      color: var(--ink);
    }}
    .empty {{
      border: 1px dashed var(--border);
      border-radius: 18px;
      padding: 24px;
      color: var(--muted);
      text-align: center;
    }}
    .footer {{
      text-align: right;
      color: var(--muted);
      font-size: 0.9rem;
    }}
    @media (max-width: 900px) {{
      .layout {{ grid-template-columns: 1fr; }}
      .legend {{ max-height: none; }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <p class="eyebrow">Bewley Annotated Document</p>
      <h1>{safe_title}</h1>
      <p>Inspect one document with inline code highlights. Toggle codes from the legend, search within annotation text and memos, and use the annotation list to focus the document view.</p>
      <div class="meta-grid" id="summary"></div>
    </section>
    <section class="controls">
      <div class="toolbar">
        <label class="search" aria-label="Search annotations">
          <span>Search</span>
          <input id="search" type="search" placeholder="code, memo, or selected text">
        </label>
        <div>
          <button id="show-all" class="is-active" type="button">Show all codes</button>
          <button id="clear-focus" type="button">Clear focus</button>
        </div>
      </div>
    </section>
    <div class="layout">
      <aside class="sidebar">
        <h2>Legend</h2>
        <div class="legend" id="legend"></div>
      </aside>
      <main class="document-panel">
        <h2>Document</h2>
        <div class="doc-tags" id="doc-tags"></div>
        <div class="doc-frame">
          <div class="doc-header">
            <span id="doc-path"></span>
            <span id="doc-meta"></span>
          </div>
          <pre class="document-text" id="document-text"></pre>
        </div>
        <div class="annotation-list" id="annotation-list"></div>
      </main>
    </div>
    <div class="footer" id="footer"></div>
  </div>
  <script>
    const data = {data_json};
    const state = {{
      selectedCode: null,
      selectedAnnotation: null,
      search: "",
    }};

    const summaryEl = document.getElementById("summary");
    const legendEl = document.getElementById("legend");
    const docTagsEl = document.getElementById("doc-tags");
    const docPathEl = document.getElementById("doc-path");
    const docMetaEl = document.getElementById("doc-meta");
    const docTextEl = document.getElementById("document-text");
    const annotationListEl = document.getElementById("annotation-list");
    const footerEl = document.getElementById("footer");
    const searchEl = document.getElementById("search");

    function escapeHtml(value) {{
      return value
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;");
    }}

    function fmtCount(value, singular, plural) {{
      return `${{value}} ${{value === 1 ? singular : plural}}`;
    }}

    function renderSummary() {{
      const rows = [
        ["Codes", data.code_count],
        ["Annotations", data.annotation_count],
        ["Lines", data.line_count],
        ["Document tags", data.document_annotations.length],
      ];
      summaryEl.innerHTML = rows.map(([label, value]) => `
        <div class="stat">
          <p class="stat-label">${{label}}</p>
          <p class="stat-value">${{value}}</p>
        </div>
      `).join("");
    }}

    function matchesAnnotation(annotation) {{
      if (state.selectedCode && annotation.code_id !== state.selectedCode) {{
        return false;
      }}
      if (!state.search) {{
        return true;
      }}
      const haystack = [
        annotation.code_name,
        annotation.memo || "",
        annotation.exact_text || "",
        annotation.anchor_status,
      ].join("\\n").toLowerCase();
      return haystack.includes(state.search);
    }}

    function renderLegend() {{
      legendEl.innerHTML = data.codes.map((code) => `
        <article class="legend-item ${{state.selectedCode === code.code_id ? "is-selected" : ""}}" data-code-id="${{code.code_id}}">
          <div class="legend-top">
            <span class="swatch" style="background:${{code.display_color}}"></span>
            <div class="legend-name">${{escapeHtml(code.name)}}</div>
          </div>
          <div class="legend-meta">
            <span>${{fmtCount(code.annotation_count, "annotation", "annotations")}}</span>
            <span>${{fmtCount(code.document_annotation_count, "document tag", "document tags")}}</span>
          </div>
        </article>
      `).join("");
      for (const node of legendEl.querySelectorAll(".legend-item")) {{
        node.addEventListener("click", () => {{
          const codeId = node.dataset.codeId;
          state.selectedCode = state.selectedCode === codeId ? null : codeId;
          state.selectedAnnotation = null;
          render();
        }});
      }}
    }}

    function renderTags() {{
      const tags = data.document_annotations.filter((annotation) => !state.selectedCode || annotation.code_id === state.selectedCode);
      if (!tags.length) {{
        docTagsEl.innerHTML = `<div class="empty">No document-level codes visible.</div>`;
        return;
      }}
      docTagsEl.innerHTML = tags.map((annotation) => `
        <span class="tag" style="background:${{annotation.highlight_color}}">
          <span class="swatch" style="background:${{annotation.code_color}}"></span>
          ${{escapeHtml(annotation.code_name)}}
        </span>
      `).join("");
    }}

    function renderDocument() {{
      docTextEl.innerHTML = data.rendered_text;
      for (const node of docTextEl.querySelectorAll(".anno-segment")) {{
        const ids = (node.dataset.annotationIds || "").split(",").filter(Boolean);
        const shouldDim = ids.every((annotationId) => {{
          const annotation = data.annotation_index[annotationId];
          return !matchesAnnotation(annotation);
        }});
        node.classList.toggle("is-dim", shouldDim);
        node.classList.toggle("is-match", state.selectedAnnotation && ids.includes(state.selectedAnnotation));
      }}
    }}

    function renderAnnotationList() {{
      const items = data.span_annotations.filter(matchesAnnotation);
      if (!items.length) {{
        annotationListEl.innerHTML = `<div class="empty">No span annotations match the current filters.</div>`;
        return;
      }}
      annotationListEl.innerHTML = items.map((annotation) => `
        <article class="annotation-chip ${{state.selectedAnnotation === annotation.annotation_id ? "is-selected" : ""}}" data-annotation-id="${{annotation.annotation_id}}">
          <strong>${{escapeHtml(annotation.code_name)}}</strong> · lines ${{annotation.start_line}}-${{annotation.end_line}} · ${{escapeHtml(annotation.anchor_status)}}<br>
          ${{annotation.memo ? escapeHtml(annotation.memo) + "<br>" : ""}}
          <span>${{escapeHtml(annotation.exact_text || "")}}</span>
        </article>
      `).join("");
      for (const node of annotationListEl.querySelectorAll(".annotation-chip")) {{
        node.addEventListener("click", () => {{
          const annotationId = node.dataset.annotationId;
          state.selectedAnnotation = state.selectedAnnotation === annotationId ? null : annotationId;
          renderDocument();
          renderAnnotationList();
          const match = docTextEl.querySelector(`[data-annotation-ids*="${{annotationId}}"]`);
          if (match) {{
            match.scrollIntoView({{ behavior: "smooth", block: "center" }});
          }}
        }});
      }}
    }}

    function render() {{
      document.getElementById("show-all").classList.toggle("is-active", !state.selectedCode);
      renderLegend();
      renderTags();
      renderDocument();
      renderAnnotationList();
      docPathEl.textContent = data.document_path;
      docMetaEl.textContent = `${{data.revision_id}} · ${{data.line_count}} lines · ${{data.byte_length}} bytes`;
      footerEl.textContent = `Generated from ${{data.project_root}} on ${{data.generated_at}}`;
    }}

    searchEl.addEventListener("input", (event) => {{
      state.search = event.target.value.trim().toLowerCase();
      state.selectedAnnotation = null;
      render();
    }});

    document.getElementById("show-all").addEventListener("click", () => {{
      state.selectedCode = null;
      state.selectedAnnotation = null;
      render();
    }});

    document.getElementById("clear-focus").addEventListener("click", () => {{
      state.selectedCode = null;
      state.selectedAnnotation = null;
      state.search = "";
      searchEl.value = "";
      render();
    }});

    renderSummary();
    render();
  </script>
</body>
</html>
"""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="bewley")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init")
    sub.add_parser("status")
    sub.add_parser("fsck")
    sub.add_parser("rebuild-index")

    add = sub.add_parser("add")
    add.add_argument("path")

    update = sub.add_parser("update")
    update.add_argument("path")

    list_parser = sub.add_parser("list")
    list_sub = list_parser.add_subparsers(dest="list_what", required=True)
    list_sub.add_parser("documents")

    show = sub.add_parser("show")
    show_sub = show.add_subparsers(dest="show_what", required=True)
    show_doc = show_sub.add_parser("document")
    show_doc.add_argument("document_ref")
    show_snippets = show_sub.add_parser("snippets")
    show_snippets.add_argument("--code", required=True)

    code = sub.add_parser("code")
    code_sub = code.add_subparsers(dest="code_cmd", required=True)
    code_create = code_sub.add_parser("create")
    code_create.add_argument("name")
    code_create.add_argument("--description")
    code_create.add_argument("--color")
    code_sub.add_parser("list")
    code_show = code_sub.add_parser("show")
    code_show.add_argument("code_ref")
    code_rename = code_sub.add_parser("rename")
    code_rename.add_argument("old")
    code_rename.add_argument("new")
    code_alias = code_sub.add_parser("alias")
    code_alias.add_argument("code_ref")
    code_alias.add_argument("alias")
    code_merge = code_sub.add_parser("merge")
    code_merge.add_argument("sources", nargs="+")
    code_merge.add_argument("--into", required=True)
    code_split = code_sub.add_parser("split")
    code_split.add_argument("source")
    code_split.add_argument("--new", required=True)
    code_split.add_argument("--annotation", action="append", default=[])
    code_split.add_argument("--description")
    code_split.add_argument("--color")

    annotate = sub.add_parser("annotate")
    annotate_sub = annotate.add_subparsers(dest="annotate_cmd", required=True)
    ann_apply = annotate_sub.add_parser("apply")
    ann_apply.add_argument("code_ref")
    ann_apply.add_argument("document_ref")
    mode_group = ann_apply.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--document", action="store_true")
    mode_group.add_argument("--bytes")
    mode_group.add_argument("--lines")
    ann_apply.add_argument("--memo")
    ann_remove = annotate_sub.add_parser("remove")
    ann_remove.add_argument("annotation_id")
    ann_show = annotate_sub.add_parser("show")
    ann_show.add_argument("annotation_id")
    ann_resolve = annotate_sub.add_parser("resolve")
    ann_resolve.add_argument("annotation_id")
    ann_resolve.add_argument("--bytes", required=True)
    ann_resolve.add_argument("--memo")

    query = sub.add_parser("query")
    query.add_argument("expr")
    query.add_argument("--mode", choices=["document", "annotation"])

    export = sub.add_parser("export")
    export_sub = export.add_subparsers(dest="export_what", required=True)
    export_snippets = export_sub.add_parser("snippets")
    export_snippets.add_argument("--code", required=True)
    export_snippets.add_argument("--format", choices=["jsonl", "text"], required=True)
    export_snippets.add_argument("--context-lines", type=int, default=0)
    export_quotes = export_sub.add_parser("quotes")
    export_quotes_selector = export_quotes.add_mutually_exclusive_group(required=True)
    export_quotes_selector.add_argument("--code")
    export_quotes_selector.add_argument("--query")
    export_quotes.add_argument("--format", choices=["jsonl", "text"], required=True)
    export_quotes.add_argument("--context-lines", type=int, default=0)
    export_html = export_sub.add_parser("html")
    export_html.add_argument("--output", default="bewley-codes.html")
    export_html.add_argument("--title")
    export_document_html = export_sub.add_parser("document-html")
    export_document_html.add_argument("document_ref")
    export_document_html.add_argument("--output", default="bewley-document.html")
    export_document_html.add_argument("--title")

    history = sub.add_parser("history")
    history.add_argument("--document")
    history.add_argument("--code")
    history.add_argument("--annotation")

    undo = sub.add_parser("undo")
    undo.add_argument("event_id")

    return parser


def cmd_status(project: Project) -> int:
    with project.connect() as conn:
        doc_count = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        rev_count = conn.execute("SELECT COUNT(*) FROM document_revisions").fetchone()[0]
        code_count = conn.execute("SELECT COUNT(*) FROM codes").fetchone()[0]
        ann_count = conn.execute("SELECT COUNT(*) FROM annotations WHERE is_active = 1").fetchone()[0]
        conflict_count = conn.execute(
            "SELECT COUNT(*) FROM annotations WHERE is_active = 1 AND anchor_status = 'conflicted'"
        ).fetchone()[0]
    print(f"documents\t{doc_count}")
    print(f"revisions\t{rev_count}")
    print(f"codes\t{code_count}")
    print(f"active_annotations\t{ann_count}")
    print(f"conflicted_annotations\t{conflict_count}")
    return 0


def cmd_list_documents(project: Project) -> int:
    with project.connect() as conn:
        rows = conn.execute(
            """
            SELECT d.document_id, d.current_path, COUNT(r.revision_id) AS revision_count
            FROM documents d
            LEFT JOIN document_revisions r ON r.document_id = d.document_id
            GROUP BY d.document_id, d.current_path
            ORDER BY d.current_path
            """
        ).fetchall()
    print_table([(row["document_id"], row["current_path"], row["revision_count"]) for row in rows])
    return 0


def cmd_show_document(project: Project, ref: str) -> int:
    with project.connect() as conn:
        doc = project.resolve_document(conn, ref)
        revisions = conn.execute(
            """
            SELECT revision_id, created_at, byte_length, line_count, is_current
            FROM document_revisions
            WHERE document_id = ?
            ORDER BY created_at
            """,
            (doc["document_id"],),
        ).fetchall()
        annotations = conn.execute(
            """
            SELECT a.annotation_id, c.canonical_name, a.scope_type, a.start_line, a.end_line, a.anchor_status, a.is_active
            FROM annotations a
            JOIN codes c ON c.code_id = a.code_id
            WHERE a.document_id = ?
            ORDER BY a.created_at
            """,
            (doc["document_id"],),
        ).fetchall()
    print(f"document_id\t{doc['document_id']}")
    print(f"path\t{doc['current_path']}")
    print("revisions")
    print_table([(r["revision_id"], r["created_at"], r["byte_length"], r["line_count"], r["is_current"]) for r in revisions])
    print("annotations")
    print_table(
        [
            (a["annotation_id"], a["canonical_name"], a["scope_type"], a["start_line"], a["end_line"], a["anchor_status"], a["is_active"])
            for a in annotations
        ]
    )
    return 0


def cmd_code_list(project: Project) -> int:
    with project.connect() as conn:
        rows = conn.execute("SELECT * FROM codes ORDER BY canonical_name").fetchall()
    print_table([(row["code_id"], row["canonical_name"], row["status"]) for row in rows])
    return 0


def cmd_code_show(project: Project, ref: str) -> int:
    with project.connect() as conn:
        code = project.resolve_code(conn, ref)
        aliases = conn.execute("SELECT alias_name FROM code_aliases WHERE code_id = ? ORDER BY alias_name", (code["code_id"],)).fetchall()
        count = conn.execute("SELECT COUNT(*) FROM annotations WHERE code_id = ? AND is_active = 1", (code["code_id"],)).fetchone()[0]
    print(f"code_id\t{code['code_id']}")
    print(f"name\t{code['canonical_name']}")
    print(f"status\t{code['status']}")
    print(f"active_annotations\t{count}")
    print(f"aliases\t{', '.join(row['alias_name'] for row in aliases)}")
    return 0


def cmd_annotate_show(project: Project, annotation_id: str) -> int:
    with project.connect() as conn:
        row = conn.execute(
            """
            SELECT a.*, c.canonical_name, d.current_path
            FROM annotations a
            JOIN codes c ON c.code_id = a.code_id
            JOIN documents d ON d.document_id = a.document_id
            WHERE a.annotation_id = ?
            """,
            (annotation_id,),
        ).fetchone()
        if row is None:
            raise BewleyError(f"unknown annotation id: {annotation_id}")
    for key in row.keys():
        print(f"{key}\t{row[key]}")
    return 0


def snippets_for_code(project: Project, code_ref: str) -> list[sqlite3.Row]:
    with project.connect() as conn:
        code = project.resolve_code(conn, code_ref)
        return conn.execute(
            """
            SELECT a.*, c.canonical_name, d.current_path
            FROM annotations a
            JOIN codes c ON c.code_id = a.code_id
            JOIN documents d ON d.document_id = a.document_id
            WHERE a.code_id = ? AND a.is_active = 1
            ORDER BY d.current_path, COALESCE(a.start_line, 0), a.annotation_id
            """,
            (code["code_id"],),
        ).fetchall()


def export_rows_for_selector(project: Project, code_ref: str | None = None, query_expr: str | None = None) -> list[sqlite3.Row]:
    if bool(code_ref) == bool(query_expr):
        raise BewleyError("provide exactly one of --code or --query")
    if code_ref is not None:
        return snippets_for_code(project, code_ref)
    assert query_expr is not None
    return project.query_annotations(query_expr)


def line_window(text: str, start_line: int, end_line: int, context_lines: int) -> tuple[str, str]:
    lines = text.splitlines()
    before_start = max(0, start_line - 1 - context_lines)
    before_end = max(0, start_line - 1)
    after_start = min(len(lines), end_line)
    after_end = min(len(lines), end_line + context_lines)
    before = "\n".join(lines[before_start:before_end])
    after = "\n".join(lines[after_start:after_end])
    return before, after


def current_text_by_document(project: Project, rows: list[sqlite3.Row]) -> dict[str, str]:
    texts: dict[str, str] = {}
    with project.connect() as conn:
        for row in rows:
            if row["document_id"] in texts:
                continue
            revision = project.current_revision(conn, row["document_id"])
            content = (project.objects_dir / revision["revision_id"]).read_bytes()
            texts[row["document_id"]] = safe_decode(content)
    return texts


def snippet_export_item(row: sqlite3.Row, context_lines: int, text_by_document: dict[str, str]) -> dict[str, Any]:
    item = {
        "code_name": row["canonical_name"],
        "code_id": row["code_id"],
        "document_id": row["document_id"],
        "document_path": row["current_path"],
        "revision_id": row["document_revision_id"],
        "annotation_id": row["annotation_id"],
        "start_line": row["start_line"],
        "end_line": row["end_line"],
        "selected_text": row["exact_text"] if row["scope_type"] == "span" else None,
        "anchor_status": row["anchor_status"],
    }
    if context_lines > 0 and row["scope_type"] == "span" and row["start_line"] is not None and row["end_line"] is not None:
        before, after = line_window(
            text_by_document[row["document_id"]],
            row["start_line"],
            row["end_line"],
            context_lines,
        )
        item["context_before"] = before
        item["context_after"] = after
        item["context_lines"] = context_lines
    return item


def quote_export_item(row: sqlite3.Row, context_lines: int, text_by_document: dict[str, str]) -> dict[str, Any]:
    item = {
        "code_name": row["canonical_name"],
        "code_id": row["code_id"],
        "document_id": row["document_id"],
        "document_path": row["current_path"],
        "revision_id": row["document_revision_id"],
        "annotation_id": row["annotation_id"],
        "start_byte": row["start_byte"],
        "end_byte": row["end_byte"],
        "start_line": row["start_line"],
        "end_line": row["end_line"],
        "exact_text": row["exact_text"],
        "anchor_status": row["anchor_status"],
    }
    if context_lines > 0 and row["start_line"] is not None and row["end_line"] is not None:
        before, after = line_window(
            text_by_document[row["document_id"]],
            row["start_line"],
            row["end_line"],
            context_lines,
        )
        item["context_before"] = before
        item["context_after"] = after
        item["context_lines"] = context_lines
    return item


def code_explorer_payload(project: Project) -> dict[str, Any]:
    with project.connect() as conn:
        codes = conn.execute(
            """
            SELECT c.*,
                   COUNT(DISTINCT CASE WHEN a.is_active = 1 THEN a.annotation_id END) AS annotation_count,
                   COUNT(DISTINCT CASE WHEN a.is_active = 1 THEN a.document_id END) AS document_count
            FROM codes c
            LEFT JOIN annotations a ON a.code_id = c.code_id
            GROUP BY c.code_id
            ORDER BY annotation_count DESC, c.canonical_name
            """
        ).fetchall()
        alias_rows = conn.execute(
            "SELECT code_id, alias_name FROM code_aliases ORDER BY alias_name"
        ).fetchall()
        annotations = conn.execute(
            """
            SELECT a.*, c.canonical_name, c.color, d.current_path
            FROM annotations a
            JOIN codes c ON c.code_id = a.code_id
            JOIN documents d ON d.document_id = a.document_id
            WHERE a.is_active = 1
            ORDER BY c.canonical_name, d.current_path, COALESCE(a.start_line, 0), a.annotation_id
            """
        ).fetchall()
    aliases_by_code: dict[str, list[str]] = {}
    for row in alias_rows:
        aliases_by_code.setdefault(row["code_id"], []).append(row["alias_name"])
    code_items = [
        {
            "code_id": row["code_id"],
            "name": row["canonical_name"],
            "description": row["description"],
            "status": row["status"],
            "annotation_count": row["annotation_count"],
            "document_count": row["document_count"],
            "display_color": coerce_code_color(row["color"], row["canonical_name"]),
            "aliases": aliases_by_code.get(row["code_id"], []),
        }
        for row in codes
    ]
    snippet_items = [
        {
            "annotation_id": row["annotation_id"],
            "code_id": row["code_id"],
            "code_name": row["canonical_name"],
            "code_color": coerce_code_color(row["color"], row["canonical_name"]),
            "document_path": row["current_path"],
            "scope_type": row["scope_type"],
            "start_line": row["start_line"],
            "end_line": row["end_line"],
            "anchor_status": row["anchor_status"],
            "memo": row["memo"],
            "exact_text": row["exact_text"],
        }
        for row in annotations
    ]
    return {
        "generated_at": utcnow(),
        "project_root": str(project.root),
        "code_count": len(code_items),
        "snippet_count": len(snippet_items),
        "document_count": len({item["document_path"] for item in snippet_items}),
        "codes": code_items,
        "snippets": snippet_items,
    }


def document_viewer_payload(project: Project, document_ref: str) -> dict[str, Any]:
    with project.connect() as conn:
        doc = project.resolve_document(conn, document_ref)
        revision = project.current_revision(conn, doc["document_id"])
        annotations = conn.execute(
            """
            SELECT a.*, c.canonical_name, c.color
            FROM annotations a
            JOIN codes c ON c.code_id = a.code_id
            WHERE a.document_id = ? AND a.is_active = 1
            ORDER BY COALESCE(a.start_byte, -1), a.annotation_id
            """,
            (doc["document_id"],),
        ).fetchall()
    content = (project.objects_dir / revision["revision_id"]).read_bytes()
    text = safe_decode(content)
    span_annotations = []
    document_annotations = []
    code_counts: dict[str, dict[str, Any]] = {}
    annotation_index: dict[str, dict[str, Any]] = {}
    for row in annotations:
        code_slug = row["canonical_name"].replace("_", "-")
        code_color = coerce_code_color(row["color"], row["canonical_name"])
        highlight_color = soft_color(code_color, 0.28)
        code_entry = code_counts.setdefault(
            row["code_id"],
            {
                "code_id": row["code_id"],
                "name": row["canonical_name"],
                "display_color": code_color,
                "annotation_count": 0,
                "document_annotation_count": 0,
            },
        )
        item = {
            "annotation_id": row["annotation_id"],
            "code_id": row["code_id"],
            "code_name": row["canonical_name"],
            "code_slug": code_slug,
            "code_color": code_color,
            "highlight_color": highlight_color,
            "scope_type": row["scope_type"],
            "start_byte": row["start_byte"],
            "end_byte": row["end_byte"],
            "start_line": row["start_line"],
            "end_line": row["end_line"],
            "anchor_status": row["anchor_status"],
            "memo": row["memo"],
            "exact_text": row["exact_text"],
        }
        annotation_index[row["annotation_id"]] = item
        if row["scope_type"] == "document":
            document_annotations.append(item)
            code_entry["document_annotation_count"] += 1
            continue
        span_annotations.append(item)
        code_entry["annotation_count"] += 1
    codes = sorted(
        code_counts.values(),
        key=lambda item: (-item["annotation_count"], -item["document_annotation_count"], item["name"]),
    )
    return {
        "generated_at": utcnow(),
        "project_root": str(project.root),
        "document_id": doc["document_id"],
        "document_path": doc["current_path"],
        "revision_id": revision["revision_id"],
        "byte_length": revision["byte_length"],
        "line_count": revision["line_count"],
        "code_count": len(codes),
        "annotation_count": len(span_annotations),
        "document_annotations": document_annotations,
        "span_annotations": span_annotations,
        "codes": codes,
        "annotation_index": annotation_index,
        "rendered_text": render_annotated_document_html(text, span_annotations),
    }


def cmd_show_snippets(project: Project, code_ref: str) -> int:
    rows = snippets_for_code(project, code_ref)
    for row in rows:
        text = row["exact_text"] if row["scope_type"] == "span" else "<document>"
        print_table(
            [(
                row["annotation_id"],
                row["canonical_name"],
                row["current_path"],
                row["start_line"],
                row["end_line"],
                row["anchor_status"],
                text,
            )]
        )
    return 0


def cmd_query(project: Project, expr: str, mode: str | None) -> int:
    cfg_mode = project.config().get("default_query_mode", DEFAULT_QUERY_MODE)
    selected_mode = mode or cfg_mode
    if selected_mode == "document":
        rows = project.query_documents(expr)
        print_table([(row["document_id"], row["current_path"]) for row in rows])
        return 0
    rows = project.query_annotations(expr)
    print_table(
        [
            (row["annotation_id"], row["canonical_name"], row["current_path"], row["start_line"], row["end_line"], row["anchor_status"])
            for row in rows
        ]
    )
    return 0


def cmd_export_snippets(project: Project, code_ref: str, fmt: str, context_lines: int) -> int:
    rows = snippets_for_code(project, code_ref)
    text_by_document = current_text_by_document(project, rows) if context_lines > 0 else {}
    if fmt == "text":
        for row in rows:
            selected = row["exact_text"] if row["scope_type"] == "span" else "<document>"
            if context_lines > 0 and row["scope_type"] == "span" and row["start_line"] is not None and row["end_line"] is not None:
                before, after = line_window(
                    text_by_document[row["document_id"]],
                    row["start_line"],
                    row["end_line"],
                    context_lines,
                )
                print(
                    f"{row['canonical_name']}\t{row['current_path']}\t{row['annotation_id']}\t"
                    f"before={before!r}\tselected={selected!r}\tafter={after!r}"
                )
                continue
            print(f"{row['canonical_name']}\t{row['current_path']}\t{row['annotation_id']}\t{selected}")
        return 0
    for row in rows:
        print(json.dumps(snippet_export_item(row, context_lines, text_by_document), ensure_ascii=False))
    return 0


def cmd_export_quotes(project: Project, code_ref: str | None, query_expr: str | None, fmt: str, context_lines: int) -> int:
    rows = [row for row in export_rows_for_selector(project, code_ref=code_ref, query_expr=query_expr) if row["scope_type"] == "span"]
    text_by_document = current_text_by_document(project, rows) if context_lines > 0 else {}
    if fmt == "text":
        for row in rows:
            item = quote_export_item(row, context_lines, text_by_document)
            parts = [
                row["canonical_name"],
                row["current_path"],
                row["annotation_id"],
                f"bytes={row['start_byte']}:{row['end_byte']}",
                f"lines={row['start_line']}:{row['end_line']}",
                f"exact={row['exact_text']!r}",
            ]
            if context_lines > 0:
                parts.append(f"before={item.get('context_before', '')!r}")
                parts.append(f"after={item.get('context_after', '')!r}")
            print("\t".join(parts))
        return 0
    for row in rows:
        print(json.dumps(quote_export_item(row, context_lines, text_by_document), ensure_ascii=False))
    return 0


def cmd_export_html(project: Project, output_path: str, title: str | None) -> int:
    payload = code_explorer_payload(project)
    document_count = payload["document_count"]
    resolved_title = title or f"Bewley Explorer · {project.root.name} · {payload['code_count']} codes / {document_count} docs"
    target = Path(output_path)
    if not target.is_absolute():
        target = project.root / target
    atomic_write_text(target, build_code_explorer_html(payload, resolved_title))
    print(str(target))
    return 0


def cmd_export_document_html(project: Project, document_ref: str, output_path: str, title: str | None) -> int:
    payload = document_viewer_payload(project, document_ref)
    resolved_title = title or f"Bewley Document Viewer · {payload['document_path']}"
    target = Path(output_path)
    if not target.is_absolute():
        target = project.root / target
    atomic_write_text(target, build_document_viewer_html(payload, resolved_title))
    print(str(target))
    return 0


def cmd_history(project: Project, document: str | None, code: str | None, annotation: str | None) -> int:
    rows = project.history(document_ref=document, code_ref=code, annotation_id=annotation)
    for event in rows:
        print_table([(event["sequence_number"], event["timestamp"], event["event_type"], event["event_id"])])
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "init":
            project = Project(Path.cwd())
            project.init_project()
            print("initialized")
            return 0

        project = Project.discover()

        if args.command == "status":
            return cmd_status(project)
        if args.command == "fsck":
            problems = project.fsck()
            if problems:
                for problem in problems:
                    print(problem, file=sys.stderr)
                return 1
            print("ok")
            return 0
        if args.command == "rebuild-index":
            project.rebuild_index()
            project.append_event("index_rebuilt", {"timestamp": utcnow()})
            print("rebuilt")
            return 0
        if args.command == "add":
            event = project.add_document(args.path)
            print(event["payload"]["document_id"])
            return 0
        if args.command == "update":
            event = project.update_document(args.path)
            if event is None:
                print("no-op")
            else:
                print(event["payload"]["revision_id"])
            return 0
        if args.command == "list" and args.list_what == "documents":
            return cmd_list_documents(project)
        if args.command == "show" and args.show_what == "document":
            return cmd_show_document(project, args.document_ref)
        if args.command == "show" and args.show_what == "snippets":
            return cmd_show_snippets(project, args.code)
        if args.command == "code":
            if args.code_cmd == "create":
                event = project.add_code(args.name, args.description, args.color)
                print(event["payload"]["code_id"])
                return 0
            if args.code_cmd == "list":
                return cmd_code_list(project)
            if args.code_cmd == "show":
                return cmd_code_show(project, args.code_ref)
            if args.code_cmd == "rename":
                event = project.rename_code(args.old, args.new)
                print(event["event_id"])
                return 0
            if args.code_cmd == "alias":
                event = project.alias_code(args.code_ref, args.alias)
                print(event["event_id"])
                return 0
            if args.code_cmd == "merge":
                event = project.merge_codes(args.sources, args.into)
                print(event["event_id"])
                return 0
            if args.code_cmd == "split":
                event = project.split_code(args.source, args.new, args.annotation, args.description, args.color)
                print(event["payload"]["new_code_id"])
                return 0
        if args.command == "annotate":
            if args.annotate_cmd == "apply":
                if args.document:
                    event = project.add_annotation(args.code_ref, args.document_ref, "document", None, args.memo)
                elif args.bytes:
                    event = project.add_annotation(args.code_ref, args.document_ref, "span", parse_byte_range(args.bytes), args.memo)
                else:
                    with project.connect() as conn:
                        doc = project.resolve_document(conn, args.document_ref)
                        rev = project.current_revision(conn, doc["document_id"])
                    content = (project.objects_dir / rev["revision_id"]).read_bytes().decode("utf-8")
                    byte_range = lines_to_byte_range(content, *parse_byte_range(args.lines))
                    event = project.add_annotation(args.code_ref, args.document_ref, "span", byte_range, args.memo)
                print(event["payload"]["annotation_id"])
                return 0
            if args.annotate_cmd == "remove":
                event = project.remove_annotation(args.annotation_id)
                print(event["event_id"])
                return 0
            if args.annotate_cmd == "show":
                return cmd_annotate_show(project, args.annotation_id)
            if args.annotate_cmd == "resolve":
                event = project.resolve_annotation(args.annotation_id, parse_byte_range(args.bytes), args.memo)
                print(event["event_id"])
                return 0
        if args.command == "query":
            return cmd_query(project, args.expr, args.mode)
        if args.command == "export" and args.export_what == "snippets":
            return cmd_export_snippets(project, args.code, args.format, args.context_lines)
        if args.command == "export" and args.export_what == "quotes":
            return cmd_export_quotes(project, args.code, args.query, args.format, args.context_lines)
        if args.command == "export" and args.export_what == "html":
            return cmd_export_html(project, args.output, args.title)
        if args.command == "export" and args.export_what == "document-html":
            return cmd_export_document_html(project, args.document_ref, args.output, args.title)
        if args.command == "history":
            return cmd_history(project, args.document, args.code, args.annotation)
        if args.command == "undo":
            event = project.undo(args.event_id)
            print(event["event_id"])
            return 0
        raise BewleyError("unimplemented command")
    except BewleyError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("error: interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
