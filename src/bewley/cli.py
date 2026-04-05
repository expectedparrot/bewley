from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import html
import hashlib
import json
import mimetypes
import os
import shutil
import sqlite3
import subprocess
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
AUDIO_OBJECTS_DIR = Path(PROJECT_DIR) / "objects" / "audio"
VIDEO_OBJECTS_DIR = Path(PROJECT_DIR) / "objects" / "video"
LOCK_PATH = Path(PROJECT_DIR) / "locks" / "write.lock"
CONFIG_PATH = Path(PROJECT_DIR) / "config.toml"
HEAD_PATH = Path(PROJECT_DIR) / "HEAD"
DEFAULT_QUERY_MODE = "document"
CONTEXT_BYTES = 32
FUZZY_RELOCATION_THRESHOLD = 0.92
OPENAI_AUDIO_LIMIT_BYTES = 25 * 1024 * 1024
OPENAI_MEDIA_TARGET_BYTES = 24 * 1024 * 1024
DEFAULT_VIDEO_CHUNK_OVERLAP_SECONDS = 3.0
DEFAULT_EXTRACT_AUDIO_BITRATE_KBPS = 96


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


def format_timestamp(seconds: Any) -> str:
    try:
        total = max(0.0, float(seconds))
    except (TypeError, ValueError):
        return "00:00.00"
    minutes = int(total // 60)
    remainder = total - (minutes * 60)
    return f"{minutes:02d}:{remainder:05.2f}"


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
    def audio_objects_dir(self) -> Path:
        return self.root / AUDIO_OBJECTS_DIR

    @property
    def video_objects_dir(self) -> Path:
        return self.root / VIDEO_OBJECTS_DIR

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
                CREATE TABLE IF NOT EXISTS document_audio_sources (
                  document_id TEXT PRIMARY KEY,
                  audio_sha256 TEXT NOT NULL,
                  original_audio_path TEXT NOT NULL,
                  original_audio_filename TEXT NOT NULL,
                  media_type TEXT NOT NULL,
                  transcription_model TEXT NOT NULL,
                  transcription_response_format TEXT NOT NULL,
                  transcription_language TEXT,
                  transcript_style TEXT NOT NULL,
                  segment_count INTEGER NOT NULL DEFAULT 0,
                  metadata_json TEXT NOT NULL,
                  created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS document_video_sources (
                  document_id TEXT PRIMARY KEY,
                  video_sha256 TEXT NOT NULL,
                  original_video_path TEXT NOT NULL,
                  original_video_filename TEXT NOT NULL,
                  media_type TEXT NOT NULL,
                  duration_seconds REAL NOT NULL,
                  transcription_model TEXT NOT NULL,
                  transcription_response_format TEXT NOT NULL,
                  transcription_language TEXT,
                  transcript_style TEXT NOT NULL,
                  chunk_count INTEGER NOT NULL DEFAULT 0,
                  metadata_json TEXT NOT NULL,
                  created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS document_video_chunks (
                  document_id TEXT NOT NULL,
                  chunk_index INTEGER NOT NULL,
                  extract_start_seconds REAL NOT NULL,
                  extract_end_seconds REAL NOT NULL,
                  logical_start_seconds REAL NOT NULL,
                  logical_end_seconds REAL NOT NULL,
                  chunk_audio_sha256 TEXT NOT NULL,
                  byte_length INTEGER NOT NULL,
                  media_type TEXT NOT NULL,
                  metadata_json TEXT NOT NULL,
                  PRIMARY KEY (document_id, chunk_index)
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
                CREATE TABLE IF NOT EXISTS memos (
                  memo_id TEXT PRIMARY KEY,
                  target_type TEXT NOT NULL,
                  target_id TEXT,
                  title TEXT,
                  content_sha256 TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  is_active INTEGER NOT NULL DEFAULT 1
                );
                CREATE INDEX IF NOT EXISTS idx_memos_target ON memos (target_type, target_id);
                CREATE TABLE IF NOT EXISTS code_links (
                  link_id TEXT PRIMARY KEY,
                  source_code_id TEXT NOT NULL,
                  target_code_id TEXT NOT NULL,
                  relationship TEXT NOT NULL,
                  memo TEXT,
                  created_at TEXT NOT NULL,
                  is_active INTEGER NOT NULL DEFAULT 1
                );
                CREATE INDEX IF NOT EXISTS idx_code_links_source ON code_links (source_code_id);
                CREATE INDEX IF NOT EXISTS idx_code_links_target ON code_links (target_code_id);
                CREATE TABLE IF NOT EXISTS project_settings (
                  key TEXT PRIMARY KEY,
                  value TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );
                """
            )
            # Migration: add parent_code_id column to codes if missing.
            try:
                conn.execute("ALTER TABLE codes ADD COLUMN parent_code_id TEXT")
            except sqlite3.OperationalError:
                pass

    def init_project(self) -> None:
        if self.meta.exists():
            raise BewleyError("project already initialized")
        for rel in [
            "corpus",
            ".bewley/events",
            ".bewley/objects/documents",
            ".bewley/objects/audio",
            ".bewley/objects/video",
            ".bewley/objects/memos",
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
            CREATE TABLE document_audio_sources (
              document_id TEXT PRIMARY KEY,
              audio_sha256 TEXT NOT NULL,
              original_audio_path TEXT NOT NULL,
              original_audio_filename TEXT NOT NULL,
              media_type TEXT NOT NULL,
              transcription_model TEXT NOT NULL,
              transcription_response_format TEXT NOT NULL,
              transcription_language TEXT,
              transcript_style TEXT NOT NULL,
              segment_count INTEGER NOT NULL DEFAULT 0,
              metadata_json TEXT NOT NULL,
              created_at TEXT NOT NULL
            );
            CREATE TABLE document_video_sources (
              document_id TEXT PRIMARY KEY,
              video_sha256 TEXT NOT NULL,
              original_video_path TEXT NOT NULL,
              original_video_filename TEXT NOT NULL,
              media_type TEXT NOT NULL,
              duration_seconds REAL NOT NULL,
              transcription_model TEXT NOT NULL,
              transcription_response_format TEXT NOT NULL,
              transcription_language TEXT,
              transcript_style TEXT NOT NULL,
              chunk_count INTEGER NOT NULL DEFAULT 0,
              metadata_json TEXT NOT NULL,
              created_at TEXT NOT NULL
            );
            CREATE TABLE document_video_chunks (
              document_id TEXT NOT NULL,
              chunk_index INTEGER NOT NULL,
              extract_start_seconds REAL NOT NULL,
              extract_end_seconds REAL NOT NULL,
              logical_start_seconds REAL NOT NULL,
              logical_end_seconds REAL NOT NULL,
              chunk_audio_sha256 TEXT NOT NULL,
              byte_length INTEGER NOT NULL,
              media_type TEXT NOT NULL,
              metadata_json TEXT NOT NULL,
              PRIMARY KEY (document_id, chunk_index)
            );
            CREATE TABLE codes (
              code_id TEXT PRIMARY KEY,
              canonical_name TEXT NOT NULL UNIQUE,
              description TEXT,
              color TEXT,
              status TEXT NOT NULL,
              created_at TEXT NOT NULL,
              parent_code_id TEXT
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
            CREATE TABLE memos (
              memo_id TEXT PRIMARY KEY,
              target_type TEXT NOT NULL,
              target_id TEXT,
              title TEXT,
              content_sha256 TEXT NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              is_active INTEGER NOT NULL DEFAULT 1
            );
            CREATE INDEX idx_memos_target ON memos (target_type, target_id);
            CREATE TABLE code_links (
              link_id TEXT PRIMARY KEY,
              source_code_id TEXT NOT NULL,
              target_code_id TEXT NOT NULL,
              relationship TEXT NOT NULL,
              memo TEXT,
              created_at TEXT NOT NULL,
              is_active INTEGER NOT NULL DEFAULT 1
            );
            CREATE INDEX idx_code_links_source ON code_links (source_code_id);
            CREATE INDEX idx_code_links_target ON code_links (target_code_id);
            CREATE TABLE project_settings (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
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
        if etype == "document_audio_linked":
            conn.execute(
                """
                INSERT OR REPLACE INTO document_audio_sources (
                  document_id, audio_sha256, original_audio_path, original_audio_filename, media_type,
                  transcription_model, transcription_response_format, transcription_language, transcript_style,
                  segment_count, metadata_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["document_id"],
                    payload["audio_sha256"],
                    payload["original_audio_path"],
                    payload["original_audio_filename"],
                    payload["media_type"],
                    payload["transcription_model"],
                    payload["transcription_response_format"],
                    payload.get("transcription_language"),
                    payload["transcript_style"],
                    payload["segment_count"],
                    json.dumps(payload["transcription_metadata"], ensure_ascii=False, sort_keys=True),
                    event["timestamp"],
                ),
            )
            return
        if etype == "document_video_linked":
            conn.execute(
                """
                INSERT OR REPLACE INTO document_video_sources (
                  document_id, video_sha256, original_video_path, original_video_filename, media_type,
                  duration_seconds, transcription_model, transcription_response_format, transcription_language,
                  transcript_style, chunk_count, metadata_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["document_id"],
                    payload["video_sha256"],
                    payload["original_video_path"],
                    payload["original_video_filename"],
                    payload["media_type"],
                    payload["duration_seconds"],
                    payload["transcription_model"],
                    payload["transcription_response_format"],
                    payload.get("transcription_language"),
                    payload["transcript_style"],
                    payload["chunk_count"],
                    json.dumps(payload["transcription_metadata"], ensure_ascii=False, sort_keys=True),
                    event["timestamp"],
                ),
            )
            conn.execute("DELETE FROM document_video_chunks WHERE document_id = ?", (payload["document_id"],))
            for chunk in payload["chunks"]:
                conn.execute(
                    """
                    INSERT INTO document_video_chunks (
                      document_id, chunk_index, extract_start_seconds, extract_end_seconds,
                      logical_start_seconds, logical_end_seconds, chunk_audio_sha256, byte_length,
                      media_type, metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        payload["document_id"],
                        chunk["chunk_index"],
                        chunk["extract_start_seconds"],
                        chunk["extract_end_seconds"],
                        chunk["logical_start_seconds"],
                        chunk["logical_end_seconds"],
                        chunk["chunk_audio_sha256"],
                        chunk["byte_length"],
                        chunk["media_type"],
                        json.dumps(chunk["transcription_metadata"], ensure_ascii=False, sort_keys=True),
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
                conn.execute(
                    "UPDATE codes SET parent_code_id = ? WHERE parent_code_id = ?",
                    (payload["target_code_id"], source_code_id),
                )
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
        if etype == "memo_created":
            conn.execute(
                """
                INSERT INTO memos (memo_id, target_type, target_id, title, content_sha256, created_at, updated_at, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (payload["memo_id"], payload["target_type"], payload.get("target_id"), payload.get("title"),
                 payload["content_sha256"], event["timestamp"], event["timestamp"]),
            )
            return
        if etype == "memo_updated":
            conn.execute(
                "UPDATE memos SET content_sha256 = ?, updated_at = ? WHERE memo_id = ?",
                (payload["content_sha256"], event["timestamp"], payload["memo_id"]),
            )
            return
        if etype == "memo_deleted":
            conn.execute("UPDATE memos SET is_active = 0, updated_at = ? WHERE memo_id = ?", (event["timestamp"], payload["memo_id"]))
            return
        if etype == "code_parent_set":
            conn.execute(
                "UPDATE codes SET parent_code_id = ? WHERE code_id = ?",
                (payload["parent_code_id"], payload["code_id"]),
            )
            return
        if etype == "code_link_created":
            conn.execute(
                """
                INSERT INTO code_links (link_id, source_code_id, target_code_id, relationship, memo, created_at, is_active)
                VALUES (?, ?, ?, ?, ?, ?, 1)
                """,
                (payload["link_id"], payload["source_code_id"], payload["target_code_id"],
                 payload["relationship"], payload.get("memo"), event["timestamp"]),
            )
            return
        if etype == "code_link_removed":
            conn.execute("UPDATE code_links SET is_active = 0 WHERE link_id = ?", (payload["link_id"],))
            return
        if etype == "core_category_set":
            conn.execute(
                "INSERT OR REPLACE INTO project_settings (key, value, updated_at) VALUES ('core_category_code_id', ?, ?)",
                (payload["code_id"], event["timestamp"]),
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
        if undone_type == "memo_created":
            conn.execute("UPDATE memos SET is_active = 0 WHERE memo_id = ?", (original["memo_id"],))
            return
        if undone_type == "memo_updated":
            conn.execute(
                "UPDATE memos SET content_sha256 = ? WHERE memo_id = ?",
                (original["old_content_sha256"], original["memo_id"]),
            )
            return
        if undone_type == "memo_deleted":
            conn.execute("UPDATE memos SET is_active = 1 WHERE memo_id = ?", (original["memo_id"],))
            return
        if undone_type == "code_parent_set":
            conn.execute(
                "UPDATE codes SET parent_code_id = ? WHERE code_id = ?",
                (original.get("old_parent_code_id"), original["code_id"]),
            )
            return
        if undone_type == "code_link_created":
            conn.execute("UPDATE code_links SET is_active = 0 WHERE link_id = ?", (original["link_id"],))
            return
        if undone_type == "code_link_removed":
            conn.execute("UPDATE code_links SET is_active = 1 WHERE link_id = ?", (original["link_id"],))
            return
        if undone_type == "core_category_set":
            old = original.get("old_code_id")
            if old:
                conn.execute(
                    "INSERT OR REPLACE INTO project_settings (key, value, updated_at) VALUES ('core_category_code_id', ?, ?)",
                    (old, event["timestamp"]),
                )
            else:
                conn.execute("DELETE FROM project_settings WHERE key = 'core_category_code_id'")
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

    def store_audio_object(self, data: bytes) -> str:
        digest = sha256_bytes(data)
        target = self.audio_objects_dir / digest
        if not target.exists():
            target.write_bytes(data)
        return digest

    def store_video_object(self, data: bytes) -> str:
        digest = sha256_bytes(data)
        target = self.video_objects_dir / digest
        if not target.exists():
            target.write_bytes(data)
        return digest

    def store_memo_object(self, content: str) -> str:
        data = content.encode("utf-8")
        digest = sha256_bytes(data)
        memo_dir = self.root / PROJECT_DIR / "objects" / "memos"
        memo_dir.mkdir(parents=True, exist_ok=True)
        target = memo_dir / digest
        if not target.exists():
            atomic_write_text(target, content)
        return digest

    def read_memo_content(self, content_sha256: str) -> str:
        memo_path = self.root / PROJECT_DIR / "objects" / "memos" / content_sha256
        if not memo_path.exists():
            raise BewleyError(f"missing memo object: {content_sha256}")
        return memo_path.read_text(encoding="utf-8")

    @staticmethod
    def _open_editor(initial_content: str = "") -> str:
        editor = os.environ.get("EDITOR", os.environ.get("VISUAL", "vi"))
        with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False, encoding="utf-8") as tmp:
            tmp.write(initial_content)
            tmp_path = tmp.name
        try:
            subprocess.run([editor, tmp_path], check=True)
            return Path(tmp_path).read_text(encoding="utf-8")
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(tmp_path)

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
        revision_id = uuid.uuid4().hex
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

    def resolve_output_path(self, path_arg: str | None, source_path: Path) -> tuple[Path, Path]:
        output_ref = Path(path_arg) if path_arg else Path("corpus") / f"{source_path.stem}.txt"
        output_path = (self.root / output_ref).resolve() if not output_ref.is_absolute() else output_ref
        try:
            rel = output_path.relative_to(self.root)
        except ValueError as exc:
            raise BewleyError("transcript output path must be inside the project root") from exc
        return output_path, rel

    def normalize_segments(self, transcription: dict[str, Any]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for segment in transcription.get("segments") or []:
            text = str(segment.get("text") or "").strip()
            if not text:
                continue
            try:
                start = float(segment.get("start", 0.0))
                end = float(segment.get("end", start))
            except (TypeError, ValueError):
                continue
            if end <= start:
                continue
            normalized.append(
                {
                    "start": start,
                    "end": end,
                    "speaker": str(segment.get("speaker") or "speaker").strip(),
                    "text": text,
                }
            )
        return normalized

    def render_transcript_text(self, transcription: dict[str, Any], transcript_style: str) -> str:
        if transcript_style == "segments":
            segments = self.normalize_segments(transcription)
            lines: list[str] = []
            for segment in segments:
                lines.append(
                    f"[{format_timestamp(segment['start'])} - {format_timestamp(segment['end'])}] "
                    f"{segment['speaker']}: {segment['text']}"
                )
            if lines:
                return "\n".join(lines) + "\n"
        text = str(transcription.get("text", "")).strip()
        if not text:
            raise BewleyError("transcription response did not contain transcript text")
        return text + "\n"

    def ffprobe_duration_seconds(self, media_path: Path) -> float:
        completed = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(media_path),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        try:
            duration = float(completed.stdout.strip())
        except ValueError as exc:
            raise BewleyError(f"ffprobe returned an invalid duration for {media_path}") from exc
        if duration <= 0:
            raise BewleyError(f"ffprobe returned a non-positive duration for {media_path}")
        return duration

    def build_video_chunk_plan(
        self,
        video_path: Path,
        *,
        max_upload_bytes: int,
        overlap_seconds: float,
        audio_bitrate_kbps: int,
    ) -> list[dict[str, Any]]:
        duration_seconds = self.ffprobe_duration_seconds(video_path)
        bytes_per_second = (audio_bitrate_kbps * 1000) / 8
        logical_chunk_seconds = max(30.0, max_upload_bytes / bytes_per_second)
        chunks: list[dict[str, Any]] = []
        logical_start = 0.0
        chunk_index = 0
        while logical_start < duration_seconds:
            logical_end = min(duration_seconds, logical_start + logical_chunk_seconds)
            extract_start = logical_start if chunk_index == 0 else max(0.0, logical_start - overlap_seconds)
            extract_end = logical_end
            chunks.append(
                {
                    "chunk_index": chunk_index,
                    "extract_start_seconds": round(extract_start, 3),
                    "extract_end_seconds": round(extract_end, 3),
                    "logical_start_seconds": round(logical_start, 3),
                    "logical_end_seconds": round(logical_end, 3),
                }
            )
            if logical_end >= duration_seconds:
                break
            logical_start = logical_end
            chunk_index += 1
        return chunks

    def extract_audio_chunk(
        self,
        video_path: Path,
        chunk_path: Path,
        *,
        extract_start_seconds: float,
        extract_end_seconds: float,
        audio_bitrate_kbps: int,
    ) -> None:
        duration = max(0.1, extract_end_seconds - extract_start_seconds)
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-ss",
                f"{extract_start_seconds:.3f}",
                "-i",
                str(video_path),
                "-t",
                f"{duration:.3f}",
                "-vn",
                "-ac",
                "1",
                "-b:a",
                f"{audio_bitrate_kbps}k",
                str(chunk_path),
            ],
            check=True,
            capture_output=True,
            text=True,
        )

    def merge_chunk_transcriptions(self, chunk_results: list[dict[str, Any]]) -> dict[str, Any]:
        merged_segments: list[dict[str, Any]] = []
        merged_text_parts: list[str] = []
        language: str | None = None
        for result in sorted(chunk_results, key=lambda item: item["chunk_index"]):
            transcription = result["transcription"]
            if not language:
                language = transcription.get("language")
            local_segments = self.normalize_segments(transcription)
            absolute_segments: list[dict[str, Any]] = []
            for segment in local_segments:
                abs_start = result["extract_start_seconds"] + segment["start"]
                abs_end = result["extract_start_seconds"] + segment["end"]
                if abs_end <= result["logical_start_seconds"]:
                    continue
                absolute_segments.append(
                    {
                        "start": max(abs_start, result["logical_start_seconds"]),
                        "end": min(abs_end, result["logical_end_seconds"]),
                        "speaker": segment["speaker"],
                        "text": segment["text"],
                    }
                )
            if absolute_segments:
                merged_segments.extend(absolute_segments)
            else:
                text = str(transcription.get("text") or "").strip()
                if text:
                    merged_text_parts.append(text)
        merged: dict[str, Any] = {"language": language}
        if merged_segments:
            merged["segments"] = merged_segments
            merged["text"] = " ".join(segment["text"] for segment in merged_segments)
        else:
            merged["text"] = "\n\n".join(merged_text_parts).strip()
        return merged

    def transcribe_audio_with_openai(
        self,
        audio_path: Path,
        *,
        model: str,
        language: str | None,
        prompt: str | None,
        response_format: str,
    ) -> dict[str, Any]:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise BewleyError("OPENAI_API_KEY is required for media transcription")
        if not audio_path.is_file():
            raise BewleyError(f"media file not found: {audio_path}")
        size = audio_path.stat().st_size
        if size > OPENAI_AUDIO_LIMIT_BYTES:
            raise BewleyError("media file exceeds OpenAI's 25 MB transcription upload limit")
        if response_format == "diarized_json" and prompt:
            raise BewleyError("prompt is not supported with diarized_json transcripts")
        command = [
            "curl",
            "--silent",
            "--show-error",
            "--fail-with-body",
            "https://api.openai.com/v1/audio/transcriptions",
            "-H",
            f"Authorization: Bearer {api_key}",
            "-F",
            f"file=@{audio_path}",
            "-F",
            f"model={model}",
            "-F",
            f"response_format={response_format}",
        ]
        if language:
            command.extend(["-F", f"language={language}"])
        if prompt:
            command.extend(["-F", f"prompt={prompt}"])
        completed = subprocess.run(command, check=True, capture_output=True, text=True)
        try:
            payload = json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise BewleyError("OpenAI transcription response was not valid JSON") from exc
        if not isinstance(payload, dict):
            raise BewleyError("unexpected OpenAI transcription response shape")
        return payload

    def add_audio_document(
        self,
        audio_path_arg: str,
        transcript_path_arg: str | None,
        *,
        model: str,
        language: str | None,
        prompt: str | None,
        response_format: str,
    ) -> dict[str, Any]:
        audio_path = (self.root / audio_path_arg).resolve() if not Path(audio_path_arg).is_absolute() else Path(audio_path_arg)
        if not audio_path.is_file():
            raise BewleyError(f"audio file not found: {audio_path_arg}")
        transcript_path, resolved_transcript_rel = self.resolve_output_path(transcript_path_arg, audio_path)
        transcription = self.transcribe_audio_with_openai(
            audio_path,
            model=model,
            language=language,
            prompt=prompt,
            response_format=response_format,
        )
        transcript_style = "segments" if response_format == "diarized_json" and transcription.get("segments") else "plain"
        transcript_text = self.render_transcript_text(transcription, transcript_style)
        atomic_write_text(transcript_path, transcript_text)
        add_event = self.add_document(str(resolved_transcript_rel))
        audio_bytes = audio_path.read_bytes()
        audio_sha256 = self.store_audio_object(audio_bytes)
        mime_type = mimetypes.guess_type(audio_path.name)[0] or "application/octet-stream"
        link_event = self.append_event(
            "document_audio_linked",
            {
                "document_id": add_event["payload"]["document_id"],
                "audio_sha256": audio_sha256,
                "original_audio_path": str(audio_path),
                "original_audio_filename": audio_path.name,
                "media_type": mime_type,
                "transcription_model": model,
                "transcription_response_format": response_format,
                "transcription_language": transcription.get("language") or language,
                "transcript_style": transcript_style,
                "segment_count": len(transcription.get("segments") or []),
                "transcription_metadata": transcription,
            },
        )
        return {
            "document_id": add_event["payload"]["document_id"],
            "transcript_path": str(resolved_transcript_rel),
            "audio_link_event_id": link_event["event_id"],
        }

    def add_video_document(
        self,
        video_path_arg: str,
        transcript_path_arg: str | None,
        *,
        model: str,
        language: str | None,
        prompt: str | None,
        response_format: str,
        audio_bitrate_kbps: int,
        chunk_overlap_seconds: float,
    ) -> dict[str, Any]:
        video_path = (self.root / video_path_arg).resolve() if not Path(video_path_arg).is_absolute() else Path(video_path_arg)
        if not video_path.is_file():
            raise BewleyError(f"video file not found: {video_path_arg}")
        transcript_path, resolved_transcript_rel = self.resolve_output_path(transcript_path_arg, video_path)
        chunk_plan = self.build_video_chunk_plan(
            video_path,
            max_upload_bytes=OPENAI_MEDIA_TARGET_BYTES,
            overlap_seconds=chunk_overlap_seconds,
            audio_bitrate_kbps=audio_bitrate_kbps,
        )
        chunk_results: list[dict[str, Any]] = []
        chunk_payloads: list[dict[str, Any]] = []
        with tempfile.TemporaryDirectory() as tempdir:
            temp_dir = Path(tempdir)
            for chunk in chunk_plan:
                chunk_path = temp_dir / f"{video_path.stem}.chunk-{chunk['chunk_index']:03d}.mp3"
                self.extract_audio_chunk(
                    video_path,
                    chunk_path,
                    extract_start_seconds=chunk["extract_start_seconds"],
                    extract_end_seconds=chunk["extract_end_seconds"],
                    audio_bitrate_kbps=audio_bitrate_kbps,
                )
                chunk_bytes = chunk_path.read_bytes()
                if len(chunk_bytes) > OPENAI_AUDIO_LIMIT_BYTES:
                    raise BewleyError(
                        f"extracted chunk {chunk['chunk_index']} exceeds OpenAI's 25 MB transcription upload limit"
                    )
                chunk_audio_sha256 = self.store_audio_object(chunk_bytes)
                transcription = self.transcribe_audio_with_openai(
                    chunk_path,
                    model=model,
                    language=language,
                    prompt=prompt,
                    response_format=response_format,
                )
                chunk_results.append({**chunk, "transcription": transcription})
                chunk_payloads.append(
                    {
                        "chunk_index": chunk["chunk_index"],
                        "extract_start_seconds": chunk["extract_start_seconds"],
                        "extract_end_seconds": chunk["extract_end_seconds"],
                        "logical_start_seconds": chunk["logical_start_seconds"],
                        "logical_end_seconds": chunk["logical_end_seconds"],
                        "chunk_audio_sha256": chunk_audio_sha256,
                        "byte_length": len(chunk_bytes),
                        "media_type": "audio/mpeg",
                        "transcription_metadata": transcription,
                    }
                )
        merged_transcription = self.merge_chunk_transcriptions(chunk_results)
        transcript_style = "segments" if self.normalize_segments(merged_transcription) else "plain"
        transcript_text = self.render_transcript_text(merged_transcription, transcript_style)
        atomic_write_text(transcript_path, transcript_text)
        add_event = self.add_document(str(resolved_transcript_rel))
        video_bytes = video_path.read_bytes()
        video_sha256 = self.store_video_object(video_bytes)
        mime_type = mimetypes.guess_type(video_path.name)[0] or "application/octet-stream"
        duration_seconds = max(chunk["logical_end_seconds"] for chunk in chunk_plan)
        link_event = self.append_event(
            "document_video_linked",
            {
                "document_id": add_event["payload"]["document_id"],
                "video_sha256": video_sha256,
                "original_video_path": str(video_path),
                "original_video_filename": video_path.name,
                "media_type": mime_type,
                "duration_seconds": duration_seconds,
                "transcription_model": model,
                "transcription_response_format": response_format,
                "transcription_language": merged_transcription.get("language") or language,
                "transcript_style": transcript_style,
                "chunk_count": len(chunk_payloads),
                "chunks": chunk_payloads,
                "transcription_metadata": merged_transcription,
            },
        )
        return {
            "document_id": add_event["payload"]["document_id"],
            "transcript_path": str(resolved_transcript_rel),
            "video_link_event_id": link_event["event_id"],
        }

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
        new_revision_id = uuid.uuid4().hex
        event = self.append_event(
            "document_updated",
            {
                "document_id": doc["document_id"],
                "current_path": doc["current_path"],
                "revision_id": new_revision_id,
                "content_sha256": digest,
                "byte_length": len(data),
                "line_count": count_lines(text),
                "source_path": doc["current_path"],
                "parent_revision_id": revision["revision_id"],
            },
        )
        self.relocate_annotations(doc["document_id"], revision["revision_id"], new_revision_id)
        return event

    def revision_content(self, conn: sqlite3.Connection, revision_id: str) -> bytes:
        row = conn.execute(
            "SELECT content_sha256 FROM document_revisions WHERE revision_id = ?",
            (revision_id,),
        ).fetchone()
        if row is None:
            raise BewleyError(f"unknown revision: {revision_id}")
        return (self.objects_dir / row["content_sha256"]).read_bytes()

    def relocate_annotations(self, document_id: str, old_revision_id: str, new_revision_id: str) -> None:
        with self.connect() as conn:
            old_bytes = self.revision_content(conn, old_revision_id)
            new_bytes = self.revision_content(conn, new_revision_id)
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
        content = (self.objects_dir / revision["content_sha256"]).read_bytes()
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
        content = (self.objects_dir / revision["content_sha256"]).read_bytes()
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
            if "content_sha256" in payload:
                if event["event_type"] in {"memo_created", "memo_updated"}:
                    obj_path = self.root / PROJECT_DIR / "objects" / "memos" / payload["content_sha256"]
                else:
                    obj_path = self.objects_dir / payload["content_sha256"]
                if not obj_path.exists():
                    problems.append(f"missing object: {payload['content_sha256']}")
            if "audio_sha256" in payload:
                obj_path = self.audio_objects_dir / payload["audio_sha256"]
                if not obj_path.exists():
                    problems.append(f"missing audio object: {payload['audio_sha256']}")
            if "video_sha256" in payload:
                obj_path = self.video_objects_dir / payload["video_sha256"]
                if not obj_path.exists():
                    problems.append(f"missing video object: {payload['video_sha256']}")
            for chunk in payload.get("chunks", []):
                obj_path = self.audio_objects_dir / chunk["chunk_audio_sha256"]
                if not obj_path.exists():
                    problems.append(f"missing chunk audio object: {chunk['chunk_audio_sha256']}")
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
            for table in [
                "documents",
                "document_revisions",
                "document_audio_sources",
                "document_video_sources",
                "document_video_chunks",
                "codes",
                "code_aliases",
                "annotations",
                "events",
                "memos",
                "code_links",
                "project_settings",
            ]:
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
        if target["event_type"] not in {
            "code_renamed", "annotation_added",
            "memo_created", "memo_updated", "memo_deleted",
            "code_parent_set", "code_link_created", "code_link_removed",
            "core_category_set",
        }:
            raise BewleyError(f"undo not supported for event type: {target['event_type']}")
        return self.append_event(
            "undo_recorded",
            {
                "undone_event_id": event_id,
                "undone_event_type": target["event_type"],
                "original_payload": target["payload"],
            },
        )

    # ── Memos ────────────────────────────────────────────────────────────

    def resolve_memo(self, conn: sqlite3.Connection, memo_id: str) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM memos WHERE memo_id = ? AND is_active = 1", (memo_id,)).fetchone()
        if row is None:
            raise BewleyError(f"unknown or deleted memo: {memo_id}")
        return row

    def list_memos(self, conn: sqlite3.Connection, *, target_type: str | None = None, target_id: str | None = None) -> list[sqlite3.Row]:
        query = "SELECT * FROM memos WHERE is_active = 1"
        params: list[Any] = []
        if target_type is not None:
            query += " AND target_type = ?"
            params.append(target_type)
        if target_id is not None:
            query += " AND target_id = ?"
            params.append(target_id)
        query += " ORDER BY created_at"
        return conn.execute(query, params).fetchall()

    def create_memo(self, target_type: str, target_ref: str | None, content: str, title: str | None = None) -> dict[str, Any]:
        target_id: str | None = None
        if target_type == "code":
            with self.connect() as conn:
                target_id = self.resolve_code(conn, target_ref)["code_id"]
        elif target_type == "document":
            with self.connect() as conn:
                target_id = self.resolve_document(conn, target_ref)["document_id"]
        content_sha256 = self.store_memo_object(content)
        return self.append_event(
            "memo_created",
            {
                "memo_id": uuid.uuid4().hex,
                "target_type": target_type,
                "target_id": target_id,
                "title": title,
                "content_sha256": content_sha256,
            },
        )

    def update_memo(self, memo_id: str, content: str) -> dict[str, Any]:
        with self.connect() as conn:
            memo = self.resolve_memo(conn, memo_id)
        old_sha = memo["content_sha256"]
        new_sha = self.store_memo_object(content)
        if old_sha == new_sha:
            raise BewleyError("memo content unchanged")
        return self.append_event(
            "memo_updated",
            {"memo_id": memo_id, "content_sha256": new_sha, "old_content_sha256": old_sha},
        )

    def delete_memo(self, memo_id: str) -> dict[str, Any]:
        with self.connect() as conn:
            self.resolve_memo(conn, memo_id)
        return self.append_event("memo_deleted", {"memo_id": memo_id})

    # ── Code hierarchies ─────────────────────────────────────────────────

    def _would_create_cycle(self, conn: sqlite3.Connection, code_id: str, proposed_parent_id: str) -> bool:
        visited: set[str] = set()
        current: str | None = proposed_parent_id
        while current is not None:
            if current == code_id:
                return True
            if current in visited:
                return True
            visited.add(current)
            row = conn.execute("SELECT parent_code_id FROM codes WHERE code_id = ?", (current,)).fetchone()
            current = row["parent_code_id"] if row else None
        return False

    def set_code_parent(self, code_ref: str, parent_ref: str) -> dict[str, Any]:
        with self.connect() as conn:
            code = self.resolve_code(conn, code_ref)
            parent = self.resolve_code(conn, parent_ref)
            if code["code_id"] == parent["code_id"]:
                raise BewleyError("a code cannot be its own parent")
            if parent["status"] == "merged":
                raise BewleyError("cannot set parent to a merged code")
            if self._would_create_cycle(conn, code["code_id"], parent["code_id"]):
                raise BewleyError("setting this parent would create a cycle")
            old_parent = code["parent_code_id"]
        return self.append_event(
            "code_parent_set",
            {"code_id": code["code_id"], "parent_code_id": parent["code_id"], "old_parent_code_id": old_parent},
        )

    def clear_code_parent(self, code_ref: str) -> dict[str, Any]:
        with self.connect() as conn:
            code = self.resolve_code(conn, code_ref)
            old_parent = code["parent_code_id"]
        if old_parent is None:
            raise BewleyError("code has no parent")
        return self.append_event(
            "code_parent_set",
            {"code_id": code["code_id"], "parent_code_id": None, "old_parent_code_id": old_parent},
        )

    # ── Code links ───────────────────────────────────────────────────────

    def create_code_link(self, source_ref: str, target_ref: str, relationship: str, memo: str | None = None) -> dict[str, Any]:
        with self.connect() as conn:
            source = self.resolve_code(conn, source_ref)
            target = self.resolve_code(conn, target_ref)
            existing = conn.execute(
                """
                SELECT link_id FROM code_links
                WHERE source_code_id = ? AND target_code_id = ? AND relationship = ? AND is_active = 1
                """,
                (source["code_id"], target["code_id"], relationship),
            ).fetchone()
            if existing:
                raise BewleyError(f"duplicate link: {source['canonical_name']} --{relationship}--> {target['canonical_name']}")
        return self.append_event(
            "code_link_created",
            {
                "link_id": uuid.uuid4().hex,
                "source_code_id": source["code_id"],
                "target_code_id": target["code_id"],
                "relationship": relationship,
                "memo": memo,
            },
        )

    def remove_code_link(self, link_id: str) -> dict[str, Any]:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM code_links WHERE link_id = ? AND is_active = 1", (link_id,)).fetchone()
            if row is None:
                raise BewleyError(f"unknown or removed link: {link_id}")
        return self.append_event(
            "code_link_removed",
            {
                "link_id": link_id,
                "source_code_id": row["source_code_id"],
                "target_code_id": row["target_code_id"],
                "relationship": row["relationship"],
            },
        )

    def list_code_links(self, conn: sqlite3.Connection, code_ref: str | None = None) -> list[sqlite3.Row]:
        if code_ref is None:
            return conn.execute("SELECT * FROM code_links WHERE is_active = 1 ORDER BY created_at").fetchall()
        code = self.resolve_code(conn, code_ref)
        return conn.execute(
            "SELECT * FROM code_links WHERE (source_code_id = ? OR target_code_id = ?) AND is_active = 1 ORDER BY created_at",
            (code["code_id"], code["code_id"]),
        ).fetchall()

    # ── Core category ────────────────────────────────────────────────────

    def set_core_category(self, code_ref: str) -> dict[str, Any]:
        with self.connect() as conn:
            code = self.resolve_code(conn, code_ref)
            if code["status"] == "merged":
                raise BewleyError("cannot set a merged code as core category")
            old_row = conn.execute("SELECT value FROM project_settings WHERE key = 'core_category_code_id'").fetchone()
            old_code_id = old_row["value"] if old_row else None
        return self.append_event(
            "core_category_set",
            {"code_id": code["code_id"], "old_code_id": old_code_id},
        )

    def get_core_category(self, conn: sqlite3.Connection) -> sqlite3.Row | None:
        row = conn.execute("SELECT value FROM project_settings WHERE key = 'core_category_code_id'").fetchone()
        if row is None:
            return None
        return conn.execute("SELECT * FROM codes WHERE code_id = ?", (row["value"],)).fetchone()

    # ── Theory export ────────────────────────────────────────────────────

    def export_theory_json(self) -> dict[str, Any]:
        with self.connect() as conn:
            core = self.get_core_category(conn)
            codes = conn.execute("SELECT * FROM codes WHERE status = 'active' ORDER BY canonical_name").fetchall()
            links = conn.execute("SELECT * FROM code_links WHERE is_active = 1 ORDER BY created_at").fetchall()
            memos = conn.execute("SELECT * FROM memos WHERE is_active = 1 ORDER BY created_at").fetchall()
            result: dict[str, Any] = {
                "core_category": {"code_id": core["code_id"], "name": core["canonical_name"]} if core else None,
                "codes": [],
                "hierarchy": [],
                "links": [],
                "memos": [],
            }
            for c in codes:
                ann_count = conn.execute(
                    "SELECT COUNT(*) FROM annotations WHERE code_id = ? AND is_active = 1", (c["code_id"],)
                ).fetchone()[0]
                result["codes"].append({
                    "code_id": c["code_id"],
                    "name": c["canonical_name"],
                    "description": c["description"],
                    "parent_code_id": c["parent_code_id"],
                    "annotation_count": ann_count,
                })
                if c["parent_code_id"]:
                    result["hierarchy"].append({"parent": c["parent_code_id"], "child": c["code_id"]})
            for link in links:
                result["links"].append({
                    "link_id": link["link_id"],
                    "source_code_id": link["source_code_id"],
                    "target_code_id": link["target_code_id"],
                    "relationship": link["relationship"],
                    "memo": link["memo"],
                })
            for m in memos:
                result["memos"].append({
                    "memo_id": m["memo_id"],
                    "target_type": m["target_type"],
                    "target_id": m["target_id"],
                    "title": m["title"],
                    "content_sha256": m["content_sha256"],
                })
        return result

    def export_theory_mermaid(self) -> str:
        data = self.export_theory_json()
        lines = ["graph TD"]
        code_map = {c["code_id"]: c for c in data["codes"]}
        def _node_id(code_id: str) -> str:
            name = code_map[code_id]["name"] if code_id in code_map else code_id[:8]
            return name.replace("-", "_").replace(" ", "_")
        if data["core_category"]:
            lines.append("    classDef core fill:#f9f,stroke:#333,stroke-width:3px")
        for c in data["codes"]:
            nid = _node_id(c["code_id"])
            label = f'{c["name"]} ({c["annotation_count"]})'
            lines.append(f'    {nid}["{label}"]')
            if data["core_category"] and c["code_id"] == data["core_category"]["code_id"]:
                lines.append(f"    {nid}:::core")
        for h in data["hierarchy"]:
            lines.append(f"    {_node_id(h['parent'])} --> {_node_id(h['child'])}")
        for link in data["links"]:
            src = _node_id(link["source_code_id"])
            tgt = _node_id(link["target_code_id"])
            rel = link["relationship"]
            lines.append(f'    {src} -->|"{rel}"| {tgt}')
        return "\n".join(lines) + "\n"

    def export_narrative(self) -> str:
        data = self.export_theory_json()
        code_map = {c["code_id"]: c for c in data["codes"]}
        lines: list[str] = []
        core_name = data["core_category"]["name"] if data["core_category"] else "Unset"
        lines.append(f"# Theory: {core_name}")
        lines.append("")
        # Core category section
        lines.append("## Core Category")
        if data["core_category"]:
            cc = code_map.get(data["core_category"]["code_id"])
            if cc:
                desc = cc.get("description") or ""
                lines.append(f"**{cc['name']}**: {desc}".strip())
        else:
            lines.append("No core category set.")
        lines.append("")
        # Project memos
        project_memos = [m for m in data["memos"] if m["target_type"] == "project"]
        if project_memos:
            lines.append("### Project Memos")
            for m in project_memos:
                title = m.get("title") or m["memo_id"][:8]
                try:
                    content = self.read_memo_content(m["content_sha256"])
                    lines.append(f"- **{title}**: {content.strip()}")
                except BewleyError:
                    lines.append(f"- **{title}**: (content unavailable)")
            lines.append("")
        # Categories
        lines.append("## Categories")
        lines.append("")
        for c in data["codes"]:
            parent_note = ""
            if c["parent_code_id"] and c["parent_code_id"] in code_map:
                parent_note = f" (child of {code_map[c['parent_code_id']]['name']})"
            lines.append(f"### {c['name']}{parent_note} — {c['annotation_count']} annotations")
            if c.get("description"):
                lines.append(c["description"])
            # Code memos
            code_memos = [m for m in data["memos"] if m["target_type"] == "code" and m["target_id"] == c["code_id"]]
            for m in code_memos:
                title = m.get("title") or "Memo"
                try:
                    content = self.read_memo_content(m["content_sha256"])
                    lines.append(f"- *{title}*: {content.strip()}")
                except BewleyError:
                    lines.append(f"- *{title}*: (content unavailable)")
            # Relationships
            code_links = [lk for lk in data["links"] if lk["source_code_id"] == c["code_id"] or lk["target_code_id"] == c["code_id"]]
            for lk in code_links:
                if lk["source_code_id"] == c["code_id"]:
                    other = code_map.get(lk["target_code_id"], {}).get("name", lk["target_code_id"][:8])
                    lines.append(f"- --{lk['relationship']}--> {other}")
                else:
                    other = code_map.get(lk["source_code_id"], {}).get("name", lk["source_code_id"][:8])
                    lines.append(f"- <--{lk['relationship']}-- {other}")
            lines.append("")
        # Summary
        with self.connect() as conn:
            doc_count = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
            active_codes = conn.execute("SELECT COUNT(*) FROM codes WHERE status = 'active'").fetchone()[0]
            active_anns = conn.execute("SELECT COUNT(*) FROM annotations WHERE is_active = 1").fetchone()[0]
        lines.append("## Data Summary")
        lines.append(f"- Documents: {doc_count}")
        lines.append(f"- Active codes: {active_codes}")
        lines.append(f"- Active annotations: {active_anns}")
        lines.append(f"- Core category: {core_name}")
        lines.append("")
        return "\n".join(lines)


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


def _json_output(data: Any) -> None:
    """Print data as formatted JSON to stdout."""
    print(json.dumps(data, ensure_ascii=False, indent=2, default=str))


def _human_kv(data: dict, keys: list[str] | None = None) -> None:
    """Print dict as tab-separated key-value lines."""
    for key in (keys or data.keys()):
        if key in data and data[key] is not None:
            print(f"{key}\t{data[key]}")


def _human_rows(rows: list[dict], columns: list[str]) -> None:
    """Print list of dicts as tab-separated table rows."""
    for row in rows:
        print("\t".join("" if row.get(c) is None else str(row[c]) for c in columns))


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
      --ep-green: #428a5f;
      --ep-green-light: #5ba97a;
      --ep-green-soft: rgba(66, 138, 95, 0.10);
      --ep-dark: #1a1a1a;
      --ep-gray: #666666;
      --ep-light-gray: #f5f5f5;
      --ep-border: #e0e0e0;
      --font-serif: Georgia, 'Times New Roman', serif;
      --font-sans: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
      --font-mono: 'SF Mono', Consolas, 'Liberation Mono', Menlo, monospace;
    }}
    * {{ box-sizing: border-box; }}
    html {{ font-size: 16px; -webkit-font-smoothing: antialiased; }}
    body {{
      font-family: var(--font-sans);
      line-height: 1.5;
      color: var(--ep-dark);
      background: #fff;
      margin: 0;
      padding: 1rem 1.5rem 2rem;
    }}
    .shell {{
      max-width: 1200px;
      margin: 0 auto;
    }}
    header {{
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 1rem;
      border-bottom: 3px solid var(--ep-green);
      padding-bottom: 0.4rem;
      margin-bottom: 1rem;
    }}
    header h1 {{
      margin: 0;
      font-family: var(--font-serif);
      font-size: 1.5rem;
      font-weight: 600;
      color: var(--ep-dark);
    }}
    header .brand {{
      font-family: var(--font-serif);
      color: var(--ep-green);
      font-size: 0.9rem;
    }}
    .summary {{
      display: flex;
      gap: 1.5rem;
      margin-bottom: 0.75rem;
      font-size: 0.85rem;
      color: var(--ep-gray);
    }}
    .summary .stat-value {{
      font-weight: 700;
      color: var(--ep-dark);
    }}
    .controls {{
      display: flex;
      flex-wrap: wrap;
      gap: 0.5rem;
      align-items: center;
      margin-bottom: 1rem;
    }}
    .search {{
      flex: 1 1 200px;
      min-width: 180px;
      border: 1px solid var(--ep-border);
      border-radius: 4px;
      padding: 6px 10px;
      font: inherit;
      font-size: 0.85rem;
      outline: none;
    }}
    .search:focus {{
      border-color: var(--ep-green);
    }}
    .controls button {{
      border: 1px solid var(--ep-border);
      background: #fff;
      color: var(--ep-dark);
      border-radius: 4px;
      padding: 6px 12px;
      font: inherit;
      font-size: 0.8rem;
      cursor: pointer;
    }}
    .controls button:hover {{
      background: var(--ep-light-gray);
    }}
    .controls button.is-active {{
      background: var(--ep-green);
      color: white;
      border-color: var(--ep-green);
    }}
    .layout {{
      display: grid;
      grid-template-columns: 240px minmax(0, 1fr);
      gap: 1rem;
      align-items: start;
    }}
    .sidebar h2, .main h2 {{
      font-family: var(--font-serif);
      font-size: 1rem;
      color: var(--ep-green);
      margin: 0 0 0.5rem;
      border-bottom: 1px solid var(--ep-border);
      padding-bottom: 0.2rem;
    }}
    .code-list {{
      display: grid;
      gap: 4px;
      max-height: 80vh;
      overflow: auto;
    }}
    .code-card {{
      border: 1px solid transparent;
      border-radius: 4px;
      padding: 6px 8px;
      cursor: pointer;
      font-size: 0.85rem;
      line-height: 1.3;
    }}
    .code-card:hover {{
      background: var(--ep-light-gray);
    }}
    .code-card.is-selected {{
      background: var(--ep-green-soft);
      border-color: var(--ep-green);
    }}
    .code-top {{
      display: flex;
      gap: 6px;
      align-items: center;
    }}
    .swatch {{
      width: 10px;
      height: 10px;
      border-radius: 50%;
      flex: 0 0 auto;
    }}
    .code-name {{
      font-weight: 600;
      word-break: break-word;
    }}
    .code-meta {{
      color: var(--ep-gray);
      font-size: 0.78rem;
      margin-top: 2px;
      padding-left: 16px;
    }}
    .snippet-list {{
      display: grid;
      gap: 8px;
    }}
    .snippet {{
      border: 1px solid var(--ep-border);
      border-radius: 6px;
      padding: 10px 12px;
      background: #fff;
    }}
    .snippet-head {{
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 6px;
    }}
    .snippet-title {{
      display: flex;
      gap: 6px;
      align-items: center;
    }}
    .chip {{
      display: inline-flex;
      align-items: center;
      gap: 4px;
      border-radius: 3px;
      background: var(--ep-green-soft);
      padding: 2px 8px;
      color: var(--ep-green);
      font-size: 0.78rem;
      font-weight: 700;
    }}
    .snippet-meta {{
      color: var(--ep-gray);
      font-size: 0.78rem;
    }}
    .context-scroll {{
      max-height: 260px;
      overflow: auto;
      margin-top: 6px;
      border-radius: 4px;
      border: 1px solid var(--ep-border);
    }}
    .context-scroll pre {{
      margin: 0;
      padding: 8px 10px;
      background: var(--ep-light-gray);
      color: var(--ep-dark);
      white-space: pre-wrap;
      font-family: var(--font-mono);
      font-size: 0.82rem;
      line-height: 1.55;
    }}
    .context-scroll .hl {{
      background: rgba(255, 220, 50, 0.45);
      display: inline;
    }}
    .recenter {{
      display: none;
      position: sticky;
      bottom: 4px;
      float: right;
      margin: -24px 4px 0 0;
      border: 1px solid var(--ep-border);
      background: #fff;
      color: var(--ep-gray);
      border-radius: 3px;
      padding: 2px 8px;
      font-size: 0.72rem;
      cursor: pointer;
      z-index: 1;
      opacity: 0.85;
    }}
    .recenter:hover {{
      background: var(--ep-light-gray);
      opacity: 1;
    }}
    .context-scroll.scrolled-away .recenter {{
      display: block;
    }}
    .context-scroll .ctx {{
      color: var(--ep-gray);
      display: inline;
    }}
    .snippet .memo {{
      margin: 6px 0 0;
      font-size: 0.85rem;
      color: var(--ep-gray);
      font-style: italic;
      line-height: 1.4;
    }}
    .empty {{
      border: 1px dashed var(--ep-border);
      border-radius: 6px;
      padding: 1.5rem;
      color: var(--ep-gray);
      text-align: center;
    }}
    .footer {{
      color: var(--ep-gray);
      font-size: 0.78rem;
      text-align: right;
      margin-top: 1rem;
    }}
    @media (max-width: 768px) {{
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
    <header>
      <h1>{safe_title}</h1>
      <span class="brand">E[&#x1f99c;] Expected Parrot</span>
    </header>
    <div class="summary" id="summary"></div>
    <div class="controls">
      <input id="search" class="search" type="search" placeholder="Search codes, documents, snippet text&hellip;">
      <button class="is-active" data-scope="all" type="button">All</button>
      <button data-scope="span" type="button">Span</button>
      <button data-scope="document" type="button">Document</button>
      <button id="clear-filters" type="button">Clear</button>
    </div>
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
    const docTexts = data.document_texts || {{}};
    const state = {{ selectedCode: null, scope: "all", search: "" }};

    const codeListEl = document.getElementById("code-list");
    const snippetListEl = document.getElementById("snippet-list");
    const summaryEl = document.getElementById("summary");
    const footerEl = document.getElementById("footer");
    const searchEl = document.getElementById("search");
    const scopeButtons = Array.from(document.querySelectorAll("[data-scope]"));

    function escapeHtml(v) {{
      return v.replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;");
    }}

    function cardForCode(code) {{
      const sel = state.selectedCode === code.code_id;
      return `<div class="code-card ${{sel ? "is-selected" : ""}}" data-code-id="${{code.code_id}}">
        <div class="code-top">
          <span class="swatch" style="background:${{code.display_color}}"></span>
          <span class="code-name">${{escapeHtml(code.name)}}</span>
        </div>
        <div class="code-meta">${{code.annotation_count}} annot · ${{code.document_count}} doc${{code.document_count !== 1 ? "s" : ""}}</div>
      </div>`;
    }}

    function buildContextHtml(snippet) {{
      if (snippet.scope_type === "document") {{
        const lines = docTexts[snippet.document_path];
        if (lines && lines.length > 0) {{
          const maxLines = Math.min(lines.length, 60);
          let parts = lines.slice(0, maxLines).map(l => `<span class="ctx">${{escapeHtml(l)}}</span>`);
          if (lines.length > maxLines) {{
            parts.push(`<span class="ctx">... (${{lines.length - maxLines}} more lines)</span>`);
          }}
          return `<pre>${{parts.join("\\n")}}</pre>`;
        }}
        return `<pre>&lt;document-level annotation&gt;</pre>`;
      }}
      const lines = docTexts[snippet.document_path];
      if (!lines) {{
        return `<pre>${{escapeHtml(snippet.exact_text || "")}}</pre>`;
      }}
      const start = snippet.start_line;
      const end = snippet.end_line;
      const ctxBefore = Math.max(0, start - 1 - 10);
      const ctxAfter = Math.min(lines.length, end + 10);
      let parts = [];
      for (let i = ctxBefore; i < ctxAfter; i++) {{
        const lineNum = i + 1;
        const escaped = escapeHtml(lines[i]);
        if (lineNum >= start && lineNum <= end) {{
          parts.push(`<span class="hl">${{escaped}}</span>`);
        }} else {{
          parts.push(`<span class="ctx">${{escaped}}</span>`);
        }}
      }}
      return `<pre>${{parts.join("\\n")}}</pre>`;
    }}

    function snippetCard(snippet) {{
      const range = snippet.scope_type === "document"
        ? "Whole document"
        : `Lines ${{snippet.start_line}}\u2013${{snippet.end_line}}`;
      const memo = snippet.memo ? `<div class="memo">${{escapeHtml(snippet.memo)}}</div>` : "";
      const contextHtml = buildContextHtml(snippet);
      return `<article class="snippet">
        <div class="snippet-head">
          <div class="snippet-title">
            <span class="chip"><span class="swatch" style="background:${{snippet.code_color}}"></span>${{escapeHtml(snippet.code_name)}}</span>
          </div>
          <div class="snippet-meta">${{escapeHtml(snippet.document_path)}} &middot; ${{escapeHtml(range)}}</div>
        </div>
        <div class="context-scroll">${{contextHtml}}<button class="recenter" type="button">&uarr; Back to highlight</button></div>
        ${{memo}}
      </article>`;
    }}

    function matchesSnippet(s) {{
      if (state.selectedCode && s.code_id !== state.selectedCode) return false;
      if (state.scope !== "all" && s.scope_type !== state.scope) return false;
      if (!state.search) return true;
      return [s.code_name, s.document_path, s.memo||"", s.exact_text||""].join("\\n").toLowerCase().includes(state.search);
    }}

    function renderSummary(snippets) {{
      const codes = state.selectedCode ? 1 : data.codes.length;
      const docs = new Set(snippets.map(s => s.document_path)).size;
      const conflicted = snippets.filter(s => s.anchor_status === "conflicted").length;
      summaryEl.innerHTML = [
        `<span><span class="stat-value">${{codes}}</span> codes</span>`,
        `<span><span class="stat-value">${{snippets.length}}</span> snippets</span>`,
        `<span><span class="stat-value">${{docs}}</span> documents</span>`,
        conflicted ? `<span><span class="stat-value">${{conflicted}}</span> conflicted</span>` : "",
      ].filter(Boolean).join("");
    }}

    function renderCodes() {{
      codeListEl.innerHTML = data.codes.map(cardForCode).join("");
      for (const n of codeListEl.querySelectorAll(".code-card")) {{
        n.addEventListener("click", () => {{
          const id = n.getAttribute("data-code-id");
          state.selectedCode = state.selectedCode === id ? null : id;
          render();
        }});
      }}
    }}

    function renderSnippets() {{
      const filtered = data.snippets.filter(matchesSnippet);
      snippetListEl.innerHTML = filtered.length
        ? filtered.map(snippetCard).join("")
        : `<div class="empty">No snippets match the current filters.</div>`;
      renderSummary(filtered);
      // Auto-scroll highlighted text into view and wire up recenter buttons
      for (const el of snippetListEl.querySelectorAll(".context-scroll")) {{
        const hl = el.querySelector(".hl");
        if (!hl) continue;
        const scrollToHl = () => {{
          const top = hl.offsetTop - el.offsetTop - el.clientHeight / 3;
          el.scrollTop = Math.max(0, top);
          el.classList.remove("scrolled-away");
        }};
        scrollToHl();
        el.addEventListener("scroll", () => {{
          const hlTop = hl.offsetTop - el.offsetTop;
          const visible = hlTop >= el.scrollTop && hlTop < el.scrollTop + el.clientHeight;
          el.classList.toggle("scrolled-away", !visible);
        }});
        const btn = el.querySelector(".recenter");
        if (btn) btn.addEventListener("click", scrollToHl);
      }}
      footerEl.textContent = `Generated ${{data.generated_at}}`;
    }}

    function render() {{
      scopeButtons.forEach(b => b.classList.toggle("is-active", b.dataset.scope === state.scope));
      renderCodes();
      renderSnippets();
    }}

    searchEl.addEventListener("input", e => {{
      state.search = e.target.value.trim().toLowerCase();
      renderSnippets();
    }});
    for (const b of scopeButtons) {{
      b.addEventListener("click", () => {{ state.scope = b.dataset.scope; render(); }});
    }}
    document.getElementById("clear-filters").addEventListener("click", () => {{
      state.selectedCode = null; state.scope = "all"; state.search = ""; searchEl.value = "";
      render();
    }});
    render();
  </script>
</body>
</html>
"""


def build_static_code_explorer_html(payload: dict[str, Any], title: str) -> str:
    """Build a pure HTML/CSS code explorer with no JavaScript.

    For whole-document annotations, the full document text is rendered in the
    snippet area.  For span annotations, the exact annotated text is shown with
    context lines from the surrounding document.
    """
    safe_title = html.escape(title)
    codes = sorted(payload["codes"], key=lambda c: -c["annotation_count"])
    codes = [c for c in codes if c["annotation_count"] > 0]
    doc_texts: dict[str, list[str]] = payload.get("document_texts", {})

    # Group snippets by code
    snippets_by_code: dict[str, list[dict]] = {}
    for s in payload.get("snippets", []):
        snippets_by_code.setdefault(s["code_name"], []).append(s)

    def _escape(text: str) -> str:
        return html.escape(text)

    def _snippet_html(s: dict) -> str:
        scope = s.get("scope_type", "document")
        doc_path = s.get("document_path", "")
        if scope == "span" and s.get("exact_text"):
            text = s["exact_text"].strip()
        elif scope == "document" and doc_path in doc_texts:
            text = "\n".join(doc_texts[doc_path])
        elif s.get("exact_text"):
            text = s["exact_text"].strip()
        else:
            text = "(document-level annotation)"
        memo = s.get("memo") or ""
        lines = []
        lines.append('<div class="snippet">')
        lines.append(f'<pre class="snippet-text">{_escape(text)}</pre>')
        if memo:
            lines.append(f'<div class="snippet-memo">{_escape(memo)}</div>')
        lines.append(f'<div class="snippet-source">{_escape(doc_path)}</div>')
        lines.append('</div>')
        return "\n".join(lines)

    # Build code sections
    code_sections = []
    for c in codes:
        slug = c["name"].replace("/", "-")
        color = c.get("display_color", "var(--ep-green)")
        desc = c.get("description", "")
        code_snippets = snippets_by_code.get(c["name"], [])

        section_lines = []
        section_lines.append(f'<section class="code-section" id="{_escape(slug)}">')
        section_lines.append(f'<div class="code-header">')
        section_lines.append(f'<h2><span class="color-dot" style="background:{color}"></span>{_escape(c["name"])}</h2>')
        section_lines.append(f'<span class="badge">{c["annotation_count"]} annotation{"s" if c["annotation_count"] != 1 else ""}</span>')
        section_lines.append('</div>')
        if desc:
            section_lines.append(f'<p class="desc">{_escape(desc)}</p>')
        for s in code_snippets:
            section_lines.append(_snippet_html(s))
        section_lines.append('</section>')
        code_sections.append("\n".join(section_lines))

    # Build TOC
    toc_items = []
    for c in codes:
        slug = c["name"].replace("/", "-")
        toc_items.append(
            f'<div class="toc-item">'
            f'<a href="#{_escape(slug)}">{_escape(c["name"])}</a> '
            f'<span class="toc-count">({c["annotation_count"]})</span>'
            f'</div>'
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{safe_title}</title>
  <style>
    :root {{
      --ep-green: #428a5f;
      --ep-green-light: #5ba97a;
      --ep-green-soft: rgba(66, 138, 95, 0.10);
      --ep-dark: #1a1a1a;
      --ep-gray: #666666;
      --ep-light-gray: #f5f5f5;
      --ep-border: #e0e0e0;
      --font-serif: Georgia, 'Times New Roman', serif;
      --font-sans: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
      --font-mono: 'SF Mono', Consolas, 'Liberation Mono', Menlo, monospace;
    }}
    * {{ box-sizing: border-box; }}
    html {{ font-size: 16px; -webkit-font-smoothing: antialiased; }}
    body {{
      font-family: var(--font-sans);
      line-height: 1.6;
      color: var(--ep-dark);
      background: #fff;
      margin: 0;
      padding: 1rem 1.5rem 2rem;
    }}
    .shell {{ max-width: 960px; margin: 0 auto; }}
    header {{
      border-bottom: 3px solid var(--ep-green);
      padding-bottom: 0.4rem;
      margin-bottom: 1rem;
      display: flex;
      align-items: baseline;
      justify-content: space-between;
    }}
    header h1 {{
      margin: 0;
      font-family: var(--font-serif);
      font-size: 1.5rem;
      font-weight: 600;
      color: var(--ep-dark);
    }}
    header .brand {{
      font-family: var(--font-serif);
      color: var(--ep-green);
      font-size: 0.9rem;
    }}
    .stats {{
      color: var(--ep-gray);
      font-size: 0.9rem;
      margin-bottom: 1.5rem;
    }}
    .toc {{
      columns: 2;
      column-gap: 2rem;
      margin-bottom: 2rem;
      padding: 1rem;
      background: var(--ep-light-gray);
      border-radius: 6px;
    }}
    .toc-item {{
      break-inside: avoid;
      padding: 0.15rem 0;
    }}
    .toc a {{
      text-decoration: none;
      color: var(--ep-dark);
      font-family: var(--font-mono);
      font-size: 0.85rem;
    }}
    .toc a:hover {{ color: var(--ep-green); }}
    .toc-count {{ color: var(--ep-gray); font-size: 0.8rem; }}
    .code-section {{
      margin-bottom: 2rem;
      border-top: 1px solid var(--ep-border);
      padding-top: 0.5rem;
    }}
    .code-header {{
      display: flex;
      align-items: baseline;
      gap: 0.8rem;
      flex-wrap: wrap;
    }}
    .code-header h2 {{
      flex: 1;
      font-family: var(--font-mono);
      font-size: 1.1rem;
      color: var(--ep-dark);
      margin: 0.5rem 0 0.2rem 0;
    }}
    .color-dot {{
      display: inline-block;
      width: 10px;
      height: 10px;
      border-radius: 50%;
      margin-right: 0.5rem;
      vertical-align: middle;
    }}
    .badge {{
      display: inline-block;
      background: var(--ep-green);
      color: #fff;
      font-size: 0.75rem;
      padding: 0.15rem 0.5rem;
      border-radius: 10px;
      white-space: nowrap;
    }}
    .desc {{
      color: var(--ep-gray);
      font-style: italic;
      margin: 0.2rem 0 0.8rem 0;
    }}
    .snippet {{
      background: var(--ep-light-gray);
      border-left: 3px solid var(--ep-green);
      padding: 0.6rem 1rem;
      margin-bottom: 0.6rem;
      border-radius: 0 4px 4px 0;
    }}
    .snippet-text {{
      font-family: var(--font-sans);
      font-size: 0.9rem;
      white-space: pre-wrap;
      word-wrap: break-word;
      margin: 0;
      max-height: 20rem;
      overflow-y: auto;
    }}
    .snippet-memo {{
      font-size: 0.8rem;
      color: var(--ep-green);
      font-style: italic;
      margin-top: 0.3rem;
    }}
    .snippet-source {{
      font-size: 0.8rem;
      color: var(--ep-gray);
      margin-top: 0.3rem;
      font-family: var(--font-mono);
    }}
    .footer {{
      text-align: center;
      color: var(--ep-gray);
      font-size: 0.8rem;
      margin-top: 3rem;
      border-top: 1px solid var(--ep-border);
      padding-top: 1rem;
    }}
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <h1>{safe_title}</h1>
      <span class="brand">Expected Parrot</span>
    </header>
    <p class="stats">{payload['code_count']} codes &middot; {payload['snippet_count']} annotations &middot; {payload['document_count']} documents</p>
    <div class="toc">
{"".join(toc_items)}
    </div>
{"".join(code_sections)}
    <div class="footer">Generated {payload.get('generated_at', '')} by bewley &middot; Expected Parrot</div>
  </div>
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
    parser = argparse.ArgumentParser(
        prog="bewley",
        description=(
            "Bewley — a local-first CLI for qualitative coding of interview data and UTF-8 text corpora.\n\n"
            "Bewley manages a project stored in a .bewley/ directory (similar to .git/).\n"
            "Every mutation is recorded as an append-only JSON event. The SQLite index\n"
            "is a derived projection and can be rebuilt from events at any time.\n\n"
            "Quick start:\n"
            "  bewley init                         Create a new project in the current directory\n"
            "  bewley add interview.txt             Add a document to the corpus\n"
            "  bewley code create themes/trust      Create an analytic code\n"
            "  bewley annotate apply trust doc1 --lines 10:20   Apply code to a text span\n"
            "  bewley query 'trust & rapport'       Query annotations by boolean expression\n"
            "  bewley export snippets --code trust --format text   Export coded snippets\n\n"
            "Use 'bewley <command> --help' for detailed help on any command.\n"
            "Use 'bewley <command> <subcommand> --help' for help on subcommands."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--human-output", "-H", action="store_true", default=False,
                        help="Output human-readable text instead of JSON.")
    sub = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    # --- Project management ---

    sub.add_parser(
        "init",
        help="Create a new bewley project in the current directory.",
        description=(
            "Initialize a new bewley project in the current working directory.\n\n"
            "Creates the .bewley/ metadata directory with subdirectories for events,\n"
            "objects, index, locks, and refs. Also creates an empty corpus/ directory\n"
            "for user documents.\n\n"
            "This command must be run once before any other bewley command.\n"
            "It is safe to run in an already-initialized directory (it will error\n"
            "if .bewley/ already exists).\n\n"
            "Output: prints 'initialized' on success."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    sub.add_parser(
        "status",
        help="Show project summary: document, revision, code, and annotation counts.",
        description=(
            "Display a tab-separated summary of the current project state.\n\n"
            "Output columns (one per line, tab-separated key/value):\n"
            "  documents                Number of logical documents\n"
            "  revisions                Total document revisions across all documents\n"
            "  codes                    Number of analytic codes\n"
            "  active_annotations       Number of active (non-removed) annotations\n"
            "  conflicted_annotations   Annotations with anchor_status='conflicted'\n\n"
            "Exit code: 0 on success."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    sub.add_parser(
        "fsck",
        help="Verify project integrity: events, objects, and index consistency.",
        description=(
            "Run integrity checks on the project.\n\n"
            "Validates:\n"
            "  - Event sequence numbering and hash chains\n"
            "  - Content-addressed object integrity (SHA-256 verification)\n"
            "  - Referential integrity between events and index\n\n"
            "Output: prints 'ok' if no problems found.\n"
            "On failure: prints each problem to stderr, exits with code 1."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    sub.add_parser(
        "rebuild-index",
        help="Rebuild the SQLite index from the append-only event log.",
        description=(
            "Destroy and rebuild the SQLite index (bewley.sqlite) by replaying\n"
            "all events from .bewley/events/.\n\n"
            "This is safe because the event log is the source of truth, not SQLite.\n"
            "Use this if the index becomes corrupted or after manual event edits.\n\n"
            "Output: prints 'rebuilt' on success."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # --- Document management ---

    add = sub.add_parser(
        "add",
        help="Add a UTF-8 text file to the corpus as a new document.",
        description=(
            "Add a new document to the project corpus.\n\n"
            "The file must be valid UTF-8. It is copied into content-addressed\n"
            "storage (.bewley/objects/documents/<sha256>) and a corresponding\n"
            "entry is created in the corpus/ directory.\n\n"
            "Output: prints the new document_id (a UUID).\n\n"
            "Example:\n"
            "  bewley add interviews/participant_01.txt"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    add.add_argument("path", help="Path to the UTF-8 text file to add.")

    add_audio = sub.add_parser(
        "add-audio",
        help="Transcribe an audio file with OpenAI and add the transcript to the corpus.",
        description=(
            "Transcribe an audio file with OpenAI, write the transcript into the corpus,\n"
            "track it as a Bewley document, and store explicit linkage metadata back to\n"
            "the source audio file.\n\n"
            "By default the transcript is written to corpus/<audio-stem>.txt.\n"
            "If you use the diarization model with --response-format diarized_json,\n"
            "the transcript text is rendered as timestamped speaker turns to make the\n"
            "connection between transcript and recording inspectable.\n\n"
            "Requires OPENAI_API_KEY in the environment.\n\n"
            "Example:\n"
            "  bewley add-audio interviews/alice.m4a --output corpus/alice.txt\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    add_audio.add_argument("audio_path", help="Path to the source audio file.")
    add_audio.add_argument("--output", help="Transcript path to create inside the project. Default: corpus/<audio-stem>.txt")
    add_audio.add_argument("--model", default="gpt-4o-transcribe", help="OpenAI transcription model.")
    add_audio.add_argument(
        "--response-format",
        default="json",
        choices=["json", "verbose_json", "diarized_json"],
        help="Requested OpenAI transcription response format.",
    )
    add_audio.add_argument("--language", help="Optional language hint like 'en'.")
    add_audio.add_argument("--prompt", help="Optional transcription prompt.")

    add_video = sub.add_parser(
        "add-video",
        help="Extract audio from a video, transcribe it, and add the transcript to the corpus.",
        description=(
            "Extract audio from a video file with ffmpeg, split it into transcription-safe\n"
            "chunks when needed, transcribe each chunk with OpenAI, merge the results into\n"
            "one transcript, and track the transcript as a Bewley document.\n\n"
            "Chunk metadata and the original video file are stored so the transcript can be\n"
            "traced back to the source media.\n\n"
            "Requires ffmpeg, ffprobe, and OPENAI_API_KEY.\n\n"
            "Example:\n"
            "  bewley add-video interviews/alice.mp4 --output corpus/alice.txt\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    add_video.add_argument("video_path", help="Path to the source video file.")
    add_video.add_argument("--output", help="Transcript path to create inside the project. Default: corpus/<video-stem>.txt")
    add_video.add_argument("--model", default="gpt-4o-transcribe", help="OpenAI transcription model.")
    add_video.add_argument(
        "--response-format",
        default="verbose_json",
        choices=["json", "verbose_json", "diarized_json"],
        help="Requested OpenAI transcription response format.",
    )
    add_video.add_argument("--language", help="Optional language hint like 'en'.")
    add_video.add_argument("--prompt", help="Optional transcription prompt.")
    add_video.add_argument(
        "--audio-bitrate-kbps",
        type=int,
        default=DEFAULT_EXTRACT_AUDIO_BITRATE_KBPS,
        help="Bitrate for extracted audio chunks. Lower values allow longer chunks.",
    )
    add_video.add_argument(
        "--chunk-overlap-seconds",
        type=float,
        default=DEFAULT_VIDEO_CHUNK_OVERLAP_SECONDS,
        help="Overlap between adjacent logical chunks to avoid losing speech at boundaries.",
    )

    update = sub.add_parser(
        "update",
        help="Update an existing document with a new revision from the file on disk.",
        description=(
            "Create a new revision of an existing document.\n\n"
            "The file at the given path must already be tracked by bewley (i.e.,\n"
            "it was previously added with 'bewley add'). If the file content has\n"
            "changed since the last revision, a new immutable revision is stored.\n"
            "Existing span annotations are relocated to the new revision using\n"
            "best-effort fuzzy matching.\n\n"
            "Output: prints the new revision_id, or 'no-op' if content is unchanged.\n\n"
            "Example:\n"
            "  bewley update corpus/participant_01.txt"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    update.add_argument("path", help="Path to the updated UTF-8 text file (must already be tracked).")

    list_parser = sub.add_parser(
        "list",
        help="List project entities (documents, etc.).",
        description="List entities in the project. Use a subcommand to specify what to list.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    list_sub = list_parser.add_subparsers(dest="list_what", required=True, metavar="ENTITY")
    list_sub.add_parser(
        "documents",
        help="List all documents with their IDs, paths, and revision counts.",
        description=(
            "List all documents in the project.\n\n"
            "Output: tab-separated table with columns:\n"
            "  document_id    UUID identifying the logical document\n"
            "  current_path   Relative path in the corpus/ directory\n"
            "  revision_count Number of stored revisions"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    show = sub.add_parser(
        "show",
        help="Show detailed information about a document or code snippets.",
        description="Show detailed information. Use a subcommand to specify what to show.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    show_sub = show.add_subparsers(dest="show_what", required=True, metavar="ENTITY")

    show_doc = show_sub.add_parser(
        "document",
        help="Show metadata, revisions, and annotations for a document.",
        description=(
            "Display detailed information about a single document.\n\n"
            "Shows: document_id, current path, all revisions (with timestamps,\n"
            "byte lengths, line counts), and all active annotations on the document.\n\n"
            "The document_ref can be a document_id (UUID), a path, or a path prefix.\n\n"
            "Example:\n"
            "  bewley show document participant_01"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    show_doc.add_argument("document_ref", help="Document identifier: UUID, path, or path prefix.")

    show_audio = show_sub.add_parser(
        "audio",
        help="Show the audio source linked to a transcript document.",
        description=(
            "Display the stored audio/transcription provenance for a document created\n"
            "from audio, including the original audio path, stored object path, model,\n"
            "response format, and any diarized segments with timestamps.\n\n"
            "Example:\n"
            "  bewley show audio corpus/alice.txt"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    show_audio.add_argument("document_ref", help="Document identifier: UUID, path, or path prefix.")

    show_video = show_sub.add_parser(
        "video",
        help="Show the video source and chunk metadata linked to a transcript document.",
        description=(
            "Display the stored video/transcription provenance for a document created\n"
            "from video, including the original video path, stored object path, chunk\n"
            "boundaries, and any returned timestamped segments.\n\n"
            "Example:\n"
            "  bewley show video corpus/alice.txt"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    show_video.add_argument("document_ref", help="Document identifier: UUID, path, or path prefix.")

    show_snippets = show_sub.add_parser(
        "snippets",
        help="Show text snippets for all annotations of a given code.",
        description=(
            "Display the actual text content of every annotation for a code.\n\n"
            "For span annotations, shows the text within the byte range.\n"
            "For document-level annotations, shows '(whole document)'.\n\n"
            "Example:\n"
            "  bewley show snippets --code trust"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    show_snippets.add_argument("--code", required=True, help="Code name, alias, or code_id to show snippets for.")

    # --- Code management ---

    code = sub.add_parser(
        "code",
        help="Create, list, show, rename, merge, split, and organize analytic codes.",
        description=(
            "Manage analytic codes (qualitative labels applied to text).\n\n"
            "Codes are the primary analytic unit in qualitative coding. Each code\n"
            "has a unique code_id, a canonical name, optional description and color,\n"
            "and may have aliases, a parent (for hierarchy), and links to other codes.\n\n"
            "Subcommands:\n"
            "  create        Create a new code\n"
            "  list          List all codes\n"
            "  show          Show details of a single code\n"
            "  rename        Rename a code\n"
            "  alias         Add an alias to a code\n"
            "  merge         Merge multiple codes into one\n"
            "  split         Split annotations from one code into a new code\n"
            "  set-parent    Set a code's parent in the hierarchy\n"
            "  clear-parent  Remove a code from its parent\n"
            "  link          Create a named relationship between two codes\n"
            "  links         List code relationships\n"
            "  unlink        Remove a code relationship\n"
            "  set-core      Designate a code as the core category\n"
            "  show-core     Show the current core category"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    code_sub = code.add_subparsers(dest="code_cmd", required=True, metavar="SUBCOMMAND")

    code_create = code_sub.add_parser(
        "create",
        help="Create a new analytic code.",
        description=(
            "Create a new analytic code with a given name.\n\n"
            "Code names may contain slashes for organizational grouping\n"
            "(e.g., 'themes/trust', 'emotions/positive/joy').\n\n"
            "Output: prints the new code_id (a UUID).\n\n"
            "Examples:\n"
            "  bewley code create trust\n"
            "  bewley code create themes/rapport --description 'Mutual understanding'\n"
            "  bewley code create urgent --color red"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    code_create.add_argument("name", help="Name for the new code (may include slashes for grouping).")
    code_create.add_argument("--description", help="Free-text description of what this code represents.")
    code_create.add_argument("--color", help="Display color (for HTML exports).")

    code_list_p = code_sub.add_parser(
        "list",
        help="List all codes with their IDs, names, and annotation counts.",
        description=(
            "List all analytic codes in the project.\n\n"
            "Output: tab-separated table with columns:\n"
            "  code_id          UUID identifying the code\n"
            "  canonical_name   Current name of the code\n"
            "  annotation_count Number of active annotations using this code\n\n"
            "With --tree: displays codes as an indented hierarchy based on\n"
            "parent-child relationships set via 'code set-parent'."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    code_list_p.add_argument("--tree", action="store_true", help="Show codes as an indented parent-child hierarchy.")

    code_show = code_sub.add_parser(
        "show",
        help="Show detailed info for a code: metadata, aliases, annotations.",
        description=(
            "Display detailed information about a single code.\n\n"
            "Shows: code_id, canonical name, description, color, aliases,\n"
            "parent code (if any), and all active annotations.\n\n"
            "The code_ref can be a code_id (UUID), canonical name, or alias.\n\n"
            "Example:\n"
            "  bewley code show trust"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    code_show.add_argument("code_ref", help="Code identifier: UUID, canonical name, or alias.")

    code_rename = code_sub.add_parser(
        "rename",
        help="Rename a code (all annotations follow automatically).",
        description=(
            "Change the canonical name of an existing code.\n\n"
            "All annotations referencing this code remain valid. The old name\n"
            "is not retained as an alias (use 'code alias' if you want that).\n\n"
            "Output: prints the event_id.\n\n"
            "Example:\n"
            "  bewley code rename trust interpersonal_trust"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    code_rename.add_argument("old", help="Current name (or code_id) of the code to rename.")
    code_rename.add_argument("new", help="New canonical name for the code.")

    code_alias = code_sub.add_parser(
        "alias",
        help="Add an alternative name (alias) to a code.",
        description=(
            "Add an alias to an existing code. After aliasing, the code can\n"
            "be referenced by either its canonical name or any alias.\n\n"
            "Output: prints the event_id.\n\n"
            "Example:\n"
            "  bewley code alias trust rapport"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    code_alias.add_argument("code_ref", help="Code to add the alias to (name, alias, or UUID).")
    code_alias.add_argument("alias", help="New alias name.")

    code_merge = code_sub.add_parser(
        "merge",
        help="Merge one or more source codes into a target code.",
        description=(
            "Merge multiple source codes into a single target code.\n\n"
            "All annotations from the source codes are reassigned to the target.\n"
            "Source codes are deactivated after merging. This is useful when\n"
            "codes turn out to represent the same concept.\n\n"
            "Output: prints the event_id.\n\n"
            "Example:\n"
            "  bewley code merge trust_v1 trust_v2 --into trust"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    code_merge.add_argument("sources", nargs="+", help="One or more source codes to merge (names or UUIDs).")
    code_merge.add_argument("--into", required=True, help="Target code that absorbs the source annotations.")

    code_split = code_sub.add_parser(
        "split",
        help="Move selected annotations from one code to a new code.",
        description=(
            "Split a subset of annotations from an existing code into a new code.\n\n"
            "You must specify which annotations to move using --annotation flags.\n"
            "The source code retains all annotations not explicitly moved.\n\n"
            "Output: prints the new code_id.\n\n"
            "Example:\n"
            "  bewley code split trust --new deep_trust --annotation ann1 --annotation ann2"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    code_split.add_argument("source", help="Source code to split from (name or UUID).")
    code_split.add_argument("--new", required=True, help="Name for the new code.")
    code_split.add_argument("--annotation", action="append", default=[], help="Annotation ID to move (repeat for multiple).")
    code_split.add_argument("--description", help="Description for the new code.")
    code_split.add_argument("--color", help="Color for the new code.")

    code_set_parent = code_sub.add_parser(
        "set-parent",
        help="Set a code's parent to build a hierarchical code tree.",
        description=(
            "Assign a parent code, creating a hierarchical relationship.\n\n"
            "Use 'code list --tree' to visualize the hierarchy.\n\n"
            "Output: prints the event_id.\n\n"
            "Example:\n"
            "  bewley code set-parent deep_trust trust"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    code_set_parent.add_argument("code_ref", help="Child code (name or UUID).")
    code_set_parent.add_argument("parent_ref", help="Parent code (name or UUID).")

    code_clear_parent = code_sub.add_parser(
        "clear-parent",
        help="Remove a code from its parent (make it a root code).",
        description=(
            "Remove the parent-child relationship for a code, making it a root node.\n\n"
            "Output: prints the event_id.\n\n"
            "Example:\n"
            "  bewley code clear-parent deep_trust"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    code_clear_parent.add_argument("code_ref", help="Code to detach from its parent (name or UUID).")

    code_link = code_sub.add_parser(
        "link",
        help="Create a named relationship (link) between two codes.",
        description=(
            "Create a directional, labeled relationship between two codes.\n\n"
            "The relationship is a free-text string (e.g., 'causes', 'contradicts',\n"
            "'is_context_for'). Optionally attach a memo explaining the link.\n\n"
            "Output: prints the new link_id.\n\n"
            "Example:\n"
            "  bewley code link trust rapport causes --memo 'Trust enables rapport'"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    code_link.add_argument("source", help="Source code of the relationship (name or UUID).")
    code_link.add_argument("target", help="Target code of the relationship (name or UUID).")
    code_link.add_argument("relationship", help="Label for the relationship (e.g., 'causes', 'contradicts').")
    code_link.add_argument("--memo", help="Optional memo explaining the link.")

    code_links_p = code_sub.add_parser(
        "links",
        help="List relationships (links) between codes.",
        description=(
            "List all code-to-code relationships, optionally filtered to a single code.\n\n"
            "Output: tab-separated table with columns:\n"
            "  link_id        UUID of the link\n"
            "  source_name    Source code name\n"
            "  relationship   Relationship label\n"
            "  target_name    Target code name\n\n"
            "Example:\n"
            "  bewley code links          # all links\n"
            "  bewley code links trust    # links involving 'trust'"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    code_links_p.add_argument("code_ref", nargs="?", help="Optional code to filter links by (name or UUID).")

    code_unlink = code_sub.add_parser(
        "unlink",
        help="Remove a relationship (link) between two codes.",
        description=(
            "Remove a code-to-code relationship by its link_id.\n\n"
            "Use 'bewley code links' to find the link_id.\n\n"
            "Output: prints the event_id."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    code_unlink.add_argument("link_id", help="UUID of the link to remove (from 'code links' output).")

    code_set_core = code_sub.add_parser(
        "set-core",
        help="Designate a code as the core category for grounded theory.",
        description=(
            "Set a code as the project's core category.\n\n"
            "In grounded theory, the core category is the central concept that\n"
            "integrates and explains the main pattern in the data.\n\n"
            "Output: prints the event_id.\n\n"
            "Example:\n"
            "  bewley code set-core trust"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    code_set_core.add_argument("code_ref", help="Code to designate as core category (name or UUID).")

    code_sub.add_parser(
        "show-core",
        help="Show the current core category (if set).",
        description=(
            "Display the project's current core category.\n\n"
            "Output: tab-separated code_id and canonical_name, or\n"
            "'no core category set' if none is designated."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # --- Annotation management ---

    annotate = sub.add_parser(
        "annotate",
        help="Apply, remove, show, or resolve annotations (coded labels on text).",
        description=(
            "Manage annotations — applications of codes to documents or text spans.\n\n"
            "An annotation links a code to either:\n"
            "  - A whole document (--document)\n"
            "  - A byte range within a document revision (--bytes START:END)\n"
            "  - A line range within a document revision (--lines START:END)\n\n"
            "Subcommands:\n"
            "  apply     Apply a code to a document or text span\n"
            "  remove    Remove an annotation\n"
            "  show      Show details of a single annotation\n"
            "  resolve   Manually fix a conflicted annotation's byte range"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    annotate_sub = annotate.add_subparsers(dest="annotate_cmd", required=True, metavar="SUBCOMMAND")

    ann_apply = annotate_sub.add_parser(
        "apply",
        help="Apply a code to a document or text span.",
        description=(
            "Create a new annotation linking a code to a document.\n\n"
            "Exactly one of --document, --bytes, or --lines is required:\n"
            "  --document       Apply to the whole document\n"
            "  --bytes S:E      Apply to byte range [S, E) in the current revision\n"
            "  --lines S:E      Apply to line range [S, E] (1-based, inclusive),\n"
            "                   which is converted to a byte range internally\n\n"
            "Output: prints the new annotation_id (a UUID).\n\n"
            "Examples:\n"
            "  bewley annotate apply trust doc1 --document\n"
            "  bewley annotate apply trust participant_01 --bytes 100:250\n"
            "  bewley annotate apply trust participant_01 --lines 10:20\n"
            "  bewley annotate apply trust doc1 --lines 5:8 --memo 'Key passage about trust'"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ann_apply.add_argument("code_ref", help="Code to apply (name, alias, or UUID).")
    ann_apply.add_argument("document_ref", help="Document to annotate (UUID, path, or path prefix).")
    mode_group = ann_apply.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--document", action="store_true", help="Apply code to the entire document.")
    mode_group.add_argument("--bytes", help="Byte range as START:END (0-based, exclusive end).")
    mode_group.add_argument("--lines", help="Line range as START:END (1-based, inclusive).")
    ann_apply.add_argument("--memo", help="Optional memo to attach to this annotation.")

    ann_remove = annotate_sub.add_parser(
        "remove",
        help="Remove (deactivate) an annotation.",
        description=(
            "Remove an annotation by its annotation_id.\n\n"
            "The annotation is deactivated (not deleted from history).\n"
            "This can be undone with 'bewley undo'.\n\n"
            "Output: prints the event_id."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ann_remove.add_argument("annotation_id", help="UUID of the annotation to remove.")

    ann_show = annotate_sub.add_parser(
        "show",
        help="Show full details of a single annotation.",
        description=(
            "Display detailed metadata for an annotation.\n\n"
            "Shows: annotation_id, code, document, scope type, byte range,\n"
            "line range, anchor status, and the annotated text content.\n\n"
            "Example:\n"
            "  bewley annotate show <annotation_id>"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ann_show.add_argument("annotation_id", help="UUID of the annotation to show.")

    ann_resolve = annotate_sub.add_parser(
        "resolve",
        help="Manually resolve a conflicted annotation by setting a new byte range.",
        description=(
            "Fix a conflicted annotation by specifying the correct byte range\n"
            "in the current document revision.\n\n"
            "When a document is updated, span annotations are relocated using\n"
            "fuzzy matching. If matching confidence is below the threshold,\n"
            "the annotation is marked 'conflicted'. Use this command to\n"
            "manually set the correct range.\n\n"
            "Output: prints the event_id.\n\n"
            "Example:\n"
            "  bewley annotate resolve <annotation_id> --bytes 120:280"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ann_resolve.add_argument("annotation_id", help="UUID of the conflicted annotation.")
    ann_resolve.add_argument("--bytes", required=True, help="New byte range as START:END.")
    ann_resolve.add_argument("--memo", help="Optional memo explaining the resolution.")

    # --- Querying ---

    query = sub.add_parser(
        "query",
        help="Query annotations using boolean code expressions (AND, OR, NOT).",
        description=(
            "Search for documents or annotations matching a boolean code expression.\n\n"
            "Expression syntax:\n"
            "  code_name           Documents/annotations with this code\n"
            "  A & B               AND — both codes present\n"
            "  A | B               OR — either code present\n"
            "  !A                  NOT — code absent\n"
            "  (A & B) | C         Parentheses for grouping\n\n"
            "Modes:\n"
            "  --mode document     (default) Return documents matching the expression\n"
            "  --mode annotation   Return individual annotations matching the expression\n\n"
            "Output: tab-separated table of matching entities.\n\n"
            "Examples:\n"
            "  bewley query trust\n"
            "  bewley query 'trust & rapport'\n"
            "  bewley query '!trust' --mode document\n"
            "  bewley query '(trust | rapport) & !small_talk' --mode annotation"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    query.add_argument("expr", help="Boolean code expression (use quotes if it contains spaces or shell metacharacters).")
    query.add_argument("--mode", choices=["document", "annotation"], help="Query mode: 'document' (default) or 'annotation'.")

    # --- Export ---

    export = sub.add_parser(
        "export",
        help="Export coded data as snippets, quotes, HTML, theory diagrams, or narratives.",
        description=(
            "Export project data in various formats.\n\n"
            "Subcommands:\n"
            "  snippets        Export text snippets for a code (JSONL or plain text)\n"
            "  quotes          Export quotes filtered by code or query expression\n"
            "  html            Export all codes and annotations as a standalone HTML page\n"
            "  document-html   Export a single document with inline annotations as HTML\n"
            "  theory          Export code hierarchy, links, and core category as JSON or Mermaid\n"
            "  narrative       Export an integrative narrative summary"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    export_sub = export.add_subparsers(dest="export_what", required=True, metavar="FORMAT")

    export_snippets = export_sub.add_parser(
        "snippets",
        help="Export text snippets for a code as JSONL or plain text.",
        description=(
            "Export all annotated text snippets for a given code.\n\n"
            "Formats:\n"
            "  text   Plain text with snippet boundaries marked\n"
            "  jsonl  One JSON object per line with fields:\n"
            "         annotation_id, code, document_path, text, start_byte, end_byte\n\n"
            "Example:\n"
            "  bewley export snippets --code trust --format jsonl\n"
            "  bewley export snippets --code trust --format text --context-lines 2"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    export_snippets.add_argument("--code", required=True, help="Code to export snippets for (name or UUID).")
    export_snippets.add_argument("--format", choices=["jsonl", "text"], default="text", help="Output format for human mode: 'jsonl' or 'text'.")
    export_snippets.add_argument("--context-lines", type=int, default=0, help="Number of surrounding lines to include (default: 0).")

    export_quotes = export_sub.add_parser(
        "quotes",
        help="Export quotes filtered by code or boolean query expression.",
        description=(
            "Export annotated quotes, filtered by a single code or a boolean query.\n\n"
            "Exactly one of --code or --query is required.\n\n"
            "Formats:\n"
            "  text   Plain text with quote boundaries marked\n"
            "  jsonl  One JSON object per line\n\n"
            "Examples:\n"
            "  bewley export quotes --code trust --format text\n"
            "  bewley export quotes --query 'trust & rapport' --format jsonl"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    export_quotes_selector = export_quotes.add_mutually_exclusive_group(required=True)
    export_quotes_selector.add_argument("--code", help="Single code to filter by (name or UUID).")
    export_quotes_selector.add_argument("--query", help="Boolean code expression to filter by.")
    export_quotes.add_argument("--format", choices=["jsonl", "text"], default="text", help="Output format for human mode: 'jsonl' or 'text'.")
    export_quotes.add_argument("--context-lines", type=int, default=0, help="Number of surrounding lines to include (default: 0).")

    export_html = export_sub.add_parser(
        "html",
        help="Export all codes and annotations as a standalone HTML file.",
        description=(
            "Generate a self-contained HTML page showing all codes and their\n"
            "annotated snippets.\n\n"
            "Example:\n"
            "  bewley export html --output analysis.html --title 'My Analysis'"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    export_html.add_argument("--output", default="bewley-codes.html", help="Output file path (default: bewley-codes.html).")
    export_html.add_argument("--title", help="Page title for the HTML output.")
    export_html.add_argument("--static", action="store_true", help="Generate a pure HTML/CSS page with no JavaScript. Useful for hosting on platforms that sandbox JS.")
    export_html.add_argument("--embed", action="store_true", help="Generate an embeddable HTML fragment (a scoped <div>) instead of a full page. Designed for inclusion in pandoc reports.")

    export_document_html = export_sub.add_parser(
        "document-html",
        help="Export a single document with inline annotation highlights as HTML.",
        description=(
            "Generate a self-contained HTML page showing a single document\n"
            "with annotation spans highlighted inline.\n\n"
            "Example:\n"
            "  bewley export document-html participant_01 --output doc.html"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    export_document_html.add_argument("document_ref", help="Document to export (UUID, path, or prefix).")
    export_document_html.add_argument("--output", default="bewley-document.html", help="Output file path (default: bewley-document.html).")
    export_document_html.add_argument("--title", help="Page title for the HTML output.")

    export_theory = export_sub.add_parser(
        "theory",
        help="Export code hierarchy, links, and core category as JSON or Mermaid diagram.",
        description=(
            "Export the theoretical structure: code hierarchy, inter-code links,\n"
            "and the designated core category.\n\n"
            "Formats:\n"
            "  mermaid  (default) A Mermaid diagram suitable for rendering\n"
            "  json     Structured JSON with codes, links, hierarchy, and core\n\n"
            "Examples:\n"
            "  bewley export theory\n"
            "  bewley export theory --format json --output theory.json"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    export_theory.add_argument("--format", choices=["json", "mermaid"], default="mermaid", help="Output format (default: mermaid).")
    export_theory.add_argument("--output", help="Write output to file instead of stdout.")

    export_narrative = export_sub.add_parser(
        "narrative",
        help="Export an integrative narrative summary of the project.",
        description=(
            "Generate a narrative summary integrating codes, memos, hierarchy,\n"
            "and the core category into a readable text.\n\n"
            "Examples:\n"
            "  bewley export narrative\n"
            "  bewley export narrative --output narrative.md"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    export_narrative.add_argument("--output", help="Write output to file instead of stdout.")

    # --- History & undo ---

    history = sub.add_parser(
        "history",
        help="Show the event history, optionally filtered by document, code, or annotation.",
        description=(
            "Display the append-only event log.\n\n"
            "Without filters, shows all events. Use filters to narrow:\n"
            "  --document DOC_REF   Events related to a specific document\n"
            "  --code CODE_REF      Events related to a specific code\n"
            "  --annotation ANN_ID  Events related to a specific annotation\n\n"
            "Output: tab-separated table with event_id, event_type, and timestamp.\n\n"
            "Examples:\n"
            "  bewley history\n"
            "  bewley history --document participant_01\n"
            "  bewley history --code trust"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    history.add_argument("--document", help="Filter events by document (UUID or path prefix).")
    history.add_argument("--code", help="Filter events by code (name, alias, or UUID).")
    history.add_argument("--annotation", help="Filter events by annotation UUID.")

    undo = sub.add_parser(
        "undo",
        help="Undo a previous event by emitting a compensating event.",
        description=(
            "Undo a previous operation by appending a compensating event.\n\n"
            "The original event is NOT deleted (the log is append-only).\n"
            "Instead, a new event is recorded that reverses the effect.\n\n"
            "Not all event types may be undoable.\n\n"
            "Output: prints the new compensating event_id.\n\n"
            "Example:\n"
            "  bewley undo evt_00000000003"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    undo.add_argument("event_id", help="Event ID to undo (from 'bewley history' output).")

    # --- Memos ---

    memo = sub.add_parser(
        "memo",
        help="Create, list, show, edit, and delete analytic memos.",
        description=(
            "Manage analytic memos — free-text notes attached to codes, documents,\n"
            "or the project as a whole.\n\n"
            "Memos are a core part of qualitative analysis. They capture the\n"
            "researcher's evolving interpretations, theoretical insights, and\n"
            "methodological decisions.\n\n"
            "Subcommands:\n"
            "  add      Create a new memo\n"
            "  list     List memos (optionally filtered by code or document)\n"
            "  show     Show the full content of a memo\n"
            "  edit     Edit a memo in your $EDITOR\n"
            "  delete   Delete a memo"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    memo_sub = memo.add_subparsers(dest="memo_cmd", required=True, metavar="SUBCOMMAND")

    memo_add = memo_sub.add_parser(
        "add",
        help="Create a new memo, optionally attached to a code or document.",
        description=(
            "Create a new analytic memo.\n\n"
            "Target (optional, defaults to project-level):\n"
            "  --code CODE_REF       Attach to a code\n"
            "  --document DOC_REF    Attach to a document\n\n"
            "Content can be provided as a positional argument or omitted to\n"
            "open $EDITOR for composition.\n\n"
            "Output: prints the new memo_id.\n\n"
            "Examples:\n"
            "  bewley memo add 'Initial thoughts on trust theme'\n"
            "  bewley memo add --code trust 'Trust appears in 8 of 12 interviews'\n"
            "  bewley memo add --code trust --title 'Saturation note' 'No new properties since interview 10'"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    memo_target = memo_add.add_mutually_exclusive_group()
    memo_target.add_argument("--code", help="Attach memo to this code (name, alias, or UUID).")
    memo_target.add_argument("--document", help="Attach memo to this document (UUID or path prefix).")
    memo_add.add_argument("--title", help="Optional title for the memo.")
    memo_add.add_argument("content", nargs="?", help="Memo content (omit to open $EDITOR).")

    memo_list = memo_sub.add_parser(
        "list",
        help="List memos, optionally filtered by code or document.",
        description=(
            "List analytic memos in the project.\n\n"
            "Without filters, lists all memos (project-level and attached).\n"
            "Use --code or --document to filter.\n\n"
            "Output: tab-separated table with memo_id, target, title, and timestamp."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    memo_list_target = memo_list.add_mutually_exclusive_group()
    memo_list_target.add_argument("--code", help="Filter memos attached to this code.")
    memo_list_target.add_argument("--document", help="Filter memos attached to this document.")

    memo_show = memo_sub.add_parser(
        "show",
        help="Show the full content of a memo.",
        description=(
            "Display the full content and metadata of a single memo.\n\n"
            "Example:\n"
            "  bewley memo show <memo_id>"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    memo_show.add_argument("memo_id", help="UUID of the memo to show.")

    memo_edit = memo_sub.add_parser(
        "edit",
        help="Edit a memo in your $EDITOR.",
        description=(
            "Open a memo's content in $EDITOR for editing.\n\n"
            "The updated content is saved as a new event (the old content\n"
            "is preserved in the event history)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    memo_edit.add_argument("memo_id", help="UUID of the memo to edit.")

    memo_delete = memo_sub.add_parser(
        "delete",
        help="Delete a memo.",
        description=(
            "Delete a memo by its memo_id.\n\n"
            "The deletion is recorded as an event (the memo content is\n"
            "preserved in event history and can be recovered with undo).\n\n"
            "Output: prints the event_id."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    memo_delete.add_argument("memo_id", help="UUID of the memo to delete.")

    return parser


def _human_status(result: dict) -> None:
    for key in ("documents", "revisions", "codes", "active_annotations", "conflicted_annotations"):
        print(f"{key}\t{result[key]}")


def _human_list_documents(result: list[dict]) -> None:
    _human_rows(result, ["document_id", "current_path", "revision_count"])


def _human_show_document(result: dict) -> None:
    print(f"document_id\t{result['document_id']}")
    print(f"path\t{result['path']}")
    if result.get("audio_source"):
        audio = result["audio_source"]
        print("audio_source")
        _human_rows(
            [audio],
            [
                "original_audio_filename",
                "original_audio_path",
                "media_type",
                "transcription_model",
                "transcription_response_format",
                "transcription_language",
                "transcript_style",
                "segment_count",
            ],
        )
    if result.get("video_source"):
        video = result["video_source"]
        print("video_source")
        _human_rows(
            [video],
            [
                "original_video_filename",
                "original_video_path",
                "media_type",
                "duration",
                "transcription_model",
                "transcription_response_format",
                "transcription_language",
                "transcript_style",
                "chunk_count",
            ],
        )
    print("revisions")
    _human_rows(result["revisions"], ["revision_id", "created_at", "byte_length", "line_count", "is_current"])
    print("annotations")
    _human_rows(result["annotations"], ["annotation_id", "canonical_name", "scope_type", "start_line", "end_line", "anchor_status", "is_active"])


def _human_show_audio(result: dict) -> None:
    _human_kv(
        result,
        [
            "document_id",
            "path",
            "original_audio_filename",
            "original_audio_path",
            "stored_audio_path",
            "stored_audio_sha256",
            "media_type",
            "transcription_model",
            "transcription_response_format",
            "transcription_language",
            "transcript_style",
            "segment_count",
        ],
    )
    print("segments")
    _human_rows(result["segments"], ["start", "end", "speaker", "text"])


def _human_show_video(result: dict) -> None:
    _human_kv(
        result,
        [
            "document_id",
            "path",
            "original_video_filename",
            "original_video_path",
            "stored_video_path",
            "stored_video_sha256",
            "media_type",
            "duration",
            "transcription_model",
            "transcription_response_format",
            "transcription_language",
            "transcript_style",
            "chunk_count",
        ],
    )
    print("chunks")
    _human_rows(
        result["chunks"],
        ["chunk_index", "extract_start", "extract_end", "logical_start", "logical_end", "byte_length"],
    )
    print("segments")
    _human_rows(result["segments"], ["start", "end", "speaker", "text"])


def _human_code_list(result: list[dict]) -> None:
    _human_rows(result, ["code_id", "canonical_name", "status"])


def _human_code_tree(nodes: list[dict], indent: int = 0) -> None:
    for node in nodes:
        print(f"{'  ' * indent}{node['canonical_name']}")
        if "children" in node:
            _human_code_tree(node["children"], indent + 1)


def _human_code_show(result: dict) -> None:
    print(f"code_id\t{result['code_id']}")
    print(f"name\t{result['name']}")
    print(f"status\t{result['status']}")
    print(f"active_annotations\t{result['active_annotations']}")
    print(f"aliases\t{', '.join(result['aliases'])}")
    if "parent" in result:
        print(f"parent\t{result['parent']}")
    if "children" in result:
        print(f"children\t{', '.join(result['children'])}")
    if "links" in result:
        print("links")
        for lk in result["links"]:
            print(f"  {lk['link_id'][:8]}\t{lk['source_name']} --{lk['relationship']}--> {lk['target_name']}")


def _human_code_links(result: list[dict]) -> None:
    if not result:
        print("no links")
        return
    for lk in result:
        memo_part = f"  ({lk['memo']})" if lk.get("memo") else ""
        print(f"{lk['link_id']}\t{lk['source_name']} --{lk['relationship']}--> {lk['target_name']}{memo_part}")


def _human_memo_list(result: list[dict]) -> None:
    if not result:
        print("no memos")
        return
    for row in result:
        print(f"{row['memo_id']}\t{row['target_type']}\t{row['title']}\t{row['created_at']}")


def _human_memo_show(result: dict) -> None:
    print(f"memo_id\t{result['memo_id']}")
    print(f"target_type\t{result['target_type']}")
    print(f"target_id\t{result['target_id']}")
    if result.get("title"):
        print(f"title\t{result['title']}")
    print(f"created_at\t{result['created_at']}")
    print(f"updated_at\t{result['updated_at']}")
    print("---")
    print(result["content"])


def _human_annotate_show(result: dict) -> None:
    for key, val in result.items():
        print(f"{key}\t{val}")


def _human_show_snippets(result: list[dict]) -> None:
    _human_rows(result, ["annotation_id", "code_name", "document_path", "start_line", "end_line", "anchor_status", "text"])


def _human_query_documents(result: list[dict]) -> None:
    _human_rows(result, ["document_id", "current_path"])


def _human_query_annotations(result: list[dict]) -> None:
    _human_rows(result, ["annotation_id", "canonical_name", "current_path", "start_line", "end_line", "anchor_status"])


def _human_history(result: list[dict]) -> None:
    _human_rows(result, ["sequence_number", "timestamp", "event_type", "event_id"])


def cmd_status(project: Project) -> dict:
    with project.connect() as conn:
        doc_count = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        rev_count = conn.execute("SELECT COUNT(*) FROM document_revisions").fetchone()[0]
        code_count = conn.execute("SELECT COUNT(*) FROM codes").fetchone()[0]
        ann_count = conn.execute("SELECT COUNT(*) FROM annotations WHERE is_active = 1").fetchone()[0]
        conflict_count = conn.execute(
            "SELECT COUNT(*) FROM annotations WHERE is_active = 1 AND anchor_status = 'conflicted'"
        ).fetchone()[0]
    return {
        "documents": doc_count,
        "revisions": rev_count,
        "codes": code_count,
        "active_annotations": ann_count,
        "conflicted_annotations": conflict_count,
    }


def cmd_list_documents(project: Project) -> list[dict]:
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
    return [{"document_id": row["document_id"], "current_path": row["current_path"], "revision_count": row["revision_count"]} for row in rows]


def cmd_show_document(project: Project, ref: str) -> dict:
    with project.connect() as conn:
        doc = project.resolve_document(conn, ref)
        audio_row = conn.execute(
            "SELECT * FROM document_audio_sources WHERE document_id = ?",
            (doc["document_id"],),
        ).fetchone()
        video_row = conn.execute(
            "SELECT * FROM document_video_sources WHERE document_id = ?",
            (doc["document_id"],),
        ).fetchone()
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
    return {
        "document_id": doc["document_id"],
        "path": doc["current_path"],
        "audio_source": (
            {
                "original_audio_filename": audio_row["original_audio_filename"],
                "original_audio_path": audio_row["original_audio_path"],
                "media_type": audio_row["media_type"],
                "transcription_model": audio_row["transcription_model"],
                "transcription_response_format": audio_row["transcription_response_format"],
                "transcription_language": audio_row["transcription_language"],
                "transcript_style": audio_row["transcript_style"],
                "segment_count": audio_row["segment_count"],
            }
            if audio_row
            else None
        ),
        "video_source": (
            {
                "original_video_filename": video_row["original_video_filename"],
                "original_video_path": video_row["original_video_path"],
                "media_type": video_row["media_type"],
                "duration": format_timestamp(video_row["duration_seconds"]),
                "transcription_model": video_row["transcription_model"],
                "transcription_response_format": video_row["transcription_response_format"],
                "transcription_language": video_row["transcription_language"],
                "transcript_style": video_row["transcript_style"],
                "chunk_count": video_row["chunk_count"],
            }
            if video_row
            else None
        ),
        "revisions": [
            {"revision_id": r["revision_id"], "created_at": r["created_at"], "byte_length": r["byte_length"], "line_count": r["line_count"], "is_current": r["is_current"]}
            for r in revisions
        ],
        "annotations": [
            {"annotation_id": a["annotation_id"], "canonical_name": a["canonical_name"], "scope_type": a["scope_type"], "start_line": a["start_line"], "end_line": a["end_line"], "anchor_status": a["anchor_status"], "is_active": a["is_active"]}
            for a in annotations
        ],
    }


def cmd_show_audio(project: Project, ref: str) -> dict:
    with project.connect() as conn:
        doc = project.resolve_document(conn, ref)
        row = conn.execute(
            "SELECT * FROM document_audio_sources WHERE document_id = ?",
            (doc["document_id"],),
        ).fetchone()
    if row is None:
        raise BewleyError(f"document has no linked audio source: {ref}")
    metadata = json.loads(row["metadata_json"])
    segments = metadata.get("segments") or []
    return {
        "document_id": doc["document_id"],
        "path": doc["current_path"],
        "original_audio_filename": row["original_audio_filename"],
        "original_audio_path": row["original_audio_path"],
        "stored_audio_path": str(project.audio_objects_dir / row["audio_sha256"]),
        "stored_audio_sha256": row["audio_sha256"],
        "media_type": row["media_type"],
        "transcription_model": row["transcription_model"],
        "transcription_response_format": row["transcription_response_format"],
        "transcription_language": row["transcription_language"],
        "transcript_style": row["transcript_style"],
        "segment_count": row["segment_count"],
        "segments": [
            {
                "start": format_timestamp(segment.get("start")),
                "end": format_timestamp(segment.get("end")),
                "speaker": segment.get("speaker") or "",
                "text": str(segment.get("text") or "").strip(),
            }
            for segment in segments
        ],
    }


def cmd_show_video(project: Project, ref: str) -> dict:
    with project.connect() as conn:
        doc = project.resolve_document(conn, ref)
        row = conn.execute(
            "SELECT * FROM document_video_sources WHERE document_id = ?",
            (doc["document_id"],),
        ).fetchone()
        chunk_rows = conn.execute(
            """
            SELECT chunk_index, extract_start_seconds, extract_end_seconds, logical_start_seconds,
                   logical_end_seconds, byte_length
            FROM document_video_chunks
            WHERE document_id = ?
            ORDER BY chunk_index
            """,
            (doc["document_id"],),
        ).fetchall()
    if row is None:
        raise BewleyError(f"document has no linked video source: {ref}")
    metadata = json.loads(row["metadata_json"])
    segments = metadata.get("segments") or []
    return {
        "document_id": doc["document_id"],
        "path": doc["current_path"],
        "original_video_filename": row["original_video_filename"],
        "original_video_path": row["original_video_path"],
        "stored_video_path": str(project.video_objects_dir / row["video_sha256"]),
        "stored_video_sha256": row["video_sha256"],
        "media_type": row["media_type"],
        "duration": format_timestamp(row["duration_seconds"]),
        "transcription_model": row["transcription_model"],
        "transcription_response_format": row["transcription_response_format"],
        "transcription_language": row["transcription_language"],
        "transcript_style": row["transcript_style"],
        "chunk_count": row["chunk_count"],
        "chunks": [
            {
                "chunk_index": chunk["chunk_index"],
                "extract_start": format_timestamp(chunk["extract_start_seconds"]),
                "extract_end": format_timestamp(chunk["extract_end_seconds"]),
                "logical_start": format_timestamp(chunk["logical_start_seconds"]),
                "logical_end": format_timestamp(chunk["logical_end_seconds"]),
                "byte_length": chunk["byte_length"],
            }
            for chunk in chunk_rows
        ],
        "segments": [
            {
                "start": format_timestamp(segment.get("start")),
                "end": format_timestamp(segment.get("end")),
                "speaker": segment.get("speaker") or "",
                "text": str(segment.get("text") or "").strip(),
            }
            for segment in segments
        ],
    }


def cmd_code_list(project: Project, *, tree: bool = False) -> list[dict]:
    with project.connect() as conn:
        rows = conn.execute("SELECT * FROM codes WHERE status = 'active' ORDER BY canonical_name").fetchall()
    if not tree:
        return [{"code_id": row["code_id"], "canonical_name": row["canonical_name"], "status": row["status"]} for row in rows]
    # Tree structure
    by_parent: dict[str | None, list[sqlite3.Row]] = {}
    for row in rows:
        by_parent.setdefault(row["parent_code_id"], []).append(row)

    def _build_tree(parent_id: str | None) -> list[dict]:
        result = []
        for child in by_parent.get(parent_id, []):
            node = {"canonical_name": child["canonical_name"], "code_id": child["code_id"]}
            children = _build_tree(child["code_id"])
            if children:
                node["children"] = children
            result.append(node)
        return result
    return _build_tree(None)


def cmd_code_show(project: Project, ref: str) -> dict:
    with project.connect() as conn:
        code = project.resolve_code(conn, ref)
        aliases = conn.execute("SELECT alias_name FROM code_aliases WHERE code_id = ? ORDER BY alias_name", (code["code_id"],)).fetchall()
        count = conn.execute("SELECT COUNT(*) FROM annotations WHERE code_id = ? AND is_active = 1", (code["code_id"],)).fetchone()[0]
        parent_name = None
        if code["parent_code_id"]:
            parent_row = conn.execute("SELECT canonical_name FROM codes WHERE code_id = ?", (code["parent_code_id"],)).fetchone()
            parent_name = parent_row["canonical_name"] if parent_row else code["parent_code_id"]
        children = conn.execute(
            "SELECT canonical_name FROM codes WHERE parent_code_id = ? AND status = 'active' ORDER BY canonical_name",
            (code["code_id"],),
        ).fetchall()
        links = conn.execute(
            "SELECT * FROM code_links WHERE (source_code_id = ? OR target_code_id = ?) AND is_active = 1",
            (code["code_id"], code["code_id"]),
        ).fetchall()
        link_items = []
        for lk in links:
            src = conn.execute("SELECT canonical_name FROM codes WHERE code_id = ?", (lk["source_code_id"],)).fetchone()
            tgt = conn.execute("SELECT canonical_name FROM codes WHERE code_id = ?", (lk["target_code_id"],)).fetchone()
            link_items.append({
                "link_id": lk["link_id"],
                "source_name": src["canonical_name"] if src else lk["source_code_id"][:8],
                "relationship": lk["relationship"],
                "target_name": tgt["canonical_name"] if tgt else lk["target_code_id"][:8],
            })
    result: dict[str, Any] = {
        "code_id": code["code_id"],
        "name": code["canonical_name"],
        "status": code["status"],
        "active_annotations": count,
        "aliases": [row["alias_name"] for row in aliases],
    }
    if parent_name:
        result["parent"] = parent_name
    if children:
        result["children"] = [row["canonical_name"] for row in children]
    if link_items:
        result["links"] = link_items
    return result


def cmd_code_links(project: Project, code_ref: str | None = None) -> list[dict]:
    with project.connect() as conn:
        links = project.list_code_links(conn, code_ref)
        result = []
        for lk in links:
            src = conn.execute("SELECT canonical_name FROM codes WHERE code_id = ?", (lk["source_code_id"],)).fetchone()
            tgt = conn.execute("SELECT canonical_name FROM codes WHERE code_id = ?", (lk["target_code_id"],)).fetchone()
            result.append({
                "link_id": lk["link_id"],
                "source_name": src["canonical_name"] if src else lk["source_code_id"][:8],
                "relationship": lk["relationship"],
                "target_name": tgt["canonical_name"] if tgt else lk["target_code_id"][:8],
                "memo": lk["memo"],
            })
    return result


def cmd_memo_list(project: Project, *, target_type: str | None = None, target_ref: str | None = None) -> list[dict]:
    target_id: str | None = None
    with project.connect() as conn:
        if target_type == "code" and target_ref:
            target_id = project.resolve_code(conn, target_ref)["code_id"]
        elif target_type == "document" and target_ref:
            target_id = project.resolve_document(conn, target_ref)["document_id"]
        rows = project.list_memos(conn, target_type=target_type, target_id=target_id)
    return [{"memo_id": row["memo_id"], "target_type": row["target_type"], "title": row["title"] or "", "created_at": row["created_at"]} for row in rows]


def cmd_memo_show(project: Project, memo_id: str) -> dict:
    with project.connect() as conn:
        memo = project.resolve_memo(conn, memo_id)
    content = project.read_memo_content(memo["content_sha256"])
    return {
        "memo_id": memo["memo_id"],
        "target_type": memo["target_type"],
        "target_id": memo["target_id"] or "(project)",
        "title": memo["title"],
        "created_at": memo["created_at"],
        "updated_at": memo["updated_at"],
        "content": content,
    }


def cmd_annotate_show(project: Project, annotation_id: str) -> dict:
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
    return {key: row[key] for key in row.keys()}


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
            content = (project.objects_dir / revision["content_sha256"]).read_bytes()
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
        # Collect document texts for in-context snippet rendering
        # Include span annotations (for context lines) and document-scope annotations (for full text display)
        doc_paths_needed = {row["current_path"] for row in annotations}
        doc_texts: dict[str, list[str]] = {}
        for dpath in doc_paths_needed:
            doc_row = conn.execute(
                "SELECT document_id FROM documents WHERE current_path = ?", (dpath,)
            ).fetchone()
            if doc_row:
                rev = project.current_revision(conn, doc_row["document_id"])
                content = (project.objects_dir / rev["content_sha256"]).read_bytes()
                doc_texts[dpath] = safe_decode(content).splitlines()
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
        "document_texts": doc_texts,
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
    content = (project.objects_dir / revision["content_sha256"]).read_bytes()
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


def cmd_show_snippets(project: Project, code_ref: str) -> list[dict]:
    rows = snippets_for_code(project, code_ref)
    return [
        {
            "annotation_id": row["annotation_id"],
            "code_name": row["canonical_name"],
            "document_path": row["current_path"],
            "start_line": row["start_line"],
            "end_line": row["end_line"],
            "anchor_status": row["anchor_status"],
            "text": row["exact_text"] if row["scope_type"] == "span" else "<document>",
        }
        for row in rows
    ]


def cmd_query(project: Project, expr: str, mode: str | None) -> list[dict]:
    cfg_mode = project.config().get("default_query_mode", DEFAULT_QUERY_MODE)
    selected_mode = mode or cfg_mode
    if selected_mode == "document":
        rows = project.query_documents(expr)
        return [{"document_id": row["document_id"], "current_path": row["current_path"]} for row in rows]
    rows = project.query_annotations(expr)
    return [
        {"annotation_id": row["annotation_id"], "canonical_name": row["canonical_name"], "current_path": row["current_path"], "start_line": row["start_line"], "end_line": row["end_line"], "anchor_status": row["anchor_status"]}
        for row in rows
    ]


def cmd_export_snippets(project: Project, code_ref: str, fmt: str, context_lines: int, human_output: bool) -> list[dict] | None:
    rows = snippets_for_code(project, code_ref)
    text_by_document = current_text_by_document(project, rows) if context_lines > 0 else {}
    if human_output:
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
        else:
            for row in rows:
                print(json.dumps(snippet_export_item(row, context_lines, text_by_document), ensure_ascii=False))
        return None  # Already printed in human mode
    return [snippet_export_item(row, context_lines, text_by_document) for row in rows]


def cmd_export_quotes(project: Project, code_ref: str | None, query_expr: str | None, fmt: str, context_lines: int, human_output: bool) -> list[dict] | None:
    rows = [row for row in export_rows_for_selector(project, code_ref=code_ref, query_expr=query_expr) if row["scope_type"] == "span"]
    text_by_document = current_text_by_document(project, rows) if context_lines > 0 else {}
    if human_output:
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
        else:
            for row in rows:
                print(json.dumps(quote_export_item(row, context_lines, text_by_document), ensure_ascii=False))
        return None
    return [quote_export_item(row, context_lines, text_by_document) for row in rows]


def cmd_export_html(project: Project, output_path: str, title: str | None, *, static: bool = False) -> dict:
    payload = code_explorer_payload(project)
    document_count = payload["document_count"]
    resolved_title = title or f"Qualitative coding explorer · {project.root.name} · {payload['code_count']} codes / {document_count} docs"
    target = Path(output_path)
    if not target.is_absolute():
        target = project.root / target
    if static:
        html_content = build_static_code_explorer_html(payload, resolved_title)
    else:
        html_content = build_code_explorer_html(payload, resolved_title)
    atomic_write_text(target, html_content)
    return {"output_path": str(target)}


def cmd_export_document_html(project: Project, document_ref: str, output_path: str, title: str | None) -> dict:
    payload = document_viewer_payload(project, document_ref)
    resolved_title = title or f"Bewley Document Viewer · {payload['document_path']}"
    target = Path(output_path)
    if not target.is_absolute():
        target = project.root / target
    atomic_write_text(target, build_document_viewer_html(payload, resolved_title))
    return {"output_path": str(target)}


def cmd_history(project: Project, document: str | None, code: str | None, annotation: str | None) -> list[dict]:
    rows = project.history(document_ref=document, code_ref=code, annotation_id=annotation)
    return [{"sequence_number": event["sequence_number"], "timestamp": event["timestamp"], "event_type": event["event_type"], "event_id": event["event_id"]} for event in rows]


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    human = args.human_output

    def _output(result: Any, human_fn=None) -> None:
        if human and human_fn:
            human_fn(result)
        elif human:
            print(result if isinstance(result, str) else json.dumps(result, ensure_ascii=False, indent=2, default=str))
        else:
            _json_output(result)

    def _output_id(result: dict) -> None:
        """Output a single-value dict like {"document_id": "..."} -- in human mode, print just the value."""
        if human:
            val = next(iter(result.values()))
            print(val)
        else:
            _json_output(result)

    try:
        if args.command == "init":
            project = Project(Path.cwd())
            project.init_project()
            _output({"status": "initialized"}, lambda r: print("initialized"))
            return 0

        project = Project.discover()

        if args.command == "status":
            _output(cmd_status(project), _human_status)
            return 0
        if args.command == "fsck":
            problems = project.fsck()
            if problems:
                if human:
                    for problem in problems:
                        print(problem, file=sys.stderr)
                else:
                    _json_output({"status": "error", "problems": problems})
                return 1
            _output({"status": "ok"}, lambda r: print("ok"))
            return 0
        if args.command == "rebuild-index":
            project.rebuild_index()
            project.append_event("index_rebuilt", {"timestamp": utcnow()})
            _output({"status": "rebuilt"}, lambda r: print("rebuilt"))
            return 0
        if args.command == "add":
            event = project.add_document(args.path)
            _output_id({"document_id": event["payload"]["document_id"]})
            return 0
        if args.command == "add-audio":
            result = project.add_audio_document(
                args.audio_path,
                args.output,
                model=args.model,
                language=args.language,
                prompt=args.prompt,
                response_format=args.response_format,
            )
            _output_id({"document_id": result["document_id"]})
            return 0
        if args.command == "add-video":
            result = project.add_video_document(
                args.video_path,
                args.output,
                model=args.model,
                language=args.language,
                prompt=args.prompt,
                response_format=args.response_format,
                audio_bitrate_kbps=args.audio_bitrate_kbps,
                chunk_overlap_seconds=args.chunk_overlap_seconds,
            )
            _output_id({"document_id": result["document_id"]})
            return 0
        if args.command == "update":
            event = project.update_document(args.path)
            if event is None:
                _output({"status": "no-op"}, lambda r: print("no-op"))
            else:
                _output_id({"revision_id": event["payload"]["revision_id"]})
            return 0
        if args.command == "list" and args.list_what == "documents":
            _output(cmd_list_documents(project), _human_list_documents)
            return 0
        if args.command == "show" and args.show_what == "document":
            _output(cmd_show_document(project, args.document_ref), _human_show_document)
            return 0
        if args.command == "show" and args.show_what == "audio":
            _output(cmd_show_audio(project, args.document_ref), _human_show_audio)
            return 0
        if args.command == "show" and args.show_what == "video":
            _output(cmd_show_video(project, args.document_ref), _human_show_video)
            return 0
        if args.command == "show" and args.show_what == "snippets":
            _output(cmd_show_snippets(project, args.code), _human_show_snippets)
            return 0
        if args.command == "code":
            if args.code_cmd == "create":
                event = project.add_code(args.name, args.description, args.color)
                _output_id({"code_id": event["payload"]["code_id"]})
                return 0
            if args.code_cmd == "list":
                result = cmd_code_list(project, tree=args.tree)
                if args.tree:
                    _output(result, _human_code_tree)
                else:
                    _output(result, _human_code_list)
                return 0
            if args.code_cmd == "show":
                _output(cmd_code_show(project, args.code_ref), _human_code_show)
                return 0
            if args.code_cmd == "rename":
                event = project.rename_code(args.old, args.new)
                _output_id({"event_id": event["event_id"]})
                return 0
            if args.code_cmd == "alias":
                event = project.alias_code(args.code_ref, args.alias)
                _output_id({"event_id": event["event_id"]})
                return 0
            if args.code_cmd == "merge":
                event = project.merge_codes(args.sources, args.into)
                _output_id({"event_id": event["event_id"]})
                return 0
            if args.code_cmd == "split":
                event = project.split_code(args.source, args.new, args.annotation, args.description, args.color)
                _output_id({"new_code_id": event["payload"]["new_code_id"]})
                return 0
            if args.code_cmd == "set-parent":
                event = project.set_code_parent(args.code_ref, args.parent_ref)
                _output_id({"event_id": event["event_id"]})
                return 0
            if args.code_cmd == "clear-parent":
                event = project.clear_code_parent(args.code_ref)
                _output_id({"event_id": event["event_id"]})
                return 0
            if args.code_cmd == "link":
                event = project.create_code_link(args.source, args.target, args.relationship, args.memo)
                _output_id({"link_id": event["payload"]["link_id"]})
                return 0
            if args.code_cmd == "links":
                _output(cmd_code_links(project, args.code_ref), _human_code_links)
                return 0
            if args.code_cmd == "unlink":
                event = project.remove_code_link(args.link_id)
                _output_id({"event_id": event["event_id"]})
                return 0
            if args.code_cmd == "set-core":
                event = project.set_core_category(args.code_ref)
                _output_id({"event_id": event["event_id"]})
                return 0
            if args.code_cmd == "show-core":
                with project.connect() as conn:
                    core = project.get_core_category(conn)
                if core:
                    result = {"code_id": core["code_id"], "canonical_name": core["canonical_name"]}
                    if human:
                        print(f"{core['code_id']}\t{core['canonical_name']}")
                    else:
                        _json_output(result)
                else:
                    if human:
                        print("no core category set")
                    else:
                        _json_output(None)
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
                    content = (project.objects_dir / rev["content_sha256"]).read_bytes().decode("utf-8")
                    byte_range = lines_to_byte_range(content, *parse_byte_range(args.lines))
                    event = project.add_annotation(args.code_ref, args.document_ref, "span", byte_range, args.memo)
                _output_id({"annotation_id": event["payload"]["annotation_id"]})
                return 0
            if args.annotate_cmd == "remove":
                event = project.remove_annotation(args.annotation_id)
                _output_id({"event_id": event["event_id"]})
                return 0
            if args.annotate_cmd == "show":
                _output(cmd_annotate_show(project, args.annotation_id), _human_annotate_show)
                return 0
            if args.annotate_cmd == "resolve":
                event = project.resolve_annotation(args.annotation_id, parse_byte_range(args.bytes), args.memo)
                _output_id({"event_id": event["event_id"]})
                return 0
        if args.command == "query":
            result = cmd_query(project, args.expr, args.mode)
            cfg_mode = project.config().get("default_query_mode", DEFAULT_QUERY_MODE)
            selected_mode = args.mode or cfg_mode
            if human:
                if selected_mode == "document":
                    _human_query_documents(result)
                else:
                    _human_query_annotations(result)
            else:
                _json_output(result)
            return 0
        if args.command == "export" and args.export_what == "snippets":
            result = cmd_export_snippets(project, args.code, args.format, args.context_lines, human)
            if result is not None:
                _json_output(result)
            return 0
        if args.command == "export" and args.export_what == "quotes":
            result = cmd_export_quotes(project, args.code, args.query, args.format, args.context_lines, human)
            if result is not None:
                _json_output(result)
            return 0
        if args.command == "export" and args.export_what == "html":
            result = cmd_export_html(project, args.output, args.title, static=args.static)
            _output(result, lambda r: print(r["output_path"]))
            return 0
        if args.command == "export" and args.export_what == "document-html":
            result = cmd_export_document_html(project, args.document_ref, args.output, args.title)
            _output(result, lambda r: print(r["output_path"]))
            return 0
        if args.command == "export" and args.export_what == "theory":
            if human:
                if args.format == "json":
                    text = json.dumps(project.export_theory_json(), indent=2, ensure_ascii=False)
                else:
                    text = project.export_theory_mermaid()
                if args.output:
                    Path(args.output).write_text(text, encoding="utf-8")
                    print(f"wrote {args.output}")
                else:
                    print(text)
            else:
                result = project.export_theory_json()
                if args.output:
                    Path(args.output).write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
                    _json_output({"output_path": args.output})
                else:
                    _json_output(result)
            return 0
        if args.command == "export" and args.export_what == "narrative":
            text = project.export_narrative()
            if human:
                if args.output:
                    Path(args.output).write_text(text, encoding="utf-8")
                    print(f"wrote {args.output}")
                else:
                    print(text)
            else:
                if args.output:
                    Path(args.output).write_text(text, encoding="utf-8")
                    _json_output({"output_path": args.output})
                else:
                    _json_output({"text": text})
            return 0
        if args.command == "memo":
            if args.memo_cmd == "add":
                if args.code:
                    target_type, target_ref = "code", args.code
                elif args.document:
                    target_type, target_ref = "document", args.document
                else:
                    target_type, target_ref = "project", None
                content = args.content
                if content is None:
                    content = project._open_editor()
                    if not content.strip():
                        if human:
                            print("aborted: empty memo")
                        else:
                            _json_output({"error": "aborted: empty memo"})
                        return 1
                event = project.create_memo(target_type, target_ref, content, args.title)
                _output_id({"memo_id": event["payload"]["memo_id"]})
                return 0
            if args.memo_cmd == "list":
                if args.code:
                    result = cmd_memo_list(project, target_type="code", target_ref=args.code)
                elif args.document:
                    result = cmd_memo_list(project, target_type="document", target_ref=args.document)
                else:
                    result = cmd_memo_list(project)
                _output(result, _human_memo_list)
                return 0
            if args.memo_cmd == "show":
                _output(cmd_memo_show(project, args.memo_id), _human_memo_show)
                return 0
            if args.memo_cmd == "edit":
                with project.connect() as conn:
                    memo = project.resolve_memo(conn, args.memo_id)
                old_content = project.read_memo_content(memo["content_sha256"])
                new_content = project._open_editor(old_content)
                if not new_content.strip():
                    if human:
                        print("aborted: empty memo")
                    else:
                        _json_output({"error": "aborted: empty memo"})
                    return 1
                event = project.update_memo(args.memo_id, new_content)
                _output_id({"event_id": event["event_id"]})
                return 0
            if args.memo_cmd == "delete":
                event = project.delete_memo(args.memo_id)
                _output_id({"event_id": event["event_id"]})
                return 0
        if args.command == "history":
            _output(cmd_history(project, args.document, args.code, args.annotation), _human_history)
            return 0
        if args.command == "undo":
            event = project.undo(args.event_id)
            _output_id({"event_id": event["event_id"]})
            return 0
        raise BewleyError("unimplemented command")
    except BewleyError as exc:
        if human:
            print(f"error: {exc}", file=sys.stderr)
        else:
            _json_output({"error": str(exc)})
        return 2
    except KeyboardInterrupt:
        if human:
            print("error: interrupted", file=sys.stderr)
        else:
            _json_output({"error": "interrupted"})
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
