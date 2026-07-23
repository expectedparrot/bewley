from __future__ import annotations

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
    def __init__(self, message: str, code: str = "ERROR", context: dict | None = None, hint: str = ""):
        super().__init__(message)
        self.code = code
        self.message = message
        self.context = context or {}
        self.hint = hint


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


def parse_byte_range(spec: str) -> tuple[int, int]:
    try:
        start_text, end_text = spec.split(":", 1)
        start = int(start_text)
        end = int(end_text)
    except ValueError as exc:
        raise BewleyError("expected range format START:END") from exc
    return start, end


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
            if "new_description" in payload:
                conn.execute(
                    "UPDATE codes SET description = ? WHERE code_id = ?",
                    (payload["new_description"], payload["code_id"]),
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
            if "new_description" in original:
                conn.execute("UPDATE codes SET description = ? WHERE code_id = ?", (original.get("old_description"), original["code_id"]))
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
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", str(media_path)],
            check=True, capture_output=True, text=True,
        )
        try:
            duration = float(completed.stdout.strip())
        except ValueError as exc:
            raise BewleyError(f"ffprobe returned an invalid duration for {media_path}") from exc
        if duration <= 0:
            raise BewleyError(f"ffprobe returned a non-positive duration for {media_path}")
        return duration

    def build_video_chunk_plan(self, video_path: Path, *, max_upload_bytes: int, overlap_seconds: float, audio_bitrate_kbps: int) -> list[dict[str, Any]]:
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
            chunks.append({
                "chunk_index": chunk_index,
                "extract_start_seconds": round(extract_start, 3),
                "extract_end_seconds": round(extract_end, 3),
                "logical_start_seconds": round(logical_start, 3),
                "logical_end_seconds": round(logical_end, 3),
            })
            if logical_end >= duration_seconds:
                break
            logical_start = logical_end
            chunk_index += 1
        return chunks

    def extract_audio_chunk(self, video_path: Path, chunk_path: Path, *, extract_start_seconds: float, extract_end_seconds: float, audio_bitrate_kbps: int) -> None:
        duration = max(0.1, extract_end_seconds - extract_start_seconds)
        subprocess.run(
            ["ffmpeg", "-y", "-ss", f"{extract_start_seconds:.3f}", "-i", str(video_path),
             "-t", f"{duration:.3f}", "-vn", "-ac", "1", "-b:a", f"{audio_bitrate_kbps}k", str(chunk_path)],
            check=True, capture_output=True, text=True,
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
                absolute_segments.append({
                    "start": max(abs_start, result["logical_start_seconds"]),
                    "end": min(abs_end, result["logical_end_seconds"]),
                    "speaker": segment["speaker"],
                    "text": segment["text"],
                })
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

    def transcribe_audio_with_openai(self, audio_path: Path, *, model: str, language: str | None, prompt: str | None, response_format: str) -> dict[str, Any]:
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
            "curl", "--silent", "--show-error", "--fail-with-body",
            "https://api.openai.com/v1/audio/transcriptions",
            "-H", f"Authorization: Bearer {api_key}",
            "-F", f"file=@{audio_path}",
            "-F", f"model={model}",
            "-F", f"response_format={response_format}",
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

    def add_audio_document(self, audio_path_arg: str, transcript_path_arg: str | None, *, model: str, language: str | None, prompt: str | None, response_format: str) -> dict[str, Any]:
        audio_path = (self.root / audio_path_arg).resolve() if not Path(audio_path_arg).is_absolute() else Path(audio_path_arg)
        if not audio_path.is_file():
            raise BewleyError(f"audio file not found: {audio_path_arg}")
        transcript_path, resolved_transcript_rel = self.resolve_output_path(transcript_path_arg, audio_path)
        transcription = self.transcribe_audio_with_openai(audio_path, model=model, language=language, prompt=prompt, response_format=response_format)
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

    def add_video_document(self, video_path_arg: str, transcript_path_arg: str | None, *, model: str, language: str | None, prompt: str | None, response_format: str, audio_bitrate_kbps: int, chunk_overlap_seconds: float) -> dict[str, Any]:
        video_path = (self.root / video_path_arg).resolve() if not Path(video_path_arg).is_absolute() else Path(video_path_arg)
        if not video_path.is_file():
            raise BewleyError(f"video file not found: {video_path_arg}")
        transcript_path, resolved_transcript_rel = self.resolve_output_path(transcript_path_arg, video_path)
        chunk_plan = self.build_video_chunk_plan(video_path, max_upload_bytes=OPENAI_MEDIA_TARGET_BYTES, overlap_seconds=chunk_overlap_seconds, audio_bitrate_kbps=audio_bitrate_kbps)
        chunk_results: list[dict[str, Any]] = []
        chunk_payloads: list[dict[str, Any]] = []
        with tempfile.TemporaryDirectory() as tempdir:
            temp_dir = Path(tempdir)
            for chunk in chunk_plan:
                chunk_path = temp_dir / f"{video_path.stem}.chunk-{chunk['chunk_index']:03d}.mp3"
                self.extract_audio_chunk(video_path, chunk_path, extract_start_seconds=chunk["extract_start_seconds"], extract_end_seconds=chunk["extract_end_seconds"], audio_bitrate_kbps=audio_bitrate_kbps)
                chunk_bytes = chunk_path.read_bytes()
                if len(chunk_bytes) > OPENAI_AUDIO_LIMIT_BYTES:
                    raise BewleyError(f"extracted chunk {chunk['chunk_index']} exceeds OpenAI's 25 MB transcription upload limit")
                chunk_audio_sha256 = self.store_audio_object(chunk_bytes)
                transcription = self.transcribe_audio_with_openai(chunk_path, model=model, language=language, prompt=prompt, response_format=response_format)
                chunk_results.append({**chunk, "transcription": transcription})
                chunk_payloads.append({
                    "chunk_index": chunk["chunk_index"],
                    "extract_start_seconds": chunk["extract_start_seconds"],
                    "extract_end_seconds": chunk["extract_end_seconds"],
                    "logical_start_seconds": chunk["logical_start_seconds"],
                    "logical_end_seconds": chunk["logical_end_seconds"],
                    "chunk_audio_sha256": chunk_audio_sha256,
                    "byte_length": len(chunk_bytes),
                    "media_type": "audio/mpeg",
                    "transcription_metadata": transcription,
                })
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
        row = conn.execute("SELECT content_sha256 FROM document_revisions WHERE revision_id = ?", (revision_id,)).fetchone()
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
                "SELECT * FROM annotations WHERE document_id = ? AND document_revision_id = ? AND is_active = 1",
                (document_id, old_revision_id),
            ).fetchall()
        for row in rows:
            if row["scope_type"] == "document":
                self.append_event("annotation_reanchored", {
                    "annotation_id": row["annotation_id"],
                    "document_revision_id": new_revision_id,
                    "start_byte": None, "end_byte": None, "start_line": None, "end_line": None,
                    "exact_text": None, "prefix_context": None, "suffix_context": None,
                    "anchor_status": "clean",
                })
                continue
            start = row["start_byte"]
            end = row["end_byte"]
            exact_text = row["exact_text"] or ""
            if start is None or end is None:
                continue
            if end <= len(new_bytes) and safe_decode(new_bytes[start:end]) == exact_text:
                start_line, end_line = byte_to_line_range(new_text, start, end)
                self.append_event("annotation_reanchored", self.make_anchor_payload(row["annotation_id"], new_revision_id, new_bytes, start, end, "clean", start_line, end_line))
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
                    self.append_event("annotation_reanchored", self.make_anchor_payload(row["annotation_id"], new_revision_id, new_bytes, start, end, "relocated", start_line, end_line))
                    continue
            self.append_event("annotation_conflicted", {
                "annotation_id": row["annotation_id"],
                "memo": f"automatic relocation failed when moving from {old_revision_id} to {new_revision_id}",
            })

    def make_anchor_payload(self, annotation_id: str, revision_id: str, content: bytes, start: int, end: int, status: str, start_line: int, end_line: int) -> dict[str, Any]:
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
        return self.append_event("code_created", {"code_id": uuid.uuid4().hex, "canonical_name": name, "description": description, "color": color})

    def rename_code(self, old_ref: str, new_name: str, new_description: str | None = None) -> dict[str, Any]:
        with self.connect() as conn:
            code = self.resolve_code(conn, old_ref)
            if new_name != code["canonical_name"] and self.code_name_taken(conn, new_name):
                raise BewleyError(f"code name already exists: {new_name}")
            payload: dict[str, Any] = {"code_id": code["code_id"], "old_name": code["canonical_name"], "new_name": new_name}
            if new_description is not None:
                payload["old_description"] = code["description"]
                payload["new_description"] = new_description
        return self.append_event("code_renamed", payload)

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
        return self.append_event("code_merged", {"source_code_ids": source_ids, "target_code_id": target["code_id"]})

    def split_code(self, source_ref: str, new_name: str, annotation_ids: list[str], description: str | None = None, color: str | None = None) -> dict[str, Any]:
        if not annotation_ids:
            raise BewleyError("split requires at least one --annotation id")
        with self.connect() as conn:
            source = self.resolve_code(conn, source_ref)
            if self.code_name_taken(conn, new_name):
                raise BewleyError(f"code name already exists: {new_name}")
            rows = conn.execute(
                "SELECT annotation_id FROM annotations WHERE code_id = ? AND is_active = 1 AND annotation_id IN ({})".format(",".join("?" for _ in annotation_ids)),
                (source["code_id"], *annotation_ids),
            ).fetchall()
        found_ids = {row["annotation_id"] for row in rows}
        missing = [aid for aid in annotation_ids if aid not in found_ids]
        if missing:
            raise BewleyError(f"annotations not active on source code: {', '.join(missing)}")
        return self.append_event("code_split", {
            "source_code_id": source["code_id"],
            "new_code_id": uuid.uuid4().hex,
            "new_canonical_name": new_name,
            "annotation_ids": annotation_ids,
            "description": description,
            "color": color,
        })

    def add_annotation(self, code_ref: str, document_ref: str, scope_type: str, byte_range: tuple[int, int] | None, memo: str | None) -> dict[str, Any]:
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
            payload.update({
                "start_byte": start, "end_byte": end,
                "start_line": start_line, "end_line": end_line,
                "exact_text": exact_text,
                "prefix_context": safe_decode(content[max(0, start - CONTEXT_BYTES):start]),
                "suffix_context": safe_decode(content[end:end + CONTEXT_BYTES]),
            })
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
                        "SELECT DISTINCT c.canonical_name FROM annotations a JOIN codes c ON c.code_id = a.code_id WHERE a.document_id = ? AND a.is_active = 1",
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
            for table in ["documents", "document_revisions", "document_audio_sources", "document_video_sources", "document_video_chunks", "codes", "code_aliases", "annotations", "events", "memos", "code_links", "project_settings"]:
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
            if code_id and code_id in {payload.get("code_id"), payload.get("source_code_id"), payload.get("target_code_id"), payload.get("new_code_id")}:
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
        if target["event_type"] not in {"code_renamed", "annotation_added", "memo_created", "memo_updated", "memo_deleted", "code_parent_set", "code_link_created", "code_link_removed", "core_category_set"}:
            raise BewleyError(f"undo not supported for event type: {target['event_type']}")
        return self.append_event("undo_recorded", {
            "undone_event_id": event_id,
            "undone_event_type": target["event_type"],
            "original_payload": target["payload"],
        })

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
        return self.append_event("memo_created", {
            "memo_id": uuid.uuid4().hex,
            "target_type": target_type,
            "target_id": target_id,
            "title": title,
            "content_sha256": content_sha256,
        })

    def update_memo(self, memo_id: str, content: str) -> dict[str, Any]:
        with self.connect() as conn:
            memo = self.resolve_memo(conn, memo_id)
        old_sha = memo["content_sha256"]
        new_sha = self.store_memo_object(content)
        if old_sha == new_sha:
            raise BewleyError("memo content unchanged")
        return self.append_event("memo_updated", {"memo_id": memo_id, "content_sha256": new_sha, "old_content_sha256": old_sha})

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
        return self.append_event("code_parent_set", {"code_id": code["code_id"], "parent_code_id": parent["code_id"], "old_parent_code_id": old_parent})

    def clear_code_parent(self, code_ref: str) -> dict[str, Any]:
        with self.connect() as conn:
            code = self.resolve_code(conn, code_ref)
            old_parent = code["parent_code_id"]
        if old_parent is None:
            raise BewleyError("code has no parent")
        return self.append_event("code_parent_set", {"code_id": code["code_id"], "parent_code_id": None, "old_parent_code_id": old_parent})

    # ── Code links ───────────────────────────────────────────────────────

    def create_code_link(self, source_ref: str, target_ref: str, relationship: str, memo: str | None = None) -> dict[str, Any]:
        with self.connect() as conn:
            source = self.resolve_code(conn, source_ref)
            target = self.resolve_code(conn, target_ref)
            existing = conn.execute(
                "SELECT link_id FROM code_links WHERE source_code_id = ? AND target_code_id = ? AND relationship = ? AND is_active = 1",
                (source["code_id"], target["code_id"], relationship),
            ).fetchone()
            if existing:
                raise BewleyError(f"duplicate link: {source['canonical_name']} --{relationship}--> {target['canonical_name']}")
        return self.append_event("code_link_created", {
            "link_id": uuid.uuid4().hex,
            "source_code_id": source["code_id"],
            "target_code_id": target["code_id"],
            "relationship": relationship,
            "memo": memo,
        })

    def remove_code_link(self, link_id: str) -> dict[str, Any]:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM code_links WHERE link_id = ? AND is_active = 1", (link_id,)).fetchone()
            if row is None:
                raise BewleyError(f"unknown or removed link: {link_id}")
        return self.append_event("code_link_removed", {
            "link_id": link_id,
            "source_code_id": row["source_code_id"],
            "target_code_id": row["target_code_id"],
            "relationship": row["relationship"],
        })

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
        return self.append_event("core_category_set", {"code_id": code["code_id"], "old_code_id": old_code_id})

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
                "codes": [], "hierarchy": [], "links": [], "memos": [],
            }
            for c in codes:
                ann_count = conn.execute("SELECT COUNT(*) FROM annotations WHERE code_id = ? AND is_active = 1", (c["code_id"],)).fetchone()[0]
                result["codes"].append({
                    "code_id": c["code_id"], "name": c["canonical_name"],
                    "description": c["description"], "parent_code_id": c["parent_code_id"],
                    "annotation_count": ann_count,
                })
                if c["parent_code_id"]:
                    result["hierarchy"].append({"parent": c["parent_code_id"], "child": c["code_id"]})
            for link in links:
                result["links"].append({
                    "link_id": link["link_id"], "source_code_id": link["source_code_id"],
                    "target_code_id": link["target_code_id"], "relationship": link["relationship"],
                    "memo": link["memo"],
                })
            for m in memos:
                result["memos"].append({
                    "memo_id": m["memo_id"], "target_type": m["target_type"],
                    "target_id": m["target_id"], "title": m["title"],
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
        lines.append("## Core Category")
        if data["core_category"]:
            cc = code_map.get(data["core_category"]["code_id"])
            if cc:
                desc = cc.get("description") or ""
                lines.append(f"**{cc['name']}**: {desc}".strip())
        else:
            lines.append("No core category set.")
        lines.append("")
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
        lines.append("## Categories")
        lines.append("")
        for c in data["codes"]:
            parent_note = ""
            if c["parent_code_id"] and c["parent_code_id"] in code_map:
                parent_note = f" (child of {code_map[c['parent_code_id']]['name']})"
            lines.append(f"### {c['name']}{parent_note} — {c['annotation_count']} annotations")
            if c.get("description"):
                lines.append(c["description"])
            code_memos = [m for m in data["memos"] if m["target_type"] == "code" and m["target_id"] == c["code_id"]]
            for m in code_memos:
                title = m.get("title") or "Memo"
                try:
                    content = self.read_memo_content(m["content_sha256"])
                    lines.append(f"- *{title}*: {content.strip()}")
                except BewleyError:
                    lines.append(f"- *{title}*: (content unavailable)")
            code_links = [lk for lk in data["links"] if lk["source_code_id"] == c["code_id"] or lk["target_code_id"] == c["code_id"]]
            for lk in code_links:
                if lk["source_code_id"] == c["code_id"]:
                    other = code_map.get(lk["target_code_id"], {}).get("name", lk["target_code_id"][:8])
                    lines.append(f"- --{lk['relationship']}--> {other}")
                else:
                    other = code_map.get(lk["source_code_id"], {}).get("name", lk["source_code_id"][:8])
                    lines.append(f"- <--{lk['relationship']}-- {other}")
            lines.append("")
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


# ── Module-level command functions ───────────────────────────────────────────

def cmd_status(project: Project) -> dict:
    with project.connect() as conn:
        doc_count = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        rev_count = conn.execute("SELECT COUNT(*) FROM document_revisions").fetchone()[0]
        code_count = conn.execute("SELECT COUNT(*) FROM codes").fetchone()[0]
        ann_count = conn.execute("SELECT COUNT(*) FROM annotations WHERE is_active = 1").fetchone()[0]
        conflict_count = conn.execute("SELECT COUNT(*) FROM annotations WHERE is_active = 1 AND anchor_status = 'conflicted'").fetchone()[0]
    return {"documents": doc_count, "revisions": rev_count, "codes": code_count, "active_annotations": ann_count, "conflicted_annotations": conflict_count}


def cmd_list_documents(project: Project) -> list[dict]:
    with project.connect() as conn:
        rows = conn.execute(
            "SELECT d.document_id, d.current_path, COUNT(r.revision_id) AS revision_count FROM documents d LEFT JOIN document_revisions r ON r.document_id = d.document_id GROUP BY d.document_id, d.current_path ORDER BY d.current_path"
        ).fetchall()
    return [{"document_id": row["document_id"], "current_path": row["current_path"], "revision_count": row["revision_count"]} for row in rows]


def cmd_show_document(project: Project, ref: str) -> dict:
    with project.connect() as conn:
        doc = project.resolve_document(conn, ref)
        audio_row = conn.execute("SELECT * FROM document_audio_sources WHERE document_id = ?", (doc["document_id"],)).fetchone()
        video_row = conn.execute("SELECT * FROM document_video_sources WHERE document_id = ?", (doc["document_id"],)).fetchone()
        revisions = conn.execute("SELECT revision_id, created_at, byte_length, line_count, is_current FROM document_revisions WHERE document_id = ? ORDER BY created_at", (doc["document_id"],)).fetchall()
        annotations = conn.execute(
            "SELECT a.annotation_id, c.canonical_name, a.scope_type, a.start_line, a.end_line, a.anchor_status, a.is_active FROM annotations a JOIN codes c ON c.code_id = a.code_id WHERE a.document_id = ? ORDER BY a.created_at",
            (doc["document_id"],),
        ).fetchall()
    return {
        "document_id": doc["document_id"],
        "path": doc["current_path"],
        "audio_source": ({
            "original_audio_filename": audio_row["original_audio_filename"],
            "original_audio_path": audio_row["original_audio_path"],
            "media_type": audio_row["media_type"],
            "transcription_model": audio_row["transcription_model"],
            "transcription_response_format": audio_row["transcription_response_format"],
            "transcription_language": audio_row["transcription_language"],
            "transcript_style": audio_row["transcript_style"],
            "segment_count": audio_row["segment_count"],
        } if audio_row else None),
        "video_source": ({
            "original_video_filename": video_row["original_video_filename"],
            "original_video_path": video_row["original_video_path"],
            "media_type": video_row["media_type"],
            "duration": format_timestamp(video_row["duration_seconds"]),
            "transcription_model": video_row["transcription_model"],
            "transcription_response_format": video_row["transcription_response_format"],
            "transcription_language": video_row["transcription_language"],
            "transcript_style": video_row["transcript_style"],
            "chunk_count": video_row["chunk_count"],
        } if video_row else None),
        "revisions": [{"revision_id": r["revision_id"], "created_at": r["created_at"], "byte_length": r["byte_length"], "line_count": r["line_count"], "is_current": r["is_current"]} for r in revisions],
        "annotations": [{"annotation_id": a["annotation_id"], "canonical_name": a["canonical_name"], "scope_type": a["scope_type"], "start_line": a["start_line"], "end_line": a["end_line"], "anchor_status": a["anchor_status"], "is_active": a["is_active"]} for a in annotations],
    }


def cmd_show_audio(project: Project, ref: str) -> dict:
    with project.connect() as conn:
        doc = project.resolve_document(conn, ref)
        row = conn.execute("SELECT * FROM document_audio_sources WHERE document_id = ?", (doc["document_id"],)).fetchone()
    if row is None:
        raise BewleyError(f"document has no linked audio source: {ref}")
    metadata = json.loads(row["metadata_json"])
    segments = metadata.get("segments") or []
    return {
        "document_id": doc["document_id"], "path": doc["current_path"],
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
        "segments": [{"start": format_timestamp(s.get("start")), "end": format_timestamp(s.get("end")), "speaker": s.get("speaker") or "", "text": str(s.get("text") or "").strip()} for s in segments],
    }


def cmd_show_video(project: Project, ref: str) -> dict:
    with project.connect() as conn:
        doc = project.resolve_document(conn, ref)
        row = conn.execute("SELECT * FROM document_video_sources WHERE document_id = ?", (doc["document_id"],)).fetchone()
        chunk_rows = conn.execute("SELECT chunk_index, extract_start_seconds, extract_end_seconds, logical_start_seconds, logical_end_seconds, byte_length FROM document_video_chunks WHERE document_id = ? ORDER BY chunk_index", (doc["document_id"],)).fetchall()
    if row is None:
        raise BewleyError(f"document has no linked video source: {ref}")
    metadata = json.loads(row["metadata_json"])
    segments = metadata.get("segments") or []
    return {
        "document_id": doc["document_id"], "path": doc["current_path"],
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
        "chunks": [{"chunk_index": c["chunk_index"], "extract_start": format_timestamp(c["extract_start_seconds"]), "extract_end": format_timestamp(c["extract_end_seconds"]), "logical_start": format_timestamp(c["logical_start_seconds"]), "logical_end": format_timestamp(c["logical_end_seconds"]), "byte_length": c["byte_length"]} for c in chunk_rows],
        "segments": [{"start": format_timestamp(s.get("start")), "end": format_timestamp(s.get("end")), "speaker": s.get("speaker") or "", "text": str(s.get("text") or "").strip()} for s in segments],
    }


def cmd_code_list(project: Project, *, tree: bool = False) -> list[dict]:
    with project.connect() as conn:
        rows = conn.execute("SELECT * FROM codes WHERE status = 'active' ORDER BY canonical_name").fetchall()
    if not tree:
        return [{"code_id": row["code_id"], "canonical_name": row["canonical_name"], "status": row["status"]} for row in rows]
    by_parent: dict[str | None, list] = {}
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
        children = conn.execute("SELECT canonical_name FROM codes WHERE parent_code_id = ? AND status = 'active' ORDER BY canonical_name", (code["code_id"],)).fetchall()
        links = conn.execute("SELECT * FROM code_links WHERE (source_code_id = ? OR target_code_id = ?) AND is_active = 1", (code["code_id"], code["code_id"])).fetchall()
        link_items = []
        for lk in links:
            src = conn.execute("SELECT canonical_name FROM codes WHERE code_id = ?", (lk["source_code_id"],)).fetchone()
            tgt = conn.execute("SELECT canonical_name FROM codes WHERE code_id = ?", (lk["target_code_id"],)).fetchone()
            link_items.append({"link_id": lk["link_id"], "source_name": src["canonical_name"] if src else lk["source_code_id"][:8], "relationship": lk["relationship"], "target_name": tgt["canonical_name"] if tgt else lk["target_code_id"][:8]})
    result: dict[str, Any] = {"code_id": code["code_id"], "name": code["canonical_name"], "status": code["status"], "active_annotations": count, "aliases": [row["alias_name"] for row in aliases]}
    if parent_name:
        result["parent"] = parent_name
    if children:
        result["children"] = [row["canonical_name"] for row in children]
    if link_items:
        result["links"] = link_items
    return result


def cmd_code_coverage(project: Project, code_ref: str, breakdown: bool = False) -> dict:
    with project.connect() as conn:
        code = project.resolve_code(conn, code_ref)
        total_docs = conn.execute("SELECT COUNT(*) FROM documents WHERE archived_at IS NULL").fetchone()[0]
        def get_descendants(cid: str) -> list[str]:
            children = conn.execute("SELECT code_id FROM codes WHERE parent_code_id = ? AND status = 'active'", (cid,)).fetchall()
            result = [cid]
            for child in children:
                result.extend(get_descendants(child["code_id"]))
            return result
        code_ids = get_descendants(code["code_id"])
        placeholders = ",".join("?" * len(code_ids))
        direct_docs = conn.execute("SELECT COUNT(DISTINCT document_id) FROM annotations WHERE code_id = ? AND is_active = 1", (code["code_id"],)).fetchone()[0]
        inclusive_docs = conn.execute(f"SELECT COUNT(DISTINCT document_id) FROM annotations WHERE code_id IN ({placeholders}) AND is_active = 1", code_ids).fetchone()[0]
        desc_names = []
        per_descendant: list[dict] = []
        for cid in code_ids:
            row = conn.execute("SELECT canonical_name FROM codes WHERE code_id = ?", (cid,)).fetchone()
            if not row:
                continue
            if cid != code["code_id"]:
                desc_names.append(row["canonical_name"])
            if breakdown:
                doc_count = conn.execute("SELECT COUNT(DISTINCT document_id) FROM annotations WHERE code_id = ? AND is_active = 1", (cid,)).fetchone()[0]
                per_descendant.append({
                    "code": row["canonical_name"],
                    "is_target": cid == code["code_id"],
                    "respondents": doc_count,
                })
    result: dict = {"code": code["canonical_name"], "total_respondents": total_docs, "direct": direct_docs, "inclusive": inclusive_docs, "descendants": desc_names}
    if breakdown:
        per_descendant.sort(key=lambda x: (-x["respondents"], x["code"]))
        result["breakdown"] = per_descendant
    return result


def cmd_code_links(project: Project, code_ref: str | None = None) -> list[dict]:
    with project.connect() as conn:
        links = project.list_code_links(conn, code_ref)
        result = []
        for lk in links:
            src = conn.execute("SELECT canonical_name FROM codes WHERE code_id = ?", (lk["source_code_id"],)).fetchone()
            tgt = conn.execute("SELECT canonical_name FROM codes WHERE code_id = ?", (lk["target_code_id"],)).fetchone()
            result.append({"link_id": lk["link_id"], "source_name": src["canonical_name"] if src else lk["source_code_id"][:8], "relationship": lk["relationship"], "target_name": tgt["canonical_name"] if tgt else lk["target_code_id"][:8], "memo": lk["memo"]})
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
    return {"memo_id": memo["memo_id"], "target_type": memo["target_type"], "target_id": memo["target_id"] or "(project)", "title": memo["title"], "created_at": memo["created_at"], "updated_at": memo["updated_at"], "content": content}


def cmd_annotate_show(project: Project, annotation_id: str) -> dict:
    with project.connect() as conn:
        row = conn.execute(
            "SELECT a.*, c.canonical_name, d.current_path FROM annotations a JOIN codes c ON c.code_id = a.code_id JOIN documents d ON d.document_id = a.document_id WHERE a.annotation_id = ?",
            (annotation_id,),
        ).fetchone()
        if row is None:
            raise BewleyError(f"unknown annotation id: {annotation_id}")
    return {key: row[key] for key in row.keys()}


def snippets_for_code(project: Project, code_ref: str) -> list[sqlite3.Row]:
    with project.connect() as conn:
        code = project.resolve_code(conn, code_ref)
        return conn.execute(
            "SELECT a.*, c.canonical_name, d.current_path FROM annotations a JOIN codes c ON c.code_id = a.code_id JOIN documents d ON d.document_id = a.document_id WHERE a.code_id = ? AND a.is_active = 1 ORDER BY d.current_path, COALESCE(a.start_line, 0), a.annotation_id",
            (code["code_id"],),
        ).fetchall()


def export_rows_for_selector(project: Project, code_ref: str | None = None, query_expr: str | None = None, all_quotes: bool = False) -> list[sqlite3.Row]:
    provided = sum(1 for v in (code_ref, query_expr, all_quotes) if v)
    if provided != 1:
        raise BewleyError("provide exactly one of --code, --query, or --all")
    if all_quotes:
        with project.connect() as conn:
            return conn.execute(
                "SELECT a.*, c.canonical_name, d.current_path FROM annotations a JOIN codes c ON c.code_id = a.code_id JOIN documents d ON d.document_id = a.document_id WHERE a.is_active = 1 ORDER BY c.canonical_name, d.current_path, COALESCE(a.start_line, 0), a.annotation_id"
            ).fetchall()
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
        "code_name": row["canonical_name"], "code_id": row["code_id"],
        "document_id": row["document_id"], "document_path": row["current_path"],
        "revision_id": row["document_revision_id"], "annotation_id": row["annotation_id"],
        "start_line": row["start_line"], "end_line": row["end_line"],
        "selected_text": row["exact_text"] if row["scope_type"] == "span" else None,
        "anchor_status": row["anchor_status"],
    }
    if context_lines > 0 and row["scope_type"] == "span" and row["start_line"] is not None and row["end_line"] is not None:
        before, after = line_window(text_by_document[row["document_id"]], row["start_line"], row["end_line"], context_lines)
        item["context_before"] = before
        item["context_after"] = after
        item["context_lines"] = context_lines
    return item


def quote_export_item(row: sqlite3.Row, context_lines: int, text_by_document: dict[str, str]) -> dict[str, Any]:
    item = {
        "code_name": row["canonical_name"], "code_id": row["code_id"],
        "document_id": row["document_id"], "document_path": row["current_path"],
        "revision_id": row["document_revision_id"], "annotation_id": row["annotation_id"],
        "start_byte": row["start_byte"], "end_byte": row["end_byte"],
        "start_line": row["start_line"], "end_line": row["end_line"],
        "exact_text": row["exact_text"], "anchor_status": row["anchor_status"],
    }
    if context_lines > 0 and row["start_line"] is not None and row["end_line"] is not None:
        before, after = line_window(text_by_document[row["document_id"]], row["start_line"], row["end_line"], context_lines)
        item["context_before"] = before
        item["context_after"] = after
        item["context_lines"] = context_lines
    return item


def cmd_show_snippets(project: Project, code_ref: str) -> list[dict]:
    rows = snippets_for_code(project, code_ref)
    return [{"annotation_id": row["annotation_id"], "code_name": row["canonical_name"], "document_path": row["current_path"], "start_line": row["start_line"], "end_line": row["end_line"], "anchor_status": row["anchor_status"], "text": row["exact_text"] if row["scope_type"] == "span" else "<document>"} for row in rows]


def cmd_query(project: Project, expr: str, mode: str | None) -> list[dict]:
    cfg_mode = project.config().get("default_query_mode", DEFAULT_QUERY_MODE)
    selected_mode = mode or cfg_mode
    if selected_mode == "document":
        rows = project.query_documents(expr)
        return [{"document_id": row["document_id"], "current_path": row["current_path"]} for row in rows]
    rows = project.query_annotations(expr)
    return [{"annotation_id": row["annotation_id"], "canonical_name": row["canonical_name"], "current_path": row["current_path"], "start_line": row["start_line"], "end_line": row["end_line"], "anchor_status": row["anchor_status"]} for row in rows]


def cmd_history(project: Project, document: str | None, code: str | None, annotation: str | None) -> list[dict]:
    rows = project.history(document_ref=document, code_ref=code, annotation_id=annotation)
    return [{"sequence_number": event["sequence_number"], "timestamp": event["timestamp"], "event_type": event["event_type"], "event_id": event["event_id"]} for event in rows]


def code_explorer_payload(project: Project) -> dict[str, Any]:
    with project.connect() as conn:
        codes = conn.execute(
            "SELECT c.*, COUNT(DISTINCT CASE WHEN a.is_active = 1 THEN a.annotation_id END) AS annotation_count, COUNT(DISTINCT CASE WHEN a.is_active = 1 THEN a.document_id END) AS document_count FROM codes c LEFT JOIN annotations a ON a.code_id = c.code_id GROUP BY c.code_id ORDER BY annotation_count DESC, c.canonical_name"
        ).fetchall()
        alias_rows = conn.execute("SELECT code_id, alias_name FROM code_aliases ORDER BY alias_name").fetchall()
        annotations = conn.execute(
            "SELECT a.*, c.canonical_name, c.color, d.current_path FROM annotations a JOIN codes c ON c.code_id = a.code_id JOIN documents d ON d.document_id = a.document_id WHERE a.is_active = 1 ORDER BY c.canonical_name, d.current_path, COALESCE(a.start_line, 0), a.annotation_id"
        ).fetchall()
        doc_paths_needed = {row["current_path"] for row in annotations}
        doc_texts: dict[str, list[str]] = {}
        for dpath in doc_paths_needed:
            doc_row = conn.execute("SELECT document_id FROM documents WHERE current_path = ?", (dpath,)).fetchone()
            if doc_row:
                rev = project.current_revision(conn, doc_row["document_id"])
                content = (project.objects_dir / rev["content_sha256"]).read_bytes()
                doc_texts[dpath] = safe_decode(content).splitlines()
    aliases_by_code: dict[str, list[str]] = {}
    for row in alias_rows:
        aliases_by_code.setdefault(row["code_id"], []).append(row["alias_name"])
    code_items = [{"code_id": row["code_id"], "name": row["canonical_name"], "description": row["description"], "status": row["status"], "annotation_count": row["annotation_count"], "document_count": row["document_count"], "display_color": coerce_code_color(row["color"], row["canonical_name"]), "aliases": aliases_by_code.get(row["code_id"], [])} for row in codes]
    snippet_items = [{"annotation_id": row["annotation_id"], "code_id": row["code_id"], "code_name": row["canonical_name"], "code_color": coerce_code_color(row["color"], row["canonical_name"]), "document_path": row["current_path"], "scope_type": row["scope_type"], "start_line": row["start_line"], "end_line": row["end_line"], "anchor_status": row["anchor_status"], "memo": row["memo"], "exact_text": row["exact_text"]} for row in annotations]
    return {"generated_at": utcnow(), "project_root": str(project.root), "code_count": len(code_items), "snippet_count": len(snippet_items), "document_count": len({item["document_path"] for item in snippet_items}), "codes": code_items, "snippets": snippet_items, "document_texts": doc_texts}


def document_viewer_payload(project: Project, document_ref: str) -> dict[str, Any]:
    with project.connect() as conn:
        doc = project.resolve_document(conn, document_ref)
        revision = project.current_revision(conn, doc["document_id"])
        annotations = conn.execute("SELECT a.*, c.canonical_name, c.color FROM annotations a JOIN codes c ON c.code_id = a.code_id WHERE a.document_id = ? AND a.is_active = 1 ORDER BY COALESCE(a.start_byte, -1), a.annotation_id", (doc["document_id"],)).fetchall()
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
        code_entry = code_counts.setdefault(row["code_id"], {"code_id": row["code_id"], "name": row["canonical_name"], "display_color": code_color, "annotation_count": 0, "document_annotation_count": 0})
        item = {"annotation_id": row["annotation_id"], "code_id": row["code_id"], "code_name": row["canonical_name"], "code_slug": code_slug, "code_color": code_color, "highlight_color": highlight_color, "scope_type": row["scope_type"], "start_byte": row["start_byte"], "end_byte": row["end_byte"], "start_line": row["start_line"], "end_line": row["end_line"], "anchor_status": row["anchor_status"], "memo": row["memo"], "exact_text": row["exact_text"]}
        annotation_index[row["annotation_id"]] = item
        if row["scope_type"] == "document":
            document_annotations.append(item)
            code_entry["document_annotation_count"] += 1
            continue
        span_annotations.append(item)
        code_entry["annotation_count"] += 1
    codes = sorted(code_counts.values(), key=lambda item: (-item["annotation_count"], -item["document_annotation_count"], item["name"]))
    return {"generated_at": utcnow(), "project_root": str(project.root), "document_id": doc["document_id"], "document_path": doc["current_path"], "revision_id": revision["revision_id"], "byte_length": revision["byte_length"], "line_count": revision["line_count"], "code_count": len(codes), "annotation_count": len(span_annotations), "document_annotations": document_annotations, "span_annotations": span_annotations, "codes": codes, "annotation_index": annotation_index, "rendered_text": render_annotated_document_html(text, span_annotations)}


def cmd_export_html(project: Project, output_path: str, title: str | None, *, static: bool = False, embed: bool = False) -> dict:
    payload = code_explorer_payload(project)
    document_count = payload["document_count"]
    resolved_title = title or f"Qualitative coding explorer · {project.root.name} · {payload['code_count']} codes / {document_count} docs"
    target = Path(output_path)
    if not target.is_absolute():
        target = project.root / target
    if embed:
        html_content = build_embeddable_code_explorer_html(payload, resolved_title)
    elif static:
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


def build_embeddable_code_explorer_html(payload: dict[str, Any], title: str) -> str:
    """Build an embeddable HTML fragment for a code explorer.

    Returns a ``<div class="bewley-embed">`` containing scoped CSS (via
    ``.bewley-embed`` prefix on every rule) plus the code sections and TOC.
    No ``<html>``, ``<head>``, or ``<body>`` wrapper — designed to be dropped
    into a pandoc ``--embed-resources`` HTML report or any container page.
    """
    safe_title = html.escape(title)
    codes = sorted(payload["codes"], key=lambda c: -c["annotation_count"])
    codes = [c for c in codes if c["annotation_count"] > 0]
    doc_texts: dict[str, list[str]] = payload.get("document_texts", {})

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
        lines.append('<div class="bw-snippet">')
        lines.append(f'<pre class="bw-snippet-text">{_escape(text)}</pre>')
        if memo:
            lines.append(f'<div class="bw-snippet-memo">{_escape(memo)}</div>')
        lines.append(f'<div class="bw-snippet-source">{_escape(doc_path)}</div>')
        lines.append('</div>')
        return "\n".join(lines)

    code_sections = []
    for c in codes:
        slug = c["name"].replace("/", "-")
        color = c.get("display_color", "#428a5f")
        desc = c.get("description", "")
        code_snippets = snippets_by_code.get(c["name"], [])

        section_lines = []
        section_lines.append(f'<details class="bw-code-section" id="bw-{_escape(slug)}">')
        section_lines.append(f'<summary class="bw-code-header">')
        section_lines.append(f'<span class="bw-color-dot" style="background:{color}"></span>')
        section_lines.append(f'<strong class="bw-code-name">{_escape(c["name"])}</strong>')
        section_lines.append(f'<span class="bw-badge">{c["annotation_count"]}</span>')
        if desc:
            section_lines.append(f' <span class="bw-code-desc">{_escape(desc)}</span>')
        section_lines.append('</summary>')
        for s in code_snippets:
            section_lines.append(_snippet_html(s))
        section_lines.append('</details>')
        code_sections.append("\n".join(section_lines))

    toc_items = []
    for c in codes:
        slug = c["name"].replace("/", "-")
        toc_items.append(
            f'<span class="bw-toc-item">'
            f'<a href="#bw-{_escape(slug)}">{_escape(c["name"])}</a>'
            f'<span class="bw-toc-count">({c["annotation_count"]})</span>'
            f'</span>'
        )

    return f"""<div class="bewley-embed">
<style>
  .bewley-embed {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
    line-height: 1.5;
    color: #1a1a1a;
  }}
  .bewley-embed .bw-header {{
    border-bottom: 2px solid #428a5f;
    padding-bottom: 0.4rem;
    margin-bottom: 0.8rem;
  }}
  .bewley-embed .bw-header h3 {{
    margin: 0;
    font-size: 1.15rem;
    font-weight: 600;
    color: #1a1a1a;
  }}
  .bewley-embed .bw-stats {{
    color: #666;
    font-size: 0.85rem;
    margin-bottom: 1rem;
  }}
  .bewley-embed .bw-toc {{
    display: flex;
    flex-wrap: wrap;
    gap: 0.2rem 1.2rem;
    margin-bottom: 1.2rem;
    padding: 0.8rem;
    background: #f5f5f5;
    border-radius: 5px;
    font-size: 0.82rem;
  }}
  .bewley-embed .bw-toc-item a {{
    text-decoration: none;
    color: #1a1a1a;
    font-family: 'SF Mono', Consolas, 'Liberation Mono', Menlo, monospace;
  }}
  .bewley-embed .bw-toc-item a:hover {{ color: #428a5f; }}
  .bewley-embed .bw-toc-count {{ color: #999; font-size: 0.78rem; }}
  .bewley-embed .bw-code-section {{
    margin-bottom: 0.6rem;
    border: 1px solid #e0e0e0;
    border-radius: 5px;
  }}
  .bewley-embed .bw-code-header {{
    cursor: pointer;
    padding: 0.5rem 0.8rem;
    background: #fafafa;
    border-radius: 5px;
    font-size: 0.9rem;
    list-style: none;
  }}
  .bewley-embed .bw-code-header::-webkit-details-marker {{ display: none; }}
  .bewley-embed .bw-code-header::before {{
    content: '\\25B6';
    display: inline-block;
    margin-right: 0.4rem;
    font-size: 0.6rem;
    transition: transform 0.15s;
    color: #999;
  }}
  .bewley-embed details[open] > .bw-code-header::before {{
    transform: rotate(90deg);
  }}
  .bewley-embed .bw-color-dot {{
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    margin-right: 0.4rem;
    vertical-align: middle;
  }}
  .bewley-embed .bw-code-name {{
    font-family: 'SF Mono', Consolas, 'Liberation Mono', Menlo, monospace;
    font-size: 0.88rem;
  }}
  .bewley-embed .bw-badge {{
    display: inline-block;
    background: #428a5f;
    color: #fff;
    font-size: 0.68rem;
    padding: 0.1rem 0.4rem;
    border-radius: 8px;
    margin-left: 0.3rem;
    vertical-align: middle;
  }}
  .bewley-embed .bw-code-desc {{
    color: #888;
    font-style: italic;
    font-size: 0.82rem;
    margin-left: 0.5rem;
  }}
  .bewley-embed .bw-snippet {{
    background: #f8f8f8;
    border-left: 3px solid #428a5f;
    padding: 0.5rem 0.8rem;
    margin: 0.4rem 0.8rem;
    border-radius: 0 4px 4px 0;
  }}
  .bewley-embed .bw-snippet-text {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
    font-size: 0.85rem;
    white-space: pre-wrap;
    word-wrap: break-word;
    margin: 0;
    max-height: 12rem;
    overflow-y: auto;
  }}
  .bewley-embed .bw-snippet-memo {{
    font-size: 0.78rem;
    color: #428a5f;
    font-style: italic;
    margin-top: 0.2rem;
  }}
  .bewley-embed .bw-snippet-source {{
    font-size: 0.75rem;
    color: #999;
    margin-top: 0.2rem;
    font-family: 'SF Mono', Consolas, 'Liberation Mono', Menlo, monospace;
  }}
  .bewley-embed .bw-footer {{
    text-align: center;
    color: #999;
    font-size: 0.75rem;
    margin-top: 1rem;
    padding-top: 0.5rem;
    border-top: 1px solid #e0e0e0;
  }}
</style>
  <div class="bw-header">
    <h3>{safe_title}</h3>
  </div>
  <p class="bw-stats">{payload['code_count']} codes &middot; {payload['snippet_count']} annotations &middot; {payload['document_count']} documents</p>
  <div class="bw-toc">
{"".join(toc_items)}
  </div>
{"".join(code_sections)}
  <div class="bw-footer">Generated by bewley</div>
</div>
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


# ── Workflow phase constants ──────────────────────────────────────────────────

_PHASE_INIT = "init"
_PHASE_CORPUS = "corpus"
_PHASE_OPEN_CODING = "open_coding"
_PHASE_ANNOTATING = "annotating"
_PHASE_ANALYSIS = "analysis"

_PHASE_CHECKLISTS: dict[str, list[str]] = {
    _PHASE_INIT: [
        "Run `bewley init` to create the project.",
    ],
    _PHASE_CORPUS: [
        "Copy text files into corpus/ and run `bewley add corpus/<file>` for each.",
        "Verify with `bewley list documents`.",
    ],
    _PHASE_OPEN_CODING: [
        "Read all documents and write qualitative-analysis/corpus_summary.md.",
        "Run `bewley codegen open-coding` to generate the EDSL open-coding script.",
        "Run the generated script: `python qualitative-analysis/run_open_coding.py`.",
        "Review candidate_codes.csv, then `bewley code create <name>` for each keeper.",
    ],
    _PHASE_ANNOTATING: [
        "Resolve quotes: `python qualitative-analysis/resolve_quotes.py candidate_codes.csv -o candidate_codes_resolved.csv`.",
        "Apply annotations: `bewley annotate apply <code> <doc_id> --bytes S:E`.",
        "Build hierarchy: `bewley code set-parent <child> <parent>`.",
        "Create links: `bewley code link <source> <target> <relationship>`.",
        "Write memos: `bewley memo add --code <ref> 'Analytical note'`.",
    ],
    _PHASE_ANALYSIS: [
        "Continue constant comparison: `bewley show snippets --code <ref>`.",
        "Set core category: `bewley code set-core <ref>`.",
        "Export: `bewley export theory --format json --output theory.json`.",
    ],
}

_PHASE_DOCS: dict[str, str] = {
    _PHASE_INIT: "getting-started",
    _PHASE_CORPUS: "getting-started",
    _PHASE_OPEN_CODING: "workflow",
    _PHASE_ANNOTATING: "workflow",
    _PHASE_ANALYSIS: "grounded-theory",
}


def _infer_phase(project: "Project | None") -> str:
    if project is None:
        return _PHASE_INIT
    with project.connect() as conn:
        doc_count = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        code_count = conn.execute("SELECT COUNT(*) FROM codes").fetchone()[0]
        ann_count = conn.execute("SELECT COUNT(*) FROM annotations WHERE is_active = 1").fetchone()[0]
    if doc_count == 0:
        return _PHASE_CORPUS
    if code_count == 0:
        return _PHASE_OPEN_CODING
    if ann_count == 0:
        return _PHASE_ANNOTATING
    return _PHASE_ANALYSIS


def _next_steps_for_phase(phase: str) -> list[dict]:
    if phase == _PHASE_INIT:
        return [{"label": "Initialize project", "command": "bewley init"}]
    if phase == _PHASE_CORPUS:
        return [{"label": "Add first document", "command": "bewley add corpus/<filename>"}]
    if phase == _PHASE_OPEN_CODING:
        return [
            {"label": "Generate EDSL open-coding script", "command": "bewley codegen open-coding"},
            {"label": "See getting-started docs", "command": "bewley docs show getting-started"},
        ]
    if phase == _PHASE_ANNOTATING:
        return [
            {"label": "Resolve quotes", "command": "python qualitative-analysis/resolve_quotes.py candidate_codes.csv -o candidate_codes_resolved.csv"},
            {"label": "See workflow docs", "command": "bewley docs show workflow"},
        ]
    return [
        {"label": "See grounded theory docs", "command": "bewley docs show grounded-theory"},
        {"label": "Export theory", "command": "bewley export theory --format json --output theory.json"},
    ]


def _phase_state(project: "Project | None", project_exists: bool) -> dict:
    phase = _infer_phase(project)
    counts: dict = {}
    if project:
        with project.connect() as conn:
            counts = {
                "documents": conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0],
                "codes": conn.execute("SELECT COUNT(*) FROM codes").fetchone()[0],
                "active_annotations": conn.execute(
                    "SELECT COUNT(*) FROM annotations WHERE is_active = 1"
                ).fetchone()[0],
            }
    return {
        "phase": phase,
        "project_exists": project_exists,
        "counts": counts,
        "checklist": _PHASE_CHECKLISTS.get(phase, []),
        "recommended_next_steps": _next_steps_for_phase(phase),
        "primary_doc": _PHASE_DOCS.get(phase, "overview"),
    }


_OPEN_CODING_SCRIPT_BODY = r'''
def _number_lines(text: str) -> str:
    lines = text.split("\n")
    return "\n".join(f"{i+1:>4}| {line}" for i, line in enumerate(lines))


def main() -> None:
    import ast
    import csv
    import json
    import sys
    from collections import Counter

    if not CORPUS_SUMMARY_PATH.exists():
        print(f"Error: corpus summary not found at {CORPUS_SUMMARY_PATH}", file=sys.stderr)
        sys.exit(1)

    corpus_summary = CORPUS_SUMMARY_PATH.read_text(encoding="utf-8")

    documents = []
    for doc in DOCUMENTS:
        path = PROJECT_DIR / doc["document_path"]
        if not path.exists():
            print(f"Warning: {path} not found, skipping", file=sys.stderr)
            continue
        text = path.read_text(encoding="utf-8")
        documents.append({
            "document_id": doc["document_id"],
            "document_path": doc["document_path"],
            "document_text": text,
            "document_text_numbered": _number_lines(text),
            "corpus_summary": corpus_summary,
        })

    if not documents:
        print("Error: no documents found in project.", file=sys.stderr)
        sys.exit(1)

    print(f"Running open coding on {len(documents)} documents...")

    from edsl import Model, Scenario, ScenarioList, Survey
    from edsl.questions import QuestionFreeText

    scenarios = ScenarioList([
        Scenario({
            "document_id": d["document_id"],
            "document_path": d["document_path"],
            "document_text": d["document_text"],
            "document_text_numbered": d["document_text_numbered"],
            "corpus_summary": d["corpus_summary"],
        })
        for d in documents
    ])

    q_coding = QuestionFreeText(
        question_name="open_coding",
        question_text=(
            "You are a qualitative researcher performing open coding on a text corpus.\n\n"
            "## Corpus overview\n"
            "{{ corpus_summary }}\n\n"
            "## Document to code (with line numbers for reference)\n"
            "Path: {{ document_path }}\n\n"
            "{{ document_text_numbered }}\n\n"
            "## Task\n"
            "Suggest qualitative codes for this document. Return a JSON array of objects, "
            "each with three keys:\n"
            '- "code": a short analytic label (1-4 words, lowercase_with_underscores)\n'
            '- "description": a one-sentence description of what the code captures\n'
            '- "quote": the VERBATIM text from the document that supports this code. '
            "Copy the EXACT words as they appear. Do NOT paraphrase or summarize.\n\n"
            "Guidelines:\n"
            "- Aim for specificity: 'speed_punishment' is better than 'workload'\n"
            "- Prefer IN VIVO codes using the participant's own words\n"
            "- Each quote should be the shortest passage that captures the coded concept\n"
            "- Suggest 3-15 codes per document\n\n"
            "Return ONLY a valid JSON array, no other text."
        ),
    )

    kwargs = {}
    if MODEL:
        kwargs["model"] = Model(MODEL)
    results = Survey(questions=[q_coding]).by(scenarios).run(**kwargs)

    candidates = []
    parse_failures = 0
    for i in range(len(results)):
        row = results[i]
        doc_id = row.sub_dicts["scenario"]["document_id"]
        doc_path = row.sub_dicts["scenario"]["document_path"]
        raw = row.sub_dicts["answer"]["open_coding"]

        entries = []
        if isinstance(raw, list):
            entries = raw
        elif isinstance(raw, str):
            text = raw.strip()
            if text.startswith("```"):
                text = "\n".join(text.split("\n")[1:])
            if text.endswith("```"):
                text = "\n".join(text.split("\n")[:-1])
            text = text.strip()
            start = text.find("[")
            end = text.rfind("]") + 1
            if start >= 0 and end > start:
                fragment = text[start:end]
                try:
                    entries = json.loads(fragment)
                except json.JSONDecodeError:
                    try:
                        entries = ast.literal_eval(fragment)
                    except (ValueError, SyntaxError):
                        parse_failures += 1

        for entry in entries:
            if isinstance(entry, dict):
                candidates.append({
                    "code_name": entry.get("code", "").strip(),
                    "description": entry.get("description", ""),
                    "quote": entry.get("quote", ""),
                    "source_document_id": doc_id,
                    "source_document_path": doc_path,
                })

    if parse_failures:
        print(f"Warning: {parse_failures} documents had unparseable responses", file=sys.stderr)

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["code_name", "description", "quote", "source_document_id", "source_document_path"]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(candidates)

    counts = Counter(c["code_name"] for c in candidates)
    print(f"\nWrote {len(candidates)} candidate codes ({len(counts)} unique) to {OUTPUT_CSV}")
    print("\nNext steps:")
    print(f"  1. Review {OUTPUT_CSV}")
    print(f"  2. python qualitative-analysis/resolve_quotes.py {OUTPUT_CSV} \\")
    print(f"         --project-dir {PROJECT_DIR} \\")
    print(f"         -o qualitative-analysis/candidate_codes_resolved.csv")
    print("  3. bewley code create <name> --description '<desc>'")
    print("  4. bewley annotate apply <code> <doc_id> --bytes <start>:<end>")


if __name__ == "__main__":
    main()
'''


def _build_open_coding_script(project: "Project", project_dir: Path, summary_path: Path, output_csv: Path, model_name: str | None) -> str:
    """Generate the content of the EDSL open-coding script."""
    with project.connect() as conn:
        rows = conn.execute(
            "SELECT document_id, current_path FROM documents ORDER BY current_path"
        ).fetchall()
    docs_list = [{"document_id": r["document_id"], "document_path": r["current_path"]} for r in rows]

    header_lines = [
        '"""',
        "Generated by: bewley codegen open-coding",
        f"Project: {project_dir}",
        f"Documents: {len(docs_list)}",
        "",
        "Run this script to perform open coding on the corpus using EDSL.",
        "",
        "Steps after running:",
        f"  1. Review {output_csv}",
        f"  2. python qualitative-analysis/resolve_quotes.py {output_csv} \\",
        f"         --project-dir {project_dir} \\",
        "         -o qualitative-analysis/candidate_codes_resolved.csv",
        "  3. bewley code create <name> --description '<desc>'",
        "  4. bewley annotate apply <code> <doc_id> --bytes <start>:<end>",
        '"""',
        "from __future__ import annotations",
        "",
        "from pathlib import Path",
        "",
        f"PROJECT_DIR = Path({repr(str(project_dir))})",
        f"CORPUS_SUMMARY_PATH = Path({repr(str(summary_path))})",
        f"OUTPUT_CSV = Path({repr(str(output_csv))})",
        f"MODEL = {repr(model_name)}",
        f"DOCUMENTS = {repr(docs_list)}",
        "",
    ]
    return "\n".join(header_lines) + _OPEN_CODING_SCRIPT_BODY


_RESOLVE_QUOTES_SCRIPT_BODY = r'''

def _strip_surround(s: str) -> str:
    return s.strip().strip(".,;:!?—–-").strip()


def _resolve_quote(doc_bytes: bytes, quote: str) -> dict:
    """Try to locate a quote in a document. Returns dict with keys
    byte_start, byte_end, resolve_status, resolved_text (the substring actually matched)."""
    try:
        doc_text = doc_bytes.decode("utf-8")
    except UnicodeDecodeError:
        doc_text = doc_bytes.decode("utf-8", errors="replace")

    def _byte_range(char_start: int, char_len: int, status: str):
        matched = doc_text[char_start:char_start + char_len]
        bstart = len(doc_text[:char_start].encode("utf-8"))
        bend = bstart + len(matched.encode("utf-8"))
        return {"byte_start": bstart, "byte_end": bend, "resolve_status": status, "resolved_text": matched}

    if not quote:
        return {"byte_start": "", "byte_end": "", "resolve_status": "empty_quote", "resolved_text": ""}

    idx = doc_text.find(quote)
    if idx >= 0:
        return _byte_range(idx, len(quote), "resolved")

    stripped = _strip_surround(quote)
    if stripped and stripped != quote:
        idx = doc_text.find(stripped)
        if idx >= 0:
            return _byte_range(idx, len(stripped), "resolved_stripped")

    lower_doc = doc_text.lower()
    lower_q = quote.lower()
    idx = lower_doc.find(lower_q)
    if idx >= 0:
        return _byte_range(idx, len(quote), "resolved_case_insensitive")

    if stripped:
        lower_stripped = stripped.lower()
        idx = lower_doc.find(lower_stripped)
        if idx >= 0:
            return _byte_range(idx, len(stripped), "resolved_stripped_ci")

    return {"byte_start": "", "byte_end": "", "resolve_status": "not_found", "resolved_text": ""}


def main() -> None:
    import csv
    import sys

    if not INPUT_CSV.exists():
        print(f"Error: input CSV not found at {INPUT_CSV}", file=sys.stderr)
        sys.exit(1)

    with INPUT_CSV.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print("No rows in input CSV.", file=sys.stderr)
        sys.exit(1)

    doc_cache: dict = {}
    def load_doc(rel_path):
        if rel_path not in doc_cache:
            doc_cache[rel_path] = (PROJECT_DIR / rel_path).read_bytes()
        return doc_cache[rel_path]

    by_status: dict = {}
    out_fieldnames = list(rows[0].keys()) + ["byte_start", "byte_end", "resolve_status"]

    out_rows = []
    for r in rows:
        doc_bytes = load_doc(r["source_document_path"])
        result = _resolve_quote(doc_bytes, r["quote"])
        by_status[result["resolve_status"]] = by_status.get(result["resolve_status"], 0) + 1
        row_out = dict(r)
        row_out["byte_start"] = result["byte_start"]
        row_out["byte_end"] = result["byte_end"]
        row_out["resolve_status"] = result["resolve_status"]
        out_rows.append(row_out)

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=out_fieldnames)
        w.writeheader()
        w.writerows(out_rows)

    total = len(rows)
    print(f"Resolved {total - by_status.get('not_found', 0) - by_status.get('empty_quote', 0)}/{total} candidates "
          f"({by_status}) -> {OUTPUT_CSV}", file=sys.stderr)

    unresolved = [r for r in out_rows if r["resolve_status"] in ("not_found", "empty_quote")]
    if unresolved:
        print("\nUnresolved quotes (likely LLM paraphrased; fix manually and re-run):", file=sys.stderr)
        for r in unresolved:
            short = (r["quote"] or "")[:120]
            print(f"  [{r['source_document_path']}] {r['code_name']}: {short}", file=sys.stderr)


if __name__ == "__main__":
    main()
'''


_APPLY_RESOLVED_SCRIPT_BODY = r'''

def _run_bewley(args, dry_run=False):
    import subprocess
    cmd = ["bewley"] + list(args)
    if dry_run:
        print("  [dry-run] " + " ".join(repr(a) if " " in a else a for a in cmd))
        return 0, "", ""
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(PROJECT_DIR))
    return proc.returncode, proc.stdout, proc.stderr


def _code_exists(name: str) -> bool:
    rc, _, _ = _run_bewley(["code", "show", name], dry_run=False)
    return rc == 0


def main() -> None:
    import argparse
    import csv
    import sys

    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Print commands without executing them.")
    ap.add_argument("--skip-existing-codes", action="store_true", default=True, help="Don't error if a code already exists (default: true).")
    args = ap.parse_args()

    if not INPUT_CSV.exists():
        print(f"Error: resolved CSV not found at {INPUT_CSV}", file=sys.stderr)
        sys.exit(1)

    with INPUT_CSV.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print("No rows in resolved CSV.", file=sys.stderr)
        sys.exit(1)

    resolved_statuses = {"resolved", "resolved_stripped", "resolved_case_insensitive", "resolved_stripped_ci"}

    codes_seen: dict = {}
    for r in rows:
        name = r["code_name"]
        if name and name not in codes_seen:
            codes_seen[name] = r.get("description", "")

    print(f"Plan: create/reuse {len(codes_seen)} codes, apply annotations from {len(rows)} rows.", file=sys.stderr)
    if args.dry_run:
        print("[dry-run mode — no changes will be made]", file=sys.stderr)

    print("\n== Phase 1: codes ==", file=sys.stderr)
    created = 0
    reused = 0
    code_errors = []
    for name, desc in codes_seen.items():
        if _code_exists(name):
            reused += 1
            print(f"  reuse {name}", file=sys.stderr)
            continue
        rc, _, err = _run_bewley(
            ["code", "create", name, "--description", desc or ""],
            dry_run=args.dry_run,
        )
        if rc == 0 or args.dry_run:
            created += 1
            if not args.dry_run:
                print(f"  create {name}", file=sys.stderr)
        else:
            code_errors.append((name, err.strip()))
            print(f"  FAIL {name}: {err.strip()}", file=sys.stderr)

    print(f"\n  codes: created={created} reused={reused} errors={len(code_errors)}", file=sys.stderr)

    print("\n== Phase 2: annotations ==", file=sys.stderr)
    applied = 0
    skipped_unresolved = 0
    annot_errors = []
    for r in rows:
        status = (r.get("resolve_status") or "").strip()
        if status not in resolved_statuses:
            skipped_unresolved += 1
            continue
        code = r["code_name"]
        doc = r["source_document_id"] or r["source_document_path"]
        start = (r.get("byte_start") or "").strip()
        end = (r.get("byte_end") or "").strip()
        if not (start and end and code and doc):
            annot_errors.append((code, doc, "missing fields"))
            continue
        rc, _, err = _run_bewley(
            ["annotate", "apply", code, doc, "--bytes", f"{start}:{end}"],
            dry_run=args.dry_run,
        )
        if rc == 0 or args.dry_run:
            applied += 1
        else:
            annot_errors.append((code, doc, err.strip()))
            print(f"  FAIL {code} @ {doc} {start}:{end}: {err.strip()}", file=sys.stderr)

    print(f"\n  annotations: applied={applied} skipped_unresolved={skipped_unresolved} errors={len(annot_errors)}", file=sys.stderr)

    if code_errors or annot_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
'''


def _build_apply_resolved_script(project: "Project", project_dir: Path, input_csv: Path) -> str:
    """Generate the content of the apply-resolved script."""
    header_lines = [
        '"""',
        "Generated by: bewley codegen apply-resolved",
        f"Project: {project_dir}",
        "",
        "Batch-create codes and apply annotations from a resolved quote CSV.",
        "Run with --dry-run first to preview the commands.",
        "",
        "Idempotent: existing codes are reused, not recreated. Rows with",
        "resolve_status outside the resolved-* family are skipped with a warning.",
        '"""',
        "from __future__ import annotations",
        "",
        "from pathlib import Path",
        "",
        f"PROJECT_DIR = Path({repr(str(project_dir))})",
        f"INPUT_CSV = Path({repr(str(input_csv))})",
        "",
    ]
    return "\n".join(header_lines) + _APPLY_RESOLVED_SCRIPT_BODY


def _build_resolve_quotes_script(project: "Project", project_dir: Path, input_csv: Path, output_csv: Path) -> str:
    """Generate the content of the resolve-quotes script."""
    header_lines = [
        '"""',
        "Generated by: bewley codegen resolve-quotes",
        f"Project: {project_dir}",
        "",
        "Map LLM-generated candidate quotes to exact byte ranges in their source",
        "documents. Uses a fuzzy cascade — exact match → strip surrounding punct →",
        "case-insensitive — before giving up.",
        "",
        "Run:",
        f"  python <this script>",
        "",
        "Each output row has `byte_start`, `byte_end`, and `resolve_status` columns.",
        "`resolve_status` values:",
        "  resolved                    — exact substring match (preferred)",
        "  resolved_stripped           — matched after trimming surrounding punctuation",
        "  resolved_case_insensitive   — matched ignoring case",
        "  resolved_stripped_ci        — matched after both trim and lowercasing",
        "  not_found / empty_quote     — unresolvable; fix manually",
        '"""',
        "from __future__ import annotations",
        "",
        "from pathlib import Path",
        "",
        f"PROJECT_DIR = Path({repr(str(project_dir))})",
        f"INPUT_CSV = Path({repr(str(input_csv))})",
        f"OUTPUT_CSV = Path({repr(str(output_csv))})",
        "",
    ]
    return "\n".join(header_lines) + _RESOLVE_QUOTES_SCRIPT_BODY


_THEORY_EXPLORER_HTML_TEMPLATE = r'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>__TITLE__</title>
<style>
  * { box-sizing: border-box; }
  html, body { margin: 0; padding: 0; height: 100%; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: #222; }
  header { padding: 8px 16px; border-bottom: 1px solid #ddd; display: flex; align-items: baseline; gap: 16px; flex-wrap: wrap; }
  header h1 { margin: 0; font-size: 1.05rem; }
  header .summary { color: #666; font-size: 0.85rem; }
  main { display: grid; grid-template-columns: 240px 1fr 360px; height: calc(100vh - 44px); }
  aside { overflow-y: auto; padding: 10px 12px; border-right: 1px solid #eee; font-size: 0.88rem; }
  aside.right { border-right: none; border-left: 1px solid #eee; }
  aside h2 { font-size: 0.78rem; text-transform: uppercase; color: #888; letter-spacing: 0.05em; margin: 14px 0 6px; }
  aside h2:first-child { margin-top: 0; }
  .filter-group label { display: block; padding: 2px 0; cursor: pointer; font-size: 0.85rem; }
  .filter-group input[type=checkbox] { margin-right: 6px; vertical-align: middle; }
  .swatch { display: inline-block; width: 10px; height: 10px; border-radius: 2px; margin-right: 6px; vertical-align: middle; border: 1px solid #0002; }
  svg { width: 100%; height: 100%; display: block; background: #fafafa; }
  .node circle { stroke: #333; stroke-width: 1.2; cursor: pointer; }
  .node.core circle { stroke: #c0f; stroke-width: 3.5px; }
  .node.selected circle { stroke: #000; stroke-width: 3px; }
  .node text { font-size: 11px; pointer-events: none; user-select: none; paint-order: stroke; stroke: #fff; stroke-width: 3px; stroke-linejoin: round; }
  .edge { fill: none; }
  .edge.hierarchy { stroke: #ccc; stroke-width: 1; stroke-dasharray: 3 3; }
  .edge.typed { stroke: #555; stroke-width: 1.4; }
  .edge-label { font-size: 10px; fill: #555; pointer-events: none; paint-order: stroke; stroke: #fafafa; stroke-width: 3px; }
  .dimmed { opacity: 0.12; }
  #detail h3 { margin: 0 0 4px; font-size: 1rem; }
  #detail .meta { color: #666; font-size: 0.82rem; }
  #detail .links { margin-top: 6px; font-size: 0.82rem; }
  #detail .links .rel { color: #555; font-style: italic; }
  #detail .links a { color: #06c; text-decoration: none; }
  #detail .links a:hover { text-decoration: underline; }
  #detail .desc { color: #444; font-size: 0.88rem; margin: 6px 0; }
  .quote { border-left: 3px solid #888; padding: 5px 8px; margin: 6px 0; background: #f6f6f6; font-size: 0.85rem; }
  .quote .loc { color: #888; font-size: 0.72rem; margin-top: 3px; }
  .status-warn { color: #a60; font-weight: bold; }
  .legend { position: absolute; bottom: 10px; right: 10px; background: rgba(255,255,255,0.92); padding: 6px 10px; border-radius: 4px; font-size: 0.78rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1); max-width: 240px; }
  .legend .item { display: flex; align-items: center; gap: 6px; margin: 1px 0; }
  button.reset { font-size: 0.78rem; margin-top: 6px; padding: 2px 8px; }
  input[type=range] { width: 100%; }
  .center-wrap { position: relative; height: 100%; }
  .count-badge { color: #888; font-size: 0.75rem; }
</style>
</head>
<body>
<header>
  <h1>__TITLE__</h1>
  <span class="summary" id="summary"></span>
</header>
<main>
  <aside class="left">
    <h2>Category</h2>
    <div class="filter-group" id="category-filter"></div>
    <h2>Document</h2>
    <div class="filter-group" id="document-filter"></div>
    <h2>Min annotations</h2>
    <input type="range" id="count-slider" min="0" value="0">
    <div><span id="count-value">0</span>+ annotations</div>
    <button class="reset" onclick="window.__resetFilters()">Reset filters</button>
  </aside>
  <section>
    <div class="center-wrap">
      <svg id="graph"></svg>
      <div class="legend" id="legend"></div>
    </div>
  </section>
  <aside class="right">
    <div id="detail"><em style="color:#888">Click a node to see its quotes.</em></div>
  </aside>
</main>
<script>window.PAYLOAD = __PAYLOAD_JSON__;</script>
<script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
<script>
(function(){
  const P = window.PAYLOAD;
  const centerEl = document.querySelector('section');
  const W = centerEl.clientWidth;
  const H = centerEl.clientHeight;

  const byId = new Map(P.codes.map(c => [c.code_id, c]));
  function rootOf(codeId) {
    let cur = byId.get(codeId);
    const seen = new Set();
    while (cur && cur.parent_code_id && byId.has(cur.parent_code_id) && !seen.has(cur.code_id)) {
      seen.add(cur.code_id);
      cur = byId.get(cur.parent_code_id);
    }
    return cur ? cur.code_id : codeId;
  }
  P.codes.forEach(c => { c.root_code_id = rootOf(c.code_id); });
  P.codes.forEach(c => { c.root_name = (byId.get(c.root_code_id) || c).name; });

  const roots = Array.from(new Set(P.codes.map(c => c.root_code_id))).sort((a, b) => {
    const an = byId.get(a).name, bn = byId.get(b).name;
    return an.localeCompare(bn);
  });
  const palette = d3.schemeTableau10.concat(d3.schemeSet3).concat(d3.schemePastel1);
  const color = d3.scaleOrdinal().domain(roots).range(palette);

  const snippetCount = P.snippets.length;
  const docPaths = Array.from(new Set(P.snippets.map(s => s.document_path))).sort();
  document.getElementById('summary').textContent =
    P.codes.length + ' codes · ' + snippetCount + ' annotations · ' + docPaths.length + ' documents' +
    (P.core_code_id && byId.get(P.core_code_id) ? ' · core: ' + byId.get(P.core_code_id).name : '');

  const rootCats = roots.map(rid => ({
    id: rid,
    name: (byId.get(rid) || {name: rid}).name,
    color: color(rid),
    count: P.codes.filter(c => c.root_code_id === rid).length,
  })).sort((a, b) => b.count - a.count);

  const catEl = document.getElementById('category-filter');
  rootCats.forEach(rc => {
    const lbl = document.createElement('label');
    lbl.innerHTML = '<input type="checkbox" checked data-cat="' + rc.id + '">' +
      '<span class="swatch" style="background:' + rc.color + '"></span>' +
      escapeHtml(rc.name) + ' <span class="count-badge">(' + rc.count + ')</span>';
    catEl.appendChild(lbl);
  });

  const docEl = document.getElementById('document-filter');
  docPaths.forEach(d => {
    const lbl = document.createElement('label');
    const short = d.split('/').pop();
    const n = P.snippets.filter(s => s.document_path === d).length;
    lbl.innerHTML = '<input type="checkbox" checked data-doc="' + escapeHtml(d) + '">' +
      escapeHtml(short) + ' <span class="count-badge">(' + n + ')</span>';
    docEl.appendChild(lbl);
  });

  const maxCount = d3.max(P.codes, c => c.annotation_count) || 0;
  const slider = document.getElementById('count-slider');
  slider.max = maxCount;
  slider.addEventListener('input', () => {
    document.getElementById('count-value').textContent = slider.value;
    applyFilters();
  });
  catEl.addEventListener('change', applyFilters);
  docEl.addEventListener('change', applyFilters);

  const svg = d3.select('#graph').attr('viewBox', [0, 0, W, H]);
  svg.append('defs').append('marker').attr('id', 'arrow').attr('viewBox', '0 -5 10 10')
    .attr('refX', 22).attr('refY', 0).attr('markerWidth', 6).attr('markerHeight', 6)
    .attr('orient', 'auto').append('path').attr('d', 'M0,-5L10,0L0,5').attr('fill', '#555');

  const g = svg.append('g');
  svg.call(d3.zoom().scaleExtent([0.2, 4]).on('zoom', e => g.attr('transform', e.transform)));

  const nodes = P.codes.map(c => Object.assign({}, c));
  const hierarchyEdges = P.codes
    .filter(c => c.parent_code_id && byId.has(c.parent_code_id))
    .map(c => ({source: c.parent_code_id, target: c.code_id, kind: 'hierarchy'}));
  const typedEdges = P.links.map(l => ({
    source: l.source_code_id, target: l.target_code_id,
    kind: 'typed', relationship: l.relationship, memo: l.memo
  }));
  const edges = hierarchyEdges.concat(typedEdges);

  function radius(d) { return 7 + Math.sqrt(d.annotation_count || 0) * 4; }

  const sim = d3.forceSimulation(nodes)
    .force('link', d3.forceLink(edges).id(d => d.code_id).distance(d => d.kind === 'hierarchy' ? 55 : 90).strength(d => d.kind === 'hierarchy' ? 0.6 : 0.3))
    .force('charge', d3.forceManyBody().strength(-260))
    .force('center', d3.forceCenter(W/2, H/2))
    .force('collide', d3.forceCollide(d => radius(d) + 6))
    .force('x', d3.forceX(W/2).strength(0.03))
    .force('y', d3.forceY(H/2).strength(0.03));

  const linkSel = g.selectAll('.edge').data(edges).join('path')
    .attr('class', d => 'edge ' + d.kind)
    .attr('marker-end', d => d.kind === 'typed' ? 'url(#arrow)' : null);

  const linkLabelSel = g.selectAll('.edge-label').data(typedEdges).join('text')
    .attr('class', 'edge-label')
    .attr('text-anchor', 'middle')
    .text(d => d.relationship);

  const nodeSel = g.selectAll('.node').data(nodes).join('g')
    .attr('class', d => 'node' + (d.code_id === P.core_code_id ? ' core' : ''))
    .call(d3.drag()
      .on('start', (event, d) => { if (!event.active) sim.alphaTarget(0.25).restart(); d.fx = d.x; d.fy = d.y; })
      .on('drag', (event, d) => { d.fx = event.x; d.fy = event.y; })
      .on('end', (event, d) => { if (!event.active) sim.alphaTarget(0); d.fx = null; d.fy = null; }));

  nodeSel.append('circle')
    .attr('r', radius)
    .attr('fill', d => color(d.root_code_id))
    .on('click', (_, d) => selectCode(d.code_id));

  nodeSel.append('title').text(d => d.name + ' (' + (d.annotation_count || 0) + ')' + (d.description ? '\n' + d.description : ''));

  nodeSel.append('text')
    .attr('dx', d => radius(d) + 3).attr('dy', 4).text(d => d.name);

  sim.on('tick', () => {
    linkSel.attr('d', d => 'M' + d.source.x + ',' + d.source.y + 'L' + d.target.x + ',' + d.target.y);
    linkLabelSel
      .attr('x', d => (d.source.x + d.target.x) / 2)
      .attr('y', d => (d.source.y + d.target.y) / 2 - 2);
    nodeSel.attr('transform', d => 'translate(' + d.x + ',' + d.y + ')');
  });

  const legendEl = document.getElementById('legend');
  rootCats.forEach(rc => {
    const div = document.createElement('div');
    div.className = 'item';
    div.innerHTML = '<span class="swatch" style="background:' + rc.color + '"></span>' + escapeHtml(rc.name);
    legendEl.appendChild(div);
  });
  if (P.core_code_id && byId.get(P.core_code_id)) {
    const div = document.createElement('div');
    div.className = 'item';
    div.innerHTML = '<span class="swatch" style="background:#fff;border:2px solid #c0f"></span>core category';
    legendEl.appendChild(div);
  }

  function applyFilters() {
    const activeCats = new Set(Array.from(catEl.querySelectorAll('input:checked')).map(i => i.dataset.cat));
    const activeDocs = new Set(Array.from(docEl.querySelectorAll('input:checked')).map(i => i.dataset.doc));
    const minCount = +slider.value;
    const visible = new Set();
    P.codes.forEach(c => {
      if (!activeCats.has(c.root_code_id)) return;
      if ((c.annotation_count || 0) < minCount) return;
      const snips = P.snippets.filter(s => s.code_id === c.code_id);
      if (snips.length === 0) { visible.add(c.code_id); return; }
      if (snips.some(s => activeDocs.has(s.document_path))) visible.add(c.code_id);
    });
    nodeSel.classed('dimmed', d => !visible.has(d.code_id));
    linkSel.classed('dimmed', d => {
      const sid = d.source.code_id || d.source;
      const tid = d.target.code_id || d.target;
      return !visible.has(sid) || !visible.has(tid);
    });
    linkLabelSel.classed('dimmed', d => {
      const sid = d.source.code_id || d.source;
      const tid = d.target.code_id || d.target;
      return !visible.has(sid) || !visible.has(tid);
    });
  }

  window.__resetFilters = function() {
    catEl.querySelectorAll('input').forEach(i => i.checked = true);
    docEl.querySelectorAll('input').forEach(i => i.checked = true);
    slider.value = 0;
    document.getElementById('count-value').textContent = '0';
    applyFilters();
  };

  let selectedId = null;
  function selectCode(codeId) {
    selectedId = codeId;
    const c = byId.get(codeId);
    if (!c) return;
    const quotes = P.snippets.filter(s => s.code_id === codeId);
    const outLinks = P.links.filter(l => l.source_code_id === codeId);
    const inLinks = P.links.filter(l => l.target_code_id === codeId);
    const parent = c.parent_code_id ? byId.get(c.parent_code_id) : null;
    const children = P.codes.filter(x => x.parent_code_id === codeId);

    let html = '<h3>' + escapeHtml(c.name) + (c.code_id === P.core_code_id ? ' <span style="color:#c0f">(core)</span>' : '') + '</h3>';
    html += '<div class="meta">';
    if (parent) html += 'parent: <a href="#" onclick="window.__select(\'' + parent.code_id + '\');return false">' + escapeHtml(parent.name) + '</a> · ';
    html += (c.annotation_count || 0) + ' annotation' + (c.annotation_count === 1 ? '' : 's');
    if (c.document_count) html += ' across ' + c.document_count + ' document' + (c.document_count === 1 ? '' : 's');
    html += '</div>';
    if (c.description) html += '<div class="desc">' + escapeHtml(c.description) + '</div>';
    if (children.length) {
      html += '<div class="links"><strong>children:</strong> ';
      html += children.map(ch => '<a href="#" onclick="window.__select(\'' + ch.code_id + '\');return false">' + escapeHtml(ch.name) + '</a>').join(', ');
      html += '</div>';
    }
    if (outLinks.length || inLinks.length) {
      html += '<div class="links" style="margin-top:8px">';
      outLinks.forEach(l => {
        const t = byId.get(l.target_code_id);
        html += '<div>→ <span class="rel">' + escapeHtml(l.relationship) + '</span> → ' +
          '<a href="#" onclick="window.__select(\'' + l.target_code_id + '\');return false">' + escapeHtml(t ? t.name : l.target_code_id) + '</a>' +
          (l.memo ? ' <span style="color:#888">(' + escapeHtml(l.memo) + ')</span>' : '') + '</div>';
      });
      inLinks.forEach(l => {
        const s = byId.get(l.source_code_id);
        html += '<div>' +
          '<a href="#" onclick="window.__select(\'' + l.source_code_id + '\');return false">' + escapeHtml(s ? s.name : l.source_code_id) + '</a>' +
          ' ← <span class="rel">' + escapeHtml(l.relationship) + '</span> ←' +
          (l.memo ? ' <span style="color:#888">(' + escapeHtml(l.memo) + ')</span>' : '') + '</div>';
      });
      html += '</div>';
    }
    if (quotes.length) {
      html += '<h2 style="margin-top:14px">Quotes (' + quotes.length + ')</h2>';
      quotes.forEach(q => {
        const status = (q.anchor_status && q.anchor_status !== 'clean') ? ' <span class="status-warn">[' + escapeHtml(q.anchor_status) + ']</span>' : '';
        const text = q.exact_text || '<document-level>';
        let loc = '';
        if (q.start_line != null) {
          loc = 'L' + q.start_line + (q.end_line && q.end_line !== q.start_line ? '–' + q.end_line : '');
        }
        html += '<div class="quote">' + escapeHtml(text) +
          '<div class="loc">' + escapeHtml(q.document_path) + (loc ? ' ' + loc : '') + status + '</div></div>';
      });
    } else {
      html += '<p style="color:#888;margin-top:10px">No quote anchors — this node is a parent category.</p>';
    }
    document.getElementById('detail').innerHTML = html;
    nodeSel.classed('selected', d => d.code_id === codeId);
  }
  window.__select = selectCode;

  function escapeHtml(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  }

  applyFilters();
})();
</script>
</body>
</html>
'''


_THEORY_EXPLORER_SCRIPT_BODY = r'''

def main() -> None:
    import sys
    html = TEMPLATE.replace("__PAYLOAD_JSON__", PAYLOAD_JSON)
    OUTPUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_HTML.write_text(html, encoding="utf-8")
    print("Wrote " + str(OUTPUT_HTML), file=sys.stderr)


if __name__ == "__main__":
    main()
'''


def _collect_theory_explorer_payload(project: "Project") -> dict[str, Any]:
    """Collect structure + snippets for the interactive theory explorer."""
    theory = project.export_theory_json()
    explorer = code_explorer_payload(project)
    meta_by_id = {c["code_id"]: c for c in explorer["codes"]}
    codes = []
    for c in theory["codes"]:
        meta = meta_by_id.get(c["code_id"], {})
        codes.append({
            "code_id": c["code_id"],
            "name": c["name"],
            "description": c.get("description") or "",
            "parent_code_id": c.get("parent_code_id"),
            "annotation_count": meta.get("annotation_count", c.get("annotation_count", 0)),
            "document_count": meta.get("document_count", 0),
        })
    snippets = [{
        "code_id": s["code_id"],
        "code_name": s["code_name"],
        "document_path": s["document_path"],
        "scope_type": s["scope_type"],
        "start_line": s.get("start_line"),
        "end_line": s.get("end_line"),
        "anchor_status": s.get("anchor_status"),
        "exact_text": s.get("exact_text"),
    } for s in explorer["snippets"]]
    links = [{
        "source_code_id": l["source_code_id"],
        "target_code_id": l["target_code_id"],
        "relationship": l["relationship"],
        "memo": l.get("memo"),
    } for l in theory["links"]]
    core_code_id = theory["core_category"]["code_id"] if theory["core_category"] else None
    return {
        "codes": codes,
        "snippets": snippets,
        "links": links,
        "core_code_id": core_code_id,
        "project_name": project.root.name,
    }


def _build_theory_explorer_script(project: "Project", project_dir: Path, output_html: Path, title: str | None) -> str:
    """Generate the content of the interactive theory-explorer script."""
    import json as _json
    from html import escape as _html_escape

    payload = _collect_theory_explorer_payload(project)
    resolved_title = title or f"Theory explorer · {project.root.name}"
    payload_json = _json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
    template = _THEORY_EXPLORER_HTML_TEMPLATE.replace("__TITLE__", _html_escape(resolved_title))

    header_lines = [
        '"""',
        "Generated by: bewley codegen theory-explorer",
        f"Project: {project_dir}",
        f"Codes: {len(payload['codes'])}  Annotations: {len(payload['snippets'])}",
        "",
        "Run this script to produce the interactive HTML theory explorer.",
        "  python <this script>",
        "",
        "Customization:",
        "  - Edit TEMPLATE below to change CSS / layout / JS behavior.",
        "  - Regenerate (bewley codegen theory-explorer) when codes or annotations change.",
        '"""',
        "from __future__ import annotations",
        "",
        "from pathlib import Path",
        "",
        f"PROJECT_DIR = Path({repr(str(project_dir))})",
        f"OUTPUT_HTML = Path({repr(str(output_html))})",
        "",
        f"PAYLOAD_JSON = {repr(payload_json)}",
        "",
        f"TEMPLATE = {repr(template)}",
        "",
    ]
    return "\n".join(header_lines) + _THEORY_EXPLORER_SCRIPT_BODY


