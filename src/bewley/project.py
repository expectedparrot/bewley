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


from .exceptions import (  # noqa: F401  (re-exported for compatibility)
    BewleyError,
)
from .util import (  # noqa: F401  (re-exported for compatibility)
    annotation_overlap,
    atomic_write_text,
    byte_to_char_index_map,
    byte_to_line_range,
    coerce_code_color,
    count_lines,
    default_code_color,
    ensure_utf8_bytes,
    format_timestamp,
    json_dumps,
    line_offsets,
    lines_to_byte_range,
    load_toml,
    parse_byte_range,
    safe_decode,
    sha256_bytes,
    sha256_text,
    soft_color,
    utcnow,
)
from .query_engine import (  # noqa: F401  (re-exported for compatibility)
    BinOp,
    BoolExpr,
    ExprParser,
    Not,
    Term,
)
from .html_export import (  # noqa: F401  (re-exported for compatibility)
    build_code_explorer_html,
    build_document_viewer_html,
    build_embeddable_code_explorer_html,
    build_static_code_explorer_html,
    code_explorer_payload,
    document_viewer_payload,
    render_annotated_document_html,
)
from .workflow import (  # noqa: F401  (re-exported for compatibility)
    _PHASE_ANALYSIS,
    _PHASE_ANNOTATING,
    _PHASE_CHECKLISTS,
    _PHASE_CORPUS,
    _PHASE_DOCS,
    _PHASE_INIT,
    _PHASE_OPEN_CODING,
    _infer_phase,
    _next_steps_for_phase,
    _phase_state,
)
from .theory_explorer import (  # noqa: F401  (re-exported for compatibility)
    _THEORY_EXPLORER_HTML_TEMPLATE,
    _THEORY_EXPLORER_SCRIPT_BODY,
    _build_theory_explorer_script,
    _collect_theory_explorer_payload,
)

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


def find_project_root(start: Path | None = None) -> Path:
    current = (start or Path.cwd()).resolve()
    for candidate in (current, *current.parents):
        if (candidate / PROJECT_DIR).is_dir():
            return candidate
    raise BewleyError("not inside a bewley project", code="NOT_FOUND", hint="Run `bewley init` in the project directory.")


_SCHEMA_SQL = """
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
            self._apply_schema(conn)

    def _apply_schema(self, conn: sqlite3.Connection) -> None:
        """Apply the single authoritative schema (idempotent)."""
        conn.executescript(_SCHEMA_SQL)
        try:
            conn.execute("ALTER TABLE codes ADD COLUMN parent_code_id TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE codes ADD COLUMN merged_into TEXT")
        except sqlite3.OperationalError:
            pass

    def init_project(self) -> None:
        if self.meta.exists():
            raise BewleyError("project already initialized", code="ALREADY_EXISTS")
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
        self._apply_schema(conn)

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
                conn.execute(
                    "UPDATE codes SET status = 'merged', merged_into = ? WHERE code_id = ?",
                    (payload["target_code_id"], source_code_id),
                )
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
            raise BewleyError("document has no current revision", code="INTEGRITY_ERROR")
        return row

    def merge_resolution_map(self, conn: sqlite3.Connection) -> dict[str, str]:
        """Map every code_id to the terminal code it resolves to through merges."""
        parents = {
            row["code_id"]: row["merged_into"]
            for row in conn.execute("SELECT code_id, merged_into FROM codes")
        }

        def root(code_id: str) -> str:
            seen: set[str] = set()
            while parents.get(code_id) and code_id not in seen:
                seen.add(code_id)
                code_id = parents[code_id]
            return code_id

        return {code_id: root(code_id) for code_id in parents}

    def code_family(self, conn: sqlite3.Connection, code_id: str) -> list[str]:
        """All code_ids that resolve to the same merge target as code_id.

        Merging preserves each annotation's original code for provenance;
        evidence surfaces use the family so a target absorbs its sources.
        """
        resolution = self.merge_resolution_map(conn)
        root = resolution.get(code_id, code_id)
        return sorted(cid for cid, target in resolution.items() if target == root) or [code_id]

    def resolve_active_code(self, conn: sqlite3.Connection, ref: str) -> sqlite3.Row:
        """Resolve a code reference, following merges to the surviving target."""
        code = self.resolve_code(conn, ref)
        resolution = self.merge_resolution_map(conn)
        root_id = resolution.get(code["code_id"], code["code_id"])
        if root_id == code["code_id"]:
            return code
        target = conn.execute("SELECT * FROM codes WHERE code_id = ?", (root_id,)).fetchone()
        return target if target is not None else code

    def resolve_document(self, conn: sqlite3.Connection, ref: str) -> sqlite3.Row:
        def ambiguous(rows: list[sqlite3.Row]) -> BewleyError:
            return BewleyError(
                f"ambiguous document reference: {ref}",
                code="AMBIGUOUS_DOCUMENT",
                context={
                    "ref": ref,
                    "matches": [
                        {"document_id": row["document_id"], "path": row["current_path"]}
                        for row in rows
                    ],
                },
                hint="Refer to the document by its full document_id or exact path.",
            )

        exact = conn.execute(
            "SELECT * FROM documents WHERE document_id = ? OR current_path = ?",
            (ref, ref),
        ).fetchall()
        if len(exact) == 1:
            return exact[0]
        if len(exact) > 1:
            raise ambiguous(exact)
        basename = Path(ref).name
        matches = conn.execute("SELECT * FROM documents WHERE current_path LIKE ?", (f"%{basename}",)).fetchall()
        if len(matches) == 1:
            return matches[0]
        if not matches:
            raise BewleyError(
                f"unknown document reference: {ref}",
                code="NOT_FOUND",
                context={"ref": ref},
                hint="Run `bewley list documents` to see tracked documents.",
            )
        raise ambiguous(matches)

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
            raise BewleyError(
                f"unknown code reference: {ref}",
                code="NOT_FOUND",
                context={"ref": ref},
                hint="Run `bewley code list` to see defined codes.",
            )
        seen = {row["code_id"] for row in rows}
        if len(seen) == 1:
            return rows[0]
        raise BewleyError(
            f"ambiguous code reference: {ref}",
            code="AMBIGUOUS_CODE",
            context={
                "ref": ref,
                "matches": sorted(
                    {(row["code_id"], row["canonical_name"]) for row in rows},
                ),
            },
            hint="Refer to the code by its full code_id or canonical name.",
        )

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
            raise BewleyError(f"missing memo object: {content_sha256}", code="INTEGRITY_ERROR")
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
            raise BewleyError(f"document not found: {path_arg}", code="NOT_FOUND")
        with self.connect() as conn:
            existing = conn.execute(
                "SELECT document_id FROM documents WHERE current_path = ?",
                (str(rel),),
            ).fetchone()
            if existing is not None:
                raise BewleyError(f"path is already tracked: {rel}", code="ALREADY_EXISTS")
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
            raise BewleyError("OPENAI_API_KEY is required for media transcription", code="MISSING_DEPENDENCY")
        if not audio_path.is_file():
            raise BewleyError(f"media file not found: {audio_path}", code="NOT_FOUND")
        size = audio_path.stat().st_size
        if size > OPENAI_AUDIO_LIMIT_BYTES:
            raise BewleyError("media file exceeds OpenAI's 25 MB transcription upload limit", code="INVALID_INPUT")
        if response_format == "diarized_json" and prompt:
            raise BewleyError("prompt is not supported with diarized_json transcripts", code="INVALID_INPUT")
        command = [
            "curl", "--silent", "--show-error", "--fail-with-body",
            "--config", "-",
            "https://api.openai.com/v1/audio/transcriptions",
            "-F", f"file=@{audio_path}",
            "-F", f"model={model}",
            "-F", f"response_format={response_format}",
        ]
        if language:
            command.extend(["-F", f"language={language}"])
        if prompt:
            command.extend(["-F", f"prompt={prompt}"])
        # The Authorization header travels via stdin (--config -), never on the
        # argv, so the key is not visible in the process table.
        header_config = f'header = "Authorization: Bearer {api_key}"\n'
        completed = subprocess.run(command, check=True, capture_output=True, text=True, input=header_config)
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
            raise BewleyError(f"audio file not found: {audio_path_arg}", code="NOT_FOUND")
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
            raise BewleyError(f"video file not found: {video_path_arg}", code="NOT_FOUND")
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
            raise BewleyError(f"unknown revision: {revision_id}", code="NOT_FOUND")
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
                raise BewleyError(f"code name already exists: {name}", code="ALREADY_EXISTS")
        return self.append_event("code_created", {"code_id": uuid.uuid4().hex, "canonical_name": name, "description": description, "color": color})

    def rename_code(self, old_ref: str, new_name: str, new_description: str | None = None) -> dict[str, Any]:
        with self.connect() as conn:
            code = self.resolve_code(conn, old_ref)
            if new_name != code["canonical_name"] and self.code_name_taken(conn, new_name):
                raise BewleyError(f"code name already exists: {new_name}", code="ALREADY_EXISTS")
            payload: dict[str, Any] = {"code_id": code["code_id"], "old_name": code["canonical_name"], "new_name": new_name}
            if new_description is not None:
                payload["old_description"] = code["description"]
                payload["new_description"] = new_description
        return self.append_event("code_renamed", payload)

    def alias_code(self, ref: str, alias_name: str) -> dict[str, Any]:
        with self.connect() as conn:
            code = self.resolve_code(conn, ref)
            if self.code_name_taken(conn, alias_name):
                raise BewleyError(f"alias name already exists: {alias_name}", code="ALREADY_EXISTS")
        return self.append_event("code_aliased", {"code_id": code["code_id"], "alias_name": alias_name})

    def merge_codes(self, sources: list[str], target_ref: str) -> dict[str, Any]:
        with self.connect() as conn:
            target = self.resolve_code(conn, target_ref)
            resolved = [self.resolve_code(conn, src) for src in sources]
        source_ids = [row["code_id"] for row in resolved if row["code_id"] != target["code_id"]]
        if not source_ids:
            raise BewleyError("merge requires at least one source distinct from target", code="INVALID_INPUT")
        return self.append_event("code_merged", {"source_code_ids": source_ids, "target_code_id": target["code_id"]})

    def split_code(self, source_ref: str, new_name: str, annotation_ids: list[str], description: str | None = None, color: str | None = None) -> dict[str, Any]:
        if not annotation_ids:
            raise BewleyError("split requires at least one --annotation id")
        with self.connect() as conn:
            source = self.resolve_code(conn, source_ref)
            if self.code_name_taken(conn, new_name):
                raise BewleyError(f"code name already exists: {new_name}", code="ALREADY_EXISTS")
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
            code = self.resolve_active_code(conn, code_ref)
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

    def _effective_code_names(self, conn: sqlite3.Connection) -> dict[str, set[str]]:
        """Per code_id: its own canonical name plus its merge target's name."""
        resolution = self.merge_resolution_map(conn)
        names = {
            row["code_id"]: row["canonical_name"]
            for row in conn.execute("SELECT code_id, canonical_name FROM codes")
        }
        return {
            code_id: {name, names.get(resolution.get(code_id, code_id), name)}
            for code_id, name in names.items()
        }

    def query_documents(self, expr_text: str) -> list[sqlite3.Row]:
        expr = ExprParser(expr_text).parse()
        with self.connect() as conn:
            effective = self._effective_code_names(conn)
            docs = conn.execute("SELECT * FROM documents ORDER BY current_path").fetchall()
            matches: list[sqlite3.Row] = []
            for doc in docs:
                names: set[str] = set()
                for row in conn.execute(
                    "SELECT DISTINCT code_id FROM annotations WHERE document_id = ? AND is_active = 1",
                    (doc["document_id"],),
                ):
                    names |= effective.get(row["code_id"], set())
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
            effective = self._effective_code_names(conn)
            matches: list[sqlite3.Row] = []
            for group in doc_groups.values():
                for row in group:
                    comparable = [other for other in group if annotation_overlap(row, other)]
                    names: set[str] = set()
                    for item in comparable:
                        names |= effective.get(item["code_id"], {item["canonical_name"]})
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
        family = project.code_family(conn, code["code_id"])
        placeholders = ",".join("?" * len(family))
        count = conn.execute(f"SELECT COUNT(*) FROM annotations WHERE code_id IN ({placeholders}) AND is_active = 1", family).fetchone()[0]
        absorbs = [
            row["canonical_name"]
            for row in conn.execute(
                f"SELECT canonical_name FROM codes WHERE code_id IN ({placeholders}) AND code_id != ? ORDER BY canonical_name",
                [*family, code["code_id"]],
            )
        ]
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
    if absorbs:
        result["absorbs"] = absorbs
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
        expanded: list[str] = []
        for cid in code_ids:
            for member in project.code_family(conn, cid):
                if member not in expanded:
                    expanded.append(member)
        direct_family = project.code_family(conn, code["code_id"])
        direct_placeholders = ",".join("?" * len(direct_family))
        placeholders = ",".join("?" * len(expanded))
        direct_docs = conn.execute(f"SELECT COUNT(DISTINCT document_id) FROM annotations WHERE code_id IN ({direct_placeholders}) AND is_active = 1", direct_family).fetchone()[0]
        inclusive_docs = conn.execute(f"SELECT COUNT(DISTINCT document_id) FROM annotations WHERE code_id IN ({placeholders}) AND is_active = 1", expanded).fetchone()[0]
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
        family = project.code_family(conn, code["code_id"])
        placeholders = ",".join("?" * len(family))
        return conn.execute(
            f"SELECT a.*, c.canonical_name, d.current_path FROM annotations a JOIN codes c ON c.code_id = a.code_id JOIN documents d ON d.document_id = a.document_id WHERE a.code_id IN ({placeholders}) AND a.is_active = 1 ORDER BY d.current_path, COALESCE(a.start_line, 0), a.annotation_id",
            family,
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


# ── Workflow phase constants ──────────────────────────────────────────────────


