"""Read-only validation of the authoritative store and its derived projection."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from pathlib import Path
from typing import Any

from .exceptions import BewleyError


def file_digest(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def read_events(
    project, *, check_head: bool = True
) -> tuple[list[dict[str, Any]], list[str]]:
    events, problems, ids = [], [], set()
    previous = None
    for sequence, path in enumerate(sorted(project.events_dir.glob("*.json")), 1):
        try:
            event = json.loads(path.read_text(encoding="utf-8"))
            required = {
                "event_id",
                "sequence_number",
                "event_type",
                "timestamp",
                "actor",
                "tool_version",
                "payload",
                "parent_event_ids",
                "event_sha256",
            }
            if (
                not isinstance(event, dict)
                or not required <= event.keys()
                or not isinstance(event["payload"], dict)
            ):
                raise ValueError("missing or invalid event fields")
            digest_input = {
                key: value for key, value in event.items() if key != "event_sha256"
            }
            expected = hashlib.sha256(
                json.dumps(digest_input, ensure_ascii=False, sort_keys=True).encode()
            ).hexdigest()
            if event["event_sha256"] != expected:
                problems.append(f"event hash mismatch: {path.name}")
            if (
                event["sequence_number"] != sequence
                or path.name != f"{sequence:012d}.json"
            ):
                problems.append(f"event sequence mismatch: {path.name}")
            if event["event_id"] in ids:
                problems.append(f"duplicate event id: {path.name}")
            if event["parent_event_ids"] != ([previous] if previous else []):
                problems.append(f"event parent mismatch: {path.name}")
            ids.add(event["event_id"])
            previous = event["event_id"]
            events.append(event)
        except (OSError, ValueError, TypeError, KeyError) as exc:
            problems.append(f"invalid event {path.name}: {exc}")
    if not events:
        problems.append("event log is empty")
    elif events[0]["event_type"] != "project_initialized":
        problems.append("first event is not project_initialized")
    if check_head:
        try:
            if int(project.head_path.read_text().strip()) != len(events):
                problems.append("HEAD does not match the event log")
        except (OSError, ValueError):
            problems.append("HEAD is missing or invalid")
    return events, problems


def object_problems(project, events: list[dict]) -> list[str]:
    objects: set[tuple[str, str]] = set()
    for event in events:
        payload = event["payload"]
        if event["event_type"] in {"source_registered", "transcription_registered"}:
            objects.add(
                (
                    "sources"
                    if event["event_type"] == "source_registered"
                    else "transcriptions",
                    payload["sha256"],
                )
            )
        for artifact in payload.get("artifacts", []):
            objects.add(("artifacts", artifact["sha256"]))
        if "content_sha256" in payload:
            kind = (
                "memos"
                if event["event_type"] in {"memo_created", "memo_updated"}
                else "documents"
            )
            objects.add((kind, payload["content_sha256"]))
        for key, kind in [
            ("audio_sha256", "audio"),
            ("video_sha256", "video"),
            ("image_sha256", "images"),
        ]:
            if key in payload:
                objects.add((kind, payload[key]))
        for chunk in payload.get("chunks", []):
            objects.add(("audio", chunk["chunk_audio_sha256"]))
    problems = []
    for kind, digest in sorted(objects):
        if not isinstance(digest, str) or not re.fullmatch("[a-f0-9]{64}", digest):
            problems.append(f"invalid {kind} object digest")
            continue
        path = project.meta / "objects" / kind / digest
        try:
            if path.is_symlink() or file_digest(path) != digest:
                problems.append(f"object hash mismatch: {kind}/{digest}")
        except OSError:
            problems.append(f"missing object: {kind}/{digest}")
    return problems


def code_graph_problems(conn: sqlite3.Connection) -> list[str]:
    codes = {row["code_id"]: dict(row) for row in conn.execute("SELECT * FROM codes")}
    problems = []
    for column in ("merged_into", "parent_code_id"):
        for code_id in codes:
            seen, current = set(), code_id
            while current is not None:
                if current not in codes:
                    problems.append(f"unknown {column} target for code {code_id}")
                    break
                if current in seen:
                    problems.append(f"{column} cycle for code {code_id}")
                    break
                seen.add(current)
                current = codes[current][column]
    return problems


def projection_problems(conn: sqlite3.Connection) -> list[str]:
    problems = code_graph_problems(conn)
    checks = {
        "transcription sources": "SELECT 1 FROM raw_transcriptions r LEFT JOIN source_artifacts s USING(source_id) WHERE s.source_id IS NULL",
        "document lineage": "SELECT 1 FROM document_lineage l LEFT JOIN document_revisions r ON r.revision_id=l.revision_id AND r.document_id=l.document_id LEFT JOIN raw_transcriptions t USING(transcription_id) WHERE r.revision_id IS NULL OR t.transcription_id IS NULL",
        "document revisions": "SELECT 1 FROM document_revisions r LEFT JOIN documents d USING(document_id) WHERE d.document_id IS NULL",
        "current revisions": "SELECT d.document_id FROM documents d LEFT JOIN document_revisions r ON r.document_id=d.document_id AND r.is_current=1 GROUP BY d.document_id HAVING COUNT(r.revision_id) != 1",
        "annotation references": "SELECT 1 FROM annotations a LEFT JOIN codes c USING(code_id) LEFT JOIN document_revisions r ON r.revision_id=a.document_revision_id AND r.document_id=a.document_id WHERE c.code_id IS NULL OR r.revision_id IS NULL",
        "annotation bounds": "SELECT 1 FROM annotations a JOIN document_revisions r ON r.revision_id=a.document_revision_id WHERE a.scope_type='span' AND (a.start_byte IS NULL OR a.end_byte IS NULL OR a.start_byte<0 OR a.end_byte<=a.start_byte OR a.end_byte>r.byte_length)",
        "attribute references": "SELECT 1 FROM attribute_values v LEFT JOIN cases c USING(case_id) LEFT JOIN attribute_definitions d USING(attribute_id) WHERE c.case_id IS NULL OR d.attribute_id IS NULL",
        "speaker references": "SELECT 1 FROM speaker_turns s LEFT JOIN document_revisions r ON r.revision_id=s.revision_id AND r.document_id=s.document_id WHERE r.revision_id IS NULL OR s.start_byte<0 OR s.end_byte<=s.start_byte OR s.end_byte>r.byte_length",
    }
    for label, query in checks.items():
        if conn.execute(query).fetchone():
            problems.append(f"invalid {label}")
    return problems


def replay(project, events: list[dict], conn: sqlite3.Connection) -> None:
    try:
        for event in events:
            project.apply_event(conn, event)
        problems = projection_problems(conn)
        if problems:
            raise BewleyError(
                "Event log violates project invariants.",
                code="INTEGRITY_ERROR",
                context={"problems": problems},
            )
    except (sqlite3.DatabaseError, KeyError, TypeError, ValueError) as exc:
        raise BewleyError(
            "Event log cannot be replayed.",
            code="INTEGRITY_ERROR",
            context={"detail": str(exc)},
        ) from exc


def check_project(project) -> list[str]:
    events, problems = read_events(project)
    if problems:
        return problems
    problems.extend(object_problems(project, events))
    with sqlite3.connect(":memory:") as rebuilt:
        rebuilt.row_factory = sqlite3.Row
        project._init_connection(rebuilt)
        try:
            replay(project, events, rebuilt)
        except BewleyError as exc:
            return problems + [exc.message, *exc.context.get("problems", [])]
        with project.connect() as actual:
            for row in rebuilt.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ):
                table = row["name"]
                # Table names come only from our schema, never a supplied query.
                columns = [
                    r["name"] for r in rebuilt.execute(f'PRAGMA table_info("{table}")')
                ]
                selection = ",".join(f'"{column}"' for column in columns)
                query = f'SELECT {selection} FROM "{table}" ORDER BY {selection}'

                def canonical(rows):
                    result = []
                    for record in rows:
                        values = []
                        for column, value in zip(columns, record):
                            if value is not None and column in {
                                "actor",
                                "snapshot",
                                "metadata_json",
                                "allowed_values",
                            }:
                                try:
                                    value = json.dumps(
                                        json.loads(value),
                                        sort_keys=True,
                                        ensure_ascii=False,
                                    )
                                except (ValueError, TypeError):
                                    pass
                            values.append(value)
                        result.append(tuple(values))
                    return sorted(
                        result, key=lambda value: json.dumps(value, default=str)
                    )

                if canonical(actual.execute(query)) != canonical(
                    rebuilt.execute(query)
                ):
                    problems.append(f"projection content mismatch for {table}")
            for row in rebuilt.execute(
                "SELECT a.annotation_id,a.start_byte,a.end_byte,a.exact_text,r.content_sha256 FROM annotations a JOIN document_revisions r ON r.revision_id=a.document_revision_id WHERE a.scope_type='span'"
            ):
                try:
                    content = (project.objects_dir / row["content_sha256"]).read_bytes()
                    if (
                        content[row["start_byte"] : row["end_byte"]].decode("utf-8")
                        != row["exact_text"]
                    ):
                        problems.append(
                            f"annotation text mismatch: {row['annotation_id']}"
                        )
                except (OSError, UnicodeError):
                    problems.append(
                        f"annotation source unreadable: {row['annotation_id']}"
                    )
    return problems


def validate_mutation(project, conn, event):
    """Check state-dependent preconditions while the writer lock is held."""
    kind, payload = event["event_type"], event["payload"]
    names = {
        "code_created": payload.get("canonical_name"),
        "code_aliased": payload.get("alias_name"),
        "code_split": payload.get("new_canonical_name"),
    }
    name = names.get(kind)
    if kind == "code_renamed":
        current = conn.execute(
            "SELECT canonical_name FROM codes WHERE code_id=?", (payload["code_id"],)
        ).fetchone()
        if current and current["canonical_name"] != payload["old_name"]:
            raise BewleyError(
                "Code changed before the rename was committed.", code="INVALID_INPUT"
            )
        if payload["new_name"] != payload["old_name"]:
            name = payload["new_name"]
    if name and project.code_name_taken(conn, name):
        raise BewleyError("Code name or alias already exists.", code="ALREADY_EXISTS")
    for event_type, table, column, value in [
        ("case_created", "cases", "name", payload.get("name")),
        ("attribute_defined", "attribute_definitions", "name", payload.get("name")),
        ("codebook_released", "codebook_releases", "name", payload.get("name")),
    ]:
        if (
            kind == event_type
            and conn.execute(
                f"SELECT 1 FROM {table} WHERE {column}=?", (value,)
            ).fetchone()
        ):
            raise BewleyError("Named object already exists.", code="ALREADY_EXISTS")
    if kind in {"annotation_added", "document_updated"}:
        revision = project.current_revision(conn, payload["document_id"])
        expected = (
            payload["document_revision_id"]
            if kind == "annotation_added"
            else payload["parent_revision_id"]
        )
        if revision["revision_id"] != expected:
            raise BewleyError(
                "Document changed before this operation was committed.",
                code="STALE_REVISION",
            )
