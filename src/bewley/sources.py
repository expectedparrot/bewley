"""Immutable sources, raw transcriptions, and explicitly bounded derivatives."""

from __future__ import annotations

import mimetypes
import uuid

from .exceptions import BewleyError
from .integrity import file_digest
from .util import atomic_create_bytes, sha256_bytes

SCHEMA = """
CREATE TABLE IF NOT EXISTS source_artifacts (
 source_id TEXT PRIMARY KEY, sha256 TEXT NOT NULL, filename TEXT NOT NULL,
 media_type TEXT NOT NULL, locator TEXT, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS raw_transcriptions (
 transcription_id TEXT PRIMARY KEY, source_id TEXT NOT NULL, sha256 TEXT NOT NULL,
 tool TEXT NOT NULL, tool_version TEXT, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS document_lineage (
 lineage_id TEXT PRIMARY KEY, document_id TEXT NOT NULL, revision_id TEXT NOT NULL,
 transcription_id TEXT NOT NULL, raw_start INTEGER NOT NULL, raw_end INTEGER NOT NULL,
 unit_type TEXT NOT NULL, boundary_status TEXT NOT NULL, transformation TEXT NOT NULL,
 created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS document_metadata (
 document_id TEXT NOT NULL, field TEXT NOT NULL, original TEXT, normalized TEXT,
 provenance TEXT NOT NULL, confidence REAL, evidence TEXT, review_status TEXT NOT NULL,
 PRIMARY KEY(document_id,field)
);
"""


def apply_source_event(conn, event):
    payload = event["payload"]
    kind = event["event_type"]
    timestamp = event["timestamp"]
    if kind == "source_registered":
        conn.execute(
            "INSERT INTO source_artifacts VALUES (?,?,?,?,?,?)",
            (
                payload["source_id"],
                payload["sha256"],
                payload["filename"],
                payload["media_type"],
                payload.get("locator"),
                timestamp,
            ),
        )
    elif kind == "transcription_registered":
        conn.execute(
            "INSERT INTO raw_transcriptions VALUES (?,?,?,?,?,?)",
            (
                payload["transcription_id"],
                payload["source_id"],
                payload["sha256"],
                payload["tool"],
                payload.get("tool_version"),
                timestamp,
            ),
        )
    elif kind == "document_derived":
        conn.execute(
            "INSERT INTO document_lineage VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                payload["lineage_id"],
                payload["document_id"],
                payload["revision_id"],
                payload["transcription_id"],
                payload["raw_start"],
                payload["raw_end"],
                payload["unit_type"],
                payload["boundary_status"],
                payload["transformation"],
                timestamp,
            ),
        )
    elif kind == "document_boundary_reviewed":
        conn.execute(
            "UPDATE document_lineage SET boundary_status=? WHERE lineage_id=?",
            (payload["status"], payload["lineage_id"]),
        )
    elif kind == "document_metadata_set":
        conn.execute(
            "INSERT OR REPLACE INTO document_metadata VALUES (?,?,?,?,?,?,?,?)",
            (
                payload["document_id"],
                payload["field"],
                payload.get("original"),
                payload.get("normalized"),
                payload["provenance"],
                payload.get("confidence"),
                payload.get("evidence"),
                payload["review_status"],
            ),
        )
    else:
        return False
    return True


def _store(project, kind, path):
    content = path.read_bytes()
    digest = sha256_bytes(content)
    directory = project.meta / "objects" / kind
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / digest
    if destination.exists():
        if file_digest(destination) != digest:
            raise BewleyError(
                "Stored source object is corrupt.", code="INTEGRITY_ERROR"
            )
    else:
        try:
            atomic_create_bytes(destination, content)
        except FileExistsError:
            if file_digest(destination) != digest:
                raise BewleyError(
                    "Stored source object is corrupt.", code="INTEGRITY_ERROR"
                )
    return digest


def _resolve(conn, table, column, reference):
    rows = conn.execute(
        f"SELECT * FROM {table} WHERE {column}=? OR {column} LIKE ?",
        (reference, reference + "%"),
    ).fetchall()
    if len(rows) != 1:
        raise BewleyError(
            "Source reference is missing or ambiguous.",
            code="NOT_FOUND" if not rows else "AMBIGUOUS_SOURCE",
            context={"reference": reference},
        )
    return rows[0]


def add_source(project, path, locator=None):
    path = path if path.is_absolute() else project.root / path
    if not path.is_file():
        raise BewleyError("Source file does not exist.", code="NOT_FOUND")
    payload = {
        "source_id": uuid.uuid4().hex,
        "sha256": _store(project, "sources", path),
        "filename": path.name,
        "media_type": mimetypes.guess_type(path.name)[0] or "application/octet-stream",
        "locator": locator,
    }
    return project.append_event("source_registered", payload)["payload"]


def add_transcription(project, source, path, tool, version=None):
    if not tool.strip():
        raise BewleyError("Transcription tool must be recorded.", code="INVALID_INPUT")
    with project.connect() as conn:
        row = _resolve(conn, "source_artifacts", "source_id", source)
    path = path if path.is_absolute() else project.root / path
    payload = {
        "transcription_id": uuid.uuid4().hex,
        "source_id": row["source_id"],
        "sha256": _store(project, "transcriptions", path),
        "tool": tool,
        "tool_version": version,
    }
    return project.append_event("transcription_registered", payload)["payload"]


def derive_document(
    project,
    transcription,
    path,
    unit,
    transformation,
    byte_range=None,
    boundary_status="confirmed",
    existing=False,
):
    if not unit.strip() or not transformation.strip():
        raise BewleyError(
            "Unit type and transformation are required.", code="INVALID_INPUT"
        )
    if boundary_status not in {"confirmed", "needs-review"}:
        raise BewleyError(
            "Boundary status must be confirmed or needs-review.", code="INVALID_INPUT"
        )
    with project.connect() as conn:
        raw = _resolve(conn, "raw_transcriptions", "transcription_id", transcription)
    content = (project.meta / "objects" / "transcriptions" / raw["sha256"]).read_bytes()
    start, end = byte_range or (0, len(content))
    if not 0 <= start < end <= len(content):
        raise BewleyError("Raw byte range is invalid.", code="INVALID_INPUT")
    # The supplied derivative is never rewritten or prefixed with metadata.
    if existing:
        with project.connect() as conn:
            document = project.resolve_document(conn, str(path))
            revision = project.current_revision(conn, document["document_id"])
            document_id, revision_id = document["document_id"], revision["revision_id"]
            if conn.execute(
                "SELECT 1 FROM document_lineage WHERE revision_id=?", (revision_id,)
            ).fetchone():
                raise BewleyError(
                    "This revision already has source lineage.", code="ALREADY_EXISTS"
                )
    else:
        event = project.add_document(str(path))
        document_id, revision_id = (
            event["payload"]["document_id"],
            event["payload"]["revision_id"],
        )
    payload = {
        "lineage_id": uuid.uuid4().hex,
        "document_id": document_id,
        "revision_id": revision_id,
        "transcription_id": raw["transcription_id"],
        "raw_start": start,
        "raw_end": end,
        "unit_type": unit,
        "boundary_status": boundary_status,
        "transformation": transformation,
    }
    return project.append_event("document_derived", payload)["payload"]


def set_metadata(
    project,
    document,
    field,
    original,
    normalized,
    provenance,
    confidence,
    evidence,
    status,
):
    if field not in {
        "author",
        "recipient",
        "date",
        "document_type",
        "language",
        "collection",
    }:
        raise BewleyError("Unsupported metadata field.", code="INVALID_INPUT")
    if provenance not in {
        "catalog",
        "text",
        "filename",
        "user",
        "inferred",
    } or status not in {"proposed", "accepted", "rejected", "unknown"}:
        raise BewleyError(
            "Invalid metadata provenance or review status.", code="INVALID_INPUT"
        )
    if status == "unknown" and (original is not None or normalized is not None):
        raise BewleyError(
            "Unknown metadata must have null values.", code="INVALID_INPUT"
        )
    if confidence is not None and not 0 <= confidence <= 1:
        raise BewleyError(
            "Confidence must be between zero and one.", code="INVALID_INPUT"
        )
    with project.connect() as conn:
        doc = project.resolve_document(conn, document)
    payload = {
        "document_id": doc["document_id"],
        "field": field,
        "original": original,
        "normalized": normalized,
        "provenance": provenance,
        "confidence": confidence,
        "evidence": evidence,
        "review_status": status,
    }
    return project.append_event("document_metadata_set", payload)["payload"]


def document_lineage(project, document, revision=None):
    with project.connect() as conn:
        doc = project.resolve_document(conn, document)
        revision = (
            revision
            or project.current_revision(conn, doc["document_id"])["revision_id"]
        )
        rows = [
            dict(row)
            for row in conn.execute(
                """SELECT l.*,r.sha256 raw_sha256,r.tool,r.tool_version,s.source_id,s.sha256 source_sha256,s.filename,s.locator
            FROM document_lineage l JOIN raw_transcriptions r USING(transcription_id) JOIN source_artifacts s USING(source_id)
            WHERE l.document_id=? AND l.revision_id=?""",
                (doc["document_id"], revision),
            )
        ]
        metadata = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM document_metadata WHERE document_id=? ORDER BY field",
                (doc["document_id"],),
            )
        ]
    return {
        "document_id": doc["document_id"],
        "revision_id": revision,
        "lineage": rows,
        "metadata": metadata,
        "lineage_status": "recorded" if rows else "unrecorded",
    }


def review_boundary(project, document, status, reason):
    if status not in {"confirmed", "needs-review"} or not reason.strip():
        raise BewleyError(
            "Boundary review requires a valid status and reason.", code="INVALID_INPUT"
        )
    lineage = document_lineage(project, document)["lineage"]
    if len(lineage) != 1:
        raise BewleyError(
            "Current revision has no unique lineage record.", code="NOT_FOUND"
        )
    return project.append_event(
        "document_boundary_reviewed",
        {"lineage_id": lineage[0]["lineage_id"], "status": status, "reason": reason},
    )["payload"]
