"""Local registration of source, transcription, and analysis lineage."""

from pathlib import Path
from typing import Optional

import typer

from bewley import sources
from bewley.exceptions import BewleyError
from bewley.util import parse_byte_range
from .common import fail, finish, get_project

app = typer.Typer(
    help="Preserve source files, raw transcription, and analysis lineage."
)


def _run(operation, *args, **kwargs):
    project = get_project()
    try:
        result = operation(project, *args, **kwargs)
    except (BewleyError, OSError, UnicodeError) as exc:
        fail(
            "",
            exc
            if isinstance(exc, BewleyError)
            else BewleyError(str(exc), code="IO_ERROR"),
            True,
        )
        return
    finish("", result)


@app.command("add")
def add(
    path: Path,
    locator: Optional[str] = typer.Option(
        None, "--locator", help="Durable catalog or source locator."
    ),
):
    """Preserve an exact source artifact locally without executing OCR."""
    _run(sources.add_source, path, locator)


@app.command("transcription")
def transcription(
    source: str,
    path: Path,
    tool: str = typer.Option(..., "--tool"),
    version: Optional[str] = typer.Option(None, "--tool-version"),
):
    """Import the exact raw output of an external transcription tool."""
    _run(sources.add_transcription, source, path, tool, version)


@app.command("derive")
def derive(
    transcription: str,
    path: Path,
    unit: str = typer.Option(..., "--unit"),
    transformation: str = typer.Option(..., "--transformation"),
    raw_bytes: Optional[str] = typer.Option(
        None,
        "--raw-bytes",
        help="START:END byte boundary in the raw output; default is the entire output.",
    ),
    boundary_status: str = typer.Option("confirmed", "--boundary-status"),
    existing: bool = typer.Option(
        False,
        "--existing",
        help="Attach lineage to an already registered current document revision.",
    ),
):
    """Register a prepared analysis document and its explicit raw-source boundary."""
    try:
        byte_range = parse_byte_range(raw_bytes) if raw_bytes else None
    except BewleyError as exc:
        fail("", exc, True)
        return
    _run(
        sources.derive_document,
        transcription,
        path,
        unit,
        transformation,
        byte_range,
        boundary_status,
        existing,
    )


@app.command("show")
def show(document: str):
    """Show lineage and metadata for the current analysis revision."""
    _run(sources.document_lineage, document)


@app.command("metadata")
def metadata(
    document: str,
    field: str,
    original: Optional[str] = typer.Option(None, "--original"),
    normalized: Optional[str] = typer.Option(None, "--normalized"),
    provenance: str = typer.Option(..., "--provenance"),
    confidence: Optional[float] = typer.Option(None, "--confidence"),
    evidence: Optional[str] = typer.Option(None, "--evidence"),
    status: str = typer.Option("proposed", "--status"),
):
    """Record a metadata judgment outside the analyzable document text."""
    _run(
        sources.set_metadata,
        document,
        field,
        original,
        normalized,
        provenance,
        confidence,
        evidence,
        status,
    )


@app.command("export-raw")
def export_raw(transcription: str, output: Path = typer.Option(..., "--output")):
    """Retrieve an exact raw transcription into a new file."""
    project = get_project()
    try:
        with project.connect() as conn:
            raw = sources._resolve(
                conn, "raw_transcriptions", "transcription_id", transcription
            )
        from bewley.util import atomic_create_bytes, sha256_bytes

        content = (
            project.meta / "objects" / "transcriptions" / raw["sha256"]
        ).read_bytes()
        if sha256_bytes(content) != raw["sha256"]:
            raise BewleyError(
                "Raw transcription object is corrupt.", code="INTEGRITY_ERROR"
            )
        atomic_create_bytes(output, content)
    except (BewleyError, OSError) as exc:
        fail(
            "",
            exc
            if isinstance(exc, BewleyError)
            else BewleyError(str(exc), code="IO_ERROR"),
            True,
        )
        return
    finish("", {"output": str(output), "sha256": raw["sha256"]})


@app.command("boundary")
def boundary(
    document: str,
    status: str = typer.Option(..., "--status"),
    reason: str = typer.Option(..., "--reason"),
):
    """Record a decision about an analysis document's source boundary."""
    _run(sources.review_boundary, document, status, reason)
