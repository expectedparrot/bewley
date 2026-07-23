from __future__ import annotations

from typing import Optional

import typer

from ..project import BewleyError, cmd_annotate_show, parse_byte_range, lines_to_byte_range
from .common import HumanOption, fail, finish, get_project, should_emit_json

app = typer.Typer(help="Annotation management.")


@app.command("apply")
def annotate_apply(
    code_ref: str = typer.Argument(..., help="Code to apply (name, alias, or UUID)."),
    document_ref: str = typer.Argument(..., help="Document to annotate (UUID, path, or path prefix)."),
    document: bool = typer.Option(False, "--document", help="Apply code to the entire document."),
    bytes_range: Optional[str] = typer.Option(None, "--bytes", help="Byte range as START:END (0-based, exclusive end)."),
    lines_range: Optional[str] = typer.Option(None, "--lines", help="Line range as START:END (1-based, inclusive)."),
    memo: Optional[str] = typer.Option(None, "--memo", help="Optional memo to attach to this annotation."),
    human: bool = HumanOption,
) -> None:
    """Apply a code to a document or text span."""
    command = "annotate apply"
    json_flag = should_emit_json(human)
    if sum([bool(document), bool(bytes_range), bool(lines_range)]) != 1:
        fail(command, BewleyError("Specify exactly one of --document, --bytes, or --lines", code="INVALID_INPUT"), json_flag)
    try:
        project = get_project()
        if document:
            event = project.add_annotation(code_ref, document_ref, "document", None, memo)
        elif bytes_range:
            event = project.add_annotation(code_ref, document_ref, "span", parse_byte_range(bytes_range), memo)
        else:
            with project.connect() as conn:
                doc = project.resolve_document(conn, document_ref)
                rev = project.current_revision(conn, doc["document_id"])
            content = (project.objects_dir / rev["content_sha256"]).read_bytes().decode("utf-8")
            byte_range = lines_to_byte_range(content, *parse_byte_range(lines_range))
            event = project.add_annotation(code_ref, document_ref, "span", byte_range, memo)
    except BewleyError as e:
        fail(command, e, json_flag)
    ann_id = event["payload"]["annotation_id"]
    if json_flag:
        finish(command, {"annotation_id": ann_id})
    else:
        typer.echo(ann_id)


@app.command("remove")
def annotate_remove(
    annotation_id: str = typer.Argument(..., help="UUID of the annotation to remove."),
    human: bool = HumanOption,
) -> None:
    """Remove (deactivate) an annotation."""
    command = "annotate remove"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        event = project.remove_annotation(annotation_id)
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, {"event_id": event["event_id"]})
    else:
        typer.echo(event["event_id"])


@app.command("show")
def annotate_show(
    annotation_id: str = typer.Argument(..., help="UUID of the annotation to show."),
    human: bool = HumanOption,
) -> None:
    """Show full details of a single annotation."""
    command = "annotate show"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        result = cmd_annotate_show(project, annotation_id)
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, result)
    else:
        for key, val in result.items():
            typer.echo(f"{key}\t{val}")


@app.command("resolve")
def annotate_resolve(
    annotation_id: str = typer.Argument(..., help="UUID of the conflicted annotation."),
    bytes_range: str = typer.Option(..., "--bytes", help="New byte range as START:END."),
    memo: Optional[str] = typer.Option(None, "--memo", help="Optional memo explaining the resolution."),
    human: bool = HumanOption,
) -> None:
    """Manually resolve a conflicted annotation by setting a new byte range."""
    command = "annotate resolve"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        event = project.resolve_annotation(annotation_id, parse_byte_range(bytes_range), memo)
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, {"event_id": event["event_id"]})
    else:
        typer.echo(event["event_id"])
