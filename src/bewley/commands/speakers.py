"""Speaker segmentation and roles for transcripts (RFC 001 slice 3, issue #3)."""
from __future__ import annotations

from typing import List, Optional

import typer

from ..project import BewleyError, cmd_speakers_list
from .common import HumanOption, action, fail, finish, get_project, should_emit_json

app = typer.Typer(help="Speaker turns and roles in transcript documents.")


@app.command("detect")
def speakers_detect_command(
    document_ref: str = typer.Argument(..., help="Transcript document (path or id)."),
    labels: Optional[List[str]] = typer.Option(
        None, "--label",
        help="Explicit speaker label (repeatable) for mixed-case transcripts; default rule matches ALL-CAPS labels.",
    ),
    human: bool = HumanOption,
) -> None:
    """Segment a transcript into speaker turns (recorded as an event)."""
    command = "speakers detect"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        event = project.segment_document(document_ref, labels=list(labels) if labels else None)
        payload = event["payload"]
        labels_found = sorted({turn["label"] for turn in payload["turns"]})
        data = {
            "document_id": payload["document_id"],
            "revision_id": payload["revision_id"],
            "rule": payload["rule"],
            "turn_count": len(payload["turns"]),
            "labels": labels_found,
        }
    except BewleyError as e:
        fail(command, e, json_flag)
        return
    if json_flag:
        finish(command, data, next_actions=[
            action(
                "assign-roles",
                "Assign each detected label a role so coding can respect the interviewer boundary.",
                ["bewley", "speakers", "set-role", "<label>", "<interviewer|participant|other>"],
                mutates_state=True,
            ),
        ])
    else:
        typer.echo(f"{data['turn_count']} turns, labels: {', '.join(labels_found)}")


@app.command("list")
def speakers_list_command(
    document_ref: str = typer.Argument(..., help="Transcript document (path or id)."),
    human: bool = HumanOption,
) -> None:
    """Show a document's speakers: turns, share of text, role, linked case."""
    command = "speakers list"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        data = cmd_speakers_list(project, document_ref)
    except BewleyError as e:
        fail(command, e, json_flag)
        return
    if json_flag:
        finish(command, data)
        return
    from rich.table import Table

    from .common import rich_console

    table = Table(title=f"{data['current_path']} — {data['turn_count']} turns")
    table.add_column("label")
    table.add_column("role")
    table.add_column("turns", justify="right")
    table.add_column("share", justify="right")
    table.add_column("case")
    for speaker in data["speakers"]:
        table.add_row(
            speaker["label"],
            speaker["role"] or "[unassigned]",
            str(speaker["turns"]),
            f"{speaker['byte_share']:.0%}",
            speaker["case"] or "-",
        )
    rich_console().print(table)


@app.command("set-role")
def speakers_set_role_command(
    label: str = typer.Argument(..., help="A label from `bewley speakers detect`."),
    role: str = typer.Argument(..., help="interviewer | participant | other (project-wide)."),
    human: bool = HumanOption,
) -> None:
    """Assign a role to a speaker label, project-wide."""
    command = "speakers set-role"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        project.set_speaker_role(label, role)
    except BewleyError as e:
        fail(command, e, json_flag)
        return
    if json_flag:
        finish(command, {"label": label, "role": role})
    else:
        typer.echo("ok")


@app.command("link-case")
def speakers_link_case_command(
    document_ref: str = typer.Argument(..., help="Transcript document (path or id)."),
    label: str = typer.Argument(..., help="Speaker label within that document."),
    case_ref: str = typer.Argument(..., help="Case the voice belongs to."),
    human: bool = HumanOption,
) -> None:
    """Link a document's speaker to a case (whose voice this is)."""
    command = "speakers link-case"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        event = project.link_speaker_case(document_ref, label, case_ref)
        data = {"link_id": event["payload"]["link_id"]}
    except BewleyError as e:
        fail(command, e, json_flag)
        return
    if json_flag:
        finish(command, data)
    else:
        typer.echo(data["link_id"])
