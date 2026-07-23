"""History and undo commands."""
from __future__ import annotations

from typing import Optional

import typer

from bewley.commands.common import HumanOption, fail, finish, get_project, should_emit_json
from bewley.project import BewleyError, cmd_history

app = typer.Typer(help="View event history and undo operations.")


@app.command("history")
def history_command(
    document: Optional[str] = typer.Option(None, "--document", help="Filter events by document (UUID or path prefix)."),
    code: Optional[str] = typer.Option(None, "--code", help="Filter events by code (name, alias, or UUID)."),
    annotation: Optional[str] = typer.Option(None, "--annotation", help="Filter events by annotation UUID."),
    human: bool = HumanOption,
) -> None:
    """Show the event history, optionally filtered by document, code, or annotation."""
    json_flag = should_emit_json(human)
    command = "history"
    project = get_project(command, json_flag)
    try:
        result = cmd_history(project, document, code, annotation)
    except BewleyError as exc:
        fail(command, exc, json_flag)
        raise typer.Exit(2)
    if json_flag:
        finish(command, result)
    else:
        for row in result:
            print(f"{row['sequence_number']}\t{row['timestamp']}\t{row['event_type']}\t{row['event_id']}")


@app.command("undo")
def undo_command(
    event_id: str = typer.Argument(..., help="Event ID to undo (from 'bewley history' output)."),
    human: bool = HumanOption,
) -> None:
    """Undo a previous event by emitting a compensating event."""
    json_flag = should_emit_json(human)
    command = "undo"
    project = get_project(command, json_flag)
    try:
        event = project.undo(event_id)
    except BewleyError as exc:
        fail(command, exc, json_flag)
        raise typer.Exit(2)
    finish(command, {"event_id": event["event_id"]})
