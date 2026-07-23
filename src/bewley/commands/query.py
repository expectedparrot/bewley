from __future__ import annotations

from typing import Optional

import typer

from ..project import BewleyError, DEFAULT_QUERY_MODE, cmd_query
from .common import HumanOption, fail, finish, get_project, should_emit_json

app = typer.Typer(help="Query annotations.")


@app.command("query")
def query_command(
    expr: str = typer.Argument(..., help="Boolean code expression (use quotes if it contains spaces or shell metacharacters)."),
    mode: Optional[str] = typer.Option(None, "--mode", help="Query mode: 'document' (default) or 'annotation'."),
    human: bool = HumanOption,
) -> None:
    """Query annotations using boolean code expressions (AND, OR, NOT)."""
    command = "query"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        result = cmd_query(project, expr, mode)
        cfg_mode = project.config().get("default_query_mode", DEFAULT_QUERY_MODE)
        selected_mode = mode or cfg_mode
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, result)
    else:
        if selected_mode == "document":
            for row in result:
                typer.echo(f"{row['document_id']}\t{row['current_path']}")
        else:
            for row in result:
                typer.echo(f"{row['annotation_id']}\t{row['canonical_name']}\t{row['current_path']}\t{row['start_line']}\t{row['end_line']}\t{row['anchor_status']}")
