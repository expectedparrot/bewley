from __future__ import annotations

from typing import Optional

import typer

from ..project import BewleyError, DEFAULT_QUERY_MODE, cmd_query
from .common import rich_console, HumanOption, fail, finish, get_project, should_emit_json

app = typer.Typer(help="Query annotations.")


@app.command("query")
def query_command(
    expr: str = typer.Argument(..., help="Boolean code expression (use quotes if it contains spaces or shell metacharacters)."),
    mode: Optional[str] = typer.Option(None, "--mode", help="Query mode: 'document' (default) or 'annotation'."),
    case: Optional[str] = typer.Option(None, "--case", help="Restrict evidence to an explicitly linked case."),
    attributes: Optional[list[str]] = typer.Option(None, "--attribute", help="Case attribute equality NAME=VALUE; repeat for AND."),
    speaker: Optional[str] = typer.Option(None, "--speaker", help="Speaker label or role whose turns overlap the evidence."),
    metadata: Optional[list[str]] = typer.Option(None, "--metadata", help="Accepted document metadata FIELD=VALUE; repeat for AND."),
    human: bool = HumanOption,
) -> None:
    """Query annotations using boolean code expressions (AND, OR, NOT)."""
    command = "query"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        cfg_mode = project.config().get("default_query_mode", DEFAULT_QUERY_MODE)
        selected_mode = mode or cfg_mode
        if selected_mode not in {'document','annotation'}:
            raise BewleyError('Query mode must be document or annotation.',code='INVALID_INPUT')
        if case or attributes or speaker or metadata:
            from bewley.case_analysis import query_scoped
            result = query_scoped(project,expr,selected_mode,case=case,attributes=attributes or [],speaker=speaker,metadata=metadata or [])
        else:
            result = cmd_query(project, expr, mode)
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, result)
    else:
        console = rich_console()
        if selected_mode == "document":
            from rich.table import Table

            table = Table(title=f"{len(result)} matching document(s)", show_header=True, header_style="bold green")
            table.add_column("Path", overflow="fold")
            table.add_column("Document ID", no_wrap=True)
            for row in result:
                table.add_row(row["current_path"], row["document_id"][:12])
            console.print(table)
        else:
            from rich.panel import Panel

            console.print(f"[bold green]{len(result)} matching annotation(s)[/bold green]")
            for row in result:
                lines = ""
                if row["start_line"] is not None:
                    lines = f" · lines {row['start_line']}–{row['end_line']}"
                console.print(Panel(
                    row.get("text") or "<document>",
                    title=f"[bold]{row['canonical_name']}[/bold]",
                    subtitle=f"{row['current_path']}{lines}",
                    border_style="green",
                ))
