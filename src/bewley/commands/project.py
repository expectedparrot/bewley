from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from ..project import BewleyError, Project, cmd_status
from ..bundle import pack_project, unpack_project
from .common import HumanOption, QuietOption, action, fail, finish, get_project, should_emit_json

app = typer.Typer(help="Project management.")
bundle_app = typer.Typer(help="Pack or restore portable Bewley projects.")


@app.command("init")
def init_command(
    human: bool = HumanOption,
    quiet: bool = QuietOption,
) -> None:
    """Create a new bewley project in the current directory."""
    command = "init"
    json_flag = should_emit_json(human)
    try:
        project = Project(Path.cwd())
        project.init_project()
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(
            command,
            {"status": "initialized"},
            next_actions=[
                action(
                    "add-document",
                    "Add the first source document.",
                    ["bewley", "add", "corpus/<filename>"],
                    mutates_state=True,
                )
            ],
        )
    elif not quiet:
        typer.echo("initialized")


@app.command("status")
def status_command(human: bool = HumanOption) -> None:
    """Show project summary counts."""
    command = "status"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        result = cmd_status(project)
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, result)
    else:
        for key in ("documents", "revisions", "codes", "active_annotations", "conflicted_annotations"):
            typer.echo(f"{key}\t{result[key]}")


@app.command("fsck")
def fsck_command(human: bool = HumanOption) -> None:
    """Verify project integrity: events, objects, and index consistency."""
    command = "fsck"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        problems = project.fsck()
    except BewleyError as e:
        fail(command, e, json_flag)
    if problems:
        if json_flag:
            fail(
                command,
                BewleyError(
                    "Project integrity check failed.",
                    code="INTEGRITY_ERROR",
                    context={"problems": problems},
                    hint="Inspect the reported problems before rebuilding derived state.",
                ),
                json_flag,
            )
        else:
            for problem in problems:
                typer.echo(problem, err=True)
        raise typer.Exit(code=1)
    if json_flag:
        finish(command, {"status": "ok"})
    else:
        typer.echo("ok")


@app.command("rebuild-index")
def rebuild_index_command(human: bool = HumanOption) -> None:
    """Rebuild the SQLite index from the append-only event log."""
    import datetime as dt
    command = "rebuild-index"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        project.rebuild_index()
        project.append_event("index_rebuilt", {"timestamp": dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")})
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, {"status": "rebuilt"})
    else:
        typer.echo("rebuilt")


@bundle_app.command("pack")
def pack_command(
    output: Path = typer.Option(..., "--output", "-o", help="New .bewley bundle path."),
    human: bool = HumanOption,
) -> None:
    """Pack the current project into a portable, integrity-checked bundle."""
    command = "project pack"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        result = pack_project(project, output)
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, result)
    else:
        typer.echo(result["path"])


@bundle_app.command("unpack")
def unpack_command(
    bundle: Path = typer.Argument(..., help="Source .bewley bundle."),
    dest: Path = typer.Option(..., "--dest", "-d", help="New destination directory."),
    human: bool = HumanOption,
) -> None:
    """Validate and restore a bundle into a new project directory."""
    command = "project unpack"
    json_flag = should_emit_json(human)
    try:
        result = unpack_project(bundle, dest)
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, result)
    else:
        typer.echo(result["path"])
