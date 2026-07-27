"""Thin typer entry point for bewley CLI."""
from __future__ import annotations

import typer
import click
from typer import _click as typer_click

# Re-export Project for backwards compatibility (tests import from bewley.cli)
from bewley.project import Project as Project  # noqa: F401

from bewley.commands.annotations import app as annotate_app
from bewley.commands.agent import (
    app as agent_app,
    capabilities_command,
    guide_command,
    next_command,
    version_command,
)
from bewley.commands.codegen import app as codegen_app
from bewley.commands.codes import app as code_app, codebook_app
from bewley.commands.docs import app as docs_app
from bewley.commands.documents import (
    add_audio_command,
    add_command,
    add_video_command,
    list_app,
    show_app,
    update_command,
)
from bewley.commands.examples import app as example_app
from bewley.commands.export import app as export_app
from bewley.commands.history import history_command, undo_command
from bewley.commands.memos import app as memo_app
from bewley.commands.open_coding import app as open_coding_app
from bewley.commands.project import fsck_command, init_command, rebuild_index_command, status_command
from bewley.commands.cases import attribute_app, case_app, link_app
from bewley.commands.query import query_command
from bewley.commands.speakers import app as speakers_app
from bewley.commands.study import question_app, study_app

app = typer.Typer(
    name="bewley",
    help="Local-first qualitative coding CLI.",
    no_args_is_help=True,
)

# Project lifecycle and flat document commands.
app.command("init")(init_command)
app.command("status")(status_command)
app.command("fsck")(fsck_command)
app.command("rebuild-index")(rebuild_index_command)
app.command("add")(add_command)
app.command("add-audio")(add_audio_command)
app.command("add-video")(add_video_command)
app.command("update")(update_command)

# list <entity> and show <entity> top-level subcommand groups
app.add_typer(list_app, name="list")
app.add_typer(show_app, name="show")

# Code management
app.add_typer(code_app, name="code")
app.add_typer(codebook_app, name="codebook")

# Annotation management
app.add_typer(annotate_app, name="annotate")

# Query
app.command("query")(query_command)

# Export
app.add_typer(export_app, name="export")

# History and undo
app.command("history")(history_command)
app.command("undo")(undo_command)

# Memos
app.add_typer(memo_app, name="memo")

# Docs
app.add_typer(docs_app, name="docs")
app.add_typer(example_app, name="example")
app.add_typer(study_app, name="study")
app.add_typer(question_app, name="question")
app.add_typer(case_app, name="case")
app.add_typer(attribute_app, name="attribute")
app.add_typer(link_app, name="link")
app.add_typer(speakers_app, name="speakers")

# Codegen
app.add_typer(codegen_app, name="codegen")
app.add_typer(open_coding_app, name="open-coding")

# Stable agent contract and workflow inspection.
app.command("capabilities")(capabilities_command)
app.command("version")(version_command)
app.command("guide")(guide_command)
app.command("next")(next_command)
app.add_typer(agent_app, name="agent")


def main(argv: list[str] | None = None) -> int:
    """Entry point compatible with both console_scripts and test code that passes argv."""
    import os
    import sys

    if argv is not None:
        # Support legacy global -H / --human-output flag by converting it to
        # BEWLEY_HUMAN_OUTPUT env var so per-command should_emit_json() picks it up.
        argv = list(argv)
        human = False
        while "-H" in argv:
            argv.remove("-H")
            human = True
        while "--human-output" in argv:
            argv.remove("--human-output")
            human = True
        old_env = os.environ.get("BEWLEY_HUMAN_OUTPUT")
        if human:
            os.environ["BEWLEY_HUMAN_OUTPUT"] = "true"
        old_argv = sys.argv
        sys.argv = ["bewley"] + argv
    else:
        old_env = None
        old_argv = None

    try:
        from bewley.commands.common import fail_unexpected, should_emit_json
        from bewley.project import BewleyError

        try:
            result = app(args=argv, prog_name="bewley", standalone_mode=False)
            return int(result) if isinstance(result, int) else 0
        except (click.exceptions.Exit, typer_click.exceptions.Exit) as exc:
            return int(exc.exit_code)
        except (click.ClickException, typer_click.ClickException) as exc:
            from bewley.commands.common import fail

            try:
                fail(
                    "",
                    BewleyError(
                        exc.format_message(),
                        code="CLI_USAGE",
                        context={"exit_code": exc.exit_code},
                        hint="Run the command with --help to inspect its arguments.",
                    ),
                    should_emit_json(False),
                )
            except typer.Exit as exit_exc:
                return int(exit_exc.exit_code)
        except Exception as exc:
            fail_unexpected(exc, should_emit_json(False))
            return 1
    finally:
        if old_argv is not None:
            sys.argv = old_argv
        if old_env is None and argv is not None:
            os.environ.pop("BEWLEY_HUMAN_OUTPUT", None)
        elif old_env is not None:
            os.environ["BEWLEY_HUMAN_OUTPUT"] = old_env


if __name__ == "__main__":
    raise SystemExit(main())
