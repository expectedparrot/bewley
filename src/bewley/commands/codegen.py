"""codegen subcommands: local, model-free artifact generators."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from bewley.commands.common import HumanOption, fail, finish, get_project, should_emit_json
from bewley.project import BewleyError, _build_theory_explorer_script

app = typer.Typer(help="Generate local artifact-rendering scripts (no model execution).")


@app.command("theory-explorer")
def codegen_theory_explorer(
    output: str = typer.Option(
        "qualitative-analysis/render_theory_explorer.py",
        "--output", "-o",
        help="Path to write the generated render script.",
    ),
    html_output: str = typer.Option(
        "qualitative-analysis/theory_explorer.html",
        "--html-output",
        help="Path the generated script will write the interactive HTML to.",
    ),
    title: Optional[str] = typer.Option(None, "--title", help="Page title for the explorer HTML."),
    human: bool = HumanOption,
) -> None:
    """Generate a Python script that renders an interactive D3 theory explorer as HTML.

    The generated script is standalone (stdlib only) and embeds a snapshot of
    the project's codes, hierarchy, links, and annotations. Regenerate when
    those change.
    """
    json_flag = should_emit_json(human)
    command = "codegen theory-explorer"
    project = get_project(command, json_flag)
    try:
        output_path = Path(output)
        html_path = Path(html_output)
        script_content = _build_theory_explorer_script(project, project.root, html_path, title)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(script_content, encoding="utf-8")
        result = {
            "script_path": str(output_path),
            "run_command": f"python {output_path}",
            "output_html": str(html_path),
        }
    except BewleyError as exc:
        fail(command, exc, json_flag)
        raise typer.Exit(2)
    if json_flag:
        finish(command, result)
    else:
        print(f"Script written to: {output_path}")
        print(f"Run: python {output_path}")
        print(f"Will produce: {html_path}")
