"""codegen subcommands: open-coding, resolve-quotes, theory-explorer."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from bewley.commands.common import HumanOption, fail, finish, get_project, should_emit_json
from bewley.project import (
    BewleyError,
    _build_apply_resolved_script,
    _build_open_coding_script,
    _build_resolve_quotes_script,
    _build_theory_explorer_script,
)

app = typer.Typer(help="Generate EDSL scripts for AI-assisted coding steps.")


@app.command("open-coding")
def codegen_open_coding(
    output: str = typer.Option(
        "qualitative-analysis/run_open_coding.py",
        "--output", "-o",
        help="Path to write the generated script.",
    ),
    summary: str = typer.Option(
        "qualitative-analysis/corpus_summary.md",
        "--summary",
        help="Path to corpus summary file.",
    ),
    csv_output: str = typer.Option(
        "qualitative-analysis/candidate_codes.csv",
        "--csv-output",
        help="Output CSV path for candidate codes.",
    ),
    model: Optional[str] = typer.Option(None, "--model", help="EDSL model name (e.g., 'claude-opus-4-6')."),
    human: bool = HumanOption,
) -> None:
    """Generate a Python EDSL script for open coding the corpus."""
    json_flag = should_emit_json(human)
    command = "codegen open-coding"
    project = get_project(command, json_flag)
    try:
        output_path = Path(output)
        summary_path = Path(summary)
        csv_path = Path(csv_output)
        script_content = _build_open_coding_script(project, project.root, summary_path, csv_path, model)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(script_content, encoding="utf-8")
        result = {
            "script_path": str(output_path),
            "run_command": f"python {output_path}",
            "output_csv": str(csv_path),
        }
    except BewleyError as exc:
        fail(command, exc, json_flag)
        raise typer.Exit(2)
    if json_flag:
        finish(command, result)
    else:
        print(f"Script written to: {output_path}")
        print(f"Run: python {output_path}")


@app.command("resolve-quotes")
def codegen_resolve_quotes(
    input_csv: str = typer.Option(
        "qualitative-analysis/candidate_codes.csv",
        "--input", "-i",
        help="CSV produced by `bewley codegen open-coding` (or equivalent).",
    ),
    output_csv: str = typer.Option(
        "qualitative-analysis/candidate_codes_resolved.csv",
        "--output-csv",
        help="CSV path the generated script will write to (adds byte_start/byte_end/resolve_status columns).",
    ),
    output: str = typer.Option(
        "qualitative-analysis/run_resolve_quotes.py",
        "--output", "-o",
        help="Path to write the generated script.",
    ),
    human: bool = HumanOption,
) -> None:
    """Generate a script that maps candidate quotes to exact byte ranges.

    The generated script implements a fuzzy fallback cascade (exact →
    strip surrounding punctuation → case-insensitive) so minor LLM drift
    like trailing periods or capitalization shifts resolve automatically.
    """
    json_flag = should_emit_json(human)
    command = "codegen resolve-quotes"
    project = get_project(command, json_flag)
    try:
        output_path = Path(output)
        input_path = Path(input_csv)
        csv_out_path = Path(output_csv)
        script_content = _build_resolve_quotes_script(project, project.root, input_path, csv_out_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(script_content, encoding="utf-8")
        result = {
            "script_path": str(output_path),
            "run_command": f"python {output_path}",
            "input_csv": str(input_path),
            "output_csv": str(csv_out_path),
        }
    except BewleyError as exc:
        fail(command, exc, json_flag)
        raise typer.Exit(2)
    if json_flag:
        finish(command, result)
    else:
        print(f"Script written to: {output_path}")
        print(f"Run: python {output_path}")


@app.command("apply-resolved")
def codegen_apply_resolved(
    input_csv: str = typer.Option(
        "qualitative-analysis/candidate_codes_resolved.csv",
        "--input", "-i",
        help="Resolved CSV (output of `bewley codegen resolve-quotes`).",
    ),
    output: str = typer.Option(
        "qualitative-analysis/run_apply_resolved.py",
        "--output", "-o",
        help="Path to write the generated script.",
    ),
    human: bool = HumanOption,
) -> None:
    """Generate a script that batch-applies codes + annotations from a resolved CSV.

    The generated script shells out to `bewley` for each code create / annotate
    apply. It's idempotent (existing codes are reused) and supports `--dry-run`
    to preview commands before executing.
    """
    json_flag = should_emit_json(human)
    command = "codegen apply-resolved"
    project = get_project(command, json_flag)
    try:
        output_path = Path(output)
        input_path = Path(input_csv)
        script_content = _build_apply_resolved_script(project, project.root, input_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(script_content, encoding="utf-8")
        result = {
            "script_path": str(output_path),
            "run_command": f"python {output_path}",
            "dry_run_command": f"python {output_path} --dry-run",
            "input_csv": str(input_path),
        }
    except BewleyError as exc:
        fail(command, exc, json_flag)
        raise typer.Exit(2)
    if json_flag:
        finish(command, result)
    else:
        print(f"Script written to: {output_path}")
        print(f"Dry run: python {output_path} --dry-run")
        print(f"Apply:   python {output_path}")


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
