"""Export subcommands: snippets, quotes, html, document-html, theory, narrative."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

from bewley.commands.common import HumanOption, fail, finish, get_project, should_emit_json
from bewley.project import (
    BewleyError,
    cmd_export_document_html,
    cmd_export_html,
    current_text_by_document,
    export_rows_for_selector,
    line_window,
    quote_export_item,
    snippet_export_item,
    snippets_for_code,
)
from bewley.plots import export_plots as write_plots

app = typer.Typer(help="Export coded data as snippets, quotes, plots, HTML, theory diagrams, or narratives.")


@app.command("plots")
def export_plots(
    output_dir: str = typer.Option("bewley-plots", "--output-dir", "-o", help="Directory for SVG plots and JSON data."),
    human: bool = HumanOption,
) -> None:
    """Export code prevalence, document density, and co-occurrence plots."""
    json_flag = should_emit_json(human)
    command = "export plots"
    project = get_project(command, json_flag)
    target = Path(output_dir)
    if not target.is_absolute():
        target = project.root / target
    try:
        result = write_plots(project, target)
    except (BewleyError, OSError) as exc:
        error = exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="IO_ERROR")
        fail(command, error, json_flag)
        return
    if json_flag:
        finish(command, result)
    else:
        for path in result["plots"].values():
            print(path)


@app.command("snippets")
def export_snippets(
    code: str = typer.Option(..., "--code", help="Code to export snippets for (name or UUID)."),
    fmt: str = typer.Option("text", "--format", help="Output format: 'jsonl' or 'text'."),
    context_lines: int = typer.Option(0, "--context-lines", help="Number of surrounding lines to include."),
    human: bool = HumanOption,
) -> None:
    """Export text snippets for a code as JSONL or plain text."""
    json_flag = should_emit_json(human)
    command = "export snippets"
    project = get_project(command, json_flag)
    try:
        rows = snippets_for_code(project, code)
        text_by_document = current_text_by_document(project, rows) if context_lines > 0 else {}
    except BewleyError as exc:
        fail(command, exc, json_flag)
        raise typer.Exit(2)
    if not json_flag:
        if fmt == "text":
            for row in rows:
                selected = row["exact_text"] if row["scope_type"] == "span" else "<document>"
                if context_lines > 0 and row["scope_type"] == "span" and row["start_line"] is not None and row["end_line"] is not None:
                    before, after = line_window(text_by_document[row["document_id"]], row["start_line"], row["end_line"], context_lines)
                    print(f"{row['canonical_name']}\t{row['current_path']}\t{row['annotation_id']}\tbefore={before!r}\tselected={selected!r}\tafter={after!r}")
                else:
                    print(f"{row['canonical_name']}\t{row['current_path']}\t{row['annotation_id']}\t{selected}")
        else:
            for row in rows:
                print(json.dumps(snippet_export_item(row, context_lines, text_by_document), ensure_ascii=False))
    else:
        result = [snippet_export_item(row, context_lines, text_by_document) for row in rows]
        finish(command, result)


@app.command("quotes")
def export_quotes(
    code: Optional[str] = typer.Option(None, "--code", help="Single code to filter by (name or UUID)."),
    query: Optional[str] = typer.Option(None, "--query", help="Boolean code expression to filter by."),
    all_quotes: bool = typer.Option(False, "--all", help="Export every active span annotation in the project."),
    fmt: str = typer.Option("text", "--format", help="Output format: 'jsonl' or 'text'."),
    context_lines: int = typer.Option(0, "--context-lines", help="Number of surrounding lines to include."),
    human: bool = HumanOption,
) -> None:
    """Export quotes filtered by code, boolean query expression, or all annotations."""
    json_flag = should_emit_json(human)
    command = "export quotes"
    provided = sum(1 for v in (code, query, all_quotes) if v)
    if provided != 1:
        fail(command, BewleyError("Provide exactly one of --code, --query, or --all.", code="INVALID_INPUT"), json_flag)
        raise typer.Exit(2)
    project = get_project(command, json_flag)
    try:
        rows = [row for row in export_rows_for_selector(project, code_ref=code, query_expr=query, all_quotes=all_quotes) if row["scope_type"] == "span"]
        text_by_document = current_text_by_document(project, rows) if context_lines > 0 else {}
    except BewleyError as exc:
        fail(command, exc, json_flag)
        raise typer.Exit(2)
    if not json_flag:
        if fmt == "text":
            for row in rows:
                item = quote_export_item(row, context_lines, text_by_document)
                parts = [row["canonical_name"], row["current_path"], row["annotation_id"], f"bytes={row['start_byte']}:{row['end_byte']}", f"lines={row['start_line']}:{row['end_line']}", f"exact={row['exact_text']!r}"]
                if context_lines > 0:
                    parts.append(f"before={item.get('context_before', '')!r}")
                    parts.append(f"after={item.get('context_after', '')!r}")
                print("\t".join(parts))
        else:
            for row in rows:
                print(json.dumps(quote_export_item(row, context_lines, text_by_document), ensure_ascii=False))
    else:
        result = [quote_export_item(row, context_lines, text_by_document) for row in rows]
        finish(command, result)


@app.command("html")
def export_html(
    output: str = typer.Option("bewley-codes.html", "--output", help="Output file path."),
    title: Optional[str] = typer.Option(None, "--title", help="Page title for the HTML output."),
    static: bool = typer.Option(False, "--static", help="Generate a pure HTML/CSS page with no JavaScript."),
    embed: bool = typer.Option(False, "--embed", help="Generate an embeddable HTML fragment instead of a full page."),
    human: bool = HumanOption,
) -> None:
    """Export all codes and annotations as a standalone HTML file."""
    json_flag = should_emit_json(human)
    command = "export html"
    project = get_project(command, json_flag)
    try:
        result = cmd_export_html(project, output, title, static=static, embed=embed)
    except BewleyError as exc:
        fail(command, exc, json_flag)
        raise typer.Exit(2)
    if json_flag:
        finish(command, result)
    else:
        print(result["output_path"])


@app.command("document-html")
def export_document_html(
    document_ref: str = typer.Argument(..., help="Document to export (UUID, path, or prefix)."),
    output: str = typer.Option("bewley-document.html", "--output", help="Output file path."),
    title: Optional[str] = typer.Option(None, "--title", help="Page title for the HTML output."),
    human: bool = HumanOption,
) -> None:
    """Export a single document with inline annotation highlights as HTML."""
    json_flag = should_emit_json(human)
    command = "export document-html"
    project = get_project(command, json_flag)
    try:
        result = cmd_export_document_html(project, document_ref, output, title)
    except BewleyError as exc:
        fail(command, exc, json_flag)
        raise typer.Exit(2)
    if json_flag:
        finish(command, result)
    else:
        print(result["output_path"])


@app.command("theory")
def export_theory(
    fmt: str = typer.Option("mermaid", "--format", help="Output format: 'json' or 'mermaid'."),
    output: Optional[str] = typer.Option(None, "--output", help="Write output to file instead of stdout."),
    human: bool = HumanOption,
) -> None:
    """Export code hierarchy, links, and core category as JSON or Mermaid diagram."""
    json_flag = should_emit_json(human)
    command = "export theory"
    project = get_project(command, json_flag)
    try:
        if fmt == "json":
            result = project.export_theory_json()
            text = json.dumps(result, indent=2, ensure_ascii=False)
        else:
            text = project.export_theory_mermaid()
            result = {"text": text}
    except BewleyError as exc:
        fail(command, exc, json_flag)
        raise typer.Exit(2)
    if output:
        Path(output).write_text(text, encoding="utf-8")
        if json_flag:
            finish(command, {"output_path": output})
        else:
            print(f"wrote {output}")
    elif json_flag:
        if fmt == "json":
            finish(command, result)
        else:
            finish(command, {"text": text})
    else:
        print(text)


@app.command("narrative")
def export_narrative(
    output: Optional[str] = typer.Option(None, "--output", help="Write output to file instead of stdout."),
    human: bool = HumanOption,
) -> None:
    """Export an integrative narrative summary of the project."""
    json_flag = should_emit_json(human)
    command = "export narrative"
    project = get_project(command, json_flag)
    try:
        text = project.export_narrative()
    except BewleyError as exc:
        fail(command, exc, json_flag)
        raise typer.Exit(2)
    if output:
        Path(output).write_text(text, encoding="utf-8")
        if json_flag:
            finish(command, {"output_path": output})
        else:
            print(f"wrote {output}")
    elif json_flag:
        finish(command, {"text": text})
    else:
        print(text)
