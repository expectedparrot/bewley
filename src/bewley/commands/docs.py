"""Docs subcommands: list, show, search."""
from __future__ import annotations

import typer

from bewley.commands.common import HumanOption, fail, finish, get_project, should_emit_json
from bewley.docs import DOCS, load_doc, search_docs
from bewley.project import BewleyError

app = typer.Typer(help="Read built-in documentation.")


@app.command("list")
def docs_list(
    human: bool = HumanOption,
) -> None:
    """List all available documentation topics."""
    json_flag = should_emit_json(human)
    command = "docs list"
    result = [{"topic": k, "title": v["title"], "summary": v["summary"]} for k, v in DOCS.items()]
    if json_flag:
        finish(command, result)
    else:
        for item in result:
            print(f"{item['topic']}\t{item['title']}\t{item['summary']}")


@app.command("show")
def docs_show(
    topic: str = typer.Argument(..., help="Documentation topic to show."),
    human: bool = HumanOption,
) -> None:
    """Show the content of a documentation topic."""
    json_flag = should_emit_json(human)
    command = "docs show"
    try:
        text = load_doc(topic)
    except KeyError:
        fail(command, BewleyError(f"unknown topic: {topic!r}", code="NOT_FOUND"), json_flag)
        raise typer.Exit(2)
    if json_flag:
        finish(command, {"topic": topic, "markdown": text})
    else:
        print(text)


@app.command("search")
def docs_search(
    query: str = typer.Argument(..., help="Search query string."),
    human: bool = HumanOption,
) -> None:
    """Search documentation by keyword."""
    json_flag = should_emit_json(human)
    command = "docs search"
    results = search_docs(query)
    if json_flag:
        finish(command, results)
    else:
        for item in results:
            print(f"{item['topic']}\t{item['title']}\tscore={item['score']}")
            if item.get("snippet"):
                print(f"  ...{item['snippet']}...")
