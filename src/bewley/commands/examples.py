"""Bundled example corpora: list them and materialize one locally."""
from __future__ import annotations

from importlib import resources
from pathlib import Path
from typing import Optional

import typer

from ..project import BewleyError
from .common import HumanOption, action, fail, finish, should_emit_json

app = typer.Typer(help="Bundled example corpora.")


def _examples_root():
    return resources.files("bewley") / "examples"


def _example_dirs() -> list:
    root = _examples_root()
    if not root.is_dir():
        return []
    return sorted((entry for entry in root.iterdir() if entry.is_dir()), key=lambda e: e.name)


def _describe(example) -> dict:
    corpus = example / "corpus"
    documents = sorted(f.name for f in corpus.iterdir() if f.name.endswith(".txt")) if corpus.is_dir() else []
    paragraph: list[str] = []
    readme = example / "README.md"
    if readme.is_file():
        for line in readme.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if stripped:
                paragraph.append(stripped)
            elif paragraph:
                break
    return {"name": example.name, "documents": len(documents), "description": " ".join(paragraph)}


@app.command("list")
def list_examples_command(human: bool = HumanOption) -> None:
    """List the example corpora bundled with the installed package."""
    command = "example list"
    json_flag = should_emit_json(human)
    rows = [_describe(example) for example in _example_dirs()]
    if json_flag:
        finish(
            command,
            {"examples": rows},
            next_actions=[
                action(
                    "fetch-example",
                    "Write one example corpus into a local directory.",
                    ["bewley", "example", "fetch", "<name>"],
                    mutates_state=False,
                )
            ],
        )
        return
    from rich.table import Table

    from .common import rich_console

    table = Table(title="Bundled examples")
    table.add_column("name")
    table.add_column("documents", justify="right")
    table.add_column("description")
    for row in rows:
        table.add_row(row["name"], str(row["documents"]), row["description"])
    rich_console().print(table)


@app.command("fetch")
def fetch_example_command(
    name: str = typer.Argument(..., help="Example name from `bewley example list`."),
    dest: Optional[Path] = typer.Option(
        None, "--dest", help="Directory to create (default: ./<name>). Must not already exist."
    ),
    human: bool = HumanOption,
) -> None:
    """Write a bundled example corpus into a new local directory."""
    command = "example fetch"
    json_flag = should_emit_json(human)
    target = dest if dest is not None else Path(name)
    try:
        available = {example.name: example for example in _example_dirs()}
        if name not in available:
            raise BewleyError(
                f"Unknown example '{name}'.",
                code="UNKNOWN_EXAMPLE",
                context={"requested": name, "available": sorted(available)},
                hint="Run `bewley example list` to see the bundled examples.",
            )
        if target.exists():
            raise BewleyError(
                f"Destination '{target}' already exists.",
                code="DESTINATION_EXISTS",
                context={"dest": str(target)},
                hint="Choose a new directory with --dest, or remove the existing one yourself.",
            )
        written: list[str] = []

        def _copy(node, out: Path) -> None:
            if node.name.startswith("."):
                return
            if node.is_dir():
                out.mkdir(parents=True)
                for child in sorted(node.iterdir(), key=lambda e: e.name):
                    _copy(child, out / child.name)
            else:
                out.write_bytes(node.read_bytes())
                written.append(out.relative_to(target).as_posix())

        _copy(available[name], target)
    except BewleyError as e:
        fail(command, e, json_flag)
        return
    documents = sum(1 for path in written if path.endswith(".txt") and "corpus/" in path.replace("\\", "/"))
    data = {
        "example": name,
        "dest": str(target),
        "documents": documents,
        "files_written": sorted(written),
    }
    if json_flag:
        finish(
            command,
            data,
            next_actions=[
                action(
                    "init-project",
                    f"Initialize the project inside {target}/ (cd there first).",
                    ["bewley", "init"],
                    mutates_state=True,
                )
            ],
        )
    else:
        typer.echo(f"wrote {len(written)} files to {target}/ ({documents} corpus documents)")
        typer.echo(f"next: cd {target} && bewley init")
