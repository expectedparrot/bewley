"""Docs↔CLI drift checks: every documented command exists, and vice versa."""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).parents[1]
DOCS_CONTENT = REPO / "src" / "bewley" / "docs_content"
PROSE_SOURCES = [
    *sorted(DOCS_CONTENT.glob("*.md")),
    REPO / "README.md",
    REPO / "CLAUDE.md",
    REPO / "docs" / "index.html",
]

_COMMAND_RE = re.compile(
    r"\bbewley ([a-z][a-z0-9-]*)(?: ([a-z][a-z0-9-]*))?"
    r"(?: ([a-z][a-z0-9-]*))?"
)
_CODE_SPAN_RE = re.compile(
    r"```.*?```|`[^`\n]+`|<code>.*?</code>|<pre>.*?</pre>",
    re.DOTALL,
)


def _code_spans(text: str) -> str:
    """Return only the code-typed portions of a document.

    Command references in prose ("bewley is a local-first…") are not
    contracts; commands shown in code fences, inline code, and <code> blocks
    are.
    """
    return "\n".join(match.group(0) for match in _CODE_SPAN_RE.finditer(text))


def _registered_commands() -> tuple[set[tuple[str, ...]], set[tuple[str, ...]]]:
    from bewley.cli import app

    def name_of(command) -> str:
        return command.name or command.callback.__name__.replace("_", "-")

    commands: set[tuple[str, ...]] = set()
    groups: set[tuple[str, ...]] = set()

    def walk(typer, prefix: tuple[str, ...] = ()) -> None:
        commands.update(prefix + (name_of(command),) for command in typer.registered_commands)
        for group in typer.registered_groups:
            group_path = prefix + (group.name,)
            groups.add(group_path)
            walk(group.typer_instance, group_path)

    walk(app)
    return commands, groups


def test_every_documented_command_exists() -> None:
    commands, groups = _registered_commands()
    problems: list[str] = []
    for source in PROSE_SOURCES:
        text = _code_spans(source.read_text(encoding="utf-8"))
        for match in _COMMAND_RE.finditer(text):
            path = tuple(part for part in match.groups() if part is not None)
            is_command = any(path[:len(command)] == command for command in commands)
            if not is_command and path not in groups:
                problems.append(f"{source.name}: bewley {' '.join(path)}")
    assert problems == [], "documented commands missing from the CLI:\n" + "\n".join(problems)


def test_every_command_is_documented_in_reference() -> None:
    commands, _ = _registered_commands()
    reference = (DOCS_CONTENT / "commands.md").read_text(encoding="utf-8")
    command_paths = sorted(" ".join(path) for path in commands)
    missing = [path for path in command_paths if f"bewley {path}" not in reference]
    assert missing == [], "commands missing from docs_content/commands.md:\n" + "\n".join(missing)
