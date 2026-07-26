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

_COMMAND_RE = re.compile(r"\bbewley ([a-z][a-z0-9-]*)(?: ([a-z][a-z0-9-]*))?")
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


def _registered_commands() -> tuple[set[str], dict[str, set[str]]]:
    from bewley.cli import app

    def name_of(command) -> str:
        return command.name or command.callback.__name__.replace("_", "-")

    top = {name_of(command) for command in app.registered_commands}
    groups: dict[str, set[str]] = {}
    for group in app.registered_groups:
        groups[group.name] = {
            name_of(command) for command in group.typer_instance.registered_commands
        }
    return top, groups


def test_every_documented_command_exists() -> None:
    top, groups = _registered_commands()
    problems: list[str] = []
    for source in PROSE_SOURCES:
        text = _code_spans(source.read_text(encoding="utf-8"))
        for match in _COMMAND_RE.finditer(text):
            first, second = match.group(1), match.group(2)
            if first in top:
                continue
            if first in groups:
                # Second token, when present and word-like, must be a real
                # subcommand; bare group references are fine.
                if second is not None and second not in groups[first]:
                    problems.append(f"{source.name}: bewley {first} {second}")
                continue
            problems.append(f"{source.name}: bewley {first}")
    assert problems == [], "documented commands missing from the CLI:\n" + "\n".join(problems)


def test_every_command_is_documented_in_reference() -> None:
    top, groups = _registered_commands()
    reference = (DOCS_CONTENT / "commands.md").read_text(encoding="utf-8")
    paths = sorted(top) + sorted(
        f"{group} {sub}" for group, subs in groups.items() for sub in subs
    )
    missing = [path for path in paths if f"bewley {path}" not in reference]
    assert missing == [], "commands missing from docs_content/commands.md:\n" + "\n".join(missing)
