#!/usr/bin/env python3

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent
EXPORTS = ROOT / "exports"
CORPUS = ROOT / "corpus"
BASE_REPORT = EXPORTS / "upwork-research-report.md"
OUTPUT_REPORT = EXPORTS / "upwork-research-report-with-appendix.md"


def strip_frontmatter(text: str) -> str:
    lines = text.splitlines()
    if lines and lines[0].strip() == "---":
        for idx in range(1, len(lines)):
            if lines[idx].strip() == "---":
                return "\n".join(lines[idx + 1 :]).lstrip()
    return text


def load_metadata(text: str) -> dict[str, str]:
    lines = text.splitlines()
    data: dict[str, str] = {}
    if not lines or lines[0].strip() != "---":
        return data
    for idx in range(1, len(lines)):
        line = lines[idx]
        if line.strip() == "---":
            break
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        data[key.strip()] = value.strip().strip('"')
    return data


def participant_label(metadata: dict[str, str], path: Path) -> str:
    title = metadata.get("title")
    if title:
        return title
    return re.sub(r"[-_]+", " ", path.stem).title()


def build_appendix() -> str:
    sections: list[str] = ["## Appendix: Full Interview Transcripts", ""]
    for path in sorted(CORPUS.glob("*.md")):
        raw = path.read_text(encoding="utf-8")
        metadata = load_metadata(raw)
        body = strip_frontmatter(raw).strip()
        sections.extend(
            [
                r"\newpage",
                "",
                f"### {participant_label(metadata, path)}",
                "",
                f"Source corpus file: `{path.name}`",
                "",
                body,
                "",
            ]
        )
    return "\n".join(sections).rstrip() + "\n"


def main() -> int:
    base = BASE_REPORT.read_text(encoding="utf-8").rstrip() + "\n\n"
    compiled = base + build_appendix()
    OUTPUT_REPORT.write_text(compiled, encoding="utf-8")
    print(OUTPUT_REPORT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
