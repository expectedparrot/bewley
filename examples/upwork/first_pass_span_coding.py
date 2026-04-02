#!/usr/bin/env python3

from __future__ import annotations

import os
import re
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parent
REPO_ROOT = ROOT.parents[1]
BEWLEY_SRC = REPO_ROOT / "src"
DB_PATH = ROOT / ".bewley" / "index" / "bewley.sqlite"
CORPUS_DIR = ROOT / "corpus"


@dataclass(frozen=True)
class CodeRule:
    name: str
    description: str
    pattern: re.Pattern[str]


@dataclass(frozen=True)
class Block:
    start_line: int
    end_line: int
    text: str
    speaker: str | None


CODE_RULES = [
    CodeRule(
        "hiring-selection-and-fit",
        "How participants source, evaluate, and choose freelancers.",
        re.compile(
            r"\b(hire|hiring|interview|candidate|candidates|recommendation|recommendations|search for|"
            r"fit|vet|vett|screen|background checks?|looks amazing on paper)\b",
            re.IGNORECASE,
        ),
    ),
    CodeRule(
        "scope-budget-and-timeline-control",
        "Managing scope changes, spend, hours, pricing, and deadlines.",
        re.compile(
            r"\b(scope|budget|cost|price|pricing|fee|fees|hours|timeline|timelines|deadline|deadlines|"
            r"milestone|milestones|out of scope)\b",
            re.IGNORECASE,
        ),
    ),
    CodeRule(
        "communication-and-coordination",
        "Communication overhead, handoffs, scheduling, and multi-party coordination.",
        re.compile(
            r"\b(communication|communications|check[\s-]?ins|handoff|handoffs|time zone|time zones|"
            r"status meeting|status meetings|stand[\s-]?up|stand[\s-]?ups|responsiveness|collaborat|"
            r"follow[\s-]?up|coordinate|coordination)\b",
            re.IGNORECASE,
        ),
    ),
    CodeRule(
        "onboarding-and-business-context",
        "Briefing freelancers, transferring context, and helping them understand the business.",
        re.compile(
            r"\b(onboard|onboarding|context|background information|background|knowledge|brief|briefing|"
            r"ramp|ramp up|teach|inform|immersed|industry knowledge|business context)\b",
            re.IGNORECASE,
        ),
    ),
    CodeRule(
        "trust-and-work-verification",
        "Monitoring, trust, validation, and confidence in freelancer output or claimed work.",
        re.compile(
            r"\b(trust|trusted|tracking|track(?:ing)?|validate|validation|verify|report them|approve(?:d)? "
            r"the hours|lied|unusual activity|invasive|productivity|don't trust|do not trust|how are they gonna know)\b",
            re.IGNORECASE,
        ),
    ),
    CodeRule(
        "replacement-and-documentation",
        "Replacing freelancers, transitions, handover, and missing documentation.",
        re.compile(
            r"\b(replace|replacing|replacement|transition|handover|new person|documentation|docs|"
            r"document retention|freelancer number two)\b",
            re.IGNORECASE,
        ),
    ),
    CodeRule(
        "ai-assisted-management",
        "AI or automation proposed for hiring, coordination, analysis, or oversight.",
        re.compile(
            r"\b(ai|automation|automated|agent|assistant|robot|machine|predictive)\b",
            re.IGNORECASE,
        ),
    ),
    CodeRule(
        "tooling-and-platform-ecosystem",
        "References to the software stack used to hire, track, and manage work.",
        re.compile(
            r"\b(Upwork|Smartsheet|Monday|Jira|Asana|Workday|OneDrive|Google Docs|Google Sheets|"
            r"Excel|Word|Rudder|LinkedIn|Fiverr|Indeed|Slack|QuickBooks)\b",
            re.IGNORECASE,
        ),
    ),
]


def run_bewley(*args: str) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(BEWLEY_SRC)
    return subprocess.run(
        [sys.executable, "-m", "bewley.cli", *args],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def existing_codes() -> set[str]:
    with connect_db() as conn:
        rows = conn.execute("SELECT canonical_name FROM codes WHERE status = 'active'").fetchall()
    return {row["canonical_name"] for row in rows}


def existing_span_annotations() -> set[tuple[str, str, int, int]]:
    with connect_db() as conn:
        rows = conn.execute(
            """
            SELECT d.current_path, c.canonical_name, a.start_line, a.end_line
            FROM annotations a
            JOIN documents d ON d.document_id = a.document_id
            JOIN codes c ON c.code_id = a.code_id
            WHERE a.is_active = 1 AND a.scope_type = 'span'
            """
        ).fetchall()
    return {
        (row["current_path"], row["canonical_name"], int(row["start_line"]), int(row["end_line"]))
        for row in rows
        if row["start_line"] is not None and row["end_line"] is not None
    }


def ensure_codes() -> int:
    present = existing_codes()
    created = 0
    for rule in CODE_RULES:
        if rule.name in present:
            continue
        run_bewley("code", "create", rule.name, "--description", rule.description)
        created += 1
    return created


def strip_frontmatter(lines: list[str]) -> tuple[int, list[str]]:
    if len(lines) >= 2 and lines[0].strip() == "---":
        for idx in range(1, len(lines)):
            if lines[idx].strip() == "---":
                return idx + 1, lines[idx + 1 :]
    return 0, lines


def parse_blocks(path: Path) -> list[Block]:
    lines = path.read_text(encoding="utf-8").splitlines()
    offset, body_lines = strip_frontmatter(lines)
    blocks: list[Block] = []
    start: int | None = None
    buffer: list[str] = []

    def flush(end_idx: int) -> None:
        nonlocal start, buffer
        if start is None or not buffer:
            start = None
            buffer = []
            return
        speaker = None
        match = re.match(r"^\*\*(.+?):\*\*", buffer[0].strip())
        if match:
            speaker = match.group(1).strip()
        blocks.append(Block(start_line=start + offset + 1, end_line=end_idx + offset + 1, text="\n".join(buffer), speaker=speaker))
        start = None
        buffer = []

    for idx, line in enumerate(body_lines):
        if line.strip():
            if start is None:
                start = idx
            buffer.append(line)
        else:
            flush(idx - 1)
    flush(len(body_lines) - 1)
    return blocks


def relevant_blocks(path: Path) -> list[Block]:
    blocks = parse_blocks(path)
    return [block for block in blocks if block.speaker and block.speaker.lower() != "ginger"]


def merge_ranges(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    if not ranges:
        return []
    merged: list[list[int]] = [[ranges[0][0], ranges[0][1]]]
    for start, end in ranges[1:]:
        last = merged[-1]
        if start <= last[1] + 6:
            last[1] = max(last[1], end)
        else:
            merged.append([start, end])
    return [(start, end) for start, end in merged]


def candidate_ranges(path: Path, rule: CodeRule) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    for block in relevant_blocks(path):
        if rule.pattern.search(block.text):
            ranges.append((block.start_line, block.end_line))
    return merge_ranges(ranges)


def annotation_memo(code_name: str) -> str:
    return f"first-pass thematic span: {code_name}"


def apply_annotations(dry_run: bool = False) -> tuple[int, int]:
    existing = existing_span_annotations()
    created = 0
    skipped = 0

    for path in sorted(CORPUS_DIR.glob("*.md")):
        relpath = path.relative_to(ROOT).as_posix()
        for rule in CODE_RULES:
            for start_line, end_line in candidate_ranges(path, rule):
                key = (relpath, rule.name, start_line, end_line)
                if key in existing:
                    skipped += 1
                    continue
                if dry_run:
                    print(f"DRY RUN {relpath} {rule.name} {start_line}:{end_line}")
                else:
                    run_bewley(
                        "annotate",
                        "apply",
                        rule.name,
                        relpath,
                        "--lines",
                        f"{start_line}:{end_line}",
                        "--memo",
                        annotation_memo(rule.name),
                    )
                existing.add(key)
                created += 1

    return created, skipped


def main(argv: list[str]) -> int:
    dry_run = "--dry-run" in argv
    created_codes = ensure_codes()
    created_annotations, skipped_annotations = apply_annotations(dry_run=dry_run)
    print(f"Created {created_codes} code(s)")
    print(f"{'Planned' if dry_run else 'Created'} {created_annotations} span annotation(s)")
    print(f"Skipped {skipped_annotations} existing span annotation(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
