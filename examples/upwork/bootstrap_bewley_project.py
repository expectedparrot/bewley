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
TRANSCRIPTS_DIR = ROOT / "Transcripts"
CORPUS_DIR = ROOT / "corpus"
DB_PATH = ROOT / ".bewley" / "index" / "bewley.sqlite"

NOISE_SUFFIX = "_ENGLISH (UNITED STATES)_MT"
IMAGE_LINE_RE = re.compile(r"^!\[[^\]]*]\(.*\)\s*$")
MULTIBLANK_RE = re.compile(r"\n{3,}")


@dataclass(frozen=True)
class TranscriptMeta:
    source_path: Path
    corpus_relpath: Path
    title: str
    participant_name: str
    participant_slug: str
    participant_type: str
    business_segment: str | None
    codes: tuple[str, ...]


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


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return re.sub(r"-{2,}", "-", slug)


def parse_source_name(path: Path) -> TranscriptMeta:
    raw_name = path.stem
    trimmed = raw_name.removesuffix(NOISE_SUFFIX).strip()

    if trimmed.startswith("Current User_"):
        participant_type = "current-user"
        remainder = trimmed[len("Current User_") :].strip()
        participant_name = remainder
        business_segment = None
    elif trimmed.startswith("Prospects_"):
        participant_type = "prospect"
        remainder = trimmed[len("Prospects_") :].strip()
        if " - " in remainder:
            participant_name, business_segment = [part.strip() for part in remainder.split(" - ", 1)]
        else:
            participant_name = remainder
            business_segment = None
    else:
        raise ValueError(f"Unrecognized transcript filename pattern: {path.name}")

    participant_slug = slugify(participant_name)
    segment_slug = slugify(business_segment) if business_segment else None

    filename_parts = [participant_type, participant_slug]
    if segment_slug:
        filename_parts.append(segment_slug)
    corpus_name = "-".join(filename_parts) + ".md"

    title_bits = [participant_name]
    if business_segment:
        title_bits.append(business_segment)
    title_bits.append(participant_type.replace("-", " ").title())

    codes = ["transcript", participant_type, f"participant-{participant_slug}"]
    if segment_slug:
        codes.append(segment_slug)

    return TranscriptMeta(
        source_path=path,
        corpus_relpath=Path("corpus") / corpus_name,
        title=" | ".join(title_bits),
        participant_name=participant_name,
        participant_slug=participant_slug,
        participant_type=participant_type,
        business_segment=business_segment,
        codes=tuple(codes),
    )


def clean_markdown(content: str) -> str:
    lines = [line.rstrip() for line in content.splitlines()]
    filtered = [line for line in lines if not IMAGE_LINE_RE.match(line)]
    text = "\n".join(filtered).strip() + "\n"
    return MULTIBLANK_RE.sub("\n\n", text)


def yaml_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def render_corpus_markdown(meta: TranscriptMeta, content: str) -> str:
    frontmatter = [
        "---",
        f'title: "{yaml_escape(meta.title)}"',
        f'participant_name: "{yaml_escape(meta.participant_name)}"',
        f'participant_slug: "{yaml_escape(meta.participant_slug)}"',
        f'participant_type: "{yaml_escape(meta.participant_type)}"',
        f'source_file: "{yaml_escape(meta.source_path.name)}"',
    ]
    if meta.business_segment:
        frontmatter.append(f'business_segment: "{yaml_escape(meta.business_segment)}"')
    frontmatter.extend(["---", ""])
    return "\n".join(frontmatter) + clean_markdown(content)


def ensure_project() -> None:
    if (ROOT / ".bewley").exists():
        return
    run_bewley("init")


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def tracked_document_paths() -> set[str]:
    if not DB_PATH.exists():
        return set()
    with connect_db() as conn:
        rows = conn.execute("SELECT current_path FROM documents").fetchall()
    return {row["current_path"] for row in rows}


def existing_codes() -> set[str]:
    if not DB_PATH.exists():
        return set()
    with connect_db() as conn:
        rows = conn.execute("SELECT canonical_name FROM codes WHERE status = 'active'").fetchall()
    return {row["canonical_name"] for row in rows}


def existing_document_annotations() -> set[tuple[str, str]]:
    if not DB_PATH.exists():
        return set()
    with connect_db() as conn:
        rows = conn.execute(
            """
            SELECT d.current_path, c.canonical_name
            FROM annotations a
            JOIN documents d ON d.document_id = a.document_id
            JOIN codes c ON c.code_id = a.code_id
            WHERE a.is_active = 1 AND a.scope_type = 'document'
            """
        ).fetchall()
    return {(row["current_path"], row["canonical_name"]) for row in rows}


def write_corpus_documents() -> list[TranscriptMeta]:
    CORPUS_DIR.mkdir(exist_ok=True)
    metas: list[TranscriptMeta] = []

    for path in sorted(TRANSCRIPTS_DIR.glob("*.md")):
        meta = parse_source_name(path)
        content = path.read_text(encoding="utf-8")
        corpus_text = render_corpus_markdown(meta, content)
        target_path = ROOT / meta.corpus_relpath
        target_path.write_text(corpus_text, encoding="utf-8")
        metas.append(meta)

    return metas


def sync_documents(metas: list[TranscriptMeta]) -> tuple[int, int]:
    tracked = tracked_document_paths()
    added = 0
    updated = 0

    for meta in metas:
        relpath = meta.corpus_relpath.as_posix()
        if relpath in tracked:
            run_bewley("update", relpath)
            updated += 1
        else:
            run_bewley("add", relpath)
            added += 1

    return added, updated


def ensure_codes(metas: list[TranscriptMeta]) -> int:
    present = existing_codes()
    needed = sorted({code for meta in metas for code in meta.codes})
    created = 0

    for code in needed:
        if code in present:
            continue
        run_bewley("code", "create", code)
        present.add(code)
        created += 1

    return created


def apply_document_codes(metas: list[TranscriptMeta]) -> int:
    active = existing_document_annotations()
    applied = 0

    for meta in metas:
        relpath = meta.corpus_relpath.as_posix()
        for code in meta.codes:
            key = (relpath, code)
            if key in active:
                continue
            memo = f"Initial import code for {meta.participant_name}"
            run_bewley("annotate", "apply", code, relpath, "--document", "--memo", memo)
            active.add(key)
            applied += 1

    return applied


def main() -> int:
    ensure_project()
    metas = write_corpus_documents()
    added, updated = sync_documents(metas)
    created_codes = ensure_codes(metas)
    applied_annotations = apply_document_codes(metas)

    print(f"Prepared {len(metas)} corpus documents")
    print(f"Added {added} document(s)")
    print(f"Updated {updated} document(s)")
    print(f"Created {created_codes} code(s)")
    print(f"Applied {applied_annotations} document-level annotation(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
