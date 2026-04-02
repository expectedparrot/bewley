#!/usr/bin/env python3

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert all .docx files in a directory tree to Markdown using pandoc."
    )
    parser.add_argument(
        "input_dir",
        nargs="?",
        default="Transcripts",
        help="Directory to scan for .docx files. Defaults to ./Transcripts",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        help=(
            "Directory for generated .md files. If omitted, files are written next to "
            "their source documents."
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing .md files.",
    )
    return parser.parse_args()


def ensure_pandoc() -> None:
    if shutil.which("pandoc"):
        return
    print("Error: pandoc is not installed or not on PATH.", file=sys.stderr)
    sys.exit(1)


def relative_markdown_path(docx_path: Path, input_root: Path) -> Path:
    return docx_path.relative_to(input_root).with_suffix(".md")


def convert_file(docx_path: Path, md_path: Path) -> None:
    md_path.parent.mkdir(parents=True, exist_ok=True)
    media_dir_name = f"{md_path.stem}_media"
    subprocess.run(
        [
            "pandoc",
            str(docx_path),
            "-f",
            "docx",
            "-t",
            "gfm",
            "--extract-media",
            media_dir_name,
            "-o",
            md_path.name,
        ],
        check=True,
        cwd=md_path.parent,
    )


def main() -> int:
    args = parse_args()
    ensure_pandoc()

    input_root = Path(args.input_dir).expanduser().resolve()
    if not input_root.exists() or not input_root.is_dir():
        print(f"Error: input directory not found: {input_root}", file=sys.stderr)
        return 1

    output_root = Path(args.output_dir).expanduser().resolve() if args.output_dir else None
    docx_files = sorted(input_root.rglob("*.docx"))

    if not docx_files:
        print(f"No .docx files found under {input_root}")
        return 0

    converted = 0
    skipped = 0

    for docx_path in docx_files:
        relative_md = relative_markdown_path(docx_path, input_root)
        md_path = (output_root / relative_md) if output_root else docx_path.with_suffix(".md")

        if md_path.exists() and not args.overwrite:
            print(f"Skipping existing file: {md_path}")
            skipped += 1
            continue

        try:
            convert_file(docx_path, md_path)
            print(f"Converted: {docx_path} -> {md_path}")
            converted += 1
        except subprocess.CalledProcessError as exc:
            print(f"Failed: {docx_path} ({exc})", file=sys.stderr)
            return exc.returncode or 1

    print(f"Done. Converted {converted} file(s); skipped {skipped} existing file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
