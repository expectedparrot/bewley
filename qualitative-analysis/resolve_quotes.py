#!/usr/bin/env python3
"""Resolve quoted text to byte/line ranges in bewley documents.

Takes a CSV of candidate codes with a 'quote' column and a bewley project,
finds where each quote appears in the source document, and outputs the same
CSV with start_byte, end_byte, start_line, end_line columns filled in.

Matching strategy (tried in order):
  1. Exact substring match
  2. Normalized match (collapse whitespace, strip punctuation edges)
  3. Longest common substring (if >= 60% of quote length)

Unresolved quotes are written to a separate file for manual review.

Usage:
  python resolve_quotes.py candidate_codes.csv \\
      --project-dir /path/to/qualitative-coding \\
      --output candidate_codes_resolved.csv \\
      --unresolved unresolved_quotes.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
from pathlib import Path


def _normalize(s: str) -> str:
    """Collapse whitespace, lowercase, strip outer punctuation."""
    s = re.sub(r"\s+", " ", s).strip().lower()
    s = s.strip(".,;:!?\"'()-")
    return s


def _longest_common_substring(a: str, b: str) -> tuple[int, int, int]:
    """Return (length, start_in_a, start_in_b) of longest common substring."""
    m, n = len(a), len(b)
    # Use rolling-row DP to save memory
    prev = [0] * (n + 1)
    best_len = 0
    best_i = 0
    best_j = 0
    for i in range(1, m + 1):
        curr = [0] * (n + 1)
        for j in range(1, n + 1):
            if a[i - 1] == b[j - 1]:
                curr[j] = prev[j - 1] + 1
                if curr[j] > best_len:
                    best_len = curr[j]
                    best_i = i - best_len
                    best_j = j - best_len
            else:
                curr[j] = 0
        prev = curr
    return best_len, best_i, best_j


def _byte_offset_to_lines(text: str, start_byte: int, end_byte: int) -> tuple[int, int]:
    """Convert byte offsets to 1-based inclusive line numbers."""
    encoded = text.encode("utf-8")
    # Count newlines before start_byte to get start_line
    start_line = encoded[:start_byte].count(b"\n") + 1
    # Count newlines before end_byte to get end_line
    end_line = encoded[:end_byte].count(b"\n") + 1
    return start_line, end_line


def resolve_quote(quote: str, doc_text: str, min_lcs_ratio: float = 0.6) -> dict | None:
    """Try to find the quote in the document text.

    Returns dict with start_byte, end_byte, start_line, end_line, match_type
    or None if no match found.
    """
    if not quote or not doc_text:
        return None

    # Strategy 1: Exact substring
    idx = doc_text.find(quote)
    if idx >= 0:
        sb = len(doc_text[:idx].encode("utf-8"))
        eb = sb + len(quote.encode("utf-8"))
        sl, el = _byte_offset_to_lines(doc_text, sb, eb)
        return {
            "start_byte": sb, "end_byte": eb,
            "start_line": sl, "end_line": el,
            "match_type": "exact",
        }

    # Strategy 2: Normalized match
    norm_quote = _normalize(quote)
    norm_doc = _normalize(doc_text)
    idx = norm_doc.find(norm_quote)
    if idx >= 0 and len(norm_quote) >= 10:
        # Map back to original text position.
        # Walk through original doc matching normalized characters.
        norm_pos = 0
        orig_start = None
        orig_end = None
        oi = 0
        di = 0
        orig_chars = list(doc_text)
        norm_chars = list(_normalize(doc_text))

        # Simpler approach: search original text case-insensitively with collapsed whitespace
        pattern = re.escape(norm_quote)
        pattern = pattern.replace(r"\ ", r"\s+")
        m = re.search(pattern, doc_text, re.IGNORECASE)
        if m:
            matched_text = m.group(0)
            start = m.start()
            sb = len(doc_text[:start].encode("utf-8"))
            eb = sb + len(matched_text.encode("utf-8"))
            sl, el = _byte_offset_to_lines(doc_text, sb, eb)
            return {
                "start_byte": sb, "end_byte": eb,
                "start_line": sl, "end_line": el,
                "match_type": "normalized",
            }

    # Strategy 3: Longest common substring
    if len(quote) >= 20:
        lcs_len, _, lcs_start_in_doc = _longest_common_substring(
            doc_text.lower(), quote.lower()
        )
        if lcs_len >= len(quote) * min_lcs_ratio and lcs_len >= 15:
            # Find the actual match in original case
            matched = doc_text[lcs_start_in_doc:lcs_start_in_doc + lcs_len]
            sb = len(doc_text[:lcs_start_in_doc].encode("utf-8"))
            eb = sb + len(matched.encode("utf-8"))
            sl, el = _byte_offset_to_lines(doc_text, sb, eb)
            return {
                "start_byte": sb, "end_byte": eb,
                "start_line": sl, "end_line": el,
                "match_type": f"lcs({lcs_len}/{len(quote)})",
            }

    return None


def get_document_texts(project_dir: Path) -> dict[str, str]:
    """Load all document texts from a bewley project, keyed by corpus path."""
    result = subprocess.run(
        ["bewley", "list", "documents"],
        capture_output=True, text=True, cwd=project_dir,
    )
    if result.returncode != 0:
        print(f"Error listing documents: {result.stderr}", file=sys.stderr)
        sys.exit(1)

    raw = result.stdout.strip()
    texts = {}
    if not raw:
        return texts
    try:
        parsed = json.loads(raw)
        rows = [(e.get("path") or e.get("current_path"),) for e in parsed]
        paths = [r[0] for r in rows]
    except (json.JSONDecodeError, KeyError):
        paths = []
        for line in raw.splitlines():
            parts = line.split("\t")
            if len(parts) >= 2:
                paths.append(parts[1].strip())
    for doc_path in paths:
        if not doc_path:
            continue
        full_path = project_dir / doc_path
        if full_path.exists():
            texts[doc_path] = full_path.read_text(encoding="utf-8")
    return texts


def main():
    parser = argparse.ArgumentParser(
        description="Resolve quoted text to byte/line ranges in bewley documents.",
    )
    parser.add_argument("csv_path", type=Path, help="Input CSV with candidate codes")
    parser.add_argument("--project-dir", type=Path, default=Path("."),
                        help="Path to bewley project directory")
    parser.add_argument("-o", "--output", type=Path, required=True,
                        help="Output CSV with resolved ranges")
    parser.add_argument("--unresolved", type=Path, default=None,
                        help="Output CSV for unresolved quotes (default: <output>_unresolved.csv)")
    parser.add_argument("--min-lcs-ratio", type=float, default=0.6,
                        help="Minimum LCS length as fraction of quote length (default: 0.6)")
    args = parser.parse_args()

    # Load document texts
    doc_texts = get_document_texts(args.project_dir.resolve())
    print(f"Loaded {len(doc_texts)} documents")

    # Read input CSV
    rows = []
    with open(args.csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        input_fields = reader.fieldnames
        rows = list(reader)
    print(f"Read {len(rows)} candidate codes")

    if "quote" not in input_fields:
        print("ERROR: Input CSV must have a 'quote' column", file=sys.stderr)
        sys.exit(1)

    # Resolve quotes
    resolved = []
    unresolved = []
    for row in rows:
        quote = row.get("quote", "").strip()
        doc_path = row.get("source_document_path", "")
        doc_text = doc_texts.get(doc_path, "")

        match = resolve_quote(quote, doc_text, min_lcs_ratio=args.min_lcs_ratio)
        if match:
            row["start_byte"] = match["start_byte"]
            row["end_byte"] = match["end_byte"]
            row["start_line"] = match["start_line"]
            row["end_line"] = match["end_line"]
            row["match_type"] = match["match_type"]
            resolved.append(row)
        else:
            row["start_byte"] = ""
            row["end_byte"] = ""
            row["start_line"] = ""
            row["end_line"] = ""
            row["match_type"] = "UNRESOLVED"
            unresolved.append(row)

    # Write resolved output
    out_fields = [f for f in input_fields if f not in ("start_line", "end_line")]
    out_fields += ["start_byte", "end_byte", "start_line", "end_line", "match_type"]
    # Deduplicate field names while preserving order
    seen = set()
    out_fields = [f for f in out_fields if f not in seen and not seen.add(f)]

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(resolved)

    print(f"\nResolved: {len(resolved)}/{len(rows)}")
    print(f"Unresolved: {len(unresolved)}/{len(rows)}")
    print(f"Wrote {args.output}")

    # Write unresolved
    if unresolved:
        unresolved_path = args.unresolved or args.output.with_name(
            args.output.stem + "_unresolved" + args.output.suffix
        )
        with open(unresolved_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=out_fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(unresolved)
        print(f"Wrote unresolved to {unresolved_path}")

    # Print match type breakdown
    from collections import Counter
    types = Counter(r.get("match_type", "UNRESOLVED") for r in resolved)
    print(f"\nMatch types: {dict(types)}")


if __name__ == "__main__":
    main()
