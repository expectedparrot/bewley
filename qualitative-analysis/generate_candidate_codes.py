#!/usr/bin/env python3
"""Generate candidate qualitative codes for a bewley corpus using EDSL.

Workflow:
  1. Agent creates corpus_summary.md (a direct agent task, done before this script).
  2. This script reads corpus_summary.md and all corpus documents.
  3. It builds an EDSL ScenarioList (one Scenario per document) and runs a
     Survey asking an LLM to suggest open codes for each document.
  4. Results are saved to candidate_codes.csv for review and refinement.

Usage:
  python generate_candidate_codes.py [--project-dir DIR] [--summary FILE]
                                     [--output FILE] [--model MODEL]
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def get_documents(project_dir: Path) -> list[dict[str, str]]:
    """Get document IDs and texts from a bewley project."""
    result = subprocess.run(
        ["bewley", "list", "documents"],
        capture_output=True, text=True, cwd=project_dir,
    )
    if result.returncode != 0:
        print(f"Error listing documents: {result.stderr}", file=sys.stderr)
        sys.exit(1)

    # bewley list documents outputs tab-separated lines: <id>\t<path>\t<revision>
    # Fall back to JSON parsing for older versions.
    documents = []
    raw = result.stdout.strip()
    if not raw:
        return documents
    try:
        parsed = json.loads(raw)
        rows = [(e["document_id"], e.get("path") or e.get("current_path")) for e in parsed]
    except (json.JSONDecodeError, KeyError):
        rows = []
        for line in raw.splitlines():
            parts = line.split("\t")
            if len(parts) >= 2:
                rows.append((parts[0].strip(), parts[1].strip()))
    for doc_id, doc_path in rows:
        # Read the document text
        full_path = project_dir / doc_path
        if not full_path.exists():
            print(f"Warning: {full_path} not found, skipping", file=sys.stderr)
            continue
        text = full_path.read_text(encoding="utf-8")
        documents.append({
            "document_id": doc_id,
            "document_path": doc_path,
            "document_text": text,
        })
    return documents


def _number_lines(text: str) -> str:
    """Prepend 1-based line numbers to each line of text."""
    lines = text.split("\n")
    return "\n".join(f"{i+1:>4}| {line}" for i, line in enumerate(lines))


def build_scenarios(documents: list[dict[str, str]], corpus_summary: str):
    """Build an EDSL ScenarioList from documents and corpus summary."""
    from edsl import Scenario, ScenarioList

    scenarios = []
    for doc in documents:
        scenarios.append(Scenario({
            "document_id": doc["document_id"],
            "document_path": doc["document_path"],
            "document_text": doc["document_text"],
            "document_text_numbered": _number_lines(doc["document_text"]),
            "corpus_summary": corpus_summary,
        }))
    return ScenarioList(scenarios)


def build_survey():
    """Build the EDSL Survey for open coding.

    Uses a single question that returns codes with verbatim quotes in one pass.
    The document is shown with line numbers so the LLM can reference specific
    passages, but we ask for the actual quoted text (not line numbers) because
    LLMs are unreliable at counting lines.
    """
    from edsl import Survey
    from edsl.questions import QuestionFreeText

    q_coding = QuestionFreeText(
        question_name="open_coding",
        question_text=(
            "You are a qualitative researcher performing open coding on a text corpus.\n\n"
            "## Corpus overview\n"
            "{{ corpus_summary }}\n\n"
            "## Document to code (with line numbers for reference)\n"
            "Path: {{ document_path }}\n\n"
            "{{ document_text_numbered }}\n\n"
            "## Task\n"
            "Suggest qualitative codes for this document. Return a JSON array of objects, "
            "each with three keys:\n"
            '- "code": a short analytic label (1-4 words, lowercase_with_underscores)\n'
            '- "description": a one-sentence description of what the code captures\n'
            '- "quote": the VERBATIM text from the document that supports this code. '
            "Copy the EXACT words as they appear above (including any typos, punctuation, "
            "or formatting). Do NOT paraphrase or summarize.\n\n"
            "Guidelines:\n"
            "- Aim for specificity: 'speed_punishment' is better than 'workload'\n"
            "- Prefer IN VIVO codes using the participant's own words when their language "
            "captures the concept precisely\n"
            "- Each quote should be the shortest passage that captures the coded concept\n"
            "- Suggest 3-15 codes per document\n\n"
            "Return ONLY a valid JSON array, no other text.\n\n"
            "Example:\n"
            '[{"code": "trust_building", "description": "Participant describes developing '
            'trust with supervisor", "quote": "after a few weeks I started to actually trust my supervisor"}, '
            '{"code": "emotional_labor", "description": "Managing emotions as part of work", '
            '"quote": "you have to smile even when customers are screaming at you"}]'
        ),
    )

    return Survey(questions=[q_coding])


def run_survey(survey, scenario_list, model_name: str | None = None):
    """Run the survey and return results."""
    from edsl import Model

    kwargs = {}
    if model_name:
        model = Model(model_name)
        kwargs["model"] = model

    results = survey.by(scenario_list).run(**kwargs)
    return results


def extract_candidate_codes(results) -> list[dict[str, str]]:
    """Extract candidate codes from EDSL results into a flat list."""
    import ast

    candidates = []
    parse_failures = 0
    for i in range(len(results)):
        row = results[i]
        doc_id = row.sub_dicts["scenario"]["document_id"]
        doc_path = row.sub_dicts["scenario"]["document_path"]
        raw = row.sub_dicts["answer"]["open_coding"]

        # Parse the JSON array of {code, description, quote} objects
        entries = []
        if isinstance(raw, list):
            entries = raw
        elif isinstance(raw, str):
            text = raw.strip()
            # Strip markdown code fences
            if text.startswith("```"):
                text = "\n".join(text.split("\n")[1:])
            if text.endswith("```"):
                text = "\n".join(text.split("\n")[:-1])
            text = text.strip()
            # Find the JSON array
            start = text.find("[")
            end = text.rfind("]") + 1
            if start >= 0 and end > start:
                fragment = text[start:end]
                try:
                    entries = json.loads(fragment)
                except json.JSONDecodeError:
                    try:
                        entries = ast.literal_eval(fragment)
                    except (ValueError, SyntaxError):
                        parse_failures += 1

        for entry in entries:
            if isinstance(entry, dict):
                candidates.append({
                    "code_name": entry.get("code", "").strip(),
                    "description": entry.get("description", ""),
                    "quote": entry.get("quote", ""),
                    "source_document_id": doc_id,
                    "source_document_path": doc_path,
                })

    if parse_failures:
        print(f"Warning: {parse_failures} documents had unparseable responses", file=sys.stderr)
    return candidates


def save_results(candidates: list[dict[str, str]], output_path: Path) -> None:
    """Save candidate codes to a CSV file."""
    import csv

    fieldnames = ["code_name", "description", "quote", "source_document_id", "source_document_path"]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(candidates)
    print(f"Wrote {len(candidates)} candidate codes to {output_path}")


def print_summary(candidates: list[dict[str, str]]) -> None:
    """Print a summary of candidate codes to stdout."""
    from collections import Counter
    code_counts = Counter(c["code_name"] for c in candidates)
    print(f"\n{'='*60}")
    print(f"Candidate codes: {len(code_counts)} unique across {len(candidates)} total")
    print(f"{'='*60}")
    print(f"\n{'Code':<40} {'Documents':>10}")
    print(f"{'-'*40} {'-'*10}")
    for code_name, count in code_counts.most_common():
        print(f"{code_name:<40} {count:>10}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate candidate qualitative codes for a bewley corpus using EDSL.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--project-dir", type=Path, default=Path("."),
        help="Path to the bewley project directory (default: current directory).",
    )
    parser.add_argument(
        "--summary", type=Path, default=Path("corpus_summary.md"),
        help="Path to the corpus summary file (default: corpus_summary.md).",
    )
    parser.add_argument(
        "--output", type=Path, default=Path("candidate_codes.csv"),
        help="Output CSV path (default: qualitative-analysis/candidate_codes.csv).",
    )
    parser.add_argument(
        "--model", type=str, default=None,
        help="EDSL model to use (e.g., 'claude-3-5-sonnet-20241022'). Defaults to EDSL default.",
    )
    args = parser.parse_args()

    project_dir = args.project_dir.resolve()

    # Read corpus summary
    summary_path = project_dir / args.summary if not args.summary.is_absolute() else args.summary
    if not summary_path.exists():
        print(
            f"Error: {summary_path} not found.\n"
            "Create a corpus summary first (e.g., have an agent read all documents\n"
            "and write a summary to this file).",
            file=sys.stderr,
        )
        sys.exit(1)
    corpus_summary = summary_path.read_text(encoding="utf-8")

    # Get documents
    documents = get_documents(project_dir)
    if not documents:
        print("Error: no documents found in project.", file=sys.stderr)
        sys.exit(1)
    print(f"Found {len(documents)} documents")

    # Build EDSL components
    scenario_list = build_scenarios(documents, corpus_summary)
    survey = build_survey()

    # Run
    print("Running EDSL survey for open coding...")
    results = run_survey(survey, scenario_list, model_name=args.model)

    # Extract and save
    candidates = extract_candidate_codes(results)
    output_path = project_dir / args.output if not args.output.is_absolute() else args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    save_results(candidates, output_path)
    print_summary(candidates)


if __name__ == "__main__":
    main()
