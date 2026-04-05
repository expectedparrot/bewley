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

    documents = []
    for line in result.stdout.strip().splitlines():
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        doc_id, doc_path = parts[0], parts[1]
        # Read the document text from corpus/
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


def build_scenarios(documents: list[dict[str, str]], corpus_summary: str):
    """Build an EDSL ScenarioList from documents and corpus summary."""
    from edsl import Scenario, ScenarioList

    scenarios = []
    for doc in documents:
        scenarios.append(Scenario({
            "document_id": doc["document_id"],
            "document_path": doc["document_path"],
            "document_text": doc["document_text"],
            "corpus_summary": corpus_summary,
        }))
    return ScenarioList(scenarios)


def build_survey():
    """Build the EDSL Survey for open coding."""
    from edsl import Survey
    from edsl.questions import QuestionList, QuestionFreeText

    q_codes = QuestionList(
        question_name="candidate_codes",
        question_text=(
            "You are a qualitative researcher performing open coding on a text corpus.\n\n"
            "## Corpus overview\n"
            "{{ corpus_summary }}\n\n"
            "## Document to code\n"
            "Path: {{ document_path }}\n\n"
            "{{ document_text }}\n\n"
            "## Task\n"
            "Suggest qualitative codes for this document. Each code should be a short, "
            "analytic label (1-4 words) that captures a concept, theme, process, or "
            "pattern present in the text. Use lowercase with underscores for multi-word "
            "codes (e.g., trust_building, emotional_labor, power_dynamics).\n\n"
            "Aim for specificity over generality. Prefer codes grounded in the data "
            "rather than pre-existing theoretical categories."
        ),
        max_list_items=20,
        min_list_items=3,
    )

    q_descriptions = QuestionFreeText(
        question_name="code_descriptions",
        question_text=(
            "For each qualitative code you proposed:\n"
            "{{ candidate_codes.answer }}\n\n"
            "Provide a JSON object mapping each code name to a one-sentence "
            "description of what it captures. Return ONLY valid JSON, no other text.\n\n"
            "Example format:\n"
            '{"trust_building": "Instances where participants describe developing '
            'trust with others", "emotional_labor": "Passages about managing '
            'emotions as part of work duties"}'
        ),
    )

    return Survey(questions=[q_codes, q_descriptions])


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
    candidates = []
    for i in range(len(results)):
        row = results[i]
        doc_id = row["scenario.document_id"]
        doc_path = row["scenario.document_path"]
        codes = row["answer.candidate_codes"]
        raw_descriptions = row["answer.code_descriptions"]

        # Parse descriptions JSON (best-effort)
        descriptions = {}
        if isinstance(raw_descriptions, str):
            # Try to extract JSON from the response
            try:
                descriptions = json.loads(raw_descriptions)
            except json.JSONDecodeError:
                # Try to find JSON within the text
                start = raw_descriptions.find("{")
                end = raw_descriptions.rfind("}") + 1
                if start >= 0 and end > start:
                    try:
                        descriptions = json.loads(raw_descriptions[start:end])
                    except json.JSONDecodeError:
                        pass

        if isinstance(codes, list):
            for code_name in codes:
                candidates.append({
                    "code_name": code_name.strip(),
                    "description": descriptions.get(code_name.strip(), ""),
                    "source_document_id": doc_id,
                    "source_document_path": doc_path,
                })
    return candidates


def save_results(candidates: list[dict[str, str]], output_path: Path) -> None:
    """Save candidate codes to a CSV file."""
    import csv

    fieldnames = ["code_name", "description", "source_document_id", "source_document_path"]
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
        "--summary", type=Path, default=Path("qualitative-analysis/corpus_summary.md"),
        help="Path to the corpus summary file (default: qualitative-analysis/corpus_summary.md).",
    )
    parser.add_argument(
        "--output", type=Path, default=Path("qualitative-analysis/candidate_codes.csv"),
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
