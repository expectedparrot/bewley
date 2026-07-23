"""Package and ingest agent-run EDSL open-coding work."""
from __future__ import annotations

import ast
import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Optional

import typer

from bewley.commands.common import HumanOption, action, fail, finish, get_project, should_emit_json
from bewley.project import BewleyError, safe_decode


app = typer.Typer(help="Build open-coding Jobs packages and ingest EDSL Results.")

QUESTION_NAME = "open_coding"
QUESTION_TEXT = """You are open-coding one document in a qualitative corpus.

Corpus context:
{{ corpus_summary }}

Document (line-numbered for orientation):
{{ document_text_numbered }}

Return only a JSON array containing 3–15 objects. Each object must have:
- "code": a concise snake_case conceptual code
- "description": one sentence defining the code
- "quote": an exact, verbatim quotation copied from the document

Prefer analytically meaningful concepts over topic labels. Do not invent or
normalize quotation text. Every quote must occur exactly in the document."""


def _path(project_root: Path, value: Path) -> Path:
    return value if value.is_absolute() else project_root / value


def _edsl() -> tuple[Any, Any, Any, Any, Any]:
    try:
        from edsl import Jobs, QuestionFreeText, Results, Scenario, ScenarioList
    except ImportError as exc:
        raise BewleyError(
            "EDSL is required for open-coding Jobs and Results.",
            code="MISSING_DEPENDENCY",
            hint="Install Bewley with its declared dependencies and retry.",
        ) from exc
    return Jobs, QuestionFreeText, Results, Scenario, ScenarioList


def _result_value(result: Any, group: str, key: str) -> Any:
    try:
        value = result[group]
        if isinstance(value, dict):
            return value.get(key)
        return getattr(value, key, None)
    except (KeyError, TypeError):
        return None


def _scenario_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else dict(value)


def _scenario_key(scenario: dict[str, Any]) -> tuple[str, str]:
    return str(scenario.get("document_id")), str(scenario.get("revision_id"))


def _parse_answer(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, list):
        parsed = raw
    elif isinstance(raw, str):
        text = raw.strip()
        if text.startswith("```"):
            text = "\n".join(text.splitlines()[1:])
        if text.rstrip().endswith("```"):
            text = "\n".join(text.splitlines()[:-1])
        start, end = text.find("["), text.rfind("]")
        if start < 0 or end < start:
            raise ValueError("answer does not contain a JSON array")
        fragment = text[start : end + 1]
        try:
            parsed = json.loads(fragment)
        except json.JSONDecodeError:
            parsed = ast.literal_eval(fragment)
    else:
        raise ValueError("answer is neither a list nor text")
    if not isinstance(parsed, list):
        raise ValueError("answer is not a list")
    entries = []
    for entry in parsed:
        if not isinstance(entry, dict):
            raise ValueError("answer contains a non-object entry")
        if not all(isinstance(entry.get(k), str) and entry[k].strip() for k in ("code", "description", "quote")):
            raise ValueError("each entry requires non-empty code, description, and quote strings")
        entries.append(entry)
    return entries


def _resolve_quote(text: str, quote: str) -> tuple[str, Optional[int], Optional[int]]:
    first = text.find(quote)
    if first < 0:
        return "not_found", None, None
    if text.find(quote, first + 1) >= 0:
        return "ambiguous", None, None
    start = len(text[:first].encode("utf-8"))
    return "exact", start, start + len(quote.encode("utf-8"))


@app.command("jobs")
def jobs_command(
    output: Path = typer.Option(Path("jobs.ep"), "--output", "-o"),
    summary: Path = typer.Option(
        Path("qualitative-analysis/corpus_summary.md"), "--summary",
        help="Corpus context embedded in every scenario; optional if absent.",
    ),
    pilot: Optional[int] = typer.Option(None, "--pilot", min=1),
    force: bool = typer.Option(False, "--force", help="Replace an existing Jobs package."),
    human: bool = HumanOption,
) -> None:
    """Build an EDSL Jobs package; execute it separately with the ep CLI."""
    json_flag = should_emit_json(human)
    command = "open-coding jobs"
    project = get_project(command, json_flag)
    target = _path(project.root, output)
    try:
        if target.suffix != ".ep":
            raise BewleyError("--output must use the .ep extension", code="VALIDATION_ERROR")
        if target.exists() and not force:
            raise BewleyError(f"{target} already exists", code="ALREADY_EXISTS", hint="Use --force to replace it.")
        Jobs, QuestionFreeText, _, Scenario, ScenarioList = _edsl()
        summary_path = _path(project.root, summary)
        corpus_summary = summary_path.read_text(encoding="utf-8") if summary_path.exists() else ""
        scenarios = []
        with project.connect() as conn:
            documents = conn.execute(
                "SELECT document_id, current_path FROM documents WHERE archived_at IS NULL ORDER BY current_path"
            ).fetchall()
            for document in documents:
                revision = project.current_revision(conn, document["document_id"])
                text = safe_decode((project.objects_dir / revision["content_sha256"]).read_bytes())
                scenarios.append(Scenario({
                    "document_id": document["document_id"],
                    "document_path": document["current_path"],
                    "revision_id": revision["revision_id"],
                    "content_sha256": revision["content_sha256"],
                    "document_text": text,
                    "document_text_numbered": "\n".join(
                        f"{number:>6}  {line}" for number, line in enumerate(text.splitlines(), 1)
                    ),
                    "corpus_summary": corpus_summary,
                }))
        if pilot is not None:
            scenarios = scenarios[:pilot]
        if not scenarios:
            raise BewleyError("The project has no active documents.", code="EMPTY_CORPUS")
        question = QuestionFreeText(question_name=QUESTION_NAME, question_text=QUESTION_TEXT)
        job = Jobs(survey=question.to_survey()).by(ScenarioList(scenarios))
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            target.unlink()
        saved = job.git.save(target)
        expected = target.with_name("results.ep")
        data = {
            "object_type": "Jobs",
            "output": str(target),
            "question": QUESTION_NAME,
            "scenario_count": len(scenarios),
            "pilot": pilot is not None,
            "saved": saved,
            "expected_results": str(expected),
            "answer_contract": {
                "type": "json_array",
                "item_required_keys": ["code", "description", "quote"],
                "quote_policy": "exact_verbatim",
            },
        }
    except (BewleyError, OSError) as exc:
        err = exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="IO_ERROR")
        fail(command, err, json_flag)
        return
    next_action = action(
        "run-open-coding-jobs", "Run the packaged jobs with EDSL",
        ["ep", "run", str(target), "--model", "<model-name>", "--output", str(expected)],
        mutates_state=True, requires_network=True,
    )
    if json_flag:
        finish(command, data, next_actions=[next_action])
    else:
        print(f"Jobs written to: {target}")
        print("Run: " + " ".join(next_action["command"]))


@app.command("ingest")
def ingest_command(
    results_path: Path = typer.Argument(..., help="Results .ep file produced by `ep run`."),
    jobs_path: Optional[Path] = typer.Option(None, "--jobs", help="Originating Jobs package for coverage audit."),
    output: Path = typer.Option(Path("qualitative-analysis/candidate_codes.csv"), "--output", "-o"),
    allow_partial: bool = typer.Option(False, "--allow-partial"),
    force: bool = typer.Option(False, "--force"),
    human: bool = HumanOption,
) -> None:
    """Audit Results and write exact-quote candidate codes for human review."""
    json_flag = should_emit_json(human)
    command = "open-coding ingest"
    project = get_project(command, json_flag)
    source = _path(project.root, results_path)
    jobs_source = _path(project.root, jobs_path) if jobs_path else None
    target = _path(project.root, output)
    try:
        Jobs, _, Results, _, _ = _edsl()
        if not source.exists():
            raise BewleyError(f"{source} does not exist", code="NOT_FOUND")
        if target.exists() and not force:
            raise BewleyError(f"{target} already exists", code="ALREADY_EXISTS", hint="Use --force to replace it.")
        expected: set[tuple[str, str]] = set()
        if jobs_source is not None:
            if not jobs_source.exists():
                raise BewleyError(f"{jobs_source} does not exist", code="NOT_FOUND")
            jobs = Jobs.git.load(jobs_source)
            expected = {_scenario_key(_scenario_dict(item)) for item in jobs.scenarios}
        results = Results.git.load(source)
        rows: list[dict[str, Any]] = []
        returned: list[tuple[str, str]] = []
        failures: list[dict[str, Any]] = []
        stale = unresolved = 0
        with project.connect() as conn:
            for index, result in enumerate(results):
                scenario = _scenario_dict(result["scenario"])
                key = _scenario_key(scenario)
                returned.append(key)
                exception = _result_value(result, "exceptions", QUESTION_NAME)
                raw = _result_value(result, "answer", QUESTION_NAME)
                try:
                    if exception:
                        raise ValueError("model exception")
                    entries = _parse_answer(raw)
                    document = project.resolve_document(conn, str(scenario.get("document_id")))
                    revision = project.current_revision(conn, document["document_id"])
                    if revision["revision_id"] != scenario.get("revision_id") or revision["content_sha256"] != scenario.get("content_sha256"):
                        stale += 1
                        raise ValueError("document revision no longer matches the packaged scenario")
                    text = safe_decode((project.objects_dir / revision["content_sha256"]).read_bytes())
                    for entry_index, entry in enumerate(entries):
                        status, start, end = _resolve_quote(text, entry["quote"])
                        if status != "exact":
                            unresolved += 1
                        candidate_id = hashlib.sha256(
                            f"{key[0]}:{key[1]}:{entry_index}:{entry['code']}:{entry['quote']}".encode()
                        ).hexdigest()[:16]
                        rows.append({
                            "candidate_id": candidate_id,
                            "code_name": entry["code"].strip(),
                            "description": entry["description"].strip(),
                            "quote": entry["quote"],
                            "source_document_id": key[0],
                            "source_document_path": scenario.get("document_path", ""),
                            "source_revision_id": key[1],
                            "byte_start": "" if start is None else start,
                            "byte_end": "" if end is None else end,
                            "resolve_status": status,
                        })
                except (ValueError, TypeError, KeyError, BewleyError) as exc:
                    failures.append({"index": index, "document_id": key[0], "error": str(exc)})
        missing = expected - set(returned) if expected else set()
        duplicates = len(returned) - len(set(returned))
        incomplete = bool(failures or missing or duplicates)
        if incomplete and not allow_partial:
            raise BewleyError(
                "Results failed validation; no candidate CSV was written.",
                code="INCOMPLETE_RESULTS",
                context={"failures": failures[:10], "missing_scenarios": len(missing), "duplicate_scenarios": duplicates},
                hint="Correct/rerun the results, or use --allow-partial to ingest valid rows.",
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            "candidate_id", "code_name", "description", "quote", "source_document_id",
            "source_document_path", "source_revision_id", "byte_start", "byte_end", "resolve_status",
        ]
        with target.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        data = {
            "object_type": "CandidateCodes",
            "output": str(target),
            "candidate_count": len(rows),
            "scenario_count": len(returned),
            "expected_scenarios": len(expected) if expected else None,
            "missing_scenarios": len(missing) if expected else None,
            "duplicate_scenarios": duplicates,
            "failed_scenarios": len(failures),
            "stale_scenarios": stale,
            "unresolved_quotes": unresolved,
            "partial": incomplete,
        }
    except (BewleyError, OSError) as exc:
        err = exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="IO_ERROR")
        fail(command, err, json_flag)
        return
    if json_flag:
        finish(command, data)
    else:
        print(f"Wrote {len(rows)} candidates to: {target}")
