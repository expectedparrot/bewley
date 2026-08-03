"""Rapid, auditable survey insights from AI-interviewer transcripts."""
from __future__ import annotations

import html
import hashlib
import json
import math
import random
import re
from collections import Counter
from pathlib import Path
from typing import Any, Optional

import typer

from bewley.commands.common import HumanOption, action, fail, finish, get_project, should_emit_json
from bewley.project import BewleyError, utcnow
from bewley.util import safe_decode

app = typer.Typer(help="Build auditable qualitative insight workflows and deterministic reports.")
discover_app = typer.Typer(help="Discover candidate codes from reproducible bundles of responses.")
consolidate_app = typer.Typer(help="Consolidate discovered candidates into a compact frozen codebook.")
classify_app = typer.Typer(help="Classify every response against a frozen feedback codebook.")
app.add_typer(discover_app, name="discover")
app.add_typer(consolidate_app, name="consolidate")
app.add_typer(classify_app, name="classify")

QUESTION_NAME = "rapid_insights"
SENTIMENTS = {"positive", "negative", "mixed", "neutral", "not_applicable"}
QUESTION_TEXT = """Analyze only the supplied respondent statements from one AI-led interview.
Interviewer prompts, identifiers, metadata, and post-interview interface feedback have
already been excluded.

Return one JSON object with:
- "summary": 1-3 sentences describing this respondent's substantive answer
- "sentiment": {"label": positive|negative|mixed|neutral|not_applicable,
  "score": number from -1 to 1, "confidence": number from 0 to 1}
- "themes": up to 5 objects with "code_id", "confidence", and "rationale"
- "standout_quotes": up to 2 objects with "exact_text" and "rationale"

Sentiment means the respondent's stance toward AI's effect on their work, not their
general mood. Use not_applicable when no such stance is expressed. Assign only theme
code_ids from the fixed framework. A standout quote must be a verbatim contiguous
substring of respondent_text; prefer concise, vivid, representative statements.
Never invent text or ids. Return JSON only.

Fixed themes:
{{ themes_json }}

Respondent text:
{{ respondent_text }}"""

FEEDBACK_QUESTION_NAME = "interviewer_feedback_insights"
FEEDBACK_QUESTION_TEXT = """Analyze this complete set of free-text feedback about an AI interviewer.

Return one JSON object with:
- "overall_summary": a concise 2-4 sentence synthesis
- "themes": 6-12 distinct objects with "theme_key" (lowercase snake_case),
  "name", "description", and integer "response_count" estimating how many
  responses substantively express that theme
- "sentiment_distribution": an object with integer positive, negative, mixed,
  and neutral counts that sum to the supplied response count
- "standout_quotes": up to 12 objects with "document_id", "exact_text",
  "theme_keys", and "rationale"

Themes should describe the interview experience itself: usability, pacing, voice,
question quality, conversational flow, technical problems, or other patterns actually
present. Do not analyze respondents' occupations or attitudes toward AI at work.
Do not invent text, ids, or themes unsupported by the response set. Return JSON only.

Feedback responses:
{{ feedback_json }}"""

DISCOVERY_QUESTION_NAME = "feedback_code_discovery"
DISCOVERY_QUESTION_TEXT = """Discover recurring qualitative codes in this bundle of short
feedback responses. Work across responses: do not create a separate code for every phrase.

Return only a JSON array of candidate-code objects with:
- "code_key": lowercase snake_case
- "name": concise human-readable label
- "description": what the pattern means
- "inclusion_criteria" and "exclusion_criteria"
- "evidence": 2-5 objects containing "source_id" and "exact_text"

Requirements:
- Propose only patterns supported by at least two different source_ids in this bundle.
- Consolidate synonymous wording into one candidate.
- Distinguish praise, criticism, concrete usability problems, and suggestions where supported.
- Treat empty, placeholder, or no-comment answers as data quality, not substantive feedback.
- Every exact_text must be a verbatim contiguous substring of that source's response.
- Never estimate corpus prevalence and never invent source ids or text.

Bundle:
{{ bundle_text }}"""

CONSOLIDATION_QUESTION_NAME = "feedback_code_consolidation"
CONSOLIDATION_QUESTION_TEXT = """Consolidate this complete inventory of validated candidate
codes into a compact codebook for classifying individual AI-interviewer feedback responses.

Return only one JSON object with:
- "themes": 4-10 objects with theme_key, name, and description
- "codes": {{ min_codes }}-{{ max_codes }} objects with code_key, theme_key, name,
  description, inclusion_criteria, and exclusion_criteria

Requirements:
- Merge synonyms and near-synonyms while preserving materially different praise,
  criticism, usability problems, and suggestions.
- Include a code for non-substantive/no-comment responses when supported.
- Codes must be suitable for exhaustive response-level classification.
- Do not report prevalence or invent quotations.

Validated candidates:
{{ candidates_json }}

Correction context from a previously rejected draft (empty on the first run):
{{ correction_context }}"""

CLASSIFICATION_QUESTION_NAME = "feedback_classification"
CLASSIFICATION_QUESTION_TEXT = """Classify this one AI-interviewer feedback response against
the fixed codebook. Return only one JSON object with:
- "sentiment": positive|negative|mixed|neutral|not_applicable
- "assignments": array of objects with code_key, exact_text, confidence (0-1)
- "potential_new_theme": null or a concise description of substantively important feedback
  that no code covers

Assign every supported code, but never assign a code without exact evidence. exact_text must
be a verbatim contiguous substring of response_text. Use the explicit no-comment/data-quality
code for placeholders when present. Do not invent codes, text, or aggregate counts.

Frozen codebook (fingerprint {{ codebook_fingerprint }}):
{{ codebook_json }}

Response:
{{ response_text }}"""


def _edsl():
    try:
        from edsl import Jobs, Model, ModelList, QuestionFreeText, Results, Scenario, ScenarioList
    except ImportError as exc:
        raise BewleyError("EDSL is required for rapid insights", code="DEPENDENCY_MISSING") from exc
    return Jobs, Model, ModelList, QuestionFreeText, Results, Scenario, ScenarioList


def _configured_model(Model, name: str, max_tokens: int):
    service = "openai" if name.startswith(("gpt-", "o1", "o3", "o4")) else None
    return Model(name, service_name=service, max_tokens=max_tokens, reasoning_effort="low")


def _save_model_list_json(Model, ModelList, name: str, max_tokens: int, target: Path, force: bool) -> Path:
    if target.exists() and not force:
        raise BewleyError(f"{target} already exists", code="ALREADY_EXISTS")
    target.parent.mkdir(parents=True, exist_ok=True)
    models = ModelList([_configured_model(Model, name, max_tokens)])
    target.write_text(json.dumps(models.to_dict(), indent=2) + "\n", encoding="utf-8")
    return target


def _path(root: Path, value: Path) -> Path:
    return value if value.is_absolute() else root / value


def _respondent_text(text: str) -> str:
    """Keep interview-body respondent turns; exclude prompts and feedback section."""
    output: list[str] = []
    current: str | None = None
    for line in text.splitlines():
        stripped = line.strip()
        if "[Feedback about the AI interviewer]" in stripped:
            break
        match = re.match(r"^([A-Z][A-Z _-]*):\s*(.*)$", stripped)
        if match:
            current = match.group(1)
            if current == "RESPONDENT" and match.group(2):
                output.append(match.group(2))
        elif current == "RESPONDENT" and stripped:
            output.append(stripped)
    return "\n".join(output).strip()


def _feedback_text(text: str) -> str:
    lines = text.splitlines()
    marker = next((i for i, line in enumerate(lines) if "[Feedback about the AI interviewer]" in line), None)
    if marker is None:
        return ""
    output = []
    for line in lines[marker + 1:]:
        stripped = re.sub(r"^RESPONDENT:\s*", "", line.strip())
        if stripped:
            output.append(stripped)
    return "\n".join(output).strip()


def _feedback_documents(project) -> list[dict[str, str]]:
    with project.connect() as conn:
        rows = conn.execute("SELECT document_id, current_path FROM documents ORDER BY current_path").fetchall()
        output = []
        for row in rows:
            revision = project.current_revision(conn, row["document_id"])
            text = safe_decode((project.objects_dir / revision["content_sha256"]).read_bytes())
            feedback = _feedback_text(text)
            if feedback:
                output.append({
                    "document_id": row["document_id"], "document_path": row["current_path"],
                    "revision_id": revision["revision_id"], "feedback_text": feedback,
                })
    return output


def _themes(project) -> list[dict[str, Any]]:
    project.ensure_db()
    with project.connect() as conn:
        rows = conn.execute(
            """SELECT c.code_id, c.canonical_name, c.description,
                      c.inclusion_criteria, c.exclusion_criteria,
                      p.canonical_name theme_name
               FROM codes c LEFT JOIN codes p ON p.code_id = c.parent_code_id
               WHERE c.status='active' AND c.code_layer='focused'
               ORDER BY c.canonical_name"""
        ).fetchall()
    if not rows:
        raise BewleyError(
            "Rapid insights requires an applied focused codebook.", code="MISSING_FOCUSED_CODEBOOK",
            hint="Run `bewley codebook focused framework-jobs` first.",
        )
    return [dict(row) for row in rows]


def _documents(project) -> list[dict[str, str]]:
    with project.connect() as conn:
        rows = conn.execute("SELECT document_id, current_path FROM documents ORDER BY current_path").fetchall()
        output = []
        for row in rows:
            revision = project.current_revision(conn, row["document_id"])
            text = safe_decode((project.objects_dir / revision["content_sha256"]).read_bytes())
            respondent = _respondent_text(text)
            if respondent:
                output.append({
                    "document_id": row["document_id"], "document_path": row["current_path"],
                    "revision_id": revision["revision_id"], "respondent_text": respondent,
                })
    return output


def _plain_feedback_documents(project) -> list[dict[str, str]]:
    """Return complete document text for a feedback-only corpus."""
    with project.connect() as conn:
        rows = conn.execute("SELECT document_id, current_path FROM documents ORDER BY current_path").fetchall()
        output = []
        for row in rows:
            revision = project.current_revision(conn, row["document_id"])
            text = safe_decode((project.objects_dir / revision["content_sha256"]).read_bytes()).strip()
            text = re.sub(r"^RESPONDENT:\s*", "", text).strip()
            output.append({
                "document_id": row["document_id"], "document_path": row["current_path"],
                "revision_id": revision["revision_id"], "feedback_text": text,
            })
    return output


def _bundle_rows(documents: list[dict[str, str]], seed: int, bundle_size: int, coverage: int) -> list[list[dict[str, str]]]:
    bundles: list[list[dict[str, str]]] = []
    for pass_index in range(coverage):
        shuffled = list(documents)
        random.Random(f"{seed}:{pass_index}").shuffle(shuffled)
        bundle_count = math.ceil(len(shuffled) / bundle_size)
        base, extra = divmod(len(shuffled), bundle_count)
        start = 0
        for bundle_index in range(bundle_count):
            size = base + (1 if bundle_index < extra else 0)
            bundles.append(shuffled[start:start + size])
            start += size
    return bundles


@discover_app.command("jobs")
def discovery_jobs_command(
    output: Path = typer.Option(Path("runs/001-discovery/jobs.ep"), "--output", "-o"),
    seed: int = typer.Option(20260803, "--seed"),
    bundle_size: int = typer.Option(25, "--bundle-size", min=5, max=100),
    coverage: int = typer.Option(2, "--coverage", min=1, max=5),
    model: Optional[str] = typer.Option(None, "--model"),
    max_tokens: int = typer.Option(6000, "--max-tokens", min=1000),
    force: bool = typer.Option(False, "--force"),
    human: bool = HumanOption,
) -> None:
    """Package reproducibly shuffled response bundles for candidate-code discovery."""
    command, json_flag = "insights discover jobs", should_emit_json(human)
    project = get_project(command, json_flag)
    target = _path(project.root, output)
    try:
        if target.exists() and not force:
            raise BewleyError(f"{target} already exists", code="ALREADY_EXISTS")
        documents = _plain_feedback_documents(project)
        if not documents:
            raise BewleyError("No feedback documents found.", code="NO_FEEDBACK_TEXT")
        bundles = _bundle_rows(documents, seed, bundle_size, coverage)
        Jobs, Model, ModelList, QuestionFreeText, _, Scenario, ScenarioList = _edsl()
        scenarios = []
        manifest_bundles = []
        for index, bundle in enumerate(bundles, 1):
            source_map = {f"DOC_{row['document_id'][:12]}": row for row in bundle}
            bundle_text = "\n\n".join(
                f"[{source_id}]\n{row['feedback_text']}" for source_id, row in source_map.items()
            )
            bundle_id = f"pass-{(index - 1) // math.ceil(len(documents) / bundle_size) + 1:02d}-bundle-{index:03d}"
            scenarios.append(Scenario({
                "bundle_id": bundle_id, "bundle_text": bundle_text,
                "sources_json": json.dumps({key: value for key, value in source_map.items()}, ensure_ascii=False),
            }))
            manifest_bundles.append({"bundle_id": bundle_id, "source_ids": list(source_map)})
        question = QuestionFreeText(question_name=DISCOVERY_QUESTION_NAME, question_text=DISCOVERY_QUESTION_TEXT)
        jobs = Jobs(survey=question.to_survey()).by(ScenarioList(scenarios))
        model_target = None
        if model:
            model_target = _save_model_list_json(Model, ModelList, model, max_tokens, target.with_name("models.json"), force)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            target.unlink()
        saved = jobs.git.save(target)
        manifest = {
            "schema_version": "1.0", "seed": seed, "bundle_size": bundle_size,
            "coverage": coverage, "document_count": len(documents), "bundles": manifest_bundles,
            "corpus_fingerprint": hashlib.sha256(json.dumps([
                (row["document_id"], row["revision_id"]) for row in documents
            ]).encode()).hexdigest(),
        }
        manifest_target = target.with_name("bundle-manifest.json")
        manifest_target.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        data = {
            "output": str(target), "manifest": str(manifest_target),
            "document_count": len(documents), "bundle_count": len(bundles),
            "coverage": coverage, "expected_model_calls": len(bundles),
            "models": str(model_target) if model_target else None, "saved": saved,
        }
    except (BewleyError, OSError) as exc:
        fail(command, exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="IO_ERROR"), json_flag)
        return
    model_args = [] if model_target else ["--model", "<model-name>"]
    finish(command, data, next_actions=[action(
        "run-feedback-discovery", "Run bundled code discovery externally",
        ["ep", "run", str(target), *model_args, "--output", str(target.with_name("results.ep"))],
        mutates_state=True, requires_network=True, requires_user_approval=True,
    )])


def _parse_discovery(raw: Any, sources: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    if isinstance(raw, str):
        start, end = raw.find("["), raw.rfind("]")
        if start < 0 or end < start:
            raise ValueError("answer does not contain a JSON array")
        raw = json.loads(raw[start:end + 1])
    if not isinstance(raw, list):
        raise ValueError("answer is not an array")
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("candidate is not an object")
        fields = ("code_key", "name", "description", "inclusion_criteria", "exclusion_criteria")
        if not all(isinstance(item.get(field), str) and item[field].strip() for field in fields):
            raise ValueError("candidate fields are incomplete")
        if not re.fullmatch(r"[a-z][a-z0-9_]{2,63}", item["code_key"]):
            raise ValueError("invalid candidate code_key")
        evidence = item.get("evidence")
        if not isinstance(evidence, list) or not 2 <= len(evidence) <= 5:
            raise ValueError("candidate evidence must contain 2-5 excerpts")
        evidence_sources = set()
        for row in evidence:
            if not isinstance(row, dict) or row.get("source_id") not in sources:
                raise ValueError("unknown evidence source")
            exact = row.get("exact_text")
            if not isinstance(exact, str) or not exact.strip() or exact not in sources[row["source_id"]]["feedback_text"]:
                raise ValueError("evidence is not an exact source substring")
            evidence_sources.add(row["source_id"])
        if len(evidence_sources) < 2:
            raise ValueError("candidate must be supported by two different sources")
        if item["code_key"] in seen:
            raise ValueError("duplicate candidate code_key in bundle")
        seen.add(item["code_key"])
    return raw


@discover_app.command("ingest")
def discovery_ingest_command(
    results: Path = typer.Argument(...),
    jobs: Path = typer.Option(Path("runs/001-discovery/jobs.ep"), "--jobs"),
    output: Path = typer.Option(Path("runs/001-discovery/candidates.jsonl"), "--output", "-o"),
    force: bool = typer.Option(False, "--force"),
    human: bool = HumanOption,
) -> None:
    """Validate bundle coverage, candidate schemas, and exact discovery evidence."""
    command, json_flag = "insights discover ingest", should_emit_json(human)
    project = get_project(command, json_flag)
    result_path, jobs_path, target = (_path(project.root, value) for value in (results, jobs, output))
    try:
        if target.exists() and not force:
            raise BewleyError(f"{target} already exists", code="ALREADY_EXISTS")
        Jobs, _, _, _, Results, _, _ = _edsl()
        expected = {dict(row)["bundle_id"] for row in Jobs.git.load(jobs_path).scenarios}
        rows, seen, failures, rejected = [], set(), [], []
        for result in Results.git.load(result_path):
            scenario = dict(result["scenario"])
            bundle_id = scenario.get("bundle_id", "unknown")
            try:
                if bundle_id in seen:
                    raise ValueError("duplicate bundle result")
                answer = result["answer"]
                raw = answer.get(DISCOVERY_QUESTION_NAME) if isinstance(answer, dict) else None
                sources = json.loads(scenario["sources_json"])
                if isinstance(raw, str):
                    start, end = raw.find("["), raw.rfind("]")
                    if start < 0 or end < start:
                        raise ValueError("answer does not contain a JSON array")
                    raw = json.loads(raw[start:end + 1])
                if not isinstance(raw, list):
                    raise ValueError("answer is not an array")
                for candidate_index, candidate in enumerate(raw):
                    try:
                        valid = _parse_discovery([candidate], sources)[0]
                        rows.append({"bundle_id": bundle_id, **valid})
                    except (ValueError, KeyError, TypeError) as exc:
                        rejected.append({
                            "bundle_id": bundle_id, "candidate_index": candidate_index,
                            "code_key": candidate.get("code_key") if isinstance(candidate, dict) else None,
                            "reason": str(exc),
                        })
                seen.add(bundle_id)
            except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
                failures.append({"bundle_id": bundle_id, "error": str(exc)})
        if failures or seen != expected:
            raise BewleyError("Discovery results failed validation; no output was written.", code="INCOMPLETE_RESULTS", context={"failures": failures, "missing_bundles": sorted(expected - seen)})
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        log = target.with_name("ingest-log.jsonl")
        with log.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({
                "ingested_at": utcnow(), "results": str(result_path),
                "accepted_candidates": rows, "rejected_candidates": rejected,
            }, ensure_ascii=False) + "\n")
        data = {
            "output": str(target), "bundle_count": len(seen),
            "candidate_count": len(rows), "rejected_candidate_count": len(rejected),
            "rejected_candidates": rejected, "ingest_log": str(log),
        }
    except (BewleyError, OSError) as exc:
        fail(command, exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="IO_ERROR"), json_flag)
        return
    finish(command, data)


def _candidate_inventory(path: Path) -> list[dict[str, Any]]:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    output = []
    for index, row in enumerate(rows):
        compact = {key: row[key] for key in (
            "bundle_id", "code_key", "name", "description", "inclusion_criteria", "exclusion_criteria", "evidence"
        )}
        compact["candidate_id"] = hashlib.sha256(
            json.dumps(compact, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()[:20]
        output.append(compact)
    return output


@consolidate_app.command("jobs")
def consolidation_jobs_command(
    candidates: Path = typer.Option(Path("runs/001-discovery-retry2/candidates.jsonl"), "--candidates"),
    output: Path = typer.Option(Path("runs/002-consolidation/jobs.ep"), "--output", "-o"),
    min_codes: int = typer.Option(8, "--min-codes", min=4, max=50),
    max_codes: int = typer.Option(20, "--max-codes", min=4, max=60),
    model: Optional[str] = typer.Option(None, "--model"),
    max_tokens: int = typer.Option(12000, "--max-tokens", min=1000),
    prior_results: Optional[Path] = typer.Option(None, "--prior-results"),
    force: bool = typer.Option(False, "--force"),
    human: bool = HumanOption,
) -> None:
    """Package all validated discovery candidates for global consolidation."""
    command, json_flag = "insights consolidate jobs", should_emit_json(human)
    project = get_project(command, json_flag)
    source, target = _path(project.root, candidates), _path(project.root, output)
    try:
        if min_codes > max_codes:
            raise BewleyError("--min-codes cannot exceed --max-codes", code="INVALID_INPUT")
        if target.exists() and not force:
            raise BewleyError(f"{target} already exists", code="ALREADY_EXISTS")
        inventory = _candidate_inventory(source)
        if not inventory:
            raise BewleyError("No validated discovery candidates found.", code="INVALID_INPUT")
        Jobs, Model, ModelList, QuestionFreeText, Results, Scenario, ScenarioList = _edsl()
        correction_context = ""
        if prior_results:
            prior_path = _path(project.root, prior_results)
            prior_rows = list(Results.git.load(prior_path))
            if len(prior_rows) != 1:
                raise BewleyError("--prior-results must contain one result", code="INVALID_INPUT")
            answer = prior_rows[0]["answer"]
            prior_raw = answer.get(CONSOLIDATION_QUESTION_NAME) if isinstance(answer, dict) else ""
            prior_value = json.loads(prior_raw[prior_raw.find("{"):prior_raw.rfind("}") + 1])
            expected_ids = {row["candidate_id"] for row in inventory}
            mapped_ids = [item for code in prior_value.get("codes", []) for item in code.get("candidate_ids", [])]
            missing_ids = sorted(expected_ids - set(mapped_ids))
            correction_context = json.dumps({
                "rejected_prior_draft": prior_value,
                "validation_error": "candidate_ids must map every expected candidate exactly once",
                "missing_candidate_ids": missing_ids,
                "instruction": "Return a complete corrected object. Preserve good consolidation choices where possible and place every missing candidate_id exactly once.",
            }, ensure_ascii=False)
        scenario = Scenario({
            "candidate_ids": [row["candidate_id"] for row in inventory],
            "candidate_fingerprint": hashlib.sha256(json.dumps(inventory, sort_keys=True).encode()).hexdigest(),
            "min_codes": min_codes, "max_codes": max_codes,
            "candidates_json": json.dumps(inventory, ensure_ascii=False),
            "correction_context": correction_context,
        })
        question = QuestionFreeText(question_name=CONSOLIDATION_QUESTION_NAME, question_text=CONSOLIDATION_QUESTION_TEXT)
        jobs = Jobs(survey=question.to_survey()).by(ScenarioList([scenario]))
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            target.unlink()
        saved = jobs.git.save(target)
        model_target = _save_model_list_json(Model, ModelList, model, max_tokens, target.with_name("models.json"), force) if model else None
        data = {
            "output": str(target), "candidate_count": len(inventory),
            "candidate_fingerprint": scenario["candidate_fingerprint"],
            "expected_model_calls": 1, "models": str(model_target) if model_target else None, "saved": saved,
        }
    except (BewleyError, OSError, json.JSONDecodeError, KeyError) as exc:
        error = exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="INVALID_INPUT")
        fail(command, error, json_flag)
        return
    finish(command, data, next_actions=[action(
        "run-feedback-consolidation", "Run global candidate consolidation externally",
        ["ep", "run", "--jobs", str(target), "--model_list", str(model_target or target.with_name("models.json")), "--output", str(target.with_name("results.ep"))],
        mutates_state=True, requires_network=True, requires_user_approval=True,
    )])


def _parse_consolidation(raw: Any, expected: set[str], minimum: int, maximum: int) -> dict[str, Any]:
    if isinstance(raw, str):
        start, end = raw.find("{"), raw.rfind("}")
        if start < 0 or end < start:
            raise ValueError("answer does not contain a JSON object")
        raw = json.loads(raw[start:end + 1])
    if not isinstance(raw, dict):
        raise ValueError("answer is not an object")
    themes, codes = raw.get("themes"), raw.get("codes")
    if not isinstance(themes, list) or not 4 <= len(themes) <= 10:
        raise ValueError("themes must contain 4-10 entries")
    if not isinstance(codes, list) or not minimum <= len(codes) <= maximum:
        raise ValueError(f"codes must contain {minimum}-{maximum} entries")
    theme_keys, code_keys = set(), set()
    for theme in themes:
        if not isinstance(theme, dict) or not re.fullmatch(r"[a-z][a-z0-9_]{2,63}", str(theme.get("theme_key", ""))):
            raise ValueError("invalid theme")
        if not all(isinstance(theme.get(key), str) and theme[key].strip() for key in ("name", "description")):
            raise ValueError("incomplete theme")
        theme_keys.add(theme["theme_key"])
    if len(theme_keys) != len(themes):
        raise ValueError("duplicate theme keys")
    for code in codes:
        fields = ("code_key", "name", "description", "inclusion_criteria", "exclusion_criteria")
        if not isinstance(code, dict) or not all(isinstance(code.get(key), str) and code[key].strip() for key in fields):
            raise ValueError("incomplete code")
        if not re.fullmatch(r"[a-z][a-z0-9_]{2,63}", code["code_key"]) or code["theme_key"] not in theme_keys:
            raise ValueError("invalid code key or theme reference")
        code_keys.add(code["code_key"])
    if len(code_keys) != len(codes):
        raise ValueError("duplicate code keys")
    clean_codes = [
        {key: code[key] for key in ("code_key", "theme_key", "name", "description", "inclusion_criteria", "exclusion_criteria")}
        for code in codes
    ]
    return {"themes": themes, "codes": clean_codes}


@consolidate_app.command("ingest")
def consolidation_ingest_command(
    results: Path = typer.Argument(...),
    jobs: Path = typer.Option(Path("runs/002-consolidation/jobs.ep"), "--jobs"),
    output: Path = typer.Option(Path("qualitative-analysis/feedback-codebook.json"), "--output", "-o"),
    force: bool = typer.Option(False, "--force"),
    human: bool = HumanOption,
) -> None:
    """Validate and freeze the compact semantic codebook."""
    command, json_flag = "insights consolidate ingest", should_emit_json(human)
    project = get_project(command, json_flag)
    result_path, jobs_path, target = (_path(project.root, value) for value in (results, jobs, output))
    try:
        if target.exists() and not force:
            raise BewleyError(f"{target} already exists", code="ALREADY_EXISTS")
        Jobs, _, _, _, Results, _, _ = _edsl()
        scenarios, result_rows = list(Jobs.git.load(jobs_path).scenarios), list(Results.git.load(result_path))
        if len(scenarios) != 1 or len(result_rows) != 1:
            raise BewleyError("Expected exactly one consolidation result.", code="INCOMPLETE_RESULTS")
        scenario = dict(scenarios[0]); answer = result_rows[0]["answer"]
        raw = answer.get(CONSOLIDATION_QUESTION_NAME) if isinstance(answer, dict) else None
        codebook = _parse_consolidation(raw, set(scenario["candidate_ids"]), scenario["min_codes"], scenario["max_codes"])
        artifact = {
            "schema_version": "1.0", "frozen_at": utcnow(),
            "candidate_fingerprint": scenario["candidate_fingerprint"], **codebook,
        }
        artifact["codebook_fingerprint"] = hashlib.sha256(json.dumps(codebook, sort_keys=True).encode()).hexdigest()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(artifact, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        log = jobs_path.parent / "ingest-log.jsonl"
        with log.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"ingested_at": utcnow(), "results": str(result_path), "artifact": artifact}, ensure_ascii=False) + "\n")
        data = {"output": str(target), "theme_count": len(codebook["themes"]), "code_count": len(codebook["codes"]), "codebook_fingerprint": artifact["codebook_fingerprint"], "ingest_log": str(log)}
    except (BewleyError, OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        error = exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="INVALID_RESULTS")
        fail(command, error, json_flag); return
    finish(command, data)


@classify_app.command("jobs")
def classification_jobs_command(
    codebook: Path = typer.Option(Path("qualitative-analysis/feedback-codebook.json"), "--codebook"),
    output: Path = typer.Option(Path("runs/003-classification/jobs.ep"), "--output", "-o"),
    model: Optional[str] = typer.Option(None, "--model"),
    max_tokens: int = typer.Option(4000, "--max-tokens", min=1000),
    force: bool = typer.Option(False, "--force"),
    human: bool = HumanOption,
) -> None:
    """Package one exhaustive fixed-codebook classification job per response."""
    command, json_flag = "insights classify jobs", should_emit_json(human)
    project = get_project(command, json_flag)
    source, target = _path(project.root, codebook), _path(project.root, output)
    try:
        if target.exists() and not force:
            raise BewleyError(f"{target} already exists", code="ALREADY_EXISTS")
        artifact = json.loads(source.read_text(encoding="utf-8"))
        documents = _plain_feedback_documents(project)
        Jobs, Model, ModelList, QuestionFreeText, _, Scenario, ScenarioList = _edsl()
        compact = {"themes": artifact["themes"], "codes": artifact["codes"]}
        scenarios = [Scenario({
            **row, "response_text": row["feedback_text"],
            "codebook_fingerprint": artifact["codebook_fingerprint"],
            "codebook_json": json.dumps(compact, ensure_ascii=False),
        }) for row in documents]
        question = QuestionFreeText(question_name=CLASSIFICATION_QUESTION_NAME, question_text=CLASSIFICATION_QUESTION_TEXT)
        jobs = Jobs(survey=question.to_survey()).by(ScenarioList(scenarios))
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists(): target.unlink()
        saved = jobs.git.save(target)
        model_target = _save_model_list_json(Model, ModelList, model, max_tokens, target.with_name("models.json"), force) if model else None
        data = {"output": str(target), "document_count": len(documents), "code_count": len(artifact["codes"]), "codebook_fingerprint": artifact["codebook_fingerprint"], "expected_model_calls": len(documents), "models": str(model_target) if model_target else None, "saved": saved}
    except (BewleyError, OSError, KeyError, json.JSONDecodeError) as exc:
        error = exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="INVALID_INPUT")
        fail(command, error, json_flag); return
    finish(command, data, next_actions=[action(
        "run-feedback-classification", "Classify every feedback response externally",
        ["ep", "run", "--jobs", str(target), "--model_list", str(model_target or target.with_name("models.json")), "--output", str(target.with_name("results.ep"))],
        mutates_state=True, requires_network=True, requires_user_approval=True,
    )])


def _parse_classification(raw: Any, text: str, allowed: set[str]) -> dict[str, Any]:
    if isinstance(raw, str):
        start, end = raw.find("{"), raw.rfind("}")
        if start < 0 or end < start: raise ValueError("answer does not contain a JSON object")
        raw = json.loads(raw[start:end + 1])
    if not isinstance(raw, dict) or raw.get("sentiment") not in SENTIMENTS:
        raise ValueError("invalid sentiment")
    assignments = raw.get("assignments")
    if not isinstance(assignments, list): raise ValueError("assignments must be an array")
    seen = set(); accepted = []; rejected = []
    for item in assignments:
        if not isinstance(item, dict): raise ValueError("assignment is not an object")
        if item.get("code_key") not in allowed:
            rejected.append({"assignment": item, "reason": "unknown code_key"}); continue
        if item["code_key"] in seen:
            rejected.append({"assignment": item, "reason": "duplicate code_key"}); continue
        exact, confidence = item.get("exact_text"), item.get("confidence")
        if not isinstance(exact, str) or not exact.strip() or exact not in text:
            raise ValueError("assignment evidence is not an exact source substring")
        if not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
            raise ValueError("invalid assignment confidence")
        seen.add(item["code_key"])
        accepted.append(item)
    novel = raw.get("potential_new_theme")
    if novel is not None and (not isinstance(novel, str) or not novel.strip()):
        raise ValueError("potential_new_theme must be null or nonempty text")
    return {"sentiment": raw["sentiment"], "assignments": accepted, "potential_new_theme": novel, "_rejected_assignments": rejected}


@classify_app.command("ingest")
def classification_ingest_command(
    results: Path = typer.Argument(...),
    jobs: Path = typer.Option(Path("runs/003-classification/jobs.ep"), "--jobs"),
    codebook: Path = typer.Option(Path("qualitative-analysis/feedback-codebook.json"), "--codebook"),
    output: Path = typer.Option(Path("qualitative-analysis/feedback-classifications.jsonl"), "--output", "-o"),
    force: bool = typer.Option(False, "--force"),
    human: bool = HumanOption,
) -> None:
    """Validate complete response coverage, frozen IDs, and exact assignment evidence."""
    command, json_flag = "insights classify ingest", should_emit_json(human)
    project = get_project(command, json_flag)
    result_path, jobs_path, codebook_path, target = (_path(project.root, value) for value in (results, jobs, codebook, output))
    try:
        if target.exists() and not force: raise BewleyError(f"{target} already exists", code="ALREADY_EXISTS")
        artifact = json.loads(codebook_path.read_text(encoding="utf-8")); allowed = {row["code_key"] for row in artifact["codes"]}
        Jobs, _, _, _, Results, _, _ = _edsl()
        expected = {dict(row)["document_id"] for row in Jobs.git.load(jobs_path).scenarios}
        rows, seen, failures, rejected_assignments = [], set(), [], []
        for result in Results.git.load(result_path):
            scenario = dict(result["scenario"]); document_id = scenario.get("document_id", "unknown")
            try:
                if document_id in seen: raise ValueError("duplicate document result")
                if scenario["codebook_fingerprint"] != artifact["codebook_fingerprint"]: raise ValueError("stale codebook fingerprint")
                answer = result["answer"]; raw = answer.get(CLASSIFICATION_QUESTION_NAME) if isinstance(answer, dict) else None
                parsed = _parse_classification(raw, scenario["response_text"], allowed)
                for rejected in parsed.pop("_rejected_assignments"):
                    rejected_assignments.append({"document_id": document_id, **rejected})
                rows.append({"document_id": document_id, "document_path": scenario["document_path"], "revision_id": scenario["revision_id"], "response_text": scenario["response_text"], **parsed}); seen.add(document_id)
            except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
                failures.append({"document_id": document_id, "error": str(exc)})
        if failures or seen != expected:
            raise BewleyError("Classification results failed validation; no output was written.", code="INCOMPLETE_RESULTS", context={"failures": failures, "missing_document_count": len(expected-seen)})
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding="utf-8") as handle:
            for row in sorted(rows, key=lambda value: value["document_path"]): handle.write(json.dumps(row, ensure_ascii=False)+"\n")
        log = result_path.parent / "ingest-log.jsonl"
        with log.open("a", encoding="utf-8") as handle: handle.write(json.dumps({"ingested_at":utcnow(),"results":str(result_path),"response_count":len(rows),"codebook_fingerprint":artifact["codebook_fingerprint"],"rejected_assignments":rejected_assignments},ensure_ascii=False)+"\n")
        data = {"output":str(target),"response_count":len(rows),"assignment_count":sum(len(row["assignments"]) for row in rows),"rejected_assignment_count":len(rejected_assignments),"rejected_assignments":rejected_assignments,"potential_new_theme_count":sum(row["potential_new_theme"] is not None for row in rows),"ingest_log":str(log)}
    except (BewleyError,OSError,KeyError,json.JSONDecodeError) as exc:
        error=exc if isinstance(exc,BewleyError) else BewleyError(str(exc),code="INVALID_RESULTS"); fail(command,error,json_flag); return
    finish(command,data)


@app.command("aggregate")
def aggregate_command(
    classifications: Path = typer.Option(Path("qualitative-analysis/feedback-classifications.jsonl"), "--classifications"),
    codebook: Path = typer.Option(Path("qualitative-analysis/feedback-codebook.json"), "--codebook"),
    output: Path = typer.Option(Path("qualitative-analysis/feedback-aggregate.json"), "--output", "-o"),
    human: bool = HumanOption,
) -> None:
    """Compute deterministic theme and sentiment tables from validated classifications."""
    command,json_flag="insights aggregate",should_emit_json(human); project=get_project(command,json_flag)
    try:
        rows=[json.loads(line) for line in _path(project.root,classifications).read_text(encoding="utf-8").splitlines() if line.strip()]
        book=json.loads(_path(project.root,codebook).read_text(encoding="utf-8")); codes={row["code_key"]:row for row in book["codes"]}; themes={row["theme_key"]:row for row in book["themes"]}
        code_counts=Counter(item["code_key"] for row in rows for item in row["assignments"]); sentiment=Counter(row["sentiment"] for row in rows)
        theme_docs={key:set() for key in themes}
        for row in rows:
            for key in {codes[item["code_key"]]["theme_key"] for item in row["assignments"]}: theme_docs[key].add(row["document_id"])
        artifact={"schema_version":"1.0","generated_at":utcnow(),"response_count":len(rows),"codebook_fingerprint":book["codebook_fingerprint"],"sentiment_counts":dict(sentiment),"themes":[{"theme_key":key,"name":themes[key]["name"],"response_count":len(ids)} for key,ids in theme_docs.items()],"codes":[{"code_key":key,"name":codes[key]["name"],"theme_key":codes[key]["theme_key"],"response_count":code_counts[key]} for key in codes],"unassigned_response_count":sum(not row["assignments"] for row in rows),"potential_new_theme_count":sum(row["potential_new_theme"] is not None for row in rows)}
        target=_path(project.root,output); target.write_text(json.dumps(artifact,indent=2,ensure_ascii=False)+"\n",encoding="utf-8"); data={"output":str(target),**{key:artifact[key] for key in ("response_count","unassigned_response_count","potential_new_theme_count")}}
    except (OSError,KeyError,json.JSONDecodeError) as exc: fail(command,BewleyError(str(exc),code="INVALID_INPUT"),json_flag); return
    finish(command,data)


@app.command("evidence-export")
def evidence_export_command(
    aggregate: Path = typer.Option(Path("qualitative-analysis/feedback-aggregate.json"), "--aggregate"),
    classifications: Path = typer.Option(Path("qualitative-analysis/feedback-classifications.jsonl"), "--classifications"),
    codebook: Path = typer.Option(Path("qualitative-analysis/feedback-codebook.json"), "--codebook"),
    output: Path = typer.Option(Path("qualitative-analysis/feedback-insights-explorer.html"), "--output", "-o"),
    title: str = typer.Option("AI interviewer feedback insights", "--title"),
    human: bool = HumanOption,
) -> None:
    """Export deterministic tables, verified quotes, codebook details, and response evidence."""
    command,json_flag="insights evidence-export",should_emit_json(human); project=get_project(command,json_flag)
    try:
        agg=json.loads(_path(project.root,aggregate).read_text(encoding="utf-8")); book=json.loads(_path(project.root,codebook).read_text(encoding="utf-8")); rows=[json.loads(line) for line in _path(project.root,classifications).read_text(encoding="utf-8").splitlines() if line.strip()]
        codes={row["code_key"]:row for row in book["codes"]}; themes={row["theme_key"]:row for row in book["themes"]}
        evidence={key:[] for key in codes}
        for row in rows:
            for item in row["assignments"]:
                evidence[item["code_key"]].append({"document_path":row["document_path"],"exact_text":item["exact_text"],"confidence":item["confidence"]})
        for values in evidence.values(): values.sort(key=lambda value:(-value["confidence"],-len(value["exact_text"]),value["document_path"]))
        theme_cards="".join(f'<article><h3>{html.escape(item["name"])}</h3><b>{item["response_count"]} of {agg["response_count"]}</b><div class="bar"><i style="width:{100*item["response_count"]/max(1,agg["response_count"]):.1f}%"></i></div><p>{html.escape(themes[item["theme_key"]]["description"])}</p></article>' for item in sorted(agg["themes"],key=lambda value:-value["response_count"]))
        code_rows="".join(f'<article><h3>{html.escape(item["name"])}</h3><b>{item["response_count"]} responses</b><p>{html.escape(codes[item["code_key"]]["description"])}</p>'+(''.join(f'<div class="quote"><button class="copy" data-copy="{html.escape(q["exact_text"],quote=True)}">Copy</button><blockquote>{html.escape(q["exact_text"])}</blockquote><small>{html.escape(q["document_path"])}</small></div>' for q in evidence[item["code_key"]][:2]) or '<small>No validated assignment.</small>')+'</article>' for item in sorted(agg["codes"],key=lambda value:-value["response_count"]))
        codebook_rows="".join(f'<article><h3>{html.escape(code["name"])}</h3><small>{html.escape(themes[code["theme_key"]]["name"])}</small><p>{html.escape(code["description"])}</p><details><summary>Criteria</summary><p><b>Include:</b> {html.escape(code["inclusion_criteria"])}</p><p><b>Exclude:</b> {html.escape(code["exclusion_criteria"])}</p></details></article>' for code in book["codes"])
        response_rows="".join(f'<article><h3>{html.escape(row["document_path"])}</h3><button class="copy" data-copy="{html.escape(row["response_text"],quote=True)}">Copy response</button><p class="response">{html.escape(row["response_text"])}</p><p><b>Sentiment:</b> {html.escape(row["sentiment"].replace("_"," "))}</p><p><b>Codes:</b> {html.escape(", ".join(codes[item["code_key"]]["name"] for item in row["assignments"]) or "None")}</p></article>' for row in rows)
        gaps=[row for row in rows if row["potential_new_theme"]]
        gap_rows="".join(f'<article><h3>{html.escape(row["document_path"])}</h3><p>{html.escape(row["potential_new_theme"])}</p><button class="copy" data-copy="{html.escape(row["response_text"],quote=True)}">Copy response</button><blockquote>{html.escape(row["response_text"])}</blockquote></article>' for row in gaps)
        sentiments="".join(f'<div class="metric"><b>{count}</b><span>{html.escape(label.replace("_"," ").title())}</span></div>' for label,count in sorted(agg["sentiment_counts"].items()))
        payload=json.dumps({"aggregate":agg,"codebook":book,"classifications":rows},ensure_ascii=False).replace("</","<\\/")
        page=f'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>{html.escape(title)}</title><style>body{{font:15px system-ui;margin:0;color:#202520;background:#fafbf9}}main{{max-width:1100px;margin:auto;padding:24px}}h1,h2,h3{{font-family:Georgia;color:#286b47}}nav{{display:flex;gap:6px;flex-wrap:wrap;border-bottom:2px solid #428a5f;position:sticky;top:0;background:#fafbf9;padding-top:6px}}nav button{{padding:10px;border:0;background:transparent;cursor:pointer}}nav button.on{{border-bottom:3px solid #428a5f;color:#286b47}}section[hidden]{{display:none}}.metrics{{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:8px}}.metric,article{{background:white;border:1px solid #d9ded9;border-radius:8px;padding:14px;margin:9px 0}}.metric b{{display:block;font-size:25px;color:#428a5f}}.bar{{height:6px;background:#e8ece8;border-radius:4px;margin-top:8px}}.bar i{{display:block;height:100%;background:#5ba97a}}blockquote{{font:17px Georgia;line-height:1.45;border-left:3px solid #8abb9b;padding-left:12px}}small{{color:#667066}}.copy{{float:right;cursor:pointer}}.response{{white-space:pre-wrap;line-height:1.5}}details summary{{cursor:pointer}}</style></head><body><main><h1>{html.escape(title)}</h1><p>Evidence-grounded analysis of {agg["response_count"]} feedback responses. Counts are computed from validated response-level classifications; quotes are exact source substrings.</p><nav><button class="on" data-tab="overview">Overview</button><button data-tab="themes">Themes & quotes</button><button data-tab="codebook">Codebook</button><button data-tab="responses">Responses</button><button data-tab="gaps">Coverage gaps ({len(gaps)})</button><button id="download">Export JSON</button></nav><section data-panel="overview"><h2>Summary</h2><p>{agg["sentiment_counts"].get("positive",0)} responses were classified positive, {agg["sentiment_counts"].get("negative",0)} negative, and {agg["sentiment_counts"].get("mixed",0)} mixed. The most prevalent themes were {html.escape(sorted(agg["themes"],key=lambda value:-value["response_count"])[0]["name"])} and {html.escape(sorted(agg["themes"],key=lambda value:-value["response_count"])[1]["name"])}.</p><div class="metrics"><div class="metric"><b>{agg["response_count"]}</b><span>Responses</span></div><div class="metric"><b>{sum(item["response_count"] for item in agg["codes"])}</b><span>Code assignments</span></div><div class="metric"><b>{agg["unassigned_response_count"]}</b><span>Unassigned</span></div></div><h2>Sentiment</h2><div class="metrics">{sentiments}</div><h2>Themes</h2>{theme_cards}</section><section data-panel="themes" hidden><h2>Codes and illustrative evidence</h2>{code_rows}</section><section data-panel="codebook" hidden><h2>Frozen codebook</h2><p><small>Fingerprint: {book["codebook_fingerprint"]}</small></p>{codebook_rows}</section><section data-panel="responses" hidden><h2>Response-level evidence</h2>{response_rows}</section><section data-panel="gaps" hidden><h2>Potential codebook gaps</h2><p>These model flags are not included in prevalence counts unless an existing validated code was also assigned.</p>{gap_rows}</section><script>const DATA={payload};document.querySelectorAll('[data-tab]').forEach(b=>b.onclick=()=>{{document.querySelectorAll('[data-tab]').forEach(x=>x.classList.toggle('on',x===b));document.querySelectorAll('[data-panel]').forEach(x=>x.hidden=x.dataset.panel!==b.dataset.tab;);}});document.addEventListener('click',e=>{{if(e.target.matches('[data-copy]'))navigator.clipboard.writeText(e.target.dataset.copy)}});document.getElementById('download').onclick=()=>{{const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([JSON.stringify(DATA,null,2)],{{type:'application/json'}}));a.download='feedback-insights-evidence.json';a.click()}};</script></main></body></html>'''
        page=page.replace("x.hidden=x.dataset.panel!==b.dataset.tab;);","x.hidden=x.dataset.panel!==b.dataset.tab);" )
        target=_path(project.root,output); target.write_text(page,encoding="utf-8"); data={"output":str(target),"response_count":len(rows),"theme_count":len(book["themes"]),"code_count":len(book["codes"]),"coverage_gap_count":len(gaps)}
    except (OSError,KeyError,json.JSONDecodeError) as exc: fail(command,BewleyError(str(exc),code="INVALID_INPUT"),json_flag); return
    finish(command,data)


def _answer(result: Any) -> Any:
    try:
        value = result["answer"]
        return value.get(QUESTION_NAME) if isinstance(value, dict) else getattr(value, QUESTION_NAME, None)
    except (KeyError, TypeError):
        return None


def _parse(raw: Any) -> dict[str, Any]:
    if isinstance(raw, str):
        text = raw.strip()
        start, end = text.find("{"), text.rfind("}")
        if start < 0 or end < start:
            raise ValueError("answer does not contain a JSON object")
        raw = json.loads(text[start:end + 1])
    if not isinstance(raw, dict):
        raise ValueError("answer is not an object")
    summary, sentiment = raw.get("summary"), raw.get("sentiment")
    themes, quotes = raw.get("themes"), raw.get("standout_quotes")
    if not isinstance(summary, str) or not summary.strip():
        raise ValueError("summary is required")
    if not isinstance(sentiment, dict) or sentiment.get("label") not in SENTIMENTS:
        raise ValueError("invalid sentiment label")
    if not isinstance(sentiment.get("score"), (int, float)) or not -1 <= sentiment["score"] <= 1:
        raise ValueError("sentiment score must be between -1 and 1")
    if not isinstance(sentiment.get("confidence"), (int, float)) or not 0 <= sentiment["confidence"] <= 1:
        raise ValueError("sentiment confidence must be between 0 and 1")
    if not isinstance(themes, list) or len(themes) > 5 or not isinstance(quotes, list) or len(quotes) > 2:
        raise ValueError("themes or standout_quotes exceeds its limit")
    for item in themes:
        if not isinstance(item, dict) or not isinstance(item.get("code_id"), str):
            raise ValueError("invalid theme assignment")
        if not isinstance(item.get("confidence"), (int, float)) or not 0 <= item["confidence"] <= 1:
            raise ValueError("invalid theme confidence")
        if not isinstance(item.get("rationale"), str) or not item["rationale"].strip():
            raise ValueError("theme rationale is required")
    for item in quotes:
        if not isinstance(item, dict) or not all(
            isinstance(item.get(key), str) and item[key].strip() for key in ("exact_text", "rationale")
        ):
            raise ValueError("invalid standout quote")
    return {"summary": summary.strip(), "sentiment": sentiment, "themes": themes, "standout_quotes": quotes}


@app.command("jobs")
def jobs_command(
    output: Path = typer.Option(Path("rapid-insights.jobs.ep"), "--output", "-o"),
    model: Optional[str] = typer.Option(None, "--model"),
    max_tokens: int = typer.Option(2500, "--max-tokens", min=500),
    force: bool = typer.Option(False, "--force"),
    human: bool = HumanOption,
) -> None:
    """Package one respondent-only insight job per AI-led interview."""
    command, json_flag = "insights jobs", should_emit_json(human)
    project = get_project(command, json_flag)
    target = _path(project.root, output)
    try:
        if target.exists() and not force:
            raise BewleyError(f"{target} already exists", code="ALREADY_EXISTS")
        themes, documents = _themes(project), _documents(project)
        if not documents:
            raise BewleyError(
                "No respondent turns were found in interview bodies.", code="NO_RESPONDENT_TEXT",
                hint="Rapid insights expects transcript lines beginning `RESPONDENT:`.",
            )
        Jobs, Model, ModelList, QuestionFreeText, _, Scenario, ScenarioList = _edsl()
        theme_json = json.dumps(themes, ensure_ascii=False)
        scenarios = [Scenario({**row, "themes_json": theme_json}) for row in documents]
        question = QuestionFreeText(question_name=QUESTION_NAME, question_text=QUESTION_TEXT)
        jobs = Jobs(survey=question.to_survey()).by(ScenarioList(scenarios))
        model_target = None
        if model:
            models = ModelList([_configured_model(Model, model, max_tokens)])
            jobs = jobs.by(models)
            model_target = target.with_name("rapid-insights.models.ep")
            if model_target.exists() and not force:
                raise BewleyError(f"{model_target} already exists", code="ALREADY_EXISTS")
            if model_target.exists():
                model_target.unlink()
            models.git.save(model_target)
        if target.exists():
            target.unlink()
        saved = jobs.git.save(target)
        data = {
            "output": str(target), "document_count": len(documents),
            "focused_theme_count": len(themes), "expected_model_calls": len(documents),
            "input_boundary": "RESPONDENT turns before AI-interviewer feedback marker",
            "models": str(model_target) if model_target else None, "saved": saved,
        }
    except (BewleyError, OSError) as exc:
        fail(command, exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="IO_ERROR"), json_flag)
        return
    model_args = [] if model_target else ["--model", "<model-name>"]
    finish(command, data, next_actions=[action(
        "run-rapid-insights", "Run respondent insight jobs externally",
        ["ep", "run", str(target), *model_args, "--output", str(target.with_name("rapid-insights.results.ep"))],
        mutates_state=True, requires_network=True, requires_user_approval=True,
    )])


@app.command("ingest")
def ingest_command(
    results: Path = typer.Argument(...),
    jobs: Path = typer.Option(Path("rapid-insights.jobs.ep"), "--jobs"),
    output: Path = typer.Option(Path("qualitative-analysis/rapid_insights.jsonl"), "--output", "-o"),
    force: bool = typer.Option(False, "--force"),
    human: bool = HumanOption,
) -> None:
    """Validate summaries, sentiment, theme ids, and verbatim quote evidence."""
    command, json_flag = "insights ingest", should_emit_json(human)
    project = get_project(command, json_flag)
    result_path, jobs_path, target = (_path(project.root, p) for p in (results, jobs, output))
    try:
        if target.exists() and not force:
            raise BewleyError(f"{target} already exists", code="ALREADY_EXISTS")
        Jobs, _, _, _, Results, _, _ = _edsl()
        expected = {dict(s)["document_id"] for s in Jobs.git.load(jobs_path).scenarios}
        rows, seen, failures = [], set(), []
        for result in Results.git.load(result_path):
            scenario = dict(result["scenario"])
            document_id = scenario["document_id"]
            try:
                if document_id in seen:
                    raise ValueError("duplicate document result")
                insight = _parse(_answer(result))
                allowed = {row["code_id"] for row in json.loads(scenario["themes_json"])}
                assigned = [row["code_id"] for row in insight["themes"]]
                if len(set(assigned)) != len(assigned) or not set(assigned) <= allowed:
                    raise ValueError("unknown or duplicate focused theme id")
                if any(row["exact_text"] not in scenario["respondent_text"] for row in insight["standout_quotes"]):
                    raise ValueError("standout quote is not verbatim respondent text")
                seen.add(document_id)
                rows.append({
                    "document_id": document_id, "document_path": scenario["document_path"],
                    "revision_id": scenario["revision_id"], **insight,
                })
            except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
                failures.append({"document_id": document_id, "error": str(exc)})
        if failures or seen != expected:
            raise BewleyError(
                "Rapid insight results failed validation; no output was written.",
                code="INCOMPLETE_RESULTS",
                context={"failures": failures, "missing_document_count": len(expected - seen)},
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding="utf-8") as handle:
            for row in sorted(rows, key=lambda item: item["document_path"]):
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        log = target.parent / "rapid_insights_ingest_log.jsonl"
        with log.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"ingested_at": utcnow(), "results": str(result_path), "rows": rows}, ensure_ascii=False) + "\n")
        data = {"output": str(target), "response_count": len(rows), "ingest_log": str(log)}
    except (BewleyError, OSError) as exc:
        fail(command, exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="IO_ERROR"), json_flag)
        return
    finish(command, data)


def feedback_jobs_command(
    output: Path = typer.Option(Path("interviewer-feedback.jobs.ep"), "--output", "-o"),
    model: Optional[str] = typer.Option(None, "--model"),
    max_tokens: int = typer.Option(16000, "--max-tokens", min=2000),
    force: bool = typer.Option(False, "--force"),
    human: bool = HumanOption,
) -> None:
    """Package one global job over AI-interviewer feedback responses only."""
    command, json_flag = "insights feedback-jobs", should_emit_json(human)
    project = get_project(command, json_flag)
    target = _path(project.root, output)
    try:
        if target.exists() and not force:
            raise BewleyError(f"{target} already exists", code="ALREADY_EXISTS")
        documents = _feedback_documents(project)
        if not documents:
            raise BewleyError("No marked AI-interviewer feedback was found.", code="NO_FEEDBACK_TEXT")
        Jobs, Model, ModelList, QuestionFreeText, _, Scenario, ScenarioList = _edsl()
        scenario = Scenario({
            "feedback_json": json.dumps([
                {"document_id": row["document_id"], "feedback_text": row["feedback_text"]}
                for row in documents
            ], ensure_ascii=False),
            "document_ids": [row["document_id"] for row in documents],
            "document_paths": {row["document_id"]: row["document_path"] for row in documents},
            "revision_ids": {row["document_id"]: row["revision_id"] for row in documents},
        })
        question = QuestionFreeText(
            question_name=FEEDBACK_QUESTION_NAME, question_text=FEEDBACK_QUESTION_TEXT,
        )
        jobs = Jobs(survey=question.to_survey()).by(ScenarioList([scenario]))
        model_target = None
        if model:
            models = ModelList([_configured_model(Model, model, max_tokens)])
            jobs = jobs.by(models)
            model_target = target.with_name("interviewer-feedback.models.ep")
            if model_target.exists() and not force:
                raise BewleyError(f"{model_target} already exists", code="ALREADY_EXISTS")
            if model_target.exists():
                model_target.unlink()
            models.git.save(model_target)
        if target.exists():
            target.unlink()
        saved = jobs.git.save(target)
        data = {
            "output": str(target), "feedback_response_count": len(documents),
            "expected_model_calls": 1,
            "input_boundary": "text after [Feedback about the AI interviewer] only",
            "models": str(model_target) if model_target else None, "saved": saved,
        }
    except (BewleyError, OSError) as exc:
        fail(command, exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="IO_ERROR"), json_flag)
        return
    model_args = [] if model_target else ["--model", "<model-name>"]
    finish(command, data, next_actions=[action(
        "run-interviewer-feedback-insights", "Run one global feedback synthesis externally",
        ["ep", "run", str(target), *model_args, "--output", str(target.with_name("interviewer-feedback.results.ep"))],
        mutates_state=True, requires_network=True, requires_user_approval=True,
    )])


def _parse_feedback(raw: Any) -> dict[str, Any]:
    if isinstance(raw, str):
        start, end = raw.find("{"), raw.rfind("}")
        if start < 0 or end < start:
            raise ValueError("answer does not contain a JSON object")
        raw = json.loads(raw[start:end + 1])
    if not isinstance(raw, dict) or not isinstance(raw.get("overall_summary"), str):
        raise ValueError("overall_summary is required")
    themes = raw.get("themes")
    distribution, quotes = raw.get("sentiment_distribution"), raw.get("standout_quotes")
    if not isinstance(themes, list) or not 6 <= len(themes) <= 12:
        raise ValueError("themes must contain 6-12 entries")
    if not isinstance(distribution, dict) or not isinstance(quotes, list) or len(quotes) > 12:
        raise ValueError("sentiment_distribution must be an object and standout_quotes an array")
    keys = []
    for theme in themes:
        if not isinstance(theme, dict) or not all(
            isinstance(theme.get(field), str) and theme[field].strip()
            for field in ("theme_key", "name", "description")
        ) or not re.fullmatch(r"[a-z][a-z0-9_]{2,63}", theme["theme_key"]):
            raise ValueError("invalid feedback theme")
        if not isinstance(theme.get("response_count"), int) or theme["response_count"] < 0:
            raise ValueError("feedback theme response_count must be nonnegative")
        keys.append(theme["theme_key"])
    if len(set(keys)) != len(keys):
        raise ValueError("feedback theme keys are not unique")
    if set(distribution) != {"positive", "negative", "mixed", "neutral"} or not all(
        isinstance(value, int) and value >= 0 for value in distribution.values()
    ):
        raise ValueError("invalid feedback sentiment distribution")
    for quote in quotes:
        if not isinstance(quote, dict) or not all(
            isinstance(quote.get(field), str) and quote[field].strip()
            for field in ("document_id", "exact_text", "rationale")
        ):
            raise ValueError("invalid feedback standout quote")
        assigned = quote.get("theme_keys")
        if not isinstance(assigned, list) or not set(assigned) <= set(keys):
            raise ValueError("invalid quote theme assignment")
    return raw


def feedback_ingest_command(
    results: Path = typer.Argument(...),
    jobs: Path = typer.Option(Path("interviewer-feedback.jobs.ep"), "--jobs"),
    output: Path = typer.Option(Path("qualitative-analysis/interviewer_feedback_insights.json"), "--output", "-o"),
    force: bool = typer.Option(False, "--force"),
    human: bool = HumanOption,
) -> None:
    """Validate complete feedback synthesis, sentiment, themes, and exact quotes."""
    command, json_flag = "insights feedback-ingest", should_emit_json(human)
    project = get_project(command, json_flag)
    result_path, jobs_path, target = (_path(project.root, p) for p in (results, jobs, output))
    try:
        if target.exists() and not force:
            raise BewleyError(f"{target} already exists", code="ALREADY_EXISTS")
        Jobs, _, _, _, Results, _, _ = _edsl()
        scenarios, result_rows = list(Jobs.git.load(jobs_path).scenarios), list(Results.git.load(result_path))
        if len(scenarios) != 1 or len(result_rows) != 1:
            raise BewleyError("Expected exactly one feedback synthesis result.", code="INCOMPLETE_RESULTS")
        scenario = dict(scenarios[0])
        raw_answer = result_rows[0]["answer"]
        answer = raw_answer.get(FEEDBACK_QUESTION_NAME) if isinstance(raw_answer, dict) else None
        insight = _parse_feedback(answer)
        expected = set(scenario["document_ids"])
        if sum(insight["sentiment_distribution"].values()) != len(expected):
            raise BewleyError("Feedback sentiment counts must sum to the response count.", code="INCOMPLETE_RESULTS")
        feedback = {row["document_id"]: row["feedback_text"] for row in json.loads(scenario["feedback_json"])}
        for theme in insight["themes"]:
            if theme["response_count"] > len(expected):
                raise BewleyError("Feedback theme count exceeds response count.", code="INVALID_RESULTS")
        for quote in insight["standout_quotes"]:
            if quote["document_id"] not in expected or quote["exact_text"] not in feedback[quote["document_id"]]:
                raise BewleyError("Feedback standout quote is not verbatim.", code="INVALID_RESULTS")
            quote["document_path"] = scenario["document_paths"][quote["document_id"]]
        artifact = {"schema_version": "1.0", "generated_at": utcnow(), **insight}
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(artifact, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        log = target.parent / "interviewer_feedback_ingest_log.jsonl"
        with log.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"ingested_at": utcnow(), "results": str(result_path), "artifact": artifact}, ensure_ascii=False) + "\n")
        data = {
            "output": str(target), "response_count": len(expected),
            "theme_count": len(insight["themes"]), "ingest_log": str(log),
        }
    except (BewleyError, OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        error = exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="INVALID_RESULTS")
        fail(command, error, json_flag)
        return
    finish(command, data)


def _report_html(
    rows: list[dict[str, Any]], theme_names: dict[str, str], title: str,
    subtitle: str = "Respondent-only analysis of AI-led interviews.",
    overall_summary: str = "",
) -> str:
    sentiments = Counter(row["sentiment"]["label"] for row in rows)
    themes = Counter(item["code_id"] for row in rows for item in row["themes"])
    quotes = [
        {"document_path": row["document_path"], **quote}
        for row in rows for quote in row["standout_quotes"]
    ]
    payload = json.dumps({"rows": rows, "theme_names": theme_names}, ensure_ascii=False).replace("</", "<\\/")
    theme_cards = "".join(
        f'<div class="bar-row"><span>{html.escape(theme_names.get(code, code))}</span><b>{count}</b><i style="width:{100*count/max(1,len(rows)):.1f}%"></i></div>'
        for code, count in themes.most_common()
    )
    sentiment_cards = "".join(
        f'<div class="metric"><b>{count}</b><span>{html.escape(label.replace("_", " ").title())}</span></div>'
        for label, count in sentiments.most_common()
    )
    quote_cards = "".join(
        f'<article><button class="copy" data-copy-quote="{index}">Copy</button><blockquote>{html.escape(q["exact_text"])}</blockquote><small>{html.escape(q["document_path"])} — {html.escape(q["rationale"])}</small></article>'
        for index, q in enumerate(quotes)
    )
    summaries = "".join(
        f'<article><h3>{html.escape(row["document_path"])}</h3><p>{html.escape(row["summary"])}</p><small>{html.escape(row["sentiment"]["label"])} ({row["sentiment"]["score"]:+.2f})</small></article>'
        for row in rows
    )
    return f'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>{html.escape(title)}</title>
<style>body{{font:15px system-ui;margin:0;color:#202520}}main{{max-width:1100px;margin:auto;padding:24px}}h1,h2{{font-family:Georgia;color:#286b47}}nav{{display:flex;gap:8px;border-bottom:2px solid #428a5f}}nav button{{padding:10px;border:0;background:white;cursor:pointer}}nav button.on{{color:#286b47;border-bottom:3px solid #428a5f}}section[hidden]{{display:none}}.metrics{{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:8px;margin:16px 0}}.metric,article,.bar-row{{border:1px solid #ddd;border-radius:7px;padding:12px}}.metric b{{display:block;font-size:24px;color:#428a5f}}.bar-row{{display:grid;grid-template-columns:1fr auto;gap:8px;margin:6px 0;overflow:hidden}}.bar-row i{{grid-column:1/3;height:5px;background:#5ba97a}}article{{margin:9px 0}}blockquote{{font:18px Georgia;line-height:1.5}}.copy{{float:right}}small{{color:#666}}button{{cursor:pointer}}</style></head><body><main>
<h1>{html.escape(title)}</h1><p>{html.escape(subtitle)}</p>{f'<p><strong>Summary:</strong> {html.escape(overall_summary)}</p>' if overall_summary else ''}
<nav><button class="on" data-tab="overview">Overview</button><button data-tab="themes">Themes</button><button data-tab="quotes">Standout quotes</button><button data-tab="responses">Response summaries</button><button id="download">Export JSON</button></nav>
<section data-panel="overview"><div class="metrics"><div class="metric"><b>{len(rows)}</b><span>Responses</span></div><div class="metric"><b>{len(themes)}</b><span>Themes observed</span></div><div class="metric"><b>{len(quotes)}</b><span>Validated quotes</span></div></div><h2>Sentiment toward AI at work</h2><div class="metrics">{sentiment_cards}</div></section>
<section data-panel="themes" hidden><h2>Theme prevalence across respondents</h2>{theme_cards}</section>
<section data-panel="quotes" hidden><h2>Standout quotes</h2>{quote_cards or '<p>No quotes selected.</p>'}</section>
<section data-panel="responses" hidden><h2>Response summaries</h2>{summaries}</section>
<script>const DATA={payload};const QUOTES=DATA.rows.flatMap(r=>r.standout_quotes||[]);document.querySelectorAll('[data-tab]').forEach(b=>b.onclick=()=>{{document.querySelectorAll('[data-tab]').forEach(x=>x.classList.toggle('on',x===b));document.querySelectorAll('[data-panel]').forEach(x=>x.hidden=x.dataset.panel!==b.dataset.tab)}});document.addEventListener('click',e=>{{if(e.target.matches('.copy'))navigator.clipboard.writeText(QUOTES[Number(e.target.dataset.copyQuote)].exact_text)}});document.getElementById('download').onclick=()=>{{const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([JSON.stringify(DATA,null,2)],{{type:'application/json'}}));a.download='rapid-insights.json';a.click()}};</script></main></body></html>'''


@app.command("export")
def export_command(
    input_jsonl: Path = typer.Option(Path("qualitative-analysis/rapid_insights.jsonl"), "--input", "-i"),
    output: Path = typer.Option(Path("qualitative-analysis/rapid-insights.html"), "--output", "-o"),
    title: str = typer.Option("Rapid insights from AI-led interviews", "--title"),
    human: bool = HumanOption,
) -> None:
    """Export sentiment, theme prevalence, summaries, and standout quotes as HTML."""
    command, json_flag = "insights export", should_emit_json(human)
    project = get_project(command, json_flag)
    source, target = _path(project.root, input_jsonl), _path(project.root, output)
    try:
        rows = [json.loads(line) for line in source.read_text(encoding="utf-8").splitlines() if line.strip()]
        names = {row["code_id"]: row["canonical_name"] for row in _themes(project)}
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(_report_html(
            rows, names, title,
            subtitle=f"Respondent-only analysis of {len(rows)} AI-led interviews. Interviewer prompts, identifiers, metadata, and post-interview feedback excluded.",
        ), encoding="utf-8")
        data = {"output": str(target), "response_count": len(rows)}
    except (BewleyError, OSError, json.JSONDecodeError) as exc:
        fail(command, exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="IO_ERROR"), json_flag)
        return
    finish(command, data)


def feedback_export_command(
    input_json: Path = typer.Option(Path("qualitative-analysis/interviewer_feedback_insights.json"), "--input", "-i"),
    output: Path = typer.Option(Path("qualitative-analysis/interviewer-feedback-insights.html"), "--output", "-o"),
    title: str = typer.Option("AI interviewer feedback — rapid insights", "--title"),
    human: bool = HumanOption,
) -> None:
    """Export the AI-interviewer feedback dashboard and downloadable evidence."""
    command, json_flag = "insights feedback-export", should_emit_json(human)
    project = get_project(command, json_flag)
    source, target = _path(project.root, input_json), _path(project.root, output)
    try:
        artifact = json.loads(source.read_text(encoding="utf-8"))
        total = sum(artifact["sentiment_distribution"].values())
        theme_rows = "".join(
            f'<article><h3>{html.escape(row["name"])}</h3><b>{row["response_count"]} of {total}</b>'
            f'<div class="bar"><i style="width:{100*row["response_count"]/max(1,total):.1f}%"></i></div>'
            f'<p>{html.escape(row["description"])}</p></article>'
            for row in sorted(artifact["themes"], key=lambda item: -item["response_count"])
        )
        sentiment_rows = "".join(
            f'<div class="metric"><b>{count}</b><span>{html.escape(label.title())}</span></div>'
            for label, count in artifact["sentiment_distribution"].items()
        )
        quote_rows = "".join(
            f'<article><button class="copy" data-quote="{index}">Copy</button>'
            f'<blockquote>{html.escape(row["exact_text"])}</blockquote>'
            f'<small>{html.escape(row["document_path"])} — {html.escape(row["rationale"])}</small></article>'
            for index, row in enumerate(artifact["standout_quotes"])
        )
        payload = json.dumps(artifact, ensure_ascii=False).replace("</", "<\\/")
        page = f'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>{html.escape(title)}</title>
<style>body{{font:15px system-ui;color:#202520;margin:0}}main{{max-width:1050px;margin:auto;padding:24px}}h1,h2,h3{{font-family:Georgia;color:#286b47}}nav{{display:flex;gap:8px;border-bottom:2px solid #428a5f}}nav button{{padding:10px;border:0;background:white;cursor:pointer}}nav button.on{{border-bottom:3px solid #428a5f;color:#286b47}}section[hidden]{{display:none}}.metrics{{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin:16px 0}}.metric,article{{border:1px solid #ddd;border-radius:7px;padding:12px;margin:8px 0}}.metric b{{display:block;font-size:25px;color:#428a5f}}.bar{{height:6px;background:#eee;border-radius:5px}}.bar i{{display:block;height:100%;background:#5ba97a}}blockquote{{font:18px Georgia;line-height:1.5}}small{{color:#666}}.copy{{float:right}}</style></head><body><main>
<h1>{html.escape(title)}</h1><p>Analysis of {total} free-text responses about the AI interviewer only.</p><p><strong>Summary:</strong> {html.escape(artifact["overall_summary"])}</p>
<nav><button class="on" data-tab="overview">Overview</button><button data-tab="themes">Themes</button><button data-tab="quotes">Standout quotes</button><button id="download">Export JSON</button></nav>
<section data-panel="overview"><h2>Sentiment</h2><div class="metrics">{sentiment_rows}</div><p><small>Aggregate model estimates; counts sum to all {total} responses.</small></p></section>
<section data-panel="themes" hidden><h2>Detected themes</h2><p><small>Theme prevalence is an aggregate model estimate; responses may express more than one theme.</small></p>{theme_rows}</section>
<section data-panel="quotes" hidden><h2>Standout quotes</h2>{quote_rows}</section>
<script>const DATA={payload};document.querySelectorAll('[data-tab]').forEach(b=>b.onclick=()=>{{document.querySelectorAll('[data-tab]').forEach(x=>x.classList.toggle('on',x===b));document.querySelectorAll('[data-panel]').forEach(x=>x.hidden=x.dataset.panel!==b.dataset.tab)}});document.addEventListener('click',e=>{{if(e.target.matches('.copy'))navigator.clipboard.writeText(DATA.standout_quotes[Number(e.target.dataset.quote)].exact_text)}});document.getElementById('download').onclick=()=>{{const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([JSON.stringify(DATA,null,2)],{{type:'application/json'}}));a.download='interviewer-feedback-insights.json';a.click()}};</script></main></body></html>'''
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(page, encoding="utf-8")
        data = {"output": str(target), "response_count": total, "theme_count": len(artifact["themes"])}
    except (OSError, KeyError, json.JSONDecodeError) as exc:
        fail(command, BewleyError(str(exc), code="IO_ERROR"), json_flag)
        return
    finish(command, data)
