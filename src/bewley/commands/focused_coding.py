"""EDSL-powered focused coding over an existing open-code inventory."""
from __future__ import annotations

import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Optional

import typer

from bewley.commands.common import HumanOption, action, fail, finish, get_project, should_emit_json
from bewley.project import BewleyError, cmd_study_show, utcnow

app = typer.Typer(help="Build and apply a global focused-code framework.")

FRAMEWORK_QUESTION = "focused_framework"
MAPPING_QUESTION = "focused_mapping"
_KEY = re.compile(r"^[a-z][a-z0-9_]{2,63}$")

FRAMEWORK_PROMPT = """You are conducting second-cycle focused coding over a complete
inventory of first-cycle open codes. Construct a coherent global analytic
framework, not a list of synonyms. The framework must answer the declared
research question and remain within the declared study purpose.

Study context:
{{ study_context }}

Return one JSON object with:
- "themes": array of objects with theme_key, name, description
- "focused_codes": array of objects with focused_key, theme_key, name,
  description, inclusion_criteria, exclusion_criteria

Requirements:
- Create between {{ min_focused }} and {{ max_focused }} focused codes.
- Use 4-12 themes.
- Keys are unique lowercase snake_case.
- Focused codes must be broad enough to subsume multiple open codes while
  preserving analytically important differences in mechanism, direction,
  temporality, conditions, and valence.
- Preserve distinctions that matter for this study, including positive,
  negative, mixed, and non-substantive feedback when those distinctions are
  present in the inventory. Do not introduce categories from another domain.
- Make categories mutually distinguishable and collectively capable of
  covering the inventory.
- Do not return mappings yet and do not invent quotations.

Complete compact open-code inventory:
{{ inventory_json }}"""

MAPPING_PROMPT = """Map every supplied open code to exactly one primary code in
the fixed focused-code framework. Do not create, rename, or omit categories.

Return only a JSON array with one object per open code:
- "open_code_id"
- "focused_key"
- "rationale"
- "confidence" from 0 to 1

Use the code definition and evidence, not name similarity alone. If a code is
about respondent occupation/context or interview/interface quality, use the
corresponding framework category rather than forcing it into an AI-impact
finding.

Fixed framework:
{{ framework_json }}

Open codes to map:
{{ codes_json }}"""


def _edsl():
    try:
        from edsl import Jobs, Model, ModelList, QuestionFreeText, Results, Scenario, ScenarioList
    except ImportError as exc:
        raise BewleyError("EDSL is required for focused coding", code="DEPENDENCY_MISSING") from exc
    return Jobs, Model, ModelList, QuestionFreeText, Results, Scenario, ScenarioList


def _path(root: Path, value: Path) -> Path:
    return value if value.is_absolute() else root / value


def _answer(result: Any, name: str) -> Any:
    try:
        value = result["answer"]
        return value.get(name) if isinstance(value, dict) else getattr(value, name, None)
    except (KeyError, TypeError):
        return None


def _json_value(raw: Any, opening: str, closing: str) -> Any:
    if not isinstance(raw, str):
        return raw
    text = raw.strip()
    start, end = text.find(opening), text.rfind(closing)
    if start < 0 or end < start:
        raise ValueError("answer does not contain the required JSON value")
    return json.loads(text[start:end + 1])


def _open_codes(project, *, evidence_limit: int = 1) -> list[dict[str, Any]]:
    project.ensure_db()
    with project.connect() as conn:
        rows = conn.execute(
            """SELECT c.code_id, c.canonical_name, c.description,
                      c.inclusion_criteria, c.exclusion_criteria,
                      COUNT(DISTINCT a.annotation_id) annotation_count
               FROM codes c LEFT JOIN annotations a
                 ON a.code_id = c.code_id AND a.is_active = 1
               WHERE c.status = 'active' AND COALESCE(c.code_layer, 'open') = 'open'
               GROUP BY c.code_id ORDER BY c.canonical_name"""
        ).fetchall()
        output = []
        for row in rows:
            evidence = conn.execute(
                """SELECT annotation_id, exact_text
                   FROM annotations
                   WHERE code_id = ? AND is_active = 1 AND exact_text IS NOT NULL
                   ORDER BY annotation_id LIMIT ?""",
                (row["code_id"], evidence_limit),
            ).fetchall()
            output.append({
                "open_code_id": row["code_id"],
                "name": row["canonical_name"],
                "description": row["description"] or "",
                "inclusion_criteria": row["inclusion_criteria"] or "",
                "exclusion_criteria": row["exclusion_criteria"] or "",
                "annotation_count": row["annotation_count"],
                "evidence": [
                    {
                        "annotation_id": item["annotation_id"],
                        "text": (item["exact_text"] or "")[:500],
                    }
                    for item in evidence
                ],
            })
    return output


def _fingerprint(project) -> str:
    compact = [
        (row["open_code_id"], row["name"], row["description"])
        for row in _open_codes(project, evidence_limit=0)
    ]
    return hashlib.sha256(json.dumps(compact, ensure_ascii=False).encode()).hexdigest()


def _save_models(Model, ModelList, target: Path, model: Optional[str], max_tokens: int, force: bool) -> Optional[Path]:
    if not model:
        return None
    model_target = target.with_name(target.stem.replace(".jobs", "") + ".models.ep")
    if model_target.exists() and not force:
        raise BewleyError(f"{model_target} already exists", code="ALREADY_EXISTS")
    if model_target.exists():
        model_target.unlink()
    ModelList([Model(model, max_tokens=max_tokens)]).git.save(model_target)
    return model_target


@app.command("framework-jobs")
def framework_jobs(
    output: Path = typer.Option(Path("focused-framework.jobs.ep"), "--output", "-o"),
    model: Optional[str] = typer.Option(None, "--model"),
    min_focused: int = typer.Option(30, "--min-focused", min=5, max=100),
    max_focused: int = typer.Option(60, "--max-focused", min=5, max=120),
    max_tokens: int = typer.Option(16000, "--max-tokens", min=1000),
    force: bool = typer.Option(False, "--force"),
    human: bool = HumanOption,
) -> None:
    """Package one global job that constructs the focused-code framework."""
    command, json_flag = "codebook focused framework-jobs", should_emit_json(human)
    project = get_project(command, json_flag)
    target = _path(project.root, output)
    try:
        if min_focused > max_focused:
            raise BewleyError("--min-focused cannot exceed --max-focused", code="INVALID_INPUT")
        if target.exists() and not force:
            raise BewleyError(f"{target} already exists", code="ALREADY_EXISTS")
        codes = _open_codes(project, evidence_limit=0)
        if not codes:
            raise BewleyError("No active open codes found.", code="INVALID_INPUT")
        inventory = [
            {
                "open_code_id": row["open_code_id"],
                "name": row["name"],
                "description": row["description"][:80],
                "annotation_count": row["annotation_count"],
            }
            for row in codes
        ]
        Jobs, Model, ModelList, QuestionFreeText, _, Scenario, ScenarioList = _edsl()
        study = cmd_study_show(project)
        study_context = {
            "method": study.get("method"),
            "unit_of_analysis": study.get("unit_of_analysis"),
            "purpose": study.get("purpose"),
            "research_questions": [
                row["text"] for row in study.get("research_questions", [])
                if row.get("status") == "active"
            ],
        }
        scenario = Scenario({
            "codebook_fingerprint": _fingerprint(project),
            "open_code_count": len(codes),
            "min_focused": min_focused,
            "max_focused": max_focused,
            "study_context": json.dumps(study_context, ensure_ascii=False),
            "inventory_json": json.dumps(inventory, ensure_ascii=False),
        })
        question = QuestionFreeText(question_name=FRAMEWORK_QUESTION, question_text=FRAMEWORK_PROMPT)
        job = Jobs(survey=question.to_survey()).by(ScenarioList([scenario]))
        if model:
            job = job.by(ModelList([Model(model, max_tokens=max_tokens)]))
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            target.unlink()
        saved = job.git.save(target)
        models = _save_models(Model, ModelList, target, model, max_tokens, force)
        data = {
            "output": str(target), "open_code_count": len(codes),
            "codebook_fingerprint": scenario["codebook_fingerprint"],
            "expected_model_calls": 1, "saved": saved,
            "models": str(models) if models else None,
        }
    except (BewleyError, OSError) as exc:
        fail(command, exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="IO_ERROR"), json_flag)
        return
    result_target = target.with_name("focused-framework.results.ep")
    model_args = [] if models else ["--model", "<model-name>"]
    finish(command, data, next_actions=[action(
        "run-focused-framework", "Run the global framework job externally",
        ["ep", "run", str(target), *model_args, "--output", str(result_target)],
        mutates_state=True, requires_network=True, requires_user_approval=True,
    )])


def _validate_framework(raw: Any, minimum: int, maximum: int) -> dict[str, Any]:
    value = _json_value(raw, "{", "}")
    if not isinstance(value, dict):
        raise ValueError("framework answer is not an object")
    themes, focused = value.get("themes"), value.get("focused_codes")
    if not isinstance(themes, list) or not 4 <= len(themes) <= 12:
        raise ValueError("themes must contain 4-12 entries")
    if not isinstance(focused, list) or not minimum <= len(focused) <= maximum:
        raise ValueError(f"focused_codes must contain {minimum}-{maximum} entries")
    theme_keys: set[str] = set()
    names: set[str] = set()
    for item in themes:
        if not isinstance(item, dict):
            raise ValueError("theme is not an object")
        key, name, description = item.get("theme_key"), item.get("name"), item.get("description")
        if not isinstance(key, str) or not _KEY.fullmatch(key) or key in theme_keys:
            raise ValueError("theme keys must be unique lowercase snake_case")
        if not all(isinstance(x, str) and x.strip() for x in (name, description)):
            raise ValueError("theme name and description are required")
        theme_keys.add(key)
        names.add(name.casefold())
    focused_keys: set[str] = set()
    for item in focused:
        if not isinstance(item, dict):
            raise ValueError("focused code is not an object")
        key, theme, name = item.get("focused_key"), item.get("theme_key"), item.get("name")
        if not isinstance(key, str) or not _KEY.fullmatch(key) or key in focused_keys:
            raise ValueError("focused keys must be unique lowercase snake_case")
        if theme not in theme_keys:
            raise ValueError("focused code references an unknown theme")
        fields = (name, item.get("description"), item.get("inclusion_criteria"), item.get("exclusion_criteria"))
        if not all(isinstance(x, str) and x.strip() for x in fields):
            raise ValueError("focused code name, definition, and criteria are required")
        if name.casefold() in names:
            raise ValueError("framework names must be unique")
        names.add(name.casefold())
        focused_keys.add(key)
    return {"themes": themes, "focused_codes": focused}


@app.command("framework-ingest")
def framework_ingest(
    results: Path = typer.Argument(...),
    jobs: Path = typer.Option(Path("focused-framework.jobs.ep"), "--jobs"),
    output: Path = typer.Option(Path("qualitative-analysis/focused_framework.json"), "--output", "-o"),
    force: bool = typer.Option(False, "--force"),
    human: bool = HumanOption,
) -> None:
    """Validate and save the globally proposed focused-code framework."""
    command, json_flag = "codebook focused framework-ingest", should_emit_json(human)
    project = get_project(command, json_flag)
    result_path, jobs_path, target = (_path(project.root, p) for p in (results, jobs, output))
    try:
        if target.exists() and not force:
            raise BewleyError(f"{target} already exists", code="ALREADY_EXISTS")
        Jobs, _, _, _, Results, _, _ = _edsl()
        job = Jobs.git.load(jobs_path)
        scenarios = list(job.scenarios)
        result_rows = list(Results.git.load(result_path))
        if len(scenarios) != 1 or len(result_rows) != 1:
            raise BewleyError("Expected exactly one framework result.", code="INCOMPLETE_RESULTS")
        scenario = dict(scenarios[0])
        framework = _validate_framework(
            _answer(result_rows[0], FRAMEWORK_QUESTION),
            int(scenario["min_focused"]), int(scenario["max_focused"]),
        )
        artifact = {
            "schema_version": "1.0",
            "created_at": utcnow(),
            "codebook_fingerprint": scenario["codebook_fingerprint"],
            "open_code_count": scenario["open_code_count"],
            **framework,
        }
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(artifact, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        log = target.parent / "focused_framework_ingest_log.jsonl"
        with log.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"ingested_at": utcnow(), "results": str(result_path), "framework": artifact}, ensure_ascii=False) + "\n")
        data = {
            "output": str(target), "theme_count": len(framework["themes"]),
            "focused_code_count": len(framework["focused_codes"]), "ingest_log": str(log),
        }
    except (BewleyError, OSError, ValueError, json.JSONDecodeError) as exc:
        error = exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="INVALID_RESULTS")
        fail(command, error, json_flag)
        return
    finish(command, data)


def _load_framework(project, path: Path) -> tuple[Path, dict[str, Any]]:
    target = _path(project.root, path)
    if not target.exists():
        raise BewleyError(f"{target} does not exist", code="NOT_FOUND")
    return target, json.loads(target.read_text(encoding="utf-8"))


@app.command("mapping-jobs")
def mapping_jobs(
    framework: Path = typer.Option(Path("qualitative-analysis/focused_framework.json"), "--framework"),
    output: Path = typer.Option(Path("focused-mapping.jobs.ep"), "--output", "-o"),
    batch_size: int = typer.Option(30, "--batch-size", min=5, max=60),
    model: Optional[str] = typer.Option(None, "--model"),
    max_tokens: int = typer.Option(8000, "--max-tokens", min=1000),
    force: bool = typer.Option(False, "--force"),
    human: bool = HumanOption,
) -> None:
    """Package exhaustive open-code mappings against the fixed framework."""
    command, json_flag = "codebook focused mapping-jobs", should_emit_json(human)
    project = get_project(command, json_flag)
    target = _path(project.root, output)
    try:
        framework_path, artifact = _load_framework(project, framework)
        if artifact["codebook_fingerprint"] != _fingerprint(project):
            raise BewleyError("The open codebook changed after framework generation.", code="STALE_CODEBOOK")
        if target.exists() and not force:
            raise BewleyError(f"{target} already exists", code="ALREADY_EXISTS")
        codes = _open_codes(project, evidence_limit=2)
        fixed = {"themes": artifact["themes"], "focused_codes": artifact["focused_codes"]}
        Jobs, Model, ModelList, QuestionFreeText, _, Scenario, ScenarioList = _edsl()
        scenarios = []
        for start in range(0, len(codes), batch_size):
            batch = codes[start:start + batch_size]
            scenarios.append(Scenario({
                "batch_index": start // batch_size,
                "codebook_fingerprint": artifact["codebook_fingerprint"],
                "open_code_ids": [row["open_code_id"] for row in batch],
                "framework_json": json.dumps(fixed, ensure_ascii=False),
                "codes_json": json.dumps(batch, ensure_ascii=False),
            }))
        question = QuestionFreeText(question_name=MAPPING_QUESTION, question_text=MAPPING_PROMPT)
        job = Jobs(survey=question.to_survey()).by(ScenarioList(scenarios))
        if model:
            job = job.by(ModelList([Model(model, max_tokens=max_tokens)]))
        if target.exists():
            target.unlink()
        saved = job.git.save(target)
        models = _save_models(Model, ModelList, target, model, max_tokens, force)
        data = {
            "output": str(target), "framework": str(framework_path),
            "open_code_count": len(codes), "batch_count": len(scenarios),
            "expected_model_calls": len(scenarios), "saved": saved,
            "models": str(models) if models else None,
        }
    except (BewleyError, OSError, KeyError, json.JSONDecodeError) as exc:
        error = exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="INVALID_FRAMEWORK")
        fail(command, error, json_flag)
        return
    result_target = target.with_name("focused-mapping.results.ep")
    model_args = [] if models else ["--model", "<model-name>"]
    finish(command, data, next_actions=[action(
        "run-focused-mapping", "Run exhaustive mappings externally",
        ["ep", "run", str(target), *model_args, "--output", str(result_target)],
        mutates_state=True, requires_network=True, requires_user_approval=True,
    )])


def _parse_mappings(raw: Any) -> list[dict[str, Any]]:
    value = _json_value(raw, "[", "]")
    if not isinstance(value, list):
        raise ValueError("mapping answer is not an array")
    output = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("mapping is not an object")
        code_id, focused_key = item.get("open_code_id"), item.get("focused_key")
        rationale, confidence = item.get("rationale"), item.get("confidence")
        if not all(isinstance(x, str) and x.strip() for x in (code_id, focused_key, rationale)):
            raise ValueError("mapping id, focused key, and rationale are required")
        if not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
            raise ValueError("mapping confidence must be between 0 and 1")
        output.append({
            "open_code_id": code_id, "focused_key": focused_key,
            "rationale": rationale.strip(), "confidence": float(confidence),
        })
    return output


@app.command("mapping-ingest")
def mapping_ingest(
    results: Path = typer.Argument(...),
    jobs: Path = typer.Option(Path("focused-mapping.jobs.ep"), "--jobs"),
    framework: Path = typer.Option(Path("qualitative-analysis/focused_framework.json"), "--framework"),
    output: Path = typer.Option(Path("qualitative-analysis/focused_mapping.csv"), "--output", "-o"),
    force: bool = typer.Option(False, "--force"),
    human: bool = HumanOption,
) -> None:
    """Validate a complete one-to-one mapping of open codes to focused codes."""
    command, json_flag = "codebook focused mapping-ingest", should_emit_json(human)
    project = get_project(command, json_flag)
    result_path, jobs_path, target = (_path(project.root, p) for p in (results, jobs, output))
    try:
        _, artifact = _load_framework(project, framework)
        Jobs, _, _, _, Results, _, _ = _edsl()
        job = Jobs.git.load(jobs_path)
        expected_batches = {int(dict(s)["batch_index"]) for s in job.scenarios}
        focused_keys = {row["focused_key"] for row in artifact["focused_codes"]}
        rows, seen_batches, seen_codes, failures = [], set(), set(), []
        for result in Results.git.load(result_path):
            scenario = dict(result["scenario"])
            batch = int(scenario["batch_index"])
            seen_batches.add(batch)
            allowed = set(scenario["open_code_ids"])
            try:
                mappings = _parse_mappings(_answer(result, MAPPING_QUESTION))
                returned = {row["open_code_id"] for row in mappings}
                if len(returned) != len(mappings) or returned != allowed:
                    raise ValueError("batch must map every supplied open code exactly once")
                if any(row["focused_key"] not in focused_keys for row in mappings):
                    raise ValueError("mapping references an unknown focused key")
                if returned & seen_codes:
                    raise ValueError("open code appears in multiple batches")
                seen_codes |= returned
                for row in mappings:
                    rows.append({
                        "codebook_fingerprint": scenario["codebook_fingerprint"],
                        "batch_index": batch,
                        **row,
                    })
            except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
                failures.append({"batch_index": batch, "error": str(exc)})
        expected_codes = {row["open_code_id"] for row in _open_codes(project, evidence_limit=0)}
        if failures or seen_batches != expected_batches or seen_codes != expected_codes:
            raise BewleyError(
                "Focused mappings failed completeness validation; no mapping was written.",
                code="INCOMPLETE_RESULTS",
                context={
                    "failures": failures,
                    "missing_batches": sorted(expected_batches - seen_batches),
                    "missing_code_count": len(expected_codes - seen_codes),
                },
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        fields = ["codebook_fingerprint", "batch_index", "open_code_id", "focused_key", "rationale", "confidence"]
        with target.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(sorted(rows, key=lambda row: row["open_code_id"]))
        log = target.parent / "focused_mapping_ingest_log.jsonl"
        with log.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"ingested_at": utcnow(), "results": str(result_path), "mappings": rows}, ensure_ascii=False) + "\n")
        data = {
            "output": str(target), "mapping_count": len(rows),
            "focused_codes_used": len({row["focused_key"] for row in rows}),
            "ingest_log": str(log),
        }
    except (BewleyError, OSError, KeyError, json.JSONDecodeError) as exc:
        error = exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="INVALID_RESULTS")
        fail(command, error, json_flag)
        return
    finish(command, data)


@app.command("apply")
def apply_focused(
    framework: Path = typer.Option(Path("qualitative-analysis/focused_framework.json"), "--framework"),
    mapping: Path = typer.Option(Path("qualitative-analysis/focused_mapping.csv"), "--mapping"),
    dry_run: bool = typer.Option(False, "--dry-run"),
    human: bool = HumanOption,
) -> None:
    """Create theme/focused layers and parent every active open code."""
    command, json_flag = "codebook focused apply", should_emit_json(human)
    project = get_project(command, json_flag)
    try:
        framework_path, artifact = _load_framework(project, framework)
        mapping_path = _path(project.root, mapping)
        with mapping_path.open(newline="", encoding="utf-8") as handle:
            mappings = list(csv.DictReader(handle))
        if artifact["codebook_fingerprint"] != _fingerprint(project):
            raise BewleyError("The open codebook changed after focused coding.", code="STALE_CODEBOOK")
        with project.connect() as conn:
            layered = conn.execute(
                "SELECT COUNT(*) FROM codes WHERE status='active' AND code_layer IN ('focused','theme')"
            ).fetchone()[0]
            existing_names = {
                row["canonical_name"].casefold()
                for row in conn.execute("SELECT canonical_name FROM codes")
            }
        if layered:
            raise BewleyError("Focused coding has already been applied.", code="ALREADY_APPLIED")
        framework_names = [row["name"] for row in artifact["themes"] + artifact["focused_codes"]]
        if len({name.casefold() for name in framework_names}) != len(framework_names):
            raise BewleyError("Framework contains duplicate names.", code="INVALID_FRAMEWORK")
        conflicts = sorted(name for name in framework_names if name.casefold() in existing_names)
        if conflicts:
            raise BewleyError("Framework names conflict with existing codes.", code="ALREADY_EXISTS", context={"names": conflicts})
        open_ids = {row["open_code_id"] for row in _open_codes(project, evidence_limit=0)}
        mapped_ids = {row["open_code_id"] for row in mappings}
        focused_keys = {row["focused_key"] for row in artifact["focused_codes"]}
        if open_ids != mapped_ids or any(row["focused_key"] not in focused_keys for row in mappings):
            raise BewleyError("Mapping is incomplete or references unknown focused codes.", code="INVALID_MAPPING")
        plan = {
            "themes_created": len(artifact["themes"]),
            "focused_codes_created": len(artifact["focused_codes"]),
            "open_codes_parented": len(mappings),
        }
        event_ids: list[str] = []
        if not dry_run:
            theme_ids = {}
            for row in artifact["themes"]:
                event = project.add_code(row["name"], row["description"], code_layer="theme")
                theme_ids[row["theme_key"]] = event["payload"]["code_id"]
                event_ids.append(event["event_id"])
            focused_ids = {}
            for row in artifact["focused_codes"]:
                event = project.add_code(row["name"], row["description"], code_layer="focused")
                code_id = event["payload"]["code_id"]
                focused_ids[row["focused_key"]] = code_id
                event_ids.append(event["event_id"])
                event_ids.append(project.update_code(
                    code_id,
                    inclusion_criteria=row["inclusion_criteria"],
                    exclusion_criteria=row["exclusion_criteria"],
                )["event_id"])
                event_ids.append(project.set_code_parent(code_id, theme_ids[row["theme_key"]])["event_id"])
            for row in mappings:
                event_ids.append(
                    project.set_code_parent(row["open_code_id"], focused_ids[row["focused_key"]])["event_id"]
                )
            log = mapping_path.parent / "focused_apply_log.jsonl"
            with log.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps({
                    "applied_at": utcnow(), "framework": str(framework_path),
                    "mapping": str(mapping_path), "plan": plan, "event_ids": event_ids,
                }) + "\n")
        data = {**plan, "dry_run": dry_run, "event_ids": event_ids}
    except (BewleyError, OSError, KeyError, json.JSONDecodeError) as exc:
        error = exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="INVALID_INPUT")
        fail(command, error, json_flag)
        return
    finish(command, data)
