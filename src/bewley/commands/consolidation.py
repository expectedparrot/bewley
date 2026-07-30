"""Agent-assisted, human-reviewed code consolidation."""
from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Optional

import typer

from bewley.commands.common import HumanOption, action, fail, finish, get_project, should_emit_json
from bewley.project import BewleyError, utcnow

app = typer.Typer(help="Package, review, and apply agent-proposed code merges.")

QUESTION_NAME = "code_consolidation"
QUESTION_TEXT = """You are consolidating provisional open codes in one qualitative codebook batch.

Each code includes a stable id, name, definition, counts, and representative
participant quotations. Propose merges only when codes express the same
analytic concept, not merely related topics. Preserve temporal, directional,
conditional, and positive/negative distinctions. When uncertain, propose
nothing.

Return only a JSON array. Each object must contain:
- "source_code_ids": non-empty array of code ids to absorb
- "target_code_id": another code id in this batch that best names the concept
- "rationale": concise evidence-based explanation
- "confidence": number from 0 to 1
- "evidence_annotation_ids": annotation ids supporting equivalence

Never invent ids. Do not place the target in source_code_ids. A source code may
appear in at most one proposal."""


def _edsl():
    try:
        from edsl import Jobs, Model, ModelList, QuestionFreeText, Results, Scenario, ScenarioList
    except ImportError as exc:
        raise BewleyError("EDSL is required for code consolidation", code="DEPENDENCY_MISSING") from exc
    return Jobs, Model, ModelList, QuestionFreeText, Results, Scenario, ScenarioList


def _path(root: Path, value: Path) -> Path:
    return value if value.is_absolute() else root / value


def _result_value(result: Any, group: str, key: str) -> Any:
    try:
        value = result[group]
        return value.get(key) if isinstance(value, dict) else getattr(value, key, None)
    except (KeyError, TypeError):
        return None


def _fingerprint(project) -> str:
    with project.connect() as conn:
        rows = conn.execute(
            """SELECT code_id, canonical_name, COALESCE(description, ''), COALESCE(parent_code_id, '')
               FROM codes WHERE status = 'active' ORDER BY code_id"""
        ).fetchall()
    return hashlib.sha256(json.dumps([tuple(row) for row in rows]).encode()).hexdigest()


def _code_payload(project) -> list[dict[str, Any]]:
    with project.connect() as conn:
        codes = conn.execute(
            """SELECT c.code_id, c.canonical_name, c.description, c.inclusion_criteria,
                      c.exclusion_criteria, c.parent_code_id,
                      COUNT(DISTINCT a.annotation_id) annotations,
                      COUNT(DISTINCT a.document_id) documents
               FROM codes c LEFT JOIN annotations a
                 ON a.code_id = c.code_id AND a.is_active = 1
               WHERE c.status = 'active'
               GROUP BY c.code_id ORDER BY c.canonical_name"""
        ).fetchall()
        output = []
        for code in codes:
            evidence = conn.execute(
                """SELECT annotation_id, exact_text, d.current_path
                   FROM annotations a JOIN documents d ON d.document_id = a.document_id
                   WHERE a.code_id = ? AND a.is_active = 1 AND a.exact_text IS NOT NULL
                   ORDER BY a.annotation_id LIMIT 3""",
                (code["code_id"],),
            ).fetchall()
            output.append({
                "code_id": code["code_id"],
                "name": code["canonical_name"],
                "description": code["description"],
                "inclusion_criteria": code["inclusion_criteria"],
                "exclusion_criteria": code["exclusion_criteria"],
                "parent_code_id": code["parent_code_id"],
                "annotation_count": code["annotations"],
                "document_count": code["documents"],
                "evidence": [dict(row) for row in evidence],
            })
    return output


def _parse_proposals(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, str):
        text = raw.strip()
        if text.startswith("```"):
            text = "\n".join(text.splitlines()[1:])
        if text.rstrip().endswith("```"):
            text = "\n".join(text.splitlines()[:-1])
        start, end = text.find("["), text.rfind("]")
        if start < 0 or end < start:
            raise ValueError("answer does not contain a JSON array")
        raw = json.loads(text[start:end + 1])
    if not isinstance(raw, list):
        raise ValueError("answer is not an array")
    proposals = []
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("proposal is not an object")
        sources = item.get("source_code_ids")
        if not isinstance(sources, list) or not sources or not all(isinstance(x, str) for x in sources):
            raise ValueError("source_code_ids must be a non-empty string array")
        target = item.get("target_code_id")
        rationale = item.get("rationale")
        confidence = item.get("confidence")
        evidence = item.get("evidence_annotation_ids")
        if not isinstance(target, str) or not isinstance(rationale, str) or not rationale.strip():
            raise ValueError("target_code_id and rationale are required")
        if not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
            raise ValueError("confidence must be between 0 and 1")
        if not isinstance(evidence, list) or not all(isinstance(x, str) for x in evidence):
            raise ValueError("evidence_annotation_ids must be a string array")
        proposals.append({
            "source_code_ids": list(dict.fromkeys(sources)),
            "target_code_id": target,
            "rationale": rationale.strip(),
            "confidence": float(confidence),
            "evidence_annotation_ids": list(dict.fromkeys(evidence)),
        })
    return proposals


@app.command("jobs")
def jobs_command(
    output: Path = typer.Option(Path("consolidation.jobs.ep"), "--output", "-o"),
    batch_size: int = typer.Option(30, "--batch-size", min=5, max=60),
    model: Optional[str] = typer.Option(None, "--model"),
    max_tokens: int = typer.Option(4000, "--max-tokens", min=1),
    force: bool = typer.Option(False, "--force"),
    human: bool = HumanOption,
) -> None:
    """Package active codes and representative evidence as consolidation Jobs."""
    command = "codebook consolidate jobs"
    json_flag = should_emit_json(human)
    project = get_project(command, json_flag)
    target = _path(project.root, output)
    try:
        if target.suffix != ".ep":
            raise BewleyError("--output must use .ep", code="VALIDATION_ERROR")
        if target.exists() and not force:
            raise BewleyError(f"{target} already exists", code="ALREADY_EXISTS", hint="Use --force to replace it.")
        codes = _code_payload(project)
        if len(codes) < 2:
            raise BewleyError("At least two active codes are required.", code="INVALID_INPUT")
        fingerprint = _fingerprint(project)
        Jobs, Model, ModelList, QuestionFreeText, _, Scenario, ScenarioList = _edsl()
        scenarios = []
        for index in range(0, len(codes), batch_size):
            batch = codes[index:index + batch_size]
            scenarios.append(Scenario({
                "batch_index": index // batch_size,
                "codebook_fingerprint": fingerprint,
                "code_ids": [item["code_id"] for item in batch],
                "codes_json": json.dumps(batch, ensure_ascii=False),
            }))
        question = QuestionFreeText(
            question_name=QUESTION_NAME,
            question_text=QUESTION_TEXT + "\n\nBatch:\n{{ codes_json }}",
        )
        job = Jobs(survey=question.to_survey()).by(ScenarioList(scenarios))
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            target.unlink()
        saved = job.git.save(target)
        Jobs.git.load(target)
        models_target = None
        if model:
            models_target = target.with_name("consolidation.models.ep")
            if models_target.exists() and not force:
                raise BewleyError(f"{models_target} already exists", code="ALREADY_EXISTS")
            if models_target.exists():
                models_target.unlink()
            ModelList([Model(model, max_tokens=max_tokens)]).git.save(models_target)
        data = {
            "output": str(target),
            "codebook_fingerprint": fingerprint,
            "code_count": len(codes),
            "batch_count": len(scenarios),
            "expected_model_calls": len(scenarios),
            "batch_size": batch_size,
            "saved": saved,
            "models": {"output": str(models_target) if models_target else None, "model": model},
        }
    except (BewleyError, OSError) as exc:
        fail(command, exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="IO_ERROR"), json_flag)
        return
    run = ["ep", "run", str(target), "--model", model or "<model-name>", "--output", str(target.with_name("consolidation.results.ep"))]
    finish(command, data, next_actions=[action(
        "run-consolidation-jobs", "Run consolidation externally", run,
        mutates_state=True, requires_network=True, requires_user_approval=True,
    )])


@app.command("ingest")
def ingest_command(
    results: Path = typer.Argument(...),
    jobs: Path = typer.Option(Path("consolidation.jobs.ep"), "--jobs"),
    output: Path = typer.Option(Path("qualitative-analysis/consolidation_candidates.csv"), "--output", "-o"),
    force: bool = typer.Option(False, "--force"),
    human: bool = HumanOption,
) -> None:
    """Validate consolidation Results and create a reviewable proposal queue."""
    command = "codebook consolidate ingest"
    json_flag = should_emit_json(human)
    project = get_project(command, json_flag)
    result_path, jobs_path, target = (_path(project.root, item) for item in (results, jobs, output))
    try:
        if target.exists() and not force:
            raise BewleyError(f"{target} already exists", code="ALREADY_EXISTS", hint="Use --force to replace it.")
        Jobs, _, _, _, Results, _, _ = _edsl()
        job = Jobs.git.load(jobs_path)
        expected = {int(dict(s)["batch_index"]) for s in job.scenarios}
        valid_codes = {item["code_id"] for item in _code_payload(project)}
        known_annotations = set()
        with project.connect() as conn:
            known_annotations = {row["annotation_id"] for row in conn.execute("SELECT annotation_id FROM annotations WHERE is_active=1")}
        rows = []
        seen_batches = set()
        used_sources: set[str] = set()
        all_targets: set[str] = set()
        failures = []
        for result in Results.git.load(result_path):
            scenario = dict(result["scenario"])
            batch_index = int(scenario["batch_index"])
            seen_batches.add(batch_index)
            allowed = set(scenario["code_ids"])
            raw = _result_value(result, "answer", QUESTION_NAME)
            exception = _result_value(result, "exceptions", QUESTION_NAME)
            try:
                if exception:
                    raise ValueError("model exception")
                proposals = _parse_proposals(raw)
                for proposal_index, proposal in enumerate(proposals):
                    sources = set(proposal["source_code_ids"])
                    target_id = proposal["target_code_id"]
                    if target_id in sources or not sources | {target_id} <= allowed:
                        raise ValueError("proposal references invalid batch code ids")
                    if sources & used_sources:
                        raise ValueError("a source code appears in multiple proposals")
                    if not set(proposal["evidence_annotation_ids"]) <= known_annotations:
                        raise ValueError("proposal references unknown evidence annotations")
                    used_sources |= sources
                    all_targets.add(target_id)
                    candidate_id = hashlib.sha256(
                        f"{scenario['codebook_fingerprint']}:{batch_index}:{proposal_index}:{sorted(sources)}:{target_id}".encode()
                    ).hexdigest()[:16]
                    rows.append({
                        "candidate_id": candidate_id,
                        "codebook_fingerprint": scenario["codebook_fingerprint"],
                        "batch_index": batch_index,
                        "source_code_ids": json.dumps(sorted(sources)),
                        "target_code_id": target_id,
                        "rationale": proposal["rationale"],
                        "confidence": proposal["confidence"],
                        "evidence_annotation_ids": json.dumps(proposal["evidence_annotation_ids"]),
                    })
            except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
                failures.append({"batch_index": batch_index, "error": str(exc)})
        if failures or seen_batches != expected:
            raise BewleyError(
                "Consolidation results failed validation; no queue was written.",
                code="INCOMPLETE_RESULTS",
                context={"failures": failures, "missing_batches": sorted(expected - seen_batches)},
            )
        if all_targets & used_sources:
            raise BewleyError(
                "A proposed target is also a source in another merge.",
                code="CONFLICTING_PROPOSALS",
                context={"code_ids": sorted(all_targets & used_sources)},
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        fields = ["candidate_id", "codebook_fingerprint", "batch_index", "source_code_ids", "target_code_id", "rationale", "confidence", "evidence_annotation_ids"]
        with target.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
        log = target.parent / "consolidation_ingest_log.jsonl"
        with log.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"ingested_at": utcnow(), "results": str(result_path), "candidates": rows}) + "\n")
        data = {"output": str(target), "candidate_count": len(rows), "ingest_log": str(log)}
    except (BewleyError, OSError) as exc:
        fail(command, exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="IO_ERROR"), json_flag)
        return
    finish(command, data)


def _load_candidates(project, input_csv: Path) -> tuple[Path, list[dict[str, str]]]:
    source = _path(project.root, input_csv)
    if not source.exists():
        raise BewleyError(f"{source} does not exist", code="NOT_FOUND")
    with source.open(newline="", encoding="utf-8") as handle:
        return source, list(csv.DictReader(handle))


@app.command("candidates")
def candidates_command(
    input_csv: Path = typer.Option(Path("qualitative-analysis/consolidation_candidates.csv"), "--input", "-i"),
    human: bool = HumanOption,
) -> None:
    """List consolidation proposals and their review decisions."""
    command = "codebook consolidate candidates"
    json_flag = should_emit_json(human)
    project = get_project(command, json_flag)
    try:
        source, rows = _load_candidates(project, input_csv)
        decisions = project.consolidation_decisions()
        names = {item["code_id"]: item["name"] for item in _code_payload(project)}
        for row in rows:
            row["source_names"] = [names.get(x, x) for x in json.loads(row["source_code_ids"])]
            row["target_name"] = names.get(row["target_code_id"], row["target_code_id"])
            verdict = decisions.get(row["candidate_id"])
            row["decision"] = verdict["decision"] if verdict else ""
            row["decision_reason"] = verdict.get("reason", "") if verdict else ""
        data = {
            "input": str(source),
            "candidate_count": len(rows),
            "undecided_count": sum(not row["decision"] for row in rows),
            "candidates": rows,
        }
    except (BewleyError, OSError) as exc:
        fail(command, exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="IO_ERROR"), json_flag)
        return
    finish(command, data)


@app.command("review")
def review_command(
    candidate_ref: Optional[str] = typer.Argument(None),
    decision: str = typer.Option(..., "--decision", help="accept | reject"),
    reason: Optional[str] = typer.Option(None, "--reason"),
    all_remaining: bool = typer.Option(False, "--all-remaining"),
    input_csv: Path = typer.Option(Path("qualitative-analysis/consolidation_candidates.csv"), "--input", "-i"),
    human: bool = HumanOption,
) -> None:
    """Record accept/reject decisions for consolidation proposals."""
    command = "codebook consolidate review"
    json_flag = should_emit_json(human)
    project = get_project(command, json_flag)
    try:
        if decision not in {"accept", "reject"}:
            raise BewleyError("--decision must be accept or reject", code="INVALID_INPUT")
        if (candidate_ref is None) == (not all_remaining):
            raise BewleyError("provide a candidate id or --all-remaining", code="INVALID_INPUT")
        _, rows = _load_candidates(project, input_csv)
        existing = project.consolidation_decisions()
        if all_remaining:
            targets = [row["candidate_id"] for row in rows if row["candidate_id"] not in existing]
        else:
            targets = sorted({row["candidate_id"] for row in rows if row["candidate_id"].startswith(candidate_ref)})
            if len(targets) != 1:
                raise BewleyError("candidate id is missing or ambiguous", code="AMBIGUOUS_CANDIDATE")
        if not targets:
            raise BewleyError("no undecided candidates", code="INVALID_INPUT")
        for target in targets:
            project.record_consolidation_decision(target, decision, reason)
        remaining = len([row for row in rows if row["candidate_id"] not in existing and row["candidate_id"] not in targets])
        data = {"decision": decision, "recorded": targets, "undecided_remaining": remaining}
    except (BewleyError, OSError) as exc:
        fail(command, exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="IO_ERROR"), json_flag)
        return
    finish(command, data)


@app.command("apply")
def apply_command(
    input_csv: Path = typer.Option(Path("qualitative-analysis/consolidation_candidates.csv"), "--input", "-i"),
    dry_run: bool = typer.Option(False, "--dry-run"),
    human: bool = HumanOption,
) -> None:
    """Preview or apply reviewed consolidation merges."""
    command = "codebook consolidate apply"
    json_flag = should_emit_json(human)
    project = get_project(command, json_flag)
    try:
        source, rows = _load_candidates(project, input_csv)
        decisions = project.consolidation_decisions()
        undecided = [row["candidate_id"] for row in rows if row["candidate_id"] not in decisions]
        if undecided:
            raise BewleyError(
                "Consolidation proposals remain undecided.",
                code="UNDECIDED_CANDIDATES",
                context={"candidate_ids": undecided[:20], "count": len(undecided)},
            )
        fingerprints = {row["codebook_fingerprint"] for row in rows}
        if len(fingerprints) > 1 or (fingerprints and _fingerprint(project) not in fingerprints):
            raise BewleyError("The codebook changed after proposals were generated.", code="STALE_CODEBOOK")
        accepted = [row for row in rows if decisions[row["candidate_id"]]["decision"] == "accept"]
        plan = [{
            "candidate_id": row["candidate_id"],
            "source_code_ids": json.loads(row["source_code_ids"]),
            "target_code_id": row["target_code_id"],
            "rationale": row["rationale"],
        } for row in accepted]
        events = []
        if not dry_run:
            for item in plan:
                event = project.merge_codes(item["source_code_ids"], item["target_code_id"])
                events.append(event["event_id"])
            log = source.parent / "consolidation_apply_log.jsonl"
            with log.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps({"applied_at": utcnow(), "input": str(source), "plan": plan, "events": events}) + "\n")
        data = {
            "input": str(source),
            "dry_run": dry_run,
            "accepted_merges": len(plan),
            "rejected_proposals": len(rows) - len(plan),
            "source_codes_merged": sum(len(item["source_code_ids"]) for item in plan),
            "plan": plan,
            "event_ids": events,
        }
    except (BewleyError, OSError) as exc:
        fail(command, exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="IO_ERROR"), json_flag)
        return
    finish(command, data)
