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
from bewley.project import BewleyError, safe_decode, utcnow


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


DEFAULT_MAX_TOKENS = 4000


def _edsl() -> tuple[Any, Any, Any, Any, Any, Any, Any]:
    try:
        from edsl import Jobs, Model, ModelList, QuestionFreeText, Results, Scenario, ScenarioList
    except ImportError as exc:
        raise BewleyError(
            "EDSL is required for open-coding Jobs and Results.",
            code="MISSING_DEPENDENCY",
            hint="Install Bewley with its declared dependencies and retry.",
        ) from exc
    return Jobs, Model, ModelList, QuestionFreeText, Results, Scenario, ScenarioList


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




def _failed_document_ids(
    project: Any, Jobs: Any, Results: Any,
    results_source: Path, jobs_source: Path | None,
) -> set[str]:
    """Document IDs whose (scenario, model) pairs lack a valid answer.

    Includes scenarios the originating Jobs expected but that never returned,
    when the Jobs package is supplied as the denominator.
    """
    results = Results.git.load(results_source)
    expected: set[tuple[str, str]] = set()
    if jobs_source is not None:
        jobs = Jobs.git.load(jobs_source)
        expected = {_scenario_key(_scenario_dict(item)) for item in jobs.scenarios}
    seen_pairs: set[tuple[tuple[str, str], str]] = set()
    valid_pairs: set[tuple[tuple[str, str], str]] = set()
    with project.connect() as conn:
        for result in results:
            scenario = _scenario_dict(result["scenario"])
            key = _scenario_key(scenario)
            model_name = str(_result_value(result, "model", "model") or "")
            pair = (key, model_name)
            seen_pairs.add(pair)
            if _result_value(result, "exceptions", QUESTION_NAME):
                continue
            try:
                _parse_answer(_result_value(result, "answer", QUESTION_NAME))
                document = project.resolve_document(conn, key[0])
                revision = project.current_revision(conn, document["document_id"])
                if revision["revision_id"] != scenario.get("revision_id"):
                    continue
            except (ValueError, TypeError, KeyError, BewleyError):
                continue
            valid_pairs.add(pair)
    failed = {key[0] for key, _ in (seen_pairs - valid_pairs)}
    if expected:
        models_seen = {model for _, model in seen_pairs} or {""}
        for key in expected:
            for model in models_seen:
                if (key, model) not in seen_pairs:
                    failed.add(key[0])
    return failed


@app.command("jobs")
def jobs_command(
    output: Path = typer.Option(Path("jobs.ep"), "--output", "-o"),
    summary: Path = typer.Option(
        Path("qualitative-analysis/corpus_summary.md"), "--summary",
        help="Corpus context embedded in every scenario; optional if absent.",
    ),
    pilot: Optional[int] = typer.Option(None, "--pilot", min=1),
    model: Optional[str] = typer.Option(
        None, "--model",
        help="Also write models.ep for this model so the suggested ep run command is executable verbatim.",
    ),
    max_tokens: int = typer.Option(
        DEFAULT_MAX_TOKENS, "--max-tokens", min=1,
        help="Completion budget stored in models.ep; provider defaults truncate long JSON answers.",
    ),
    from_failures: Optional[Path] = typer.Option(
        None, "--from-failures",
        help="Repackage only scenarios lacking a valid answer in this Results file.",
    ),
    jobs_denominator: Optional[Path] = typer.Option(
        None, "--jobs",
        help="With --from-failures: the originating Jobs, so never-returned scenarios are included.",
    ),
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
        Jobs, Model, ModelList, QuestionFreeText, _, Scenario, ScenarioList = _edsl()
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
        failed_documents: Optional[int] = None
        if from_failures is not None:
            failures_source = _path(project.root, from_failures)
            if not failures_source.exists():
                raise BewleyError(f"{failures_source} does not exist", code="NOT_FOUND")
            denominator = _path(project.root, jobs_denominator) if jobs_denominator else None
            if denominator is not None and not denominator.exists():
                raise BewleyError(f"{denominator} does not exist", code="NOT_FOUND")
            _, _, _, _, Results, _, _ = _edsl()
            failed_ids = _failed_document_ids(project, Jobs, Results, failures_source, denominator)
            scenarios = [item for item in scenarios if dict(item).get("document_id") in failed_ids]
            failed_documents = len(scenarios)
            if not scenarios:
                raise BewleyError(
                    "No failed scenarios to re-run; every document already has a valid answer.",
                    code="INVALID_INPUT",
                    context={"from_failures": str(failures_source)},
                )
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
        Jobs.git.load(target)  # prove the package can be reloaded before reporting success
        expected = target.with_name("results.ep")
        models_target: Optional[Path] = None
        if model is not None:
            models_target = target.with_name("models.ep")
            if models_target.exists() and not force:
                raise BewleyError(
                    f"{models_target} already exists", code="ALREADY_EXISTS", hint="Use --force to replace it.",
                )
            if models_target.exists():
                models_target.unlink()
            model_list = ModelList([Model(model, max_tokens=max_tokens)])
            model_list.git.save(models_target)
            ModelList.git.load(models_target)
        data = {
            "object_type": "Jobs",
            "output": str(target),
            "question": QUESTION_NAME,
            "scenario_count": len(scenarios),
            "expected_model_calls": len(scenarios),
            "pilot": pilot is not None,
            "from_failures": str(from_failures) if from_failures is not None else None,
            "failed_documents": failed_documents,
            "saved": saved,
            "expected_results": str(expected),
            "models": {
                "output": str(models_target) if models_target else None,
                "model": model,
                "max_tokens": max_tokens if model is not None else None,
            },
            "inference": "external",
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
    if model is not None:
        run_argv = ["ep", "run", str(target), "--model_list", str(models_target), "--output", str(expected)]
    else:
        run_argv = ["ep", "run", str(target), "--model", "<model-name>", "--output", str(expected)]
    next_action = action(
        "run-open-coding-jobs", "Run the packaged jobs with the external ep CLI",
        run_argv,
        mutates_state=True, requires_network=True, requires_user_approval=True,
    )
    if json_flag:
        finish(command, data, next_actions=[next_action])
    else:
        print(f"Jobs written to: {target}")
        print("Run: " + " ".join(next_action["command"]))


@app.command("ingest")
def ingest_command(
    results_paths: list[Path] = typer.Argument(
        ..., help="Results .ep files in run order; later files supply retries.",
    ),
    jobs_path: Optional[Path] = typer.Option(None, "--jobs", help="Originating Jobs package for coverage audit."),
    output: Path = typer.Option(Path("qualitative-analysis/candidate_codes.csv"), "--output", "-o"),
    allow_partial: bool = typer.Option(False, "--allow-partial"),
    force: bool = typer.Option(False, "--force"),
    human: bool = HumanOption,
) -> None:
    """Audit Results (merging retries by stable identity) and write candidates.

    Rows merge by (scenario, model): the first valid answer wins and is
    attributed to its source file, later valid answers from retry files are
    counted as superseded (a warning, not a failure), and a pair is a failure
    only when no file supplies a valid answer for it.
    """
    json_flag = should_emit_json(human)
    command = "open-coding ingest"
    project = get_project(command, json_flag)
    sources = [_path(project.root, item) for item in results_paths]
    jobs_source = _path(project.root, jobs_path) if jobs_path else None
    target = _path(project.root, output)
    try:
        Jobs, _, _, _, Results, _, _ = _edsl()
        for source in sources:
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

        Pair = tuple[tuple[str, str], str]
        order: list[Pair] = []
        rows_by_pair: dict[Pair, list[tuple[int, Any, dict[str, Any]]]] = {}
        models: set[str] = set()
        total_rows = 0
        for file_index, source in enumerate(sources):
            for result in Results.git.load(source):
                total_rows += 1
                scenario = _scenario_dict(result["scenario"])
                key = _scenario_key(scenario)
                model_name = str(_result_value(result, "model", "model") or "")
                if model_name:
                    models.add(model_name)
                pair: Pair = (key, model_name)
                if pair not in rows_by_pair:
                    order.append(pair)
                    rows_by_pair[pair] = []
                rows_by_pair[pair].append((file_index, result, scenario))

        rows: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        unresolved_details: list[dict[str, Any]] = []
        stale = unresolved = duplicates = superseded = 0
        source_counts = {str(source): 0 for source in sources}
        with project.connect() as conn:
            for pair in order:
                key, model_name = pair
                valids: list[tuple[int, list[dict[str, Any]], dict[str, Any]]] = []
                last_error: str | None = None
                for file_index, result, scenario in rows_by_pair[pair]:
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
                        valids.append((file_index, entries, scenario))
                    except (ValueError, TypeError, KeyError, BewleyError) as exc:
                        last_error = str(exc)
                if not valids:
                    failures.append({"document_id": key[0], "model": model_name, "error": last_error})
                    continue
                retained_index, entries, scenario = valids[0]
                for other_index, _, _ in valids[1:]:
                    if other_index == retained_index:
                        # A genuine duplicate inside one results file.
                        duplicates += 1
                    else:
                        # A retry re-answered an already-valid pair; expected.
                        superseded += 1
                source_label = str(sources[retained_index])
                source_counts[source_label] += 1
                document = project.resolve_document(conn, str(scenario.get("document_id")))
                current = project.current_revision(conn, document["document_id"])
                text = safe_decode((project.objects_dir / current["content_sha256"]).read_bytes())
                for entry_index, entry in enumerate(entries):
                    status, start_byte, end_byte = _resolve_quote(text, entry["quote"])
                    candidate_id = hashlib.sha256(
                        f"{key[0]}:{key[1]}:{entry_index}:{entry['code']}:{entry['quote']}".encode()
                    ).hexdigest()[:16]
                    if status != "exact":
                        unresolved += 1
                        unresolved_details.append({
                            "candidate_id": candidate_id,
                            "code_name": entry["code"].strip(),
                            "resolve_status": status,
                            "document_path": scenario.get("document_path", ""),
                            "quote_prefix": entry["quote"][:120],
                        })
                    rows.append({
                        "candidate_id": candidate_id,
                        "code_name": entry["code"].strip(),
                        "description": entry["description"].strip(),
                        "quote": entry["quote"],
                        "source_document_id": key[0],
                        "source_document_path": scenario.get("document_path", ""),
                        "source_revision_id": key[1],
                        "byte_start": "" if start_byte is None else start_byte,
                        "byte_end": "" if end_byte is None else end_byte,
                        "resolve_status": status,
                        "source_results": source_label,
                    })
        # Row identity includes the model, so a multi-model run audits as
        # scenarios × models instead of reporting every scenario as duplicated.
        model_names = models or {""}
        expected_pairs = {(key, name) for key in expected for name in model_names} if expected else set()
        missing = expected_pairs - set(order) if expected else set()
        incomplete = bool(failures or missing or duplicates)
        if incomplete and not allow_partial:
            raise BewleyError(
                "Results failed validation; no candidate CSV was written.",
                code="INCOMPLETE_RESULTS",
                context={
                    "failures": failures[:10],
                    "missing_answers": len(missing),
                    "duplicate_answers": duplicates,
                },
                hint=(
                    "Rebuild a retry package with `bewley open-coding jobs --from-failures "
                    f"{sources[0]}` and pass both results files to ingest, or use --allow-partial."
                ),
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            "candidate_id", "code_name", "description", "quote", "source_document_id",
            "source_document_path", "source_revision_id", "byte_start", "byte_end",
            "resolve_status", "source_results",
        ]
        with target.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        # Append-only provenance: the CSV is a working file the reviewer edits
        # (deleting rejected rows), so the original proposal set survives here.
        ingest_log = target.parent / "ingest_log.jsonl"
        with ingest_log.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({
                "ingested_at": utcnow(),
                "results": [str(source) for source in sources],
                "output": str(target),
                "candidates": [
                    {
                        "candidate_id": row["candidate_id"],
                        "code_name": row["code_name"],
                        "source_document_path": row["source_document_path"],
                        "resolve_status": row["resolve_status"],
                    }
                    for row in rows
                ],
            }) + "\n")
        warnings_list = []
        if superseded:
            warnings_list.append(
                f"{superseded} already-valid answer(s) were re-run in retry files; the first valid answer was retained."
            )
        data = {
            "object_type": "CandidateCodes",
            "output": str(target),
            "ingest_log": str(ingest_log),
            "results": [str(source) for source in sources],
            "result_count": total_rows,
            "candidate_count": len(rows),
            "scenario_count": len(order),
            "models": sorted(models),
            "retained_by_source": source_counts,
            "expected_scenarios": len(expected) if expected else None,
            "expected_answers": len(expected_pairs) if expected else None,
            "missing_answers": len(missing) if expected else None,
            "duplicate_scenarios": duplicates,
            "superseded_answers": superseded,
            "failed_scenarios": len(failures),
            "stale_scenarios": stale,
            "unresolved_quotes": unresolved,
            "unresolved_details": unresolved_details,
            "partial": incomplete,
        }
    except (BewleyError, OSError) as exc:
        err = exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="IO_ERROR")
        fail(command, err, json_flag)
        return
    if json_flag:
        finish(command, data, warnings=warnings_list or None)
    else:
        print(f"Wrote {len(rows)} candidates to: {target}")


@app.command("candidates")
def candidates_command(
    input_csv: Path = typer.Option(
        Path("qualitative-analysis/candidate_codes.csv"), "--input", "-i",
        help="Candidate CSV produced by `open-coding ingest`.",
    ),
    human: bool = HumanOption,
) -> None:
    """List the proposed candidate codes awaiting review.

    This is the review queue: read it, delete rejected rows from the CSV,
    then run `open-coding apply`.
    """
    json_flag = should_emit_json(human)
    command = "open-coding candidates"
    project = get_project(command, json_flag)
    source = _path(project.root, input_csv)
    try:
        if not source.exists():
            raise BewleyError(
                f"{source} does not exist", code="NOT_FOUND",
                hint="Run `bewley open-coding ingest` first.",
            )
        with source.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        by_code: dict[str, int] = {}
        for row in rows:
            by_code[row["code_name"]] = by_code.get(row["code_name"], 0) + 1
        data = {
            "input": str(source),
            "candidate_count": len(rows),
            "proposed_codes": [
                {"code_name": name, "candidates": count}
                for name, count in sorted(by_code.items())
            ],
            "candidates": rows,
        }
    except (BewleyError, OSError) as exc:
        err = exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="IO_ERROR")
        fail(command, err, json_flag)
        return
    next_action = action(
        "apply-reviewed-candidates",
        "After deleting rejected rows from the CSV, preview the application",
        ["bewley", "open-coding", "apply", "--dry-run"],
        mutates_state=False,
    )
    if json_flag:
        finish(command, data, next_actions=[next_action])
        return
    from rich.table import Table

    from bewley.commands.common import rich_console

    table = Table(
        title=f"{len(rows)} candidates across {len(by_code)} proposed codes",
        show_header=True, header_style="bold green",
    )
    table.add_column("Code", no_wrap=True)
    table.add_column("Description", overflow="fold", max_width=28)
    table.add_column("Quote", overflow="fold")
    table.add_column("Document", no_wrap=True)
    for row in sorted(rows, key=lambda item: (item["code_name"], item["source_document_path"])):
        quote = row["quote"]
        if len(quote) > 110:
            quote = quote[:110].rsplit(" ", 1)[0] + " …"
        document = Path(row["source_document_path"]).name.replace("-adams.txt", "")
        table.add_row(row["code_name"], row["description"], quote, document)
    rich_console().print(table)


@app.command("apply")
def apply_command(
    input_csv: Path = typer.Option(
        Path("qualitative-analysis/candidate_codes.csv"), "--input", "-i",
        help="Reviewed candidate CSV produced by `open-coding ingest`.",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run",
        help="Report the plan without creating codes or annotations.",
    ),
    human: bool = HumanOption,
) -> None:
    """Apply reviewed candidate rows as real codes and exact-span annotations.

    Only rows whose quotation resolved to exactly one location are applied.
    Everything skipped is itemized with a reason; nothing is guessed. Delete
    the rows you rejected during review before running this command.
    """
    json_flag = should_emit_json(human)
    command = "open-coding apply"
    project = get_project(command, json_flag)
    source = _path(project.root, input_csv)
    try:
        if not source.exists():
            raise BewleyError(f"{source} does not exist", code="NOT_FOUND")
        with source.open(newline="", encoding="utf-8") as handle:
            candidates = list(csv.DictReader(handle))
        skipped_details: list[dict[str, Any]] = []
        plan: list[dict[str, Any]] = []
        codes_to_create: dict[str, str] = {}
        with project.connect() as conn:
            existing_annotations = {
                (row["code_id"], row["document_id"], row["start_byte"], row["end_byte"])
                for row in conn.execute(
                    "SELECT code_id, document_id, start_byte, end_byte FROM annotations WHERE is_active = 1"
                )
            }
            code_ids = {
                row["canonical_name"]: row["code_id"]
                for row in conn.execute("SELECT code_id, canonical_name FROM codes")
            }
            for row in candidates:
                candidate_id = row.get("candidate_id", "")
                code_name = (row.get("code_name") or "").strip()

                def skip(reason: str) -> None:
                    skipped_details.append({
                        "candidate_id": candidate_id,
                        "code_name": code_name,
                        "reason": reason,
                    })

                if not code_name:
                    skip("missing_code_name")
                    continue
                if row.get("resolve_status") != "exact" or not row.get("byte_start") or not row.get("byte_end"):
                    skip(f"unresolved_quote:{row.get('resolve_status') or 'missing'}")
                    continue
                document_ref = row.get("source_document_id", "")
                try:
                    document = project.resolve_document(conn, document_ref)
                    revision = project.current_revision(conn, document["document_id"])
                except BewleyError:
                    skip("document_not_found")
                    continue
                if revision["revision_id"] != row.get("source_revision_id"):
                    # Byte offsets were resolved against the ingested revision;
                    # a newer revision invalidates them rather than being guessed.
                    skip("stale_revision")
                    continue
                start, end = int(row["byte_start"]), int(row["byte_end"])
                known_code_id = code_ids.get(code_name)
                if known_code_id is not None and (
                    known_code_id, document["document_id"], start, end
                ) in existing_annotations:
                    skip("already_applied")
                    continue
                if code_name not in code_ids:
                    codes_to_create.setdefault(code_name, (row.get("description") or "").strip())
                plan.append({
                    "candidate_id": candidate_id,
                    "code_name": code_name,
                    "document_ref": document["document_id"],
                    "byte_start": start,
                    "byte_end": end,
                })
        applied = 0
        created_codes: list[str] = []
        if not dry_run:
            for code_name, description in codes_to_create.items():
                project.add_code(code_name, description=description or None)
                created_codes.append(code_name)
            for item in plan:
                project.add_annotation(
                    item["code_name"], item["document_ref"], "span",
                    (item["byte_start"], item["byte_end"]), None,
                )
                applied += 1
            with (source.parent / "apply_log.jsonl").open("a", encoding="utf-8") as handle:
                handle.write(json.dumps({
                    "applied_at": utcnow(),
                    "input": str(source),
                    "applied": [
                        {"candidate_id": item["candidate_id"], "code_name": item["code_name"]}
                        for item in plan
                    ],
                    "created_codes": created_codes,
                    "skipped": skipped_details,
                }) + "\n")
        data = {
            "input": str(source),
            "rows": len(candidates),
            "dry_run": dry_run,
            "codes_to_create": sorted(codes_to_create) if dry_run else created_codes,
            "annotations_planned": len(plan),
            "annotations_applied": applied,
            "skipped": len(skipped_details),
            "skipped_details": skipped_details,
        }
    except (BewleyError, OSError) as exc:
        err = exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="IO_ERROR")
        fail(command, err, json_flag)
        return
    next_action = action(
        "review-applied-coding",
        "Inspect the applied codes and evidence",
        ["bewley", "show", "snippets", "--code", "<code-name>"],
        mutates_state=False,
    ) if not dry_run else action(
        "apply-for-real",
        "Apply the reviewed plan",
        ["bewley", "open-coding", "apply", "--input", str(input_csv)],
        mutates_state=True,
    )
    if json_flag:
        finish(command, data, next_actions=[next_action])
    else:
        verb = "Would apply" if dry_run else "Applied"
        print(f"{verb} {len(plan)} annotations ({len(skipped_details)} skipped) from {source}")
