"""Structured corpus import commands."""
from __future__ import annotations

import ast
import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Optional

import typer

from bewley.artifacts import record_command_artifacts

from ..project import BewleyError, atomic_write_text, json_dumps, sha256_bytes
from .common import HumanOption, action, fail, finish, get_project, should_emit_json

app = typer.Typer(help="Import structured source data as text documents.")

_EDSL_PRIVATE_FIELD_PARTS = {
    "id", "uuid", "email", "name", "phone", "address", "prolific",
    "participantid", "respondentid", "submissionid", "responseid",
}


def _edsl_attributes(scenario: Any, answers: dict[str, Any], question: str) -> dict[str, str]:
    """Keep scalar study fields while excluding common respondent identifiers."""
    scenario_data = scenario if isinstance(scenario, dict) else dict(scenario or {})
    attributes: dict[str, str] = {}
    for namespace, values in (("scenario", scenario_data), ("answer", answers)):
        for raw_key, value in values.items():
            key = str(raw_key).strip()
            if not key or (namespace == "answer" and key == question):
                continue
            compact_key = re.sub(r"[^a-z0-9]", "", key.lower())
            parts = {part for part in re.split(r"[^a-z0-9]+", key.lower()) if part}
            if compact_key in _EDSL_PRIVATE_FIELD_PARTS or parts & _EDSL_PRIVATE_FIELD_PARTS:
                continue
            if not re.fullmatch(r"[A-Za-z0-9._-]+", key):
                continue
            if value is None or isinstance(value, (dict, list, tuple, set)):
                continue
            if isinstance(value, bool):
                rendered = str(value).lower()
            elif isinstance(value, (str, int, float)):
                rendered = str(value).strip()
            else:
                continue
            if rendered:
                attributes[f"record.{namespace}.{key}"] = rendered
    return attributes


@app.command("edsl-results")
def import_edsl_results(
    source: Path = typer.Argument(..., help="EDSL Results package (.ep)."),
    question: str = typer.Option(..., "--question", help="Interview question whose answer contains role/content turns."),
    output_dir: Path = typer.Option(Path("corpus/edsl-import"), "--output-dir", help="New directory for rendered transcripts."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate the Results package without writing documents or events."),
    human: bool = HumanOption,
) -> None:
    """Import interview turns directly from an EDSL Results package."""
    command = "import edsl-results"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        source_path = source if source.is_absolute() else project.root / source
        target_dir = output_dir if output_dir.is_absolute() else project.root / output_dir
        if source_path.suffix.lower() != ".ep" or not source_path.is_file():
            raise BewleyError("Source must be an existing EDSL Results .ep file.", code="INVALID_INPUT")
        if not target_dir.resolve().is_relative_to(project.root.resolve()) or target_dir.resolve().is_relative_to(project.meta.resolve()):
            raise BewleyError("--output-dir must be inside the project and outside .bewley.", code="INVALID_INPUT")
        if target_dir.exists():
            raise BewleyError(f"{target_dir} already exists", code="ALREADY_EXISTS", hint="Choose a new --output-dir; imports preserve prior evidence.")
        try:
            from edsl import Results
            results = Results.git.load(source_path)
        except ImportError as exc:
            raise BewleyError("EDSL is required to read Results packages.", code="MISSING_DEPENDENCY") from exc
        if not results:
            raise BewleyError("The Results package has no response rows.", code="EMPTY_CORPUS")

        rendered: list[dict[str, Any]] = []
        parser_counts: dict[str, int] = {}
        role_types: dict[str, str] = {}
        for index, result in enumerate(results, 1):
            answers = result["answer"]
            turns = answers.get(question) if isinstance(answers, dict) else None
            if not isinstance(turns, list) or not turns:
                raise BewleyError("Every selected answer must contain a non-empty list of interview turns.",
                                  code="INVALID_TRANSCRIPT_STRUCTURE", context={"row": index, "question": question})
            text, parser, roles, turn_count = render_transcript(json.dumps(turns, ensure_ascii=False), source_format="json")
            if not text.strip() or parser != "json":
                raise BewleyError("Could not render the selected EDSL interview answer.",
                                  code="INVALID_TRANSCRIPT_STRUCTURE", context={"row": index, "question": question})
            attributes = _edsl_attributes(result.get("scenario"), answers, question)
            parser_counts[parser] = parser_counts.get(parser, 0) + 1
            role_types.update(roles)
            rendered.append({"row": index, "filename": f"interview-{index:03d}.txt", "text": text,
                             "turn_count": turn_count, "attributes": attributes})

        source_sha256 = sha256_bytes(source_path.read_bytes())
        data: dict[str, Any] = {
            "source": str(source_path), "source_sha256": source_sha256,
            "row_count": len(rendered), "document_count": len(rendered),
            "question": question, "format": "edsl-results", "detected_parsers": parser_counts,
            "speaker_roles": role_types, "excluded_fields": sorted(_EDSL_PRIVATE_FIELD_PARTS),
            "preview": {"first_document": rendered[0]["filename"], "speaker_labels": sorted(role_types),
                        "turn_count": rendered[0]["turn_count"]},
            "output_dir": str(target_dir), "dry_run": dry_run,
        }
        if dry_run:
            finish(command, data)
            return

        from bewley.sources import add_source, set_metadata
        source_info = add_source(project, source_path)
        target_dir.mkdir(parents=True, exist_ok=False)
        documents = []
        for item in rendered:
            path = target_dir / item["filename"]
            atomic_write_text(path, item["text"])
            event = project.add_document(path)
            document_id = event["payload"]["document_id"]
            project.segment_document(document_id)
            for field, value in item["attributes"].items():
                set_metadata(project, document_id, field, value, None, "catalog", None,
                             f"edsl-results:{source_sha256}:row:{item['row']}:field:{field}", "accepted")
            documents.append({"row": item["row"], "document_id": document_id,
                              "path": event["payload"]["current_path"], "turn_count": item["turn_count"],
                              "attributes": item["attributes"]})
        for label, role in sorted(role_types.items()):
            project.set_speaker_role(label, role)
        import_id = hashlib.sha256(f"{source_sha256}:{question}:{output_dir}".encode()).hexdigest()[:16]
        manifest_path = project.root / "qualitative-analysis" / "imports" / f"{import_id}-edsl.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_text(manifest_path, json_dumps({**data, "dry_run": False,
                                                      "source_artifact": source_info, "documents": documents}))
        event = project.append_event("edsl_results_imported", {
            "import_id": import_id, "source_sha256": source_sha256,
            "manifest_path": str(manifest_path.relative_to(project.root)),
            "question": question, "document_count": len(documents),
        })
        data.update({"import_id": import_id, "manifest": str(manifest_path),
                     "event_id": event["event_id"], "documents": documents})
        record_command_artifacts(project, command, locals())
    except (BewleyError, OSError, UnicodeError, ValueError, TypeError, KeyError) as exc:
        error = exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="INVALID_INPUT")
        fail(command, error, json_flag)
        return
    if json_flag:
        finish(command, data)
    else:
        typer.echo(f"Imported {data['document_count']} interview transcripts")

_TURN_KEYS = {"role", "content"}
_ROLE_MAP = {
    "interviewer": ("INTERVIEWER", "interviewer"),
    "respondent": ("RESPONDENT", "participant"),
    "participant": ("RESPONDENT", "participant"),
}


def _content_text(value: Any) -> str:
    """Flatten common nested message-content blocks into ordinary text."""
    if isinstance(value, str):
        text = value.strip()
        if text.startswith(("[", "{")):
            for parser in (json.loads, ast.literal_eval):
                try:
                    nested = parser(text)
                except (json.JSONDecodeError, SyntaxError, ValueError):
                    continue
                if isinstance(nested, (list, dict)):
                    return _content_text(nested)
        return text
    if isinstance(value, list):
        return "\n".join(
            part for item in value if (part := _content_text(item))
        )
    if isinstance(value, dict):
        if "text" in value:
            return _content_text(value["text"])
        if "content" in value:
            return _content_text(value["content"])
        raise BewleyError(
            "Turn content contains an object without a text or content field.",
            code="INVALID_TRANSCRIPT_STRUCTURE",
        )
    if value is None:
        return ""
    raise BewleyError(
        f"Unsupported turn content type: {type(value).__name__}",
        code="INVALID_TRANSCRIPT_STRUCTURE",
    )


def parse_turns(raw: str, source_format: str = "auto") -> tuple[list[dict[str, str]] | None, str]:
    """Parse a serialized list of role/content turns, or return ``None`` for plain text."""
    text = raw.strip()
    if not text:
        return [], "empty"
    parsers = []
    if source_format in {"auto", "json"}:
        parsers.append(("json", json.loads))
    if source_format in {"auto", "python"}:
        parsers.append(("python", ast.literal_eval))
    if source_format == "plain":
        return None, "plain"
    for name, parser in parsers:
        try:
            value = parser(text)
        except (json.JSONDecodeError, SyntaxError, ValueError):
            continue
        if not isinstance(value, list) or not all(
            isinstance(item, dict) and _TURN_KEYS.issubset(item) for item in value
        ):
            if source_format != "auto":
                raise BewleyError(
                    f"{name} value is not a list of role/content turn objects.",
                    code="INVALID_TRANSCRIPT_STRUCTURE",
                )
            continue
        turns = []
        for item in value:
            content = _content_text(item.get("content"))
            if content:
                turns.append({"role": str(item["role"]).strip(), "content": content})
        return turns, name
    if source_format != "auto":
        raise BewleyError(
            f"Could not parse transcript as {source_format}.",
            code="INVALID_TRANSCRIPT_STRUCTURE",
        )
    return None, "plain"


def _speaker_label(role: str) -> tuple[str, str]:
    normalized = role.strip().lower()
    if normalized in _ROLE_MAP:
        return _ROLE_MAP[normalized]
    label = re.sub(r"[^A-Z0-9]+", "_", role.upper()).strip("_") or "OTHER"
    return label, "other"


def render_transcript(
    raw_transcript: str,
    *,
    feedback: str = "",
    source_format: str = "auto",
) -> tuple[str, str, dict[str, str], int]:
    turns, parser = parse_turns(raw_transcript, source_format)
    roles: dict[str, str] = {}
    blocks: list[str] = []
    if turns is None:
        content = raw_transcript.strip()
        if content:
            blocks.append(f"RESPONDENT: {content}")
            roles["RESPONDENT"] = "participant"
        turn_count = 1 if content else 0
    else:
        for turn in turns:
            label, role_type = _speaker_label(turn["role"])
            roles[label] = role_type
            blocks.append(f"{label}: {turn['content']}")
        turn_count = len(turns)
    if feedback.strip():
        roles["RESPONDENT"] = "participant"
        blocks.append(f"RESPONDENT: [Feedback about the AI interviewer]\n{feedback.strip()}")
        turn_count += 1
    return "\n\n".join(blocks).rstrip() + "\n", parser, roles, turn_count


def looks_like_serialized_transcript(text: str) -> bool:
    turns, parser = parse_turns(text, "auto")
    if parser in {"json", "python"} and turns is not None:
        return True
    start, end = text.find("["), text.rfind("]")
    if start >= 0 and end > start:
        turns, parser = parse_turns(text[start : end + 1], "auto")
        return parser in {"json", "python"} and turns is not None
    return False


@app.command("survey-csv")
def survey_csv_command(
    source: Path = typer.Argument(..., help="Source survey response CSV."),
    transcript_column: str = typer.Option(..., "--transcript-column", help="Column containing transcript turns or plain text."),
    feedback_column: Optional[str] = typer.Option(None, "--feedback-column", help="Optional additional respondent feedback column."),
    source_format: str = typer.Option("auto", "--format", help="auto | json | python | plain"),
    output_dir: Path = typer.Option(Path("corpus/survey-import"), "--output-dir", help="New directory for rendered transcripts."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate and preview without writing files or events."),
    human: bool = HumanOption,
) -> None:
    """Import one CSV row per document, flattening structured interview turns."""
    command = "import survey-csv"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        if source_format not in {"auto", "json", "python", "plain"}:
            raise BewleyError("--format must be auto, json, python, or plain", code="INVALID_INPUT")
        source_path = source if source.is_absolute() else project.root / source
        target_dir = output_dir if output_dir.is_absolute() else project.root / output_dir
        if not source_path.exists():
            raise BewleyError(f"{source_path} does not exist", code="NOT_FOUND")
        if target_dir.exists():
            raise BewleyError(
                f"{target_dir} already exists",
                code="ALREADY_EXISTS",
                hint="Choose a new --output-dir; imports never overwrite prior evidence.",
            )
        with source_path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            columns = reader.fieldnames or []
            required = [transcript_column, *([feedback_column] if feedback_column else [])]
            missing = [name for name in required if name not in columns]
            if missing:
                raise BewleyError(
                    "Selected CSV columns do not exist.",
                    code="INVALID_INPUT",
                    context={"missing_columns": missing, "available_columns": columns},
                )
            rows = list(reader)
        if not rows:
            raise BewleyError("The CSV has no response rows.", code="EMPTY_CORPUS")

        rendered: list[dict[str, Any]] = []
        parser_counts: dict[str, int] = {}
        role_types: dict[str, str] = {}
        malformed: list[dict[str, Any]] = []
        for number, row in enumerate(rows, 1):
            try:
                text, parser, roles, turn_count = render_transcript(
                    row.get(transcript_column, ""),
                    feedback=row.get(feedback_column, "") if feedback_column else "",
                    source_format=source_format,
                )
                if not text.strip():
                    raise BewleyError("row produced an empty document", code="INVALID_TRANSCRIPT_STRUCTURE")
            except BewleyError as exc:
                malformed.append({"row": number, "error": exc.message})
                continue
            parser_counts[parser] = parser_counts.get(parser, 0) + 1
            role_types.update(roles)
            rendered.append({
                "row": number,
                "filename": f"respondent-{number:03d}.txt",
                "text": text,
                "parser": parser,
                "turn_count": turn_count,
            })
        if malformed:
            raise BewleyError(
                "One or more CSV rows could not be rendered; nothing was imported.",
                code="INVALID_TRANSCRIPT_STRUCTURE",
                context={"malformed_rows": malformed},
            )

        source_sha256 = sha256_bytes(source_path.read_bytes())
        preview = {
            "first_document": rendered[0]["filename"],
            "speaker_labels": sorted(role_types),
            "turn_count": rendered[0]["turn_count"],
        }
        data: dict[str, Any] = {
            "source": str(source_path),
            "source_sha256": source_sha256,
            "row_count": len(rows),
            "document_count": len(rendered),
            "transcript_column": transcript_column,
            "feedback_column": feedback_column,
            "format": source_format,
            "detected_parsers": parser_counts,
            "speaker_roles": role_types,
            "excluded_columns": [name for name in columns if name not in required],
            "preview": preview,
            "dry_run": dry_run,
        }
        if dry_run:
            import_argv = [
                "bewley", "import", "survey-csv", str(source),
                "--transcript-column", transcript_column,
                "--format", source_format,
                "--output-dir", str(output_dir),
            ]
            if feedback_column:
                import_argv.extend(["--feedback-column", feedback_column])
            finish(
                command,
                data,
                next_actions=[action(
                    "import-survey-csv",
                    "Import the validated survey rows.",
                    import_argv,
                    mutates_state=True,
                )],
            )
            return

        target_dir.mkdir(parents=True, exist_ok=False)
        document_rows = []
        for item in rendered:
            target = target_dir / item["filename"]
            atomic_write_text(target, item["text"])
            event = project.add_document(target)
            project.segment_document(event["payload"]["document_id"])
            document_rows.append({
                "row": item["row"],
                "document_id": event["payload"]["document_id"],
                "path": event["payload"]["current_path"],
                "parser": item["parser"],
                "turn_count": item["turn_count"],
            })
        for label, role in sorted(role_types.items()):
            project.set_speaker_role(label, role)
        manifest = {
            **data,
            "dry_run": False,
            "documents": document_rows,
        }
        import_id = hashlib.sha256(
            f"{source_sha256}:{transcript_column}:{feedback_column}:{output_dir}".encode()
        ).hexdigest()[:16]
        manifest_path = project.root / "qualitative-analysis" / "imports" / f"{import_id}.json"
        atomic_write_text(manifest_path, json_dumps(manifest))
        event = project.append_event("survey_csv_imported", {
            "import_id": import_id,
            "source_path": str(source_path),
            "source_sha256": source_sha256,
            "manifest_path": str(manifest_path.relative_to(project.root)),
            "transcript_column": transcript_column,
            "feedback_column": feedback_column,
            "format": source_format,
            "document_count": len(document_rows),
            "excluded_columns": data["excluded_columns"],
        })
        data.update({
            "import_id": import_id,
            "manifest": str(manifest_path),
            "event_id": event["event_id"],
            "documents": document_rows,
        })
        if not dry_run:
            record_command_artifacts(project, command, locals())
    except (BewleyError, OSError, UnicodeError, csv.Error) as exc:
        error = exc if isinstance(exc, BewleyError) else BewleyError(str(exc), code="IO_ERROR")
        fail(command, error, json_flag)
        return
    if json_flag:
        finish(command, data)
    else:
        typer.echo(f"Imported {data['document_count']} documents")
