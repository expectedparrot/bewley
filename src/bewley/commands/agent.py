from __future__ import annotations

import json
from importlib.resources import files
from pathlib import Path
from typing import Any

import typer

from bewley import __version__
from bewley.commands.common import ENVELOPE_SCHEMA_VERSION, action, finish
from bewley.project import BewleyError, Project, _phase_state

app = typer.Typer(help="Inspect Bewley's agent-facing contract and workflow state.")

SCHEMA_VERSION = "1.0"
SCHEMA_NAMES = {
    "action": "action.schema.json",
    "agent-status": "agent-status.schema.json",
    "envelope": "envelope.schema.json",
}


def _schema_path(name: str) -> Any:
    filename = SCHEMA_NAMES.get(name, name)
    if filename not in SCHEMA_NAMES.values():
        raise typer.BadParameter(f"unknown schema: {name}")
    return files("bewley").joinpath("schemas", filename)


@app.command("schema")
def agent_schema(name: str = typer.Argument(..., help="Schema name: envelope, action, or agent-status.")) -> None:
    """Return one bundled, versioned JSON Schema."""
    resource = _schema_path(name)
    finish("agent schema", {"name": resource.name, "schema": json.loads(resource.read_text(encoding="utf-8"))})


@app.command("status")
def agent_status() -> None:
    """Return project phase, blockers, and executable next actions."""
    try:
        project = Project.discover()
    except BewleyError:
        project = None
    state = _phase_state(project, project is not None)
    next_actions = []
    for index, item in enumerate(state.pop("recommended_next_steps"), start=1):
        raw = item["command"].split()
        mutates_state = (
            raw[:1] == ["python"]
            or (raw[:1] == ["bewley"] and raw[1:2] in (["init"], ["add"], ["codegen"], ["export"], ["open-coding"]))
        )
        next_actions.append(
            action(
                f"phase-{index}",
                item["label"],
                raw,
                mutates_state=mutates_state,
            )
        )
    data = {
        "schema_version": SCHEMA_VERSION,
        **state,
        "ready": project is not None,
        "next_actions": next_actions,
        "blockers": [] if project else [{"code": "PROJECT_NOT_FOUND", "message": "Initialize a Bewley project."}],
    }
    finish("agent status", data)


def capabilities_data() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "interface": "agent-first-json",
        "default_output": "json-envelope",
        "human_output_opt_in": ["--human", "-H"],
        "schemas": sorted(SCHEMA_NAMES.values()),
        "commands": {
            "agent_status": ["bewley", "agent", "status"],
            "schema": ["bewley", "agent", "schema", "<name>"],
            "open_coding_jobs": ["bewley", "open-coding", "jobs", "--output", "jobs.ep"],
            "open_coding_ingest": ["bewley", "open-coding", "ingest", "results.ep", "--jobs", "jobs.ep"],
        },
        "safety": {
            "next_step_commands_are_argv_arrays": True,
            "mutations_are_declared": True,
            "unexpected_errors_are_enveloped": True,
        },
    }


def capabilities_command() -> None:
    """Describe the stable agent-facing CLI contract."""
    finish("capabilities", capabilities_data())


def version_command() -> None:
    """Report the installed Bewley build and contract versions."""
    finish("version", {
        "version": __version__,
        "package_path": str(files("bewley")),
        "envelope_schema_version": ENVELOPE_SCHEMA_VERSION,
        "agent_schema_version": SCHEMA_VERSION,
    })


def guide_command() -> None:
    """Describe the complete Bewley lifecycle and its execution boundary."""
    finish("guide", {
        "lifecycle": [
            {
                "stage": "initialize",
                "purpose": "Create the local project and import the pilot corpus.",
                "commands": ["bewley init", "bewley add corpus/<file>", "bewley list documents"],
            },
            {
                "stage": "package-open-coding",
                "purpose": "Package current document revisions as an EDSL Jobs object.",
                "commands": ["bewley open-coding jobs --output jobs.ep --model <model-name>"],
            },
            {
                "stage": "external-execution",
                "purpose": "Run the packaged model work explicitly outside Bewley.",
                "commands": ["ep run jobs.ep --model_list models.ep --output results.ep"],
                "requires_user_approval": True,
            },
            {
                "stage": "ingest-and-review",
                "purpose": "Audit Results against the originating Jobs and write candidate codes for human review.",
                "commands": [
                    "bewley open-coding ingest results.ep --jobs jobs.ep",
                    "bewley open-coding apply --dry-run",
                    "bewley open-coding apply",
                ],
            },
            {
                "stage": "codebook-and-annotation",
                "purpose": "Refine codes and attach evidence spans by hand as interpretation sharpens.",
                "commands": ["bewley code create", "bewley annotate apply", "bewley memo add"],
            },
            {
                "stage": "analyze-and-export",
                "purpose": "Compare coded evidence and export quotes, plots, theory, and HTML.",
                "commands": ["bewley query", "bewley export quotes", "bewley export html"],
            },
            {
                "stage": "integrity",
                "purpose": "Verify the event log and projections before using outputs downstream.",
                "commands": ["bewley fsck", "bewley rebuild-index"],
            },
        ],
        "execution_boundary": {
            "owner": "ep",
            "rule": "Bewley packages Jobs and consumes Results; it never executes packaged model calls.",
        },
        "operation_contracts": [
            {
                "id": "review-candidate",
                "purpose": "Record an append-only decision on an open-coding candidate.",
                "argv_templates": {
                    "accept": ["bewley", "open-coding", "review", "<candidate-ref>", "--decision", "accept", "--reason", "<reason>"],
                    "reject": ["bewley", "open-coding", "review", "<candidate-ref>", "--decision", "reject", "--reason", "<reason>"],
                    "map": ["bewley", "open-coding", "review", "<candidate-ref>", "--decision", "map", "--to", "<code-ref>", "--reason", "<reason>"],
                    "adjust": ["bewley", "open-coding", "review", "<candidate-ref>", "--decision", "adjust", "--bytes", "<start:end>", "--reason", "<reason>"],
                },
                "mutates_state": True,
                "output": "A JSON envelope; the decision is preserved in the append-only event log.",
            },
            {
                "id": "create-code",
                "purpose": "Create a code; use code update when the code already exists.",
                "argv_template": ["bewley", "code", "create", "<name>", "--description", "<definition>"],
                "required": ["name"],
                "optional": ["--description", "--color"],
                "mutates_state": True,
            },
            {
                "id": "update-code",
                "purpose": "Update an existing code's definition and criteria.",
                "argv_template": ["bewley", "code", "update", "<code-ref>", "--description", "<definition>", "--inclusion", "<criteria>", "--exclusion", "<criteria>"],
                "required": ["code-ref", "at least one update option"],
                "optional": ["--description", "--inclusion", "--exclusion"],
                "mutates_state": True,
            },
            {
                "id": "add-memo",
                "purpose": "Create a memo; content is positional rather than a --text option.",
                "argv_template": ["bewley", "memo", "add", "<content>", "--title", "<title>"],
                "required": ["content, or omit it to use $EDITOR"],
                "optional": ["--title", "--code", "--document"],
                "mutates_state": True,
            },
            {
                "id": "export-quotes",
                "purpose": "Write quotes to stdout; exactly one selector is required.",
                "argv_template": ["bewley", "export", "quotes", "--code", "<code-ref>", "--format", "jsonl"],
                "required": ["exactly one of --code, --query, --all"],
                "optional": ["--format", "--context-lines"],
                "mutates_state": False,
                "output": "Quote content is returned inside the JSON envelope; there is no --output option.",
            },
            {
                "id": "export-html",
                "purpose": "Write the project explorer to a caller-selected file.",
                "argv_template": ["bewley", "export", "html", "--output", "<path.html>"],
                "required": [],
                "optional": ["--output", "--title", "--static", "--embed"],
                "mutates_state": False,
                "output": "Writes --output (default: bewley-codes.html).",
            },
            {
                "id": "export-theory",
                "purpose": "Export the code hierarchy and links.",
                "argv_template": ["bewley", "export", "theory", "--format", "json", "--output", "<path.json>"],
                "required": [],
                "optional": ["--format", "--output"],
                "mutates_state": False,
                "output": "Returns content in the envelope unless --output is supplied.",
            },
        ],
        "resume": "Run `bewley next` after every material stage.",
        "documentation": {
            "overview": "bewley docs show overview",
            "workflow": "bewley docs show workflow",
            "commands": "bewley docs show commands",
        },
    })


def _artifact_next(project: Project) -> dict[str, Any] | None:
    """Recognize EDSL pipeline artifacts that the count-based phases cannot see."""
    root = project.root
    feedback_aggregate = root / "qualitative-analysis" / "feedback-aggregate.json"
    feedback_classifications = root / "qualitative-analysis" / "feedback-classifications.jsonl"
    feedback_codebook = root / "qualitative-analysis" / "feedback-codebook.json"
    if feedback_aggregate.exists():
        return {
            "stage": "feedback-analysis-complete",
            "artifacts": {"codebook": str(feedback_codebook), "classifications": str(feedback_classifications), "aggregate": str(feedback_aggregate)},
            "exists": {"codebook": feedback_codebook.exists(), "classifications": feedback_classifications.exists(), "aggregate": True},
            "recommendation": action("verify-feedback-analysis", "Verify project integrity before downstream reporting", ["bewley", "fsck"], mutates_state=False),
        }
    discovery = root / "runs" / "001-discovery"
    discovery_jobs = discovery / "jobs.ep"
    discovery_results = discovery / "results.ep"
    discovery_candidates = discovery / "candidates.jsonl"
    if discovery_jobs.exists() and not discovery_results.exists():
        return {
            "stage": "feedback-discovery-awaiting-external-results",
            "artifacts": {"jobs": str(discovery_jobs), "results": str(discovery_results)},
            "exists": {"jobs": True, "results": False, "candidates": discovery_candidates.exists()},
            "recommendation": action(
                "run-feedback-discovery", "Run bundled feedback-code discovery externally",
                ["ep", "run", "--jobs", str(discovery_jobs), "--output", str(discovery_results)],
                mutates_state=True, requires_network=True, requires_user_approval=True,
            ),
        }
    if discovery_results.exists() and not discovery_candidates.exists():
        return {
            "stage": "feedback-discovery-results-awaiting-ingest",
            "artifacts": {"jobs": str(discovery_jobs), "results": str(discovery_results), "candidates": str(discovery_candidates)},
            "exists": {"jobs": discovery_jobs.exists(), "results": True, "candidates": False},
            "recommendation": action(
                "ingest-feedback-discovery", "Validate bundled candidates and exact evidence",
                ["bewley", "insights", "discover", "ingest", str(discovery_results), "--jobs", str(discovery_jobs)],
                mutates_state=True,
            ),
        }
    artifacts = {
        "jobs": root / "jobs.ep",
        "models": root / "models.ep",
        "results": root / "results.ep",
        "candidates": root / "qualitative-analysis" / "candidate_codes.csv",
    }
    exists = {name: path.exists() for name, path in artifacts.items()}
    state = {
        "artifacts": {name: str(path) for name, path in artifacts.items()},
        "exists": exists,
    }
    if exists["jobs"] and not exists["results"]:
        model_list = "models.ep" if exists["models"] else "<models.ep from `bewley open-coding jobs --model <model-name>`>"
        state.update({
            "stage": "awaiting-external-results",
            "recommendation": action(
                "run-open-coding-jobs",
                "Run the packaged open-coding jobs with the external ep CLI",
                ["ep", "run", "jobs.ep", "--model_list", model_list, "--output", "results.ep"],
                mutates_state=True, requires_network=True, requires_user_approval=True,
            ),
        })
        return state
    if exists["results"] and not exists["candidates"]:
        state.update({
            "stage": "results-awaiting-ingest",
            "recommendation": action(
                "ingest-open-coding-results",
                "Audit the Results against the originating Jobs and write candidate codes",
                ["bewley", "open-coding", "ingest", "results.ep", "--jobs", "jobs.ep"],
                mutates_state=True,
            ),
        })
        return state
    if exists["candidates"]:
        with project.connect() as conn:
            code_count = conn.execute("SELECT COUNT(*) FROM codes").fetchone()[0]
        if code_count == 0:
            state.update({
                "stage": "candidates-awaiting-review",
                "recommendation": action(
                    "apply-reviewed-candidates",
                    "Review candidate_codes.csv, then apply the kept rows as codes and annotations",
                    ["bewley", "open-coding", "apply", "--dry-run"],
                    mutates_state=False,
                ),
            })
            return state
    return None


def next_command() -> None:
    """Return the single highest-priority next action from artifact state."""
    try:
        project = Project.discover()
    except BewleyError:
        project = None
    if project is None:
        finish("next", {
            "schema_version": SCHEMA_VERSION,
            "stage": "no-project",
            "ready": False,
            "blockers": [{"code": "PROJECT_NOT_FOUND", "message": "Initialize a Bewley project."}],
        }, next_actions=[
            action("init-project", "Create a Bewley project in this directory", ["bewley", "init"], mutates_state=True),
        ])
        return
    artifact_state = _artifact_next(project)
    if artifact_state is not None:
        recommendation = artifact_state.pop("recommendation")
        finish("next", {
            "schema_version": SCHEMA_VERSION,
            "ready": True,
            **artifact_state,
            "blockers": [],
        }, next_actions=[recommendation])
        return
    state = _phase_state(project, True)
    steps = state.pop("recommended_next_steps")
    finish("next", {
        "schema_version": SCHEMA_VERSION,
        "ready": True,
        "stage": state["phase"],
        **state,
        "blockers": [],
    }, next_actions=steps[:1])
