from __future__ import annotations

import json
from importlib.resources import files
from typing import Any

import typer

from bewley.commands.common import action, finish
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
            "next_actions_are_argv_arrays": True,
            "mutations_are_declared": True,
            "unexpected_errors_are_enveloped": True,
        },
    }


def capabilities_command() -> None:
    """Describe the stable agent-facing CLI contract."""
    finish("capabilities", capabilities_data())
