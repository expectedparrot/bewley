from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import typer

from ..project import BewleyError, Project


HumanOption = typer.Option(False, "--human", "-H", help="Human-readable output instead of JSON.")
QuietOption = typer.Option(False, "--quiet", help="Suppress non-error output.")


def should_emit_json(human: bool) -> bool:
    if human:
        return False
    return os.getenv("BEWLEY_HUMAN_OUTPUT", "").lower() != "true"


def _json_envelope(command: str, data: Any, warnings: list[str] | None = None, next_steps: list[str | dict] | None = None) -> dict:
    return {
        "command": command,
        "status": "ok",
        "data": data,
        "warnings": warnings or [],
        "errors": [],
        "next_steps": next_steps or [],
    }


def _error_envelope(command: str, err: BewleyError) -> dict:
    return {
        "command": command,
        "status": "error",
        "data": {},
        "warnings": [],
        "errors": [{"code": err.code if hasattr(err, "code") else "ERROR", "message": str(err), "context": {}, "hint": ""}],
        "next_steps": [],
    }


def finish(command: str, data: Any, warnings: list[str] | None = None, next_steps: list | None = None) -> None:
    payload = _json_envelope(command, data, warnings=warnings, next_steps=next_steps)
    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))


def fail(command: str, err: BewleyError, json_flag: bool) -> None:
    if json_flag:
        payload = _error_envelope(command, err)
        print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    else:
        print(f"error: {err}", file=sys.stderr)
    raise typer.Exit(code=1)


def get_project(command: str = "", json_flag: bool = True) -> Project:
    try:
        return Project.discover()
    except BewleyError as exc:
        fail(command, exc, json_flag)
        raise typer.Exit(2)
