from __future__ import annotations

import json
import os
import shlex
import sys
from typing import Any

import typer

from ..project import BewleyError, Project


HumanOption = typer.Option(False, "--human", "-H", help="Human-readable output instead of JSON.")


def rich_console():
    """A Console for --human rendering (fixed width so captures are stable)."""
    from rich.console import Console

    return Console(width=96, highlight=False)
QuietOption = typer.Option(False, "--quiet", help="Suppress non-error output.")


def should_emit_json(human: bool) -> bool:
    if human:
        return False
    return os.getenv("BEWLEY_HUMAN_OUTPUT", "").lower() != "true"


ENVELOPE_SCHEMA_VERSION = "2.0"

_COMMAND_GROUPS = {
    "list", "show", "code", "annotate", "export", "memo",
    "docs", "codegen", "open-coding", "agent",
}


def command_argv() -> list[str]:
    """Return the actual invocation as stable, machine-readable provenance."""
    return ["bewley", *sys.argv[1:]]


def command_name() -> str:
    """Return the canonical command path, independent of flags and arguments."""
    args = [arg for arg in sys.argv[1:] if not arg.startswith("-")]
    if not args:
        return "bewley"
    depth = 2 if args[0] in _COMMAND_GROUPS and len(args) > 1 else 1
    return " ".join(["bewley", *args[:depth]])


def action(
    action_id: str,
    purpose: str,
    command: list[str],
    *,
    mutates_state: bool,
    requires_network: bool = False,
    requires_user_approval: bool = False,
    reason: str = "",
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "id": action_id,
        "purpose": purpose,
        "command": command,
        "mutates_state": mutates_state,
        "requires_network": requires_network,
        "requires_user_approval": requires_user_approval,
    }
    if reason:
        result["reason"] = reason
    return result


def _normalize_action(value: str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(value, dict) and all(
        key in value
        for key in (
            "id",
            "purpose",
            "command",
            "mutates_state",
            "requires_network",
            "requires_user_approval",
        )
    ):
        return value
    if isinstance(value, dict):
        label = str(value.get("label") or value.get("purpose") or "Run next command")
        raw_command = value.get("command", [])
    else:
        label = "Run next command"
        raw_command = value
    argv = shlex.split(raw_command) if isinstance(raw_command, str) else [str(item) for item in raw_command]
    action_id = "-".join(argv[1:3]) if len(argv) > 1 else "next"
    return action(action_id or "next", label, argv, mutates_state=False)


def _json_envelope(
    data: Any,
    warnings: list[str] | None = None,
    next_actions: list[str | dict[str, Any]] | None = None,
) -> dict:
    warning_items = warnings or []
    return {
        "schema_version": ENVELOPE_SCHEMA_VERSION,
        "status": "warning" if warning_items else "ok",
        "command": command_name(),
        "argv": command_argv(),
        "data": data,
        "warnings": warning_items,
        "errors": [],
        "next_steps": [_normalize_action(item) for item in (next_actions or [])],
    }


def _error_envelope(err: BewleyError) -> dict:
    error: dict[str, Any] = {
        "code": err.code,
        "message": err.message,
        "context": dict(err.context),
    }
    if err.hint:
        error["hint"] = err.hint
    return {
        "schema_version": ENVELOPE_SCHEMA_VERSION,
        "status": "error",
        "command": command_name(),
        "argv": command_argv(),
        "data": {},
        "warnings": [],
        "errors": [error],
        "next_steps": [],
    }


def finish(
    command: str,
    data: Any,
    warnings: list[str] | None = None,
    next_steps: list | None = None,
    next_actions: list | None = None,
) -> None:
    """Emit one canonical success envelope.

    ``command`` and ``next_steps`` remain accepted during the CLI migration;
    provenance always comes from the real argv and output always uses
    ``next_actions``.
    """
    del command
    payload = _json_envelope(
        data,
        warnings=warnings,
        next_actions=next_actions if next_actions is not None else next_steps,
    )
    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))


def fail(command: str, err: BewleyError, json_flag: bool) -> None:
    del command
    if json_flag:
        payload = _error_envelope(err)
        print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    else:
        print(f"error: {err}", file=sys.stderr)
    raise typer.Exit(code=1)


def fail_unexpected(exc: Exception, json_flag: bool = True) -> None:
    err = BewleyError(
        "Unexpected internal error.",
        code="INTERNAL_ERROR",
        context={"exception_type": type(exc).__name__, "message": str(exc)},
        hint="Rerun with the same command and report this envelope if the error persists.",
    )
    if json_flag:
        print(json.dumps(_error_envelope(err), indent=2, ensure_ascii=False, default=str))
    else:
        print(f"error: {err.message} ({type(exc).__name__}: {exc})", file=sys.stderr)


def get_project(command: str = "", json_flag: bool = True) -> Project:
    try:
        return Project.discover()
    except BewleyError as exc:
        fail(command, exc, json_flag)
        raise typer.Exit(2)
