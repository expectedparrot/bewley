from __future__ import annotations

import json
from pathlib import Path

from conftest import BewleyProject
from bewley.cli import main


def envelope(project: BewleyProject, *args: str) -> tuple[int, dict, str]:
    code, stdout, stderr = project.cli(*args, human=False)
    payload = json.loads(stdout)
    assert stdout.count("\n{") == 0, "stdout must contain exactly one JSON document"
    assert payload["schema_version"] == "2.0"
    assert payload["argv"] == ["bewley", *args]
    assert isinstance(payload["command"], str) and payload["command"].startswith("bewley")
    assert isinstance(payload["warnings"], list)
    assert isinstance(payload["errors"], list)
    assert isinstance(payload["next_steps"], list)
    return code, payload, stderr


def test_success_envelope_uses_status_and_real_argv(empty_project: BewleyProject) -> None:
    code, payload, stderr = envelope(empty_project, "status")
    assert code == 0
    assert stderr == ""
    assert payload["status"] == "ok"
    assert payload["command"] == "bewley status"
    assert "data" in payload
    assert payload["errors"] == []


def test_domain_error_is_one_envelope(empty_project: BewleyProject) -> None:
    code, payload, _ = envelope(empty_project, "show", "document", "missing.txt")
    assert code != 0
    assert payload["status"] == "error"
    assert payload["command"] == "bewley show document"
    assert len(payload["errors"]) == 1
    error = payload["errors"][0]
    assert {"code", "message", "context"} <= set(error)
    assert payload["data"] == {}


def test_cli_usage_error_is_enveloped(empty_project: BewleyProject) -> None:
    code, payload, _ = envelope(empty_project, "not-a-command")
    assert code == 1
    assert payload["status"] == "error"
    assert payload["errors"][0]["code"] == "CLI_USAGE"


def test_init_returns_structured_mutating_action(tmp_path: Path) -> None:
    project = BewleyProject(tmp_path)
    code, payload, _ = envelope(project, "init")
    assert code == 0
    action = payload["next_steps"][0]
    assert action["command"] == ["bewley", "add", "corpus/<filename>"]
    assert action["mutates_state"] is True
    assert action["requires_network"] is False
    assert action["requires_user_approval"] is False


def test_capabilities_lists_versioned_schemas(empty_project: BewleyProject) -> None:
    code, payload, _ = envelope(empty_project, "capabilities")
    assert code == 0
    assert payload["data"]["schemas"] == [
        "action.schema.json",
        "agent-status.schema.json",
        "envelope.schema.json",
    ]


def test_agent_schema_returns_envelope_schema(empty_project: BewleyProject) -> None:
    code, payload, _ = envelope(empty_project, "agent", "schema", "envelope")
    assert code == 0
    schema = payload["data"]["schema"]
    assert schema["title"] == "Bewley CLI envelope"
    success_shape = schema["oneOf"][0]["properties"]
    assert success_shape["schema_version"]["const"] == "2.0"
    assert "status" in success_shape and "next_steps" in success_shape


def test_agent_status_returns_executable_actions(empty_project: BewleyProject) -> None:
    code, payload, _ = envelope(empty_project, "agent", "status")
    assert code == 0
    data = payload["data"]
    assert data["ready"] is True
    assert data["next_actions"]
    assert isinstance(data["next_actions"][0]["command"], list)


def test_ambiguous_references_list_their_matches(empty_project: BewleyProject) -> None:
    (empty_project.root / "a").mkdir()
    (empty_project.root / "b").mkdir()
    (empty_project.root / "a" / "x.txt").write_text("alpha text\n", encoding="utf-8")
    (empty_project.root / "b" / "x.txt").write_text("beta text\n", encoding="utf-8")
    empty_project.cli("add", "a/x.txt", human=False)
    empty_project.cli("add", "b/x.txt", human=False)

    code, payload, _ = envelope(empty_project, "show", "document", "x.txt")
    assert code != 0
    error = payload["errors"][0]
    assert error["code"] == "AMBIGUOUS_DOCUMENT"
    paths = {match["path"] for match in error["context"]["matches"]}
    assert paths == {"a/x.txt", "b/x.txt"}

    code, payload, _ = envelope(empty_project, "code", "show", "nope")
    assert code != 0
    assert payload["errors"][0]["code"] == "NOT_FOUND"


def test_main_unexpected_error_is_not_silent(monkeypatch, capsys) -> None:
    from bewley import cli

    def explode(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(cli, "app", explode)
    code = main(["status"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 1
    assert payload["status"] == "error"
    assert payload["errors"][0]["code"] == "INTERNAL_ERROR"
    assert payload["errors"][0]["context"]["exception_type"] == "RuntimeError"
