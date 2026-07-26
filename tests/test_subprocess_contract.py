"""Black-box subprocess tests of the envelope contract.

These exercise the real console entry path (argv parsing, env handling, exit
codes, stdout purity) that in-process tests bypass — the safety net that makes
refactors cheap.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

SRC = str(Path(__file__).parents[1] / "src")


def run_bewley(cwd: Path, *args: str) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["PYTHONPATH"] = SRC
    env.pop("BEWLEY_HUMAN_OUTPUT", None)
    return subprocess.run(
        [sys.executable, "-m", "bewley", *args],
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def test_success_envelope_is_pure_single_json(tmp_path: Path) -> None:
    completed = run_bewley(tmp_path, "init")
    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout)
    assert completed.stdout.strip().startswith("{")
    assert completed.stdout.count("\n{") == 0
    assert payload["schema_version"] == "2.0"
    assert payload["status"] == "ok"
    assert payload["command"] == "bewley init"
    assert payload["argv"] == ["bewley", "init"]
    assert payload["errors"] == []

    completed = run_bewley(tmp_path, "status")
    assert completed.returncode == 0
    assert completed.stderr == ""
    payload = json.loads(completed.stdout)
    assert set(payload) == {
        "schema_version", "status", "command", "argv", "data", "warnings", "errors", "next_steps",
    }


def test_domain_error_exits_nonzero_with_one_envelope(tmp_path: Path) -> None:
    run_bewley(tmp_path, "init")
    completed = run_bewley(tmp_path, "show", "document", "missing.txt")
    assert completed.returncode == 1
    payload = json.loads(completed.stdout)
    assert payload["status"] == "error"
    assert payload["errors"][0]["code"]
    assert payload["data"] == {}


def test_unknown_command_is_enveloped_cli_usage(tmp_path: Path) -> None:
    completed = run_bewley(tmp_path, "not-a-command")
    assert completed.returncode == 1
    payload = json.loads(completed.stdout)
    assert payload["errors"][0]["code"] == "CLI_USAGE"


def test_version_reports_build(tmp_path: Path) -> None:
    completed = run_bewley(tmp_path, "version")
    assert completed.returncode == 0
    payload = json.loads(completed.stdout)
    assert payload["data"]["version"]
    assert payload["data"]["envelope_schema_version"] == "2.0"
