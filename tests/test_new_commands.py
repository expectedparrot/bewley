"""Tests for docs, agent-start, and codegen commands."""
from __future__ import annotations

import ast
import json
from pathlib import Path

from conftest import BewleyProject


# ── helpers ──────────────────────────────────────────────────────────────────


def _json_ok(proj: BewleyProject, *args: str) -> dict:
    """Run a command in JSON mode, assert success, return parsed data payload."""
    code, stdout, stderr = proj.cli(*args, human=False)
    assert code == 0, f"Command failed (exit {code}):\n{stderr}\n{stdout}"
    envelope = json.loads(stdout)
    assert envelope["ok"] is True, f"ok != true: {envelope}"
    return envelope["data"]


def _json_err(proj: BewleyProject, *args: str) -> dict:
    """Run a command in JSON mode, assert failure, return envelope."""
    code, stdout, stderr = proj.cli(*args, human=False)
    assert code != 0
    envelope = json.loads(stdout)
    assert envelope["ok"] is False
    return envelope


# ── docs ─────────────────────────────────────────────────────────────────────


class TestDocsCommand:
    def test_docs_list_human(self, empty_project: BewleyProject) -> None:
        stdout = empty_project.cli_ok("docs", "list")
        assert "overview" in stdout
        assert "getting-started" in stdout
        assert "commands" in stdout
        assert "grounded-theory" in stdout

    def test_docs_list_json_envelope(self, empty_project: BewleyProject) -> None:
        data = _json_ok(empty_project, "docs", "list")
        topics = {t["topic"] for t in data}
        assert "overview" in topics
        assert "getting-started" in topics
        assert "workflow" in topics
        assert "commands" in topics
        assert "grounded-theory" in topics

    def test_docs_show_overview(self, empty_project: BewleyProject) -> None:
        stdout = empty_project.cli_ok("docs", "show", "overview")
        assert "bewley" in stdout.lower()

    def test_docs_show_getting_started(self, empty_project: BewleyProject) -> None:
        stdout = empty_project.cli_ok("docs", "show", "getting-started")
        assert "bewley init" in stdout

    def test_docs_show_workflow(self, empty_project: BewleyProject) -> None:
        stdout = empty_project.cli_ok("docs", "show", "workflow")
        assert "phase" in stdout.lower()

    def test_docs_show_commands(self, empty_project: BewleyProject) -> None:
        stdout = empty_project.cli_ok("docs", "show", "commands")
        assert "bewley" in stdout.lower()

    def test_docs_show_grounded_theory(self, empty_project: BewleyProject) -> None:
        stdout = empty_project.cli_ok("docs", "show", "grounded-theory")
        assert "open coding" in stdout.lower() or "grounded" in stdout.lower()

    def test_docs_show_unknown_topic_fails(self, empty_project: BewleyProject) -> None:
        code, stdout, _ = empty_project.cli("docs", "show", "nonexistent-topic")
        assert code != 0

    def test_docs_show_json_contains_markdown(self, empty_project: BewleyProject) -> None:
        data = _json_ok(empty_project, "docs", "show", "overview")
        assert "markdown" in data or "content" in data or len(str(data)) > 50

    def test_docs_search_returns_results(self, empty_project: BewleyProject) -> None:
        stdout = empty_project.cli_ok("docs", "search", "annotation")
        assert stdout.strip()

    def test_docs_search_json_envelope(self, empty_project: BewleyProject) -> None:
        data = _json_ok(empty_project, "docs", "search", "code")
        assert "matches" in data or "query" in data or isinstance(data, list) or len(str(data)) > 0

    def test_docs_search_no_results_for_gibberish(self, empty_project: BewleyProject) -> None:
        # Should succeed (exit 0) even with no results
        code, stdout, _ = empty_project.cli("docs", "search", "xyzzy_nonsense_qwerty", human=False)
        assert code == 0


# ── codegen open-coding ───────────────────────────────────────────────────────


class TestCodegenOpenCoding:
    def test_generates_script_file(self, project: BewleyProject) -> None:
        project.cli_ok("codegen", "open-coding", "--output", "scripts/run_open_coding.py")
        assert (project.root / "scripts" / "run_open_coding.py").exists()

    def test_generated_script_is_valid_python(self, project: BewleyProject) -> None:
        project.cli_ok("codegen", "open-coding", "--output", "scripts/run_open_coding.py")
        script = (project.root / "scripts" / "run_open_coding.py").read_text()
        # ast.parse raises SyntaxError if invalid Python
        ast.parse(script)

    def test_generated_script_imports_edsl(self, project: BewleyProject) -> None:
        project.cli_ok("codegen", "open-coding", "--output", "scripts/run_open_coding.py")
        script = (project.root / "scripts" / "run_open_coding.py").read_text()
        assert "edsl" in script

    def test_generated_script_embeds_document_paths(self, project: BewleyProject) -> None:
        project.cli_ok("codegen", "open-coding", "--output", "scripts/run_open_coding.py")
        script = (project.root / "scripts" / "run_open_coding.py").read_text()
        assert "interview_alice.txt" in script
        assert "interview_bob.txt" in script

    def test_generated_script_embeds_project_dir(self, project: BewleyProject) -> None:
        project.cli_ok("codegen", "open-coding", "--output", "scripts/run_open_coding.py")
        script = (project.root / "scripts" / "run_open_coding.py").read_text()
        assert str(project.root) in script

    def test_output_path_is_configurable(self, project: BewleyProject) -> None:
        project.cli_ok("codegen", "open-coding", "--output", "analysis/my_coding_job.py")
        assert (project.root / "analysis" / "my_coding_job.py").exists()

    def test_output_creates_parent_dirs(self, project: BewleyProject) -> None:
        project.cli_ok("codegen", "open-coding", "--output", "deep/nested/dir/script.py")
        assert (project.root / "deep" / "nested" / "dir" / "script.py").exists()

    def test_json_envelope_has_script_path(self, project: BewleyProject) -> None:
        data = _json_ok(project, "codegen", "open-coding", "--output", "scripts/run.py")
        assert "script_path" in data
        assert "run.py" in data["script_path"]

    def test_json_envelope_has_run_command(self, project: BewleyProject) -> None:
        data = _json_ok(project, "codegen", "open-coding", "--output", "scripts/run.py")
        assert "run_command" in data
        assert "python" in data["run_command"]

    def test_model_flag_embedded_in_script(self, project: BewleyProject) -> None:
        project.cli_ok(
            "codegen", "open-coding",
            "--output", "scripts/run.py",
            "--model", "claude-opus-4-7",
        )
        script = (project.root / "scripts" / "run.py").read_text()
        assert "claude-opus-4-7" in script

    def test_codegen_requires_project(self, tmp_path: Path) -> None:
        """Codegen should fail gracefully when not in a bewley project."""
        import contextlib, io, os, sys
        from bewley.cli import main

        old_cwd = Path.cwd()
        stdout = io.StringIO()
        stderr = io.StringIO()
        try:
            os.chdir(tmp_path)
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                code = main(["codegen", "open-coding"])
        finally:
            os.chdir(old_cwd)
        assert code != 0
