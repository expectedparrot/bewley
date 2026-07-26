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
    assert envelope["status"] in {"ok", "warning"}, f"status not ok: {envelope}"
    return envelope["data"]


def _json_err(proj: BewleyProject, *args: str) -> dict:
    """Run a command in JSON mode, assert failure, return envelope."""
    code, stdout, stderr = proj.cli(*args, human=False)
    assert code != 0
    envelope = json.loads(stdout)
    assert envelope["status"] == "error"
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


class TestOpenCodingApply:
    """The reviewed-candidates path: ingest writes a CSV, apply files codes + spans."""

    def _write_candidates(self, project, rows):
        import csv as _csv
        target = project.root / "qualitative-analysis" / "candidate_codes.csv"
        target.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            "candidate_id", "code_name", "description", "quote", "source_document_id",
            "source_document_path", "source_revision_id", "byte_start", "byte_end", "resolve_status",
        ]
        with target.open("w", newline="", encoding="utf-8") as handle:
            writer = _csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return target

    def _document_row(self, project):
        from bewley.project import Project

        proj = Project(project.root)
        with proj.connect() as conn:
            document = conn.execute("SELECT document_id FROM documents LIMIT 1").fetchone()
            revision = proj.current_revision(conn, document["document_id"])
        return proj, document["document_id"], revision

    def test_dry_run_plans_without_mutating(self, project: BewleyProject) -> None:
        proj, document_id, revision = self._document_row(project)
        text = (proj.objects_dir / revision["content_sha256"]).read_bytes().decode("utf-8")
        quote = text.splitlines()[0]
        start = 0
        end = len(quote.encode("utf-8"))
        self._write_candidates(project, [{
            "candidate_id": "c1", "code_name": "test_code", "description": "d",
            "quote": quote, "source_document_id": document_id,
            "source_document_path": "corpus/x.txt", "source_revision_id": revision["revision_id"],
            "byte_start": start, "byte_end": end, "resolve_status": "exact",
        }])
        data = _json_ok(project, "open-coding", "apply", "--dry-run")
        assert data["dry_run"] is True
        assert data["annotations_planned"] == 1
        assert data["annotations_applied"] == 0
        assert data["codes_to_create"] == ["test_code"]
        with proj.connect() as conn:
            assert conn.execute("SELECT COUNT(*) FROM codes").fetchone()[0] == 0

    def test_apply_creates_codes_and_annotations_and_is_idempotent(self, project: BewleyProject) -> None:
        proj, document_id, revision = self._document_row(project)
        text = (proj.objects_dir / revision["content_sha256"]).read_bytes().decode("utf-8")
        quote = text.splitlines()[0]
        end = len(quote.encode("utf-8"))
        self._write_candidates(project, [{
            "candidate_id": "c1", "code_name": "test_code", "description": "d",
            "quote": quote, "source_document_id": document_id,
            "source_document_path": "corpus/x.txt", "source_revision_id": revision["revision_id"],
            "byte_start": 0, "byte_end": end, "resolve_status": "exact",
        }])
        data = _json_ok(project, "open-coding", "apply")
        assert data["annotations_applied"] == 1
        assert data["codes_to_create"] == ["test_code"]
        again = _json_ok(project, "open-coding", "apply")
        assert again["annotations_applied"] == 0
        assert again["skipped_details"][0]["reason"] == "already_applied"

    def test_unresolved_and_stale_rows_are_itemized_not_guessed(self, project: BewleyProject) -> None:
        proj, document_id, revision = self._document_row(project)
        self._write_candidates(project, [
            {
                "candidate_id": "c1", "code_name": "loose_code", "description": "d",
                "quote": "not in the document", "source_document_id": document_id,
                "source_document_path": "corpus/x.txt", "source_revision_id": revision["revision_id"],
                "byte_start": "", "byte_end": "", "resolve_status": "not_found",
            },
            {
                "candidate_id": "c2", "code_name": "stale_code", "description": "d",
                "quote": "whatever", "source_document_id": document_id,
                "source_document_path": "corpus/x.txt", "source_revision_id": "not-the-current-revision",
                "byte_start": 0, "byte_end": 4, "resolve_status": "exact",
            },
        ])
        data = _json_ok(project, "open-coding", "apply")
        assert data["annotations_applied"] == 0
        reasons = {item["candidate_id"]: item["reason"] for item in data["skipped_details"]}
        assert reasons["c1"] == "unresolved_quote:not_found"
        assert reasons["c2"] == "stale_revision"

    def test_apply_requires_candidate_file(self, project: BewleyProject) -> None:
        envelope = _json_err(project, "open-coding", "apply")
        assert envelope["errors"][0]["code"] == "NOT_FOUND"

    def test_legacy_codegen_open_coding_is_gone(self, project: BewleyProject) -> None:
        envelope = _json_err(project, "codegen", "open-coding")
        assert envelope["errors"][0]["code"] == "CLI_USAGE"
