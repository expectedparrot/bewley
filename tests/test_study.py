"""Tests for the study manifest and research questions (RFC 001, slice 1)."""
from __future__ import annotations

import json
from pathlib import Path

from conftest import BewleyProject


def _json_ok(proj: BewleyProject, *args: str) -> dict:
    code, stdout, stderr = proj.cli(*args, human=False)
    assert code == 0, f"bewley {' '.join(args)} failed (exit {code}): {stderr}\n{stdout}"
    envelope = json.loads(stdout)
    assert envelope["status"] in {"ok", "warning"}
    return envelope["data"]


def _json_err(proj: BewleyProject, *args: str) -> dict:
    code, stdout, stderr = proj.cli(*args, human=False)
    assert code != 0
    return json.loads(stdout)


class TestStudy:
    def test_set_and_show_roundtrip(self, empty_project: BewleyProject) -> None:
        data = _json_ok(empty_project, "study", "set",
                        "--method", "grounded-theory", "--unit", "document")
        assert data["method"] == "grounded-theory"
        assert data["unit_of_analysis"] == "document"
        shown = _json_ok(empty_project, "study", "show")
        assert shown["method"] == "grounded-theory"
        assert shown["research_questions"] == []

    def test_partial_update_preserves_other_fields(self, empty_project: BewleyProject) -> None:
        _json_ok(empty_project, "study", "set", "--method", "grounded-theory")
        _json_ok(empty_project, "study", "set", "--unit", "document")
        shown = _json_ok(empty_project, "study", "show")
        assert shown["method"] == "grounded-theory"
        assert shown["unit_of_analysis"] == "document"

    def test_set_requires_at_least_one_field(self, empty_project: BewleyProject) -> None:
        envelope = _json_err(empty_project, "study", "set")
        assert envelope["errors"][0]["code"] == "INVALID_INPUT"

    def test_show_human_renders_panel(self, empty_project: BewleyProject) -> None:
        empty_project.cli_ok("study", "set", "--method", "grounded-theory")
        stdout = empty_project.cli_ok("study", "show")
        assert "grounded-theory" in stdout


class TestQuestions:
    def test_add_and_list(self, empty_project: BewleyProject) -> None:
        data = _json_ok(empty_project, "question", "add", "How do letters negotiate duty?")
        assert data["question_id"]
        assert data["question_count"] == 1
        listed = _json_ok(empty_project, "question", "list")
        assert [q["text"] for q in listed["research_questions"]] == [
            "How do letters negotiate duty?"
        ]

    def test_empty_text_rejected(self, empty_project: BewleyProject) -> None:
        envelope = _json_err(empty_project, "question", "add", "   ")
        assert envelope["errors"][0]["code"] == "INVALID_INPUT"

    def test_rebuild_index_reconstructs_study(self, empty_project: BewleyProject) -> None:
        empty_project.cli_ok("study", "set", "--method", "framework")
        empty_project.cli_ok("question", "add", "What changed across waves?")
        empty_project.cli_ok("rebuild-index")
        shown = _json_ok(empty_project, "study", "show")
        assert shown["method"] == "framework"
        assert len(shown["research_questions"]) == 1


class TestStudyAwareNext:
    @staticmethod
    def _top_action(proj: BewleyProject) -> str:
        code, stdout, stderr = proj.cli("next", human=False)
        assert code == 0, stderr
        envelope = json.loads(stdout)
        return " ".join(envelope["next_steps"][0]["command"])

    def test_next_suggests_study_then_questions_then_coding(self, project: BewleyProject) -> None:
        assert "bewley study set" in self._top_action(project)
        project.cli_ok("study", "set", "--method", "grounded-theory")
        assert "bewley question add" in self._top_action(project)
        project.cli_ok("question", "add", "How do the letters negotiate duty?")
        assert "study" not in self._top_action(project)
