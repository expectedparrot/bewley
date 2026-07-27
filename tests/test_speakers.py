"""Tests for speaker segmentation, roles, and speaker-aware annotation (RFC 001 slice 3)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from conftest import BewleyProject

TRANSCRIPT = """Title: A fictional check-in (FICTIONAL)
Status: synthetic test data

INTERVIEWER: How was the harvest this year?

FARMER ROW: Better than last. The merchant complains of the farmer,
but we brought it in before the frost.

INTERVIEWER: And the prices?

FARMER ROW: The prices are a scandal, which is the one thing that
never changes.
"""


def _json(proj: BewleyProject, *args: str) -> tuple[int, dict]:
    code, stdout, stderr = proj.cli(*args, human=False)
    return code, json.loads(stdout)


@pytest.fixture
def transcript_project(empty_project: BewleyProject) -> BewleyProject:
    path = empty_project.root / "corpus" / "checkin.txt"
    path.parent.mkdir(exist_ok=True)
    path.write_text(TRANSCRIPT, encoding="utf-8")
    empty_project.cli_ok("add", "corpus/checkin.txt")
    return empty_project


class TestDetect:
    def test_caps_rule_finds_turns_but_not_headers(self, transcript_project: BewleyProject) -> None:
        code, envelope = _json(transcript_project, "speakers", "detect", "corpus/checkin.txt")
        assert code == 0
        data = envelope["data"]
        assert data["turn_count"] == 4
        assert data["labels"] == ["FARMER ROW", "INTERVIEWER"]

    def test_mixed_case_needs_explicit_labels(self, project: BewleyProject) -> None:
        code, envelope = _json(project, "speakers", "detect", "corpus/interview_alice.txt")
        assert code != 0
        assert envelope["errors"][0]["code"] == "NO_SPEAKER_TURNS"
        code, envelope = _json(
            project, "speakers", "detect", "corpus/interview_alice.txt",
            "--label", "Interviewer", "--label", "Alice",
        )
        assert code == 0
        assert envelope["data"]["rule"] == "explicit-labels"
        assert envelope["data"]["labels"] == ["Alice", "Interviewer"]

    def test_list_reports_share_and_roles(self, transcript_project: BewleyProject) -> None:
        transcript_project.cli_ok("speakers", "detect", "corpus/checkin.txt")
        transcript_project.cli_ok("speakers", "set-role", "INTERVIEWER", "interviewer")
        code, envelope = _json(transcript_project, "speakers", "list", "corpus/checkin.txt")
        assert code == 0
        speakers = {s["label"]: s for s in envelope["data"]["speakers"]}
        assert speakers["INTERVIEWER"]["role"] == "interviewer"
        assert speakers["FARMER ROW"]["role"] is None
        assert speakers["FARMER ROW"]["turns"] == 2


class TestRoles:
    def test_unknown_label_lists_known(self, transcript_project: BewleyProject) -> None:
        transcript_project.cli_ok("speakers", "detect", "corpus/checkin.txt")
        code, envelope = _json(transcript_project, "speakers", "set-role", "FARMER", "participant")
        assert code != 0
        assert envelope["errors"][0]["context"]["known_labels"] == ["FARMER ROW", "INTERVIEWER"]

    def test_invalid_role_rejected(self, transcript_project: BewleyProject) -> None:
        transcript_project.cli_ok("speakers", "detect", "corpus/checkin.txt")
        code, envelope = _json(transcript_project, "speakers", "set-role", "INTERVIEWER", "boss")
        assert code != 0
        assert envelope["errors"][0]["code"] == "INVALID_INPUT"

    def test_next_flags_unassigned_labels(self, transcript_project: BewleyProject) -> None:
        transcript_project.cli_ok("speakers", "detect", "corpus/checkin.txt")
        code, envelope = _json(transcript_project, "next")
        top = " ".join(envelope["next_steps"][0]["command"])
        assert "speakers set-role" in top


class TestSpeakerAwareAnnotation:
    @pytest.fixture
    def coded(self, transcript_project: BewleyProject) -> BewleyProject:
        transcript_project.cli_ok("speakers", "detect", "corpus/checkin.txt")
        transcript_project.cli_ok("speakers", "set-role", "INTERVIEWER", "interviewer")
        transcript_project.cli_ok("speakers", "set-role", "FARMER ROW", "participant")
        transcript_project.cli_ok("code", "create", "harvest")
        return transcript_project

    def test_participant_quote_carries_scope(self, coded: BewleyProject) -> None:
        code, envelope = _json(
            coded, "annotate", "apply", "harvest", "corpus/checkin.txt",
            "--quote", "we brought it in before the frost",
        )
        assert code == 0
        assert envelope["data"]["speaker_scope"] == "participant"

    def test_interviewer_span_blocked_then_allowed(self, coded: BewleyProject) -> None:
        code, envelope = _json(
            coded, "annotate", "apply", "harvest", "corpus/checkin.txt",
            "--quote", "How was the harvest this year?",
        )
        assert code != 0
        assert envelope["errors"][0]["code"] == "INTERVIEWER_TEXT"
        code, envelope = _json(
            coded, "annotate", "apply", "harvest", "corpus/checkin.txt",
            "--quote", "How was the harvest this year?", "--allow-interviewer",
        )
        assert code == 0
        assert envelope["data"]["speaker_scope"] == "interviewer"

    def test_turn_scope_annotates_whole_turn(self, coded: BewleyProject) -> None:
        code, envelope = _json(
            coded, "annotate", "apply", "harvest", "corpus/checkin.txt", "--turn", "2",
        )
        assert code == 0
        text = envelope["data"]["annotated_text"]
        assert text.startswith("FARMER ROW:")
        assert "before the frost" in text
        assert envelope["data"]["speaker_scope"] == "participant"

    def test_turn_out_of_range_and_unsegmented(self, coded: BewleyProject) -> None:
        code, envelope = _json(
            coded, "annotate", "apply", "harvest", "corpus/checkin.txt", "--turn", "9",
        )
        assert code != 0
        assert envelope["errors"][0]["context"]["turns"] == 4
        other = coded.root / "corpus" / "plain.txt"
        other.write_text("No speakers here at all.\n", encoding="utf-8")
        coded.cli_ok("add", "corpus/plain.txt")
        code, envelope = _json(
            coded, "annotate", "apply", "harvest", "corpus/plain.txt", "--turn", "1",
        )
        assert code != 0
        assert envelope["errors"][0]["code"] == "NOT_SEGMENTED"

    def test_mixed_span_warns(self, coded: BewleyProject) -> None:
        code, envelope = _json(
            coded, "annotate", "apply", "harvest", "corpus/checkin.txt",
            "--lines", "4:7",
        )
        assert code == 0
        assert envelope["data"]["speaker_scope"] == "mixed"
        assert envelope["status"] == "warning"

    def test_unsegmented_documents_unaffected(self, coded: BewleyProject) -> None:
        other = coded.root / "corpus" / "plain.txt"
        other.write_text("No speakers here at all.\n", encoding="utf-8")
        coded.cli_ok("add", "corpus/plain.txt")
        code, envelope = _json(
            coded, "annotate", "apply", "harvest", "corpus/plain.txt", "--lines", "1:1",
        )
        assert code == 0
        assert envelope["data"]["speaker_scope"] is None


class TestSpeakerCaseLink:
    def test_link_case_and_display(self, transcript_project: BewleyProject) -> None:
        transcript_project.cli_ok("speakers", "detect", "corpus/checkin.txt")
        transcript_project.cli_ok("case", "create", "Farmer Row", "--type", "person")
        transcript_project.cli_ok("speakers", "link-case", "corpus/checkin.txt", "FARMER ROW", "Farmer Row")
        code, envelope = _json(transcript_project, "speakers", "list", "corpus/checkin.txt")
        speakers = {s["label"]: s for s in envelope["data"]["speakers"]}
        assert speakers["FARMER ROW"]["case"] == "Farmer Row"
        code, envelope = _json(transcript_project, "link", "list")
        links = envelope["data"]["links"]
        assert any(
            row["source_kind"] == "speaker" and "FARMER ROW" in row["source"]
            for row in links
        )

    def test_rebuild_reconstructs_segmentation(self, transcript_project: BewleyProject) -> None:
        transcript_project.cli_ok("speakers", "detect", "corpus/checkin.txt")
        transcript_project.cli_ok("speakers", "set-role", "INTERVIEWER", "interviewer")
        transcript_project.cli_ok("rebuild-index")
        code, envelope = _json(transcript_project, "speakers", "list", "corpus/checkin.txt")
        assert code == 0
        assert envelope["data"]["turn_count"] == 4
        speakers = {s["label"]: s for s in envelope["data"]["speakers"]}
        assert speakers["INTERVIEWER"]["role"] == "interviewer"
