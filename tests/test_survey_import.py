from __future__ import annotations

import csv
import json

from conftest import BewleyProject


def _write_survey(project: BewleyProject, rows: list[dict[str, str]]) -> None:
    path = project.root / "responses.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["participant_id", "transcript", "feedback"],
        )
        writer.writeheader()
        writer.writerows(rows)


def _row() -> dict[str, str]:
    return {
        "participant_id": "PRIVATE-123",
        "transcript": repr([
            {
                "role": "interviewer",
                "content": [{"type": "text", "text": "What changed?"}],
                "status": {"type": "complete"},
            },
            {
                "role": "respondent",
                "content": [{"type": "text", "text": "AI makes drafting faster."}],
                "status": None,
            },
        ]),
        "feedback": "The interview felt natural.",
    }


def test_survey_csv_dry_run_detects_python_turns_without_writing(empty_project: BewleyProject) -> None:
    _write_survey(empty_project, [_row()])

    code, stdout, stderr = empty_project.cli(
        "import", "survey-csv", "responses.csv",
        "--transcript-column", "transcript",
        "--feedback-column", "feedback",
        "--output-dir", "corpus/imported",
        "--dry-run",
        human=False,
    )

    assert code == 0, stderr
    data = json.loads(stdout)["data"]
    assert data["detected_parsers"] == {"python": 1}
    assert data["speaker_roles"] == {
        "INTERVIEWER": "interviewer",
        "RESPONDENT": "participant",
    }
    assert "participant_id" in data["excluded_columns"]
    assert not (empty_project.root / "corpus" / "imported").exists()


def test_survey_csv_import_flattens_segments_and_records_provenance(empty_project: BewleyProject) -> None:
    _write_survey(empty_project, [_row()])

    code, stdout, stderr = empty_project.cli(
        "import", "survey-csv", "responses.csv",
        "--transcript-column", "transcript",
        "--feedback-column", "feedback",
        "--output-dir", "corpus/imported",
        human=False,
    )

    assert code == 0, stderr
    data = json.loads(stdout)["data"]
    transcript = (empty_project.root / "corpus/imported/respondent-001.txt").read_text()
    assert transcript == (
        "INTERVIEWER: What changed?\n\n"
        "RESPONDENT: AI makes drafting faster.\n\n"
        "RESPONDENT: [Feedback about the AI interviewer]\n"
        "The interview felt natural.\n"
    )
    assert "PRIVATE-123" not in transcript
    manifest = json.loads((empty_project.root / "qualitative-analysis/imports" / f"{data['import_id']}.json").read_text())
    assert manifest["documents"][0]["row"] == 1
    assert "PRIVATE-123" not in json.dumps(manifest)

    speakers = json.loads(empty_project.cli(
        "speakers", "list", "corpus/imported/respondent-001.txt", human=False,
    )[1])["data"]["speakers"]
    roles = {speaker["label"]: speaker["role"] for speaker in speakers}
    assert roles == {"INTERVIEWER": "interviewer", "RESPONDENT": "participant"}


def test_survey_csv_import_fails_atomically_on_malformed_forced_format(empty_project: BewleyProject) -> None:
    _write_survey(empty_project, [{**_row(), "transcript": "[not valid"}])

    code, stdout, _ = empty_project.cli(
        "import", "survey-csv", "responses.csv",
        "--transcript-column", "transcript",
        "--format", "python",
        "--output-dir", "corpus/imported",
        human=False,
    )

    assert code != 0
    assert json.loads(stdout)["errors"][0]["code"] == "INVALID_TRANSCRIPT_STRUCTURE"
    assert not (empty_project.root / "corpus" / "imported").exists()


def test_open_coding_refuses_serialized_transcript(empty_project: BewleyProject) -> None:
    path = empty_project.write_corpus("serialized.txt", _row()["transcript"])
    empty_project.cli_ok("add", str(path))

    code, stdout, _ = empty_project.cli(
        "open-coding", "jobs", "--output", "coding.jobs.ep", human=False,
    )

    assert code != 0
    error = json.loads(stdout)["errors"][0]
    assert error["code"] == "STRUCTURED_TRANSCRIPT_NOT_FLATTENED"
    assert not (empty_project.root / "coding.jobs.ep").exists()


def test_open_coding_structured_override_is_explicit(empty_project: BewleyProject) -> None:
    path = empty_project.write_corpus("serialized.txt", _row()["transcript"])
    empty_project.cli_ok("add", str(path))

    code, _, stderr = empty_project.cli(
        "open-coding", "jobs", "--output", "coding.jobs.ep",
        "--allow-structured-text",
        human=False,
    )

    assert code == 0, stderr
    assert (empty_project.root / "coding.jobs.ep").exists()
