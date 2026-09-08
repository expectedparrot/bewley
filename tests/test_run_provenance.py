import json
import zipfile

import pytest

from test_open_coding_jobs import _json, _result_for


def assert_artifact_roundtrip(project, paths):
    from conftest import BewleyProject

    _json(project, "project", "pack", "--output", "workflow.bewley")
    destination = project.root / "restored-workflow"
    _json(project, "project", "unpack", "workflow.bewley", "--dest", str(destination))
    for name in paths:
        assert (destination / name).read_bytes() == (
            project.root / name
        ).read_bytes(), name
    _json(BewleyProject(destination), "fsck")


def test_open_ingest_rejects_results_outside_pilot(project):
    from edsl import Jobs, Results, Survey

    _json(project, "open-coding", "jobs", "--output", "full.jobs.ep")
    full = Jobs.git.load(project.root / "full.jobs.ep")
    _json(project, "open-coding", "jobs", "--output", "pilot.jobs.ep", "--pilot", "1")
    Results(
        survey=Survey([]),
        data=[
            _result_for(dict(s), dict(s)["document_text"].splitlines()[0])
            for s in full.scenarios
        ],
    ).git.save(project.root / "results.ep")
    rc, out, _ = project.cli(
        "open-coding", "ingest", "results.ep", "--jobs", "pilot.jobs.ep", human=False
    )
    assert rc != 0
    assert json.loads(out)["errors"][0]["code"] == "INCOMPLETE_RESULTS"
    assert not (project.root / "qualitative-analysis/candidate_codes.csv").exists()


@pytest.mark.parametrize(
    "field,value", [("response_text", "Invented evidence."), ("revision_id", "wrong")]
)
def test_classification_rejects_changed_source_scenario(empty_project, field, value):
    from edsl import Agent, Jobs, Model, Results, Scenario, Survey
    from edsl.results import Result
    from bewley.commands.insights import CLASSIFICATION_QUESTION_NAME

    p = empty_project
    p.write_corpus("feedback.txt", "I enjoyed the conversation.\n")
    _json(p, "add", "corpus/feedback.txt")
    book = {
        "codebook_fingerprint": "test-book",
        "themes": [],
        "codes": [{"code_key": "positive"}],
    }
    (p.root / "book.json").write_text(json.dumps(book))
    _json(p, "insights", "classify", "jobs", "--codebook", "book.json")
    jobs = Jobs.git.load(p.root / "runs/003-classification/jobs.ep")
    scenario = dict(jobs.scenarios[0])
    scenario[field] = value
    answer = {
        "sentiment": "positive",
        "assignments": [
            {
                "code_key": "positive",
                "exact_text": scenario["response_text"],
                "confidence": 0.9,
            }
        ],
        "potential_new_theme": None,
    }
    row = Result(
        agent=Agent(),
        model=Model("test"),
        scenario=Scenario(scenario),
        iteration=0,
        answer={CLASSIFICATION_QUESTION_NAME: json.dumps(answer)},
    )
    Results(survey=Survey([]), data=[row]).git.save(p.root / "results.ep")
    rc, out, _ = p.cli(
        "insights",
        "classify",
        "ingest",
        "results.ep",
        "--codebook",
        "book.json",
        human=False,
    )
    assert rc != 0
    assert json.loads(out)["errors"][0]["code"] == "INCOMPLETE_RESULTS"
    assert not (p.root / "qualitative-analysis/feedback-classifications.jsonl").exists()


def test_custom_run_artifacts_survive_bundle_roundtrip(project, tmp_path):
    from edsl import Jobs, Results, Survey

    _json(project, "open-coding", "jobs", "--output", "custom/package.ep")
    jobs = Jobs.git.load(project.root / "custom/package.ep")
    Results(
        survey=Survey([]),
        data=[
            _result_for(dict(s), dict(s)["document_text"].splitlines()[0])
            for s in jobs.scenarios
        ],
    ).git.save(project.root / "custom/replies.ep")
    _json(
        project,
        "open-coding",
        "ingest",
        "custom/replies.ep",
        "--jobs",
        "custom/package.ep",
        "--output",
        "review/proposals.csv",
    )
    _json(project, "project", "pack", "--output", "snapshot.bewley")
    _json(
        project,
        "project",
        "unpack",
        "snapshot.bewley",
        "--dest",
        str(tmp_path / "restored"),
    )
    for name in [
        "custom/package.ep",
        "custom/replies.ep",
        "custom/package.run.json",
        "review/proposals.csv",
        "review/ingest_log.jsonl",
    ]:
        assert (tmp_path / "restored" / name).read_bytes() == (
            project.root / name
        ).read_bytes()
    log = json.loads((project.root / "review/ingest_log.jsonl").read_text())
    assert {
        "quote",
        "description",
        "source_revision_id",
        "source_results",
        "source_model",
        "byte_start",
        "byte_end",
    } <= log["candidates"][0].keys()
    with zipfile.ZipFile(project.root / "snapshot.bewley") as archive:
        assert any(
            name.startswith(".bewley/objects/artifacts/") for name in archive.namelist()
        )


def test_missing_entire_model_is_detected_from_manifest(project):
    from edsl import Jobs, Results, Survey

    p = project
    _json(p, "open-coding", "jobs", "--model", "test")
    manifest = p.root / "jobs.run.json"
    metadata = json.loads(manifest.read_text())
    # Simulates an explicitly configured two-model execution, only one returned.
    metadata["models"] = ["test", "missing-model"]
    manifest.write_text(json.dumps(metadata))
    from bewley.artifacts import register_files
    from bewley.project import Project

    # Fixture for a separately registered execution configuration.
    register_files(Project(p.root), "fixture execution configuration", [manifest])
    jobs = Jobs.git.load(p.root / "jobs.ep")
    Results(
        survey=Survey([]),
        data=[
            _result_for(dict(s), dict(s)["document_text"].splitlines()[0])
            for s in jobs.scenarios
        ],
    ).git.save(p.root / "results.ep")
    rc, out, _ = p.cli(
        "open-coding", "ingest", "results.ep", "--jobs", "jobs.ep", human=False
    )
    assert rc != 0
    assert json.loads(out)["errors"][0]["context"]["missing_answers"] == len(
        jobs.scenarios
    )


def test_ingest_rejects_altered_run_manifest(project):
    from edsl import Results, Survey

    _json(project, "open-coding", "jobs")
    manifest = project.root / "jobs.run.json"
    metadata = json.loads(manifest.read_text())
    metadata["models"] = ["unexpected"]
    manifest.write_text(json.dumps(metadata))
    Results(survey=Survey([]), data=[]).git.save(project.root / "results.ep")
    rc, out, _ = project.cli(
        "open-coding", "ingest", "results.ep", "--jobs", "jobs.ep", human=False
    )
    assert rc != 0
    assert json.loads(out)["errors"][0]["code"] == "INTEGRITY_ERROR"


def test_feedback_classification_evidence_roundtrip(empty_project):
    from bewley.commands.insights import CLASSIFICATION_QUESTION_NAME
    from test_focused_coding import _result_package

    p = empty_project
    p.write_corpus("feedback.txt", "The conversation felt natural.\n")
    _json(p, "add", "corpus/feedback.txt")
    book = {
        "codebook_fingerprint": "frozen-fixture",
        "themes": [
            {"theme_key": key, "name": key.title(), "description": key}
            for key in ["experience", "technical"]
        ],
        "codes": [
            {
                "code_key": "natural",
                "theme_key": "experience",
                "name": "Natural",
                "description": "Natural conversation",
                "inclusion_criteria": "Naturalness",
                "exclusion_criteria": "Technical issues",
            }
        ],
    }
    (p.root / "book.json").write_text(json.dumps(book))
    _json(
        p,
        "insights",
        "classify",
        "jobs",
        "--codebook",
        "book.json",
        "--output",
        "run/package.ep",
    )
    _result_package(
        p.root / "run/package.ep",
        p.root / "run/replies.ep",
        CLASSIFICATION_QUESTION_NAME,
        [
            json.dumps(
                {
                    "sentiment": "positive",
                    "assignments": [
                        {
                            "code_key": "natural",
                            "exact_text": "felt natural",
                            "confidence": 0.9,
                        }
                    ],
                    "potential_new_theme": None,
                }
            )
        ],
    )
    _json(
        p,
        "insights",
        "classify",
        "ingest",
        "run/replies.ep",
        "--jobs",
        "run/package.ep",
        "--codebook",
        "book.json",
    )
    _json(p, "insights", "aggregate", "--codebook", "book.json")
    _json(
        p,
        "insights",
        "evidence-export",
        "--codebook",
        "book.json",
        "--output",
        "report.html",
    )
    assert_artifact_roundtrip(
        p,
        [
            "book.json",
            "run/package.ep",
            "run/replies.ep",
            "run/ingest-log.jsonl",
            "qualitative-analysis/feedback-classifications.jsonl",
            "qualitative-analysis/feedback-aggregate.json",
            "report.html",
        ],
    )
