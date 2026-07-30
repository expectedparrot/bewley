from __future__ import annotations

import json

from conftest import BewleyProject


def _json(project: BewleyProject, *args: str) -> dict:
    code, stdout, stderr = project.cli(*args, human=False)
    assert code == 0, stderr or stdout
    return json.loads(stdout)["data"]


def _prepare(project: BewleyProject) -> tuple[dict, dict, str]:
    _json(project, "code", "create", "ai_time_savings", "--description", "AI saves work time.")
    _json(project, "code", "create", "time_saved_by_ai", "--description", "Work is faster with AI.")
    _json(project, "annotate", "apply", "ai_time_savings", "corpus/interview_alice.txt", "--lines", "5:5")
    annotation = _json(
        project, "annotate", "apply", "time_saved_by_ai",
        "corpus/interview_bob.txt", "--lines", "5:5",
    )
    codes = {
        row["canonical_name"]: row
        for row in _json(project, "code", "list")
    }
    return codes["ai_time_savings"], codes["time_saved_by_ai"], annotation["annotation_id"]


def _write_results(project: BewleyProject, source: dict, target: dict, annotation_id: str) -> None:
    from edsl import Agent, Jobs, Model, Results, Scenario, Survey
    from edsl.results import Result

    jobs = Jobs.git.load(project.root / "consolidation.jobs.ep")
    scenario = dict(jobs.scenarios[0])
    answer = json.dumps([{
        "source_code_ids": [source["code_id"]],
        "target_code_id": target["code_id"],
        "rationale": "Both describe time saved by AI.",
        "confidence": 0.96,
        "evidence_annotation_ids": [annotation_id],
    }])
    result = Result(
        agent=Agent(),
        scenario=Scenario(scenario),
        model=Model("test"),
        iteration=0,
        answer={"code_consolidation": answer},
    )
    Results(survey=Survey([]), data=[result]).git.save(project.root / "consolidation.results.ep")


def test_consolidation_review_and_apply(project: BewleyProject) -> None:
    source, target, annotation_id = _prepare(project)
    jobs = _json(
        project, "codebook", "consolidate", "jobs",
        "--output", "consolidation.jobs.ep", "--batch-size", "30",
    )
    assert jobs["code_count"] == 2
    assert jobs["batch_count"] == 1
    _write_results(project, source, target, annotation_id)

    ingested = _json(
        project, "codebook", "consolidate", "ingest",
        "consolidation.results.ep", "--jobs", "consolidation.jobs.ep",
    )
    assert ingested["candidate_count"] == 1
    queue = _json(project, "codebook", "consolidate", "candidates")
    candidate_id = queue["candidates"][0]["candidate_id"]
    assert queue["candidates"][0]["target_name"] == "time_saved_by_ai"

    _json(
        project, "codebook", "consolidate", "review", candidate_id,
        "--decision", "accept", "--reason", "Reviewed as equivalent.",
    )
    preview = _json(project, "codebook", "consolidate", "apply", "--dry-run")
    assert preview["accepted_merges"] == 1
    assert preview["source_codes_merged"] == 1
    applied = _json(project, "codebook", "consolidate", "apply")
    assert len(applied["event_ids"]) == 1

    codes = _json(project, "code", "list")
    assert [row["canonical_name"] for row in codes] == ["time_saved_by_ai"]
    assert _json(project, "code", "show", "time_saved_by_ai")["active_annotations"] == 2

    _json(project, "rebuild-index")
    codes = _json(project, "code", "list")
    assert [row["canonical_name"] for row in codes] == ["time_saved_by_ai"]
    assert _json(project, "code", "show", "time_saved_by_ai")["active_annotations"] == 2


def test_consolidation_apply_refuses_stale_codebook(project: BewleyProject) -> None:
    source, target, annotation_id = _prepare(project)
    _json(project, "codebook", "consolidate", "jobs", "--output", "consolidation.jobs.ep")
    _write_results(project, source, target, annotation_id)
    _json(
        project, "codebook", "consolidate", "ingest",
        "consolidation.results.ep", "--jobs", "consolidation.jobs.ep",
    )
    queue = _json(project, "codebook", "consolidate", "candidates")
    _json(
        project, "codebook", "consolidate", "review",
        queue["candidates"][0]["candidate_id"], "--decision", "accept",
    )
    _json(project, "code", "create", "new_code_after_proposals")

    code, stdout, _ = project.cli(
        "codebook", "consolidate", "apply", "--dry-run", human=False,
    )
    assert code != 0
    assert json.loads(stdout)["errors"][0]["code"] == "STALE_CODEBOOK"
