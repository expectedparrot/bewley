from __future__ import annotations

import csv
import json

from conftest import BewleyProject


def _json(project: BewleyProject, *args: str) -> dict:
    code, stdout, stderr = project.cli(*args, human=False)
    assert code == 0, stderr or stdout
    envelope = json.loads(stdout)
    assert envelope["status"] in {"ok", "warning"}
    return envelope["data"]


def test_open_coding_jobs_packages_current_revisions(project: BewleyProject) -> None:
    from edsl import Jobs

    data = _json(project, "open-coding", "jobs", "--output", "coding.jobs.ep", "--pilot", "1")

    assert data["object_type"] == "Jobs"
    assert data["scenario_count"] == 1
    jobs = Jobs.git.load(project.root / "coding.jobs.ep")
    scenario = dict(jobs.scenarios[0])
    assert scenario["document_id"]
    assert scenario["revision_id"]
    assert scenario["content_sha256"]
    assert scenario["document_text"]
    assert jobs.survey.question_names == ["open_coding"]


def test_open_coding_ingest_audits_and_resolves_exact_quotes(project: BewleyProject) -> None:
    from edsl import Agent, Jobs, Model, Results, Scenario, Survey
    from edsl.results import Result

    _json(project, "open-coding", "jobs", "--output", "coding.jobs.ep", "--pilot", "1")
    jobs = Jobs.git.load(project.root / "coding.jobs.ep")
    scenario_data = dict(jobs.scenarios[0])
    quote = scenario_data["document_text"].splitlines()[0]
    result = Result(
        agent=Agent(),
        scenario=Scenario(scenario_data),
        model=Model("test"),
        iteration=0,
        answer={
            "open_coding": json.dumps([{
                "code": "opening_idea",
                "description": "The document's opening idea.",
                "quote": quote,
            }])
        },
    )
    results_path = project.root / "coding.results.ep"
    Results(survey=Survey([]), data=[result]).git.save(results_path)

    data = _json(
        project, "open-coding", "ingest", "coding.results.ep",
        "--jobs", "coding.jobs.ep", "--output", "candidates.csv",
    )

    assert data["candidate_count"] == 1
    assert data["unresolved_quotes"] == 0
    assert data["unresolved_details"] == []
    with (project.root / "candidates.csv").open(encoding="utf-8") as handle:
        row = next(csv.DictReader(handle))
    assert row["code_name"] == "opening_idea"
    assert row["quote"] == quote
    assert row["resolve_status"] == "exact"
    assert int(row["byte_end"]) > int(row["byte_start"])


def _result_for(scenario_data: dict, quote: str, model_name: str = "test"):
    from edsl import Agent, Model, Scenario
    from edsl.results import Result

    return Result(
        agent=Agent(),
        scenario=Scenario(scenario_data),
        model=Model(model_name),
        iteration=0,
        answer={
            "open_coding": json.dumps([{
                "code": "opening_idea",
                "description": "The document's opening idea.",
                "quote": quote,
            }])
        },
    )


def test_incomplete_results_fail_closed_and_allow_partial_recovers(project: BewleyProject) -> None:
    from edsl import Jobs, Results, Survey

    _json(project, "open-coding", "jobs", "--output", "coding.jobs.ep", "--pilot", "2")
    jobs = Jobs.git.load(project.root / "coding.jobs.ep")
    first = dict(jobs.scenarios[0])
    quote = first["document_text"].splitlines()[0]
    # Only one of the two expected scenarios returns.
    Results(survey=Survey([]), data=[_result_for(first, quote)]).git.save(
        project.root / "coding.results.ep"
    )

    code, stdout, _ = project.cli(
        "open-coding", "ingest", "coding.results.ep",
        "--jobs", "coding.jobs.ep", "--output", "candidates.csv",
        human=False,
    )
    envelope = json.loads(stdout)
    assert code != 0
    assert envelope["errors"][0]["code"] == "INCOMPLETE_RESULTS"
    assert not (project.root / "candidates.csv").exists()

    data = _json(
        project, "open-coding", "ingest", "coding.results.ep",
        "--jobs", "coding.jobs.ep", "--output", "candidates.csv", "--allow-partial",
    )
    assert data["partial"] is True
    assert data["missing_answers"] == 1
    assert (project.root / "candidates.csv").exists()


def test_multi_model_results_are_not_duplicates(project: BewleyProject) -> None:
    from edsl import Jobs, Results, Survey

    _json(project, "open-coding", "jobs", "--output", "coding.jobs.ep", "--pilot", "1")
    jobs = Jobs.git.load(project.root / "coding.jobs.ep")
    scenario_data = dict(jobs.scenarios[0])
    quote = scenario_data["document_text"].splitlines()[0]
    Results(survey=Survey([]), data=[
        _result_for(scenario_data, quote, "test"),
        _result_for(scenario_data, quote, "gpt-4o-mini"),
    ]).git.save(project.root / "coding.results.ep")

    data = _json(
        project, "open-coding", "ingest", "coding.results.ep",
        "--jobs", "coding.jobs.ep", "--output", "candidates.csv",
    )
    # One scenario answered by two models = two expected answers, no duplicates.
    assert data["duplicate_scenarios"] == 0
    assert data["expected_answers"] == 2
    assert data["missing_answers"] == 0
    assert sorted(data["models"]) == ["gpt-4o-mini", "test"]
    assert data["partial"] is False
    assert data["candidate_count"] == 2
