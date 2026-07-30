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


def _broken_result(scenario_data: dict, model_name: str = "test"):
    from edsl import Agent, Model, Scenario
    from edsl.results import Result

    return Result(
        agent=Agent(),
        scenario=Scenario(scenario_data),
        model=Model(model_name),
        iteration=0,
        answer={
            "open_coding": (
                "[{'code': 'broken', 'description': 'bad', "
                "'quote': 'unterminated}]"
            )
        },
    )


def test_retry_flow_repackages_failures_and_merges_with_attribution(project: BewleyProject) -> None:
    from edsl import Jobs, Results, Survey

    _json(project, "open-coding", "jobs", "--output", "coding.jobs.ep", "--pilot", "2")
    jobs = Jobs.git.load(project.root / "coding.jobs.ep")
    first, second = (dict(item) for item in jobs.scenarios)
    quote_first = first["document_text"].splitlines()[0]
    quote_second = second["document_text"].splitlines()[0]

    # First run: one valid answer, one unparseable answer.
    Results(survey=Survey([]), data=[
        _result_for(first, quote_first),
        _broken_result(second),
    ]).git.save(project.root / "run1.results.ep")

    # A retry package contains only the failed document.
    retry = _json(
        project, "open-coding", "jobs",
        "--output", "retry.jobs.ep",
        "--from-failures", "run1.results.ep", "--jobs", "coding.jobs.ep",
    )
    assert retry["scenario_count"] == 1
    assert retry["failed_documents"] == 1
    retry_jobs = Jobs.git.load(project.root / "retry.jobs.ep")
    assert dict(retry_jobs.scenarios[0])["document_id"] == second["document_id"]

    # Retry run answers the failed scenario; merged ingest is complete and
    # attributes each retained row to its source file.
    Results(survey=Survey([]), data=[_result_for(second, quote_second)]).git.save(
        project.root / "run2.results.ep"
    )
    data = _json(
        project, "open-coding", "ingest", "run1.results.ep", "run2.results.ep",
        "--jobs", "coding.jobs.ep", "--output", "candidates.csv",
    )
    assert data["partial"] is False
    assert data["failed_scenarios"] == 0
    assert data["candidate_count"] == 2
    assert data["superseded_answers"] == 0
    retained = data["retained_by_source"]
    assert retained[str(project.root / "run1.results.ep")] == 1
    assert retained[str(project.root / "run2.results.ep")] == 1
    with (project.root / "candidates.csv").open(encoding="utf-8") as handle:
        by_doc = {row["source_document_id"]: row["source_results"] for row in csv.DictReader(handle)}
    assert by_doc[first["document_id"]].endswith("run1.results.ep")
    assert by_doc[second["document_id"]].endswith("run2.results.ep")


def test_retry_rerunning_valid_scenarios_is_superseded_not_duplicate(project: BewleyProject) -> None:
    from edsl import Jobs, Results, Survey

    _json(project, "open-coding", "jobs", "--output", "coding.jobs.ep", "--pilot", "1")
    jobs = Jobs.git.load(project.root / "coding.jobs.ep")
    scenario_data = dict(jobs.scenarios[0])
    quote = scenario_data["document_text"].splitlines()[0]
    Results(survey=Survey([]), data=[_result_for(scenario_data, quote)]).git.save(
        project.root / "run1.results.ep"
    )
    Results(survey=Survey([]), data=[_result_for(scenario_data, quote)]).git.save(
        project.root / "run2.results.ep"
    )

    data = _json(
        project, "open-coding", "ingest", "run1.results.ep", "run2.results.ep",
        "--jobs", "coding.jobs.ep", "--output", "candidates.csv",
    )
    assert data["partial"] is False
    assert data["duplicate_scenarios"] == 0
    assert data["superseded_answers"] == 1
    assert data["retained_by_source"][str(project.root / "run1.results.ep")] == 1
    assert data["retained_by_source"][str(project.root / "run2.results.ep")] == 0


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


def test_ingest_marks_interviewer_anchored_quotes(empty_project: BewleyProject) -> None:
    from edsl import Jobs, Results, Survey

    transcript = (
        "INTERVIEWER: What changed after the war began?\n\n"
        "NARRATOR: Everything changed. The prices, the post, the whole "
        "rhythm of the town.\n"
    )
    path = empty_project.root / "corpus" / "talk.txt"
    path.parent.mkdir(exist_ok=True)
    path.write_text(transcript, encoding="utf-8")
    empty_project.cli_ok("add", "corpus/talk.txt")
    empty_project.cli_ok("speakers", "detect", "corpus/talk.txt")
    empty_project.cli_ok("speakers", "set-role", "INTERVIEWER", "interviewer")
    empty_project.cli_ok("speakers", "set-role", "NARRATOR", "participant")

    _json(empty_project, "open-coding", "jobs", "--output", "talk.jobs.ep")
    jobs = Jobs.git.load(empty_project.root / "talk.jobs.ep")
    scenario_data = dict(jobs.scenarios[0])
    results_path = empty_project.root / "talk.results.ep"
    Results(survey=Survey([]), data=[
        _result_for(scenario_data, "What changed after the war began?"),
    ]).git.save(results_path)

    data = _json(empty_project, "open-coding", "ingest", str(results_path),
                 "--jobs", "talk.jobs.ep")
    assert data["unresolved_quotes"] == 1
    assert data["unresolved_details"][0]["resolve_status"] == "interviewer_text"

    with (empty_project.root / "qualitative-analysis" / "candidate_codes.csv").open() as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["resolve_status"] == "interviewer_text"
    assert rows[0]["byte_start"]  # located, just disallowed

    apply_data = _json(empty_project, "open-coding", "apply")
    assert apply_data["annotations_applied"] == 0
    assert apply_data["skipped_details"][0]["reason"] == "unresolved_quote:interviewer_text"


def test_ingest_participant_quotes_unaffected_by_segmentation(empty_project: BewleyProject) -> None:
    from edsl import Jobs, Results, Survey

    transcript = (
        "INTERVIEWER: What changed after the war began?\n\n"
        "NARRATOR: Everything changed. The prices, the post, the whole "
        "rhythm of the town.\n"
    )
    path = empty_project.root / "corpus" / "talk.txt"
    path.parent.mkdir(exist_ok=True)
    path.write_text(transcript, encoding="utf-8")
    empty_project.cli_ok("add", "corpus/talk.txt")
    empty_project.cli_ok("speakers", "detect", "corpus/talk.txt")
    empty_project.cli_ok("speakers", "set-role", "INTERVIEWER", "interviewer")
    empty_project.cli_ok("speakers", "set-role", "NARRATOR", "participant")

    _json(empty_project, "open-coding", "jobs", "--output", "talk.jobs.ep")
    jobs = Jobs.git.load(empty_project.root / "talk.jobs.ep")
    scenario_data = dict(jobs.scenarios[0])
    results_path = empty_project.root / "talk.results.ep"
    Results(survey=Survey([]), data=[
        _result_for(scenario_data, "The prices, the post, the whole rhythm of the town."),
    ]).git.save(results_path)

    data = _json(empty_project, "open-coding", "ingest", str(results_path),
                 "--jobs", "talk.jobs.ep")
    assert data["unresolved_quotes"] == 0
    apply_data = _json(empty_project, "open-coding", "apply")
    assert apply_data["annotations_applied"] == 1
