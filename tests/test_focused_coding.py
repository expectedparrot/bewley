from __future__ import annotations

import json

from conftest import BewleyProject


def _json(project: BewleyProject, *args: str) -> dict:
    code, stdout, stderr = project.cli(*args, human=False)
    assert code == 0, stderr or stdout
    return json.loads(stdout)["data"]


def _result_package(jobs_path, results_path, question_name: str, answers: list[str]) -> None:
    from edsl import Agent, Jobs, Model, Results, Scenario, Survey
    from edsl.results import Result

    jobs = Jobs.git.load(jobs_path)
    rows = [
        Result(
            agent=Agent(),
            scenario=Scenario(dict(scenario)),
            model=Model("test"),
            iteration=0,
            answer={question_name: answer},
        )
        for scenario, answer in zip(jobs.scenarios, answers)
    ]
    Results(survey=Survey([]), data=rows).git.save(results_path)


def test_focused_framework_mapping_apply_and_export(project: BewleyProject) -> None:
    names = [
        "faster_reports", "quicker_research", "work_schedule",
        "positive_interview_feedback", "physical_work_limit",
    ]
    for index, name in enumerate(names):
        _json(project, "code", "create", name, "--description", f"Definition {index}")
    docs = _json(project, "list", "documents")
    for index, name in enumerate(names):
        _json(project, "annotate", "apply", name, docs[index % len(docs)]["current_path"], "--lines", "1:1")

    _json(
        project, "codebook", "focused", "framework-jobs",
        "--output", "focused-framework.jobs.ep",
        "--min-focused", "5", "--max-focused", "5",
    )
    from edsl import Jobs
    framework_jobs = Jobs.git.load(project.root / "focused-framework.jobs.ep")
    context = json.loads(dict(framework_jobs.scenarios[0])["study_context"])
    assert set(context) == {"method", "unit_of_analysis", "purpose", "research_questions"}
    framework = {
        "themes": [
            {"theme_key": "ai_impacts", "name": "AI impacts", "description": "Effects of AI."},
            {"theme_key": "work_context", "name": "Work context", "description": "Context of work."},
            {"theme_key": "interview_quality", "name": "Interview quality", "description": "Interview feedback."},
            {"theme_key": "automation_limits", "name": "Automation limits", "description": "Limits of automation."},
        ],
        "focused_codes": [
            {
                "focused_key": "time_savings", "theme_key": "ai_impacts", "name": "AI-enabled time savings",
                "description": "AI reduces task time.", "inclusion_criteria": "Explicit time reduction.",
                "exclusion_criteria": "General helpfulness without time.",
            },
            {
                "focused_key": "research_support", "theme_key": "ai_impacts", "name": "AI-assisted research",
                "description": "AI supports research.", "inclusion_criteria": "Research uses.",
                "exclusion_criteria": "Writing without research.",
            },
            {
                "focused_key": "schedule_context", "theme_key": "work_context", "name": "Working schedules",
                "description": "When people work.", "inclusion_criteria": "Schedules.",
                "exclusion_criteria": "Task content.",
            },
            {
                "focused_key": "positive_feedback", "theme_key": "interview_quality", "name": "Positive interview experience",
                "description": "Positive interview feedback.", "inclusion_criteria": "Positive feedback.",
                "exclusion_criteria": "Substantive AI claims.",
            },
            {
                "focused_key": "embodied_limits", "theme_key": "automation_limits", "name": "Embodied work limits",
                "description": "Physical work resists automation.", "inclusion_criteria": "Physical constraints.",
                "exclusion_criteria": "Knowledge-work constraints.",
            },
        ],
    }
    _result_package(
        project.root / "focused-framework.jobs.ep",
        project.root / "focused-framework.results.ep",
        "focused_framework",
        [json.dumps(framework)],
    )
    ingested = _json(
        project, "codebook", "focused", "framework-ingest",
        "focused-framework.results.ep", "--jobs", "focused-framework.jobs.ep",
    )
    assert ingested["focused_code_count"] == 5

    jobs = _json(
        project, "codebook", "focused", "mapping-jobs",
        "--output", "focused-mapping.jobs.ep", "--batch-size", "5",
    )
    assert jobs["expected_model_calls"] == 1
    mapping_jobs = Jobs.git.load(project.root / "focused-mapping.jobs.ep")
    ids = dict(mapping_jobs.scenarios[0])["open_code_ids"]
    focused_keys = [
        "time_savings", "research_support", "schedule_context",
        "positive_feedback", "embodied_limits",
    ]
    mappings = [
        {
            "open_code_id": code_id, "focused_key": focused_key,
            "rationale": "Best matching focused category.", "confidence": 0.9,
        }
        for code_id, focused_key in zip(ids, focused_keys)
    ]
    _result_package(
        project.root / "focused-mapping.jobs.ep",
        project.root / "focused-mapping.results.ep",
        "focused_mapping",
        [json.dumps(mappings)],
    )
    mapped = _json(
        project, "codebook", "focused", "mapping-ingest",
        "focused-mapping.results.ep", "--jobs", "focused-mapping.jobs.ep",
    )
    assert mapped["mapping_count"] == 5
    assert _json(project, "codebook", "focused", "apply", "--dry-run")["open_codes_parented"] == 5
    applied = _json(project, "codebook", "focused", "apply")
    assert applied["themes_created"] == 4
    assert applied["focused_codes_created"] == 5

    from bewley.html_export import code_explorer_payload
    from bewley.project import Project

    payload = code_explorer_payload(Project(project.root))
    assert payload["code_count"] == 5
    assert {row["layer"] for row in payload["codes"]} == {"focused"}
    assert all(row["theme_name"] for row in payload["codes"])
    assert sum(len(row["open_codes"]) for row in payload["codes"]) == 5
    assert len(payload["snippets"]) == 5
    assert all(row["open_code_name"] != row["code_name"] for row in payload["snippets"])

    from bewley.html_export import build_code_explorer_html

    explorer = build_code_explorer_html(payload, "Focused explorer")
    assert 'data-tab="codebook"' in explorer
    assert 'id="codebook-list"' in explorer
    assert "Underlying open codes" in explorer
    assert "Copy details" in explorer

    _json(project, "rebuild-index")
    payload = code_explorer_payload(Project(project.root))
    assert payload["code_count"] == 5
