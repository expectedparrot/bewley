from __future__ import annotations

import json

from conftest import BewleyProject


def _json(project: BewleyProject, *args: str) -> dict:
    code, stdout, stderr = project.cli(*args, human=False)
    assert code == 0, stderr or stdout
    return json.loads(stdout)["data"]


def _add_interview(project: BewleyProject) -> None:
    source = project.root / "ai-interview.txt"
    source.write_text(
        "INTERVIEWER: How is AI changing your work?\n"
        "RESPONDENT: It makes my reporting much faster.\n\n"
        "RESPONDENT: [Feedback about the AI interviewer]\n"
        "RESPONDENT: The interviewer voice was pleasant.\n",
        encoding="utf-8",
    )
    _json(project, "add", str(source))


def test_rapid_insights_respondent_boundary_validation_and_export(project: BewleyProject) -> None:
    from bewley.project import Project

    p = Project(project.root)
    theme = p.add_code("AI impacts", "Effects of AI.", code_layer="theme")["payload"]["code_id"]
    focused = p.add_code("Time savings", "AI reduces task time.", code_layer="focused")["payload"]["code_id"]
    p.update_code(focused, inclusion_criteria="Explicit time effects.", exclusion_criteria="No time claim.")
    p.set_code_parent(focused, theme)
    _add_interview(project)

    packaged = _json(project, "insights", "jobs", "--output", "rapid-insights.jobs.ep")
    assert packaged["expected_model_calls"] == 1

    from edsl import Agent, Jobs, Model, Results, Scenario, Survey
    from edsl.results import Result

    jobs = Jobs.git.load(project.root / "rapid-insights.jobs.ep")
    result_rows = []
    for scenario in jobs.scenarios:
        values = dict(scenario)
        assert "INTERVIEWER:" not in values["respondent_text"]
        assert "Feedback about the AI interviewer" not in values["respondent_text"]
        quote = values["respondent_text"].splitlines()[0]
        answer = {
            "summary": "The respondent described how AI affects work.",
            "sentiment": {"label": "positive", "score": 0.6, "confidence": 0.9},
            "themes": [{"code_id": focused, "confidence": 0.9, "rationale": "Time effect."}],
            "standout_quotes": [{"exact_text": quote, "rationale": "Representative."}],
        }
        result_rows.append(Result(
            agent=Agent(), scenario=Scenario(values), model=Model("test"), iteration=0,
            answer={"rapid_insights": json.dumps(answer)},
        ))
    Results(survey=Survey([]), data=result_rows).git.save(project.root / "rapid-insights.results.ep")

    ingested = _json(
        project, "insights", "ingest", "rapid-insights.results.ep",
        "--jobs", "rapid-insights.jobs.ep",
    )
    assert ingested["response_count"] == 1
    exported = _json(project, "insights", "export")
    assert exported["response_count"] == 1
    page = (project.root / "qualitative-analysis" / "rapid-insights.html").read_text(encoding="utf-8")
    assert "Sentiment toward AI at work" in page
    assert "Standout quotes" in page
    assert "Export JSON" in page


def test_rapid_insights_rejects_nonverbatim_quote(project: BewleyProject) -> None:
    from bewley.project import Project

    p = Project(project.root)
    theme = p.add_code("AI impacts", "Effects.", code_layer="theme")["payload"]["code_id"]
    focused = p.add_code("Efficiency", "Efficiency.", code_layer="focused")["payload"]["code_id"]
    p.set_code_parent(focused, theme)
    _add_interview(project)
    _json(project, "insights", "jobs", "--output", "rapid-insights.jobs.ep")

    from edsl import Agent, Jobs, Model, Results, Scenario, Survey
    from edsl.results import Result

    jobs = Jobs.git.load(project.root / "rapid-insights.jobs.ep")
    rows = []
    for scenario in jobs.scenarios:
        answer = {
            "summary": "Summary.",
            "sentiment": {"label": "neutral", "score": 0, "confidence": 0.8},
            "themes": [{"code_id": focused, "confidence": 0.8, "rationale": "Relevant."}],
            "standout_quotes": [{"exact_text": "invented quotation", "rationale": "No evidence."}],
        }
        rows.append(Result(
            agent=Agent(), scenario=Scenario(dict(scenario)), model=Model("test"), iteration=0,
            answer={"rapid_insights": json.dumps(answer)},
        ))
    Results(survey=Survey([]), data=rows).git.save(project.root / "rapid-insights.results.ep")
    code, stdout, _ = project.cli(
        "insights", "ingest", "rapid-insights.results.ep", "--jobs", "rapid-insights.jobs.ep",
        human=False,
    )
    assert code != 0
    assert json.loads(stdout)["errors"][0]["code"] == "INCOMPLETE_RESULTS"


def test_feedback_workflow_validators_and_deterministic_export(project: BewleyProject) -> None:
    from bewley.commands.insights import (
        _bundle_rows,
        _parse_classification,
        _parse_consolidation,
        _parse_discovery,
    )

    documents = [
        {"document_id": str(index), "feedback_text": f"Response {index}"}
        for index in range(6)
    ]
    bundles = _bundle_rows(documents, seed=7, bundle_size=4, coverage=2)
    assert len(bundles) == 4
    assert sorted(row["document_id"] for bundle in bundles for row in bundle) == sorted(
        [str(index) for index in range(6)] * 2
    )

    sources = {
        "DOC_A": {"feedback_text": "The conversation felt natural."},
        "DOC_B": {"feedback_text": "It was natural and clear."},
    }
    candidate = {
        "code_key": "natural_flow",
        "name": "Natural flow",
        "description": "Conversation felt natural.",
        "inclusion_criteria": "Explicit naturalness.",
        "exclusion_criteria": "Generic praise.",
        "evidence": [
            {"source_id": "DOC_A", "exact_text": "felt natural"},
            {"source_id": "DOC_B", "exact_text": "natural and clear"},
        ],
    }
    assert _parse_discovery([candidate], sources)[0]["code_key"] == "natural_flow"

    framework = {
        "themes": [
            {"theme_key": key, "name": key.title(), "description": f"{key} theme."}
            for key in ("experience", "flow", "technical", "data_quality")
        ],
        "codes": [
            {
                "code_key": f"code_{index}", "theme_key": theme,
                "name": f"Code {index}", "description": "Definition.",
                "inclusion_criteria": "Include evidence.", "exclusion_criteria": "Exclude other evidence.",
                "candidate_ids": [f"candidate-{index}"],
            }
            for index, theme in enumerate(("experience", "flow", "technical", "data_quality"), 1)
        ],
    }
    parsed = _parse_consolidation(framework, {f"candidate-{index}" for index in range(1, 5)}, 4, 4)
    assert len(parsed["codes"]) == 4
    assert "candidate_ids" not in parsed["codes"][0]

    classification = _parse_classification({
        "sentiment": "positive",
        "assignments": [
            {"code_key": "code_1", "exact_text": "felt natural", "confidence": 0.9},
            {"code_key": "code_1", "exact_text": "natural", "confidence": 0.8},
            {"code_key": "invented", "exact_text": "natural", "confidence": 0.7},
        ],
        "potential_new_theme": None,
    }, "The conversation felt natural.", {"code_1"})
    assert len(classification["assignments"]) == 1
    assert len(classification["_rejected_assignments"]) == 2
