from __future__ import annotations

import json
import shlex
from pathlib import Path

from conftest import BewleyProject
from bewley.cli import main
from test_subprocess_contract import run_bewley


def envelope(project: BewleyProject, *args: str) -> tuple[int, dict, str]:
    code, stdout, stderr = project.cli(*args, human=False)
    payload = json.loads(stdout)
    assert stdout.count("\n{") == 0, "stdout must contain exactly one JSON document"
    assert payload["schema_version"] == "2.0"
    assert payload["argv"] == ["bewley", *args]
    assert isinstance(payload["command"], str) and payload["command"].startswith("bewley")
    assert isinstance(payload["warnings"], list)
    assert isinstance(payload["errors"], list)
    assert isinstance(payload["next_steps"], list)
    return code, payload, stderr


def test_success_envelope_uses_status_and_real_argv(empty_project: BewleyProject) -> None:
    code, payload, stderr = envelope(empty_project, "status")
    assert code == 0
    assert stderr == ""
    assert payload["status"] == "ok"
    assert payload["command"] == "bewley status"
    assert "data" in payload
    assert payload["errors"] == []


def test_domain_error_is_one_envelope(empty_project: BewleyProject) -> None:
    code, payload, _ = envelope(empty_project, "show", "document", "missing.txt")
    assert code != 0
    assert payload["status"] == "error"
    assert payload["command"] == "bewley show document"
    assert len(payload["errors"]) == 1
    error = payload["errors"][0]
    assert {"code", "message", "context"} <= set(error)
    assert payload["data"] == {}


def test_cli_usage_error_is_enveloped(empty_project: BewleyProject) -> None:
    code, payload, _ = envelope(empty_project, "not-a-command")
    assert code == 1
    assert payload["status"] == "error"
    assert payload["errors"][0]["code"] == "CLI_USAGE"
    assert payload["next_steps"][0]["command"] == ["bewley", "--help"]
    assert payload["next_steps"][0]["mutates_state"] is False


def test_nested_cli_usage_error_points_to_closest_help(empty_project: BewleyProject) -> None:
    code, payload, _ = envelope(empty_project, "memo", "add", "--text", "body")
    assert code == 1
    assert payload["errors"][0]["code"] == "CLI_USAGE"
    assert payload["next_steps"][0]["command"] == ["bewley", "memo", "add", "--help"]


def test_missing_quote_selector_returns_executable_recovery(empty_project: BewleyProject) -> None:
    code, payload, _ = envelope(empty_project, "export", "quotes")
    assert code == 1
    assert payload["errors"][0]["code"] == "INVALID_INPUT"
    assert payload["next_steps"][0]["command"] == ["bewley", "export", "quotes", "--all"]
    assert payload["next_steps"][0]["mutates_state"] is False


def test_duplicate_code_points_to_update(empty_project: BewleyProject) -> None:
    assert envelope(empty_project, "code", "create", "trust")[0] == 0
    code, payload, _ = envelope(empty_project, "code", "create", "trust")
    assert code == 1
    assert payload["errors"][0]["code"] == "ALREADY_EXISTS"
    assert payload["next_steps"][0]["command"] == ["bewley", "code", "update", "trust", "--help"]


def test_guide_exposes_exact_common_operation_contracts(empty_project: BewleyProject) -> None:
    code, payload, _ = envelope(empty_project, "guide")
    assert code == 0
    contracts = {item["id"]: item for item in payload["data"]["operation_contracts"]}
    assert contracts["add-memo"]["argv_template"][3] == "<content>"
    assert "map" in contracts["review-candidate"]["argv_templates"]
    assert contracts["export-quotes"]["required"] == ["exactly one of --code, --query, --all"]
    assert "--output" not in contracts["export-quotes"]["optional"]


def test_init_returns_executable_help_for_missing_input(tmp_path: Path) -> None:
    project = BewleyProject(tmp_path)
    code, payload, _ = envelope(project, "init")
    assert code == 0
    action = payload["next_steps"][0]
    assert action["command"] == ["bewley", "add", "--help"]
    assert action["mutates_state"] is False
    assert action["requires_network"] is False
    assert action["requires_user_approval"] is False


def test_capabilities_lists_versioned_schemas(empty_project: BewleyProject) -> None:
    code, payload, _ = envelope(empty_project, "capabilities")
    assert code == 0
    assert payload["data"]["schemas"] == [
        "action.schema.json",
        "agent-status.schema.json",
        "envelope.schema.json",
    ]


def test_agent_schema_returns_envelope_schema(empty_project: BewleyProject) -> None:
    code, payload, _ = envelope(empty_project, "agent", "schema", "envelope")
    assert code == 0
    schema = payload["data"]["schema"]
    assert schema["title"] == "Bewley CLI envelope"
    success_shape = schema["oneOf"][0]["properties"]
    assert success_shape["schema_version"]["const"] == "2.0"
    assert "status" in success_shape and "next_steps" in success_shape


def test_agent_status_returns_executable_actions(empty_project: BewleyProject) -> None:
    code, payload, _ = envelope(empty_project, "agent", "status")
    assert code == 0
    data = payload["data"]
    assert data["ready"] is True
    assert data["next_actions"]
    assert isinstance(data["next_actions"][0]["command"], list)


def test_ambiguous_references_list_their_matches(empty_project: BewleyProject) -> None:
    (empty_project.root / "a").mkdir()
    (empty_project.root / "b").mkdir()
    (empty_project.root / "a" / "x.txt").write_text("alpha text\n", encoding="utf-8")
    (empty_project.root / "b" / "x.txt").write_text("beta text\n", encoding="utf-8")
    empty_project.cli("add", "a/x.txt", human=False)
    empty_project.cli("add", "b/x.txt", human=False)

    code, payload, _ = envelope(empty_project, "show", "document", "x.txt")
    assert code != 0
    error = payload["errors"][0]
    assert error["code"] == "AMBIGUOUS_DOCUMENT"
    paths = {match["path"] for match in error["context"]["matches"]}
    assert paths == {"a/x.txt", "b/x.txt"}

    code, payload, _ = envelope(empty_project, "code", "show", "nope")
    assert code != 0
    assert payload["errors"][0]["code"] == "NOT_FOUND"


def test_main_unexpected_error_is_not_silent(monkeypatch, capsys) -> None:
    from bewley import cli

    def explode(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(cli, "app", explode)
    code = main(["status"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 1
    assert payload["status"] == "error"
    assert payload["errors"][0]["code"] == "INTERNAL_ERROR"
    assert payload["errors"][0]["context"]["exception_type"] == "RuntimeError"


def test_discovery_only_cli_journey_completes_without_usage_errors(tmp_path: Path) -> None:
    """Exercise the documented lifecycle through the real subprocess entry point.

    The only model Results used here are a locally serialized fixture. The test
    verifies that the agent-facing discovery responses are enough to complete
    review and export without running a model or probing CLI help after errors.
    """
    from edsl import Agent, Jobs, Model, Results, Scenario, Survey
    from edsl.results import Result

    root = tmp_path
    executed: list[list[str]] = []

    def invoke(command: list[str]) -> dict:
        assert command[0] == "bewley"
        args = command[1:]
        completed = run_bewley(root, *args)
        executed.append(command)
        assert completed.returncode == 0, f"{' '.join(command)} failed:\n{completed.stdout}\n{completed.stderr}"
        assert completed.stderr == ""
        payload = json.loads(completed.stdout)
        assert completed.stdout.count("\n{") == 0
        assert payload["argv"] == command
        assert payload["status"] in {"ok", "warning"}
        assert not any(error["code"] == "CLI_USAGE" for error in payload["errors"])
        assert payload["errors"] == []
        return payload

    def replace_tokens(template: list[str], values: dict[str, str | list[str]]) -> list[str]:
        used: dict[str, int] = {}
        result = []
        for token in template:
            value = values.get(token, token)
            if isinstance(value, list):
                index = used.get(token, 0)
                assert index < len(value), f"no value supplied for {token} occurrence {index + 1}"
                result.append(value[index])
                used[token] = index + 1
            else:
                result.append(value)
        unresolved = [token for token in result if token.startswith("<") and token.endswith(">")]
        assert not unresolved, f"unfilled command template values: {unresolved}"
        return result

    def next_data() -> dict:
        payload = invoke(["bewley", "next"])
        return {**payload["data"], "next_steps": payload["next_steps"]}

    def next_action(data: dict) -> dict:
        steps = data.get("next_steps", [])
        assert steps, f"no next action at stage {data.get('stage')}"
        action = steps[0]
        assert action["command"][0] == "bewley"
        return action

    def fulfill_action(action: dict, values: dict[str, str | list[str]]) -> list[str]:
        command = action["command"]
        if "--help" in command and action.get("reason", "").startswith("Supply the required values using this template:"):
            command = shlex.split(action["reason"].split("template:", 1)[1].strip())
        return replace_tokens(command, values)

    capabilities = invoke(["bewley", "capabilities"])["data"]
    guide = invoke(["bewley", "guide"])["data"]
    catalog = guide["command_catalog"]
    by_path = {tuple(item["command"][1:]): item for item in catalog}
    contracts = {item["id"]: item for item in guide["operation_contracts"]}

    def catalog_action(path: tuple[str, ...], values: dict[str, str | list[str]] | None = None) -> list[str]:
        spec = by_path[path]
        assert spec["mutates_state"] is False or not spec["requires_user_approval"]
        command = replace_tokens(spec["argv_template"], values or {})
        return command

    def run_discovered(command: list[str]) -> dict:
        matching = [
            item for item in catalog
            if command[:len(item["command"])] == item["command"]
        ]
        assert matching, f"command not present in guide catalog: {command}"
        spec = max(matching, key=lambda item: len(item["command"]))
        assert not spec["requires_network"]
        assert not spec["requires_user_approval"]
        return invoke(command)

    # Let `next` select init, then fill its concrete corpus and study values.
    initial = next_data()
    init_step = next_action(initial)
    assert init_step["id"] == "init-project"
    run_discovered(init_step["command"])
    study = next_data()
    study_step = next_action(study)
    assert study_step["id"] == "study-set"
    run_discovered(fulfill_action(study_step, {
        "<method>": "content-analysis", "<unit-of-analysis>": "document",
    }))
    question_data = next_data()
    question_step = next_action(question_data)
    assert question_step["id"] == "question-add"
    run_discovered(fulfill_action(question_step, {
        "<question>": "How did prices change during the season?",
    }))
    corpus_path = "corpus/interview.txt"
    (root / "corpus").mkdir(parents=True, exist_ok=True)
    (root / corpus_path).write_text("Prices rose all spring, then stabilized.\n", encoding="utf-8")
    corpus_state = next_data()
    add_step = next_action(corpus_state)
    assert add_step["id"] == "add-corpus/<filename>"
    run_discovered(fulfill_action(add_step, {"corpus/<filename>": corpus_path}))

    package_state = next_data()
    package_step = next_action(package_state)
    assert package_step["id"] == "open-coding-jobs"
    package_command = list(package_step["command"])
    jobs_spec = by_path[("open-coding", "jobs")]
    option_names = {option for parameter in jobs_spec["parameters"] for option in parameter["options"]}
    assert {"--pilot", "--model"} <= option_names
    package_command.extend(["--pilot", "1", "--model", "test"])
    run_discovered(package_command)

    external_state = next_data()
    external_step = external_state["next_steps"][0]
    assert external_state["stage"] == "awaiting-external-results"
    assert external_step["command"][0] == "ep"
    assert external_step["requires_network"] is True
    assert external_step["requires_user_approval"] is True
    assert not any(command[0] == "ep" for command in executed)

    # Serialize a deterministic EDSL Results fixture in place of external inference.
    jobs = Jobs.git.load(root / "jobs.ep")
    scenario_data = dict(jobs.scenarios[0])
    quote = "Prices rose all spring"
    result = Result(
        agent=Agent(), scenario=Scenario(scenario_data),
        model=Model("test", service_name="test"), iteration=0,
        answer={"open_coding": json.dumps([{
            "code": "seasonal_price_pressure",
            "description": "Prices increased during the spring season.",
            "quote": quote,
        }])},
    )
    Results(survey=Survey([]), data=[result]).git.save(root / "results.ep")

    ingest_state = next_data()
    assert ingest_state["stage"] == "results-awaiting-ingest"
    ingest_step = next_action(ingest_state)
    run_discovered(ingest_step["command"])
    review_state = next_data()
    assert review_state["stage"] == "candidates-awaiting-review"

    candidates_command = catalog_action(("open-coding", "candidates"))
    candidates = run_discovered(candidates_command)["data"]["candidates"]
    assert len(candidates) == 1
    candidate_id = candidates[0]["candidate_id"]
    decision_template = contracts["review-candidate"]["argv_templates"]["accept"]
    review_command = replace_tokens(decision_template, {
        "<candidate-ref>": candidate_id,
        "<reason>": "Accepted in the agent workflow integration fixture.",
    })
    review_result = run_discovered(review_command)

    dry_run_step = review_result["next_steps"][0]
    assert dry_run_step["command"] == ["bewley", "open-coding", "apply", "--dry-run"]
    dry_result = run_discovered(dry_run_step["command"])
    assert dry_result["data"]["annotations_applied"] == 0
    apply_command = catalog_action(("open-coding", "apply"))
    applied = run_discovered(apply_command)
    assert applied["data"]["annotations_applied"] == 1
    next_data()

    code_name = candidates[0]["code_name"]
    update_template = contracts["update-code"]["argv_template"]
    update_command = replace_tokens(update_template, {
        "<code-ref>": code_name,
        "<definition>": "Seasonal price increases reported by the author.",
        "<criteria>": ["Include explicit within-season price changes.", "Exclude price levels without a seasonal comparison."],
    })
    run_discovered(update_command)
    next_data()

    release_command = catalog_action(("codebook", "release"), {"<name>": "agent-journey"})
    run_discovered(release_command)
    next_data()

    memo_template = contracts["add-memo"]["argv_template"]
    memo_command = replace_tokens(memo_template, {
        "<content>": "The participant reports a seasonal increase followed by stabilization.",
        "<title>": "Journey memo",
    })
    run_discovered(memo_command)
    next_data()

    quote_template = contracts["export-quotes"]["argv_template"]
    quotes_command = replace_tokens(quote_template, {"<code-ref>": code_name})
    quotes = run_discovered(quotes_command)["data"]
    assert len(quotes) == 1
    assert quotes[0]["exact_text"] == quote
    next_data()

    html_template = contracts["export-html"]["argv_template"]
    html_command = replace_tokens(html_template, {"<path.html>": "journey.html"})
    run_discovered(html_command)
    assert (root / "journey.html").is_file()
    next_data()

    assert capabilities["safety"]["next_step_commands_are_argv_arrays"] is True
    assert all(command[0] == "bewley" for command in executed)
