"""Tests for the structured codebook (issue #6): criteria, lint, releases."""
from __future__ import annotations

import json

from conftest import BewleyProject


def _json_ok(proj: BewleyProject, *args: str) -> dict:
    code, stdout, stderr = proj.cli(*args, human=False)
    assert code == 0, stderr or stdout
    envelope = json.loads(stdout)
    assert envelope["status"] in {"ok", "warning"}
    return envelope["data"]


def _json_err(proj: BewleyProject, *args: str) -> dict:
    code, stdout, stderr = proj.cli(*args, human=False)
    assert code != 0
    return json.loads(stdout)


class TestCodeUpdate:
    def test_criteria_roundtrip(self, empty_project: BewleyProject) -> None:
        empty_project.cli_ok("code", "create", "trust", "--description", "Expressions of trust.")
        _json_ok(empty_project, "code", "update", "trust",
                 "--inclusion", "Explicit statements of trusting a person or system.",
                 "--exclusion", "Generic positive sentiment; use satisfaction instead.")
        shown = _json_ok(empty_project, "code", "show", "trust")
        assert "Explicit statements" in shown["inclusion_criteria"]
        assert "satisfaction" in shown["exclusion_criteria"]

    def test_update_requires_a_field(self, empty_project: BewleyProject) -> None:
        empty_project.cli_ok("code", "create", "trust")
        envelope = _json_err(empty_project, "code", "update", "trust")
        assert envelope["errors"][0]["code"] == "INVALID_INPUT"

    def test_criteria_survive_rebuild(self, empty_project: BewleyProject) -> None:
        empty_project.cli_ok("code", "create", "trust")
        _json_ok(empty_project, "code", "update", "trust", "--inclusion", "When trust is explicit.")
        empty_project.cli_ok("rebuild-index")
        shown = _json_ok(empty_project, "code", "show", "trust")
        assert shown["inclusion_criteria"] == "When trust is explicit."


class TestCodeLint:
    def test_flags_common_problems(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")  # no description, unused
        project.cli_ok("code", "create", "market tension", "--description", "Market tension")
        project.cli_ok("code", "create", "market_tensions", "--description", "Strain between buyers and sellers.")
        project.cli_ok("annotate", "apply", "market_tensions", "corpus/interview_alice.txt", "--lines", "5:5")
        findings = _json_ok(project, "code", "lint")["findings"]
        checks = {(f["code_name"], f["check"]) for f in findings}
        assert ("trust", "missing_description") in checks
        assert ("trust", "unused_code") in checks
        assert ("market tension", "definition_restates_name") in checks
        assert ("market_tensions", "missing_criteria") in checks
        assert any(f["check"] == "near_duplicate_names" for f in findings)

    def test_clean_codebook_has_no_findings(self, empty_project: BewleyProject) -> None:
        empty_project.cli_ok("code", "create", "parent_theme",
                             "--description", "An umbrella grouping for child codes.")
        empty_project.cli_ok("code", "create", "child_code",
                             "--description", "A specific well-defined idea.")
        empty_project.cli_ok("code", "set-parent", "child_code", "parent_theme")
        findings = _json_ok(empty_project, "code", "lint")["findings"]
        # child has no annotations -> unused; parent is exempt via children
        assert all(f["code_name"] != "parent_theme" for f in findings
                   if f["check"] == "unused_code")


class TestCodebookReleases:
    def test_release_and_diff(self, empty_project: BewleyProject) -> None:
        empty_project.cli_ok("code", "create", "trust", "--description", "Old definition.")
        _json_ok(empty_project, "codebook", "release", "v1")
        empty_project.cli_ok("code", "update", "trust", "--description", "New definition.")
        empty_project.cli_ok("code", "create", "friction", "--description", "Points of difficulty.")
        _json_ok(empty_project, "codebook", "release", "v2")
        diff = _json_ok(empty_project, "codebook", "diff", "v1", "v2")
        assert diff["added"] == ["friction"]
        assert diff["removed"] == []
        assert diff["changed"][0]["code_name"] == "trust"
        assert diff["changed"][0]["changes"]["description"]["to"] == "New definition."

    def test_release_names_are_immutable(self, empty_project: BewleyProject) -> None:
        empty_project.cli_ok("code", "create", "trust")
        _json_ok(empty_project, "codebook", "release", "v1")
        envelope = _json_err(empty_project, "codebook", "release", "v1")
        assert envelope["errors"][0]["code"] == "ALREADY_EXISTS"

    def test_unknown_release_lists_known(self, empty_project: BewleyProject) -> None:
        empty_project.cli_ok("code", "create", "trust")
        _json_ok(empty_project, "codebook", "release", "v1")
        envelope = _json_err(empty_project, "codebook", "diff", "v1", "nope")
        assert envelope["errors"][0]["context"]["known_releases"] == ["v1"]

    def test_merge_shows_as_removed_in_diff(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust", "--description", "Trust in systems.")
        project.cli_ok("code", "create", "confidence", "--description", "Confidence in systems.")
        _json_ok(project, "codebook", "release", "before-merge")
        project.cli_ok("code", "merge", "confidence", "--into", "trust")
        _json_ok(project, "codebook", "release", "after-merge")
        diff = _json_ok(project, "codebook", "diff", "before-merge", "after-merge")
        assert diff["removed"] == ["confidence"]
