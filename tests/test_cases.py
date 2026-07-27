"""Tests for cases, attributes, and entity links (RFC 001, slice 2)."""
from __future__ import annotations

import json
from pathlib import Path

from conftest import BewleyProject


def _json_ok(proj: BewleyProject, *args: str) -> dict:
    code, stdout, stderr = proj.cli(*args, human=False)
    assert code == 0, f"bewley {' '.join(args)} failed (exit {code}): {stderr}\n{stdout}"
    envelope = json.loads(stdout)
    assert envelope["status"] in {"ok", "warning"}
    return envelope["data"]


def _json_err(proj: BewleyProject, *args: str) -> dict:
    code, stdout, stderr = proj.cli(*args, human=False)
    assert code != 0
    return json.loads(stdout)


class TestCases:
    def test_create_list_show(self, project: BewleyProject) -> None:
        data = _json_ok(project, "case", "create", "Abigail Adams", "--type", "person")
        assert data["case_id"]
        rows = _json_ok(project, "case", "list")["cases"]
        assert [row["name"] for row in rows] == ["Abigail Adams"]
        shown = _json_ok(project, "case", "show", "Abigail Adams")
        assert shown["case_type"] == "person"
        assert shown["documents"] == []

    def test_name_prefix_resolution(self, project: BewleyProject) -> None:
        _json_ok(project, "case", "create", "Abigail Adams")
        shown = _json_ok(project, "case", "show", "abig")
        assert shown["name"] == "Abigail Adams"

    def test_ambiguous_reference_lists_matches(self, project: BewleyProject) -> None:
        _json_ok(project, "case", "create", "Abigail Adams")
        _json_ok(project, "case", "create", "Abigail Smith")
        envelope = _json_err(project, "case", "show", "Abigail")
        error = envelope["errors"][0]
        assert error["code"] == "AMBIGUOUS_CASE"
        names = [name for _, name in error["context"]["matches"]]
        assert set(names) == {"Abigail Adams", "Abigail Smith"}

    def test_duplicate_name_rejected(self, project: BewleyProject) -> None:
        _json_ok(project, "case", "create", "Abigail Adams")
        envelope = _json_err(project, "case", "create", "Abigail Adams")
        assert envelope["errors"][0]["code"] == "ALREADY_EXISTS"


class TestAttributes:
    def test_define_set_and_show(self, project: BewleyProject) -> None:
        _json_ok(project, "case", "create", "Abigail Adams")
        _json_ok(project, "attribute", "define", "location", "--type", "categorical",
                 "--values", "braintree,philadelphia")
        _json_ok(project, "case", "set", "Abigail Adams", "location", "braintree")
        shown = _json_ok(project, "case", "show", "Abigail Adams")
        assert shown["attributes"] == [{"name": "location", "value": "braintree", "special": None}]

    def test_categorical_value_validated(self, project: BewleyProject) -> None:
        _json_ok(project, "case", "create", "Abigail Adams")
        _json_ok(project, "attribute", "define", "location", "--type", "categorical",
                 "--values", "braintree,philadelphia")
        envelope = _json_err(project, "case", "set", "Abigail Adams", "location", "boston")
        error = envelope["errors"][0]
        assert error["code"] == "INVALID_INPUT"
        assert "braintree" in error["context"]["allowed_values"]

    def test_special_states(self, project: BewleyProject) -> None:
        _json_ok(project, "case", "create", "John Adams")
        _json_ok(project, "attribute", "define", "age", "--type", "number")
        _json_ok(project, "case", "set", "John Adams", "age", "--special", "unknown")
        shown = _json_ok(project, "case", "show", "John Adams")
        assert shown["attributes"] == [{"name": "age", "value": None, "special": "unknown"}]
        envelope = _json_err(project, "case", "set", "John Adams", "age", "--special", "nope")
        assert envelope["errors"][0]["code"] == "INVALID_INPUT"

    def test_value_and_special_mutually_exclusive(self, project: BewleyProject) -> None:
        _json_ok(project, "case", "create", "John Adams")
        _json_ok(project, "attribute", "define", "age", "--type", "number")
        envelope = _json_err(project, "case", "set", "John Adams", "age", "40",
                             "--special", "unknown")
        assert envelope["errors"][0]["code"] == "INVALID_INPUT"


class TestEntityLinks:
    def test_case_link_sugar_and_listing(self, project: BewleyProject) -> None:
        _json_ok(project, "case", "create", "Abigail Adams")
        _json_ok(project, "case", "link", "Abigail Adams", "corpus/interview_alice.txt",
                 "--as", "author")
        shown = _json_ok(project, "case", "show", "Abigail Adams")
        assert shown["documents"][0]["relationship"] == "author"
        links = _json_ok(project, "link", "list", "--entity", "case:Abigail Adams")["links"]
        assert len(links) == 1
        assert links[0]["target"] == "corpus/interview_alice.txt"

    def test_disallowed_combination_rejected(self, project: BewleyProject) -> None:
        _json_ok(project, "case", "create", "Abigail Adams")
        envelope = _json_err(project, "link", "add", "document:corpus/interview_alice.txt",
                             "case:Abigail Adams", "--rel", "author")
        assert envelope["errors"][0]["code"] == "INVALID_INPUT"

    def test_duplicate_link_rejected(self, project: BewleyProject) -> None:
        _json_ok(project, "case", "create", "Abigail Adams")
        _json_ok(project, "case", "link", "Abigail Adams", "corpus/interview_alice.txt",
                 "--as", "author")
        envelope = _json_err(project, "case", "link", "Abigail Adams",
                             "corpus/interview_alice.txt", "--as", "author")
        assert envelope["errors"][0]["code"] == "ALREADY_EXISTS"

    def test_remove_deactivates(self, project: BewleyProject) -> None:
        _json_ok(project, "case", "create", "Abigail Adams")
        data = _json_ok(project, "case", "link", "Abigail Adams",
                        "corpus/interview_alice.txt", "--as", "author")
        _json_ok(project, "link", "remove", data["link_id"])
        links = _json_ok(project, "link", "list")["links"]
        assert links == []

    def test_code_links_dual_materialized_and_protected(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        project.cli_ok("code", "create", "friction")
        data = _json_ok(project, "code", "link", "trust", "friction", "contradicts")
        links = _json_ok(project, "link", "list")["links"]
        assert len(links) == 1
        assert links[0]["source_kind"] == "code"
        envelope = _json_err(project, "link", "remove", links[0]["link_id"])
        assert "code unlink" in envelope["errors"][0]["hint"]

    def test_rebuild_index_reconstructs_everything(self, project: BewleyProject) -> None:
        _json_ok(project, "case", "create", "Abigail Adams", "--type", "person")
        _json_ok(project, "attribute", "define", "location", "--type", "text")
        _json_ok(project, "case", "set", "Abigail Adams", "location", "Braintree")
        _json_ok(project, "case", "link", "Abigail Adams", "corpus/interview_alice.txt",
                 "--as", "author")
        project.cli_ok("rebuild-index")
        shown = _json_ok(project, "case", "show", "Abigail Adams")
        assert shown["attributes"][0]["value"] == "Braintree"
        assert shown["documents"][0]["relationship"] == "author"
