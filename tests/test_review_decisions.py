"""Tests for review decisions (issue #5): recorded judgments drive apply."""
from __future__ import annotations

import csv
import json

import pytest

from conftest import BewleyProject


def _json(proj: BewleyProject, *args: str) -> dict:
    code, stdout, stderr = proj.cli(*args, human=False)
    assert code == 0, stderr or stdout
    envelope = json.loads(stdout)
    assert envelope["status"] in {"ok", "warning"}
    return envelope["data"]


def _envelope(proj: BewleyProject, *args: str) -> tuple[int, dict]:
    code, stdout, stderr = proj.cli(*args, human=False)
    return code, json.loads(stdout)


@pytest.fixture
def ingested(empty_project: BewleyProject) -> BewleyProject:
    """A project with two documents ingested into a candidate queue."""
    from edsl import Jobs, Results, Survey

    import sys
    sys.path.insert(0, "tests")
    from test_open_coding_jobs import _result_for

    corpus = empty_project.root / "corpus"
    corpus.mkdir(exist_ok=True)
    (corpus / "one.txt").write_text(
        "The merchant complains of the farmer.\nPrices rose all spring.\n", encoding="utf-8"
    )
    (corpus / "two.txt").write_text(
        "We waited for the post.\nNo letters came for weeks.\n", encoding="utf-8"
    )
    empty_project.cli_ok("add", "corpus/one.txt")
    empty_project.cli_ok("add", "corpus/two.txt")
    _json(empty_project, "open-coding", "jobs", "--output", "jobs.ep")
    jobs = Jobs.git.load(empty_project.root / "jobs.ep")
    scenarios = {dict(s)["document_path"].split("/")[-1]: dict(s) for s in jobs.scenarios}
    Results(survey=Survey([]), data=[
        _result_for(scenarios["one.txt"], "The merchant complains of the farmer."),
        _result_for(scenarios["two.txt"], "We waited for the post."),
    ]).git.save(empty_project.root / "results.ep")
    _json(empty_project, "open-coding", "ingest", "results.ep", "--jobs", "jobs.ep")
    return empty_project


def _candidate_ids(proj: BewleyProject) -> list[str]:
    with (proj.root / "qualitative-analysis" / "candidate_codes.csv").open() as handle:
        return [row["candidate_id"] for row in csv.DictReader(handle)]


class TestReviewCommand:
    def test_reject_with_reason_then_apply_skips_it(self, ingested: BewleyProject) -> None:
        first, second = _candidate_ids(ingested)
        _json(ingested, "open-coding", "review", first,
              "--decision", "reject", "--reason", "topic label")
        _json(ingested, "open-coding", "review", second, "--decision", "accept")
        data = _json(ingested, "open-coding", "apply")
        assert data["review_mode"] == "decisions"
        assert data["annotations_applied"] == 1
        assert data["decisions"]["rejected"] == 1
        rejected = [d for d in data["skipped_details"] if d["reason"] == "rejected"]
        assert rejected[0]["review_reason"] == "topic label"

    def test_undecided_candidates_fail_closed(self, ingested: BewleyProject) -> None:
        first, _ = _candidate_ids(ingested)
        _json(ingested, "open-coding", "review", first, "--decision", "accept")
        data = _json(ingested, "open-coding", "apply")
        assert data["annotations_applied"] == 1
        assert data["decisions"]["undecided"] == 1
        assert any(d["reason"] == "undecided" for d in data["skipped_details"])

    def test_map_applies_as_target_code(self, ingested: BewleyProject) -> None:
        first, second = _candidate_ids(ingested)
        _json(ingested, "open-coding", "review", first,
              "--decision", "map", "--to", "market_tension")
        _json(ingested, "open-coding", "review", second, "--decision", "reject")
        data = _json(ingested, "open-coding", "apply")
        assert data["annotations_applied"] == 1
        codes = _json(ingested, "code", "list")
        listed = codes["codes"] if isinstance(codes, dict) else codes
        assert "market_tension" in [c["canonical_name"] for c in listed]

    def test_all_remaining_accepts_rest(self, ingested: BewleyProject) -> None:
        first, _ = _candidate_ids(ingested)
        _json(ingested, "open-coding", "review", first, "--decision", "reject")
        data = _json(ingested, "open-coding", "review",
                     "--all-remaining", "--decision", "accept")
        assert data["recorded_count"] == 1
        apply_data = _json(ingested, "open-coding", "apply")
        assert apply_data["annotations_applied"] == 1
        assert apply_data["decisions"]["undecided"] == 0

    def test_prefix_resolution_and_ambiguity(self, ingested: BewleyProject) -> None:
        first, _ = _candidate_ids(ingested)
        _json(ingested, "open-coding", "review", first[:8], "--decision", "accept")
        code, envelope = _envelope(ingested, "open-coding", "review", "",
                                   "--decision", "accept")
        assert code != 0  # empty prefix matches everything

    def test_legacy_mode_warns_without_decisions(self, ingested: BewleyProject) -> None:
        code, envelope = _envelope(ingested, "open-coding", "apply")
        assert code == 0
        assert envelope["status"] == "warning"
        assert envelope["data"]["review_mode"] == "csv-rows"
        assert envelope["data"]["annotations_applied"] == 2

    def test_candidates_show_decisions(self, ingested: BewleyProject) -> None:
        first, _ = _candidate_ids(ingested)
        _json(ingested, "open-coding", "review", first,
              "--decision", "reject", "--reason", "too broad")
        data = _json(ingested, "open-coding", "candidates")
        assert data["undecided_count"] == 1
        by_id = {row["candidate_id"]: row for row in data["candidates"]}
        assert by_id[first]["decision"] == "reject"
        assert by_id[first]["decision_reason"] == "too broad"

    def test_decisions_survive_rebuild(self, ingested: BewleyProject) -> None:
        first, second = _candidate_ids(ingested)
        _json(ingested, "open-coding", "review", first, "--decision", "reject")
        _json(ingested, "open-coding", "review", second, "--decision", "accept")
        ingested.cli_ok("rebuild-index")
        data = _json(ingested, "open-coding", "apply")
        assert data["review_mode"] == "decisions"
        assert data["annotations_applied"] == 1

    def test_map_requires_target(self, ingested: BewleyProject) -> None:
        first, _ = _candidate_ids(ingested)
        code, envelope = _envelope(ingested, "open-coding", "review", first, "--decision", "map")
        assert code != 0
        assert envelope["errors"][0]["code"] == "INVALID_INPUT"
