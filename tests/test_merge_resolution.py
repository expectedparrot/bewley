"""Merge semantics: a target absorbs its sources across evidence surfaces,
while each annotation keeps its original code for provenance."""
from __future__ import annotations

import json

from conftest import BewleyProject


def _json(project: BewleyProject, *args: str) -> dict | list:
    code, stdout, stderr = project.cli(*args, human=False)
    assert code == 0, stderr or stdout
    envelope = json.loads(stdout)
    assert envelope["status"] in {"ok", "warning"}
    return envelope["data"]


def _setup_merge(project: BewleyProject) -> str:
    """Create codes a/b, annotate one doc with each, merge b into a."""
    docs = _json(project, "list", "documents")
    first, second = docs[0]["current_path"], docs[1]["current_path"]
    _json(project, "code", "create", "code_a")
    _json(project, "code", "create", "code_b")
    _json(project, "annotate", "apply", "code_a", first, "--lines", "1:1")
    _json(project, "annotate", "apply", "code_b", second, "--lines", "1:1")
    _json(project, "code", "merge", "code_b", "--into", "code_a")
    return second


def test_target_absorbs_source_annotations(project: BewleyProject) -> None:
    _setup_merge(project)

    snippets = _json(project, "show", "snippets", "--code", "code_a")
    names = {row["code_name"] for row in snippets}
    # The absorbed annotation keeps its original code name for provenance,
    # but surfaces under the target.
    assert len(snippets) == 2
    assert names == {"code_a", "code_b"}

    shown = _json(project, "code", "show", "code_a")
    assert shown["active_annotations"] == 2
    assert shown["absorbs"] == ["code_b"]


def test_queries_resolve_through_the_merge(project: BewleyProject) -> None:
    second = _setup_merge(project)

    matched = _json(project, "query", "code_a")
    assert {row["current_path"] for row in matched} >= {second}

    # The old name still matches its own annotations (monotone compatibility).
    matched_old = _json(project, "query", "code_b")
    assert {row["current_path"] for row in matched_old} == {second}


def test_new_annotations_with_merged_name_land_on_target(project: BewleyProject) -> None:
    _setup_merge(project)
    docs = _json(project, "list", "documents")
    third = docs[2]["current_path"]
    _json(project, "annotate", "apply", "code_b", third, "--lines", "1:1")

    shown = _json(project, "code", "show", "code_a")
    assert shown["active_annotations"] == 3

    from bewley.project import Project

    proj = Project(project.root)
    with proj.connect() as conn:
        target = proj.resolve_code(conn, "code_a")
        orphaned = conn.execute(
            "SELECT COUNT(*) FROM annotations a JOIN codes c ON c.code_id = a.code_id "
            "WHERE c.status = 'merged' AND a.is_active = 1 AND a.created_at > c.created_at",
        ).fetchone()[0]
        newest = conn.execute(
            "SELECT code_id FROM annotations WHERE is_active = 1 ORDER BY created_at DESC, annotation_id DESC LIMIT 1"
        ).fetchone()
    assert newest["code_id"] == target["code_id"]


def test_coverage_counts_absorbed_documents(project: BewleyProject) -> None:
    _setup_merge(project)
    coverage = _json(project, "code", "coverage", "code_a")
    assert coverage["direct"] == 2
    assert coverage["inclusive"] == 2
