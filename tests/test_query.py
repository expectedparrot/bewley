"""Tests for query and show snippets commands."""
from __future__ import annotations

from conftest import BewleyProject


def _setup_coded_project(project: BewleyProject) -> None:
    """Create codes and annotate all three documents for query tests."""
    project.cli_ok("code", "create", "trust")
    project.cli_ok("code", "create", "friction")
    project.cli_ok("code", "create", "workaround")

    # Alice: trust + friction + workaround
    project.cli_ok("annotate", "apply", "trust", "corpus/interview_alice.txt", "--document")
    project.cli_ok("annotate", "apply", "friction", "corpus/interview_alice.txt", "--document")
    project.cli_ok("annotate", "apply", "workaround", "corpus/interview_alice.txt", "--document")

    # Bob: friction + workaround
    project.cli_ok("annotate", "apply", "friction", "corpus/interview_bob.txt", "--document")
    project.cli_ok("annotate", "apply", "workaround", "corpus/interview_bob.txt", "--document")

    # Carol: trust + friction
    project.cli_ok("annotate", "apply", "trust", "corpus/interview_carol.txt", "--document")
    project.cli_ok("annotate", "apply", "friction", "corpus/interview_carol.txt", "--document")


class TestQueryDocument:
    def test_single_code(self, project: BewleyProject) -> None:
        _setup_coded_project(project)
        stdout = project.cli_ok("query", "trust", "--mode", "document")
        assert "interview_alice" in stdout
        assert "interview_carol" in stdout
        assert "interview_bob" not in stdout

    def test_and_query(self, project: BewleyProject) -> None:
        _setup_coded_project(project)
        stdout = project.cli_ok("query", "trust AND friction", "--mode", "document")
        assert "interview_alice" in stdout
        assert "interview_carol" in stdout
        assert "interview_bob" not in stdout

    def test_or_query(self, project: BewleyProject) -> None:
        _setup_coded_project(project)
        stdout = project.cli_ok("query", "trust OR workaround", "--mode", "document")
        assert "interview_alice" in stdout
        assert "interview_bob" in stdout
        assert "interview_carol" in stdout

    def test_not_query(self, project: BewleyProject) -> None:
        _setup_coded_project(project)
        stdout = project.cli_ok("query", "friction AND NOT trust", "--mode", "document")
        assert "interview_bob" in stdout
        assert "interview_alice" not in stdout
        assert "interview_carol" not in stdout

    def test_complex_query(self, project: BewleyProject) -> None:
        _setup_coded_project(project)
        stdout = project.cli_ok(
            "query", "(trust OR workaround) AND NOT friction", "--mode", "document"
        )
        # All three have friction, so nothing should match
        assert "interview_alice" not in stdout
        assert "interview_bob" not in stdout
        assert "interview_carol" not in stdout


class TestQueryAnnotation:
    def test_annotation_mode_with_overlapping_spans(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        project.cli_ok("code", "create", "friction")

        # Apply overlapping span annotations on alice
        project.cli_ok(
            "annotate", "apply", "trust", "corpus/interview_alice.txt", "--lines", "5:6"
        )
        project.cli_ok(
            "annotate", "apply", "friction", "corpus/interview_alice.txt", "--lines", "5:6"
        )

        stdout = project.cli_ok("query", "trust AND friction", "--mode", "annotation")
        assert "interview_alice" in stdout


class TestShowSnippets:
    def test_snippets_for_code(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        project.cli_ok(
            "annotate", "apply", "trust", "corpus/interview_alice.txt", "--lines", "5:5"
        )
        project.cli_ok(
            "annotate", "apply", "trust", "corpus/interview_carol.txt", "--lines", "24:25"
        )

        stdout = project.cli_ok("show", "snippets", "--code", "trust")
        assert "interview_alice" in stdout
        assert "interview_carol" in stdout

    def test_snippets_empty_code(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "unused")
        stdout = project.cli_ok("show", "snippets", "--code", "unused")
        # Should succeed with no results (or a "no annotations" message)
        assert stdout is not None


def test_symbolic_and_keyword_operators_are_equivalent(project: BewleyProject) -> None:
    """Docs teach & | ! and AND OR NOT; both must parse identically."""
    import json as _json

    docs = _json.loads(project.cli("list", "documents", human=False)[1])["data"]
    first = docs[0]["current_path"]
    project.cli("code", "create", "alpha", human=False)
    project.cli("code", "create", "beta", human=False)
    project.cli("annotate", "apply", "alpha", first, "--lines", "1:1", human=False)

    for expr in ["alpha & !beta", "alpha AND NOT beta", "(alpha | beta) & !beta"]:
        code, stdout, _ = project.cli("query", expr, human=False)
        assert code == 0, stdout
        rows = _json.loads(stdout)["data"]
        assert {row["current_path"] for row in rows} == {first}, expr
