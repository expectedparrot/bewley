"""Tests for export commands: snippets, quotes, html, document-html."""
from __future__ import annotations

import json

from tests.conftest import BewleyProject


class TestExportSnippets:
    def test_jsonl_export(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "friction")
        project.cli_ok(
            "annotate", "apply", "friction", "corpus/interview_alice.txt", "--lines", "15:17"
        )

        stdout = project.cli_ok(
            "export", "snippets", "--code", "friction", "--format", "jsonl"
        )
        lines = [line for line in stdout.strip().splitlines() if line.strip()]
        assert len(lines) >= 1
        record = json.loads(lines[0])
        assert record["code_name"] == "friction"
        assert "selected_text" in record
        assert "onboarding" in record["selected_text"]

    def test_jsonl_with_context_lines(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "friction")
        project.cli_ok(
            "annotate", "apply", "friction", "corpus/interview_alice.txt", "--lines", "15:15"
        )

        stdout = project.cli_ok(
            "export", "snippets", "--code", "friction", "--format", "jsonl",
            "--context-lines", "2"
        )
        record = json.loads(stdout.strip().splitlines()[0])
        assert "context_before" in record
        assert "context_after" in record


class TestExportQuotes:
    def test_quotes_have_byte_provenance(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        project.cli_ok(
            "annotate", "apply", "trust", "corpus/interview_alice.txt", "--lines", "5:5"
        )

        stdout = project.cli_ok(
            "export", "quotes", "--code", "trust", "--format", "jsonl"
        )
        record = json.loads(stdout.strip().splitlines()[0])
        assert "exact_text" in record
        assert "start_byte" in record
        assert "end_byte" in record
        assert isinstance(record["start_byte"], int)
        assert isinstance(record["end_byte"], int)
        assert record["start_byte"] < record["end_byte"]


class TestExportHtml:
    def test_code_explorer_html(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        project.cli_ok(
            "annotate", "apply", "trust", "corpus/interview_alice.txt", "--document"
        )

        stdout = project.cli_ok(
            "export", "html", "--output", "report/codes.html", "--title", "Test Study"
        )
        report_path = project.root / stdout.strip()
        assert report_path.exists()
        html_content = report_path.read_text(encoding="utf-8")
        assert "Test Study" in html_content
        assert "trust" in html_content

    def test_document_html(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "friction")
        project.cli_ok(
            "annotate", "apply", "friction", "corpus/interview_alice.txt", "--lines", "15:17"
        )

        stdout = project.cli_ok(
            "export", "document-html", "corpus/interview_alice.txt",
            "--output", "report/alice.html", "--title", "Alice Annotated"
        )
        report_path = project.root / stdout.strip()
        assert report_path.exists()
        html_content = report_path.read_text(encoding="utf-8")
        assert "Alice Annotated" in html_content
        assert "friction" in html_content
        assert "anno-segment" in html_content
