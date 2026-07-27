"""Tests for export commands: snippets, quotes, html, document-html."""
from __future__ import annotations

import json

from conftest import BewleyProject


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


class TestExportPlots:
    def test_writes_accessible_svg_plots_and_manifest(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        project.cli_ok("code", "create", "friction")
        project.cli_ok("annotate", "apply", "trust", "corpus/interview_alice.txt", "--lines", "5:5")
        project.cli_ok("annotate", "apply", "friction", "corpus/interview_alice.txt", "--lines", "15:17")

        stdout = project.cli_ok("export", "plots", "--output-dir", "report/plots")

        paths = [project.root / line for line in stdout.splitlines() if line]
        # No open-coding sidecar logs in this project, so the review funnel is skipped.
        assert len(paths) == 7
        names = {path.name for path in paths}
        assert names == {
            "code-prevalence.svg", "document-density.svg", "code-cooccurrence.svg",
            "code-document-matrix.svg", "code-discovery.svg",
            "annotation-positions.svg", "codebook-evolution.svg",
        }
        for path in paths:
            assert path.exists()
            svg = path.read_text(encoding="utf-8")
            assert "<svg" in svg
            assert "<title" in svg
            assert "<desc" in svg
        manifest = json.loads((project.root / "report/plots/plots.json").read_text())
        assert {row["canonical_name"] for row in manifest["codes"]} == {"friction", "trust"}
        assert all("coverage_share" in row for row in manifest["codes"])
        assert manifest["matrix"], "matrix cells should be present"
        assert manifest["annotation_positions"], "annotation positions should be present"
        assert any(event["type"] == "code_created" for event in manifest["events"])

    def test_review_funnel_drawn_from_sidecar_logs(self, project: BewleyProject) -> None:
        base = project.root / "qualitative-analysis"
        base.mkdir(exist_ok=True)
        (base / "ingest_log.jsonl").write_text(json.dumps({"candidates": [
            {"candidate_id": "a", "code_name": "trust"},
            {"candidate_id": "b", "code_name": "noise"},
        ]}) + "\n", encoding="utf-8")
        (base / "apply_log.jsonl").write_text(json.dumps({"applied": [
            {"candidate_id": "a", "code_name": "trust"},
        ]}) + "\n", encoding="utf-8")

        stdout = project.cli_ok("export", "plots", "--output-dir", "report/plots")

        paths = [project.root / line for line in stdout.splitlines() if line]
        assert any(path.name == "review-funnel.svg" for path in paths)
        manifest = json.loads((project.root / "report/plots/plots.json").read_text())
        assert manifest["review"]["proposed"] == {"trust": 1, "noise": 1}
        assert manifest["review"]["applied"] == {"trust": 1}

    def test_cooccurrence_is_span_level(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        project.cli_ok("code", "create", "friction")
        project.cli_ok("code", "create", "distant")
        # trust and friction within 5 lines; distant is far away in the same doc
        project.cli_ok("annotate", "apply", "trust", "corpus/interview_alice.txt", "--lines", "5:5")
        project.cli_ok("annotate", "apply", "friction", "corpus/interview_alice.txt", "--lines", "9:10")
        project.cli_ok("annotate", "apply", "distant", "corpus/interview_alice.txt", "--lines", "30:30")

        project.cli_ok("export", "plots", "--output-dir", "report/plots")

        manifest = json.loads((project.root / "report/plots/plots.json").read_text())
        names = {row["code_id"]: row["canonical_name"] for row in manifest["codes"]}
        pairs = {
            tuple(sorted((names[row["left_id"]], names[row["right_id"]]))): row["pairs"]
            for row in manifest["cooccurrence"]
        }
        assert pairs.get(("friction", "trust")) == 1
        assert ("distant", "trust") not in pairs
        assert ("distant", "friction") not in pairs

    def test_coverage_share_uses_participant_denominator(self, empty_project: BewleyProject) -> None:
        transcript = (
            "INTERVIEWER: A question that takes up half of all the bytes here?\n\n"
            "NARRATOR: A short answer that we code fully, every single byte of it.\n"
        )
        path = empty_project.root / "corpus" / "talk.txt"
        path.parent.mkdir(exist_ok=True)
        path.write_text(transcript, encoding="utf-8")
        empty_project.cli_ok("add", "corpus/talk.txt")
        empty_project.cli_ok("speakers", "detect", "corpus/talk.txt")
        empty_project.cli_ok("speakers", "set-role", "INTERVIEWER", "interviewer")
        empty_project.cli_ok("speakers", "set-role", "NARRATOR", "participant")
        empty_project.cli_ok("code", "create", "answering")
        empty_project.cli_ok("annotate", "apply", "answering", "corpus/talk.txt", "--turn", "2")

        empty_project.cli_ok("export", "plots", "--output-dir", "report/plots")

        manifest = json.loads((empty_project.root / "report/plots/plots.json").read_text())
        share = manifest["codes"][0]["coverage_share"]
        document = manifest["documents"][0]
        # denominator excludes interviewer bytes, so effective < full length
        assert document["effective_bytes"] < document["byte_length"]
        # the coded turn covers nearly all participant text
        assert share > 0.8
        assert manifest["interviewer_spans"]

    def test_merged_code_annotations_resolve_in_matrix(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        project.cli_ok("code", "create", "confidence")
        project.cli_ok("annotate", "apply", "confidence", "corpus/interview_alice.txt", "--lines", "5:5")
        project.cli_ok("code", "merge", "confidence", "--into", "trust")

        project.cli_ok("export", "plots", "--output-dir", "report/plots")

        manifest = json.loads((project.root / "report/plots/plots.json").read_text())
        trust_id = next(row["code_id"] for row in manifest["codes"] if row["canonical_name"] == "trust")
        assert all(cell["code_id"] == trust_id for cell in manifest["matrix"])
        assert all(row["code_id"] == trust_id for row in manifest["annotation_positions"])
        assert any(event["type"] == "code_merged" for event in manifest["events"])
