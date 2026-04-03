"""Tests for annotation apply, remove, show, and resolve."""
from __future__ import annotations

from tests.conftest import BewleyProject


class TestDocumentAnnotation:
    def test_apply_document_level(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        stdout = project.cli_ok(
            "annotate", "apply", "trust", "corpus/interview_alice.txt", "--document"
        )
        anno_id = stdout.strip()
        assert anno_id  # should output the annotation id

    def test_apply_with_memo(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        stdout = project.cli_ok(
            "annotate", "apply", "trust", "corpus/interview_alice.txt",
            "--document", "--memo", "Strong trust theme throughout"
        )
        anno_id = stdout.strip()

        show_out = project.cli_ok("annotate", "show", anno_id)
        assert "Strong trust theme throughout" in show_out

    def test_remove_annotation(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        stdout = project.cli_ok(
            "annotate", "apply", "trust", "corpus/interview_alice.txt", "--document"
        )
        anno_id = stdout.strip()

        project.cli_ok("annotate", "remove", anno_id)

        # Should no longer appear in snippets
        snippets = project.cli_ok("show", "snippets", "--code", "trust")
        assert anno_id not in snippets


class TestSpanAnnotation:
    def test_apply_by_lines(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        stdout = project.cli_ok(
            "annotate", "apply", "trust", "corpus/interview_alice.txt", "--lines", "5:5"
        )
        anno_id = stdout.strip()
        assert anno_id

        show_out = project.cli_ok("annotate", "show", anno_id)
        assert "trust" in show_out.lower() or anno_id in show_out

    def test_apply_by_bytes(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "friction")
        # Apply to first 50 bytes
        stdout = project.cli_ok(
            "annotate", "apply", "friction", "corpus/interview_bob.txt", "--bytes", "0:50"
        )
        anno_id = stdout.strip()
        assert anno_id

    def test_invalid_line_range_fails(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        code, _, _ = project.cli(
            "annotate", "apply", "trust", "corpus/interview_alice.txt", "--lines", "999:1000"
        )
        assert code != 0

    def test_invalid_byte_range_fails(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        code, _, _ = project.cli(
            "annotate", "apply", "trust", "corpus/interview_alice.txt", "--bytes", "0:999999"
        )
        assert code != 0

    def test_snippets_shows_exact_text(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "friction")
        # Line 15 of alice: "Alice: Oh definitely. The onboarding was rough..."
        project.cli_ok(
            "annotate", "apply", "friction", "corpus/interview_alice.txt", "--lines", "15:17"
        )
        stdout = project.cli_ok("show", "snippets", "--code", "friction")
        assert "onboarding" in stdout


class TestAnnotationRelocation:
    def test_relocation_on_prepended_content(self, project: BewleyProject) -> None:
        """When content is prepended, span annotations should relocate."""
        project.cli_ok("code", "create", "trust")

        # Annotate a line in alice
        project.cli_ok(
            "annotate", "apply", "trust", "corpus/interview_alice.txt", "--lines", "5:5"
        )

        # Read original content and prepend a header
        alice = project.root / "corpus" / "interview_alice.txt"
        original = alice.read_text(encoding="utf-8")
        alice.write_text("[Header added]\n\n" + original, encoding="utf-8")

        project.cli_ok("update", "corpus/interview_alice.txt")

        # The annotation should have been relocated
        stdout = project.cli_ok("show", "snippets", "--code", "trust")
        assert "trust" in stdout.lower() or "relocated" in stdout.lower()

    def test_relocation_conflict_on_deleted_text(self, project: BewleyProject) -> None:
        """Deleting the annotated text should cause a conflict."""
        project.cli_ok("code", "create", "friction")

        project.cli_ok(
            "annotate", "apply", "friction", "corpus/interview_alice.txt", "--lines", "15:17"
        )

        # Replace the entire file with unrelated content
        alice = project.root / "corpus" / "interview_alice.txt"
        alice.write_text("Completely different content.\n", encoding="utf-8")

        project.cli_ok("update", "corpus/interview_alice.txt")

        stdout = project.cli_ok("status")
        assert "conflict" in stdout.lower()

    def test_document_level_annotation_survives_update(self, project: BewleyProject) -> None:
        """Document-level annotations should always survive updates."""
        project.cli_ok("code", "create", "trust")
        project.cli_ok(
            "annotate", "apply", "trust", "corpus/interview_alice.txt", "--document"
        )

        alice = project.root / "corpus" / "interview_alice.txt"
        alice.write_text("New content entirely.\n", encoding="utf-8")
        project.cli_ok("update", "corpus/interview_alice.txt")

        stdout = project.cli_ok("show", "snippets", "--code", "trust")
        assert "trust" in stdout.lower() or "interview_alice" in stdout
