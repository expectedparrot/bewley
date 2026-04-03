"""Tests for document add, update, status, and listing."""
from __future__ import annotations

from pathlib import Path

from tests.conftest import BewleyProject


class TestDocumentAdd:
    def test_add_tracks_documents(self, project: BewleyProject) -> None:
        stdout = project.cli_ok("list", "documents")
        assert "interview_alice.txt" in stdout
        assert "interview_bob.txt" in stdout
        assert "interview_carol.txt" in stdout

    def test_add_duplicate_path_fails(self, project: BewleyProject) -> None:
        code, _, stderr = project.cli("add", "corpus/interview_alice.txt")
        assert code != 0

    def test_add_missing_file_fails(self, empty_project: BewleyProject) -> None:
        code, _, stderr = empty_project.cli("add", "corpus/nonexistent.txt")
        assert code != 0

    def test_add_creates_revision_object(self, project: BewleyProject) -> None:
        objects_dir = project.root / ".bewley" / "objects" / "documents"
        assert any(objects_dir.iterdir()), "should have at least one revision object"


class TestDocumentUpdate:
    def test_update_unchanged_is_noop(self, project: BewleyProject) -> None:
        stdout = project.cli_ok("update", "corpus/interview_alice.txt")
        assert "no-op" in stdout.lower() or "unchanged" in stdout.lower() or stdout.strip() == ""

    def test_update_creates_new_revision(self, project: BewleyProject) -> None:
        alice = project.root / "corpus" / "interview_alice.txt"
        original = alice.read_text(encoding="utf-8")
        alice.write_text(original + "\n[End of interview]\n", encoding="utf-8")

        project.cli_ok("update", "corpus/interview_alice.txt")

        stdout = project.cli_ok("show", "document", "corpus/interview_alice.txt")
        # The output lists revisions as tab-separated lines after "revisions" header.
        # Two revision hashes should appear (one per line).
        lines = [l for l in stdout.strip().splitlines() if l and not l.startswith(("document_id", "path", "revisions", "annotations"))]
        assert len(lines) >= 2, f"expected at least 2 revision lines, got: {lines}"

    def test_update_preserves_old_revision_object(self, project: BewleyProject) -> None:
        objects_dir = project.root / ".bewley" / "objects" / "documents"
        objects_before = set(f.name for f in objects_dir.iterdir())

        alice = project.root / "corpus" / "interview_alice.txt"
        alice.write_text("completely new content\n", encoding="utf-8")
        project.cli_ok("update", "corpus/interview_alice.txt")

        objects_after = set(f.name for f in objects_dir.iterdir())
        assert objects_before.issubset(objects_after), "old revision objects should not be deleted"
        assert len(objects_after) > len(objects_before), "should have a new revision object"


class TestStatus:
    def test_status_clean_project(self, project: BewleyProject) -> None:
        stdout = project.cli_ok("status")
        assert "interview_alice.txt" in stdout or "document" in stdout.lower()

    def test_status_shows_document_count(self, project: BewleyProject) -> None:
        stdout = project.cli_ok("status")
        # Should indicate 3 documents somewhere
        assert "3" in stdout


class TestShowDocument:
    def test_show_by_path(self, project: BewleyProject) -> None:
        stdout = project.cli_ok("show", "document", "corpus/interview_alice.txt")
        assert "alice" in stdout.lower() or "interview_alice" in stdout

    def test_show_unknown_document_fails(self, project: BewleyProject) -> None:
        code, _, _ = project.cli("show", "document", "corpus/nonexistent.txt")
        assert code != 0
