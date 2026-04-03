"""Tests for history, undo, fsck, and rebuild-index."""
from __future__ import annotations

from tests.conftest import BewleyProject


class TestHistory:
    def test_full_history(self, project: BewleyProject) -> None:
        stdout = project.cli_ok("history")
        # Should have at least the init event plus document adds
        assert "project_initialized" in stdout
        assert "document_added" in stdout

    def test_history_filtered_by_document(self, project: BewleyProject) -> None:
        stdout = project.cli_ok("history", "--document", "corpus/interview_alice.txt")
        assert "document_added" in stdout
        # Should not contain events for other documents
        assert "interview_bob" not in stdout

    def test_history_filtered_by_code(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        project.cli_ok(
            "annotate", "apply", "trust", "corpus/interview_alice.txt", "--document"
        )

        stdout = project.cli_ok("history", "--code", "trust")
        assert "code_created" in stdout


class TestUndo:
    def test_undo_annotation(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        stdout = project.cli_ok(
            "annotate", "apply", "trust", "corpus/interview_alice.txt", "--document"
        )
        anno_id = stdout.strip()

        # Find the annotation_added event
        import json
        events_dir = project.root / ".bewley" / "events"
        event_id = None
        for event_file in sorted(events_dir.glob("*.json")):
            event = json.loads(event_file.read_text(encoding="utf-8"))
            if event["event_type"] == "annotation_added" and event["payload"]["annotation_id"] == anno_id:
                event_id = event["event_id"]
                break
        assert event_id is not None

        project.cli_ok("undo", event_id)

        # Annotation should be gone
        snippets = project.cli_ok("show", "snippets", "--code", "trust")
        assert anno_id not in snippets

    def test_undo_rename(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "workaround")
        project.cli_ok("code", "rename", "workaround", "coping")

        import json
        events_dir = project.root / ".bewley" / "events"
        event_id = None
        for event_file in sorted(events_dir.glob("*.json")):
            event = json.loads(event_file.read_text(encoding="utf-8"))
            if event["event_type"] == "code_renamed":
                event_id = event["event_id"]
                break
        assert event_id is not None

        project.cli_ok("undo", event_id)

        stdout = project.cli_ok("code", "list")
        assert "workaround" in stdout

    def test_undo_unsupported_type_fails(self, project: BewleyProject) -> None:
        import json
        events_dir = project.root / ".bewley" / "events"
        # The first event is project_initialized — undo is not supported
        first_event = json.loads(sorted(events_dir.glob("*.json"))[0].read_text(encoding="utf-8"))
        code, _, _ = project.cli("undo", first_event["event_id"])
        assert code != 0


class TestFsck:
    def test_clean_project_passes_fsck(self, project: BewleyProject) -> None:
        stdout = project.cli_ok("fsck")
        assert "ok" in stdout.lower()

    def test_fsck_after_mutations(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        project.cli_ok(
            "annotate", "apply", "trust", "corpus/interview_alice.txt", "--document"
        )
        project.cli_ok("code", "rename", "trust", "credibility")

        stdout = project.cli_ok("fsck")
        assert "ok" in stdout.lower()


class TestRebuildIndex:
    def test_rebuild_from_scratch(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        project.cli_ok(
            "annotate", "apply", "trust", "corpus/interview_alice.txt", "--lines", "5:5"
        )

        # Delete the SQLite index
        db_path = project.root / ".bewley" / "index" / "bewley.sqlite"
        db_path.unlink()

        project.cli_ok("rebuild-index")

        # Everything should still work
        stdout = project.cli_ok("show", "snippets", "--code", "trust")
        assert "trust" in stdout.lower() or "interview_alice" in stdout

        stdout = project.cli_ok("fsck")
        assert "ok" in stdout.lower()
