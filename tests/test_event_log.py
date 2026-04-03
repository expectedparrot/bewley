"""Tests for event log invariants: append-only, integrity, and ordering."""
from __future__ import annotations

import json
from pathlib import Path

from tests.conftest import BewleyProject


class TestEventAppendOnly:
    def test_events_monotonically_increasing(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        project.cli_ok("code", "create", "friction")
        project.cli_ok(
            "annotate", "apply", "trust", "corpus/interview_alice.txt", "--document"
        )

        events_dir = project.root / ".bewley" / "events"
        events = []
        for path in sorted(events_dir.glob("*.json")):
            events.append(json.loads(path.read_text(encoding="utf-8")))

        sequences = [e["sequence_number"] for e in events]
        assert sequences == sorted(sequences)
        assert len(sequences) == len(set(sequences)), "no duplicate sequence numbers"

    def test_every_event_has_required_fields(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")

        events_dir = project.root / ".bewley" / "events"
        required = {"event_id", "sequence_number", "event_type", "timestamp", "actor",
                     "tool_version", "payload", "event_sha256", "parent_event_ids"}
        for path in sorted(events_dir.glob("*.json")):
            event = json.loads(path.read_text(encoding="utf-8"))
            missing = required - set(event.keys())
            assert not missing, f"event {event.get('event_id', '?')} missing fields: {missing}"

    def test_event_chain_has_parent_pointers(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        project.cli_ok("code", "create", "friction")

        events_dir = project.root / ".bewley" / "events"
        events = []
        for path in sorted(events_dir.glob("*.json")):
            events.append(json.loads(path.read_text(encoding="utf-8")))

        # First event (project_initialized) has no parents
        assert events[0]["parent_event_ids"] == []

        # Subsequent events should reference the previous event
        for i in range(1, len(events)):
            assert len(events[i]["parent_event_ids"]) > 0
            assert events[i]["parent_event_ids"][0] == events[i - 1]["event_id"]

    def test_head_matches_last_event(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")

        head = (project.root / ".bewley" / "HEAD").read_text(encoding="utf-8").strip()
        events_dir = project.root / ".bewley" / "events"
        last_file = sorted(events_dir.glob("*.json"))[-1]
        last_event = json.loads(last_file.read_text(encoding="utf-8"))
        assert int(head) == last_event["sequence_number"]


class TestContentAddressing:
    def test_revision_object_is_content_addressed(self, empty_project: BewleyProject) -> None:
        """Revision objects are named by SHA-256 of content."""
        import hashlib
        content = "Hello, world.\n"
        expected_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

        empty_project.write_corpus("doc.txt", content)
        empty_project.cli_ok("add", "corpus/doc.txt")

        obj_path = empty_project.root / ".bewley" / "objects" / "documents" / expected_hash
        assert obj_path.exists()

    def test_identical_content_two_documents(self, empty_project: BewleyProject) -> None:
        """Two documents with identical content should both be trackable."""
        content = "Identical content for testing.\n"
        empty_project.write_corpus("doc_a.txt", content)
        empty_project.write_corpus("doc_b.txt", content)
        empty_project.cli_ok("add", "corpus/doc_a.txt")
        empty_project.cli_ok("add", "corpus/doc_b.txt")

        stdout = empty_project.cli_ok("list", "documents")
        assert "doc_a.txt" in stdout
        assert "doc_b.txt" in stdout

        # Only one object file despite two documents (content-addressed)
        objects_dir = empty_project.root / ".bewley" / "objects" / "documents"
        assert len(list(objects_dir.iterdir())) == 1

    def test_different_content_different_hash(self, empty_project: BewleyProject) -> None:
        empty_project.write_corpus("doc_a.txt", "Content A\n")
        empty_project.write_corpus("doc_b.txt", "Content B\n")
        empty_project.cli_ok("add", "corpus/doc_a.txt")
        empty_project.cli_ok("add", "corpus/doc_b.txt")

        objects_dir = empty_project.root / ".bewley" / "objects" / "documents"
        object_files = list(objects_dir.iterdir())
        assert len(object_files) == 2
