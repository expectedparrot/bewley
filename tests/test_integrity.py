"""Regression tests for rejected mutations, corrupt stores, and crash recovery."""

import hashlib
import json
import sqlite3

import pytest


def data(project, *args):
    rc, out, err = project.cli(*args, human=False)
    assert rc == 0, err or out
    return json.loads(out)["data"]


def test_conflicting_undo_never_enters_event_log(empty_project):
    p = empty_project
    data(p, "code", "create", "alpha")
    event = data(p, "code", "rename", "alpha", "beta")["event_id"]
    data(p, "code", "create", "alpha")
    before = {
        f.name: f.read_bytes() for f in (p.root / ".bewley/events").glob("*.json")
    }
    rc, out, _ = p.cli("undo", event, human=False)
    assert rc != 0
    assert json.loads(out)["errors"][0]["code"] == "INVALID_INPUT"
    assert before == {
        f.name: f.read_bytes() for f in (p.root / ".bewley/events").glob("*.json")
    }
    data(p, "rebuild-index")
    data(p, "fsck")


@pytest.mark.parametrize(
    "fault",
    ["object", "projection", "head", "parent", "case_projection", "invalid_json"],
)
def test_fsck_detects_corruption(project, fault):
    p = project
    if fault == "object":
        next((p.root / ".bewley/objects/documents").iterdir()).write_bytes(b"changed")
    elif fault in {"projection", "case_projection"}:
        if fault == "case_projection":
            data(p, "case", "create", "Case")
        with sqlite3.connect(p.root / ".bewley/index/bewley.sqlite") as conn:
            conn.execute(
                "UPDATE documents SET current_path='wrong'"
                if fault == "projection"
                else "UPDATE cases SET name='wrong'"
            )
    elif fault == "head":
        (p.root / ".bewley/HEAD").write_text("0\n")
    else:
        path = sorted((p.root / ".bewley/events").glob("*.json"))[-1]
        event = json.loads(path.read_text())
        event["parent_event_ids"] = ["wrong"]
        del event["event_sha256"]
        event["event_sha256"] = hashlib.sha256(
            json.dumps(event, ensure_ascii=False, sort_keys=True).encode()
        ).hexdigest()
        path.write_text("{" if fault == "invalid_json" else json.dumps(event))
    rc, out, _ = p.cli("fsck", human=False)
    assert rc != 0
    assert json.loads(out)["errors"][0]["code"] == "INTEGRITY_ERROR"


def test_interrupted_append_is_recoverable_without_rewriting_events(
    empty_project, monkeypatch
):
    import bewley.project as module

    p = empty_project
    original = module.atomic_write_text

    def fail_head(path, text):
        if path.name == "HEAD":
            raise OSError("simulated interruption after event publication")
        return original(path, text)

    monkeypatch.setattr(module, "atomic_write_text", fail_head)
    assert p.cli("code", "create", "durable", human=False)[0] != 0
    monkeypatch.setattr(module, "atomic_write_text", original)
    before = {
        f.name: f.read_bytes() for f in (p.root / ".bewley/events").glob("*.json")
    }
    assert p.cli("code", "create", "blocked", human=False)[0] != 0
    assert p.cli("rebuild-index", human=False)[0] != 0
    data(p, "rebuild-index", "--repair-head")
    for name, content in before.items():
        assert (p.root / ".bewley/events" / name).read_bytes() == content
    assert data(p, "code", "show", "durable")["name"] == "durable"
    data(p, "fsck")


def test_merge_back_into_its_family_is_rejected(empty_project):
    p = empty_project
    for name in ["alpha", "beta"]:
        data(p, "code", "create", name)
    data(p, "code", "merge", "alpha", "--into", "beta")
    assert p.cli("code", "merge", "beta", "--into", "alpha", human=False)[0] != 0
    data(p, "fsck")


def test_rebuild_refuses_corrupt_objects_and_keeps_index(project):
    before = (project.root / ".bewley/index/bewley.sqlite").read_bytes()
    next((project.root / ".bewley/objects/documents").iterdir()).write_bytes(b"changed")
    assert project.cli("rebuild-index", human=False)[0] != 0
    assert (project.root / ".bewley/index/bewley.sqlite").read_bytes() == before


def test_corrupt_raw_transcription_is_detected(empty_project):
    p = empty_project
    (p.root / "source.bin").write_bytes(b"source")
    (p.root / "raw.txt").write_bytes(b"raw output")
    source = data(p, "source", "add", "source.bin")
    raw = data(
        p,
        "source",
        "transcription",
        source["source_id"],
        "raw.txt",
        "--tool",
        "fixture",
    )
    (p.root / ".bewley/objects/transcriptions" / raw["sha256"]).write_bytes(b"changed")
    assert p.cli("fsck", human=False)[0] != 0
