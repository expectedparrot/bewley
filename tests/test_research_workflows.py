import json

from test_open_coding_jobs import _json


def test_released_codebook_coding_uses_stable_ids_after_rename(empty_project):
    from edsl import Jobs, Results, Survey
    from test_open_coding_jobs import _result_for

    p = empty_project
    _json(p, "code", "create", "trust", "--description", "Explicit trust.")
    _json(p, "codebook", "release", "first")
    _json(p, "code", "rename", "trust", "confidence")
    p.write_corpus("new.txt", "I trust the result.\n")
    _json(p, "add", "corpus/new.txt")
    _json(
        p, "open-coding", "jobs", "--codebook", "first", "--document", "corpus/new.txt"
    )
    jobs = Jobs.git.load(p.root / "jobs.ep")
    scenario = dict(jobs.scenarios[0])
    released = json.loads(scenario["codebook_json"])
    assert released[0]["canonical_name"] == "trust"
    row = _result_for(scenario, "I trust the result.")
    row["answer"]["open_coding"] = json.dumps(
        [
            {
                "code": released[0]["code_id"],
                "description": "Explicit trust.",
                "quote": "I trust the result.",
            }
        ]
    )
    Results(survey=Survey([]), data=[row]).git.save(p.root / "results.ep")
    _json(p, "open-coding", "ingest", "results.ep", "--jobs", "jobs.ep")
    _json(p, "open-coding", "review", "--all-remaining", "--decision", "accept")
    _json(p, "code", "rename", "confidence", "reliance")
    result = _json(p, "open-coding", "apply")
    assert result["annotations_applied"] == 1
    assert result["codes_to_create"] == []
    quotes = _json(p, "export", "quotes", "--all")
    assert quotes[0]["code_name"] == "reliance"
    _json(p, "fsck")


def test_case_filters_and_matrix_respect_speaker_attribution(empty_project):
    p = empty_project
    p.write_corpus("group.txt", "ALICE: Helpful service.\nBOB: Slow service.\n")
    _json(p, "add", "corpus/group.txt")
    _json(p, "speakers", "detect", "corpus/group.txt")
    for label in ["ALICE", "BOB"]:
        _json(p, "speakers", "set-role", label, "participant")
        _json(p, "case", "create", label)
        _json(p, "case", "link", label, "corpus/group.txt", "--as", "participant")
        _json(p, "speakers", "link-case", "corpus/group.txt", label, label)
    _json(p, "attribute", "define", "site", "--type", "text")
    _json(p, "case", "set", "ALICE", "site", "north")
    _json(p, "case", "set", "BOB", "site", "south")
    _json(p, "code", "create", "positive")
    _json(
        p,
        "annotate",
        "apply",
        "positive",
        "corpus/group.txt",
        "--quote",
        "Helpful service.",
    )
    assert len(_json(p, "query", "positive", "--case", "ALICE")) == 1
    assert _json(p, "query", "positive", "--case", "BOB") == []
    assert len(_json(p, "query", "NOT positive", "--speaker", "BOB")) == 1
    assert _json(p, "query", "NOT positive", "--speaker", "ALICE") == []
    assert _json(p, "query", "positive", "--attribute", "site=south") == []
    assert (
        len(_json(p, "query", "positive", "--speaker", "ALICE", "--mode", "annotation"))
        == 1
    )
    matrix = _json(p, "export", "matrix")
    by_name = {c["name"]: c["case_id"] for c in matrix["cases"]}
    counts = {cell["case_id"]: cell for cell in matrix["cells"]}
    assert counts[by_name["ALICE"]]["document_share"] == 1
    assert counts[by_name["BOB"]]["document_share"] == 0
    assert all(cell["eligible_document_count"] == 1 for cell in matrix["cells"])


def test_source_lineage_roundtrip_preserves_raw_and_metadata(empty_project, tmp_path):
    p = empty_project
    original = b"%PDF-synthetic source bytes"
    (p.root / "scan.pdf").write_bytes(original)
    raw = b'{"ocr":"A letter with spacing.  "}\n'
    (p.root / "ocr.json").write_bytes(raw)
    source = _json(p, "source", "add", "scan.pdf", "--locator", "archive:item-1")
    transcription = _json(
        p,
        "source",
        "transcription",
        source["source_id"],
        "ocr.json",
        "--tool",
        "fixture-ocr",
        "--tool-version",
        "1",
    )
    p.write_corpus("letter.txt", "A letter with spacing.\n")
    derived = _json(
        p,
        "source",
        "derive",
        transcription["transcription_id"],
        "corpus/letter.txt",
        "--unit",
        "letter",
        "--transformation",
        "Extracted OCR text and normalized trailing spaces.",
    )
    _json(
        p,
        "source",
        "metadata",
        "corpus/letter.txt",
        "author",
        "--original",
        "A. Writer",
        "--normalized",
        "Alice Writer",
        "--provenance",
        "text",
        "--status",
        "accepted",
    )
    assert (p.root / "corpus/letter.txt").read_text() == "A letter with spacing.\n"
    _json(p, "code", "create", "letter")
    _json(p, "annotate", "apply", "letter", "corpus/letter.txt", "--quote", "A letter")
    quote = _json(p, "export", "quotes", "--all")[0]
    assert quote["source_lineage"]["lineage"][0]["source_id"] == source["source_id"]
    assert quote["source_lineage"]["metadata"][0]["normalized"] == "Alice Writer"
    _json(
        p,
        "source",
        "export-raw",
        transcription["transcription_id"],
        "--output",
        str(tmp_path / "retrieved.json"),
    )
    assert (tmp_path / "retrieved.json").read_bytes() == raw
    _json(p, "project", "pack", "--output", "study.bewley")
    _json(p, "project", "unpack", "study.bewley", "--dest", str(tmp_path / "restored"))
    from conftest import BewleyProject

    restored = BewleyProject(tmp_path / "restored")
    lineage = _json(restored, "source", "show", "corpus/letter.txt")
    assert lineage["lineage"][0]["transformation"] == derived["transformation"]
    assert (
        tmp_path / "restored/.bewley/objects/sources" / source["sha256"]
    ).read_bytes() == original
    _json(restored, "fsck")


def test_non_grounded_theory_guidance_and_executable_next(empty_project):
    p = empty_project
    rc, out, _ = p.cli("next", human=False)
    action = json.loads(out)["next_steps"][0]
    assert action["command"] == ["bewley", "study", "set", "--help"]
    assert action["mutates_state"] is False
    p.write_corpus("doc.txt", "Evidence.\n")
    for args in [
        ("add", "corpus/doc.txt"),
        ("code", "create", "evidence"),
        ("annotate", "apply", "evidence", "corpus/doc.txt", "--document"),
        ("study", "set", "--method", "thematic-analysis"),
    ]:
        _json(p, *args)
    state = _json(p, "next")
    assert state["primary_doc"] == "workflow"
    assert "core category" not in " ".join(state["checklist"])


def test_uncertain_boundaries_block_coding_until_reviewed(empty_project):
    p = empty_project
    (p.root / "source.bin").write_bytes(b"source")
    (p.root / "raw.txt").write_text("Letter one. Letter two.")
    source = _json(p, "source", "add", "source.bin")
    raw = _json(
        p,
        "source",
        "transcription",
        source["source_id"],
        "raw.txt",
        "--tool",
        "fixture",
    )
    p.write_corpus("one.txt", "Letter one.")
    _json(
        p,
        "source",
        "derive",
        raw["transcription_id"],
        "corpus/one.txt",
        "--raw-bytes",
        "0:11",
        "--unit",
        "letter",
        "--transformation",
        "Split letters.",
        "--boundary-status",
        "needs-review",
    )
    rc, out, _ = p.cli("open-coding", "jobs", human=False)
    assert rc != 0
    assert json.loads(out)["errors"][0]["code"] == "UNREVIEWED_BOUNDARY"
    _json(
        p,
        "source",
        "boundary",
        "corpus/one.txt",
        "--status",
        "confirmed",
        "--reason",
        "Checked against the scan.",
    )
    _json(p, "open-coding", "jobs")
    _json(p, "fsck")
