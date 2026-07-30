from __future__ import annotations

import json
import zipfile
from pathlib import Path

from conftest import BewleyProject

from bewley.bundle import unpack_project


def test_pack_and_unpack_round_trip(project: BewleyProject, tmp_path: Path) -> None:
    project.cli_ok("code", "create", "trust", "--description", "Evidence of trust")
    project.cli_ok("annotate", "apply", "trust", "corpus/interview_alice.txt", "--lines", "5:5")
    bundle = tmp_path / "study.bewley"

    output = project.cli_ok("project", "pack", "--output", str(bundle))
    assert Path(output.strip()) == bundle
    assert bundle.is_file()

    restored = tmp_path / "restored"
    output = project.cli_ok("project", "unpack", str(bundle), "--dest", str(restored))
    assert Path(output.strip()) == restored
    restored_project = BewleyProject(restored)
    code, stdout, stderr = restored_project.cli("code", "list", human=False)
    assert code == 0, stderr
    codes = json.loads(stdout)
    assert codes["status"] == "ok"
    assert any(row["canonical_name"] == "trust" for row in codes["data"])
    assert (restored / "corpus/interview_alice.txt").is_file()


def test_unpack_rejects_tampered_member(project: BewleyProject, tmp_path: Path) -> None:
    bundle = tmp_path / "study.bewley"
    project.cli_ok("project", "pack", "--output", str(bundle))
    tampered = tmp_path / "tampered.bewley"
    with zipfile.ZipFile(bundle) as source, zipfile.ZipFile(tampered, "w") as target:
        for name in source.namelist():
            data = source.read(name)
            if name == ".bewley/config.toml":
                data += b"\n# tampered\n"
            target.writestr(name, data)
    try:
        unpack_project(tampered, tmp_path / "bad")
    except Exception as exc:
        assert getattr(exc, "code", "") == "BUNDLE_HASH_MISMATCH"
    else:
        raise AssertionError("tampered bundle was accepted")


def test_unpack_rejects_existing_destination(project: BewleyProject, tmp_path: Path) -> None:
    bundle = tmp_path / "study.bewley"
    project.cli_ok("project", "pack", "--output", str(bundle))
    existing = tmp_path / "existing"
    existing.mkdir()
    try:
        unpack_project(bundle, existing)
    except Exception as exc:
        assert getattr(exc, "code", "") == "ALREADY_EXISTS"
    else:
        raise AssertionError("existing destination was accepted")
