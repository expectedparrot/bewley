"""Tests for the bundled-example commands (`bewley example list|fetch`)."""
from __future__ import annotations

import json
from pathlib import Path

from conftest import BewleyProject


def _envelope(proj: BewleyProject, *args: str) -> tuple[int, dict]:
    code, stdout, stderr = proj.cli(*args, human=False)
    return code, json.loads(stdout)


class TestExampleList:
    def test_lists_adams_letters(self, tmp_path: Path) -> None:
        proj = BewleyProject(tmp_path)
        code, env = _envelope(proj, "example", "list")
        assert code == 0
        rows = {row["name"]: row for row in env["data"]["examples"]}
        assert rows["adams-letters"]["documents"] == 20
        assert rows["adams-letters"]["description"]

    def test_human_mode_renders_table(self, tmp_path: Path) -> None:
        proj = BewleyProject(tmp_path)
        stdout = proj.cli_ok("example", "list")
        assert "adams-letters" in stdout


class TestExampleFetch:
    def test_writes_corpus_readme_and_license(self, tmp_path: Path) -> None:
        proj = BewleyProject(tmp_path)
        code, env = _envelope(proj, "example", "fetch", "adams-letters")
        assert code == 0
        assert env["data"]["documents"] == 20
        dest = tmp_path / "adams-letters"
        assert (dest / "README.md").is_file()
        assert (dest / "PROJECT_GUTENBERG_LICENSE.txt").is_file()
        assert len(list((dest / "corpus").glob("*.txt"))) == 20
        assert not (dest / ".bewley").exists()

    def test_honors_dest_option(self, tmp_path: Path) -> None:
        proj = BewleyProject(tmp_path)
        code, env = _envelope(proj, "example", "fetch", "adams-letters", "--dest", "letters")
        assert code == 0
        assert env["data"]["dest"] == "letters"
        assert (tmp_path / "letters" / "corpus").is_dir()

    def test_refuses_existing_destination(self, tmp_path: Path) -> None:
        (tmp_path / "adams-letters").mkdir()
        proj = BewleyProject(tmp_path)
        code, env = _envelope(proj, "example", "fetch", "adams-letters")
        assert code != 0
        assert env["errors"][0]["code"] == "DESTINATION_EXISTS"

    def test_unknown_example_lists_available(self, tmp_path: Path) -> None:
        proj = BewleyProject(tmp_path)
        code, env = _envelope(proj, "example", "fetch", "nope")
        assert code != 0
        error = env["errors"][0]
        assert error["code"] == "UNKNOWN_EXAMPLE"
        assert "adams-letters" in error["context"]["available"]

    def test_fetched_corpus_is_addable(self, tmp_path: Path) -> None:
        proj = BewleyProject(tmp_path)
        proj.cli_ok("example", "fetch", "adams-letters", "--dest", "work")
        proj.cli_ok("init")
        proj.cli_ok("add", "work/corpus/1775-april-30-john-adams.txt")
