from __future__ import annotations

import contextlib
import io
import os
import shutil
import tempfile
from pathlib import Path

import pytest

from bewley.cli import main

CORPUS_DIR = Path(__file__).parent / "corpus"


class BewleyProject:
    """Helper that wraps a bewley project in a temp directory."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def cli(self, *args: str, human: bool = True) -> tuple[int, str, str]:
        old_cwd = Path.cwd()
        stdout = io.StringIO()
        stderr = io.StringIO()
        cli_args = list(args)
        if human:
            cli_args = ["-H"] + cli_args
        try:
            os.chdir(self.root)
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                code = main(cli_args)
        finally:
            os.chdir(old_cwd)
        return code, stdout.getvalue(), stderr.getvalue()

    def cli_ok(self, *args: str) -> str:
        """Run a CLI command, assert exit 0, return stdout."""
        code, stdout, stderr = self.cli(*args)
        assert code == 0, f"bewley {' '.join(args)} failed (exit {code}): {stderr}"
        return stdout

    def write_corpus(self, name: str, content: str) -> Path:
        """Write a file into corpus/ and return its project-relative path."""
        path = self.root / "corpus" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return Path("corpus") / name


@pytest.fixture
def project(tmp_path: Path) -> BewleyProject:
    """Create an initialized bewley project with the test corpus loaded."""
    proj = BewleyProject(tmp_path)
    proj.cli_ok("init")

    # Copy test corpus files into the project
    corpus_dest = tmp_path / "corpus"
    for src_file in sorted(CORPUS_DIR.glob("*.txt")):
        shutil.copy2(src_file, corpus_dest / src_file.name)

    # Track all documents
    for txt in sorted(corpus_dest.glob("*.txt")):
        proj.cli_ok("add", f"corpus/{txt.name}")

    return proj


@pytest.fixture
def empty_project(tmp_path: Path) -> BewleyProject:
    """Create an initialized bewley project with no documents."""
    proj = BewleyProject(tmp_path)
    proj.cli_ok("init")
    return proj
