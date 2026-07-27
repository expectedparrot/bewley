"""The version the CLI reports must match the version the package declares.

Guards against stale installed metadata leaking into envelopes and docs:
the tutorial once embedded 0.3.0 from an outdated editable install while
pyproject.toml said 0.4.0.
"""
from __future__ import annotations

import tomllib
from pathlib import Path

import bewley


def test_reported_version_matches_pyproject() -> None:
    pyproject = tomllib.loads(
        (Path(__file__).resolve().parents[1] / "pyproject.toml").read_text(encoding="utf-8")
    )
    assert bewley.__version__ == pyproject["project"]["version"], (
        "installed metadata is stale — refresh with `pip install -e .`"
    )
