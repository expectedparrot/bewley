"""Tests for code create, list, rename, alias, merge, and split."""
from __future__ import annotations

from conftest import BewleyProject


class TestCodeCreate:
    def test_create_and_list(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        project.cli_ok("code", "create", "friction")
        project.cli_ok("code", "create", "workaround")

        stdout = project.cli_ok("code", "list")
        assert "trust" in stdout
        assert "friction" in stdout
        assert "workaround" in stdout

    def test_duplicate_name_fails(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        code, _, _ = project.cli("code", "create", "trust")
        assert code != 0

    def test_show_code(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        stdout = project.cli_ok("code", "show", "trust")
        assert "trust" in stdout


class TestCodeRename:
    def test_rename(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "workaround")
        project.cli_ok("code", "rename", "workaround", "coping-strategy")

        stdout = project.cli_ok("code", "list")
        assert "coping-strategy" in stdout
        assert "workaround" not in stdout

    def test_rename_to_existing_fails(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        project.cli_ok("code", "create", "friction")
        code, _, _ = project.cli("code", "rename", "trust", "friction")
        assert code != 0


class TestCodeAlias:
    def test_alias_resolves(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "coping-strategy")
        project.cli_ok("code", "alias", "coping-strategy", "workaround")

        # Should be able to resolve the code by alias
        stdout = project.cli_ok("code", "show", "workaround")
        assert "coping-strategy" in stdout

    def test_alias_duplicate_fails(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        project.cli_ok("code", "create", "friction")
        # Can't alias to an existing canonical name
        code, _, _ = project.cli("code", "alias", "trust", "friction")
        assert code != 0


class TestCodeMerge:
    def test_merge_codes(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        project.cli_ok("code", "create", "reliability")
        project.cli_ok("code", "create", "credibility")

        project.cli_ok("code", "merge", "trust", "reliability", "--into", "credibility")

        stdout = project.cli_ok("code", "list")
        assert "credibility" in stdout

    def test_merge_self_fails(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        code, _, _ = project.cli("code", "merge", "trust", "--into", "trust")
        assert code != 0


class TestCodeSplit:
    def test_split_reassigns_annotations(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "friction")

        # Annotate two documents with friction
        stdout1 = project.cli_ok(
            "annotate", "apply", "friction", "corpus/interview_alice.txt", "--document"
        )
        anno_id1 = stdout1.strip()

        stdout2 = project.cli_ok(
            "annotate", "apply", "friction", "corpus/interview_bob.txt", "--document"
        )
        anno_id2 = stdout2.strip()

        # Split one annotation into a new code
        project.cli_ok(
            "code", "split", "friction", "--new", "onboarding-friction",
            "--annotation", anno_id1,
        )

        stdout = project.cli_ok("code", "list")
        assert "onboarding-friction" in stdout
