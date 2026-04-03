"""Tests for selective coding features: memos, hierarchies, links, core category, exports."""
from __future__ import annotations

from conftest import BewleyProject


class TestMemos:
    def test_create_project_memo(self, project: BewleyProject) -> None:
        out = project.cli_ok("memo", "add", "This is a project memo")
        memo_id = out.strip()
        assert len(memo_id) == 32

    def test_create_code_memo(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        out = project.cli_ok("memo", "add", "--code", "trust", "Trust is important")
        memo_id = out.strip()
        assert len(memo_id) == 32

    def test_create_document_memo(self, project: BewleyProject) -> None:
        out = project.cli_ok("memo", "add", "--document", "corpus/interview_alice.txt", "Interesting interview")
        memo_id = out.strip()
        assert len(memo_id) == 32

    def test_memo_list_and_show(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        out1 = project.cli_ok("memo", "add", "Project note")
        out2 = project.cli_ok("memo", "add", "--code", "trust", "Code note")
        memo1 = out1.strip()
        memo2 = out2.strip()

        # List all
        listing = project.cli_ok("memo", "list")
        assert memo1 in listing
        assert memo2 in listing

        # List filtered by code
        code_listing = project.cli_ok("memo", "list", "--code", "trust")
        assert memo2 in code_listing
        assert memo1 not in code_listing

        # Show
        show_out = project.cli_ok("memo", "show", memo1)
        assert "Project note" in show_out

    def test_memo_delete(self, project: BewleyProject) -> None:
        memo_id = project.cli_ok("memo", "add", "To be deleted").strip()
        project.cli_ok("memo", "delete", memo_id)
        listing = project.cli_ok("memo", "list")
        assert memo_id not in listing

    def test_memo_with_title(self, project: BewleyProject) -> None:
        memo_id = project.cli_ok("memo", "add", "--title", "Key insight", "Something important").strip()
        show_out = project.cli_ok("memo", "show", memo_id)
        assert "Key insight" in show_out

    def test_memo_undo_create(self, project: BewleyProject) -> None:
        out = project.cli_ok("memo", "add", "Will undo this")
        memo_id = out.strip()
        # Find the event id from history — columns: seq, timestamp, event_type, event_id
        history = project.cli_ok("history")
        lines = [l for l in history.strip().split("\n") if "memo_created" in l]
        assert lines
        event_id = lines[-1].split("\t")[3]
        project.cli_ok("undo", event_id)
        listing = project.cli_ok("memo", "list")
        assert memo_id not in listing


class TestCodeHierarchies:
    def test_set_parent(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        project.cli_ok("code", "create", "institutional-trust")
        project.cli_ok("code", "set-parent", "institutional-trust", "trust")

        show = project.cli_ok("code", "show", "institutional-trust")
        assert "trust" in show

    def test_clear_parent(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        project.cli_ok("code", "create", "institutional-trust")
        project.cli_ok("code", "set-parent", "institutional-trust", "trust")
        project.cli_ok("code", "clear-parent", "institutional-trust")

        show = project.cli_ok("code", "show", "institutional-trust")
        assert "parent" not in show.lower() or "trust" not in show.split("parent")[-1] if "parent" in show.lower() else True

    def test_tree_display(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        project.cli_ok("code", "create", "institutional-trust")
        project.cli_ok("code", "create", "peer-trust")
        project.cli_ok("code", "set-parent", "institutional-trust", "trust")
        project.cli_ok("code", "set-parent", "peer-trust", "trust")

        tree = project.cli_ok("code", "list", "--tree")
        assert "trust" in tree
        assert "institutional-trust" in tree
        assert "peer-trust" in tree

    def test_cycle_detection(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "a")
        project.cli_ok("code", "create", "b")
        project.cli_ok("code", "set-parent", "b", "a")
        code, _, stderr = project.cli("code", "set-parent", "a", "b")
        assert code != 0
        assert "cycle" in stderr.lower()

    def test_self_parent_rejected(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "a")
        code, _, stderr = project.cli("code", "set-parent", "a", "a")
        assert code != 0

    def test_merge_reparents_children(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        project.cli_ok("code", "create", "confidence")
        project.cli_ok("code", "create", "self-trust")
        project.cli_ok("code", "set-parent", "self-trust", "confidence")
        project.cli_ok("code", "merge", "confidence", "--into", "trust")

        show = project.cli_ok("code", "show", "self-trust")
        assert "trust" in show

    def test_undo_parent_set(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        project.cli_ok("code", "create", "child")
        project.cli_ok("code", "set-parent", "child", "trust")

        history = project.cli_ok("history")
        lines = [l for l in history.strip().split("\n") if "code_parent_set" in l]
        event_id = lines[-1].split("\t")[3]
        project.cli_ok("undo", event_id)

        show = project.cli_ok("code", "show", "child")
        # parent should be cleared after undo
        assert "parent" not in show.lower() or "trust" not in show


class TestCodeLinks:
    def test_create_link(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "stress")
        project.cli_ok("code", "create", "burnout")
        out = project.cli_ok("code", "link", "stress", "burnout", "causes")
        link_id = out.strip()
        assert len(link_id) == 32

    def test_list_links(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "stress")
        project.cli_ok("code", "create", "burnout")
        project.cli_ok("code", "link", "stress", "burnout", "causes")

        links = project.cli_ok("code", "links")
        assert "stress" in links
        assert "burnout" in links
        assert "causes" in links

    def test_list_links_filtered(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "stress")
        project.cli_ok("code", "create", "burnout")
        project.cli_ok("code", "create", "unrelated")
        project.cli_ok("code", "link", "stress", "burnout", "causes")

        links = project.cli_ok("code", "links", "unrelated")
        assert "no links" in links.lower()

    def test_unlink(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "stress")
        project.cli_ok("code", "create", "burnout")
        link_id = project.cli_ok("code", "link", "stress", "burnout", "causes").strip()
        project.cli_ok("code", "unlink", link_id)
        links = project.cli_ok("code", "links")
        assert "no links" in links.lower()

    def test_duplicate_link_rejected(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "stress")
        project.cli_ok("code", "create", "burnout")
        project.cli_ok("code", "link", "stress", "burnout", "causes")
        code, _, stderr = project.cli("code", "link", "stress", "burnout", "causes")
        assert code != 0
        assert "duplicate" in stderr.lower()

    def test_different_relationship_allowed(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "stress")
        project.cli_ok("code", "create", "burnout")
        project.cli_ok("code", "link", "stress", "burnout", "causes")
        project.cli_ok("code", "link", "stress", "burnout", "intensifies")

        links = project.cli_ok("code", "links")
        assert "causes" in links
        assert "intensifies" in links

    def test_link_with_memo(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "stress")
        project.cli_ok("code", "create", "burnout")
        project.cli_ok("code", "link", "stress", "burnout", "causes", "--memo", "Strong causal link")
        links = project.cli_ok("code", "links")
        assert "Strong causal link" in links

    def test_link_shown_in_code_show(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "stress")
        project.cli_ok("code", "create", "burnout")
        project.cli_ok("code", "link", "stress", "burnout", "causes")
        show = project.cli_ok("code", "show", "stress")
        assert "causes" in show


class TestCoreCategory:
    def test_set_and_show_core(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "burnout")
        project.cli_ok("code", "set-core", "burnout")
        out = project.cli_ok("code", "show-core")
        assert "burnout" in out

    def test_show_core_when_unset(self, project: BewleyProject) -> None:
        out = project.cli_ok("code", "show-core")
        assert "no core" in out.lower()

    def test_change_core(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "stress")
        project.cli_ok("code", "create", "burnout")
        project.cli_ok("code", "set-core", "stress")
        project.cli_ok("code", "set-core", "burnout")
        out = project.cli_ok("code", "show-core")
        assert "burnout" in out

    def test_undo_core_category(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "burnout")
        project.cli_ok("code", "set-core", "burnout")

        history = project.cli_ok("history")
        lines = [l for l in history.strip().split("\n") if "core_category_set" in l]
        event_id = lines[-1].split("\t")[3]
        project.cli_ok("undo", event_id)

        out = project.cli_ok("code", "show-core")
        assert "no core" in out.lower()


class TestExportTheory:
    def _setup_theory(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "burnout", "--description", "Central phenomenon")
        project.cli_ok("code", "create", "long-hours")
        project.cli_ok("code", "create", "lack-of-support")
        project.cli_ok("code", "set-parent", "long-hours", "burnout")
        project.cli_ok("code", "set-parent", "lack-of-support", "burnout")
        project.cli_ok("code", "link", "long-hours", "burnout", "causes")
        project.cli_ok("code", "link", "lack-of-support", "burnout", "intensifies")
        project.cli_ok("code", "set-core", "burnout")
        project.cli_ok("memo", "add", "The core narrative centers on burnout")

    def test_export_mermaid(self, project: BewleyProject) -> None:
        self._setup_theory(project)
        out = project.cli_ok("export", "theory", "--format", "mermaid")
        assert "graph TD" in out
        assert "burnout" in out
        assert "causes" in out
        assert "core" in out

    def test_export_json(self, project: BewleyProject) -> None:
        import json
        self._setup_theory(project)
        out = project.cli_ok("export", "theory", "--format", "json")
        data = json.loads(out)
        assert data["core_category"]["name"] == "burnout"
        assert len(data["codes"]) == 3
        assert len(data["links"]) == 2
        assert len(data["hierarchy"]) == 2

    def test_export_narrative(self, project: BewleyProject) -> None:
        self._setup_theory(project)
        out = project.cli_ok("export", "narrative")
        assert "# Theory: burnout" in out
        assert "Core Category" in out
        assert "long-hours" in out
        assert "lack-of-support" in out
        assert "causes" in out

    def test_export_theory_no_core(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "trust")
        out = project.cli_ok("export", "theory", "--format", "mermaid")
        assert "graph TD" in out

    def test_export_to_file(self, project: BewleyProject) -> None:
        self._setup_theory(project)
        project.cli_ok("export", "theory", "--format", "mermaid", "--output", "theory.md")
        assert (project.root / "theory.md").exists()


class TestRebuildIndex:
    def test_rebuild_with_new_features(self, project: BewleyProject) -> None:
        """Ensure rebuild-index works with all new event types."""
        project.cli_ok("code", "create", "burnout")
        project.cli_ok("code", "create", "stress")
        project.cli_ok("code", "set-parent", "stress", "burnout")
        project.cli_ok("code", "link", "stress", "burnout", "causes")
        project.cli_ok("code", "set-core", "burnout")
        project.cli_ok("memo", "add", "--code", "burnout", "Important theme")

        project.cli_ok("rebuild-index")

        # Verify everything survived rebuild
        show = project.cli_ok("code", "show", "stress")
        assert "burnout" in show
        links = project.cli_ok("code", "links")
        assert "causes" in links
        core = project.cli_ok("code", "show-core")
        assert "burnout" in core
        memos = project.cli_ok("memo", "list")
        assert "burnout" in memos or len(memos.strip()) > 0

    def test_fsck_with_new_features(self, project: BewleyProject) -> None:
        project.cli_ok("code", "create", "burnout")
        project.cli_ok("memo", "add", "Test memo")
        project.cli_ok("code", "create", "stress")
        project.cli_ok("code", "link", "stress", "burnout", "causes")
        out = project.cli_ok("fsck")
        assert "ok" in out
