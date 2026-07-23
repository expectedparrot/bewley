from __future__ import annotations

from typing import List, Optional

import typer

from ..project import BewleyError, cmd_code_list, cmd_code_show, cmd_code_coverage, cmd_code_links
from .common import HumanOption, QuietOption, fail, finish, get_project, should_emit_json
from .documents import list_app

app = typer.Typer(help="Code management.")


@app.command("create")
def code_create(
    name: str = typer.Argument(..., help="Name for the new code."),
    description: Optional[str] = typer.Option(None, "--description", help="Free-text description."),
    color: Optional[str] = typer.Option(None, "--color", help="Display color (for HTML exports)."),
    human: bool = HumanOption,
) -> None:
    """Create a new analytic code."""
    command = "code create"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        event = project.add_code(name, description, color)
    except BewleyError as e:
        fail(command, e, json_flag)
    code_id = event["payload"]["code_id"]
    if json_flag:
        finish(command, {"code_id": code_id})
    else:
        typer.echo(code_id)


def _code_list_impl(command: str, tree: bool, human: bool) -> None:
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        result = cmd_code_list(project, tree=tree)
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, result)
    else:
        if tree:
            def _print_tree(nodes: list, indent: int = 0) -> None:
                for node in nodes:
                    typer.echo(f"{'  ' * indent}{node['canonical_name']}")
                    if "children" in node:
                        _print_tree(node["children"], indent + 1)
            _print_tree(result)
        else:
            for row in result:
                typer.echo(f"{row['code_id']}\t{row['canonical_name']}\t{row['status']}")


@app.command("list")
def code_list(
    tree: bool = typer.Option(False, "--tree", help="Show codes as an indented parent-child hierarchy."),
    human: bool = HumanOption,
) -> None:
    """List all codes with their IDs, names, and annotation counts."""
    _code_list_impl("code list", tree, human)


@list_app.command("codes")
def list_codes_alias(
    tree: bool = typer.Option(False, "--tree", help="Show codes as an indented parent-child hierarchy."),
    human: bool = HumanOption,
) -> None:
    """List all codes. Alias for `bewley code list`."""
    _code_list_impl("list codes", tree, human)


@app.command("show")
def code_show(
    code_ref: str = typer.Argument(..., help="Code identifier: UUID, canonical name, or alias."),
    human: bool = HumanOption,
) -> None:
    """Show detailed info for a code: metadata, aliases, annotations."""
    command = "code show"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        result = cmd_code_show(project, code_ref)
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, result)
    else:
        typer.echo(f"code_id\t{result['code_id']}")
        typer.echo(f"name\t{result['name']}")
        typer.echo(f"status\t{result['status']}")
        typer.echo(f"active_annotations\t{result['active_annotations']}")
        typer.echo(f"aliases\t{', '.join(result['aliases'])}")
        if result.get("parent"):
            typer.echo(f"parent\t{result['parent']}")
        if result.get("children"):
            typer.echo(f"children\t{', '.join(result['children'])}")
        if result.get("links"):
            typer.echo("links")
            for lk in result["links"]:
                typer.echo(f"  {lk['link_id'][:8]}\t{lk['source_name']} --{lk['relationship']}--> {lk['target_name']}")


@app.command("rename")
def code_rename(
    old: str = typer.Argument(..., help="Current name (or code_id) of the code to rename."),
    new: str = typer.Argument(..., help="New canonical name for the code."),
    description: Optional[str] = typer.Option(
        None, "--description", "-d",
        help="New description. Updated atomically with the rename.",
    ),
    human: bool = HumanOption,
) -> None:
    """Rename a code (all annotations follow automatically).

    Optionally update the description in the same event so it doesn't go stale.
    """
    command = "code rename"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        event = project.rename_code(old, new, description)
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, {"event_id": event["event_id"]})
    else:
        typer.echo(event["event_id"])


@app.command("alias")
def code_alias(
    code_ref: str = typer.Argument(..., help="Code to add the alias to (name, alias, or UUID)."),
    alias: str = typer.Argument(..., help="New alias name."),
    human: bool = HumanOption,
) -> None:
    """Add an alternative name (alias) to a code."""
    command = "code alias"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        event = project.alias_code(code_ref, alias)
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, {"event_id": event["event_id"]})
    else:
        typer.echo(event["event_id"])


@app.command("merge")
def code_merge(
    sources: List[str] = typer.Argument(..., help="One or more source codes to merge."),
    into: str = typer.Option(..., "--into", help="Target code that absorbs the source annotations."),
    human: bool = HumanOption,
) -> None:
    """Merge one or more source codes into a target code."""
    command = "code merge"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        event = project.merge_codes(sources, into)
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, {"event_id": event["event_id"]})
    else:
        typer.echo(event["event_id"])


@app.command("split")
def code_split(
    source: str = typer.Argument(..., help="Source code to split from (name or UUID)."),
    new: str = typer.Option(..., "--new", help="Name for the new code."),
    annotation: Optional[List[str]] = typer.Option(None, "--annotation", help="Annotation ID to move (repeat for multiple)."),
    description: Optional[str] = typer.Option(None, "--description", help="Description for the new code."),
    color: Optional[str] = typer.Option(None, "--color", help="Color for the new code."),
    human: bool = HumanOption,
) -> None:
    """Move selected annotations from one code to a new code."""
    command = "code split"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        event = project.split_code(source, new, annotation or [], description, color)
    except BewleyError as e:
        fail(command, e, json_flag)
    new_code_id = event["payload"]["new_code_id"]
    if json_flag:
        finish(command, {"new_code_id": new_code_id})
    else:
        typer.echo(new_code_id)


@app.command("set-parent")
def code_set_parent(
    code_ref: str = typer.Argument(..., help="Child code (name or UUID)."),
    parent_ref: str = typer.Argument(..., help="Parent code (name or UUID)."),
    human: bool = HumanOption,
) -> None:
    """Set a code's parent to build a hierarchical code tree."""
    command = "code set-parent"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        event = project.set_code_parent(code_ref, parent_ref)
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, {"event_id": event["event_id"]})
    else:
        typer.echo(event["event_id"])


@app.command("clear-parent")
def code_clear_parent(
    code_ref: str = typer.Argument(..., help="Code to detach from its parent (name or UUID)."),
    human: bool = HumanOption,
) -> None:
    """Remove a code from its parent (make it a root code)."""
    command = "code clear-parent"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        event = project.clear_code_parent(code_ref)
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, {"event_id": event["event_id"]})
    else:
        typer.echo(event["event_id"])


@app.command("link")
def code_link(
    source: str = typer.Argument(..., help="Source code of the relationship (name or UUID)."),
    target: str = typer.Argument(..., help="Target code of the relationship (name or UUID)."),
    relationship: str = typer.Argument(..., help="Label for the relationship (e.g., 'causes', 'contradicts')."),
    memo: Optional[str] = typer.Option(None, "--memo", help="Optional memo explaining the link."),
    human: bool = HumanOption,
) -> None:
    """Create a named relationship (link) between two codes."""
    command = "code link"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        event = project.create_code_link(source, target, relationship, memo)
    except BewleyError as e:
        fail(command, e, json_flag)
    link_id = event["payload"]["link_id"]
    if json_flag:
        finish(command, {"link_id": link_id})
    else:
        typer.echo(link_id)


@app.command("links")
def code_links(
    code_ref: Optional[str] = typer.Argument(None, help="Optional code to filter links by (name or UUID)."),
    human: bool = HumanOption,
) -> None:
    """List relationships (links) between codes."""
    command = "code links"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        result = cmd_code_links(project, code_ref)
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, result)
    else:
        if not result:
            typer.echo("no links")
        else:
            for lk in result:
                memo_part = f"  ({lk['memo']})" if lk.get("memo") else ""
                typer.echo(f"{lk['link_id']}\t{lk['source_name']} --{lk['relationship']}--> {lk['target_name']}{memo_part}")


@app.command("unlink")
def code_unlink(
    link_id: str = typer.Argument(..., help="UUID of the link to remove (from 'code links' output)."),
    human: bool = HumanOption,
) -> None:
    """Remove a relationship (link) between two codes."""
    command = "code unlink"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        event = project.remove_code_link(link_id)
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, {"event_id": event["event_id"]})
    else:
        typer.echo(event["event_id"])


@app.command("set-core")
def code_set_core(
    code_ref: str = typer.Argument(..., help="Code to designate as core category (name or UUID)."),
    human: bool = HumanOption,
) -> None:
    """Designate a code as the core category for grounded theory."""
    command = "code set-core"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        event = project.set_core_category(code_ref)
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, {"event_id": event["event_id"]})
    else:
        typer.echo(event["event_id"])


@app.command("show-core")
def code_show_core(human: bool = HumanOption) -> None:
    """Show the current core category (if set)."""
    command = "code show-core"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        with project.connect() as conn:
            core = project.get_core_category(conn)
    except BewleyError as e:
        fail(command, e, json_flag)
    if core:
        result = {"code_id": core["code_id"], "canonical_name": core["canonical_name"]}
        if json_flag:
            finish(command, result)
        else:
            typer.echo(f"{core['code_id']}\t{core['canonical_name']}")
    else:
        if json_flag:
            finish(command, None)
        else:
            typer.echo("no core category set")


@app.command("coverage")
def code_coverage(
    code_ref: str = typer.Argument(..., help="Code to inspect (name or UUID)."),
    breakdown: bool = typer.Option(
        False, "--breakdown",
        help="Show per-descendant respondent counts. Useful for parents — the inclusive rollup hides whether coverage comes from one child with strong evidence or many children with disjoint respondents.",
    ),
    human: bool = HumanOption,
) -> None:
    """Report how many respondents a code (and its descendants) covers."""
    command = "code coverage"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        result = cmd_code_coverage(project, code_ref, breakdown=breakdown)
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, result)
    else:
        typer.echo(f"code\t{result['code']}")
        typer.echo(f"total_respondents\t{result['total_respondents']}")
        typer.echo(f"direct\t{result['direct']} of {result['total_respondents']}")
        typer.echo(f"inclusive\t{result['inclusive']} of {result['total_respondents']}")
        if result["descendants"]:
            typer.echo(f"descendants\t{', '.join(result['descendants'])}")
        if result.get("breakdown"):
            typer.echo("breakdown")
            for row in result["breakdown"]:
                marker = "*" if row["is_target"] else " "
                typer.echo(f"  {marker} {row['code']}\t{row['respondents']} of {result['total_respondents']}")
