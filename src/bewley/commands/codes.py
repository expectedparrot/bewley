from __future__ import annotations

from typing import List, Optional

import typer

from ..project import BewleyError, cmd_code_list, cmd_code_show, cmd_code_coverage, cmd_code_links
from .common import rich_console, HumanOption, QuietOption, fail, finish, get_project, should_emit_json
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
        console = rich_console()
        if tree:
            from rich.tree import Tree

            root = Tree("[bold green]codebook[/bold green]")

            def _add(branch, nodes: list) -> None:
                for node in nodes:
                    label = f"[bold]{node['canonical_name']}[/bold]"
                    if node.get("annotations"):
                        label += f"  [dim]({node['annotations']} annotations)[/dim]"
                    if node.get("description"):
                        label += f"\n[dim]{node['description']}[/dim]"
                    child = branch.add(label)
                    if "children" in node:
                        _add(child, node["children"])

            _add(root, result)
            console.print(root)
        else:
            from rich.table import Table

            table = Table(title=f"{len(result)} codes", show_header=True, header_style="bold green")
            table.add_column("Code", no_wrap=True)
            table.add_column("Description", overflow="fold")
            table.add_column("Annotations", justify="right")
            table.add_column("Documents", justify="right")
            for row in result:
                table.add_row(
                    row["canonical_name"], row.get("description") or "—",
                    str(row.get("annotations", 0)), str(row.get("documents", 0)),
                )
            console.print(table)


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


@app.command("update")
def code_update(
    ref: str = typer.Argument(..., help="Code name, alias, or id."),
    description: Optional[str] = typer.Option(None, "--description", help="The full definition."),
    inclusion: Optional[str] = typer.Option(None, "--inclusion", help="When this code applies."),
    exclusion: Optional[str] = typer.Option(None, "--exclusion", help="When it does not, and what to use instead."),
    human: bool = HumanOption,
) -> None:
    """Update a code's definition and inclusion/exclusion criteria."""
    command = "code update"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        project.update_code(
            ref, description=description,
            inclusion_criteria=inclusion, exclusion_criteria=exclusion,
        )
        data = cmd_code_show(project, ref)
    except BewleyError as e:
        fail(command, e, json_flag)
        return
    if json_flag:
        finish(command, data)
    else:
        typer.echo("ok")


@app.command("lint")
def code_lint(human: bool = HumanOption) -> None:
    """Flag codebook quality problems; never auto-fixes."""
    command = "code lint"
    json_flag = should_emit_json(human)
    try:
        from ..project import cmd_code_lint

        project = get_project(command, json_flag)
        findings = cmd_code_lint(project)
    except BewleyError as e:
        fail(command, e, json_flag)
        return
    if json_flag:
        finish(command, {"finding_count": len(findings), "findings": findings})
        return
    if not findings:
        typer.echo("codebook clean: no findings")
        return
    from rich.table import Table

    table = Table(title=f"{len(findings)} codebook findings")
    table.add_column("code")
    table.add_column("check", no_wrap=True)
    table.add_column("detail", overflow="fold")
    for finding in findings:
        table.add_row(finding["code_name"], finding["check"], finding["detail"])
    rich_console().print(table)


codebook_app = typer.Typer(help="Immutable named snapshots of the structured codebook.")


@codebook_app.command("release")
def codebook_release(
    name: str = typer.Argument(..., help="Release name, e.g. pilot-1. Immutable once created."),
    human: bool = HumanOption,
) -> None:
    """Freeze the current codebook as a named, immutable release."""
    command = "codebook release"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        event = project.release_codebook(name)
        data = {
            "name": name,
            "release_id": event["payload"]["release_id"],
            "codes": len(event["payload"]["snapshot"]),
        }
    except BewleyError as e:
        fail(command, e, json_flag)
        return
    if json_flag:
        finish(command, data)
    else:
        typer.echo(f"released {name}: {data['codes']} codes")


@codebook_app.command("diff")
def codebook_diff(
    from_name: str = typer.Argument(..., help="Earlier release name."),
    to_name: str = typer.Argument(..., help="Later release name."),
    human: bool = HumanOption,
) -> None:
    """Compare two codebook releases: added, removed, and changed codes."""
    command = "codebook diff"
    json_flag = should_emit_json(human)
    try:
        from ..project import cmd_codebook_diff

        project = get_project(command, json_flag)
        data = cmd_codebook_diff(project, from_name, to_name)
    except BewleyError as e:
        fail(command, e, json_flag)
        return
    if json_flag:
        finish(command, data)
        return
    for name in data["added"]:
        typer.echo(f"+ {name}")
    for name in data["removed"]:
        typer.echo(f"- {name}")
    for entry in data["changed"]:
        fields = ", ".join(entry["changes"])
        typer.echo(f"~ {entry['code_name']} ({fields})")
