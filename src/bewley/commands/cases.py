"""Cases, attributes, and entity links (RFC 001, slice 2)."""
from __future__ import annotations

from typing import Optional

import typer

from ..project import (
    BewleyError,
    cmd_attribute_list,
    cmd_case_list,
    cmd_case_show,
    cmd_link_list,
)
from .common import HumanOption, action, fail, finish, get_project, should_emit_json

case_app = typer.Typer(help="Cases: the people, organizations, or sites the study is about.")
attribute_app = typer.Typer(help="Typed attribute definitions for cases.")
link_app = typer.Typer(help="Typed links between research entities.")


@case_app.command("create")
def case_create_command(
    name: str = typer.Argument(..., help="Display name, unique among active cases."),
    case_type: Optional[str] = typer.Option(None, "--type", help="Plain label, e.g. person, organization, site."),
    description: Optional[str] = typer.Option(None, "--description"),
    human: bool = HumanOption,
) -> None:
    """Create a case."""
    command = "case create"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        event = project.create_case(name, case_type=case_type, description=description)
        data = {"case_id": event["payload"]["case_id"], "name": name, "case_type": case_type}
    except BewleyError as e:
        fail(command, e, json_flag)
        return
    if json_flag:
        finish(command, data, next_actions=[
            action(
                "link-documents",
                "Link the documents this case authored or appears in.",
                ["bewley", "case", "link", name, "corpus/<file>", "--as", "author"],
                mutates_state=True,
            ),
        ])
    else:
        typer.echo(data["case_id"])


@case_app.command("list")
def case_list_command(human: bool = HumanOption) -> None:
    """List active cases with document and attribute counts."""
    command = "case list"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        rows = cmd_case_list(project)
    except BewleyError as e:
        fail(command, e, json_flag)
        return
    if json_flag:
        finish(command, {"cases": rows})
        return
    from rich.table import Table

    from .common import rich_console

    table = Table(title=f"{len(rows)} cases")
    table.add_column("name")
    table.add_column("type")
    table.add_column("documents", justify="right")
    table.add_column("attributes", justify="right")
    for row in rows:
        table.add_row(row["name"], row["case_type"] or "-", str(row["documents"]), str(row["attributes"]))
    rich_console().print(table)


@case_app.command("show")
def case_show_command(ref: str = typer.Argument(...), human: bool = HumanOption) -> None:
    """Show a case: attributes and linked documents."""
    command = "case show"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        data = cmd_case_show(project, ref)
    except BewleyError as e:
        fail(command, e, json_flag)
        return
    if json_flag:
        finish(command, data)
        return
    from rich.panel import Panel
    from rich.text import Text

    from .common import rich_console

    body = Text()
    if data["case_type"]:
        body.append("type  ", style="bold")
        body.append(f"{data['case_type']}\n")
    if data["description"]:
        body.append(f"{data['description']}\n")
    if data["attributes"]:
        body.append("\nAttributes\n", style="bold")
        for attribute in data["attributes"]:
            shown = attribute["value"] if attribute["value"] is not None else f"({attribute['special']})"
            body.append(f"  {attribute['name']}: {shown}\n")
    if data["documents"]:
        body.append(f"\nDocuments ({len(data['documents'])})\n", style="bold")
        for document in data["documents"]:
            body.append(f"  {document['relationship']:<12} {document['current_path']}\n")
    rich_console().print(Panel(body, title=data["name"], border_style="green"))


@case_app.command("set")
def case_set_command(
    ref: str = typer.Argument(..., help="Case name or id."),
    attribute: str = typer.Argument(..., help="Attribute name (define it first)."),
    value: Optional[str] = typer.Argument(None, help="The value; omit when using --special."),
    special: Optional[str] = typer.Option(None, "--special", help="missing | unknown | not_applicable | confidential"),
    human: bool = HumanOption,
) -> None:
    """Set an attribute value on a case."""
    command = "case set"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        project.set_case_attribute(ref, attribute, value=value, special=special)
        data = cmd_case_show(project, ref)
    except BewleyError as e:
        fail(command, e, json_flag)
        return
    if json_flag:
        finish(command, {"case_id": data["case_id"], "name": data["name"], "attributes": data["attributes"]})
    else:
        typer.echo("ok")


@case_app.command("link")
def case_link_command(
    ref: str = typer.Argument(..., help="Case name or id."),
    document: str = typer.Argument(..., help="Document path or id."),
    relationship: str = typer.Option(..., "--as", help="author | participant | subject | site | other"),
    human: bool = HumanOption,
) -> None:
    """Link a case to a document (sugar over `bewley link add`)."""
    command = "case link"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        event = project.create_entity_link("case", ref, relationship, "document", document)
        data = {"link_id": event["payload"]["link_id"], "relationship": relationship}
    except BewleyError as e:
        fail(command, e, json_flag)
        return
    if json_flag:
        finish(command, data)
    else:
        typer.echo(data["link_id"])


@attribute_app.command("define")
def attribute_define_command(
    name: str = typer.Argument(...),
    value_type: str = typer.Option(..., "--type", help="text | number | boolean | date | categorical"),
    values: Optional[str] = typer.Option(None, "--values", help="Comma-separated allowed values (categorical only)."),
    human: bool = HumanOption,
) -> None:
    """Define a typed, project-wide case attribute."""
    command = "attribute define"
    json_flag = should_emit_json(human)
    allowed = [item.strip() for item in values.split(",") if item.strip()] if values else None
    try:
        project = get_project(command, json_flag)
        event = project.define_attribute(name, value_type, allowed_values=allowed)
        data = {"attribute_id": event["payload"]["attribute_id"], "name": name,
                "value_type": value_type, "allowed_values": allowed}
    except BewleyError as e:
        fail(command, e, json_flag)
        return
    if json_flag:
        finish(command, data)
    else:
        typer.echo(data["attribute_id"])


@attribute_app.command("list")
def attribute_list_command(human: bool = HumanOption) -> None:
    """List attribute definitions and how many cases carry each."""
    command = "attribute list"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        rows = cmd_attribute_list(project)
    except BewleyError as e:
        fail(command, e, json_flag)
        return
    if json_flag:
        finish(command, {"attributes": rows})
        return
    for row in rows:
        allowed = f" ({', '.join(row['allowed_values'])})" if row["allowed_values"] else ""
        typer.echo(f"{row['name']}\t{row['value_type']}{allowed}\t{row['cases']} cases")


@link_app.command("add")
def link_add_command(
    source: str = typer.Argument(..., help='Source entity as kind:ref, e.g. case:"Abigail Adams".'),
    target: str = typer.Argument(..., help="Target entity as kind:ref, e.g. document:corpus/letter.txt."),
    relationship: str = typer.Option(..., "--rel", help="Relationship type; allowed combinations are validated."),
    memo: Optional[str] = typer.Option(None, "--memo"),
    human: bool = HumanOption,
) -> None:
    """Create a typed link between two research entities."""
    command = "link add"
    json_flag = should_emit_json(human)
    try:
        for argument in (source, target):
            if ":" not in argument:
                raise BewleyError(
                    f"entity references use kind:ref, got: {argument}", code="INVALID_INPUT"
                )
        source_kind, source_ref = source.split(":", 1)
        target_kind, target_ref = target.split(":", 1)
        project = get_project(command, json_flag)
        event = project.create_entity_link(
            source_kind, source_ref, relationship, target_kind, target_ref, memo=memo
        )
        data = {"link_id": event["payload"]["link_id"]}
    except BewleyError as e:
        fail(command, e, json_flag)
        return
    if json_flag:
        finish(command, data)
    else:
        typer.echo(data["link_id"])


@link_app.command("list")
def link_list_command(
    entity: Optional[str] = typer.Option(None, "--entity", help="Filter to links touching kind:ref."),
    human: bool = HumanOption,
) -> None:
    """List active entity links (includes code-to-code links)."""
    command = "link list"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        rows = cmd_link_list(project, entity=entity)
    except BewleyError as e:
        fail(command, e, json_flag)
        return
    if json_flag:
        finish(command, {"links": rows})
        return
    from rich.table import Table

    from .common import rich_console

    table = Table(title=f"{len(rows)} links")
    table.add_column("source")
    table.add_column("relationship")
    table.add_column("target")
    for row in rows:
        table.add_row(
            f"{row['source_kind']}:{row['source']}",
            row["relationship"],
            f"{row['target_kind']}:{row['target']}",
        )
    rich_console().print(table)


@link_app.command("remove")
def link_remove_command(link_id: str = typer.Argument(...), human: bool = HumanOption) -> None:
    """Deactivate an entity link (compensating event, never a deletion)."""
    command = "link remove"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        event = project.remove_entity_link(link_id)
        data = {"link_id": event["payload"]["link_id"], "status": "removed"}
    except BewleyError as e:
        fail(command, e, json_flag)
        return
    if json_flag:
        finish(command, data)
    else:
        typer.echo("removed")
