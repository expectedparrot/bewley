"""Memo subcommands: add, list, show, edit, delete."""
from __future__ import annotations

from typing import Optional

import typer

from bewley.commands.common import HumanOption, fail, finish, get_project, should_emit_json
from bewley.project import BewleyError, cmd_memo_list, cmd_memo_show

app = typer.Typer(help="Create, list, show, edit, and delete analytic memos.")


@app.command("add")
def memo_add(
    content: Optional[str] = typer.Argument(None, help="Memo content (omit to open $EDITOR)."),
    code: Optional[str] = typer.Option(None, "--code", help="Attach memo to this code (name, alias, or UUID)."),
    document: Optional[str] = typer.Option(None, "--document", help="Attach memo to this document (UUID or path prefix)."),
    title: Optional[str] = typer.Option(None, "--title", help="Optional title for the memo."),
    human: bool = HumanOption,
) -> None:
    """Create a new memo, optionally attached to a code or document."""
    json_flag = should_emit_json(human)
    command = "memo add"
    if code and document:
        fail(command, BewleyError("Specify at most one of --code or --document.", code="INVALID_INPUT"), json_flag)
        raise typer.Exit(2)
    project = get_project(command, json_flag)
    try:
        if code:
            target_type, target_ref = "code", code
        elif document:
            target_type, target_ref = "document", document
        else:
            target_type, target_ref = "project", None
        memo_content = content
        if memo_content is None:
            memo_content = project._open_editor()
            if not memo_content.strip():
                if json_flag:
                    fail(command, BewleyError("aborted: empty memo", code="ABORTED"), json_flag)
                else:
                    print("aborted: empty memo")
                raise typer.Exit(1)
        event = project.create_memo(target_type, target_ref, memo_content, title)
    except BewleyError as exc:
        fail(command, exc, json_flag)
        raise typer.Exit(2)
    memo_id = event["payload"]["memo_id"]
    if json_flag:
        finish(command, {"memo_id": memo_id})
    else:
        print(memo_id)


@app.command("list")
def memo_list(
    code: Optional[str] = typer.Option(None, "--code", help="Filter memos attached to this code."),
    document: Optional[str] = typer.Option(None, "--document", help="Filter memos attached to this document."),
    human: bool = HumanOption,
) -> None:
    """List memos, optionally filtered by code or document."""
    json_flag = should_emit_json(human)
    command = "memo list"
    if code and document:
        fail(command, BewleyError("Specify at most one of --code or --document.", code="INVALID_INPUT"), json_flag)
        raise typer.Exit(2)
    project = get_project(command, json_flag)
    try:
        if code:
            result = cmd_memo_list(project, target_type="code", target_ref=code)
        elif document:
            result = cmd_memo_list(project, target_type="document", target_ref=document)
        else:
            result = cmd_memo_list(project)
    except BewleyError as exc:
        fail(command, exc, json_flag)
        raise typer.Exit(2)
    if json_flag:
        finish(command, result)
    else:
        for row in result:
            print(f"{row['memo_id']}\t{row['target_type']}\t{row['title']}\t{row['created_at']}")


@app.command("show")
def memo_show(
    memo_id: str = typer.Argument(..., help="UUID of the memo to show."),
    human: bool = HumanOption,
) -> None:
    """Show the full content of a memo."""
    json_flag = should_emit_json(human)
    command = "memo show"
    project = get_project(command, json_flag)
    try:
        result = cmd_memo_show(project, memo_id)
    except BewleyError as exc:
        fail(command, exc, json_flag)
        raise typer.Exit(2)
    if json_flag:
        finish(command, result)
    else:
        print(f"memo_id\t{result['memo_id']}")
        print(f"target_type\t{result['target_type']}")
        print(f"target_id\t{result['target_id']}")
        if result.get("title"):
            print(f"title\t{result['title']}")
        print(f"created_at\t{result['created_at']}")
        print(f"updated_at\t{result['updated_at']}")
        print()
        print(result["content"])


@app.command("edit")
def memo_edit(
    memo_id: str = typer.Argument(..., help="UUID of the memo to edit."),
    human: bool = HumanOption,
) -> None:
    """Edit a memo in your $EDITOR."""
    json_flag = should_emit_json(human)
    command = "memo edit"
    project = get_project(command, json_flag)
    try:
        with project.connect() as conn:
            memo = project.resolve_memo(conn, memo_id)
        old_content = project.read_memo_content(memo["content_sha256"])
        new_content = project._open_editor(old_content)
        if not new_content.strip():
            if json_flag:
                fail(command, BewleyError("aborted: empty memo", code="ABORTED"), json_flag)
            else:
                print("aborted: empty memo")
            raise typer.Exit(1)
        event = project.update_memo(memo_id, new_content)
    except BewleyError as exc:
        fail(command, exc, json_flag)
        raise typer.Exit(2)
    if json_flag:
        finish(command, {"event_id": event["event_id"]})
    else:
        print(event["event_id"])


@app.command("delete")
def memo_delete(
    memo_id: str = typer.Argument(..., help="UUID of the memo to delete."),
    human: bool = HumanOption,
) -> None:
    """Delete a memo."""
    json_flag = should_emit_json(human)
    command = "memo delete"
    project = get_project(command, json_flag)
    try:
        event = project.delete_memo(memo_id)
    except BewleyError as exc:
        fail(command, exc, json_flag)
        raise typer.Exit(2)
    if json_flag:
        finish(command, {"event_id": event["event_id"]})
    else:
        print(event["event_id"])
