"""Study manifest and research questions (RFC 001, slice 1)."""
from __future__ import annotations

from typing import Optional

import typer

from ..project import BewleyError, cmd_study_show
from .common import HumanOption, action, fail, finish, get_project, should_emit_json

study_app = typer.Typer(help="The study manifest: method, unit of analysis, purpose.")
question_app = typer.Typer(help="Research questions the analysis answers to.")


@study_app.command("set")
def study_set_command(
    method: Optional[str] = typer.Option(None, "--method", help="Analytic approach, e.g. grounded-theory, reflexive-ta, content-analysis, framework."),
    unit: Optional[str] = typer.Option(None, "--unit", help="Unit of analysis, e.g. document, participant, organization, site."),
    purpose: Optional[str] = typer.Option(None, "--purpose", help="One-paragraph statement of what the study is for."),
    human: bool = HumanOption,
) -> None:
    """Declare or update the study design (partial updates allowed)."""
    command = "study set"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        project.configure_study(method=method, unit_of_analysis=unit, purpose=purpose)
        data = cmd_study_show(project)
    except BewleyError as e:
        fail(command, e, json_flag)
        return
    if json_flag:
        next_actions = []
        if not data["research_questions"]:
            next_actions.append(action(
                "add-question",
                "Record the research question the coding will answer.",
                ["bewley", "question", "add", "<question>"],
                mutates_state=True,
            ))
        finish(command, data, next_actions=next_actions)
    else:
        typer.echo(f"method\t{data['method'] or '-'}")
        typer.echo(f"unit_of_analysis\t{data['unit_of_analysis'] or '-'}")


@study_app.command("show")
def study_show_command(human: bool = HumanOption) -> None:
    """Show the study manifest and research questions."""
    command = "study show"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        data = cmd_study_show(project)
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
    body.append("method            ", style="bold")
    body.append(f"{data['method'] or '(unset)'}\n")
    body.append("unit of analysis  ", style="bold")
    body.append(f"{data['unit_of_analysis'] or '(unset)'}\n")
    if data["purpose"]:
        body.append("purpose           ", style="bold")
        body.append(f"{data['purpose']}\n")
    if data["research_questions"]:
        body.append("\nResearch questions\n", style="bold")
        for index, question in enumerate(data["research_questions"], 1):
            body.append(f"  {index}. {question['text']}\n")
    else:
        body.append("\n(no research questions recorded)\n", style="dim")
    rich_console().print(Panel(body, title="Study", border_style="green"))


@question_app.command("add")
def question_add_command(
    text: str = typer.Argument(..., help="The research question, quoted."),
    human: bool = HumanOption,
) -> None:
    """Record a research question."""
    command = "question add"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        event = project.add_research_question(text)
        data = {
            "question_id": event["payload"]["question_id"],
            "text": event["payload"]["text"],
            "question_count": len(cmd_study_show(project)["research_questions"]),
        }
    except BewleyError as e:
        fail(command, e, json_flag)
        return
    if json_flag:
        finish(command, data)
    else:
        typer.echo(data["question_id"])


@question_app.command("list")
def question_list_command(human: bool = HumanOption) -> None:
    """List recorded research questions."""
    command = "question list"
    json_flag = should_emit_json(human)
    try:
        project = get_project(command, json_flag)
        questions = cmd_study_show(project)["research_questions"]
    except BewleyError as e:
        fail(command, e, json_flag)
        return
    if json_flag:
        finish(command, {"research_questions": questions})
        return
    for index, question in enumerate(questions, 1):
        typer.echo(f"{index}. [{question['status']}] {question['text']}")
