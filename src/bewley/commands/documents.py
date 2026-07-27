from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

from ..project import (
    BewleyError, Project,
    cmd_list_documents, cmd_show_document, cmd_show_audio, cmd_show_video, cmd_show_snippets,
    DEFAULT_EXTRACT_AUDIO_BITRATE_KBPS, DEFAULT_VIDEO_CHUNK_OVERLAP_SECONDS,
)
from .common import rich_console, HumanOption, QuietOption, fail, finish, get_project, should_emit_json

app = typer.Typer(help="Document management.")

# Sub-apps for 'list' and 'show' subcommand groups
list_app = typer.Typer(help="List project entities.")
show_app = typer.Typer(help="Show detailed information.")

app.add_typer(list_app, name="list")
app.add_typer(show_app, name="show")


@app.command("add")
def add_command(
    path: str = typer.Argument(..., help="Path to the UTF-8 text file to add."),
    human: bool = HumanOption,
) -> None:
    """Add a UTF-8 text file to the corpus as a new document."""
    command = "add"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        event = project.add_document(path)
    except BewleyError as e:
        fail(command, e, json_flag)
    doc_id = event["payload"]["document_id"]
    if json_flag:
        finish(command, {"document_id": doc_id})
    else:
        typer.echo(doc_id)


_TRANSCRIPTION_WARNING = (
    "Transcription sends the media file to the OpenAI API and incurs provider "
    "charges billed to the OPENAI_API_KEY account."
)


@app.command("add-audio")
def add_audio_command(
    audio_path: str = typer.Argument(..., help="Path to the source audio file."),
    output: Optional[str] = typer.Option(None, "--output", help="Transcript path inside the project. Default: corpus/<audio-stem>.txt"),
    model: str = typer.Option("gpt-4o-transcribe", "--model", help="OpenAI transcription model."),
    response_format: str = typer.Option("json", "--response-format", help="Transcription response format.", show_choices=True),
    language: Optional[str] = typer.Option(None, "--language", help="Optional language hint like 'en'."),
    prompt: Optional[str] = typer.Option(None, "--prompt", help="Optional transcription prompt."),
    human: bool = HumanOption,
) -> None:
    """Transcribe an audio file with OpenAI and add the transcript to the corpus."""
    command = "add-audio"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        result = project.add_audio_document(
            audio_path, output,
            model=model, language=language, prompt=prompt, response_format=response_format,
        )
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(
            command,
            {"document_id": result["document_id"], "transcript_path": result["transcript_path"]},
            warnings=[_TRANSCRIPTION_WARNING],
        )
    else:
        typer.echo(result["document_id"])


@app.command("add-video")
def add_video_command(
    video_path: str = typer.Argument(..., help="Path to the source video file."),
    output: Optional[str] = typer.Option(None, "--output", help="Transcript path inside the project. Default: corpus/<video-stem>.txt"),
    model: str = typer.Option("gpt-4o-transcribe", "--model", help="OpenAI transcription model."),
    response_format: str = typer.Option("verbose_json", "--response-format", help="Transcription response format."),
    language: Optional[str] = typer.Option(None, "--language", help="Optional language hint like 'en'."),
    prompt: Optional[str] = typer.Option(None, "--prompt", help="Optional transcription prompt."),
    audio_bitrate_kbps: int = typer.Option(DEFAULT_EXTRACT_AUDIO_BITRATE_KBPS, "--audio-bitrate-kbps", help="Bitrate for extracted audio chunks."),
    chunk_overlap_seconds: float = typer.Option(DEFAULT_VIDEO_CHUNK_OVERLAP_SECONDS, "--chunk-overlap-seconds", help="Overlap between adjacent chunks."),
    human: bool = HumanOption,
) -> None:
    """Extract audio from a video, transcribe it, and add the transcript to the corpus."""
    command = "add-video"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        result = project.add_video_document(
            video_path, output,
            model=model, language=language, prompt=prompt, response_format=response_format,
            audio_bitrate_kbps=audio_bitrate_kbps, chunk_overlap_seconds=chunk_overlap_seconds,
        )
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(
            command,
            {"document_id": result["document_id"], "transcript_path": result["transcript_path"]},
            warnings=[_TRANSCRIPTION_WARNING],
        )
    else:
        typer.echo(result["document_id"])


@app.command("update")
def update_command(
    path: str = typer.Argument(..., help="Path to the updated UTF-8 text file (must already be tracked)."),
    human: bool = HumanOption,
) -> None:
    """Update an existing document with a new revision from the file on disk."""
    command = "update"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        event = project.update_document(path)
    except BewleyError as e:
        fail(command, e, json_flag)
    if event is None:
        if json_flag:
            finish(command, {"status": "no-op"})
        else:
            typer.echo("no-op")
    else:
        rev_id = event["payload"]["revision_id"]
        if json_flag:
            finish(command, {"revision_id": rev_id})
        else:
            typer.echo(rev_id)


@list_app.command("documents")
def list_documents(human: bool = HumanOption) -> None:
    """List all documents with their IDs, paths, and revision counts."""
    command = "list documents"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        result = cmd_list_documents(project)
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, result)
    else:
        from rich.table import Table

        table = Table(title=f"{len(result)} documents", show_header=True, header_style="bold green")
        table.add_column("Path", overflow="fold")
        table.add_column("Document ID", no_wrap=True)
        table.add_column("Revisions", justify="right")
        for row in result:
            table.add_row(row["current_path"], row["document_id"][:12], str(row["revision_count"]))
        rich_console().print(table)


@show_app.command("document")
def show_document(
    document_ref: str = typer.Argument(..., help="Document identifier: UUID, path, or path prefix."),
    human: bool = HumanOption,
) -> None:
    """Show metadata, revisions, and annotations for a document."""
    command = "show document"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        result = cmd_show_document(project, document_ref)
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, result)
    else:
        typer.echo(f"document_id\t{result['document_id']}")
        typer.echo(f"path\t{result['path']}")
        if result.get("audio_source"):
            audio = result["audio_source"]
            typer.echo("audio_source")
            for key in ("original_audio_filename", "original_audio_path", "media_type", "transcription_model", "transcription_response_format", "transcription_language", "transcript_style", "segment_count"):
                if audio.get(key) is not None:
                    typer.echo(f"  {key}\t{audio[key]}")
        if result.get("video_source"):
            video = result["video_source"]
            typer.echo("video_source")
            for key in ("original_video_filename", "original_video_path", "media_type", "duration", "transcription_model", "transcription_response_format", "transcription_language", "transcript_style", "chunk_count"):
                if video.get(key) is not None:
                    typer.echo(f"  {key}\t{video[key]}")
        typer.echo("revisions")
        for r in result["revisions"]:
            typer.echo(f"  {r['revision_id']}\t{r['created_at']}\t{r['byte_length']}\t{r['line_count']}\t{r['is_current']}")
        typer.echo("annotations")
        for a in result["annotations"]:
            typer.echo(f"  {a['annotation_id']}\t{a['canonical_name']}\t{a['scope_type']}\t{a['start_line']}\t{a['end_line']}\t{a['anchor_status']}")


@show_app.command("audio")
def show_audio(
    document_ref: str = typer.Argument(..., help="Document identifier: UUID, path, or path prefix."),
    human: bool = HumanOption,
) -> None:
    """Show the audio source linked to a transcript document."""
    command = "show audio"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        result = cmd_show_audio(project, document_ref)
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, result)
    else:
        for key in ("document_id", "path", "original_audio_filename", "original_audio_path", "stored_audio_path", "stored_audio_sha256", "media_type", "transcription_model", "transcription_response_format", "transcription_language", "transcript_style", "segment_count"):
            if result.get(key) is not None:
                typer.echo(f"{key}\t{result[key]}")
        typer.echo("segments")
        for s in result.get("segments", []):
            typer.echo(f"  {s['start']}\t{s['end']}\t{s['speaker']}\t{s['text']}")


@show_app.command("video")
def show_video(
    document_ref: str = typer.Argument(..., help="Document identifier: UUID, path, or path prefix."),
    human: bool = HumanOption,
) -> None:
    """Show the video source and chunk metadata linked to a transcript document."""
    command = "show video"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        result = cmd_show_video(project, document_ref)
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, result)
    else:
        for key in ("document_id", "path", "original_video_filename", "original_video_path", "stored_video_path", "stored_video_sha256", "media_type", "duration", "transcription_model", "transcription_response_format", "transcription_language", "transcript_style", "chunk_count"):
            if result.get(key) is not None:
                typer.echo(f"{key}\t{result[key]}")
        typer.echo("chunks")
        for c in result.get("chunks", []):
            typer.echo(f"  {c['chunk_index']}\t{c['extract_start']}\t{c['extract_end']}\t{c['logical_start']}\t{c['logical_end']}\t{c['byte_length']}")
        typer.echo("segments")
        for s in result.get("segments", []):
            typer.echo(f"  {s['start']}\t{s['end']}\t{s['speaker']}\t{s['text']}")


@show_app.command("snippets")
def show_snippets(
    code: str = typer.Option(..., "--code", help="Code name, alias, or code_id to show snippets for."),
    human: bool = HumanOption,
) -> None:
    """Show text snippets for all annotations of a given code."""
    command = "show snippets"
    json_flag = should_emit_json(human)
    try:
        project = get_project()
        result = cmd_show_snippets(project, code)
    except BewleyError as e:
        fail(command, e, json_flag)
    if json_flag:
        finish(command, result)
    else:
        from rich.panel import Panel

        console = rich_console()
        console.print(f"[bold green]{len(result)} snippet(s)[/bold green]")
        for row in result:
            lines = ""
            if row["start_line"] is not None:
                lines = f" · lines {row['start_line']}–{row['end_line']}"
            status = "" if row["anchor_status"] == "clean" else f" · [yellow]{row['anchor_status']}[/yellow]"
            console.print(Panel(
                row["text"],
                title=f"[bold]{row['code_name']}[/bold]",
                subtitle=f"{row['document_path']}{lines}{status}",
                border_style="green",
            ))
