# CLAUDE.md

## Project overview

Bewley is a local-first CLI tool for qualitative coding of interview data and UTF-8 text corpora. Think `git` for qualitative research — append-only event log, immutable document revisions, rebuildable SQLite index.

## Architecture

- **Typer-based CLI** split across `src/bewley/`. `cli.py` is a thin entry point that assembles `typer.Typer` subapps from `commands/*.py`. Core domain logic (project store, event log, workflow inference, agent brief) lives in `project.py`.
- **Entry point**: `bewley.cli:main` (registered in `pyproject.toml` as the `bewley` console script).
- **Dependencies**: `typer` and `rich` only, on top of Python 3.11+ stdlib (`sqlite3`, `tomllib`, `json`, `hashlib`).
- **Event-sourced**: Every mutation appends a JSON event to `.bewley/events/`. The SQLite database (`bewley.sqlite`) is a projection that can be rebuilt from events at any time.
- **Content-addressed storage**: Document revisions stored by SHA-256 in `.bewley/objects/documents/`.

## Agent-facing contract (follows `packages/CLI_GUIDE.md`)

- **JSON by default**; `--human` / `-H` or `BEWLEY_HUMAN_OUTPUT=true` switches to rich text. Every command emits the standard envelope (`command`, `status`, `data`, `warnings`, `errors`, `next_steps`) via `commands/common.py` (`finish`, `fail`, `should_emit_json`).
- **`bewley docs list | show <topic> | search <query>`** exposes the embedded help topics in `src/bewley/docs_content/` (overview, getting-started, workflow, commands, grounded-theory).
- **Workflow state is inferred from disk**, not stored metadata. `project._infer_phase`, `_phase_state`, and `_PHASE_DOCS` live in `project.py`.
- **Errors** raise `BewleyError(code, message, context, hint)`; `fail()` serializes them into the envelope.

## Key commands

```
bewley docs list / show <topic> / search <q>    # embedded documentation
bewley init / status / fsck / rebuild-index
bewley add / add-audio / add-video / update
bewley list <entity> / show <entity>
bewley code create / list / show / rename / alias / merge / split
bewley annotate apply / remove / show / resolve
bewley query / export (snippets | quotes | html | document-html)
bewley history / undo
bewley memo create / list / show / edit / delete
bewley codegen <phase>                          # generate EDSL scripts
```

## Build & test

```bash
pip install -e .                                # install editable
PYTHONPATH=src python -m pytest tests/          # run full test suite
```

Tests use `pytest` with fixtures in `conftest.py`. `test_new_commands.py` covers `docs`, `agent-start`, and `codegen`. The older `test_smoke.py` still exercises the full init→add→annotate→query→fsck flow.

## Project layout

```
src/bewley/
  __init__.py
  __main__.py            # delegates to cli.main()
  cli.py                 # thin typer entry point; wires commands/
  project.py             # store, event log, workflow, agent brief
  docs.py                # doc registry + load/search
  commands/              # one typer subapp per command group
    agent_start.py docs.py documents.py codes.py annotations.py
    query.py export.py history.py memos.py codegen.py project.py common.py
  docs_content/*.md      # bundled via package-data
tests/                   # pytest suite
qualitative-analysis/    # example working project
SPEC.md / README.md      # spec and user-facing docs
```

## Important conventions

- The event log is append-only; undo emits compensating events, never deletes.
- Annotation relocation across revisions is best-effort; uncertain cases become `conflicted` rather than silently guessing.
- SQLite is a cache, not the source of truth. `rebuild-index` must always recover from events alone.
- Write lock (`write.lock`) enforces single-writer concurrency.
- All file writes use atomic rename (`os.replace`) for crash safety.
- Annotation scope is mandatory: `bewley annotate apply` requires one of `--document`, `--bytes S:E`, or `--lines S:E`. Line ranges are 1-based inclusive; byte ranges are 0-based exclusive-end.
