# CLAUDE.md

## Project overview

Bewley is a local-first CLI tool for qualitative coding of interview data and UTF-8 text corpora. Think `git` for qualitative research — append-only event log, immutable document revisions, rebuildable SQLite index.

## Architecture

- **Single-file CLI**: All logic lives in `src/bewley/cli.py` (~3100 lines). No external dependencies beyond the Python 3.12+ stdlib (`argparse`, `sqlite3`, `tomllib`, `json`, `hashlib`).
- **Entry point**: `bewley.cli:main` (registered in `pyproject.toml` as the `bewley` console script).
- **Event-sourced**: Every mutation appends a JSON event to `.bewley/events/`. The SQLite database (`bewley.sqlite`) is a projection that can be rebuilt from events at any time.
- **Content-addressed storage**: Document revisions stored by SHA-256 in `.bewley/objects/documents/`.

## Key commands

```
bewley init / status / fsck / rebuild-index
bewley add / update / list documents / show document
bewley code create / list / show / rename / alias / merge / split
bewley annotate apply / remove / show / resolve
bewley query / show snippets
bewley export snippets / quotes / html / document-html
bewley history / undo
```

## Build & test

```bash
pip install -e .              # install in editable mode
python -m pytest tests/       # run tests (single smoke test)
python -m unittest tests/test_smoke.py  # alternative
```

No external test dependencies — tests use `unittest` and `tempfile`.

## Project layout

```
src/bewley/
  __init__.py       # version only
  __main__.py       # delegates to cli.main()
  cli.py            # entire implementation
tests/
  test_smoke.py     # end-to-end smoke test
examples/upwork/    # example scripts for bootstrapping projects and reports
SPEC.md             # detailed specification (event types, data model, query semantics)
README.md           # user-facing docs with tutorial and command reference
```

## Important conventions

- The event log is append-only; undo emits compensating events, never deletes.
- Annotation relocation across revisions is best-effort; uncertain cases become `conflicted` rather than silently guessing.
- SQLite is a cache, not the source of truth. `rebuild-index` must always recover from events alone.
- Write lock (`write.lock`) enforces single-writer concurrency.
- All file writes use atomic rename (`os.replace`) for crash safety.
