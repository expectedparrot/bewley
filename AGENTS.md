# Bewley repository operating contract

Use the CLI as the workflow source of truth:

```bash
bewley guide
bewley next
```

Run `bewley next` after every material stage. Use the returned recommendation;
use `--help` for exact options and defaults.

## Development checks

- Install development dependencies with `pip install -e .`.
- Run `python -m compileall -q src` and `PYTHONPATH=src python -m pytest tests/`.
- The docs↔CLI cross-check (`tests/test_contract_sync.py`) must stay green:
  update `src/bewley/docs_content/commands.md` when adding or renaming
  commands.
- Code layout: `src/bewley/commands/` holds one module per command group over
  the `project.py` library; `cli.py` is assembly only. Add new helpers to the
  owning module, not to `cli.py`.

## External execution boundary

- Bewley constructs and verifies `.ep` Jobs/ModelList packages
  (`open-coding jobs`) and consumes Results (`open-coding ingest`).
- Never add a Bewley wrapper that executes packaged model calls; the `ep run`
  step is external and requires user approval before a paid model is selected.
- `add-audio` / `add-video` call the OpenAI transcription API directly; they
  are paid external calls and require user approval.
- Preserve `jobs.ep`, `models.ep`, every `results.ep`, and the candidate-code
  CSV as run evidence.

## Authentication and private material

- Let EDSL own Expected Parrot authentication (`ep auth login`,
  `ep profiles current`, `ep check`). `OPENAI_API_KEY` is read from the
  environment for transcription only.
- Never print, copy, serialize, log, or commit API keys.
- Never publish `.env`, respondent-identifying transcripts, or confidential
  interview material.

## Artifacts and audit trail

- `.bewley/` is the durable project store. The append-only event log is the
  source of truth; SQLite is a rebuildable projection (`rebuild-index`).
- Mutate project state only through CLI commands so history stays auditable;
  undo is a compensating event, never a deletion.
- Never silently repair, normalize, replace, or delete registered events,
  revisions, or results. Annotation relocation that cannot be established
  confidently becomes `conflicted`, not a silent best guess.
- Run `bewley fsck` before handing artifacts downstream.

## Envelope contract

- Every command emits one JSON envelope (`schema_version` 2.0):
  `status` / `command` / `argv` / `data` / `warnings` / `errors` /
  `next_steps`. Failures exit nonzero.
- Error codes and bundled schemas are API surface; keep them backward
  compatible or provide an explicit migration.
