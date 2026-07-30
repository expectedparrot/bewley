# Bewley Overview

Bewley is a local-first CLI for qualitative coding of text corpora — interview transcripts, field notes, open-ended survey responses, speeches, articles, or any UTF-8 text. Think `git` for qualitative research: append-only event log, immutable document revisions, rebuildable SQLite index.

## When to use bewley

- Systematic thematic or grounded theory analysis of text data
- Coding and annotating interview transcripts
- Content analysis of any text corpus
- Multi-pass iterative coding with code refinement, merging, and splitting
- Tracking analytic memos and the provenance of coding decisions

## Core data model

| Concept | What it is |
|---|---|
| **Project** | A directory with `.bewley/` metadata and a `corpus/` folder |
| **Document** | A UTF-8 text file added to the corpus |
| **Code** | A named analytic label. Codes may occupy an `open`, `focused`, or `theme` layer in a second-cycle hierarchy. |
| **Annotation** | A code applied to a whole document or a text span |
| **Memo** | A free-text analytic note attached to a code, document, or the project |
| **Event log** | Append-only JSON log in `.bewley/events/` — the source of truth |

## Design principles

1. **No silent data loss** — all mutations are events; undo appends compensating events, never deletes history
2. **Text-first** — UTF-8 documents, byte/line span anchors
3. **Full provenance** — every annotation and code change is logged with a timestamp
4. **Rebuildable derived state** — SQLite is a cache; `bewley rebuild-index` recovers from events alone
5. **Portable projects** — `bewley project pack` creates a versioned `.bewley`
   bundle with a hashed manifest; `project unpack` validates it, restores into
   a new directory, rebuilds SQLite, and runs the integrity checker
6. **Layered interpretation** — focused coding adds theme and focused-code
   parents while preserving first-cycle open codes and their annotations as
   provenance

## Output format

By default, every Bewley command writes exactly one versioned JSON envelope to
stdout. Use `--human`/`-H` for human-readable text.

- Success: `{"schema_version":"2.0","status":"ok","command":"bewley ...","argv":[...],"data":...,"warnings":[],"errors":[],"next_steps":[]}`
- Failure: `{"schema_version":"2.0","status":"error","command":"bewley ...","argv":[...],"data":{},"warnings":[],"errors":[{"code":"...","message":"...","context":{}}],"next_steps":[]}`
- `command` is the actual invoked argv array.
- `next_steps` contain argv arrays plus mutation, network, and approval metadata.
- Exit code is zero on success and nonzero on failure.

Agents should branch on `status`, never infer success from `data`, and inspect an
action's safety fields before executing it. Run `bewley capabilities`,
`bewley agent status`, and `bewley agent schema envelope` to discover the
interface and bundled versioned schemas.

## Key constraints

- **No `--cwd` flag**: bewley discovers its project by looking for `.bewley/` in the current directory. Always `cd` to the project directory before running any command.
- **Annotation scope is mandatory**: `bewley annotate apply` requires exactly one of `--document`, `--bytes S:E`, or `--lines S:E`.
- **References**: document_ref, code_ref accept UUIDs, names, paths, or path prefixes.

## Next steps

- `bewley docs show getting-started` — Installation and first workflow
- `bewley docs show workflow` — Phases and checklist
- `bewley docs show commands` — Full command reference
