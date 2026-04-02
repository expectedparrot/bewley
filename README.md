# Bewley

A local-first command-line tool for coding qualitative interview data and other UTF-8 text corpora.

Bewley is built around four principles:

1. **No silent data loss** — every action is recorded as an immutable event; nothing is ever overwritten.
2. **Text-first** — corpora are plain UTF-8 files; no proprietary formats.
3. **Full provenance** — every coding decision is traceable to a specific document revision.
4. **Rebuildable state** — the SQLite index is a cache; it can always be reconstructed from the event log alone.

Bewley is designed for researchers who want a rigorous, inspectable audit trail for their qualitative analysis — closer in spirit to `git` than to a GUI NVivo-style tool.

---

## Installation

Requires Python 3.12+.

```bash
pip install bewley
```

Or install from source:

```bash
git clone https://github.com/expectedparrot/bewley.git
cd bewley
pip install -e .
```

Verify the install:

```bash
bewley --version
```

---

## Core concepts

| Term | Meaning |
|---|---|
| **Project** | A directory containing `corpus/` and `.bewley/` metadata. |
| **Document** | A UTF-8 text file tracked in the corpus. Has a stable identity even if the file is renamed. |
| **Revision** | An immutable snapshot of a document's content, addressed by SHA-256. |
| **Code** | A named analytic label (e.g. `trust`, `friction`, `workaround`). |
| **Annotation** | An application of a code to a whole document or a specific text span. |
| **Event** | An immutable JSON record of every state-changing operation. The source of truth. |
| **Anchor** | Metadata stored with each span annotation so it can be relocated when the document is updated. |

---

## Tutorial

### 1. Initialize a project

```bash
mkdir my-study && cd my-study
bewley init
```

This creates:

```
my-study/
  corpus/          ← put your text files here
  .bewley/         ← metadata, event log, object store, SQLite index
```

### 2. Add documents

Copy or write your interview transcripts into the `corpus/` directory, then track them:

```bash
bewley add corpus/interview-alice.txt
bewley add corpus/interview-bob.txt
```

Check what is tracked:

```bash
bewley status
bewley list documents
```

### 3. Create codes

Define your analytic codes:

```bash
bewley code create trust
bewley code create friction
bewley code create workaround
bewley code list
```

### 4. Apply annotations

**Whole-document annotation** — mark a document as belonging to a theme:

```bash
bewley annotate apply trust corpus/interview-alice.txt --document
```

**Span annotation by line range** — apply a code to specific lines:

```bash
bewley annotate apply friction corpus/interview-alice.txt --lines 14:22
```

**Span annotation by byte offset** (for precision):

```bash
bewley annotate apply workaround corpus/interview-bob.txt --bytes 1024:1280
```

Add an optional memo to any annotation:

```bash
bewley annotate apply trust corpus/interview-alice.txt --lines 5:10 --memo "Explicit trust in the platform despite past issues"
```

### 5. Browse coded data

Show all snippets for a code:

```bash
bewley show snippets --code friction
```

Inspect a specific document's revision history and annotations:

```bash
bewley show document corpus/interview-alice.txt
```

Inspect a specific annotation:

```bash
bewley annotate show <annotation-id>
```

### 6. Query across codes

Boolean queries return documents (or overlapping annotations) that match:

```bash
# Documents that have both codes somewhere in them
bewley query "trust AND friction"

# Documents with one code but not the other
bewley query "workaround AND NOT trust"

# Annotation-level: only where spans actually overlap
bewley query "trust AND friction" --mode annotation
```

Default mode is `document`. Use `--mode annotation` for stricter, co-located matching.

### 7. Manage codes

Rename a code without losing any history:

```bash
bewley code rename workaround coping-strategy
```

Add an alias so old queries still resolve:

```bash
bewley code alias coping-strategy workaround
```

Merge two codes into one:

```bash
bewley code merge trust reliability --into credibility
```

Show a code and all its annotations:

```bash
bewley code show credibility
```

### 8. Update documents

When an interview transcript is corrected or extended, update it in place. Bewley creates a new immutable revision and attempts to relocate all existing annotations automatically:

```bash
# Edit corpus/interview-alice.txt, then:
bewley update corpus/interview-alice.txt
```

If an annotation cannot be relocated with confidence it is marked `conflicted`. Resolve it manually:

```bash
bewley status                                    # shows conflicted annotations
bewley annotate resolve <annotation-id> --lines 18:25
```

### 9. Export

Export snippets for a code as JSONL (with surrounding context lines):

```bash
bewley export snippets --code friction --format jsonl --context-lines 3
```

Export verbatim quotes with byte-exact provenance:

```bash
bewley export quotes --code friction --format jsonl --context-lines 3
```

Export a full interactive HTML code explorer:

```bash
bewley export html --output analysis.html --title "My Study"
```

Export a single annotated document as HTML:

```bash
bewley export document-html corpus/interview-alice.txt --output alice-annotated.html
```

### 10. History and undo

View the full event log:

```bash
bewley history
bewley history --document corpus/interview-alice.txt
bewley history --code friction
```

Undo a specific event (where supported):

```bash
bewley undo <event-id>
```

### 11. Integrity checks

Verify that every event, object, and projection is internally consistent:

```bash
bewley fsck
```

If the SQLite index is ever corrupted or deleted, rebuild it from the event log:

```bash
bewley rebuild-index
```

---

## Project layout

```
my-study/
  corpus/
    interview-alice.txt
    interview-bob.txt
  .bewley/
    config.toml          ← project settings
    HEAD                 ← pointer to latest event
    events/              ← append-only event log (one JSON file per action)
    objects/documents/   ← immutable document snapshots (SHA-256 addressed)
    index/bewley.sqlite  ← rebuildable query index (not the source of truth)
    locks/write.lock     ← prevents concurrent writes
    logs/rebuild.log
```

The `.bewley/` directory is the only thing that needs to be backed up (along with `corpus/`). The SQLite index can always be discarded and rebuilt.

---

## Command reference

```
bewley init
bewley status
bewley fsck
bewley rebuild-index

bewley add <path>
bewley update <path>
bewley list documents
bewley show document <ref>

bewley code create <name> [--description <text>]
bewley code list
bewley code show <ref>
bewley code rename <old> <new>
bewley code alias <ref> <alias>
bewley code merge <source>... --into <target>
bewley code split <source> --new <target>

bewley annotate apply <code> <document> --document [--memo <text>]
bewley annotate apply <code> <document> --lines <start>:<end> [--memo <text>]
bewley annotate apply <code> <document> --bytes <start>:<end> [--memo <text>]
bewley annotate remove <annotation-id>
bewley annotate show <annotation-id>
bewley annotate resolve <annotation-id> --lines <start>:<end>
bewley annotate resolve <annotation-id> --bytes <start>:<end>

bewley show snippets --code <ref>
bewley query "<expr>" [--mode document|annotation]

bewley export snippets --code <ref> --format jsonl [--context-lines N]
bewley export quotes --code <ref> --format jsonl [--context-lines N]
bewley export quotes --query "<expr>" --format jsonl [--context-lines N]
bewley export html --output <file> [--title <text>]
bewley export document-html <ref> --output <file> [--title <text>]

bewley history [--document <ref>] [--code <ref>] [--annotation <id>]
bewley undo <event-id>
```

---

## Design notes

- The event log (`events/`) is append-only. No command ever modifies or deletes prior events.
- Undo is implemented as a new compensating event, not by erasing history.
- Document revisions are content-addressed by SHA-256 and stored in `objects/`. They are immutable.
- The SQLite database is a projection of the event log — a cache, not a database of record.
- One writer at a time is enforced via a file lock. Concurrent reads are safe.
- Annotation relocation across revisions is best-effort; uncertain relocations produce explicit `conflicted` status rather than silent best-guesses.

---

## License

See [LICENSE](LICENSE) if present, or contact the maintainers.
