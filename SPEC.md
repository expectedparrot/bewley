# Bewley Specification

## 1. Purpose

`bewley` is a local-first command-line application for coding qualitative interview data and other UTF-8 text corpora.

The system is designed around four principles:

1. No silent data loss.
2. Text-first workflows.
3. Full provenance for coding actions.
4. Rebuildable derived state.

`bewley` is not a collaborative server in v1. It is a local project tool with strong history and recovery guarantees similar in spirit to `git`.

## 2. Goals

### 2.1 Functional goals

- Initialize a project.
- Add arbitrary UTF-8 text documents to a corpus.
- Track document revisions over time.
- Define codes and apply them to whole documents or text spans.
- Query coded data by code expression.
- Show all snippets associated with one or more codes.
- Rename, merge, and split codes without destroying provenance.
- Inspect history and recover from mistakes.

### 2.2 Non-goals for v1

- Multi-user sync.
- Rich GUI editing.
- Binary file support.
- Full branch/merge workflows.
- Statistical analysis features beyond basic exports.

## 3. Terminology

- Project: A directory containing a `corpus/` directory and a `.bewley/` metadata directory.
- Corpus: The set of user documents managed by the project.
- Document: A UTF-8 text file in the corpus.
- Document revision: An immutable snapshot of one document's content.
- Code: A named analytic label.
- Annotation: An application of a code to either a whole document or a span in a specific document revision.
- Anchor: The data used to locate an annotation in a document revision and attempt relocation across revisions.
- Event: An immutable record of a state-changing operation.
- Projection: Derived state rebuilt from events, typically stored in SQLite.

## 4. Core Invariants

### 4.1 Safety invariants

- Raw event history is append-only.
- Historical document revisions are immutable.
- A user operation never rewrites or deletes prior events.
- Undo is represented as a new event, not by erasing history.
- Derived indexes may be discarded and rebuilt at any time.

### 4.2 Identity invariants

- `document_id` is the stable identity of a logical document.
- `revision_id` is the stable identity of an immutable document snapshot.
- `annotation_id` is the stable identity of a coded item.
- Byte offsets are positional locators, not durable identities.

### 4.3 Integrity invariants

- Every document revision is content-addressed by SHA-256 of its raw bytes.
- Every event has a unique event id and integrity hash.
- All references from projections must be reproducible from the event log.

## 5. Project Layout

```text
project/
  corpus/
    ...
  .bewley/
    config.toml
    HEAD
    events/
      000000000001.json
      000000000002.json
      ...
    objects/
      documents/
        <revision_sha256>
    refs/
      codes/
      documents/
    index/
      bewley.sqlite
    locks/
      write.lock
    logs/
      rebuild.log
```

## 6. File and Storage Model

### 6.1 `corpus/`

- Holds the user's working text files.
- Files are treated as UTF-8 text.
- Paths are project-relative.
- The path of a document may change over time without changing its `document_id`.

### 6.2 `.bewley/objects/documents/`

- Stores immutable snapshots of document revisions.
- Each file is named by SHA-256 of the raw file bytes.
- The object store is append-only.

### 6.3 `.bewley/events/`

- Stores one JSON event per file.
- Event filenames are monotonically increasing sequence numbers for easy inspection.
- Event payloads contain semantic content; filenames provide convenient ordering only.

### 6.4 `.bewley/index/bewley.sqlite`

- Stores rebuildable projections and query indexes.
- SQLite is the only mutable database in v1.
- If corrupted or deleted, it must be rebuildable from `.bewley/events/` and objects.

### 6.5 `.bewley/config.toml`

Stores:

- project format version
- default query mode
- actor name and email if configured
- text encoding policy
- relocation thresholds

## 7. Document Model

### 7.1 Document

A document is a logical corpus item with stable identity across revisions and optional renames.

Fields:

- `document_id`
- `current_path`
- `created_at`
- `archived_at` nullable

### 7.2 Document revision

A document revision is an immutable snapshot.

Fields:

- `revision_id`
- `document_id`
- `content_sha256`
- `byte_length`
- `line_count`
- `created_at`
- `source_path`
- `parent_revision_id` nullable

Notes:

- `revision_id` may equal the content SHA-256 in v1.
- The parent pointer establishes a linear history for a document.

### 7.3 Add and update behavior

- `bewley add <path>` creates a new `document_id` and initial revision if the path is not already tracked.
- `bewley update <path>` creates a new revision if file content changed.
- Importing unchanged content should be a no-op and should not create a new revision.

## 8. Code Model

### 8.1 Code

A code is a named analytic concept with stable identity independent of its display name.

Fields:

- `code_id`
- `canonical_name`
- `description` nullable
- `color` nullable
- `status` enum: `active`, `merged`, `deprecated`
- `created_at`

### 8.2 Aliases

Codes may have aliases.

Fields:

- `alias_name`
- `code_id`
- `created_at`

### 8.3 Hierarchy

Hierarchy is deferred in v1.

Rationale:

- Merge and split semantics are more important than parent-child taxonomies for initial correctness.
- A future `code_relationships` table can add hierarchy without invalidating annotation history.

## 9. Annotation Model

### 9.1 Unification

Whole-document coding and span coding use one annotation model.

- Document-level annotations use `scope_type = "document"` and null span fields.
- Span-level annotations use `scope_type = "span"` and required span fields.

### 9.2 Annotation fields

- `annotation_id`
- `code_id`
- `document_id`
- `document_revision_id`
- `scope_type` enum: `document`, `span`
- `start_byte` nullable
- `end_byte` nullable
- `start_line` nullable
- `end_line` nullable
- `exact_text` nullable
- `prefix_context` nullable
- `suffix_context` nullable
- `anchor_status` enum: `clean`, `relocated`, `conflicted`
- `created_by_event_id`
- `superseded_by_event_id` nullable
- `memo` nullable
- `created_at`

### 9.3 Span semantics

- Spans are half-open byte ranges: `[start_byte, end_byte)`.
- `start_byte` is inclusive.
- `end_byte` is exclusive.
- For span annotations, `exact_text` must match the bytes in the referenced revision exactly.

### 9.4 Line numbers

- Line numbers are stored for ergonomics and query speed.
- They are derived from the referenced revision content.
- Byte offsets remain the authoritative locator inside one revision.

### 9.5 Annotation identity

- `annotation_id` is the stable identity.
- `document_revision_id + start_byte + end_byte` locates a span in one immutable snapshot.
- The same annotation may later be relocated to newer revisions without changing `annotation_id`.

## 10. Anchoring and Revision Following

### 10.1 Design intent

Annotations should follow later document revisions when possible.

Automatic relocation is best-effort and must never silently claim confidence it does not have.

### 10.2 Anchor payload

Each span annotation stores enough information to relocate:

- `document_revision_id`
- `start_byte`
- `end_byte`
- `exact_text`
- `prefix_context`
- `suffix_context`

### 10.3 Relocation states

- `clean`: The annotation remains valid in the referenced revision without relocation.
- `relocated`: The annotation was mapped to a newer revision automatically with acceptable confidence.
- `conflicted`: The system could not relocate confidently; manual resolution is required.

### 10.4 Relocation algorithm

When a new document revision is created, the system evaluates annotations from the prior current revision in this order:

1. Exact positional carry-forward if the surrounding content is unchanged.
2. Exact match of `exact_text` plus surrounding context.
3. Unique match of `exact_text` within the document when context is still sufficiently consistent.
4. Fuzzy contextual relocation using configurable thresholds.
5. If no unique high-confidence target exists, mark `conflicted`.

### 10.5 Conflict behavior

Conflicted annotations:

- remain attached to the old revision
- are visible in status and query output
- do not disappear
- require an explicit user resolution event

This is the qualitative coding equivalent of a merge conflict.

## 11. Event Log

### 11.1 Event properties

Each state-changing operation emits one immutable event.

Required event fields:

- `event_id`
- `sequence_number`
- `event_type`
- `timestamp`
- `actor`
- `tool_version`
- `payload`
- `event_sha256`
- `parent_event_ids`

### 11.2 Event types

- `project_initialized`
- `document_added`
- `document_moved`
- `document_updated`
- `code_created`
- `code_renamed`
- `code_aliased`
- `code_merged`
- `code_split`
- `annotation_added`
- `annotation_removed`
- `annotation_reanchored`
- `annotation_conflicted`
- `annotation_resolved`
- `memo_attached`
- `config_updated`
- `index_rebuilt`
- `undo_recorded`

### 11.3 Event write guarantees

- Event files are written atomically.
- Sequence numbers are assigned under a project write lock.
- SQLite projection updates occur in a transaction after event append.
- If projection update fails after event append, rebuild must recover consistency.

## 12. Projection Database

`bewley.sqlite` is the query engine and cache, not the source of truth.

### 12.1 Required tables

#### `documents`

- `document_id` primary key
- `current_path`
- `created_at`
- `archived_at`

#### `document_revisions`

- `revision_id` primary key
- `document_id`
- `content_sha256`
- `byte_length`
- `line_count`
- `created_at`
- `source_path`
- `parent_revision_id`
- `is_current`

#### `codes`

- `code_id` primary key
- `canonical_name` unique
- `description`
- `color`
- `status`
- `created_at`

#### `code_aliases`

- `alias_name` primary key
- `code_id`
- `created_at`

#### `annotations`

- `annotation_id` primary key
- `code_id`
- `document_id`
- `document_revision_id`
- `scope_type`
- `start_byte`
- `end_byte`
- `start_line`
- `end_line`
- `exact_text`
- `prefix_context`
- `suffix_context`
- `anchor_status`
- `created_by_event_id`
- `superseded_by_event_id`
- `memo`
- `created_at`
- `is_active`

#### `events`

- `event_id` primary key
- `sequence_number` unique
- `event_type`
- `timestamp`
- `actor`

### 12.2 Suggested indexes

- annotations by `code_id`
- annotations by `document_id`
- annotations by `document_revision_id`
- annotations by `anchor_status`
- code aliases by `code_id`
- revisions by `document_id, is_current`

## 13. CLI Specification

### 13.1 Project lifecycle

```bash
bewley init
bewley status
bewley fsck
bewley rebuild-index
```

Behavior:

- `init` creates `corpus/` and `.bewley/`.
- `status` shows tracked documents, current revision counts, unresolved conflicts, and pending issues.
- `fsck` verifies event hashes, object presence, and projection consistency.
- `rebuild-index` reconstructs SQLite projections from the event log.

### 13.2 Corpus commands

```bash
bewley add <path>
bewley update <path>
bewley list documents
bewley show document <document-ref>
```

Behavior:

- `<document-ref>` may be a path, `document_id`, or unambiguous basename.
- `show document` should display revision history and active annotations.

### 13.3 Code commands

```bash
bewley code create <name>
bewley code list
bewley code show <code-ref>
bewley code rename <old> <new>
bewley code alias <code-ref> <alias>
bewley code merge <source>... --into <target>
bewley code split <source> --new <target> [selection options]
```

Behavior:

- Code names must be unique among canonical names.
- Aliases must be unique across all canonical names and aliases.
- `merge` preserves original code history.
- `split` creates a new code and reassigns selected active annotations by event.

### 13.4 Annotation commands

```bash
bewley annotate apply <code-ref> <document-ref> --document
bewley annotate apply <code-ref> <document-ref> --bytes <start>:<end>
bewley annotate apply <code-ref> <document-ref> --lines <start>:<end>
bewley annotate remove <annotation-id>
bewley annotate show <annotation-id>
bewley annotate resolve <annotation-id> --bytes <start>:<end>
```

Behavior:

- `--document` creates a document-level annotation.
- `--bytes` is exact.
- `--lines` is converted to byte offsets against the current revision.
- `resolve` records a manual conflict resolution event.

### 13.5 Query and browsing commands

```bash
bewley show snippets --code <code-ref>
bewley query "<expr>"
bewley query "<expr>" --mode annotation
bewley query "<expr>" --mode document
bewley export snippets --code <code-ref> --format jsonl
bewley export snippets --code <code-ref> --format jsonl --context-lines 3
bewley export quotes --code <code-ref> --format jsonl --context-lines 3
bewley export quotes --query "<expr>" --format jsonl --context-lines 3
```

## 14. Query Semantics

### 14.1 Supported v1 modes

- `annotation`
- `document`

### 14.2 `document` mode

An expression matches a document if the document contains active annotations satisfying the boolean expression anywhere within the document.

Example:

- `trust AND skepticism` matches if both codes appear somewhere in the same document.

### 14.3 `annotation` mode

An expression matches at the annotation level only when all terms can be satisfied within the same comparable annotation region.

V1 simplification:

- For `annotation` mode, boolean combinations are evaluated over overlapping span annotations or exact document-level annotations on the same document.
- If overlap cannot be established, the result does not match.

This keeps semantics strict and understandable.

### 14.4 Deferred query mode

Windowed proximity queries such as "within N lines" are deferred to a later version.

## 15. Code Refactor Semantics

### 15.1 Rename

- Changes only the canonical display name.
- Does not change `code_id`.
- Is trivially reversible by another rename event.

### 15.2 Merge

- Target code survives.
- Source codes become `merged` or `deprecated`.
- Historical annotations retain original provenance.
- Active query projections may optionally resolve merged sources to the target code for convenience, but provenance must remain inspectable.

### 15.3 Split

- Creates a new target code.
- Requires explicit selection criteria or explicit annotation ids.
- Reassignment is recorded as new events.
- Original history remains available.

### 15.4 Reversibility

The data model is reversible because history is append-only.

V1 CLI requirement:

- direct rename reversal support
- basic history-aware recovery via `undo`

Deferred:

- polished one-command reversal UX for merge and split

## 16. History and Recovery

### 16.1 History

```bash
bewley history
bewley history --document <document-ref>
bewley history --code <code-ref>
bewley history --annotation <annotation-id>
```

### 16.2 Undo

```bash
bewley undo <event-id>
```

Behavior:

- `undo` emits a compensating event when safe.
- Not every event type must support direct undo in v1.
- Unsupported undo attempts should fail explicitly.

### 16.3 Failure recovery

If event append succeeds but projection update fails:

1. The event log remains authoritative.
2. The project status should report index inconsistency.
3. `bewley rebuild-index` must restore queryability.

## 17. Locking and Concurrency

V1 assumes one active writer per project.

Requirements:

- acquire an exclusive write lock before appending events
- fail fast if the lock cannot be acquired
- allow concurrent read-only operations when safe

## 18. Integrity Checking

`bewley fsck` must verify:

- every event file parses
- event hashes match file content
- all referenced revision objects exist
- active projection rows can be regenerated from events
- current document refs are internally consistent

## 19. Export

V1 should support snippet export in `jsonl` and plain text.

V1 should also support quote export in `jsonl` and plain text for span annotations when exact byte provenance matters.

Each exported snippet should include:

- code name
- code id
- document id
- document path
- revision id
- annotation id
- line numbers if available
- selected text
- anchor status

Each exported quote should include:

- code name
- code id
- document id
- document path
- revision id
- annotation id
- `start_byte`
- `end_byte`
- line numbers if available
- exact text
- anchor status
- optional surrounding context when requested

## 20. Error Handling

The CLI must prefer explicit failure over silent coercion.

Examples:

- applying a code to an invalid byte range is an error
- ambiguous document references are an error
- ambiguous code references are an error
- relocation uncertainty results in `conflicted`, not silent best guess

## 21. Deferred Items

These are explicitly out of scope for the first implementation:

- branch and merge workflows across analytic timelines
- hierarchical code trees
- non-UTF-8 text import
- automatic semantic splitting of codes
- advanced proximity query operators
- collaborative sync and conflict resolution across machines

## 22. Recommended Implementation Order

1. Project init, object store, event log, write locking, and status.
2. Document add/update with immutable revisions.
3. Code create/list/show/rename.
4. Span and document-level annotations.
5. Snippet browsing and basic query modes.
6. Automatic annotation relocation and conflict handling.
7. Merge, split, history, undo, and fsck.

## 23. Acceptance Criteria for v1

`bewley` v1 is acceptable when all of the following are true:

- a user can initialize a project and add text documents
- document updates create immutable revisions
- codes can be created and applied to whole documents or spans
- all snippets for a code can be listed
- boolean queries work in `document` mode and basic `annotation` mode
- changed documents trigger relocation or explicit conflicts
- history remains inspectable after rename, merge, and split operations
- index rebuild succeeds from the event log alone
- no supported command silently destroys historical information
