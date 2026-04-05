# Qualitative Analysis with Bewley

This document is for AI agents performing qualitative coding of text corpora using the `bewley` CLI. It covers installation, the command model, common workflows, and how to get help.

## Installation

```bash
pip install git+https://github.com/expectedparrot/bewley.git
```

After installation the `bewley` command is available on `$PATH`.

Verify with:

```bash
bewley --help
```

## Core concepts

- **Project**: A directory containing a `corpus/` folder and a `.bewley/` metadata directory. Created with `bewley init`.
- **Document**: A UTF-8 text file tracked by bewley (e.g., an interview transcript).
- **Code**: A named analytic label applied to text (e.g., "trust", "themes/rapport").
- **Annotation**: An application of a code to a whole document or a byte/line span within a document revision.
- **Memo**: A free-text analytic note attached to a code, document, or the project.
- **Event log**: Append-only JSON log in `.bewley/events/`. This is the source of truth. SQLite is a rebuildable cache.

References (document_ref, code_ref) accept UUIDs, names, paths, or path prefixes.

## Getting help

Every command and subcommand has detailed `--help` output including description, argument semantics, output format, and examples:

```bash
bewley --help                        # top-level overview with quick-start examples
bewley <command> --help              # e.g., bewley code --help
bewley <command> <subcommand> --help # e.g., bewley annotate apply --help
```

When unsure about a command's arguments or output format, always run `--help` first.

## Command reference

### Project management

| Command | Purpose |
|---|---|
| `bewley init` | Create a new project in the current directory. |
| `bewley status` | Print tab-separated counts: documents, revisions, codes, active_annotations, conflicted_annotations. |
| `bewley fsck` | Verify integrity of events, objects, and index. Prints "ok" or problems to stderr. |
| `bewley rebuild-index` | Rebuild the SQLite index from the event log. |

### Document management

| Command | Purpose |
|---|---|
| `bewley add <path>` | Add a UTF-8 file as a new document. Prints the new `document_id`. |
| `bewley update <path>` | Create a new revision of an existing document. Prints `revision_id` or "no-op". |
| `bewley list documents` | List all documents (tab-separated: document_id, path, revision_count). |
| `bewley show document <ref>` | Show metadata, revisions, and annotations for a document. |

### Code management

| Command | Purpose |
|---|---|
| `bewley code create <name> [--description D] [--color C]` | Create a new code. Prints the `code_id`. Names may contain slashes (e.g., `themes/trust`). |
| `bewley code list [--tree]` | List all codes (tab-separated: code_id, name, annotation_count). `--tree` shows hierarchy. |
| `bewley code show <ref>` | Show details of a code: metadata, aliases, annotations. |
| `bewley code rename <old> <new>` | Rename a code. Annotations follow automatically. |
| `bewley code alias <ref> <alias>` | Add an alternative name for a code. |
| `bewley code merge <sources...> --into <target>` | Merge source codes into target. Sources are deactivated. |
| `bewley code split <source> --new <name> --annotation <id> [--annotation <id>...]` | Move selected annotations into a new code. |
| `bewley code set-parent <child> <parent>` | Set a parent-child relationship in the code hierarchy. |
| `bewley code clear-parent <ref>` | Remove a code from its parent. |
| `bewley code link <source> <target> <relationship> [--memo M]` | Create a named relationship between two codes. |
| `bewley code links [<ref>]` | List code-to-code links (optionally filtered). |
| `bewley code unlink <link_id>` | Remove a code link. |
| `bewley code set-core <ref>` | Designate a code as the core category (grounded theory). |
| `bewley code show-core` | Show the current core category. |

### Annotations

| Command | Purpose |
|---|---|
| `bewley annotate apply <code> <doc> (--document \| --bytes S:E \| --lines S:E) [--memo M]` | Apply a code to a document or text span. Prints the `annotation_id`. |
| `bewley annotate remove <annotation_id>` | Deactivate an annotation. |
| `bewley annotate show <annotation_id>` | Show annotation details and the annotated text. |
| `bewley annotate resolve <annotation_id> --bytes S:E [--memo M]` | Fix a conflicted annotation after a document revision update. |
| `bewley show snippets --code <ref>` | Show text content of all annotations for a code. |

### Querying

```bash
bewley query '<expr>' [--mode document|annotation]
```

Boolean expression syntax:
- `code_name` — matches documents/annotations with this code
- `A & B` — AND
- `A | B` — OR
- `!A` — NOT
- `(A & B) | C` — parentheses for grouping

Default mode is `document`. Use `--mode annotation` for individual annotation results.

### Export

| Command | Purpose |
|---|---|
| `bewley export snippets --code <ref> --format jsonl\|text [--context-lines N]` | Export annotated text snippets. |
| `bewley export quotes (--code <ref> \| --query '<expr>') --format jsonl\|text [--context-lines N]` | Export quotes filtered by code or query. |
| `bewley export html [--output F] [--title T]` | All codes and annotations as standalone HTML. |
| `bewley export document-html <ref> [--output F] [--title T]` | Single document with inline highlights as HTML. |
| `bewley export theory [--format json\|mermaid] [--output F]` | Code hierarchy + links as JSON or Mermaid diagram. |
| `bewley export narrative [--output F]` | Integrative narrative summary. |

### Memos

| Command | Purpose |
|---|---|
| `bewley memo add [--code C \| --document D] [--title T] [content]` | Create a memo. Omit content to open `$EDITOR`. Prints the `memo_id`. |
| `bewley memo list [--code C \| --document D]` | List memos (optionally filtered). |
| `bewley memo show <memo_id>` | Show full memo content. |
| `bewley memo edit <memo_id>` | Edit a memo in `$EDITOR`. |
| `bewley memo delete <memo_id>` | Delete a memo. |

### History and undo

| Command | Purpose |
|---|---|
| `bewley history [--document D] [--code C] [--annotation A]` | Show event log (optionally filtered). |
| `bewley undo <event_id>` | Emit a compensating event to reverse a prior operation. |

## Typical agent workflow

1. **Initialize**: `bewley init`
2. **Add documents**: `bewley add <path>` for each transcript/text file
3. **Summarize corpus**: Read all documents, write `qualitative-analysis/corpus_summary.md`
4. **Generate candidate codes**: Run `python qualitative-analysis/generate_candidate_codes.py`
5. **Refine codes**: Review `candidate_codes.csv`, deduplicate, then `bewley code create` for each
6. **Annotate**: `bewley annotate apply <code> <doc> --lines S:E` to code spans
7. **Write memos**: `bewley memo add --code <ref> 'Analytical note...'`
8. **Query and review**: `bewley query '<expr>'` and `bewley show snippets --code <ref>`
9. **Build hierarchy**: `bewley code set-parent` and `bewley code link`
10. **Export**: `bewley export snippets`, `bewley export theory`, etc.

## Open coding with EDSL

Bewley includes a script for generating candidate qualitative codes using EDSL (Expected Parrot's domain-specific language for LLM surveys). This automates the initial open coding pass.

### Prerequisites

```bash
pip install git+https://github.com/expectedparrot/bewley.git
pip install edsl
```

### Step 1: Create the corpus summary (agent task)

Before generating codes, the agent should read all documents and write a corpus summary:

1. Run `bewley list documents` to get all document paths.
2. Read each document from the `corpus/` directory.
3. Write `qualitative-analysis/corpus_summary.md` with:
   - What kind of texts the corpus contains (interviews, field notes, etc.)
   - Approximate size and scope (number of documents, topics covered)
   - Initial impressions of recurring themes or notable features
   - Any contextual information about the research setting

This summary provides shared context so the LLM can generate codes that are coherent across the whole corpus, not just locally relevant to each document.

### Step 2: Generate candidate codes

```bash
python qualitative-analysis/generate_candidate_codes.py
```

Options:
- `--project-dir DIR` — path to the bewley project (default: current directory)
- `--summary FILE` — path to corpus summary (default: `qualitative-analysis/corpus_summary.md`)
- `--output FILE` — output CSV (default: `qualitative-analysis/candidate_codes.csv`)
- `--model MODEL` — EDSL model name (e.g., `claude-3-5-sonnet-20241022`)

The script:
1. Reads the corpus summary and all documents
2. Creates an EDSL `ScenarioList` with one `Scenario` per document containing:
   - `document_id` — the bewley document ID
   - `document_path` — path in the corpus
   - `document_text` — full document text
   - `corpus_summary` — the summary from step 1
3. Runs a `Survey` with two questions:
   - `candidate_codes` (QuestionList) — asks for code names
   - `code_descriptions` (QuestionFreeText, piped) — asks for a description of each code
4. Saves results to `candidate_codes.csv` with columns:
   `code_name`, `description`, `source_document_id`, `source_document_path`

### Step 3: Refine and apply codes

After reviewing `candidate_codes.csv`:

1. **Deduplicate**: Merge near-synonyms (e.g., `trust_building` and `building_trust`)
2. **Create codes in bewley**:
   ```bash
   bewley code create trust_building --description "Instances where participants describe developing trust"
   ```
3. **Organize hierarchy**: Group related codes under parents
   ```bash
   bewley code set-parent trust_building interpersonal_dynamics
   ```

## Output conventions

- Most listing commands produce **tab-separated** output (parseable with `cut`, `awk`, or by splitting on `\t`).
- Mutating commands print the **ID of the created or affected entity** (document_id, code_id, annotation_id, memo_id, or event_id).
- Errors are printed to **stderr**. Exit code is **0** on success, **1** on failure.

## Important notes for agents

- All IDs are UUIDs. Capture and reuse them from command output.
- The `--lines` flag uses **1-based inclusive** ranges (e.g., `--lines 10:20` means lines 10 through 20). The `--bytes` flag uses **0-based, exclusive-end** ranges.
- Document updates may cause annotations to become `conflicted` if fuzzy relocation fails. Check `bewley status` for `conflicted_annotations > 0` and resolve with `bewley annotate resolve`.
- The event log is append-only. `bewley undo` does not delete history; it appends a compensating event.
- `bewley rebuild-index` can recover from any index corruption since SQLite is a cache, not the source of truth.
- When passing query expressions containing `&`, `|`, `!`, or parentheses, **always quote the expression** to prevent shell interpretation.
