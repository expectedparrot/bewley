# Bewley Command Reference

> **CRITICAL: Always `cd` to the bewley project directory before running any `bewley` command.**
> Bewley has **no `--cwd` flag**. It discovers its project by looking for `.bewley/` in the current directory. If you run bewley from the wrong directory, commands will silently fail or return empty results.
>
> Between Bash tool calls, shell state (including cwd) is not preserved. Always use an absolute path:
> ```bash
> cd /absolute/path/to/data/qualitative-coding && bewley list documents
> ```

All commands emit one JSON envelope by default. `--human`/`-H` is an explicit
presentation mode and should not be used by agents parsing results.

## Contents

- [Project management](#project-management)
- [Document management](#document-management)
- [Code management](#code-management)
- [Annotations](#annotations)
- [Querying](#querying)
- [Export](#export)
- [Codegen](#codegen)
- [Memos](#memos)
- [History and undo](#history-and-undo)

## Project management

| Command | Purpose |
|---|---|
| `bewley init` | Create a new project in the current directory. |
| `bewley status` | Print JSON counts: documents, revisions, codes, active_annotations, conflicted_annotations. |
| `bewley fsck` | Verify integrity of events, objects, and index. Prints "ok" or problems to stderr. |
| `bewley rebuild-index` | Rebuild the SQLite index from the event log. |
| `bewley capabilities` | Describe the versioned agent interface and bundled schemas. |
| `bewley agent status` | Return phase-aware, executable next actions with safety metadata. |
| `bewley agent schema <name>` | Return the `envelope`, `action`, or `agent-status` schema. |

## Document management

| Command | Purpose |
|---|---|
| `bewley add <path>` | Add a UTF-8 file as a new document. Prints the new `document_id`. |
| `bewley update <path>` | Create a new revision of an existing document. Prints `revision_id` or "no-op". |
| `bewley list documents` | List all documents as JSON (document_id, path, revision_count). |
| `bewley list codes [--tree]` | List all codes. Alias for `bewley code list`. |
| `bewley show document <ref>` | Show metadata, revisions, and annotations for a document. |

## Code management

| Command | Purpose |
|---|---|
| `bewley code create <name> [--description D] [--color C]` | Create a new code. Prints the `code_id`. Names may contain slashes (e.g., `themes/trust`). |
| `bewley code list [--tree]` | List all codes as JSON (code_id, name, annotation_count). `--tree` shows hierarchy. |
| `bewley code show <ref>` | Show details of a code: metadata, aliases, annotations. |
| `bewley code coverage <ref> [--breakdown]` | How many respondents a code (and descendants) covers. Pass `--breakdown` to see per-descendant counts — essential for parent categories to avoid being misled by inclusive rollups that hide divergent children. |
| `bewley code rename <old> <new> [--description D]` | Rename a code. Annotations follow automatically. Pass `--description` to update description atomically — recommended after renaming so the description doesn't go stale. |
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

## Annotations

| Command | Purpose |
|---|---|
| `bewley annotate apply <code> <doc> (--document \| --bytes S:E \| --lines S:E) [--memo M]` | Apply a code to a document or text span. **One scope flag is mandatory** — see note below. Prints the `annotation_id`. |
| `bewley annotate remove <annotation_id>` | Deactivate an annotation. |
| `bewley annotate show <annotation_id>` | Show annotation details and the annotated text. |
| `bewley annotate resolve <annotation_id> --bytes S:E [--memo M]` | Fix a conflicted annotation after a document revision update. |
| `bewley show snippets --code <ref>` | Show text content of all annotations for a code. |

> **`annotate apply` requires exactly one scope flag.** You must pass one of `--document`, `--bytes S:E`, or `--lines S:E`. Omitting all three produces `bewley error:` with an empty message and exit code 1. When generating batch annotation scripts, always include the scope flag and check exit codes:
> ```bash
> bewley annotate apply my_code doc_id --lines 5:8 || echo "FAILED: my_code on doc_id"
> ```

## Querying

```bash
bewley query '<expr>' [--mode document|annotation]
```

Boolean expression syntax:
- `code_name` -- matches documents/annotations with this code
- `A & B` -- AND
- `A | B` -- OR
- `!A` -- NOT
- `(A & B) | C` -- parentheses for grouping

Default mode is `document`. Use `--mode annotation` for individual annotation results.

## Export

| Command | Purpose |
|---|---|
| `bewley export snippets --code <ref> --format jsonl\|text [--context-lines N]` | Export annotated text snippets. |
| `bewley export quotes (--code <ref> \| --query '<expr>' \| --all) --format jsonl\|text [--context-lines N]` | Export quotes filtered by code or query, or `--all` to dump every active span annotation in the project. |
| `bewley export html [--output F] [--title T]` | All codes and annotations as standalone HTML. |
| `bewley export document-html <ref> [--output F] [--title T]` | Single document with inline highlights as HTML. |
| `bewley export theory [--format json\|mermaid] [--output F]` | Code hierarchy + links as JSON or Mermaid diagram. |
| `bewley export narrative [--output F]` | Integrative narrative summary. |

## Open coding

| Command | Purpose |
|---|---|
| `bewley open-coding jobs [--output jobs.ep] [--summary F] [--pilot N]` | Package current document revisions and the open-coding prompt as EDSL Jobs. |
| `ep run jobs.ep --model M --output results.ep` | Execute the package using the EDSL `ep` CLI. |
| `bewley open-coding ingest results.ep [--jobs jobs.ep] [--output F]` | Audit coverage and revisions, resolve exact quotes, and write a reviewable candidate-code CSV. |

## Codegen (legacy and visualization)

`bewley codegen` emits standalone Python scripts that perform LLM-assisted or
rendering steps outside the core CLI. Generated scripts hardcode project paths
and depend only on the Python stdlib (plus `edsl` for the open-coding one).

| Command | Purpose |
|---|---|
| `bewley codegen open-coding [--output F] [--summary F] [--csv-output F] [--model M]` | Legacy script workflow. Prefer `bewley open-coding jobs`. |
| `bewley codegen resolve-quotes [--input F] [--output-csv F] [--output F]` | Emit a script that maps candidate quotes to exact byte ranges in their source documents, with a fuzzy fallback cascade (exact → strip surrounding punctuation → case-insensitive) to absorb minor LLM drift. |
| `bewley codegen apply-resolved [--input F] [--output F]` | Emit a script that batch-creates codes and applies annotations from a resolved CSV. Idempotent (existing codes are reused). Supports `--dry-run` to preview commands before executing. |
| `bewley codegen theory-explorer [--output F] [--html-output F] [--title T]` | Emit a script that renders an interactive D3 theory explorer HTML — force-directed graph, category/document/count filters, click a node for quotes and links. Regenerate when codes or annotations change. |

## Memos

| Command | Purpose |
|---|---|
| `bewley memo add [--code C \| --document D] [--title T] [content]` | Create a memo. Omit content to open `$EDITOR`. Prints the `memo_id`. |
| `bewley memo list [--code C \| --document D]` | List memos (optionally filtered). |
| `bewley memo show <memo_id>` | Show full memo content. |
| `bewley memo edit <memo_id>` | Edit a memo in `$EDITOR`. |
| `bewley memo delete <memo_id>` | Delete a memo. |

## History and undo

| Command | Purpose |
|---|---|
| `bewley history [--document D] [--code C] [--annotation A]` | Show event log (optionally filtered). |
| `bewley undo <event_id>` | Emit a compensating event to reverse a prior operation. |
