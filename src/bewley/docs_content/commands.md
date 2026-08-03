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
| `bewley version` | Report the installed build plus envelope/agent schema versions. |
| `bewley guide` | Describe the complete lifecycle and the external ep-run execution boundary. |
| `bewley next` | Return the single highest-priority next action from artifact state. |
| `bewley docs list` | List embedded documentation topics. |
| `bewley docs show <topic>` | Show one embedded documentation topic. |
| `bewley docs search <query>` | Search across the embedded documentation. |
| `bewley study set [--method M] [--unit U] [--purpose P]` | Declare or update the study design (partial updates allowed). Suggested methods: grounded-theory, reflexive-ta, content-analysis, framework. |
| `bewley study show` | Show the study manifest and recorded research questions. |
| `bewley question add "<text>"` | Record a research question. Prints the `question_id`. |
| `bewley question list` | List recorded research questions. |
| `bewley case create <name> [--type T] [--description D]` | Create a case (person, organization, site, event). Prints the `case_id`. |
| `bewley case list` | List active cases with linked-document and attribute counts. |
| `bewley case show <ref>` | Show a case: attributes and linked documents. |
| `bewley case set <ref> <attribute> [<value>] [--special S]` | Set a typed attribute value, or an explicit special state: missing, unknown, not_applicable, confidential. |
| `bewley case link <ref> <doc> --as <relationship>` | Link a case to a document as author, participant, subject, site, or other. |
| `bewley attribute define <name> --type T [--values V1,V2]` | Define a project-wide typed attribute (text, number, boolean, date, categorical). |
| `bewley attribute list` | List attribute definitions and how many cases carry each. |
| `bewley link add <kind:ref> <kind:ref> --rel R [--memo M]` | Create a typed link between research entities; allowed combinations are validated. |
| `bewley link list [--entity kind:ref]` | List active entity links, including code-to-code links. |
| `bewley link remove <link_id>` | Deactivate an entity link (compensating event). |
| `bewley speakers detect <doc> [--label L ...]` | Segment a transcript into speaker turns. Default rule: ALL-CAPS labels at line starts (`INTERVIEWER:`); pass `--label` explicitly for mixed-case transcripts. Rerun after `bewley update`. |
| `bewley speakers list <doc>` | Show a document's speakers: turns, share of text, role, linked case. |
| `bewley speakers set-role <label> <role>` | Assign `interviewer`, `participant`, or `other` to a label, project-wide. Span annotations then carry a `speaker_scope`, and interviewer-only spans are refused without `--allow-interviewer`. |
| `bewley speakers link-case <doc> <label> <case>` | Record whose voice a document's speaker is (entity link speaker → case). |
| `bewley example list` | List the example corpora bundled with the installed package. |
| `bewley example fetch <name> [--dest DIR]` | Write a bundled example corpus (documents, README, license) into a new local directory. |
| `bewley fsck` | Verify integrity of events, objects, and index. Prints "ok" or problems to stderr. |
| `bewley rebuild-index` | Rebuild the SQLite index from the event log. |
| `bewley project pack --output <file.bewley>` | Create a portable, integrity-checked project bundle. Refuses to overwrite or pack a project that fails `fsck`. |
| `bewley project unpack <file.bewley> --dest <new-dir>` | Validate and restore a bundle into a new directory, rebuild its index, and run integrity checks. Never merges or overwrites. |
| `bewley capabilities` | Describe the versioned agent interface and bundled schemas. |
| `bewley agent status` | Return phase-aware, executable next actions with safety metadata. |
| `bewley agent schema <name>` | Return the `envelope`, `action`, or `agent-status` schema. |

## Document management

| Command | Purpose |
|---|---|
| `bewley add <path>` | Add a UTF-8 file as a new document. Prints the new `document_id`. |
| `bewley add-audio <path> [--output F] [--model M]` | Transcribe audio via the OpenAI API (external paid call; envelope carries a cost warning) and add the transcript. |
| `bewley add-video <path> [--output F] [--model M]` | Extract audio from video, transcribe it (external paid call), and add the transcript. |
| `bewley import survey-csv <file> --transcript-column C [--feedback-column C] [--format auto\|json\|python\|plain] [--output-dir D] [--dry-run]` | Import one CSV row per document; safely flatten serialized role/content turns, exclude unselected columns, segment speakers, assign roles, and record source provenance. Refuses to overwrite an existing output directory. |
| `bewley update <path>` | Create a new revision of an existing document. Prints `revision_id` or "no-op". |
| `bewley list documents` | List all documents as JSON (document_id, path, revision_count). |
| `bewley list codes [--tree]` | List all codes. Alias for `bewley code list`. |
| `bewley show document <ref>` | Show metadata, revisions, and annotations for a document. |
| `bewley show audio <ref>` | Show the stored transcription provenance for an audio-derived document. |
| `bewley show video <ref>` | Show the stored transcription provenance for a video-derived document. |

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
| `bewley code update <ref> [--description D] [--inclusion I] [--exclusion E]` | Update a code's definition and its inclusion/exclusion criteria — when the code applies, when it does not, and what to use instead. |
| `bewley code lint` | Flag codebook quality problems (missing definitions or criteria, definitions that restate the name, unused codes, heavily-used codes without memos, near-duplicate names). Flags, never fixes. |
| `bewley codebook release <name>` | Freeze the current structured codebook as a named, immutable snapshot (event-recorded). |
| `bewley codebook diff <from> <to>` | Compare two releases: codes added, removed, and changed (definition, criteria, parent). |
| `bewley codebook consolidate jobs [--output F] [--batch-size N] [--model M]` | Package active codes, counts, definitions, and representative evidence as external EDSL Jobs. Proposals are fingerprinted against the current codebook. |
| `bewley codebook consolidate ingest <results.ep> [--jobs F]` | Validate consolidation Results and write a reviewable merge-proposal CSV. Conflicting, incomplete, or invented-id proposals fail closed. |
| `bewley codebook consolidate candidates` | List proposed source→target merges with confidence, rationale, evidence IDs, and recorded decisions. |
| `bewley codebook consolidate review <id> --decision accept\|reject [--reason R]` | Record an append-only human decision for one merge proposal; `--all-remaining` is supported. |
| `bewley codebook consolidate apply [--dry-run]` | Preview or apply fully reviewed merges through ordinary `code_merged` events. Refuses stale codebook fingerprints and undecided proposals. |
| `bewley codebook focused framework-jobs [--min-focused N] [--max-focused N] [--model M]` | Package one global EDSL job that constructs a fixed second-cycle framework from the complete compact open-code inventory. |
| `bewley codebook focused framework-ingest <results.ep> [--jobs F]` | Validate the global theme/focused-code framework and preserve it with an append-only ingest sidecar. |
| `bewley codebook focused mapping-jobs [--framework F] [--batch-size N] [--model M]` | Package batched exhaustive mappings against the same fixed global framework. Model execution remains external. |
| `bewley codebook focused mapping-ingest <results.ep> [--jobs F] [--framework F]` | Require every active open code exactly once, reject unknown focused keys, and write an audited crosswalk CSV. |
| `bewley codebook focused apply [--dry-run]` | Create theme → focused → open-code hierarchy events without deleting or moving original annotations. Refuses stale, incomplete, conflicting, or previously applied inputs. |

## Rapid insights

| Command | Purpose |
|---|---|
| `bewley insights jobs [--output F] [--model M]` | Package one EDSL job per document using only respondent turns from the interview body. Excludes interviewer prompts and the marked AI-interviewer feedback section; uses the applied focused codebook as a fixed theme framework. |
| `bewley insights ingest <results.ep> [--jobs F]` | Validate per-response summaries, sentiment toward AI at work, focused-theme IDs, and verbatim standout quotes before writing JSONL plus an append-only evidence log. |
| `bewley insights export [--input F] [--output F] [--title T]` | Export a standalone dashboard with sentiment distribution, cross-respondent theme prevalence, response summaries, standout quotes, and downloadable JSON. |
| `bewley insights discover jobs [--output F] [--seed N] [--bundle-size N] [--coverage N] [--model M]` | Package reproducibly shuffled bundles of short feedback responses for recurring-code discovery, preserving source IDs and a corpus fingerprint. |
| `bewley insights discover ingest <results.ep> [--jobs F]` | Require every bundle result and validate candidate definitions plus exact evidence from at least two distinct responses before writing an auditable JSONL artifact. |
| `bewley insights consolidate jobs [--candidates F] [--min-codes N] [--max-codes N] [--model M]` | Package all evidence-valid discovery candidates and an optional JSON ModelList for one global compact-codebook job. |
| `bewley insights consolidate ingest <results.ep> [--jobs F]` | Validate themes, code definitions, and criteria, then freeze a fingerprinted semantic codebook; exhaustive bookkeeping belongs to the later classification stage. |
| `bewley insights classify jobs [--codebook F] [--output F] [--model M]` | Package one fixed-codebook classification job per response with the frozen fingerprint and an optional JSON ModelList. |
| `bewley insights classify ingest <results.ep> [--jobs F] [--codebook F]` | Require every response exactly once and validate code IDs, codebook fingerprint, sentiment, and exact evidence spans. |
| `bewley insights aggregate [--classifications F] [--codebook F] [--output F]` | Compute deterministic response-level code, theme, and sentiment counts; never accepts model-supplied totals. |
| `bewley insights evidence-export [--aggregate F] [--classifications F] [--codebook F] [--output F]` | Export a standalone explorer with deterministic tables, exact quotes, codebook criteria, response evidence, and explicit coverage gaps. |

## Annotations

| Command | Purpose |
|---|---|
| `bewley annotate apply <code> <doc> (--document \| --bytes S:E \| --lines S:E \| --quote "text" [--occurrence N] \| --turn N) [--allow-interviewer] [--memo M]` | Apply a code to a document or text span. **One scope flag is mandatory** — see note below. `--quote` anchors by the exact text itself (verbatim match or the command fails; multiple occurrences require `--occurrence`, 1-based) and is the least error-prone scope. `--turn` anchors a whole speaker turn in a segmented transcript. Spans in segmented documents carry a `speaker_scope`; interviewer-only spans are refused without `--allow-interviewer`. The envelope echoes the annotated text for verification. |
| `bewley annotate remove <annotation_id>` | Deactivate an annotation. |
| `bewley annotate show <annotation_id>` | Show annotation details and the annotated text. |
| `bewley annotate resolve <annotation_id> --bytes S:E [--memo M]` | Fix a conflicted annotation after a document revision update. |
| `bewley show snippets --code <ref>` | Show text content of all annotations for a code. |

> **`annotate apply` requires exactly one scope flag.** You must pass one of `--document`, `--bytes S:E`, `--lines S:E`, or `--quote "text"`. Omitting all of them produces an `INVALID_INPUT` error envelope and exit code 1. Prefer `--quote`: byte offsets are UTF-8-sensitive and line counts drift when documents are re-wrapped, while a quote either matches verbatim or fails loudly (`QUOTE_NOT_FOUND` / `AMBIGUOUS_QUOTE` with the occurrences listed). When generating batch annotation scripts, always include the scope flag and check exit codes:
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
| `bewley export html [--output F] [--title T]` | Standalone interactive explorer with full-text search and highlighting, document/scope/status/memo filters, code definitions, prevalence and coverage analytics, proximity relationships, document density, and filtered JSON download. |
| `bewley export document-html <ref> [--output F] [--title T]` | Single document with inline highlights as HTML. |
| `bewley export plots [--output-dir DIR]` | Accessible SVGs: code prevalence, coding density, code co-occurrence, code × document matrix, code-discovery curve, review outcomes, in-document annotation positions, and codebook evolution, plus the underlying JSON manifest. The review-outcomes plot is written only when open-coding sidecar logs (`ingest_log.jsonl`/`apply_log.jsonl`) exist. |
| `bewley export theory [--format json\|mermaid] [--output F]` | Code hierarchy + links as JSON or Mermaid diagram. |
| `bewley export narrative [--output F]` | Integrative narrative summary. |

## Open coding

| Command | Purpose |
|---|---|
| `bewley open-coding jobs [--output jobs.ep] [--summary F] [--pilot N] [--model M] [--max-tokens N] [--from-failures R --jobs J]` | Package current document revisions as EDSL Jobs; with `--model`, also write models.ep so the suggested `ep run` is executable verbatim; with `--from-failures`, repackage only scenarios lacking a valid answer. |
| `ep run jobs.ep --model M --output results.ep` | Execute the package using the EDSL `ep` CLI. |
| `bewley open-coding ingest results.ep [retry.ep ...] [--jobs jobs.ep] [--output F] [--allow-partial]` | Audit coverage (scenarios × models) across one or more Results files, merging retries by stable identity with per-row source attribution; resolve exact quotes and write a reviewable candidate-code CSV with unresolved quotes itemized. |
| `bewley open-coding candidates [--input F]` | List the proposed candidate codes awaiting review, with any recorded decisions; `--human` renders the review queue as a table. |
| `bewley open-coding review (<candidate-id> \| --all-remaining) --decision accept\|reject\|map\|adjust [--reason R] [--to CODE] [--bytes S:E] [--input F]` | Record a review decision as an event: who decided, what, and why enter the audit trail. `map` applies the candidate as a different code; `adjust` overrides its byte span (including repairing a non-exact resolution). Candidate ids accept a unique prefix. |
| `bewley open-coding apply [--input F] [--dry-run]` | Execute the recorded review decisions: accepted/mapped/adjusted candidates become codes and exact-span annotations; rejected ones are skipped with their reasons; undecided ones are itemized, fail-closed. With no recorded decisions, rows present in the CSV are treated as accepted (legacy review-by-deletion) with a warning. |

## Codegen (legacy and visualization)

`bewley codegen` emits standalone Python scripts that perform
rendering steps outside the core CLI. Generated scripts hardcode project paths
and depend only on the Python stdlib.

| Command | Purpose |
|---|---|
| `bewley codegen theory-explorer [--output F] [--html-output F] [--title T]` | Emit a script that renders an interactive D3 theory explorer HTML — force-directed graph, category/document/count filters, click a node for quotes and links. Regenerate when codes or annotations change. |
| `bewley codegen makefile --workflow feedback-insights [--output F] [--model M] [--seed N] [--bundle-size N] [--coverage N] [--force]` | Generate a project-specific executable Make runbook with explicit paid `ep run` targets and no automatic cross-boundary `all` target. |

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
