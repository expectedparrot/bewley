# bewley — qualitative coding CLI for interview transcripts and text corpora
<!-- id: bewley/bewley -->

<p align="center">
  <img src="docs/assets/bewley-package.png" width="760" alt="Bewley: an Expected Parrot perched on a stack of papers, framed by an E and brackets">
</p>

## Copy and paste into Codex or Claude Code

```text
Set up Bewley and help me conduct an auditable qualitative analysis in this
repository.

Install the current Bewley and EDSL `main` branches from GitHub. If `uv` is not
installed, first run `python -m pip install --upgrade uv`. Then install both
CLIs as a managed tool:

uv tool install --upgrade --force \
  --with-executables-from "edsl @ git+https://github.com/expectedparrot/edsl.git@main" \
  "bewley @ git+https://github.com/expectedparrot/bewley.git@main"

Verify that the expected command-line tools are available:

uv tool dir --bin
command -v bewley
command -v ep
bewley --help
ep --help

Confirm that `command -v` resolves both commands inside the directory reported
by `uv tool dir --bin`. If either command resolves to an older installation,
do not use it: run `uv tool update-shell`, verify again, or invoke the commands
by their absolute paths in the uv tool bin directory.

Let the EDSL CLI manage repository-local authentication. Run `ep auth status`.
If authentication is missing, run `ep auth login` and follow its login flow;
do not log in again when the existing `.env` or EDSL profile is already
configured. Never display, copy, or commit API keys. Then run
`ep profiles current` to inspect the redacted configuration and `ep check` to
verify connectivity before any paid model execution.

Once setup succeeds, run:

bewley agent status

Treat Bewley's JSON envelope as the source of truth. Follow its
`next_actions`, checking each action's mutation, network, and approval
implications against my authorization. After each action, rerun
`bewley agent status` and continue until the analysis is complete or my input
or approval is required. Start with a small pilot corpus, preserve exact source
provenance, show me candidate codes before creating or applying them, and ask
before selecting a paid model or running packaged EDSL jobs.
```

bewley is a local-first qualitative analysis workspace for coding interview transcripts, documents, audio transcripts, and other text corpora. The agent helps the user define a codebook, import source material, annotate spans, query coded evidence, and export snippets, quotes, diagrams, narratives, or HTML reports from the append-only project state.

Read the [HTML tutorial](docs/index.html). Bewley is released under the [MIT License](LICENSE).

## Agent interface

Bewley is agent-first. Every command emits exactly one versioned JSON envelope
to stdout by default:

```json
{
  "schema_version": "1.0",
  "ok": true,
  "command": ["bewley", "status"],
  "data": {},
  "warnings": [],
  "next_actions": []
}
```

Failures set `ok` to `false`, return one structured `error` object, and exit
nonzero. Agents should branch on `ok`, use the actual argv array in `command`
for provenance, and execute only reviewed `next_actions`. Each action declares
whether it mutates state, uses the network, or requires user approval.

Run `bewley capabilities`, `bewley agent status`, or
`bewley agent schema envelope` to inspect the contract. Versioned JSON Schemas
ship with the package. Human-readable output is an explicit opt-in through
`--human`/`-H`.

## When to use this
<!-- id: bewley/when-to-use -->

- The user has interviews, field notes, documents, or transcripts and needs systematic qualitative coding.
- The analysis depends on traceable excerpts, memoing, code evolution, and auditable annotation history.
- The user wants to move from raw text to themes, evidence tables, quote banks, or theory diagrams.
- The corpus can be worked locally as text, or audio/video can be transcribed before coding.

## When this is a stretch (and how to adapt)
<!-- id: bewley/when-stretch -->

- The user has only a few short answers. Still use bewley if traceability matters; create a small project and keep the codebook sparse.
- The user mainly needs numeric labels for many rows. Use bewley to develop the rubric on a sample, then feed the stable labels to [labeling](#labeling/labeling).
- The user wants persona panels from interviews. Code themes here first when interpretability matters, then use [saldana](#saldana/saldana) for AgentList construction.
- The material is audio or video. Transcribe through `bewley add-audio` or `bewley add-video`, then treat the transcript as a normal document.

## Decision rule for the calling agent
<!-- id: bewley/decision-rule -->

Before dispatching to bewley, confirm:

1. The primary evidence is textual or can be converted to text.
2. The user needs interpretive categories, coded spans, quotes, or theme synthesis.
3. The analysis should preserve an audit trail from claim back to source passage.
4. The task benefits from iterative codebook refinement rather than one-shot summarization.

If yes to the first two and either the third or fourth, bewley is the right method.

## Inputs and elicitation
<!-- id: bewley/inputs -->

### Corpus
<!-- id: bewley/inputs-corpus -->

What it is: the documents, transcripts, audio, or video files to analyze.

How the agent elicits this:
- Ask what source material exists, how many items there are, and whether they are already text.
- Ask whether speaker labels, timestamps, or metadata such as participant role should be preserved.
- For sensitive material, ask whether redaction should happen before import.

Default to suggest: import a small pilot set first, usually 3-5 documents, to stabilize codes before scaling.

Fallback: if the corpus is not ready, create the project and add one representative document so the user can see the coding flow.

### Codebook
<!-- id: bewley/inputs-codebook -->

What it is: a set of named codes, descriptions, and optional hierarchy or links.

How the agent elicits this:
- Ask for the research question and any existing themes or constructs.
- Suggest 5-12 starter codes, split between topical codes and analytic codes.
- If the user is unsure, code one document inductively and revise the codebook after reviewing excerpts.

Default to suggest: begin with broad parent codes, then add child codes only when the same distinction appears repeatedly.

Fallback: use `bewley memo` for analytic observations before committing uncertain distinctions to formal codes.

### Annotation strategy
<!-- id: bewley/inputs-annotation-strategy -->

What it is: the unit of evidence and how spans will be selected.

How the agent elicits this:
- Ask whether the unit should be sentence, paragraph, turn, or exact phrase.
- Ask whether overlapping codes are allowed and whether negating evidence should be tagged.
- Clarify whether annotation should prioritize recall for later review or precision for final export.

Default to suggest: paragraph or speaker-turn spans for interviews; exact quote spans for final evidence tables.

Fallback: annotate broadly first, then use query/export review to tighten high-value passages.

## Outputs
<!-- id: bewley/outputs -->

bewley writes durable project state under `.bewley/` and generated artifacts on demand:

- `.bewley/events/` and `.bewley/index.sqlite` — append-only events and rebuildable search/index state.
- Imported corpus objects and document revisions — local source records with stable IDs.
- Codebook, annotation, memo, and history views from `bewley list`, `bewley show`, `bewley history`, and `bewley query`.
- Exports from `bewley export` — snippets, quote tables, HTML browsers, theory diagrams, and narrative summaries.
- Generated EDSL scripts from `bewley codegen` for AI-assisted coding passes.

## Workflow
<!-- id: bewley/workflow -->

Canonical sequence:

1. `bewley init` — create a local qualitative project.
2. `bewley add <path>` or `bewley add-audio <path>` — import source material.
3. `bewley code create ...` — define starter codes and descriptions.
4. `bewley annotate apply ...` — attach codes to evidence spans, with an explicit scope such as `--lines`, `--bytes`, or `--document`.
5. `bewley memo ...` — record analytic notes as patterns emerge.
6. `bewley query ...` — retrieve evidence across boolean code expressions.
7. `bewley export ...` — create quote banks, HTML views, diagrams, or narratives.
8. `bewley fsck` — check integrity before using outputs downstream.

If the agent loses track, run `bewley status` and `bewley history` to recover current state.

### Study-scaffold integration
<!-- id: bewley/study-scaffold-integration -->

When Bewley is used inside a larger research-agent study scaffold, keep the
Bewley project local to the active task directory and use project-relative
corpus paths throughout. A reliable layout is:

```text
analysis/
  bewley_project/
    .bewley/
    corpus/
      response_000.txt
      response_001.txt
writeup/
  tables/
    qual_code_summary.csv
    qual_quotes.csv
```

Run Bewley commands from `analysis/bewley_project/`:

```bash
bewley init
bewley add corpus/response_000.txt
bewley code create time_pressure --description "Mentions schedule pressure or limited time"
bewley annotate apply time_pressure corpus/response_000.txt --document
bewley status
bewley export quotes --all --format jsonl
```

Do not pass absolute paths or paths outside the Bewley project root to
`bewley add` or `bewley annotate`; Bewley expects document paths to be inside
the initialized project. If source responses are generated elsewhere in the
task, materialize or copy them into `analysis/bewley_project/corpus/` first,
then add them using `corpus/<filename>`.

Downstream report scripts should consume compact exports such as code counts
and quote tables, not verbose command logs. Before writing representative
excerpts into a report, verify the export contains actual text. If
`bewley export quotes` is empty but `bewley status` reports active
annotations, inspect query/annotation output and adapt the export script so it
preserves document path, code name, and exact coded text.

For research-agent study reports, the downstream quote table is not valid
unless it contains at least:

- a stable document or response identifier,
- the code name,
- the exact excerpt text,
- enough study metadata to interpret the excerpt, such as scenario, treatment,
  persona, or respondent segment.

Do not treat command success as export success. After exporting, read the first
rows and assert that the excerpt-text column is non-empty. If the built-in
export shape does not include text for the current annotation strategy, write a
small task-local adapter that joins Bewley document/code identifiers back to the
materialized corpus files or annotation/query output and emits
`writeup/tables/qual_quotes.csv` with an explicit `text` column. If that cannot
be done reliably, omit representative quotes from the report and record the
limitation in `writeup/notes.md` rather than fabricating or summarizing
unsupported excerpts.

## Worked examples
<!-- id: bewley/examples -->

### Eliciting an interview codebook
<!-- id: bewley/example-interview-codebook -->

User: "I have customer interviews and want to know why churn happens."

Agent: "Let's code a pilot set first. What interview files do you have, and do you already suspect themes such as price, onboarding, missing features, or support friction?"

User: "Ten transcripts. I suspect onboarding and integrations."

Agent: "Good starting parent codes are onboarding, integrations, support, pricing, and switching-cost. I'll import three transcripts first, code broad passages, then we can split any overloaded code."

```bash
bewley init
bewley add transcripts/acme.txt
bewley code create onboarding --description "Onboarding friction"
bewley code create integrations --description "Integration gaps"
bewley annotate apply onboarding transcripts/acme.txt --lines 8:16
bewley query "onboarding OR integrations"
```

Output: coded excerpts the agent can review with the user before scaling to all ten transcripts.

### Exporting evidence for a report
<!-- id: bewley/example-export-evidence -->

```bash
bewley query "support AND NOT pricing"
bewley export quotes --code support --format jsonl > writeup/tables/support_quotes.jsonl
bewley export html --output writeup/bewley_browser.html
bewley export plots --output-dir writeup/plots
```

Output: a quote table and browsable HTML evidence file for report drafting.

## Quick command reference
<!-- id: bewley/commands -->

For full options, run `bewley <subcommand> --help`.

| Command | Purpose |
|---|---|
| `bewley init` | Create a project in the current directory. |
| `bewley status` | Show project counts and current state. |
| `bewley add` / `add-audio` / `add-video` | Import text or transcribed media into the corpus. |
| `bewley update` | Add a new document revision from disk. |
| `bewley code ...` | Create, edit, link, and organize codes. |
| `bewley annotate ...` | Add, list, update, or remove coded spans. |
| `bewley query` | Search annotations with boolean code expressions. |
| `bewley export` | Emit snippets, quotes, HTML, diagrams, or narratives. |
| `bewley memo` | Manage analytic memos. |
| `bewley history` / `undo` | Inspect or compensate prior events. |
| `bewley fsck` / `rebuild-index` | Verify or rebuild project indexes. |
| `bewley codegen` | Generate AI-assisted coding scripts. |

## Common pitfalls
<!-- id: bewley/pitfalls -->

- Importing every transcript before piloting the codebook makes early mistakes expensive to unwind; start with a representative subset.
- Running Bewley from the wrong directory causes confusing path errors. Initialize
  the project, add documents, annotate, query, and export from the same Bewley
  project root.
- Passing absolute paths to `bewley add` for study-generated files. Copy or
  materialize files into `corpus/` and add them as `corpus/<filename>`.
- Assuming `export quotes` succeeded because the command returned JSON. Check
  the `data` rows before using excerpts in `writeup/report.md`.
- Writing a quote table with only filenames, codes, or document IDs. A report
  quote table must include actual excerpt text and the study metadata needed to
  interpret it.
- Codes without descriptions drift quickly; write the inclusion rule when creating the code.
- Exact span offsets can become stale after document updates; inspect revisions before reusing old coordinates.
- Query hits are evidence, not conclusions; use memos to record the interpretation that connects excerpts.

## Cross-references
<!-- id: bewley/xrefs -->

- Upstream: [saldana](#saldana/saldana) can create persona panels from transcript sets when coding is not the primary deliverable.
- Downstream: [labeling](#labeling/labeling) can operationalize a mature codebook as LLM-as-rater labels.
- Adjacent methods: [messick](#messick/messick) validates LLM-agent study outputs; [tufte](#tufte/tufte) checks figures made from coded counts.

## State contract
<!-- id: bewley/state -->

After `bewley init`, `.bewley/` is the durable project store. The event log is the source of truth; indexes and projections are rebuildable with `bewley rebuild-index`. Agents may inspect state for recovery, but should mutate it through CLI commands so history remains auditable.
