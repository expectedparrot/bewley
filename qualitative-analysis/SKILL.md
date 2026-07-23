---
name: research-qualitative-data-analysis
description: >
  Performs qualitative coding and thematic analysis of text corpora using the
  bewley CLI and EDSL. Covers project setup, open coding, code refinement,
  annotation, memo-writing, and export. Use when the user wants to analyze
  text of any kind -- transcripts, speeches, reviews, articles, field notes,
  open-ended survey responses, social media posts, or any unstructured text
  data. Triggers on requests to code, tag, theme, categorize, or find
  patterns in text, including content analysis and discourse analysis.
tags: [study-type, qualitative, analysis, content-analysis]
---

# Qualitative Data Analysis with Bewley

This skill guides qualitative coding of text corpora using the `bewley` CLI. It covers installation, the core workflow, and EDSL-assisted open coding.

For the full bewley command reference, see `references/bewley-commands.md`.
For grounded theory methodology and the corresponding bewley workflow, see `references/grounded-theory.md`.

## Installation

```bash
pip install git+https://github.com/expectedparrot/bewley.git
```

Verify with:

```bash
bewley --help
```

## Core concepts

- **Project**: A directory containing a `corpus/` folder and a `.bewley/` metadata directory. Created with `bewley init`.
- **Document**: A UTF-8 text file tracked by bewley (e.g., an interview transcript).
- **Code**: A named analytic label applied to text (e.g., "trust", "themes/rapport").
- **Annotation**: An application of a code to a whole document or a line span within a document revision. See "Annotation granularity" below for when to use each.
- **Memo**: A free-text analytic note attached to a code, document, or the project.
- **Event log**: Append-only JSON log in `.bewley/events/`. This is the source of truth. SQLite is a rebuildable cache.

References (document_ref, code_ref) accept UUIDs, names, paths, or path prefixes.

## Getting help

Every command and subcommand has detailed `--help` output:

```bash
bewley --help                        # top-level overview
bewley <command> --help              # e.g., bewley code --help
bewley <command> <subcommand> --help # e.g., bewley annotate apply --help
```

When unsure about a command's arguments or output format, always run `--help` first.

## Project location

Qualitative analysis studies use the same directory layout as other research projects (see `skill:workflow-file-layout`). The bewley corpus and coding data live inside the study's `data/` folder:

```
sessions/topic_<alias>/study_<name>/
  data/
    qualitative-coding/      ← bewley init here
      .bewley/
      corpus/
      corpus_summary.md
      candidate_codes.csv
  analysis/
  writeup/
  …
```

Initialize the project inside `data/`:

```bash
cd sessions/topic_<alias>/study_<name>/data
mkdir qualitative-coding && cd qualitative-coding
bewley init
```

## Typical workflow

1. **Initialize**: `bewley init` inside `data/qualitative-coding/`
2. **Add documents**: `bewley add <path>` for each transcript/text file
3. **Summarize corpus**: Read all documents, write a `corpus_summary.md`
4. **Generate candidate codes**: Run `python scripts/generate_candidate_codes.py` (see "Open coding with EDSL" below)
5. **Refine codes**: Review `candidate_codes.csv`, deduplicate, then `bewley code create` for each
6. **Annotate**: `bewley annotate apply <code> <doc> --lines S:E` for thematic codes; `--document` for document-level codes (see "Annotation granularity")
7. **Write memos**: `bewley memo add --code <ref> 'Analytical note...'`
8. **Query and review**: `bewley query '<expr>'` and `bewley show snippets --code <ref>`
9. **Build hierarchy**: `bewley code set-parent` and `bewley code link`
10. **Export**: `bewley export snippets`, `bewley export theory`, etc.
11. **Render theory diagram**: Export with `bewley export theory --format json --output theory.json`, then choose a rendering format. For **report embedding**, use `python scripts/render_collapsible_diagram.py theory.json --flow flow.yml -o theory_interactive.html` (collapsible theme blocks, best for readers). For **analyst exploration**, use `python scripts/render_theory_diagram.py theory.json --format html -o theory.html` (D3 force-directed, drag/zoom/hover). For **print/PDF**, use `--format svg`. See `references/grounded-theory.md` "Collapsible report diagrams" for the flow spec format and embedding instructions.
12. **Makefile target**: Add a `qualitative_report` target that runs `bewley -H export html` and copies the output into the `writeup/` folder. The writeup should reference the generated HTML report.

## Open coding with EDSL

Bewley includes a script for generating candidate qualitative codes using EDSL. This automates the initial open coding pass.

### Prerequisites

```bash
pip install git+https://github.com/expectedparrot/bewley.git
pip install edsl
```

### Step 1: Create the corpus summary (agent task)

Before generating codes, read all documents and write a corpus summary:

1. Run `bewley list documents` to get all document paths.
2. Read each document from the `corpus/` directory.
3. Write a `corpus_summary.md` with:
   - What kind of texts the corpus contains (interviews, field notes, etc.)
   - Approximate size and scope (number of documents, topics covered)
   - Initial impressions of recurring themes or notable features
   - Any contextual information about the research setting

This summary provides shared context so the LLM can generate codes that are coherent across the whole corpus, not just locally relevant to each document.

### Step 2: Generate candidate codes

```bash
python scripts/generate_candidate_codes.py
```

Options:
- `--project-dir DIR` -- path to the bewley project (default: current directory)
- `--summary FILE` -- path to corpus summary (default: `corpus_summary.md`)
- `--output FILE` -- output CSV (default: `candidate_codes.csv`)
- `--model MODEL` -- EDSL model name (e.g., `claude-3-5-sonnet-20241022`)

The script reads the corpus summary and all documents, builds an EDSL `ScenarioList` (one `Scenario` per document), and runs a `Survey` asking an LLM to suggest open codes for each document. Results are saved to `candidate_codes.csv` with columns: `code_name`, `description`, `quote`, `source_document_id`, `source_document_path`.

**Quote-anchored coding.** The LLM returns a verbatim quote from the document for each code instead of line numbers. This is critical -- LLMs cannot reliably count line numbers, especially in documents with metadata headers or blank lines. The quote is then resolved to byte/line ranges programmatically (see Step 2b).

### Step 2b: Resolve quotes to line ranges

```bash
python scripts/resolve_quotes.py candidate_codes.csv \
    --project-dir . \
    -o candidate_codes_resolved.csv
```

This script fuzzy-matches each quote against its source document using three strategies (exact, normalized, longest common substring) and outputs a CSV with `start_byte`, `end_byte`, `start_line`, `end_line` columns. Unresolved quotes are written to a separate file for manual review.

Always review the unresolved file -- if more than ~10% of quotes fail to resolve, the LLM may be paraphrasing rather than quoting verbatim. In that case, rerun the coding pass with a stronger prompt emphasis on exact copying.

### Step 3: Refine and apply codes

After reviewing `candidate_codes_resolved.csv`:

1. **Deduplicate**: Merge near-synonyms (e.g., `trust_building` and `building_trust`)
2. **Create codes in bewley**:
   ```bash
   bewley code create trust_building --description "Instances where participants describe developing trust"
   ```
3. **Organize hierarchy**: Group related codes under parents
   ```bash
   bewley code set-parent trust_building interpersonal_dynamics
   ```
4. **Apply annotations using resolved byte ranges** (preferred over `--lines`):
   ```bash
   bewley annotate apply trust_building <doc_id> --bytes 150:280
   ```

## Annotation granularity

Use the right scope for the kind of code:

- **Thematic codes** (e.g., `route_pressure`, `dehumanization`) should be **span-level** (`--lines S:E`). These codes are grounded in specific passages, and anchoring them to lines produces better exports (the quote explorer shows the actual text) and better auditability.
- **Document-level codes** (e.g., `cautionary_tone`, `ambivalent_assessment`, `positive_overall`) are fine as **document-level** (`--document`). These describe the character or framing of the review as a whole, not a specific passage.

```bash
# Thematic code -- anchor to the passage that supports it
bewley annotate apply route_pressure <doc_id> --lines 8:10

# Document-level code -- describes the overall document
bewley annotate apply cautionary_tone <doc_id> --document
```

**You must specify exactly one of `--document`, `--bytes S:E`, or `--lines S:E`.** Omitting the scope flag is a silent error — bewley prints `bewley error:` with an empty message and exits with code 1. When generating batch annotation scripts, always include the scope flag and check exit codes.

## Working directory discipline

**Bewley has no `--cwd` flag.** It discovers its project by looking for `.bewley/` in the current working directory. Every bewley command must be run from the project directory (e.g., `data/qualitative-coding/`).

Shell state is not preserved between Bash tool calls. Always `cd` with an absolute path before each bewley command:

```bash
cd /absolute/path/to/sessions/topic_foo/study_a/data/qualitative-coding && bewley list documents
```

Define `QUAL_DIR` early in the session and reuse it:

```bash
QUAL_DIR="/absolute/path/to/sessions/topic_foo/study_a/data/qualitative-coding"
cd "$QUAL_DIR" && bewley status
```

**Anti-pattern — do NOT do this:**
```bash
bewley --cwd /some/path list documents   # WRONG: --cwd does not exist
```

## Important notes

- All IDs are UUIDs. Capture and reuse them from command output.
- The `--lines` flag uses **1-based inclusive** ranges (e.g., `--lines 10:20` means lines 10 through 20). The `--bytes` flag uses **0-based, exclusive-end** ranges.
- Document updates may cause annotations to become `conflicted` if fuzzy relocation fails. Check `bewley status` for `conflicted_annotations > 0` and resolve with `bewley annotate resolve`.
- The event log is append-only. `bewley undo` does not delete history; it appends a compensating event.
- `bewley rebuild-index` can recover from any index corruption since SQLite is a cache, not the source of truth.
- When passing query expressions containing `&`, `|`, `!`, or parentheses, **always quote the expression** to prevent shell interpretation.
- All commands produce **JSON** output.
- Mutating commands print the **ID of the created or affected entity**.
- Errors go to **stderr**. Exit code is **0** on success, **1** on failure.
