# Getting Started with Bewley

## Installation

```bash
pip install git+https://github.com/expectedparrot/bewley.git
```

Verify:

```bash
bewley --help
bewley docs list
bewley status
```

## Critical: no `--cwd` flag

**Bewley has no `--cwd` flag.** It discovers its project by looking for `.bewley/` in the current directory. Every bewley command must be run from the project directory.

Shell state is not preserved between Bash tool calls. Always `cd` with an absolute path before each command:

```bash
cd /absolute/path/to/qualitative-coding && bewley status
```

Define a variable early in the session and reuse it:

```bash
QUAL_DIR="/absolute/path/to/qualitative-coding"
cd "$QUAL_DIR" && bewley status
```

## Recommended directory layout

```
sessions/topic_<alias>/study_<name>/
  data/
    qualitative-coding/      ← bewley init here
      .bewley/               ← metadata (events, objects, index)
      corpus/                ← text documents
  qualitative-analysis/      ← scripts and generated data
    corpus_summary.md
    candidate_codes.csv
    candidate_codes_resolved.csv
    generate_candidate_codes.py
    resolve_quotes.py
    render_theory_diagram.py
    render_collapsible_diagram.py
```

## Step-by-step workflow

### 1. Initialize the project

```bash
mkdir -p sessions/topic_foo/study_a/data/qualitative-coding
cd sessions/topic_foo/study_a/data/qualitative-coding
bewley init
```

### 2. Add documents

```bash
bewley add corpus/interview-01.txt
bewley add corpus/interview-02.txt
bewley list documents    # verify
bewley status            # see counts
```

### 3. Summarize corpus (agent task)

Read all documents and write a `corpus_summary.md` covering:
- What kind of texts the corpus contains
- Number of documents, scope, topics covered
- Initial impressions of recurring themes or notable features

### 4. Generate candidate codes

```bash
bewley open-coding jobs --output jobs.ep
ep run jobs.ep --model <model-name> --output results.ep
bewley open-coding ingest results.ep --jobs jobs.ep
```

Bewley packages the prompt and immutable document revisions; the `ep` CLI executes them, and Bewley audits and ingests the Results. Review `qualitative-analysis/candidate_codes.csv` before continuing.

### 5. Resolve quotes to byte ranges

```bash
bewley codegen resolve-quotes          # writes qualitative-analysis/run_resolve_quotes.py
python qualitative-analysis/run_resolve_quotes.py
```

The generated script maps each candidate quote to an exact byte range in its source document, using a fuzzy fallback cascade (exact → strip surrounding punctuation → case-insensitive). Quotes that still fail are usually genuine LLM paraphrases (e.g., ellipses between non-contiguous passages) — fix those by hand in `candidate_codes.csv` and re-run. If more than ~10% fail, the original open-coding prompt may need a stricter "verbatim" instruction.

### 6. Create codes and apply annotations

```bash
# Create codes after deduplication
bewley code create trust_building --description "Developing trust with others"

# Apply annotations using resolved byte ranges (preferred)
bewley annotate apply trust_building <doc_id> --bytes 150:280

# Or using line ranges (1-based inclusive)
bewley annotate apply trust_building <doc_id> --lines 10:22

# Document-level code (describes the whole document, not a span)
bewley annotate apply cautionary_tone <doc_id> --document
```

**Annotation scope is mandatory.** You must pass exactly one of `--document`, `--bytes S:E`, or `--lines S:E`.

### 7. Build code hierarchy and links

```bash
bewley code set-parent trust_building interpersonal_dynamics
bewley code link resource_scarcity workaround_behavior "causes" --memo "Scarcity triggers workarounds"
```

### 8. Write memos

```bash
bewley memo add --code trust_building 'Three dimensions emerging: competence, vulnerability, institutional'
bewley memo add --document corpus/interview-07.txt 'Deviant case — zero trust but high satisfaction'
```

### 9. Query and review

```bash
bewley show snippets --code trust_building
bewley query 'trust_building & emotional_labor'
bewley query '!(trust_building | emotional_labor)'   # must quote expressions with & | !
```

### 10. Export

```bash
bewley export theory --format json --output theory.json       # structured theory export
bewley export theory --format mermaid --output theory.mmd     # Mermaid diagram source
bewley export narrative --output narrative.md                  # per-code inventory
bewley export html --output analysis.html                      # code+quote explorer
bewley export document-html <doc_ref> --output doc.html        # per-document annotated view

# Interactive theory graph with filters and clickable quote panel:
bewley codegen theory-explorer        # writes qualitative-analysis/render_theory_explorer.py
python qualitative-analysis/render_theory_explorer.py
# -> qualitative-analysis/theory_explorer.html
```

## Annotation granularity

- **Thematic codes** (e.g., `route_pressure`) → span-level (`--lines S:E` or `--bytes S:E`): anchored to specific passages, produces better exports
- **Document-level codes** (e.g., `cautionary_tone`) → document-level (`--document`): describes the whole document

## Output conventions

- JSON by default; `-H`/`--human` for human-readable
- `--lines` uses **1-based inclusive** ranges (e.g., `--lines 10:20` = lines 10–20)
- `--bytes` uses **0-based, exclusive-end** ranges
- Mutating commands print the ID of the created entity

## Next steps

- `bewley docs show workflow` — Phases and full checklist
- `bewley docs show commands` — Complete command reference
- `bewley docs show grounded-theory` — Grounded theory methodology
