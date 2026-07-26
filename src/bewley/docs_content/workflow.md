# Bewley Workflow Phases and Checklist

## Project phases

Bewley infers the current phase from what's on disk — no metadata to drift. Run `bewley next` to get the current phase and the recommended next action (`bewley status` reports counts only).

| Phase | Condition | Primary doc |
|---|---|---|
| `init` | No `.bewley/` project in current directory | getting-started |
| `corpus` | Project initialized, 0 documents | getting-started |
| `open_coding` | Documents added, 0 codes | getting-started |
| `annotating` | Codes created, 0 annotations | workflow |
| `analysis` | Annotations exist | grounded-theory |

## Full checklist

### Phase 1: Init

1. Create the project directory: `mkdir -p <path>/qualitative-coding`
2. `cd <path>/qualitative-coding && bewley init`

### Phase 2: Build corpus

1. Copy or move UTF-8 text files into `corpus/`
2. `bewley add corpus/<file>` for each document
3. `bewley list documents` to verify all documents are tracked
4. `bewley status` to confirm counts

### Phase 3: Open coding

1. Read all corpus documents (agent task)
2. Write `qualitative-analysis/corpus_summary.md`:
   - Type of texts in the corpus
   - Number and scope of documents
   - Initial impressions of recurring themes
3. Configure EDSL once: `ep auth login`, then verify with `ep auth status` and `ep check`. Keep `.env` out of version control.
4. `bewley open-coding jobs --output jobs.ep --model <model-name>`
5. `ep run jobs.ep --model_list models.ep --output results.ep` (external; requires approval)
6. `bewley open-coding ingest results.ep --jobs jobs.ep`
7. Review `candidate_codes.csv` — merge near-synonyms, remove noise, and inspect any unresolved quotes
8. **Pause and show user the candidate codes before proceeding**
9. `bewley code create <name> --description '<description>'` for each keeper

### Phase 4: Annotation

1. Apply the reviewed candidates (creates missing codes and exact-span annotations; skipped rows are itemized, never guessed):
   ```bash
   bewley open-coding apply --dry-run   # preview
   bewley open-coding apply             # execute
   ```
   For ad-hoc one-offs:
   ```bash
   bewley annotate apply <code> <doc_id> --bytes <start>:<end>
   # or: bewley annotate apply <code> <doc_id> --lines <start>:<end>
   ```
2. Build code hierarchy:
   ```bash
   bewley code set-parent <child> <parent>
   ```
3. Create named links:
   ```bash
   bewley code link <source> <target> <relationship>
   # Useful types: causes, context-for, strategy-for, enables, constrains, co-occurs-with
   ```
4. Write memos after significant insights:
   ```bash
   bewley memo add --code <ref> 'Analytical note'
   bewley memo add --document <ref> 'Document-level observation'
   ```
5. **Pause and show user the annotation summary before proceeding**

### Phase 5: Analysis and export

1. Constant comparison: `bewley show snippets --code <ref>`, or `bewley code coverage <ref> --breakdown` for parent categories (the inclusive rollup hides whether coverage is concentrated in one child or spread across divergent ones).
2. Refine codes as understanding deepens:
   ```bash
   bewley code merge trust_building building_trust --into trust_building
   bewley code split coping --new avoidance_coping --annotation <id1> --annotation <id2>
   bewley code rename trust_building gradual_trust_formation --description 'Gradual development of trust over repeated interactions'
   ```
   Pass `--description` to `rename` whenever the new name changes the concept's scope — otherwise the description goes stale.
3. Set the core category (grounded theory):
   ```bash
   bewley code set-core navigating_uncertainty
   ```
4. Export:
   ```bash
   bewley export theory --format json --output theory.json
   bewley export narrative --output narrative.md
   bewley export html --output analysis.html --title "My Analysis"
   bewley export plots --output-dir plots
   ```
5. Interactive theory explorer (force-directed D3 graph with filters and a click-to-see-quotes panel):
   ```bash
   bewley codegen theory-explorer
   python qualitative-analysis/render_theory_explorer.py
   # -> qualitative-analysis/theory_explorer.html
   ```
   For static print/PDF figures, use `bewley export theory --format mermaid` and render the `.mmd` with any Mermaid renderer.

## Undo and recovery

```bash
bewley history                      # view event log
bewley undo <event_id>              # append compensating event (never deletes history)
bewley rebuild-index                # fully rebuild SQLite from events
bewley fsck                         # verify integrity
```

## Working directory discipline

Every bewley command must run from the project directory. Between shell calls, always re-cd:

```bash
cd /absolute/path/to/qualitative-coding && bewley <command>
```

Anti-pattern — do NOT use a flag that doesn't exist:
```bash
bewley --cwd /path list documents   # WRONG: no --cwd flag
```

## User approval gates

**Pause and show the user output before proceeding after:**
- Generating candidate codes (before mass-applying annotations)
- Any code merge or split that affects existing annotations
- Completing each major phase (open coding, axial coding, selective coding)

## Query syntax

```bash
bewley query '<expr>' [--mode document|annotation]
```

- `code_name` — documents/annotations with this code
- `A & B` — AND
- `A | B` — OR
- `!A` — NOT
- `(A & B) | C` — grouping

**Always quote expressions containing `&`, `|`, `!`, or parentheses** to prevent shell interpretation.

## Next steps

- `bewley docs show commands` — Full command reference
- `bewley docs show grounded-theory` — Grounded theory methodology
