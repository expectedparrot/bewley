# Grounded Theory with Bewley

## Contents

- [Overview](#overview)
- [Open coding](#open-coding)
- [Constant comparison](#constant-comparison)
- [Axial coding](#axial-coding)
- [Selective coding and core category](#selective-coding-and-core-category)
- [Memo-writing](#memo-writing)
- [Theoretical saturation](#theoretical-saturation)
- [Theory export](#theory-export)
- [Workflow summary](#workflow-summary)

## Overview

Grounded theory is an inductive methodology where theory emerges from systematic analysis of data rather than being imposed from prior frameworks. The researcher moves through progressively more abstract levels of coding — open, axial, selective — while continuously comparing data segments, writing analytic memos, and refining categories until theoretical saturation is reached.

Bewley has first-class support for each stage of this process.

## Open coding

The first analytic pass: reading data line-by-line and assigning provisional codes grounded in the text. Codes should be specific and close to the data.

**In vivo codes** — codes using participants' own language — are preferred when their words capture a concept precisely (e.g., `just_winging_it` rather than `improvisation`).

Bewley commands:

```bash
# Create codes as they emerge
bewley code create trust_building --description "Participants describe developing trust"
bewley code create just_winging_it --description "In vivo: improvising without a plan"

# Apply codes to specific text spans
bewley annotate apply trust_building corpus/interview-01.txt --lines 14:22
bewley annotate apply trust_building corpus/interview-01.txt --lines 14:22 --memo "Explicit mention of earning trust over time"
```

The Jobs workflow can automate an initial pass: package with `bewley open-coding jobs`, execute with `ep run`, then audit and convert the Results with `bewley open-coding ingest`. Treat `candidate_codes.csv` as a starting point to review and refine, not as final codes.

## Constant comparison

The core analytic engine of grounded theory. Every new data segment is compared against existing codes and categories. This drives code refinement throughout the analysis.

In practice:

1. **Before coding a new document**, review existing codes: `bewley code list`
2. **While coding**, check what a code already contains: `bewley show snippets --code trust_building`
3. **Merge** near-synonyms when two codes capture the same concept:
   ```bash
   bewley code merge trust_building building_trust --into trust_building
   ```
4. **Split** overloaded codes when a code covers two distinct phenomena:
   ```bash
   bewley code split coping --new avoidance_coping --annotation <id1> --annotation <id2>
   ```
5. **Rename** codes as understanding deepens:
   ```bash
   bewley code rename trust_building gradual_trust_formation
   ```
6. **Write a memo** each time a comparison produces an insight about similarities, differences, or conditions.

## Axial coding

The second analytic phase: reassembling data by making explicit connections between categories. The goal is to specify conditions, contexts, strategies, and consequences that relate categories to each other.

Bewley commands:

```bash
# Create named relationships between codes
bewley code link resource_scarcity workaround_behavior "causes" --memo "Scarcity triggers informal workarounds"
bewley code link institutional_pressure compliance_theater "context-for"
bewley code link gradual_trust_formation information_sharing "enables"

# Build hierarchies — group subcategories under higher-order categories
bewley code set-parent gradual_trust_formation interpersonal_dynamics
bewley code set-parent conflict_avoidance interpersonal_dynamics

# Visualize the emerging structure
bewley export theory --format mermaid

# Review all relationships for a code
bewley code links interpersonal_dynamics
```

Useful relationship types: `causes`, `context-for`, `strategy-for`, `consequence-of`, `enables`, `constrains`, `is-a-type-of`, `co-occurs-with`.

## Selective coding and core category

The final coding phase: identifying the **core category** — the central concept that integrates the theory and accounts for the main pattern in the data. All other categories should relate to it.

Criteria for a core category:
- It appears frequently across the data
- It connects to most other major categories
- It accounts for variation (explains why and under what conditions patterns differ)
- It has clear analytic power — it "earns" its central position through the data

Bewley commands:

```bash
# Designate the core category
bewley code set-core navigating_uncertainty

# Verify it
bewley code show-core

# Ensure all major categories link to the core
bewley code link resource_scarcity navigating_uncertainty "dimension-of"
bewley code link gradual_trust_formation navigating_uncertainty "strategy-for"

# Generate an integrative narrative organized around the core
bewley export narrative
```

## Memo-writing

Memos are the written record of analytic thinking. They are not summaries of data — they are the researcher's developing theoretical ideas. Write memos early and often.

Types of memos:

- **Code memos**: What does this code mean? What are its properties and dimensions?
- **Theoretical memos**: How do codes relate? What process or pattern is emerging?
- **Operational memos**: Methodological decisions, sampling choices, next steps.
- **Saturation memos**: Has this category stopped generating new properties?

```bash
# Attach a memo to a code
bewley memo add --code trust_building 'Trust appears in 8/12 interviews. Three dimensions emerging: competence-based, vulnerability-based, and institutional.'

# Attach a memo to a document
bewley memo add --document corpus/interview-07.txt 'Deviant case — participant reports zero trust but high satisfaction. Challenges the trust-satisfaction link.'

# Project-level theoretical memo
bewley memo add --title 'Core category candidates' 'Three contenders: navigating_uncertainty, adaptive_compliance, trust_negotiation. Uncertainty has the most connections but compliance explains more variation in outcomes.'

# Saturation memo
bewley memo add --code trust_building --title 'Saturation note' 'No new properties since interview 10. Three dimensions stable. Category saturated.'
```

## Theoretical saturation

Saturation is reached when new data no longer produces new codes, new properties of existing codes, or new relationships between categories. It is assessed per-category, not globally.

How to track saturation in bewley:

1. After coding each new document, check whether any new codes were created or existing codes gained new properties.
2. Use `bewley code list` to monitor whether the code count has stabilized.
3. Use `bewley show snippets --code <ref>` to check whether new annotations add conceptual depth or merely confirm existing understanding.
4. Write explicit saturation memos for each major category.
5. When all major categories are saturated and the core category integrates the theory, coding is complete.

## Theory export

Once coding is complete and the core category is set:

```bash
# Structured JSON: codes, links, hierarchy, core category, memos
bewley export theory --format json --output theory.json

# Integrative narrative: text summary organized around the core category
bewley export narrative --output narrative.md

# Full HTML code explorer with all annotations
bewley export html --output analysis.html --title "Grounded Theory Analysis"
```

### Interactive theory explorer

The built-in Mermaid export (`bewley export theory --format mermaid`) is
limited. For richer output, generate the interactive D3 explorer:

```bash
bewley codegen theory-explorer
python qualitative-analysis/render_theory_explorer.py
```

The generated script is standalone (stdlib only) and embeds a snapshot of the
project's codes, hierarchy, links, and annotations — regenerate it when those
change. The explorer shows a force-directed graph with node size scaled by
annotation count, color by category, labeled relationship edges, the core
category highlighted, and click-through to quotes and links.

**Embedding in a report.** Use an iframe in the report markdown so the
interactive explorer is inline. Do NOT use `--embed-resources` with pandoc
when the report contains an iframe — it breaks the reference.

```markdown
<iframe src="qualitative-analysis/theory_explorer.html"
        style="width:100%; height:700px; border:1px solid #ddd; border-radius:8px;"
        loading="lazy"></iframe>

*Figure 1. Interactive theory explorer — click any theme to expand.*
```

Compile with:

```bash
pandoc report.md -o report.html --css=report.css --standalone
```

For static print figures, `bewley export theory --format json` provides the
structured theory for any external renderer.

## Workflow summary

| Phase | Key bewley commands |
|---|---|
| Open coding | `open-coding jobs`, `open-coding ingest`, `code create`, `annotate apply --lines` |
| Constant comparison | `show snippets`, `code merge`, `code split`, `code rename` |
| Axial coding | `code link`, `code set-parent`, `export theory --format mermaid` |
| Selective coding | `code set-core`, `export narrative` |
| Memo-writing | `memo add --code`, `memo add --document`, `memo add` |
| Saturation tracking | `code list`, `show snippets`, `memo add --title 'Saturation note'` |
| Theory export | `export theory --format json`, `codegen theory-explorer`, `export narrative`, `export html` |
