"""Workflow phases inferred from project state, with next-step guidance."""
from __future__ import annotations



_PHASE_INIT = "init"


_PHASE_CORPUS = "corpus"


_PHASE_OPEN_CODING = "open_coding"


_PHASE_ANNOTATING = "annotating"


_PHASE_ANALYSIS = "analysis"


_PHASE_CHECKLISTS: dict[str, list[str]] = {
    _PHASE_INIT: [
        "Run `bewley init` to create the project.",
    ],
    _PHASE_CORPUS: [
        "Copy text files into corpus/ and run `bewley add corpus/<file>` for each.",
        "Verify with `bewley list documents`.",
    ],
    _PHASE_OPEN_CODING: [
        "Read all documents and write qualitative-analysis/corpus_summary.md.",
        "Run `bewley open-coding jobs --output jobs.ep --model <model-name>` to package the corpus.",
        "Run `ep run jobs.ep --model_list models.ep --output results.ep` (external; requires approval).",
        "Run `bewley open-coding ingest results.ep --jobs jobs.ep`.",
        "Review candidate_codes.csv, then `bewley code create <name>` for each keeper.",
    ],
    _PHASE_ANNOTATING: [
        "Review candidate_codes.csv, deleting rejected rows.",
        "Preview: `bewley open-coding apply --dry-run`, then apply: `bewley open-coding apply`.",
        "Annotate by hand where needed: `bewley annotate apply <code> <doc_id> --bytes S:E`.",
        "Build hierarchy: `bewley code set-parent <child> <parent>`.",
        "Create links: `bewley code link <source> <target> <relationship>`.",
        "Write memos: `bewley memo add --code <ref> 'Analytical note'`.",
    ],
    _PHASE_ANALYSIS: [
        "Continue constant comparison: `bewley show snippets --code <ref>`.",
        "Set core category: `bewley code set-core <ref>`.",
        "Export: `bewley export theory --format json --output theory.json`.",
    ],
}


_PHASE_DOCS: dict[str, str] = {
    _PHASE_INIT: "getting-started",
    _PHASE_CORPUS: "getting-started",
    _PHASE_OPEN_CODING: "workflow",
    _PHASE_ANNOTATING: "workflow",
    _PHASE_ANALYSIS: "grounded-theory",
}


def _infer_phase(project: "Project | None") -> str:
    if project is None:
        return _PHASE_INIT
    with project.connect() as conn:
        doc_count = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        code_count = conn.execute("SELECT COUNT(*) FROM codes").fetchone()[0]
        ann_count = conn.execute("SELECT COUNT(*) FROM annotations WHERE is_active = 1").fetchone()[0]
    if doc_count == 0:
        return _PHASE_CORPUS
    if code_count == 0:
        return _PHASE_OPEN_CODING
    if ann_count == 0:
        return _PHASE_ANNOTATING
    return _PHASE_ANALYSIS


def _next_steps_for_phase(phase: str) -> list[dict]:
    if phase == _PHASE_INIT:
        return [{"label": "Initialize project", "command": "bewley init"}]
    if phase == _PHASE_CORPUS:
        return [{"label": "Add first document", "command": "bewley add corpus/<filename>"}]
    if phase == _PHASE_OPEN_CODING:
        return [
            {"label": "Package EDSL open-coding jobs", "command": "bewley open-coding jobs --output jobs.ep"},
            {"label": "See getting-started docs", "command": "bewley docs show getting-started"},
        ]
    if phase == _PHASE_ANNOTATING:
        return [
            {"label": "Preview applying reviewed candidates", "command": "bewley open-coding apply --dry-run"},
            {"label": "See workflow docs", "command": "bewley docs show workflow"},
        ]
    return [
        {"label": "See grounded theory docs", "command": "bewley docs show grounded-theory"},
        {"label": "Export theory", "command": "bewley export theory --format json --output theory.json"},
    ]


def _study_state(project: "Project | None") -> dict:
    """Study manifest summary; empty defaults for pre-study project indexes."""
    import sqlite3

    state = {"method": None, "unit_of_analysis": None, "research_questions": 0}
    if project is None:
        return state
    with project.connect() as conn:
        try:
            for row in conn.execute(
                "SELECT key, value FROM project_settings WHERE key IN ('study.method', 'study.unit_of_analysis')"
            ):
                state[row["key"].removeprefix("study.")] = row["value"]
            state["research_questions"] = conn.execute(
                "SELECT COUNT(*) FROM research_questions"
            ).fetchone()[0]
        except sqlite3.OperationalError:
            pass
    return state


def _phase_state(project: "Project | None", project_exists: bool) -> dict:
    phase = _infer_phase(project)
    counts: dict = {}
    if project:
        with project.connect() as conn:
            counts = {
                "documents": conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0],
                "codes": conn.execute("SELECT COUNT(*) FROM codes").fetchone()[0],
                "active_annotations": conn.execute(
                    "SELECT COUNT(*) FROM annotations WHERE is_active = 1"
                ).fetchone()[0],
            }
    study = _study_state(project)
    steps = _next_steps_for_phase(phase)
    # Declaring the study design comes before model-assisted coding, but never
    # blocks it: the suggestion is prepended, not substituted.
    if project and phase in (_PHASE_CORPUS, _PHASE_OPEN_CODING):
        if not study["method"]:
            steps = [{
                "label": "Declare the study design",
                "command": "bewley study set --method <method> --unit <unit-of-analysis>",
            }] + steps
        elif study["research_questions"] == 0:
            steps = [{
                "label": "Record the research question",
                "command": 'bewley question add "<question>"',
            }] + steps
    return {
        "phase": phase,
        "project_exists": project_exists,
        "counts": counts,
        "study": study,
        "checklist": _PHASE_CHECKLISTS.get(phase, []),
        "recommended_next_steps": steps,
        "primary_doc": _PHASE_DOCS.get(phase, "overview"),
    }
