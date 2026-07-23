from __future__ import annotations

import re
from importlib import resources

DOCS: dict[str, dict] = {
    "overview": {
        "title": "Bewley Overview",
        "summary": "What bewley does, its data model, and output conventions.",
        "file": "overview.md",
    },
    "getting-started": {
        "title": "Getting Started",
        "summary": "Installation, project setup, and the first coding workflow.",
        "file": "getting-started.md",
    },
    "workflow": {
        "title": "Workflow Phases and Checklist",
        "summary": "The five project phases, full checklist, and recovery commands.",
        "file": "workflow.md",
    },
    "commands": {
        "title": "Command Reference",
        "summary": "Complete reference for all bewley commands and subcommands.",
        "file": "commands.md",
    },
    "grounded-theory": {
        "title": "Grounded Theory with Bewley",
        "summary": "Open coding, constant comparison, axial and selective coding, memos, and theory export.",
        "file": "grounded-theory.md",
    },
}


def load_doc(topic: str) -> str:
    meta = DOCS[topic]
    pkg = resources.files("bewley").joinpath("docs_content")
    return pkg.joinpath(meta["file"]).read_text(encoding="utf-8")


def search_docs(query: str) -> list[dict]:
    terms = re.findall(r"[A-Za-z0-9_-]+", query.lower())
    results = []
    for topic, meta in DOCS.items():
        text = load_doc(topic)
        haystack = f"{topic} {meta['title']} {meta['summary']} {text}".lower()
        score = sum(haystack.count(t) for t in terms)
        if score > 0:
            snippet = ""
            for term in terms:
                idx = haystack.find(term)
                if idx >= 0:
                    start = max(0, idx - 90)
                    end = min(len(text), idx + 200)
                    snippet = text[start:end].strip()
                    break
            results.append({**meta, "topic": topic, "score": score, "snippet": snippet})
    return sorted(results, key=lambda r: r["score"], reverse=True)
