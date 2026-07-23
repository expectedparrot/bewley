"""Dependency-free SVG plots for qualitative coding diagnostics."""
from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any

from bewley.project import Project, utcnow


GREEN = "#428a5f"
GREEN_LIGHT = "#8fc8a5"
INK = "#1a1a1a"
MUTED = "#666666"
GRID = "#e4e4e4"


def plot_data(project: Project) -> dict[str, Any]:
    """Return the compact counts underlying every plot."""
    with project.connect() as conn:
        codes = conn.execute(
            """
            SELECT c.code_id, c.canonical_name,
                   COUNT(DISTINCT CASE WHEN a.is_active = 1 THEN a.annotation_id END) annotations,
                   COUNT(DISTINCT CASE WHEN a.is_active = 1 THEN a.document_id END) documents
            FROM codes c
            LEFT JOIN annotations a ON a.code_id = c.code_id
            WHERE c.status = 'active'
            GROUP BY c.code_id, c.canonical_name
            ORDER BY annotations DESC, c.canonical_name
            """
        ).fetchall()
        documents = conn.execute(
            """
            SELECT d.document_id, d.current_path,
                   COUNT(DISTINCT CASE WHEN a.is_active = 1 THEN a.annotation_id END) annotations,
                   COUNT(DISTINCT CASE WHEN a.is_active = 1 THEN a.code_id END) codes
            FROM documents d
            LEFT JOIN annotations a ON a.document_id = d.document_id
            WHERE d.archived_at IS NULL
            GROUP BY d.document_id, d.current_path
            ORDER BY annotations DESC, d.current_path
            """
        ).fetchall()
        pairs = conn.execute(
            """
            SELECT a.code_id left_id, b.code_id right_id, COUNT(DISTINCT a.document_id) documents
            FROM annotations a
            JOIN annotations b ON b.document_id = a.document_id
              AND b.code_id >= a.code_id AND b.is_active = 1
            WHERE a.is_active = 1
            GROUP BY a.code_id, b.code_id
            """
        ).fetchall()
    return {
        "generated_at": utcnow(),
        "codes": [dict(row) for row in codes],
        "documents": [dict(row) for row in documents],
        "cooccurrence": [dict(row) for row in pairs],
    }


def _svg(width: int, height: int, title: str, body: str, description: str) -> str:
    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-labelledby="title desc">
<title id="title">{html.escape(title)}</title><desc id="desc">{html.escape(description)}</desc>
<rect width="100%" height="100%" fill="white"/>
<style>text{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;fill:{INK}}}.title{{font:600 19px Georgia,serif}}.label{{font-size:12px}}.value{{font-size:11px;fill:{MUTED}}}.axis{{stroke:{GRID};stroke-width:1}}</style>
<text class="title" x="24" y="31">{html.escape(title)}</text>
{body}
</svg>
"""


def code_prevalence_svg(data: dict[str, Any]) -> str:
    rows = data["codes"]
    width, left, right, top, row_h = 860, 205, 165, 58, 32
    height = max(150, top + len(rows) * row_h + 30)
    maximum = max((int(row["annotations"]) for row in rows), default=1) or 1
    scale = (width - left - right) / maximum
    parts = []
    for index, row in enumerate(rows):
        y = top + index * row_h
        annotation_width = int(row["annotations"]) * scale
        document_width = int(row["documents"]) * scale
        parts.extend([
            f'<text class="label" x="{left - 10}" y="{y + 15}" text-anchor="end">{html.escape(row["canonical_name"])}</text>',
            f'<rect x="{left}" y="{y}" width="{annotation_width:.1f}" height="18" rx="2" fill="{GREEN}"/>',
            f'<rect x="{left}" y="{y + 19}" width="{document_width:.1f}" height="5" rx="2" fill="{GREEN_LIGHT}"/>',
            f'<text class="value" x="{left + annotation_width + 7:.1f}" y="{y + 14}">{row["annotations"]} annotations · {row["documents"]} documents</text>',
        ])
    return _svg(width, height, "Code prevalence", "".join(parts), "Annotation and document counts for each active code.")


def document_density_svg(data: dict[str, Any]) -> str:
    rows = data["documents"]
    width, left, right, top, row_h = 860, 280, 165, 58, 28
    height = max(150, top + len(rows) * row_h + 26)
    maximum = max((int(row["annotations"]) for row in rows), default=1) or 1
    scale = (width - left - right) / maximum
    parts = []
    for index, row in enumerate(rows):
        y = top + index * row_h
        label = Path(row["current_path"]).name
        bar_width = int(row["annotations"]) * scale
        parts.extend([
            f'<text class="label" x="{left - 10}" y="{y + 14}" text-anchor="end">{html.escape(label)}</text>',
            f'<rect x="{left}" y="{y}" width="{bar_width:.1f}" height="18" rx="2" fill="{GREEN}"/>',
            f'<text class="value" x="{left + bar_width + 7:.1f}" y="{y + 14}">{row["annotations"]} annotations · {row["codes"]} codes</text>',
        ])
    return _svg(width, height, "Coding density by document", "".join(parts), "Annotation and distinct-code counts for each document.")


def cooccurrence_svg(data: dict[str, Any]) -> str:
    codes = data["codes"][:12]
    ids = [row["code_id"] for row in codes]
    labels = [row["canonical_name"] for row in codes]
    lookup: dict[tuple[str, str], int] = {}
    for row in data["cooccurrence"]:
        lookup[(row["left_id"], row["right_id"])] = int(row["documents"])
        lookup[(row["right_id"], row["left_id"])] = int(row["documents"])
    maximum = max((lookup.get((a, b), 0) for a in ids for b in ids), default=1) or 1
    cell, left, top = 38, 220, 210
    width, height = left + len(ids) * cell + 38, top + len(ids) * cell + 35
    parts = []
    for index, label in enumerate(labels):
        x, y = left + index * cell, top + index * cell
        parts.append(f'<text class="label" x="{left - 8}" y="{y + 24}" text-anchor="end">{html.escape(label)}</text>')
        parts.append(f'<text class="label" transform="translate({x + 23},{top - 8}) rotate(-55)" text-anchor="start">{html.escape(label)}</text>')
    for row_index, left_id in enumerate(ids):
        for column_index, right_id in enumerate(ids):
            value = lookup.get((left_id, right_id), 0)
            opacity = 0.08 if value == 0 else 0.2 + 0.8 * value / maximum
            x, y = left + column_index * cell, top + row_index * cell
            parts.append(f'<rect x="{x}" y="{y}" width="{cell - 2}" height="{cell - 2}" rx="2" fill="{GREEN}" opacity="{opacity:.2f}"/>')
            if value:
                parts.append(f'<text class="label" x="{x + (cell - 2) / 2}" y="{y + 23}" text-anchor="middle">{value}</text>')
    return _svg(width, height, "Code co-occurrence by document", "".join(parts), "Number of documents in which each pair of codes appears.")


def export_plots(project: Project, output_dir: Path) -> dict[str, Any]:
    data = plot_data(project)
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "code_prevalence": output_dir / "code-prevalence.svg",
        "document_density": output_dir / "document-density.svg",
        "code_cooccurrence": output_dir / "code-cooccurrence.svg",
    }
    outputs["code_prevalence"].write_text(code_prevalence_svg(data), encoding="utf-8")
    outputs["document_density"].write_text(document_density_svg(data), encoding="utf-8")
    outputs["code_cooccurrence"].write_text(cooccurrence_svg(data), encoding="utf-8")
    manifest = output_dir / "plots.json"
    manifest.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return {
        "output_dir": str(output_dir),
        "plots": {name: str(path) for name, path in outputs.items()},
        "manifest": str(manifest),
        "code_count": len(data["codes"]),
        "document_count": len(data["documents"]),
    }
