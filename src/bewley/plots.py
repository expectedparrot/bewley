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
AMBER = "#b0803f"

# Distinguishable code colors for multi-code plots; codes beyond the palette
# share the final gray.
PALETTE = [
    "#428a5f", "#b0803f", "#5f7fb8", "#a8577e", "#5f9ea8", "#8a6fb8",
    "#c2704f", "#7a9a4f", "#b85f5f", "#4f7a8a", "#9a8a4f", "#7f7f7f",
]


def plot_data(project: Project) -> dict[str, Any]:
    """Return the compact counts underlying every plot."""
    with project.connect() as conn:
        codes = conn.execute(
            """
            SELECT c.code_id, c.canonical_name, c.color,
                   COUNT(DISTINCT CASE WHEN a.is_active = 1 THEN a.annotation_id END) annotations,
                   COUNT(DISTINCT CASE WHEN a.is_active = 1 THEN a.document_id END) documents
            FROM codes c
            LEFT JOIN annotations a ON a.code_id = c.code_id
            WHERE c.status = 'active'
            GROUP BY c.code_id, c.canonical_name, c.color
            ORDER BY annotations DESC, c.canonical_name
            """
        ).fetchall()
        documents = conn.execute(
            """
            SELECT d.document_id, d.current_path,
                   COUNT(DISTINCT CASE WHEN a.is_active = 1 THEN a.annotation_id END) annotations,
                   COUNT(DISTINCT CASE WHEN a.is_active = 1 THEN a.code_id END) codes,
                   (SELECT r.byte_length FROM document_revisions r
                    WHERE r.document_id = d.document_id AND r.is_current = 1) byte_length
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
        # Annotations keep the code they were created under; merged codes
        # resolve to their terminal target so plots agree with `code list`.
        resolution = project.merge_resolution_map(conn)
        matrix_counts: dict[tuple[str, str], int] = {}
        positions: list[dict[str, Any]] = []
        for row in conn.execute(
            """
            SELECT code_id, document_id, start_byte
            FROM annotations WHERE is_active = 1
            ORDER BY document_id, start_byte
            """
        ):
            code_id = resolution.get(row["code_id"], row["code_id"])
            key = (code_id, row["document_id"])
            matrix_counts[key] = matrix_counts.get(key, 0) + 1
            if row["start_byte"] is not None:
                positions.append({
                    "document_id": row["document_id"],
                    "code_id": code_id,
                    "start_byte": row["start_byte"],
                })
        matrix = [
            {"code_id": code_id, "document_id": document_id, "annotations": count}
            for (code_id, document_id), count in sorted(matrix_counts.items())
        ]
    return {
        "generated_at": utcnow(),
        "codes": [dict(row) for row in codes],
        "documents": [dict(row) for row in documents],
        "cooccurrence": [dict(row) for row in pairs],
        "matrix": matrix,
        "annotation_positions": positions,
        "events": _analytic_events(project),
        "review": _review_outcomes(project),
    }


def _analytic_events(project: Project) -> list[dict[str, Any]]:
    """The event log reduced to what analytic-history plots need, in order."""
    out: list[dict[str, Any]] = []
    for path in sorted(project.events_dir.glob("*.json")):
        event = json.loads(path.read_text(encoding="utf-8"))
        payload = event.get("payload", {})
        entry: dict[str, Any] = {
            "sequence": event["sequence_number"],
            "type": event["event_type"],
        }
        if event["event_type"] == "code_created":
            entry["name"] = payload.get("canonical_name")
        elif event["event_type"] == "code_renamed":
            entry["name"] = payload.get("new_name")
        elif event["event_type"] == "code_split":
            entry["name"] = payload.get("new_canonical_name")
        elif event["event_type"] == "code_merged":
            entry["sources"] = len(payload.get("source_code_ids", []))
        elif event["event_type"] == "annotation_added":
            entry["code_id"] = payload.get("code_id")
        out.append(entry)
    return out


def _review_outcomes(project: Project) -> dict[str, dict[str, int]]:
    """Per-code proposed and applied counts from the open-coding sidecar logs."""
    proposed: dict[str, int] = {}
    applied: dict[str, int] = {}
    base = project.root / "qualitative-analysis"
    ingest_log = base / "ingest_log.jsonl"
    if ingest_log.is_file():
        for line in ingest_log.read_text(encoding="utf-8").splitlines():
            for candidate in json.loads(line).get("candidates", []):
                name = candidate.get("code_name") or "(unnamed)"
                proposed[name] = proposed.get(name, 0) + 1
    apply_log = base / "apply_log.jsonl"
    if apply_log.is_file():
        for line in apply_log.read_text(encoding="utf-8").splitlines():
            for item in json.loads(line).get("applied", []):
                name = item.get("code_name") or "(unnamed)"
                applied[name] = applied.get(name, 0) + 1
    return {"proposed": proposed, "applied": applied}


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


def _short_doc_label(path: str) -> str:
    name = Path(path).name
    return name[:-4] if name.endswith(".txt") else name


def _code_colors(data: dict[str, Any]) -> dict[str, str]:
    """Stable color per code: the stored color if set, else the palette in prevalence order."""
    colors: dict[str, str] = {}
    for index, row in enumerate(data["codes"]):
        stored = row.get("color")
        colors[row["code_id"]] = stored or PALETTE[min(index, len(PALETTE) - 1)]
    return colors


def code_document_matrix_svg(data: dict[str, Any]) -> str:
    codes = data["codes"][:14]
    documents = sorted(data["documents"], key=lambda row: row["current_path"])
    lookup = {(row["code_id"], row["document_id"]): int(row["annotations"]) for row in data["matrix"]}
    maximum = max(lookup.values(), default=1) or 1
    cell, left, top = 30, 235, 205
    width = left + len(documents) * cell + 110
    height = top + len(codes) * cell + 35
    parts = []
    for index, document in enumerate(documents):
        x = left + index * cell
        label = _short_doc_label(document["current_path"])
        parts.append(f'<text class="label" transform="translate({x + 19},{top - 8}) rotate(-55)" text-anchor="start">{html.escape(label)}</text>')
    for row_index, code in enumerate(codes):
        y = top + row_index * cell
        parts.append(f'<text class="label" x="{left - 8}" y="{y + 19}" text-anchor="end">{html.escape(code["canonical_name"])}</text>')
        for column_index, document in enumerate(documents):
            value = lookup.get((code["code_id"], document["document_id"]), 0)
            x = left + column_index * cell
            opacity = 0.07 if value == 0 else 0.25 + 0.75 * value / maximum
            parts.append(f'<rect x="{x}" y="{y}" width="{cell - 2}" height="{cell - 2}" rx="2" fill="{GREEN}" opacity="{opacity:.2f}"/>')
            if value:
                fill = "white" if value / maximum > 0.55 else INK
                parts.append(f'<text class="label" x="{x + (cell - 2) / 2}" y="{y + 19}" text-anchor="middle" fill="{fill}">{value}</text>')
    return _svg(width, height, "Code × document matrix",
                "".join(parts),
                "Annotation counts for each code (rows) in each document (columns, in corpus order).")


def code_discovery_svg(data: dict[str, Any]) -> str:
    steps: list[tuple[int, int]] = []
    seen: set[str] = set()
    count = 0
    for event in data["events"]:
        if event["type"] != "annotation_added":
            continue
        count += 1
        code_id = event.get("code_id")
        if code_id and code_id not in seen:
            seen.add(code_id)
        steps.append((count, len(seen)))
    width, height, left, right, top, bottom = 860, 320, 70, 40, 58, 46
    plot_w, plot_h = width - left - right, height - top - bottom
    max_x = max((s[0] for s in steps), default=1) or 1
    max_y = max((s[1] for s in steps), default=1) or 1
    parts = [f'<line class="axis" x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" y2="{top + plot_h}"/>',
             f'<line class="axis" x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_h}"/>']
    for tick in range(0, max_y + 1, max(1, max_y // 5)):
        y = top + plot_h - tick / max_y * plot_h
        parts.append(f'<line class="axis" x1="{left - 4}" y1="{y:.1f}" x2="{left + plot_w}" y2="{y:.1f}" opacity="0.6"/>')
        parts.append(f'<text class="value" x="{left - 9}" y="{y + 4:.1f}" text-anchor="end">{tick}</text>')
    points: list[str] = [f"{left},{top + plot_h}"]
    for x_value, y_value in steps:
        x = left + x_value / max_x * plot_w
        y = top + plot_h - y_value / max_y * plot_h
        points.append(f"{x:.1f},{y:.1f}")
    parts.append(f'<polyline points="{" ".join(points)}" fill="none" stroke="{GREEN}" stroke-width="2.5"/>')
    if steps:
        final_x = left + steps[-1][0] / max_x * plot_w
        final_y = top + plot_h - steps[-1][1] / max_y * plot_h
        parts.append(f'<circle cx="{final_x:.1f}" cy="{final_y:.1f}" r="4" fill="{GREEN}"/>')
        parts.append(f'<text class="value" x="{final_x - 6:.1f}" y="{final_y - 9:.1f}" text-anchor="end">{steps[-1][1]} distinct codes after {steps[-1][0]} annotations</text>')
    parts.append(f'<text class="value" x="{left + plot_w / 2}" y="{height - 12}" text-anchor="middle">annotations, in the order they were applied</text>')
    parts.append(f'<text class="value" transform="translate(20,{top + plot_h / 2}) rotate(-90)" text-anchor="middle">distinct codes</text>')
    return _svg(width, height, "Code discovery",
                "".join(parts),
                "Cumulative count of distinct codes as annotations were applied, in event-log order. A flat tail means recent material introduced no new codes; whether that constitutes saturation is a methodological judgment the researcher makes, not a fact this plot establishes.")


def review_funnel_svg(data: dict[str, Any]) -> str:
    review = data.get("review", {})
    proposed, applied = review.get("proposed", {}), review.get("applied", {})
    names = sorted(set(proposed) | set(applied), key=lambda name: (-proposed.get(name, 0), name))
    width, left, right, top, row_h = 860, 235, 205, 58, 32
    height = max(150, top + len(names) * row_h + 30)
    maximum = max([*proposed.values(), *applied.values(), 1])
    scale = (width - left - right) / maximum
    parts = []
    for index, name in enumerate(names):
        y = top + index * row_h
        proposed_n, applied_n = proposed.get(name, 0), applied.get(name, 0)
        parts.extend([
            f'<text class="label" x="{left - 10}" y="{y + 15}" text-anchor="end">{html.escape(name)}</text>',
            f'<rect x="{left}" y="{y}" width="{proposed_n * scale:.1f}" height="18" rx="2" fill="{GREEN_LIGHT}"/>',
            f'<rect x="{left}" y="{y + 4}" width="{applied_n * scale:.1f}" height="10" rx="2" fill="{GREEN}"/>',
            f'<text class="value" x="{left + proposed_n * scale + 7:.1f}" y="{y + 14}">{proposed_n} proposed · {applied_n} applied</text>',
        ])
    return _svg(width, height, "Review outcomes by proposed code",
                "".join(parts),
                "For each code the model proposed during open coding: candidates proposed (light) versus annotations applied after human review (dark). Codes with zero applied were rejected at review.")


def annotation_positions_svg(data: dict[str, Any]) -> str:
    colors = _code_colors(data)
    documents = sorted(data["documents"], key=lambda row: row["current_path"])
    lengths = {row["document_id"]: int(row["byte_length"] or 0) for row in documents}
    by_document: dict[str, list[dict[str, Any]]] = {}
    for row in data["annotation_positions"]:
        by_document.setdefault(row["document_id"], []).append(row)
    max_length = max(lengths.values(), default=1) or 1
    width, left, right, top, row_h = 860, 235, 40, 58, 26
    track_w = width - left - right
    legend_codes = data["codes"][:12]
    legend_rows = (len(legend_codes) + 2) // 3
    height = top + len(documents) * row_h + 30 + legend_rows * 22 + 10
    parts = []
    for index, document in enumerate(documents):
        y = top + index * row_h
        length = lengths.get(document["document_id"], 0)
        bar_w = track_w * (length / max_length) if length else track_w * 0.02
        parts.append(f'<text class="label" x="{left - 10}" y="{y + 14}" text-anchor="end">{html.escape(_short_doc_label(document["current_path"]))}</text>')
        parts.append(f'<rect x="{left}" y="{y + 3}" width="{bar_w:.1f}" height="14" rx="2" fill="{GRID}"/>')
        for annotation in by_document.get(document["document_id"], []):
            position = int(annotation["start_byte"]) / (length or 1)
            x = left + min(position, 0.995) * bar_w
            color = colors.get(annotation["code_id"], PALETTE[-1])
            parts.append(f'<rect x="{x:.1f}" y="{y + 1}" width="4" height="18" rx="1" fill="{color}"/>')
    legend_y = top + len(documents) * row_h + 24
    for index, code in enumerate(legend_codes):
        x = left + (index % 3) * 210
        y = legend_y + (index // 3) * 22
        parts.append(f'<rect x="{x}" y="{y - 10}" width="12" height="12" rx="2" fill="{colors[code["code_id"]]}"/>')
        parts.append(f'<text class="value" x="{x + 18}" y="{y}">{html.escape(code["canonical_name"])}</text>')
    return _svg(width, height, "Where codes appear within documents",
                "".join(parts),
                "One track per document (length proportional to its size, in corpus order); each colored tick marks an annotation at its byte position. Clusters show where in a document a theme concentrates.")


def codebook_evolution_svg(data: dict[str, Any]) -> str:
    steps: list[tuple[int, int]] = []
    markers: list[tuple[int, int, str, str]] = []
    active = 0
    for event in data["events"]:
        kind = event["type"]
        if kind == "code_created":
            active += 1
        elif kind == "code_split":
            active += 1
        elif kind == "code_merged":
            active -= int(event.get("sources", 0))
        elif kind != "code_renamed":
            continue
        steps.append((event["sequence"], active))
        if kind == "code_merged":
            markers.append((event["sequence"], active, AMBER, "merge"))
        elif kind == "code_split":
            markers.append((event["sequence"], active, "#5f7fb8", "split"))
        elif kind == "code_renamed":
            markers.append((event["sequence"], active, MUTED, "rename"))
    width, height, left, right, top, bottom = 860, 320, 70, 40, 58, 46
    plot_w, plot_h = width - left - right, height - top - bottom
    max_x = max((event["sequence"] for event in data["events"]), default=1) or 1
    max_y = max((s[1] for s in steps), default=1) or 1
    parts = [f'<line class="axis" x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" y2="{top + plot_h}"/>',
             f'<line class="axis" x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_h}"/>']
    for tick in range(0, max_y + 1, max(1, max_y // 5)):
        y = top + plot_h - tick / max_y * plot_h
        parts.append(f'<line class="axis" x1="{left - 4}" y1="{y:.1f}" x2="{left + plot_w}" y2="{y:.1f}" opacity="0.6"/>')
        parts.append(f'<text class="value" x="{left - 9}" y="{y + 4:.1f}" text-anchor="end">{tick}</text>')

    def scale_point(sequence: int, value: int) -> tuple[float, float]:
        return (left + sequence / max_x * plot_w, top + plot_h - value / max_y * plot_h)

    points = [f"{left},{top + plot_h}"]
    previous_y = top + plot_h
    for sequence, value in steps:
        x, y = scale_point(sequence, value)
        points.append(f"{x:.1f},{previous_y:.1f}")
        points.append(f"{x:.1f},{y:.1f}")
        previous_y = y
    points.append(f"{left + plot_w},{previous_y:.1f}")
    parts.append(f'<polyline points="{" ".join(points)}" fill="none" stroke="{GREEN}" stroke-width="2.5"/>')
    for sequence, value, color, label in markers:
        x, y = scale_point(sequence, value)
        parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="{color}"/>')
        parts.append(f'<text class="value" x="{x:.1f}" y="{y - 10:.1f}" text-anchor="middle" fill="{color}">{label}</text>')
    if steps:
        parts.append(f'<text class="value" x="{left + plot_w - 4}" y="{previous_y - 9:.1f}" text-anchor="end">{steps[-1][1]} active codes</text>')
    parts.append(f'<text class="value" x="{left + plot_w / 2}" y="{height - 12}" text-anchor="middle">analytic history (event sequence)</text>')
    parts.append(f'<text class="value" transform="translate(20,{top + plot_h / 2}) rotate(-90)" text-anchor="middle">active codes</text>')
    return _svg(width, height, "Codebook evolution",
                "".join(parts),
                "Active codebook size over the project's append-only event history, with merge, split, and rename decisions marked. Drawn from the event log, so it reflects the real order of analytic decisions.")


def export_plots(project: Project, output_dir: Path) -> dict[str, Any]:
    data = plot_data(project)
    output_dir.mkdir(parents=True, exist_ok=True)
    review = data.get("review", {})
    has_review = bool(review.get("proposed") or review.get("applied"))
    builders = {
        "code_prevalence": ("code-prevalence.svg", code_prevalence_svg),
        "document_density": ("document-density.svg", document_density_svg),
        "code_cooccurrence": ("code-cooccurrence.svg", cooccurrence_svg),
        "code_document_matrix": ("code-document-matrix.svg", code_document_matrix_svg),
        "code_discovery": ("code-discovery.svg", code_discovery_svg),
        "review_funnel": ("review-funnel.svg", review_funnel_svg),
        "annotation_positions": ("annotation-positions.svg", annotation_positions_svg),
        "codebook_evolution": ("codebook-evolution.svg", codebook_evolution_svg),
    }
    if not has_review:
        # No open-coding sidecar logs (ingest_log/apply_log) — nothing to plot.
        builders.pop("review_funnel")
    outputs = {}
    for name, (filename, builder) in builders.items():
        outputs[name] = output_dir / filename
        outputs[name].write_text(builder(data), encoding="utf-8")
    manifest = output_dir / "plots.json"
    manifest.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return {
        "output_dir": str(output_dir),
        "plots": {name: str(path) for name, path in outputs.items()},
        "manifest": str(manifest),
        "code_count": len(data["codes"]),
        "document_count": len(data["documents"]),
    }
