#!/usr/bin/env python3
"""Render a bewley theory export as a rich diagram.

Takes the JSON output of `bewley export theory --format json` and produces
either a static SVG (via Graphviz dot) or an interactive HTML page (D3).

Usage:
  bewley export theory --format json --output theory.json

  # Static SVG (requires graphviz installed)
  python render_theory_diagram.py theory.json --format svg --output theory.svg

  # Interactive HTML (no dependencies beyond a browser)
  python render_theory_diagram.py theory.json --format html --output theory.html

  # Pipe from bewley
  bewley export theory --format json | python render_theory_diagram.py - --format html -o theory.html
"""

from __future__ import annotations

import argparse
import html as html_mod
import json
import subprocess
import shutil
import sys
from pathlib import Path


# ── Graphviz / DOT rendering ─────────────────────────────────────────────────

def _dot_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def build_dot(data: dict, title: str) -> str:
    """Build a Graphviz DOT string from theory JSON."""
    code_map = {c["code_id"]: c for c in data["codes"]}
    core_id = data["core_category"]["code_id"] if data.get("core_category") else None

    # Identify parent groups
    parent_ids = {c["parent_code_id"] for c in data["codes"] if c["parent_code_id"]}

    # Group children by parent
    children_by_parent: dict[str | None, list[dict]] = {}
    for c in data["codes"]:
        children_by_parent.setdefault(c["parent_code_id"], []).append(c)

    max_ann = max((c["annotation_count"] for c in data["codes"]), default=1) or 1

    # Color palette for groups
    group_colors = [
        "#4CAF50", "#2196F3", "#FF9800", "#9C27B0", "#F44336",
        "#00BCD4", "#795548", "#607D8B", "#E91E63", "#3F51B5",
    ]
    parent_color = {}
    for i, pid in enumerate(sorted(parent_ids)):
        parent_color[pid] = group_colors[i % len(group_colors)]

    def node_id(code_id: str) -> str:
        return "n_" + code_id.replace("-", "_")

    def node_attrs(c: dict) -> str:
        name = c["name"].replace("_", " ")
        ann = c["annotation_count"]
        desc = c.get("description") or ""
        is_core = c["code_id"] == core_id

        # Scale: fontsize 11-18, width 1.2-2.5
        scale = ann / max_ann
        fontsize = 11 + scale * 7
        node_width = 1.2 + scale * 1.3
        penwidth = 3 if is_core else 1.5

        # Tooltip
        tip = f"{name} ({ann} annotations)"
        if desc:
            tip += f"\\n{desc}"

        # Color
        color = "#FFD700" if is_core else parent_color.get(
            c["parent_code_id"],
            parent_color.get(c["code_id"], "#999999"),
        )
        fillcolor = color + "30"  # translucent fill

        shape = "doubleoctagon" if is_core else "box"
        style = "filled,bold" if is_core else "filled,rounded"

        label = f"{name}\\n({ann})"

        attrs = (
            f'label="{_dot_escape(label)}", '
            f'tooltip="{_dot_escape(tip)}", '
            f'shape={shape}, style="{style}", '
            f'fillcolor="{fillcolor}", color="{color}", '
            f'fontsize={fontsize:.0f}, width={node_width:.2f}, '
            f'penwidth={penwidth}'
        )
        return attrs

    lines = [
        f'digraph theory {{',
        f'  label="{_dot_escape(title)}";',
        f'  labelloc=t; fontsize=20; fontname="Helvetica";',
        f'  rankdir=TB;',
        f'  node [fontname="Helvetica", margin="0.15,0.08"];',
        f'  edge [fontname="Helvetica", fontsize=10];',
        f'  newrank=true;',
        f'  nodesep=0.6; ranksep=1.0;',
        f'',
    ]

    # Emit nodes grouped by parent in subgraphs
    emitted = set()
    for pid in sorted(parent_ids):
        parent = code_map.get(pid)
        if not parent:
            continue
        color = parent_color.get(pid, "#999999")
        group_label = parent["name"].replace("_", " ").upper()
        lines.append(f'  subgraph cluster_{node_id(pid)} {{')
        lines.append(f'    label="{_dot_escape(group_label)}";')
        lines.append(f'    style=dashed; color="{color}"; fontcolor="{color}";')
        lines.append(f'    fontsize=12;')
        # Parent node itself
        lines.append(f'    {node_id(pid)} [{node_attrs(parent)}];')
        emitted.add(pid)
        # Children
        for child in children_by_parent.get(pid, []):
            lines.append(f'    {node_id(child["code_id"])} [{node_attrs(child)}];')
            emitted.add(child["code_id"])
        lines.append(f'  }}')
        lines.append(f'')

    # Emit ungrouped nodes
    for c in data["codes"]:
        if c["code_id"] not in emitted:
            lines.append(f'  {node_id(c["code_id"])} [{node_attrs(c)}];')

    lines.append(f'')

    # Hierarchy edges (dashed)
    for h in data["hierarchy"]:
        lines.append(
            f'  {node_id(h["parent"])} -> {node_id(h["child"])} '
            f'[style=dashed, color="#bbbbbb", arrowsize=0.7];'
        )

    # Relationship edges (solid, labeled)
    rel_colors = {
        "causes": "#E53935",
        "enables": "#43A047",
        "produces": "#FB8C00",
        "results in": "#FB8C00",
        "context-for": "#1E88E5",
        "strategy-for": "#8E24AA",
        "violated by": "#D81B60",
        "constrains": "#F4511E",
        "dimension-of": "#546E7A",
    }

    for lk in data["links"]:
        rel = lk["relationship"]
        color = rel_colors.get(rel, "#666666")
        tip = rel
        if lk.get("memo"):
            tip += f"\\n{lk['memo']}"
        lines.append(
            f'  {node_id(lk["source_code_id"])} -> {node_id(lk["target_code_id"])} '
            f'[label=" {_dot_escape(rel)} ", color="{color}", '
            f'fontcolor="{color}", tooltip="{_dot_escape(tip)}", '
            f'penwidth=1.5, arrowsize=0.8];'
        )

    lines.append(f'}}')
    return "\n".join(lines)


def render_graphviz(dot_src: str, output: Path, fmt: str = "svg") -> None:
    """Run dot to produce SVG or PNG."""
    dot_bin = shutil.which("dot")
    if not dot_bin:
        print(
            "Error: graphviz 'dot' not found on PATH.\n"
            "Install with: brew install graphviz  (or apt-get install graphviz)",
            file=sys.stderr,
        )
        sys.exit(1)
    result = subprocess.run(
        [dot_bin, f"-T{fmt}"],
        input=dot_src, capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"dot error: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    output.write_text(result.stdout, encoding="utf-8")


# ── Interactive HTML / D3 rendering ──────────────────────────────────────────

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{title}</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, sans-serif;
         background: #fafafa; color: #333; }}
  #graph {{ width: 100vw; height: 100vh; }}
  svg {{ width: 100%; height: 100%; }}

  #tooltip {{
    position: absolute; pointer-events: none; opacity: 0;
    background: #fff; border: 1px solid #ccc; border-radius: 6px;
    padding: 10px 14px; max-width: 340px; font-size: 13px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.12); line-height: 1.45;
    transition: opacity 0.15s;
  }}
  #tooltip h3 {{ margin-bottom: 4px; font-size: 14px; }}
  #tooltip .desc {{ color: #555; margin-bottom: 4px; }}
  #tooltip .meta {{ color: #888; font-size: 12px; }}
  #tooltip .memo {{ margin-top: 6px; padding-top: 6px; border-top: 1px solid #eee;
                    font-style: italic; color: #666; font-size: 12px; }}

  #legend {{
    position: absolute; top: 16px; left: 16px; background: #fff;
    border: 1px solid #ddd; border-radius: 6px; padding: 12px 16px;
    font-size: 12px; line-height: 1.8; box-shadow: 0 2px 8px rgba(0,0,0,0.08);
  }}
  #legend .swatch {{ display: inline-block; width: 12px; height: 12px;
                     border-radius: 3px; margin-right: 6px; vertical-align: middle; }}

  #title-bar {{
    position: absolute; top: 16px; left: 50%; transform: translateX(-50%);
    background: #fff; border: 1px solid #ddd; border-radius: 6px;
    padding: 8px 20px; font-size: 16px; font-weight: 600;
    box-shadow: 0 2px 8px rgba(0,0,0,0.08); text-align: center;
  }}
  #title-bar .subtitle {{ font-size: 12px; font-weight: 400; color: #888; }}

  .edge-label {{ font-size: 11px; fill: #888; pointer-events: none; }}
  .node-label {{ font-size: 12px; font-weight: 500; pointer-events: none; text-anchor: middle; }}
  .node-label.core {{ font-weight: 700; font-size: 14px; }}
</style>
</head>
<body>
<div id="graph"></div>
<div id="tooltip"></div>
<div id="title-bar">{title_html}</div>

<script src="https://d3js.org/d3.v7.min.js"></script>
<script>
const data = {data_json};

const codeMap = new Map(data.codes.map(c => [c.code_id, c]));
const memosByCode = new Map();
for (const m of data.memos) {{
  if (m.target_type === "code") {{
    if (!memosByCode.has(m.target_id)) memosByCode.set(m.target_id, []);
    memosByCode.get(m.target_id).push(m);
  }}
}}

const parentIds = new Set(data.codes.filter(c => c.parent_code_id).map(c => c.parent_code_id));

const groupColors = [
  "#4CAF50", "#2196F3", "#FF9800", "#9C27B0", "#F44336",
  "#00BCD4", "#795548", "#607D8B", "#E91E63", "#3F51B5",
];
const groups = new Map();
let gi = 0;
for (const pid of parentIds) groups.set(pid, gi++);

const coreId = data.core_category ? data.core_category.code_id : null;
const maxAnn = Math.max(1, ...data.codes.map(c => c.annotation_count));

const nodes = data.codes.map(c => {{
  const isCore = c.code_id === coreId;
  const isParent = parentIds.has(c.code_id);
  const groupIdx = c.parent_code_id ? groups.get(c.parent_code_id) :
                   isParent ? groups.get(c.code_id) : -1;
  const color = groupIdx >= 0 ? groupColors[groupIdx % groupColors.length] : "#999";
  const r = 14 + (c.annotation_count / maxAnn) * 26 + (isCore ? 10 : 0);
  return {{
    id: c.code_id, name: c.name, description: c.description || "",
    annotation_count: c.annotation_count, parent_code_id: c.parent_code_id,
    isCore, isParent, groupIdx, color, r,
    memos: memosByCode.get(c.code_id) || [],
  }};
}});

const nodeMap = new Map(nodes.map(n => [n.id, n]));

const links = [];
for (const h of data.hierarchy) {{
  links.push({{ source: h.parent, target: h.child, relationship: "parent-of", isHierarchy: true }});
}}
for (const lk of data.links) {{
  links.push({{ source: lk.source_code_id, target: lk.target_code_id,
                relationship: lk.relationship, memo: lk.memo, isHierarchy: false }});
}}

const width = window.innerWidth;
const height = window.innerHeight;

const svg = d3.select("#graph").append("svg").attr("viewBox", [0, 0, width, height]);

svg.append("defs").selectAll("marker")
  .data(["arrow-hierarchy", "arrow-link"])
  .join("marker")
    .attr("id", d => d)
    .attr("viewBox", "0 -5 10 10")
    .attr("refX", 20).attr("refY", 0)
    .attr("markerWidth", 6).attr("markerHeight", 6)
    .attr("orient", "auto")
  .append("path")
    .attr("d", "M0,-5L10,0L0,5")
    .attr("fill", d => d === "arrow-hierarchy" ? "#bbb" : "#999");

const g = svg.append("g");
svg.call(d3.zoom().scaleExtent([0.2, 4]).on("zoom", (e) => g.attr("transform", e.transform)));

const linkG = g.append("g").selectAll("g").data(links).join("g");

linkG.append("line")
  .attr("stroke", d => d.isHierarchy ? "#ccc" : "#aaa")
  .attr("stroke-width", d => d.isHierarchy ? 1.5 : 2)
  .attr("stroke-dasharray", d => d.isHierarchy ? "6,4" : "none")
  .attr("marker-end", d => d.isHierarchy ? "url(#arrow-hierarchy)" : "url(#arrow-link)");

linkG.append("text").attr("class", "edge-label")
  .text(d => d.isHierarchy ? "" : d.relationship);

const nodeG = g.append("g").selectAll("g").data(nodes).join("g")
  .call(d3.drag()
    .on("start", (e,d) => {{ if (!e.active) sim.alphaTarget(0.3).restart(); d.fx=d.x; d.fy=d.y; }})
    .on("drag", (e,d) => {{ d.fx=e.x; d.fy=e.y; }})
    .on("end", (e,d) => {{ if (!e.active) sim.alphaTarget(0); d.fx=null; d.fy=null; }}));

nodeG.filter(d => d.isCore).append("circle")
  .attr("r", d => d.r + 6).attr("fill", "none")
  .attr("stroke", "#FFD700").attr("stroke-width", 3)
  .attr("stroke-dasharray", "4,3").attr("opacity", 0.7);

nodeG.append("circle")
  .attr("r", d => d.r).attr("fill", d => d.color)
  .attr("stroke", d => d.isCore ? "#FFD700" : "#fff")
  .attr("stroke-width", d => d.isCore ? 3 : 2).attr("opacity", 0.9);

nodeG.filter(d => d.annotation_count > 0).append("text")
  .attr("dy", 4).attr("text-anchor", "middle").attr("fill", "#fff")
  .attr("font-size", d => d.isCore ? 13 : 11).attr("font-weight", 600)
  .text(d => d.annotation_count);

nodeG.append("text")
  .attr("class", d => "node-label" + (d.isCore ? " core" : ""))
  .attr("dy", d => d.r + 16)
  .text(d => d.name.replace(/_/g, " "));

const tooltip = d3.select("#tooltip");
nodeG
  .on("mouseover", (event, d) => {{
    let h = `<h3>${{d.name.replace(/_/g, " ")}}</h3>`;
    if (d.description) h += `<div class="desc">${{d.description}}</div>`;
    h += `<div class="meta">${{d.annotation_count}} annotation${{d.annotation_count !== 1 ? "s" : ""}}`;
    if (d.isCore) h += ` &middot; <strong>Core category</strong>`;
    if (d.isParent) h += ` &middot; Parent category`;
    h += `</div>`;
    for (const m of d.memos) {{
      h += `<div class="memo">${{m.title || "Memo"}}</div>`;
    }}
    tooltip.html(h).style("opacity", 1);
  }})
  .on("mousemove", (e) => tooltip.style("left", (e.pageX+14)+"px").style("top", (e.pageY-10)+"px"))
  .on("mouseout", () => tooltip.style("opacity", 0));

const sim = d3.forceSimulation(nodes)
  .force("link", d3.forceLink(links).id(d => d.id).distance(140).strength(0.4))
  .force("charge", d3.forceManyBody().strength(-500))
  .force("center", d3.forceCenter(width / 2, height / 2))
  .force("collision", d3.forceCollide().radius(d => d.r + 20))
  .force("x", d3.forceX(width / 2).strength(0.04))
  .force("y", d3.forceY(height / 2).strength(0.04))
  .on("tick", () => {{
    linkG.select("line")
      .attr("x1", d => d.source.x).attr("y1", d => d.source.y)
      .attr("x2", d => d.target.x).attr("y2", d => d.target.y);
    linkG.select("text")
      .attr("x", d => (d.source.x + d.target.x) / 2)
      .attr("y", d => (d.source.y + d.target.y) / 2 - 6);
    nodeG.attr("transform", d => `translate(${{d.x}},${{d.y}})`);
  }});
</script>
</body>
</html>"""


def render_html(data: dict, title: str, output: Path) -> None:
    subtitle_parts = [
        f"{len(data.get('codes', []))} codes",
        f"{len(data.get('links', []))} relationships",
        f"{sum(c.get('annotation_count', 0) for c in data.get('codes', []))} annotations",
    ]
    subtitle = " &middot; ".join(subtitle_parts)
    title_html = f"{html_mod.escape(title)}<div class='subtitle'>{subtitle}</div>"

    page = HTML_TEMPLATE.format(
        title=html_mod.escape(title),
        title_html=title_html,
        data_json=json.dumps(data, ensure_ascii=False),
    )
    output.write_text(page, encoding="utf-8")


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Render a bewley theory JSON export as a diagram.",
    )
    parser.add_argument(
        "input", type=str,
        help="Path to theory JSON file, or '-' to read from stdin.",
    )
    parser.add_argument(
        "--format", "-f", choices=["svg", "html", "dot", "png"], default="html",
        help="Output format (default: html).",
    )
    parser.add_argument(
        "--output", "-o", type=Path, default=None,
        help="Output file path. Defaults to theory_diagram.<format>.",
    )
    parser.add_argument(
        "--title", type=str, default=None,
        help="Diagram title. Defaults to 'Grounded Theory: <core category>'.",
    )
    args = parser.parse_args()

    if args.input == "-":
        data = json.load(sys.stdin)
    else:
        data = json.loads(Path(args.input).read_text(encoding="utf-8"))

    core_name = (
        data["core_category"]["name"].replace("_", " ").title()
        if data.get("core_category") else "Untitled"
    )
    title = args.title or f"Grounded Theory: {core_name}"

    fmt = args.format
    output = args.output or Path(f"theory_diagram.{fmt}")

    if fmt == "html":
        render_html(data, title, output)
    elif fmt == "dot":
        dot_src = build_dot(data, title)
        output.write_text(dot_src, encoding="utf-8")
    elif fmt in ("svg", "png"):
        dot_src = build_dot(data, title)
        render_graphviz(dot_src, output, fmt)
    else:
        print(f"Unknown format: {fmt}", file=sys.stderr)
        sys.exit(1)

    print(f"Wrote {output}")


if __name__ == "__main__":
    main()
