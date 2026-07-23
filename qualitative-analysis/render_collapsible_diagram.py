#!/usr/bin/env python3
"""Render a bewley theory export as a collapsible HTML diagram.

Takes the JSON output of `bewley export theory --format json` and a YAML
narrative-flow spec to produce a self-contained HTML page where:

  - The theory's causal chain is laid out top-to-bottom as labeled sections.
  - Each theme is a colored, collapsible block showing its child codes on click.
  - Axial relationships appear nested inside each theme.
  - The core category and terminal outcome are highlighted.

The flow spec tells the script how to group themes into narrative layers
(e.g., "bargain formed by", "violated by", "compounded by"). Without a
flow spec, themes are listed in a single flat section.

Usage:
  # Generate with a narrative flow spec
  python render_collapsible_diagram.py theory.json \\
      --flow flow.yml \\
      --title "My Grounded Theory" \\
      -o theory_interactive.html

  # Auto-layout (flat, no narrative layers)
  python render_collapsible_diagram.py theory.json -o theory_interactive.html

  # Pipe from bewley
  bewley export theory --format json | python render_collapsible_diagram.py - -o theory.html

Flow spec format (YAML):
  title: "The Bargain Betrayed"
  subtitle: "Click any theme to expand. A grounded theory of driver retention."
  core_label: "THE BARGAIN BETRAYED"
  core_description: "Drivers enter an implicit bargain that is systematically violated."
  outcome_code: "dont_waste_your_time"   # code name for terminal outcome
  layers:
    - label: "The bargain is formed by"
      themes: [pay_as_the_hook, job_of_last_resort, flexibility_as_the_bargain]
      color: "#4CAF50"
    - label: "The bargain is violated by"
      themes: [schedule_instability, workload_and_speed, physical_toll]
      color: "#FF5722"
    - label: "Compounded by"
      themes: [just_a_number, management_failure]
      color: null  # use each theme's own color
    - label: "Structurally enabled by"
      themes: [contractor_shield, route_challenges]
      color: null
"""

from __future__ import annotations

import argparse
import html as html_mod
import json
import sys
from pathlib import Path

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

# ── Color palette for themes ───────────────────────────────────────────────────

PALETTE = [
    "#4CAF50", "#2196F3", "#FF9800", "#FF5722", "#F44336",
    "#E91E63", "#9C27B0", "#795548", "#607D8B", "#00BCD4",
]


def _esc(s: str) -> str:
    return html_mod.escape(s)


REL_COLORS = {
    "causes": "#E53935",
    "enables": "#43A047",
    "co-occurs-with": "#1E88E5",
    "context-for": "#FB8C00",
    "constrains": "#F4511E",
    "consequence-of": "#8E24AA",
    "dimension-of": "#546E7A",
    "strategy-for": "#00897B",
    "violated-by": "#D81B60",
}


def build_html(data: dict, flow: dict | None, title: str) -> str:
    """Build the full HTML string."""
    code_map = {c["code_id"]: c for c in data["codes"]}
    name_map = {c["name"]: c for c in data["codes"]}
    core_id = data["core_category"]["code_id"] if data.get("core_category") else None
    core_name = data["core_category"]["name"] if data.get("core_category") else None

    # Build parent -> children mapping
    children_of: dict[str, list[dict]] = {}
    for c in data["codes"]:
        pid = c["parent_code_id"]
        if pid:
            children_of.setdefault(pid, []).append(c)

    # Identify parent (theme) codes
    parent_ids = {c["parent_code_id"] for c in data["codes"] if c["parent_code_id"]}
    themes = [c for c in data["codes"] if c["code_id"] in parent_ids]
    theme_id_map = {c["code_id"]: c for c in themes}

    # Build link index: for each code, collect links where it or its children are involved
    links_by_theme: dict[str, list[dict]] = {}
    for link in data.get("links", []):
        src = code_map.get(link["source_code_id"])
        tgt = code_map.get(link["target_code_id"])
        if not src or not tgt:
            continue
        # Find which theme(s) this link belongs to
        src_theme = src["parent_code_id"] or src["code_id"]
        tgt_theme = tgt["parent_code_id"] or tgt["code_id"]
        for tid in {src_theme, tgt_theme}:
            links_by_theme.setdefault(tid, []).append({
                "source": src["name"],
                "target": tgt["name"],
                "relationship": link["relationship"],
            })

    # Deduplicate links per theme
    for tid in links_by_theme:
        seen = set()
        deduped = []
        for lnk in links_by_theme[tid]:
            key = (lnk["source"], lnk["relationship"], lnk["target"])
            if key not in seen:
                seen.add(key)
                deduped.append(lnk)
        links_by_theme[tid] = deduped

    # Determine layout
    if flow:
        subtitle = flow.get("subtitle", "Click any theme to expand its codes and relationships.")
        core_label = flow.get("core_label", core_name or "CORE CATEGORY")
        core_desc = flow.get("core_description", "")
        outcome_name = flow.get("outcome_code")
        layers = flow.get("layers", [])
    else:
        subtitle = "Click any theme to expand its codes and relationships."
        core_label = core_name.upper().replace("_", " ") if core_name else "CORE CATEGORY"
        core_desc = code_map[core_id]["description"] if core_id and core_id in code_map else ""
        outcome_name = None
        layers = [{"label": "Themes", "themes": [t["name"] for t in themes], "color": None}]

    # Assign colors to themes
    theme_colors: dict[str, str] = {}
    color_idx = 0
    for layer in layers:
        layer_color = layer.get("color")
        for tname in layer.get("themes", []):
            if layer_color:
                theme_colors[tname] = layer_color
            else:
                theme_colors[tname] = PALETTE[color_idx % len(PALETTE)]
                color_idx += 1

    # ── Build HTML ──────────────────────────────────────────────────────────

    parts = []
    parts.append("""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>""" + _esc(title) + """</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #fafafa; color: #333; padding: 24px; }
h1 { text-align: center; font-size: 1.6em; margin-bottom: 6px; color: #222; }
.subtitle { text-align: center; font-size: 0.9em; color: #888; margin-bottom: 24px; }
.diagram { max-width: 960px; margin: 0 auto; }
.core {
  background: linear-gradient(135deg, #E91E63, #C2185B);
  color: white; text-align: center; padding: 18px 24px;
  border-radius: 12px; font-size: 1.3em; font-weight: bold;
  margin: 0 auto 28px; max-width: 400px;
  box-shadow: 0 4px 16px rgba(233,30,99,0.3);
}
.core .desc { font-size: 0.6em; font-weight: normal; opacity: 0.9; margin-top: 6px; line-height: 1.4; }
.flow-label {
  text-align: center; font-size: 0.8em; text-transform: uppercase;
  letter-spacing: 2px; color: #999; margin: 20px 0 12px;
}
.flow-arrow { text-align: center; font-size: 1.4em; color: #ccc; margin: 8px 0; }
.theme {
  border-radius: 10px; margin-bottom: 12px;
  border: 2px solid; overflow: hidden;
  transition: box-shadow 0.2s;
}
.theme:hover { box-shadow: 0 2px 12px rgba(0,0,0,0.1); }
.theme-header {
  padding: 12px 16px; cursor: pointer; display: flex;
  align-items: center; justify-content: space-between;
  user-select: none;
}
.theme-header h3 { font-size: 1em; color: white; flex: 1; }
.theme-header .badge {
  background: rgba(255,255,255,0.25); color: white;
  font-size: 0.75em; padding: 2px 8px; border-radius: 10px;
  margin-left: 8px; white-space: nowrap;
}
.theme-header .chevron {
  color: rgba(255,255,255,0.7); font-size: 1.2em; margin-left: 10px;
  transition: transform 0.2s;
}
.theme.open .chevron { transform: rotate(90deg); }
.theme-body { display: none; background: white; padding: 12px 16px; }
.theme.open .theme-body { display: block; }
.code-row {
  display: flex; align-items: center; padding: 6px 0;
  border-bottom: 1px solid #f0f0f0;
}
.code-row:last-child { border-bottom: none; }
.code-name { font-family: "SF Mono", Menlo, monospace; font-size: 0.85em; font-weight: 600; min-width: 200px; color: #444; }
.code-desc { font-size: 0.82em; color: #777; flex: 1; padding: 0 12px; }
.code-count {
  font-size: 0.75em; font-weight: bold; color: white;
  border-radius: 10px; padding: 2px 8px;
  min-width: 24px; text-align: center;
}
.relationships { margin-top: 6px; padding-top: 8px; border-top: 1px solid #eee; }
.relationships summary { font-size: 0.8em; color: #999; cursor: pointer; padding: 4px 0; }
.rel-list { padding: 6px 0; }
.rel-item { font-size: 0.8em; color: #666; padding: 3px 0; line-height: 1.4; }
.rel-type {
  display: inline-block; font-size: 0.7em; padding: 1px 6px;
  border-radius: 8px; color: white; margin: 0 4px; font-weight: 600;
}
""")

    # Emit CSS for each relationship type
    for rel, color in REL_COLORS.items():
        css_class = rel.replace("-", "-")
        parts.append(f'.rel-{css_class} {{ background: {color}; }}\n')

    parts.append("""
.theme-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; align-items: start; }
@media (max-width: 700px) { .theme-grid { grid-template-columns: 1fr; } }
.outcome {
  background: linear-gradient(135deg, #795548, #5D4037);
  color: white; text-align: center; padding: 16px 24px;
  border-radius: 12px; font-size: 1.1em; font-weight: bold;
  margin: 0 auto; max-width: 400px;
  box-shadow: 0 4px 12px rgba(121,85,72,0.3);
}
.outcome .desc { font-size: 0.65em; font-weight: normal; opacity: 0.9; margin-top: 4px; }
.legend { max-width: 960px; margin: 24px auto 0; padding: 12px 16px; background: white; border-radius: 8px; border: 1px solid #eee; }
.legend h4 { font-size: 0.85em; color: #666; margin-bottom: 8px; }
.legend-items { display: flex; flex-wrap: wrap; gap: 8px; }
.legend-item { font-size: 0.75em; display: flex; align-items: center; gap: 4px; }
.legend-dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; }
</style>
</head>
<body>
""")

    parts.append(f'<h1>{_esc(title)}</h1>\n')
    parts.append(f'<p class="subtitle">{_esc(subtitle)}</p>\n')
    parts.append('<div class="diagram">\n')

    # Core category
    parts.append(f'<div class="core">{_esc(core_label)}<div class="desc">{_esc(core_desc)}</div></div>\n')

    # Render each layer
    for i, layer in enumerate(layers):
        label = layer.get("label", "")
        theme_names = layer.get("themes", [])

        if label:
            if i > 0:
                parts.append('<div class="flow-arrow">&#8595;</div>\n')
            parts.append(f'<div class="flow-label">{_esc(label)}</div>\n')

        use_grid = len(theme_names) >= 2
        if use_grid:
            parts.append('<div class="theme-grid">\n')

        for tname in theme_names:
            tc = name_map.get(tname)
            if not tc:
                continue
            tid = tc["code_id"]
            color = theme_colors.get(tname, "#607D8B")
            display_name = tname.replace("_", " ").title()
            children = children_of.get(tid, [])
            total_ann = sum(ch["annotation_count"] for ch in children)
            theme_links = links_by_theme.get(tid, [])
            el_id = f"theme-{tname.replace('_', '-')}"

            parts.append(f'  <div class="theme" style="border-color: {color};" id="{el_id}">\n')
            parts.append(f'    <div class="theme-header" style="background: {color};" onclick="toggle(\'{el_id}\')">\n')
            parts.append(f'      <h3>{_esc(display_name)}</h3>\n')
            parts.append(f'      <span class="badge">{total_ann} annotations</span>\n')
            parts.append(f'      <span class="chevron">&#9654;</span>\n')
            parts.append(f'    </div>\n')
            parts.append(f'    <div class="theme-body">\n')

            for ch in sorted(children, key=lambda c: -c["annotation_count"]):
                parts.append(f'      <div class="code-row">')
                parts.append(f'<span class="code-name">{_esc(ch["name"])}</span>')
                parts.append(f'<span class="code-desc">{_esc(ch.get("description", ""))}</span>')
                parts.append(f'<span class="code-count" style="background:{color}">{ch["annotation_count"]}</span>')
                parts.append(f'</div>\n')

            if theme_links:
                parts.append(f'      <details class="relationships"><summary>{len(theme_links)} relationship{"s" if len(theme_links) != 1 else ""}</summary><div class="rel-list">\n')
                for lnk in theme_links:
                    rel = lnk["relationship"]
                    rel_css = f"rel-{rel}"
                    rel_label = rel.replace("-", " ")
                    parts.append(f'        <div class="rel-item">{_esc(lnk["source"])} <span class="rel-type {rel_css}">{_esc(rel_label)}</span> {_esc(lnk["target"])}</div>\n')
                parts.append(f'      </div></details>\n')

            parts.append(f'    </div>\n')
            parts.append(f'  </div>\n')

        if use_grid:
            parts.append('</div>\n')

    # Outcome block
    if outcome_name and outcome_name in name_map:
        oc = name_map[outcome_name]
        parts.append('<div class="flow-arrow">&#8595;</div>\n')
        parts.append('<div class="flow-label">Which results in</div>\n')
        parts.append(f'<div class="outcome">{_esc(outcome_name)}<div class="desc">{_esc(oc.get("description", ""))} ({oc["annotation_count"]} annotations)</div></div>\n')

    parts.append('</div>\n')  # close .diagram

    # Legend
    used_rels = set()
    for links in links_by_theme.values():
        for lnk in links:
            used_rels.add(lnk["relationship"])
    if used_rels:
        parts.append('<div class="legend"><h4>Relationship types</h4><div class="legend-items">\n')
        for rel in sorted(used_rels):
            color = REL_COLORS.get(rel, "#999")
            parts.append(f'  <div class="legend-item"><span class="legend-dot" style="background:{color}"></span> {_esc(rel)}</div>\n')
        parts.append('</div></div>\n')

    # Script
    parts.append("""
<script>
function toggle(id) {
  document.getElementById(id).classList.toggle('open');
}
document.addEventListener('keydown', function(e) {
  if (e.key === 'a' && (e.metaKey || e.ctrlKey)) {
    e.preventDefault();
    var themes = document.querySelectorAll('.theme');
    var allOpen = Array.from(themes).every(function(t) { return t.classList.contains('open'); });
    themes.forEach(function(t) { allOpen ? t.classList.remove('open') : t.classList.add('open'); });
  }
});
</script>
</body>
</html>
""")

    return "".join(parts)


def main():
    parser = argparse.ArgumentParser(
        description="Render a bewley theory.json as a collapsible HTML diagram."
    )
    parser.add_argument("theory_json", help="Path to theory.json (or - for stdin)")
    parser.add_argument("--flow", help="YAML file describing narrative layer order")
    parser.add_argument("--title", default="Grounded Theory", help="Page title")
    parser.add_argument("-o", "--output", required=True, help="Output HTML path")
    args = parser.parse_args()

    # Load theory JSON
    if args.theory_json == "-":
        data = json.load(sys.stdin)
    else:
        data = json.loads(Path(args.theory_json).read_text())

    # Load flow spec
    flow = None
    if args.flow:
        if not HAS_YAML:
            print("ERROR: PyYAML required for --flow. Install with: pip install pyyaml", file=sys.stderr)
            sys.exit(1)
        flow = yaml.safe_load(Path(args.flow).read_text())

    html = build_html(data, flow, args.title)
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(html)
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
