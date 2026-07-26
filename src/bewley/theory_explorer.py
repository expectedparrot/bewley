"""Generator for the standalone interactive theory-explorer script."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from .html_export import code_explorer_payload


_THEORY_EXPLORER_HTML_TEMPLATE = r'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>__TITLE__</title>
<style>
  * { box-sizing: border-box; }
  html, body { margin: 0; padding: 0; height: 100%; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: #222; }
  header { padding: 8px 16px; border-bottom: 1px solid #ddd; display: flex; align-items: baseline; gap: 16px; flex-wrap: wrap; }
  header h1 { margin: 0; font-size: 1.05rem; }
  header .summary { color: #666; font-size: 0.85rem; }
  main { display: grid; grid-template-columns: 240px 1fr 360px; height: calc(100vh - 44px); }
  aside { overflow-y: auto; padding: 10px 12px; border-right: 1px solid #eee; font-size: 0.88rem; }
  aside.right { border-right: none; border-left: 1px solid #eee; }
  aside h2 { font-size: 0.78rem; text-transform: uppercase; color: #888; letter-spacing: 0.05em; margin: 14px 0 6px; }
  aside h2:first-child { margin-top: 0; }
  .filter-group label { display: block; padding: 2px 0; cursor: pointer; font-size: 0.85rem; }
  .filter-group input[type=checkbox] { margin-right: 6px; vertical-align: middle; }
  .swatch { display: inline-block; width: 10px; height: 10px; border-radius: 2px; margin-right: 6px; vertical-align: middle; border: 1px solid #0002; }
  svg { width: 100%; height: 100%; display: block; background: #fafafa; }
  .node circle { stroke: #333; stroke-width: 1.2; cursor: pointer; }
  .node.core circle { stroke: #c0f; stroke-width: 3.5px; }
  .node.selected circle { stroke: #000; stroke-width: 3px; }
  .node text { font-size: 11px; pointer-events: none; user-select: none; paint-order: stroke; stroke: #fff; stroke-width: 3px; stroke-linejoin: round; }
  .edge { fill: none; }
  .edge.hierarchy { stroke: #ccc; stroke-width: 1; stroke-dasharray: 3 3; }
  .edge.typed { stroke: #555; stroke-width: 1.4; }
  .edge-label { font-size: 10px; fill: #555; pointer-events: none; paint-order: stroke; stroke: #fafafa; stroke-width: 3px; }
  .dimmed { opacity: 0.12; }
  #detail h3 { margin: 0 0 4px; font-size: 1rem; }
  #detail .meta { color: #666; font-size: 0.82rem; }
  #detail .links { margin-top: 6px; font-size: 0.82rem; }
  #detail .links .rel { color: #555; font-style: italic; }
  #detail .links a { color: #06c; text-decoration: none; }
  #detail .links a:hover { text-decoration: underline; }
  #detail .desc { color: #444; font-size: 0.88rem; margin: 6px 0; }
  .quote { border-left: 3px solid #888; padding: 5px 8px; margin: 6px 0; background: #f6f6f6; font-size: 0.85rem; }
  .quote .loc { color: #888; font-size: 0.72rem; margin-top: 3px; }
  .status-warn { color: #a60; font-weight: bold; }
  .legend { position: absolute; bottom: 10px; right: 10px; background: rgba(255,255,255,0.92); padding: 6px 10px; border-radius: 4px; font-size: 0.78rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1); max-width: 240px; }
  .legend .item { display: flex; align-items: center; gap: 6px; margin: 1px 0; }
  button.reset { font-size: 0.78rem; margin-top: 6px; padding: 2px 8px; }
  input[type=range] { width: 100%; }
  .center-wrap { position: relative; height: 100%; }
  .count-badge { color: #888; font-size: 0.75rem; }
</style>
</head>
<body>
<header>
  <h1>__TITLE__</h1>
  <span class="summary" id="summary"></span>
</header>
<main>
  <aside class="left">
    <h2>Category</h2>
    <div class="filter-group" id="category-filter"></div>
    <h2>Document</h2>
    <div class="filter-group" id="document-filter"></div>
    <h2>Min annotations</h2>
    <input type="range" id="count-slider" min="0" value="0">
    <div><span id="count-value">0</span>+ annotations</div>
    <button class="reset" onclick="window.__resetFilters()">Reset filters</button>
  </aside>
  <section>
    <div class="center-wrap">
      <svg id="graph"></svg>
      <div class="legend" id="legend"></div>
    </div>
  </section>
  <aside class="right">
    <div id="detail"><em style="color:#888">Click a node to see its quotes.</em></div>
  </aside>
</main>
<script>window.PAYLOAD = __PAYLOAD_JSON__;</script>
<script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
<script>
(function(){
  const P = window.PAYLOAD;
  const centerEl = document.querySelector('section');
  const W = centerEl.clientWidth;
  const H = centerEl.clientHeight;

  const byId = new Map(P.codes.map(c => [c.code_id, c]));
  function rootOf(codeId) {
    let cur = byId.get(codeId);
    const seen = new Set();
    while (cur && cur.parent_code_id && byId.has(cur.parent_code_id) && !seen.has(cur.code_id)) {
      seen.add(cur.code_id);
      cur = byId.get(cur.parent_code_id);
    }
    return cur ? cur.code_id : codeId;
  }
  P.codes.forEach(c => { c.root_code_id = rootOf(c.code_id); });
  P.codes.forEach(c => { c.root_name = (byId.get(c.root_code_id) || c).name; });

  const roots = Array.from(new Set(P.codes.map(c => c.root_code_id))).sort((a, b) => {
    const an = byId.get(a).name, bn = byId.get(b).name;
    return an.localeCompare(bn);
  });
  const palette = d3.schemeTableau10.concat(d3.schemeSet3).concat(d3.schemePastel1);
  const color = d3.scaleOrdinal().domain(roots).range(palette);

  const snippetCount = P.snippets.length;
  const docPaths = Array.from(new Set(P.snippets.map(s => s.document_path))).sort();
  document.getElementById('summary').textContent =
    P.codes.length + ' codes · ' + snippetCount + ' annotations · ' + docPaths.length + ' documents' +
    (P.core_code_id && byId.get(P.core_code_id) ? ' · core: ' + byId.get(P.core_code_id).name : '');

  const rootCats = roots.map(rid => ({
    id: rid,
    name: (byId.get(rid) || {name: rid}).name,
    color: color(rid),
    count: P.codes.filter(c => c.root_code_id === rid).length,
  })).sort((a, b) => b.count - a.count);

  const catEl = document.getElementById('category-filter');
  rootCats.forEach(rc => {
    const lbl = document.createElement('label');
    lbl.innerHTML = '<input type="checkbox" checked data-cat="' + rc.id + '">' +
      '<span class="swatch" style="background:' + rc.color + '"></span>' +
      escapeHtml(rc.name) + ' <span class="count-badge">(' + rc.count + ')</span>';
    catEl.appendChild(lbl);
  });

  const docEl = document.getElementById('document-filter');
  docPaths.forEach(d => {
    const lbl = document.createElement('label');
    const short = d.split('/').pop();
    const n = P.snippets.filter(s => s.document_path === d).length;
    lbl.innerHTML = '<input type="checkbox" checked data-doc="' + escapeHtml(d) + '">' +
      escapeHtml(short) + ' <span class="count-badge">(' + n + ')</span>';
    docEl.appendChild(lbl);
  });

  const maxCount = d3.max(P.codes, c => c.annotation_count) || 0;
  const slider = document.getElementById('count-slider');
  slider.max = maxCount;
  slider.addEventListener('input', () => {
    document.getElementById('count-value').textContent = slider.value;
    applyFilters();
  });
  catEl.addEventListener('change', applyFilters);
  docEl.addEventListener('change', applyFilters);

  const svg = d3.select('#graph').attr('viewBox', [0, 0, W, H]);
  svg.append('defs').append('marker').attr('id', 'arrow').attr('viewBox', '0 -5 10 10')
    .attr('refX', 22).attr('refY', 0).attr('markerWidth', 6).attr('markerHeight', 6)
    .attr('orient', 'auto').append('path').attr('d', 'M0,-5L10,0L0,5').attr('fill', '#555');

  const g = svg.append('g');
  svg.call(d3.zoom().scaleExtent([0.2, 4]).on('zoom', e => g.attr('transform', e.transform)));

  const nodes = P.codes.map(c => Object.assign({}, c));
  const hierarchyEdges = P.codes
    .filter(c => c.parent_code_id && byId.has(c.parent_code_id))
    .map(c => ({source: c.parent_code_id, target: c.code_id, kind: 'hierarchy'}));
  const typedEdges = P.links.map(l => ({
    source: l.source_code_id, target: l.target_code_id,
    kind: 'typed', relationship: l.relationship, memo: l.memo
  }));
  const edges = hierarchyEdges.concat(typedEdges);

  function radius(d) { return 7 + Math.sqrt(d.annotation_count || 0) * 4; }

  const sim = d3.forceSimulation(nodes)
    .force('link', d3.forceLink(edges).id(d => d.code_id).distance(d => d.kind === 'hierarchy' ? 55 : 90).strength(d => d.kind === 'hierarchy' ? 0.6 : 0.3))
    .force('charge', d3.forceManyBody().strength(-260))
    .force('center', d3.forceCenter(W/2, H/2))
    .force('collide', d3.forceCollide(d => radius(d) + 6))
    .force('x', d3.forceX(W/2).strength(0.03))
    .force('y', d3.forceY(H/2).strength(0.03));

  const linkSel = g.selectAll('.edge').data(edges).join('path')
    .attr('class', d => 'edge ' + d.kind)
    .attr('marker-end', d => d.kind === 'typed' ? 'url(#arrow)' : null);

  const linkLabelSel = g.selectAll('.edge-label').data(typedEdges).join('text')
    .attr('class', 'edge-label')
    .attr('text-anchor', 'middle')
    .text(d => d.relationship);

  const nodeSel = g.selectAll('.node').data(nodes).join('g')
    .attr('class', d => 'node' + (d.code_id === P.core_code_id ? ' core' : ''))
    .call(d3.drag()
      .on('start', (event, d) => { if (!event.active) sim.alphaTarget(0.25).restart(); d.fx = d.x; d.fy = d.y; })
      .on('drag', (event, d) => { d.fx = event.x; d.fy = event.y; })
      .on('end', (event, d) => { if (!event.active) sim.alphaTarget(0); d.fx = null; d.fy = null; }));

  nodeSel.append('circle')
    .attr('r', radius)
    .attr('fill', d => color(d.root_code_id))
    .on('click', (_, d) => selectCode(d.code_id));

  nodeSel.append('title').text(d => d.name + ' (' + (d.annotation_count || 0) + ')' + (d.description ? '\n' + d.description : ''));

  nodeSel.append('text')
    .attr('dx', d => radius(d) + 3).attr('dy', 4).text(d => d.name);

  sim.on('tick', () => {
    linkSel.attr('d', d => 'M' + d.source.x + ',' + d.source.y + 'L' + d.target.x + ',' + d.target.y);
    linkLabelSel
      .attr('x', d => (d.source.x + d.target.x) / 2)
      .attr('y', d => (d.source.y + d.target.y) / 2 - 2);
    nodeSel.attr('transform', d => 'translate(' + d.x + ',' + d.y + ')');
  });

  const legendEl = document.getElementById('legend');
  rootCats.forEach(rc => {
    const div = document.createElement('div');
    div.className = 'item';
    div.innerHTML = '<span class="swatch" style="background:' + rc.color + '"></span>' + escapeHtml(rc.name);
    legendEl.appendChild(div);
  });
  if (P.core_code_id && byId.get(P.core_code_id)) {
    const div = document.createElement('div');
    div.className = 'item';
    div.innerHTML = '<span class="swatch" style="background:#fff;border:2px solid #c0f"></span>core category';
    legendEl.appendChild(div);
  }

  function applyFilters() {
    const activeCats = new Set(Array.from(catEl.querySelectorAll('input:checked')).map(i => i.dataset.cat));
    const activeDocs = new Set(Array.from(docEl.querySelectorAll('input:checked')).map(i => i.dataset.doc));
    const minCount = +slider.value;
    const visible = new Set();
    P.codes.forEach(c => {
      if (!activeCats.has(c.root_code_id)) return;
      if ((c.annotation_count || 0) < minCount) return;
      const snips = P.snippets.filter(s => s.code_id === c.code_id);
      if (snips.length === 0) { visible.add(c.code_id); return; }
      if (snips.some(s => activeDocs.has(s.document_path))) visible.add(c.code_id);
    });
    nodeSel.classed('dimmed', d => !visible.has(d.code_id));
    linkSel.classed('dimmed', d => {
      const sid = d.source.code_id || d.source;
      const tid = d.target.code_id || d.target;
      return !visible.has(sid) || !visible.has(tid);
    });
    linkLabelSel.classed('dimmed', d => {
      const sid = d.source.code_id || d.source;
      const tid = d.target.code_id || d.target;
      return !visible.has(sid) || !visible.has(tid);
    });
  }

  window.__resetFilters = function() {
    catEl.querySelectorAll('input').forEach(i => i.checked = true);
    docEl.querySelectorAll('input').forEach(i => i.checked = true);
    slider.value = 0;
    document.getElementById('count-value').textContent = '0';
    applyFilters();
  };

  let selectedId = null;
  function selectCode(codeId) {
    selectedId = codeId;
    const c = byId.get(codeId);
    if (!c) return;
    const quotes = P.snippets.filter(s => s.code_id === codeId);
    const outLinks = P.links.filter(l => l.source_code_id === codeId);
    const inLinks = P.links.filter(l => l.target_code_id === codeId);
    const parent = c.parent_code_id ? byId.get(c.parent_code_id) : null;
    const children = P.codes.filter(x => x.parent_code_id === codeId);

    let html = '<h3>' + escapeHtml(c.name) + (c.code_id === P.core_code_id ? ' <span style="color:#c0f">(core)</span>' : '') + '</h3>';
    html += '<div class="meta">';
    if (parent) html += 'parent: <a href="#" onclick="window.__select(\'' + parent.code_id + '\');return false">' + escapeHtml(parent.name) + '</a> · ';
    html += (c.annotation_count || 0) + ' annotation' + (c.annotation_count === 1 ? '' : 's');
    if (c.document_count) html += ' across ' + c.document_count + ' document' + (c.document_count === 1 ? '' : 's');
    html += '</div>';
    if (c.description) html += '<div class="desc">' + escapeHtml(c.description) + '</div>';
    if (children.length) {
      html += '<div class="links"><strong>children:</strong> ';
      html += children.map(ch => '<a href="#" onclick="window.__select(\'' + ch.code_id + '\');return false">' + escapeHtml(ch.name) + '</a>').join(', ');
      html += '</div>';
    }
    if (outLinks.length || inLinks.length) {
      html += '<div class="links" style="margin-top:8px">';
      outLinks.forEach(l => {
        const t = byId.get(l.target_code_id);
        html += '<div>→ <span class="rel">' + escapeHtml(l.relationship) + '</span> → ' +
          '<a href="#" onclick="window.__select(\'' + l.target_code_id + '\');return false">' + escapeHtml(t ? t.name : l.target_code_id) + '</a>' +
          (l.memo ? ' <span style="color:#888">(' + escapeHtml(l.memo) + ')</span>' : '') + '</div>';
      });
      inLinks.forEach(l => {
        const s = byId.get(l.source_code_id);
        html += '<div>' +
          '<a href="#" onclick="window.__select(\'' + l.source_code_id + '\');return false">' + escapeHtml(s ? s.name : l.source_code_id) + '</a>' +
          ' ← <span class="rel">' + escapeHtml(l.relationship) + '</span> ←' +
          (l.memo ? ' <span style="color:#888">(' + escapeHtml(l.memo) + ')</span>' : '') + '</div>';
      });
      html += '</div>';
    }
    if (quotes.length) {
      html += '<h2 style="margin-top:14px">Quotes (' + quotes.length + ')</h2>';
      quotes.forEach(q => {
        const status = (q.anchor_status && q.anchor_status !== 'clean') ? ' <span class="status-warn">[' + escapeHtml(q.anchor_status) + ']</span>' : '';
        const text = q.exact_text || '<document-level>';
        let loc = '';
        if (q.start_line != null) {
          loc = 'L' + q.start_line + (q.end_line && q.end_line !== q.start_line ? '–' + q.end_line : '');
        }
        html += '<div class="quote">' + escapeHtml(text) +
          '<div class="loc">' + escapeHtml(q.document_path) + (loc ? ' ' + loc : '') + status + '</div></div>';
      });
    } else {
      html += '<p style="color:#888;margin-top:10px">No quote anchors — this node is a parent category.</p>';
    }
    document.getElementById('detail').innerHTML = html;
    nodeSel.classed('selected', d => d.code_id === codeId);
  }
  window.__select = selectCode;

  function escapeHtml(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  }

  applyFilters();
})();
</script>
</body>
</html>
'''


_THEORY_EXPLORER_SCRIPT_BODY = r'''

def main() -> None:
    import sys
    html = TEMPLATE.replace("__PAYLOAD_JSON__", PAYLOAD_JSON)
    OUTPUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_HTML.write_text(html, encoding="utf-8")
    print("Wrote " + str(OUTPUT_HTML), file=sys.stderr)


if __name__ == "__main__":
    main()
'''


def _collect_theory_explorer_payload(project: "Project") -> dict[str, Any]:
    """Collect structure + snippets for the interactive theory explorer."""
    theory = project.export_theory_json()
    explorer = code_explorer_payload(project)
    meta_by_id = {c["code_id"]: c for c in explorer["codes"]}
    codes = []
    for c in theory["codes"]:
        meta = meta_by_id.get(c["code_id"], {})
        codes.append({
            "code_id": c["code_id"],
            "name": c["name"],
            "description": c.get("description") or "",
            "parent_code_id": c.get("parent_code_id"),
            "annotation_count": meta.get("annotation_count", c.get("annotation_count", 0)),
            "document_count": meta.get("document_count", 0),
        })
    snippets = [{
        "code_id": s["code_id"],
        "code_name": s["code_name"],
        "document_path": s["document_path"],
        "scope_type": s["scope_type"],
        "start_line": s.get("start_line"),
        "end_line": s.get("end_line"),
        "anchor_status": s.get("anchor_status"),
        "exact_text": s.get("exact_text"),
    } for s in explorer["snippets"]]
    links = [{
        "source_code_id": l["source_code_id"],
        "target_code_id": l["target_code_id"],
        "relationship": l["relationship"],
        "memo": l.get("memo"),
    } for l in theory["links"]]
    core_code_id = theory["core_category"]["code_id"] if theory["core_category"] else None
    return {
        "codes": codes,
        "snippets": snippets,
        "links": links,
        "core_code_id": core_code_id,
        "project_name": project.root.name,
    }


def _build_theory_explorer_script(project: "Project", project_dir: Path, output_html: Path, title: str | None) -> str:
    """Generate the content of the interactive theory-explorer script."""
    import json as _json
    from html import escape as _html_escape

    payload = _collect_theory_explorer_payload(project)
    resolved_title = title or f"Theory explorer · {project.root.name}"
    payload_json = _json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
    template = _THEORY_EXPLORER_HTML_TEMPLATE.replace("__TITLE__", _html_escape(resolved_title))

    header_lines = [
        '"""',
        "Generated by: bewley codegen theory-explorer",
        f"Project: {project_dir}",
        f"Codes: {len(payload['codes'])}  Annotations: {len(payload['snippets'])}",
        "",
        "Run this script to produce the interactive HTML theory explorer.",
        "  python <this script>",
        "",
        "Customization:",
        "  - Edit TEMPLATE below to change CSS / layout / JS behavior.",
        "  - Regenerate (bewley codegen theory-explorer) when codes or annotations change.",
        '"""',
        "from __future__ import annotations",
        "",
        "from pathlib import Path",
        "",
        f"PROJECT_DIR = Path({repr(str(project_dir))})",
        f"OUTPUT_HTML = Path({repr(str(output_html))})",
        "",
        f"PAYLOAD_JSON = {repr(payload_json)}",
        "",
        f"TEMPLATE = {repr(template)}",
        "",
    ]
    return "\n".join(header_lines) + _THEORY_EXPLORER_SCRIPT_BODY
