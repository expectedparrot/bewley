"""HTML explorer/viewer builders and their data payloads."""
from __future__ import annotations

import html
import json
from typing import Any

from .util import byte_to_char_index_map, coerce_code_color, safe_decode, soft_color, utcnow
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .project import Project


def code_explorer_payload(project: Project) -> dict[str, Any]:
    with project.connect() as conn:
        codes = conn.execute(
            "SELECT c.*, COUNT(DISTINCT CASE WHEN a.is_active = 1 THEN a.annotation_id END) AS annotation_count, COUNT(DISTINCT CASE WHEN a.is_active = 1 THEN a.document_id END) AS document_count FROM codes c LEFT JOIN annotations a ON a.code_id = c.code_id GROUP BY c.code_id ORDER BY annotation_count DESC, c.canonical_name"
        ).fetchall()
        alias_rows = conn.execute("SELECT code_id, alias_name FROM code_aliases ORDER BY alias_name").fetchall()
        annotations = conn.execute(
            "SELECT a.*, c.canonical_name, c.color, d.current_path FROM annotations a JOIN codes c ON c.code_id = a.code_id JOIN documents d ON d.document_id = a.document_id WHERE a.is_active = 1 ORDER BY c.canonical_name, d.current_path, COALESCE(a.start_line, 0), a.annotation_id"
        ).fetchall()
        doc_paths_needed = {row["current_path"] for row in annotations}
        doc_texts: dict[str, list[str]] = {}
        for dpath in doc_paths_needed:
            doc_row = conn.execute("SELECT document_id FROM documents WHERE current_path = ?", (dpath,)).fetchone()
            if doc_row:
                rev = project.current_revision(conn, doc_row["document_id"])
                content = (project.objects_dir / rev["content_sha256"]).read_bytes()
                doc_texts[dpath] = safe_decode(content).splitlines()
    aliases_by_code: dict[str, list[str]] = {}
    for row in alias_rows:
        aliases_by_code.setdefault(row["code_id"], []).append(row["alias_name"])
    code_items = [{"code_id": row["code_id"], "name": row["canonical_name"], "description": row["description"], "status": row["status"], "annotation_count": row["annotation_count"], "document_count": row["document_count"], "display_color": coerce_code_color(row["color"], row["canonical_name"]), "aliases": aliases_by_code.get(row["code_id"], [])} for row in codes]
    snippet_items = [{"annotation_id": row["annotation_id"], "code_id": row["code_id"], "code_name": row["canonical_name"], "code_color": coerce_code_color(row["color"], row["canonical_name"]), "document_path": row["current_path"], "scope_type": row["scope_type"], "start_line": row["start_line"], "end_line": row["end_line"], "anchor_status": row["anchor_status"], "memo": row["memo"], "exact_text": row["exact_text"]} for row in annotations]
    return {"generated_at": utcnow(), "project_root": str(project.root), "code_count": len(code_items), "snippet_count": len(snippet_items), "document_count": len({item["document_path"] for item in snippet_items}), "codes": code_items, "snippets": snippet_items, "document_texts": doc_texts}


def document_viewer_payload(project: Project, document_ref: str) -> dict[str, Any]:
    with project.connect() as conn:
        doc = project.resolve_document(conn, document_ref)
        revision = project.current_revision(conn, doc["document_id"])
        annotations = conn.execute("SELECT a.*, c.canonical_name, c.color FROM annotations a JOIN codes c ON c.code_id = a.code_id WHERE a.document_id = ? AND a.is_active = 1 ORDER BY COALESCE(a.start_byte, -1), a.annotation_id", (doc["document_id"],)).fetchall()
    content = (project.objects_dir / revision["content_sha256"]).read_bytes()
    text = safe_decode(content)
    span_annotations = []
    document_annotations = []
    code_counts: dict[str, dict[str, Any]] = {}
    annotation_index: dict[str, dict[str, Any]] = {}
    for row in annotations:
        code_slug = row["canonical_name"].replace("_", "-")
        code_color = coerce_code_color(row["color"], row["canonical_name"])
        highlight_color = soft_color(code_color, 0.28)
        code_entry = code_counts.setdefault(row["code_id"], {"code_id": row["code_id"], "name": row["canonical_name"], "display_color": code_color, "annotation_count": 0, "document_annotation_count": 0})
        item = {"annotation_id": row["annotation_id"], "code_id": row["code_id"], "code_name": row["canonical_name"], "code_slug": code_slug, "code_color": code_color, "highlight_color": highlight_color, "scope_type": row["scope_type"], "start_byte": row["start_byte"], "end_byte": row["end_byte"], "start_line": row["start_line"], "end_line": row["end_line"], "anchor_status": row["anchor_status"], "memo": row["memo"], "exact_text": row["exact_text"]}
        annotation_index[row["annotation_id"]] = item
        if row["scope_type"] == "document":
            document_annotations.append(item)
            code_entry["document_annotation_count"] += 1
            continue
        span_annotations.append(item)
        code_entry["annotation_count"] += 1
    codes = sorted(code_counts.values(), key=lambda item: (-item["annotation_count"], -item["document_annotation_count"], item["name"]))
    return {"generated_at": utcnow(), "project_root": str(project.root), "document_id": doc["document_id"], "document_path": doc["current_path"], "revision_id": revision["revision_id"], "byte_length": revision["byte_length"], "line_count": revision["line_count"], "code_count": len(codes), "annotation_count": len(span_annotations), "document_annotations": document_annotations, "span_annotations": span_annotations, "codes": codes, "annotation_index": annotation_index, "rendered_text": render_annotated_document_html(text, span_annotations)}


def render_annotated_document_html(text: str, spans: list[dict[str, Any]]) -> str:
    mapping = byte_to_char_index_map(text)
    boundaries = {0, len(text)}
    for span in spans:
        boundaries.add(mapping[span["start_byte"]])
        boundaries.add(mapping[span["end_byte"]])
    ordered = sorted(boundaries)
    pieces: list[str] = []
    for start, end in zip(ordered, ordered[1:]):
        segment = text[start:end]
        covering = [
            span for span in spans
            if mapping[span["start_byte"]] <= start and end <= mapping[span["end_byte"]]
        ]
        escaped = html.escape(segment)
        if not covering:
            pieces.append(escaped)
            continue
        class_names = " ".join(f"code-{span['code_slug']}" for span in covering)
        annotation_ids = ",".join(span["annotation_id"] for span in covering)
        if len(covering) == 1:
            background = covering[0]["highlight_color"]
        else:
            stripes = []
            width = 100 / len(covering)
            for idx, span in enumerate(covering):
                start_pct = idx * width
                end_pct = (idx + 1) * width
                stripes.append(f"{span['highlight_color']} {start_pct:.4f}% {end_pct:.4f}%")
            background = f"linear-gradient(90deg, {', '.join(stripes)})"
        label = " + ".join(span["code_name"] for span in covering)
        tooltip = "\n\n".join(
            "\n".join(
                part
                for part in [
                    span["code_name"],
                    f"Lines {span['start_line']}-{span['end_line']}" if span.get("start_line") is not None else None,
                    span.get("memo"),
                ]
                if part
            )
            for span in covering
        )
        pieces.append(
            f'<mark class="anno-segment {class_names}" data-annotation-ids="{annotation_ids}" '
            f'data-code-names="{html.escape(label)}" title="{html.escape(tooltip)}" '
            f'style="--segment-bg: {background};">{escaped}</mark>'
        )
    return "".join(pieces)


def build_code_explorer_html(payload: dict[str, Any], title: str) -> str:
    safe_title = html.escape(title)
    data_json = json.dumps(payload, ensure_ascii=False)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{safe_title}</title>
  <style>
    :root {{
      --ep-green: #428a5f;
      --ep-green-light: #5ba97a;
      --ep-green-soft: rgba(66, 138, 95, 0.10);
      --ep-dark: #1a1a1a;
      --ep-gray: #666666;
      --ep-light-gray: #f5f5f5;
      --ep-border: #e0e0e0;
      --font-serif: Georgia, 'Times New Roman', serif;
      --font-sans: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
      --font-mono: 'SF Mono', Consolas, 'Liberation Mono', Menlo, monospace;
    }}
    * {{ box-sizing: border-box; }}
    html {{ font-size: 16px; -webkit-font-smoothing: antialiased; }}
    body {{
      font-family: var(--font-sans);
      line-height: 1.5;
      color: var(--ep-dark);
      background: #fff;
      margin: 0;
      padding: 1rem 1.5rem 2rem;
    }}
    .shell {{
      max-width: 1200px;
      margin: 0 auto;
    }}
    header {{
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 1rem;
      border-bottom: 3px solid var(--ep-green);
      padding-bottom: 0.4rem;
      margin-bottom: 1rem;
    }}
    header h1 {{
      margin: 0;
      font-family: var(--font-serif);
      font-size: 1.5rem;
      font-weight: 600;
      color: var(--ep-dark);
    }}
    header .brand {{
      font-family: var(--font-serif);
      color: var(--ep-green);
      font-size: 0.9rem;
    }}
    .summary {{
      display: flex;
      gap: 1.5rem;
      margin-bottom: 0.75rem;
      font-size: 0.85rem;
      color: var(--ep-gray);
    }}
    .summary .stat-value {{
      font-weight: 700;
      color: var(--ep-dark);
    }}
    .controls {{
      display: flex;
      flex-wrap: wrap;
      gap: 0.5rem;
      align-items: center;
      margin-bottom: 1rem;
    }}
    .search {{
      flex: 1 1 200px;
      min-width: 180px;
      border: 1px solid var(--ep-border);
      border-radius: 4px;
      padding: 6px 10px;
      font: inherit;
      font-size: 0.85rem;
      outline: none;
    }}
    .search:focus {{
      border-color: var(--ep-green);
    }}
    .controls button {{
      border: 1px solid var(--ep-border);
      background: #fff;
      color: var(--ep-dark);
      border-radius: 4px;
      padding: 6px 12px;
      font: inherit;
      font-size: 0.8rem;
      cursor: pointer;
    }}
    .controls button:hover {{
      background: var(--ep-light-gray);
    }}
    .controls button.is-active {{
      background: var(--ep-green);
      color: white;
      border-color: var(--ep-green);
    }}
    .layout {{
      display: grid;
      grid-template-columns: 240px minmax(0, 1fr);
      gap: 1rem;
      align-items: start;
    }}
    .sidebar h2, .main h2 {{
      font-family: var(--font-serif);
      font-size: 1rem;
      color: var(--ep-green);
      margin: 0 0 0.5rem;
      border-bottom: 1px solid var(--ep-border);
      padding-bottom: 0.2rem;
    }}
    .code-list {{
      display: grid;
      gap: 4px;
      max-height: 80vh;
      overflow: auto;
    }}
    .code-card {{
      border: 1px solid transparent;
      border-radius: 4px;
      padding: 6px 8px;
      cursor: pointer;
      font-size: 0.85rem;
      line-height: 1.3;
    }}
    .code-card:hover {{
      background: var(--ep-light-gray);
    }}
    .code-card.is-selected {{
      background: var(--ep-green-soft);
      border-color: var(--ep-green);
    }}
    .code-top {{
      display: flex;
      gap: 6px;
      align-items: center;
    }}
    .swatch {{
      width: 10px;
      height: 10px;
      border-radius: 50%;
      flex: 0 0 auto;
    }}
    .code-name {{
      font-weight: 600;
      word-break: break-word;
    }}
    .code-meta {{
      color: var(--ep-gray);
      font-size: 0.78rem;
      margin-top: 2px;
      padding-left: 16px;
    }}
    .snippet-list {{
      display: grid;
      gap: 8px;
    }}
    .snippet {{
      border: 1px solid var(--ep-border);
      border-radius: 6px;
      padding: 10px 12px;
      background: #fff;
    }}
    .snippet-head {{
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 6px;
    }}
    .snippet-title {{
      display: flex;
      gap: 6px;
      align-items: center;
    }}
    .chip {{
      display: inline-flex;
      align-items: center;
      gap: 4px;
      border-radius: 3px;
      background: var(--ep-green-soft);
      padding: 2px 8px;
      color: var(--ep-green);
      font-size: 0.78rem;
      font-weight: 700;
    }}
    .snippet-meta {{
      color: var(--ep-gray);
      font-size: 0.78rem;
    }}
    .context-scroll {{
      max-height: 260px;
      overflow: auto;
      margin-top: 6px;
      border-radius: 4px;
      border: 1px solid var(--ep-border);
    }}
    .context-scroll pre {{
      margin: 0;
      padding: 8px 10px;
      background: var(--ep-light-gray);
      color: var(--ep-dark);
      white-space: pre-wrap;
      font-family: var(--font-mono);
      font-size: 0.82rem;
      line-height: 1.55;
    }}
    .context-scroll .hl {{
      background: rgba(255, 220, 50, 0.45);
      display: inline;
    }}
    .recenter {{
      display: none;
      position: sticky;
      bottom: 4px;
      float: right;
      margin: -24px 4px 0 0;
      border: 1px solid var(--ep-border);
      background: #fff;
      color: var(--ep-gray);
      border-radius: 3px;
      padding: 2px 8px;
      font-size: 0.72rem;
      cursor: pointer;
      z-index: 1;
      opacity: 0.85;
    }}
    .recenter:hover {{
      background: var(--ep-light-gray);
      opacity: 1;
    }}
    .context-scroll.scrolled-away .recenter {{
      display: block;
    }}
    .context-scroll .ctx {{
      color: var(--ep-gray);
      display: inline;
    }}
    .snippet .memo {{
      margin: 6px 0 0;
      font-size: 0.85rem;
      color: var(--ep-gray);
      font-style: italic;
      line-height: 1.4;
    }}
    .empty {{
      border: 1px dashed var(--ep-border);
      border-radius: 6px;
      padding: 1.5rem;
      color: var(--ep-gray);
      text-align: center;
    }}
    .footer {{
      color: var(--ep-gray);
      font-size: 0.78rem;
      text-align: right;
      margin-top: 1rem;
    }}
    @media (max-width: 768px) {{
      .layout {{
        grid-template-columns: 1fr;
      }}
      .code-list {{
        max-height: none;
      }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <h1>{safe_title}</h1>
      <span class="brand">E[&#x1f99c;] Expected Parrot</span>
    </header>
    <div class="summary" id="summary"></div>
    <div class="controls">
      <input id="search" class="search" type="search" placeholder="Search codes, documents, snippet text&hellip;">
      <button class="is-active" data-scope="all" type="button">All</button>
      <button data-scope="span" type="button">Span</button>
      <button data-scope="document" type="button">Document</button>
      <button id="clear-filters" type="button">Clear</button>
    </div>
    <div class="layout">
      <aside class="sidebar">
        <h2>Codes</h2>
        <div class="code-list" id="code-list"></div>
      </aside>
      <main class="main">
        <h2>Snippets</h2>
        <div class="snippet-list" id="snippet-list"></div>
      </main>
    </div>
    <div class="footer" id="footer"></div>
  </div>
  <script>
    const data = {data_json};
    const docTexts = data.document_texts || {{}};
    const state = {{ selectedCode: null, scope: "all", search: "" }};

    const codeListEl = document.getElementById("code-list");
    const snippetListEl = document.getElementById("snippet-list");
    const summaryEl = document.getElementById("summary");
    const footerEl = document.getElementById("footer");
    const searchEl = document.getElementById("search");
    const scopeButtons = Array.from(document.querySelectorAll("[data-scope]"));

    function escapeHtml(v) {{
      return v.replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;");
    }}

    function cardForCode(code) {{
      const sel = state.selectedCode === code.code_id;
      return `<div class="code-card ${{sel ? "is-selected" : ""}}" data-code-id="${{code.code_id}}">
        <div class="code-top">
          <span class="swatch" style="background:${{code.display_color}}"></span>
          <span class="code-name">${{escapeHtml(code.name)}}</span>
        </div>
        <div class="code-meta">${{code.annotation_count}} annot · ${{code.document_count}} doc${{code.document_count !== 1 ? "s" : ""}}</div>
      </div>`;
    }}

    function buildContextHtml(snippet) {{
      if (snippet.scope_type === "document") {{
        const lines = docTexts[snippet.document_path];
        if (lines && lines.length > 0) {{
          const maxLines = Math.min(lines.length, 60);
          let parts = lines.slice(0, maxLines).map(l => `<span class="ctx">${{escapeHtml(l)}}</span>`);
          if (lines.length > maxLines) {{
            parts.push(`<span class="ctx">... (${{lines.length - maxLines}} more lines)</span>`);
          }}
          return `<pre>${{parts.join("\\n")}}</pre>`;
        }}
        return `<pre>&lt;document-level annotation&gt;</pre>`;
      }}
      const lines = docTexts[snippet.document_path];
      if (!lines) {{
        return `<pre>${{escapeHtml(snippet.exact_text || "")}}</pre>`;
      }}
      const start = snippet.start_line;
      const end = snippet.end_line;
      const ctxBefore = Math.max(0, start - 1 - 10);
      const ctxAfter = Math.min(lines.length, end + 10);
      let parts = [];
      for (let i = ctxBefore; i < ctxAfter; i++) {{
        const lineNum = i + 1;
        const escaped = escapeHtml(lines[i]);
        if (lineNum >= start && lineNum <= end) {{
          parts.push(`<span class="hl">${{escaped}}</span>`);
        }} else {{
          parts.push(`<span class="ctx">${{escaped}}</span>`);
        }}
      }}
      return `<pre>${{parts.join("\\n")}}</pre>`;
    }}

    function snippetCard(snippet) {{
      const range = snippet.scope_type === "document"
        ? "Whole document"
        : `Lines ${{snippet.start_line}}\u2013${{snippet.end_line}}`;
      const memo = snippet.memo ? `<div class="memo">${{escapeHtml(snippet.memo)}}</div>` : "";
      const contextHtml = buildContextHtml(snippet);
      return `<article class="snippet">
        <div class="snippet-head">
          <div class="snippet-title">
            <span class="chip"><span class="swatch" style="background:${{snippet.code_color}}"></span>${{escapeHtml(snippet.code_name)}}</span>
          </div>
          <div class="snippet-meta">${{escapeHtml(snippet.document_path)}} &middot; ${{escapeHtml(range)}}</div>
        </div>
        <div class="context-scroll">${{contextHtml}}<button class="recenter" type="button">&uarr; Back to highlight</button></div>
        ${{memo}}
      </article>`;
    }}

    function matchesSnippet(s) {{
      if (state.selectedCode && s.code_id !== state.selectedCode) return false;
      if (state.scope !== "all" && s.scope_type !== state.scope) return false;
      if (!state.search) return true;
      return [s.code_name, s.document_path, s.memo||"", s.exact_text||""].join("\\n").toLowerCase().includes(state.search);
    }}

    function renderSummary(snippets) {{
      const codes = state.selectedCode ? 1 : data.codes.length;
      const docs = new Set(snippets.map(s => s.document_path)).size;
      const conflicted = snippets.filter(s => s.anchor_status === "conflicted").length;
      summaryEl.innerHTML = [
        `<span><span class="stat-value">${{codes}}</span> codes</span>`,
        `<span><span class="stat-value">${{snippets.length}}</span> snippets</span>`,
        `<span><span class="stat-value">${{docs}}</span> documents</span>`,
        conflicted ? `<span><span class="stat-value">${{conflicted}}</span> conflicted</span>` : "",
      ].filter(Boolean).join("");
    }}

    function renderCodes() {{
      codeListEl.innerHTML = data.codes.map(cardForCode).join("");
      for (const n of codeListEl.querySelectorAll(".code-card")) {{
        n.addEventListener("click", () => {{
          const id = n.getAttribute("data-code-id");
          state.selectedCode = state.selectedCode === id ? null : id;
          render();
        }});
      }}
    }}

    function renderSnippets() {{
      const filtered = data.snippets.filter(matchesSnippet);
      snippetListEl.innerHTML = filtered.length
        ? filtered.map(snippetCard).join("")
        : `<div class="empty">No snippets match the current filters.</div>`;
      renderSummary(filtered);
      // Auto-scroll highlighted text into view and wire up recenter buttons
      for (const el of snippetListEl.querySelectorAll(".context-scroll")) {{
        const hl = el.querySelector(".hl");
        if (!hl) continue;
        const scrollToHl = () => {{
          const top = hl.offsetTop - el.offsetTop - el.clientHeight / 3;
          el.scrollTop = Math.max(0, top);
          el.classList.remove("scrolled-away");
        }};
        scrollToHl();
        el.addEventListener("scroll", () => {{
          const hlTop = hl.offsetTop - el.offsetTop;
          const visible = hlTop >= el.scrollTop && hlTop < el.scrollTop + el.clientHeight;
          el.classList.toggle("scrolled-away", !visible);
        }});
        const btn = el.querySelector(".recenter");
        if (btn) btn.addEventListener("click", scrollToHl);
      }}
      footerEl.textContent = `Generated ${{data.generated_at}}`;
    }}

    function render() {{
      scopeButtons.forEach(b => b.classList.toggle("is-active", b.dataset.scope === state.scope));
      renderCodes();
      renderSnippets();
    }}

    searchEl.addEventListener("input", e => {{
      state.search = e.target.value.trim().toLowerCase();
      renderSnippets();
    }});
    for (const b of scopeButtons) {{
      b.addEventListener("click", () => {{ state.scope = b.dataset.scope; render(); }});
    }}
    document.getElementById("clear-filters").addEventListener("click", () => {{
      state.selectedCode = null; state.scope = "all"; state.search = ""; searchEl.value = "";
      render();
    }});
    render();
  </script>
</body>
</html>
"""


def build_static_code_explorer_html(payload: dict[str, Any], title: str) -> str:
    """Build a pure HTML/CSS code explorer with no JavaScript.

    For whole-document annotations, the full document text is rendered in the
    snippet area.  For span annotations, the exact annotated text is shown with
    context lines from the surrounding document.
    """
    safe_title = html.escape(title)
    codes = sorted(payload["codes"], key=lambda c: -c["annotation_count"])
    codes = [c for c in codes if c["annotation_count"] > 0]
    doc_texts: dict[str, list[str]] = payload.get("document_texts", {})

    # Group snippets by code
    snippets_by_code: dict[str, list[dict]] = {}
    for s in payload.get("snippets", []):
        snippets_by_code.setdefault(s["code_name"], []).append(s)

    def _escape(text: str) -> str:
        return html.escape(text)

    def _snippet_html(s: dict) -> str:
        scope = s.get("scope_type", "document")
        doc_path = s.get("document_path", "")
        if scope == "span" and s.get("exact_text"):
            text = s["exact_text"].strip()
        elif scope == "document" and doc_path in doc_texts:
            text = "\n".join(doc_texts[doc_path])
        elif s.get("exact_text"):
            text = s["exact_text"].strip()
        else:
            text = "(document-level annotation)"
        memo = s.get("memo") or ""
        lines = []
        lines.append('<div class="snippet">')
        lines.append(f'<pre class="snippet-text">{_escape(text)}</pre>')
        if memo:
            lines.append(f'<div class="snippet-memo">{_escape(memo)}</div>')
        lines.append(f'<div class="snippet-source">{_escape(doc_path)}</div>')
        lines.append('</div>')
        return "\n".join(lines)

    # Build code sections
    code_sections = []
    for c in codes:
        slug = c["name"].replace("/", "-")
        color = c.get("display_color", "var(--ep-green)")
        desc = c.get("description", "")
        code_snippets = snippets_by_code.get(c["name"], [])

        section_lines = []
        section_lines.append(f'<section class="code-section" id="{_escape(slug)}">')
        section_lines.append(f'<div class="code-header">')
        section_lines.append(f'<h2><span class="color-dot" style="background:{color}"></span>{_escape(c["name"])}</h2>')
        section_lines.append(f'<span class="badge">{c["annotation_count"]} annotation{"s" if c["annotation_count"] != 1 else ""}</span>')
        section_lines.append('</div>')
        if desc:
            section_lines.append(f'<p class="desc">{_escape(desc)}</p>')
        for s in code_snippets:
            section_lines.append(_snippet_html(s))
        section_lines.append('</section>')
        code_sections.append("\n".join(section_lines))

    # Build TOC
    toc_items = []
    for c in codes:
        slug = c["name"].replace("/", "-")
        toc_items.append(
            f'<div class="toc-item">'
            f'<a href="#{_escape(slug)}">{_escape(c["name"])}</a> '
            f'<span class="toc-count">({c["annotation_count"]})</span>'
            f'</div>'
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{safe_title}</title>
  <style>
    :root {{
      --ep-green: #428a5f;
      --ep-green-light: #5ba97a;
      --ep-green-soft: rgba(66, 138, 95, 0.10);
      --ep-dark: #1a1a1a;
      --ep-gray: #666666;
      --ep-light-gray: #f5f5f5;
      --ep-border: #e0e0e0;
      --font-serif: Georgia, 'Times New Roman', serif;
      --font-sans: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
      --font-mono: 'SF Mono', Consolas, 'Liberation Mono', Menlo, monospace;
    }}
    * {{ box-sizing: border-box; }}
    html {{ font-size: 16px; -webkit-font-smoothing: antialiased; }}
    body {{
      font-family: var(--font-sans);
      line-height: 1.6;
      color: var(--ep-dark);
      background: #fff;
      margin: 0;
      padding: 1rem 1.5rem 2rem;
    }}
    .shell {{ max-width: 960px; margin: 0 auto; }}
    header {{
      border-bottom: 3px solid var(--ep-green);
      padding-bottom: 0.4rem;
      margin-bottom: 1rem;
      display: flex;
      align-items: baseline;
      justify-content: space-between;
    }}
    header h1 {{
      margin: 0;
      font-family: var(--font-serif);
      font-size: 1.5rem;
      font-weight: 600;
      color: var(--ep-dark);
    }}
    header .brand {{
      font-family: var(--font-serif);
      color: var(--ep-green);
      font-size: 0.9rem;
    }}
    .stats {{
      color: var(--ep-gray);
      font-size: 0.9rem;
      margin-bottom: 1.5rem;
    }}
    .toc {{
      columns: 2;
      column-gap: 2rem;
      margin-bottom: 2rem;
      padding: 1rem;
      background: var(--ep-light-gray);
      border-radius: 6px;
    }}
    .toc-item {{
      break-inside: avoid;
      padding: 0.15rem 0;
    }}
    .toc a {{
      text-decoration: none;
      color: var(--ep-dark);
      font-family: var(--font-mono);
      font-size: 0.85rem;
    }}
    .toc a:hover {{ color: var(--ep-green); }}
    .toc-count {{ color: var(--ep-gray); font-size: 0.8rem; }}
    .code-section {{
      margin-bottom: 2rem;
      border-top: 1px solid var(--ep-border);
      padding-top: 0.5rem;
    }}
    .code-header {{
      display: flex;
      align-items: baseline;
      gap: 0.8rem;
      flex-wrap: wrap;
    }}
    .code-header h2 {{
      flex: 1;
      font-family: var(--font-mono);
      font-size: 1.1rem;
      color: var(--ep-dark);
      margin: 0.5rem 0 0.2rem 0;
    }}
    .color-dot {{
      display: inline-block;
      width: 10px;
      height: 10px;
      border-radius: 50%;
      margin-right: 0.5rem;
      vertical-align: middle;
    }}
    .badge {{
      display: inline-block;
      background: var(--ep-green);
      color: #fff;
      font-size: 0.75rem;
      padding: 0.15rem 0.5rem;
      border-radius: 10px;
      white-space: nowrap;
    }}
    .desc {{
      color: var(--ep-gray);
      font-style: italic;
      margin: 0.2rem 0 0.8rem 0;
    }}
    .snippet {{
      background: var(--ep-light-gray);
      border-left: 3px solid var(--ep-green);
      padding: 0.6rem 1rem;
      margin-bottom: 0.6rem;
      border-radius: 0 4px 4px 0;
    }}
    .snippet-text {{
      font-family: var(--font-sans);
      font-size: 0.9rem;
      white-space: pre-wrap;
      word-wrap: break-word;
      margin: 0;
      max-height: 20rem;
      overflow-y: auto;
    }}
    .snippet-memo {{
      font-size: 0.8rem;
      color: var(--ep-green);
      font-style: italic;
      margin-top: 0.3rem;
    }}
    .snippet-source {{
      font-size: 0.8rem;
      color: var(--ep-gray);
      margin-top: 0.3rem;
      font-family: var(--font-mono);
    }}
    .footer {{
      text-align: center;
      color: var(--ep-gray);
      font-size: 0.8rem;
      margin-top: 3rem;
      border-top: 1px solid var(--ep-border);
      padding-top: 1rem;
    }}
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <h1>{safe_title}</h1>
      <span class="brand">Expected Parrot</span>
    </header>
    <p class="stats">{payload['code_count']} codes &middot; {payload['snippet_count']} annotations &middot; {payload['document_count']} documents</p>
    <div class="toc">
{"".join(toc_items)}
    </div>
{"".join(code_sections)}
    <div class="footer">Generated {payload.get('generated_at', '')} by bewley &middot; Expected Parrot</div>
  </div>
</body>
</html>
"""


def build_embeddable_code_explorer_html(payload: dict[str, Any], title: str) -> str:
    """Build an embeddable HTML fragment for a code explorer.

    Returns a ``<div class="bewley-embed">`` containing scoped CSS (via
    ``.bewley-embed`` prefix on every rule) plus the code sections and TOC.
    No ``<html>``, ``<head>``, or ``<body>`` wrapper — designed to be dropped
    into a pandoc ``--embed-resources`` HTML report or any container page.
    """
    safe_title = html.escape(title)
    codes = sorted(payload["codes"], key=lambda c: -c["annotation_count"])
    codes = [c for c in codes if c["annotation_count"] > 0]
    doc_texts: dict[str, list[str]] = payload.get("document_texts", {})

    snippets_by_code: dict[str, list[dict]] = {}
    for s in payload.get("snippets", []):
        snippets_by_code.setdefault(s["code_name"], []).append(s)

    def _escape(text: str) -> str:
        return html.escape(text)

    def _snippet_html(s: dict) -> str:
        scope = s.get("scope_type", "document")
        doc_path = s.get("document_path", "")
        if scope == "span" and s.get("exact_text"):
            text = s["exact_text"].strip()
        elif scope == "document" and doc_path in doc_texts:
            text = "\n".join(doc_texts[doc_path])
        elif s.get("exact_text"):
            text = s["exact_text"].strip()
        else:
            text = "(document-level annotation)"
        memo = s.get("memo") or ""
        lines = []
        lines.append('<div class="bw-snippet">')
        lines.append(f'<pre class="bw-snippet-text">{_escape(text)}</pre>')
        if memo:
            lines.append(f'<div class="bw-snippet-memo">{_escape(memo)}</div>')
        lines.append(f'<div class="bw-snippet-source">{_escape(doc_path)}</div>')
        lines.append('</div>')
        return "\n".join(lines)

    code_sections = []
    for c in codes:
        slug = c["name"].replace("/", "-")
        color = c.get("display_color", "#428a5f")
        desc = c.get("description", "")
        code_snippets = snippets_by_code.get(c["name"], [])

        section_lines = []
        section_lines.append(f'<details class="bw-code-section" id="bw-{_escape(slug)}">')
        section_lines.append(f'<summary class="bw-code-header">')
        section_lines.append(f'<span class="bw-color-dot" style="background:{color}"></span>')
        section_lines.append(f'<strong class="bw-code-name">{_escape(c["name"])}</strong>')
        section_lines.append(f'<span class="bw-badge">{c["annotation_count"]}</span>')
        if desc:
            section_lines.append(f' <span class="bw-code-desc">{_escape(desc)}</span>')
        section_lines.append('</summary>')
        for s in code_snippets:
            section_lines.append(_snippet_html(s))
        section_lines.append('</details>')
        code_sections.append("\n".join(section_lines))

    toc_items = []
    for c in codes:
        slug = c["name"].replace("/", "-")
        toc_items.append(
            f'<span class="bw-toc-item">'
            f'<a href="#bw-{_escape(slug)}">{_escape(c["name"])}</a>'
            f'<span class="bw-toc-count">({c["annotation_count"]})</span>'
            f'</span>'
        )

    return f"""<div class="bewley-embed">
<style>
  .bewley-embed {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
    line-height: 1.5;
    color: #1a1a1a;
  }}
  .bewley-embed .bw-header {{
    border-bottom: 2px solid #428a5f;
    padding-bottom: 0.4rem;
    margin-bottom: 0.8rem;
  }}
  .bewley-embed .bw-header h3 {{
    margin: 0;
    font-size: 1.15rem;
    font-weight: 600;
    color: #1a1a1a;
  }}
  .bewley-embed .bw-stats {{
    color: #666;
    font-size: 0.85rem;
    margin-bottom: 1rem;
  }}
  .bewley-embed .bw-toc {{
    display: flex;
    flex-wrap: wrap;
    gap: 0.2rem 1.2rem;
    margin-bottom: 1.2rem;
    padding: 0.8rem;
    background: #f5f5f5;
    border-radius: 5px;
    font-size: 0.82rem;
  }}
  .bewley-embed .bw-toc-item a {{
    text-decoration: none;
    color: #1a1a1a;
    font-family: 'SF Mono', Consolas, 'Liberation Mono', Menlo, monospace;
  }}
  .bewley-embed .bw-toc-item a:hover {{ color: #428a5f; }}
  .bewley-embed .bw-toc-count {{ color: #999; font-size: 0.78rem; }}
  .bewley-embed .bw-code-section {{
    margin-bottom: 0.6rem;
    border: 1px solid #e0e0e0;
    border-radius: 5px;
  }}
  .bewley-embed .bw-code-header {{
    cursor: pointer;
    padding: 0.5rem 0.8rem;
    background: #fafafa;
    border-radius: 5px;
    font-size: 0.9rem;
    list-style: none;
  }}
  .bewley-embed .bw-code-header::-webkit-details-marker {{ display: none; }}
  .bewley-embed .bw-code-header::before {{
    content: '\\25B6';
    display: inline-block;
    margin-right: 0.4rem;
    font-size: 0.6rem;
    transition: transform 0.15s;
    color: #999;
  }}
  .bewley-embed details[open] > .bw-code-header::before {{
    transform: rotate(90deg);
  }}
  .bewley-embed .bw-color-dot {{
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    margin-right: 0.4rem;
    vertical-align: middle;
  }}
  .bewley-embed .bw-code-name {{
    font-family: 'SF Mono', Consolas, 'Liberation Mono', Menlo, monospace;
    font-size: 0.88rem;
  }}
  .bewley-embed .bw-badge {{
    display: inline-block;
    background: #428a5f;
    color: #fff;
    font-size: 0.68rem;
    padding: 0.1rem 0.4rem;
    border-radius: 8px;
    margin-left: 0.3rem;
    vertical-align: middle;
  }}
  .bewley-embed .bw-code-desc {{
    color: #888;
    font-style: italic;
    font-size: 0.82rem;
    margin-left: 0.5rem;
  }}
  .bewley-embed .bw-snippet {{
    background: #f8f8f8;
    border-left: 3px solid #428a5f;
    padding: 0.5rem 0.8rem;
    margin: 0.4rem 0.8rem;
    border-radius: 0 4px 4px 0;
  }}
  .bewley-embed .bw-snippet-text {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
    font-size: 0.85rem;
    white-space: pre-wrap;
    word-wrap: break-word;
    margin: 0;
    max-height: 12rem;
    overflow-y: auto;
  }}
  .bewley-embed .bw-snippet-memo {{
    font-size: 0.78rem;
    color: #428a5f;
    font-style: italic;
    margin-top: 0.2rem;
  }}
  .bewley-embed .bw-snippet-source {{
    font-size: 0.75rem;
    color: #999;
    margin-top: 0.2rem;
    font-family: 'SF Mono', Consolas, 'Liberation Mono', Menlo, monospace;
  }}
  .bewley-embed .bw-footer {{
    text-align: center;
    color: #999;
    font-size: 0.75rem;
    margin-top: 1rem;
    padding-top: 0.5rem;
    border-top: 1px solid #e0e0e0;
  }}
</style>
  <div class="bw-header">
    <h3>{safe_title}</h3>
  </div>
  <p class="bw-stats">{payload['code_count']} codes &middot; {payload['snippet_count']} annotations &middot; {payload['document_count']} documents</p>
  <div class="bw-toc">
{"".join(toc_items)}
  </div>
{"".join(code_sections)}
  <div class="bw-footer">Generated by bewley</div>
</div>
"""


def build_document_viewer_html(payload: dict[str, Any], title: str) -> str:
    safe_title = html.escape(title)
    data_json = json.dumps(payload, ensure_ascii=False)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{safe_title}</title>
  <style>
    :root {{
      --bg: #f7f2e9;
      --panel: rgba(255, 251, 244, 0.86);
      --panel-strong: rgba(255, 250, 242, 0.98);
      --ink: #221d1a;
      --muted: #6d6259;
      --accent: #8b4a2d;
      --border: rgba(54, 42, 31, 0.14);
      --shadow: 0 20px 60px rgba(58, 39, 22, 0.12);
      --radius: 22px;
      --mono: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
      --sans: "Avenir Next", "Segoe UI", sans-serif;
    }}
    * {{ box-sizing: border-box; }}
    html, body {{
      margin: 0;
      min-height: 100%;
      background:
        radial-gradient(circle at top left, rgba(255, 204, 116, 0.18), transparent 34%),
        radial-gradient(circle at top right, rgba(139, 74, 45, 0.14), transparent 32%),
        linear-gradient(180deg, #fbf6ee 0%, var(--bg) 100%);
      color: var(--ink);
      font-family: var(--sans);
    }}
    body {{ padding: 24px 16px 40px; }}
    .shell {{
      max-width: 1360px;
      margin: 0 auto;
      display: grid;
      gap: 18px;
    }}
    .hero, .controls, .sidebar, .document-panel {{
      backdrop-filter: blur(18px);
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
    }}
    .hero {{
      padding: 26px;
      position: relative;
      overflow: hidden;
    }}
    .hero::after {{
      content: "";
      position: absolute;
      inset: auto -10% -40% 40%;
      height: 220px;
      background: linear-gradient(90deg, rgba(139, 74, 45, 0), rgba(139, 74, 45, 0.25));
      transform: rotate(-8deg);
    }}
    .eyebrow {{
      margin: 0 0 8px;
      color: var(--accent);
      text-transform: uppercase;
      letter-spacing: 0.12em;
      font-size: 12px;
      font-weight: 700;
    }}
    h1 {{
      margin: 0;
      font-size: clamp(1.8rem, 5vw, 3.8rem);
      line-height: 0.96;
      max-width: 12ch;
    }}
    .hero p {{
      margin: 14px 0 0;
      max-width: 70ch;
      color: var(--muted);
      line-height: 1.5;
    }}
    .meta-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 12px;
      margin-top: 18px;
    }}
    .stat {{
      background: var(--panel-strong);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 14px;
    }}
    .stat-label {{
      margin: 0;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-size: 12px;
    }}
    .stat-value {{
      margin: 8px 0 0;
      font-size: 1.7rem;
      font-weight: 700;
    }}
    .controls {{
      padding: 16px;
      display: grid;
      gap: 14px;
    }}
    .toolbar {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
      justify-content: space-between;
    }}
    .search {{
      flex: 1 1 280px;
      min-width: 220px;
      display: flex;
      align-items: center;
      gap: 10px;
      border: 1px solid var(--border);
      background: var(--panel-strong);
      border-radius: 999px;
      padding: 0 14px;
    }}
    .search input {{
      width: 100%;
      border: 0;
      background: transparent;
      color: var(--ink);
      padding: 12px 0;
      outline: none;
      font: inherit;
    }}
    button {{
      border: 1px solid var(--border);
      background: var(--panel-strong);
      color: var(--ink);
      border-radius: 999px;
      padding: 9px 14px;
      font: inherit;
      cursor: pointer;
    }}
    button.is-active {{
      background: var(--accent);
      border-color: transparent;
      color: white;
    }}
    .layout {{
      display: grid;
      grid-template-columns: minmax(280px, 360px) minmax(0, 1fr);
      gap: 18px;
      align-items: start;
    }}
    .sidebar, .document-panel {{
      padding: 16px;
    }}
    .sidebar h2, .document-panel h2 {{
      margin: 0 0 14px;
      font-size: 1rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .legend {{
      display: grid;
      gap: 10px;
      max-height: 72vh;
      overflow: auto;
      padding-right: 4px;
    }}
    .legend-item {{
      border: 1px solid var(--border);
      background: var(--panel-strong);
      border-radius: 18px;
      padding: 14px;
      cursor: pointer;
      transition: 160ms ease;
    }}
    .legend-item.is-selected {{
      box-shadow: inset 0 0 0 2px var(--accent);
      border-color: transparent;
    }}
    .legend-top {{
      display: flex;
      gap: 10px;
      align-items: center;
    }}
    .swatch {{
      width: 14px;
      height: 14px;
      border-radius: 999px;
      flex: 0 0 auto;
      box-shadow: inset 0 0 0 1px rgba(0,0,0,0.08);
    }}
    .legend-name {{
      font-weight: 700;
      word-break: break-word;
    }}
    .legend-meta {{
      margin-top: 10px;
      color: var(--muted);
      font-size: 0.92rem;
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }}
    .doc-frame {{
      border: 1px solid var(--border);
      border-radius: 20px;
      background: #fffdf8;
      overflow: hidden;
    }}
    .doc-header {{
      padding: 14px 16px;
      border-bottom: 1px solid var(--border);
      background: linear-gradient(180deg, rgba(255,255,255,0.7), rgba(255,251,244,0.96));
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      gap: 8px;
      color: var(--muted);
      font-size: 0.92rem;
    }}
    .doc-tags {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 14px;
    }}
    .tag {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px 10px;
      border-radius: 999px;
      background: var(--panel-strong);
      border: 1px solid var(--border);
      font-size: 0.86rem;
      color: var(--muted);
    }}
    .document-text {{
      margin: 0;
      padding: 18px 18px 28px;
      font-family: var(--mono);
      white-space: pre-wrap;
      line-height: 1.72;
      font-size: 0.94rem;
    }}
    .anno-segment {{
      background: var(--segment-bg);
      border-radius: 0.35em;
      box-shadow: inset 0 -1px 0 rgba(0,0,0,0.08);
      transition: opacity 140ms ease, box-shadow 140ms ease;
    }}
    .anno-segment.is-dim {{
      opacity: 0.24;
    }}
    .anno-segment.is-match {{
      box-shadow: inset 0 -1px 0 rgba(0,0,0,0.08), 0 0 0 2px rgba(34, 29, 26, 0.22);
    }}
    .annotation-list {{
      margin-top: 18px;
      display: grid;
      gap: 10px;
    }}
    .annotation-chip {{
      border: 1px solid var(--border);
      background: var(--panel-strong);
      border-radius: 16px;
      padding: 12px;
      font-size: 0.92rem;
      color: var(--muted);
      cursor: pointer;
    }}
    .annotation-chip.is-selected {{
      box-shadow: inset 0 0 0 2px var(--accent);
      border-color: transparent;
    }}
    .annotation-chip strong {{
      color: var(--ink);
    }}
    .empty {{
      border: 1px dashed var(--border);
      border-radius: 18px;
      padding: 24px;
      color: var(--muted);
      text-align: center;
    }}
    .footer {{
      text-align: right;
      color: var(--muted);
      font-size: 0.9rem;
    }}
    @media (max-width: 900px) {{
      .layout {{ grid-template-columns: 1fr; }}
      .legend {{ max-height: none; }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <p class="eyebrow">Bewley Annotated Document</p>
      <h1>{safe_title}</h1>
      <p>Inspect one document with inline code highlights. Toggle codes from the legend, search within annotation text and memos, and use the annotation list to focus the document view.</p>
      <div class="meta-grid" id="summary"></div>
    </section>
    <section class="controls">
      <div class="toolbar">
        <label class="search" aria-label="Search annotations">
          <span>Search</span>
          <input id="search" type="search" placeholder="code, memo, or selected text">
        </label>
        <div>
          <button id="show-all" class="is-active" type="button">Show all codes</button>
          <button id="clear-focus" type="button">Clear focus</button>
        </div>
      </div>
    </section>
    <div class="layout">
      <aside class="sidebar">
        <h2>Legend</h2>
        <div class="legend" id="legend"></div>
      </aside>
      <main class="document-panel">
        <h2>Document</h2>
        <div class="doc-tags" id="doc-tags"></div>
        <div class="doc-frame">
          <div class="doc-header">
            <span id="doc-path"></span>
            <span id="doc-meta"></span>
          </div>
          <pre class="document-text" id="document-text"></pre>
        </div>
        <div class="annotation-list" id="annotation-list"></div>
      </main>
    </div>
    <div class="footer" id="footer"></div>
  </div>
  <script>
    const data = {data_json};
    const state = {{
      selectedCode: null,
      selectedAnnotation: null,
      search: "",
    }};

    const summaryEl = document.getElementById("summary");
    const legendEl = document.getElementById("legend");
    const docTagsEl = document.getElementById("doc-tags");
    const docPathEl = document.getElementById("doc-path");
    const docMetaEl = document.getElementById("doc-meta");
    const docTextEl = document.getElementById("document-text");
    const annotationListEl = document.getElementById("annotation-list");
    const footerEl = document.getElementById("footer");
    const searchEl = document.getElementById("search");

    function escapeHtml(value) {{
      return value
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;");
    }}

    function fmtCount(value, singular, plural) {{
      return `${{value}} ${{value === 1 ? singular : plural}}`;
    }}

    function renderSummary() {{
      const rows = [
        ["Codes", data.code_count],
        ["Annotations", data.annotation_count],
        ["Lines", data.line_count],
        ["Document tags", data.document_annotations.length],
      ];
      summaryEl.innerHTML = rows.map(([label, value]) => `
        <div class="stat">
          <p class="stat-label">${{label}}</p>
          <p class="stat-value">${{value}}</p>
        </div>
      `).join("");
    }}

    function matchesAnnotation(annotation) {{
      if (state.selectedCode && annotation.code_id !== state.selectedCode) {{
        return false;
      }}
      if (!state.search) {{
        return true;
      }}
      const haystack = [
        annotation.code_name,
        annotation.memo || "",
        annotation.exact_text || "",
        annotation.anchor_status,
      ].join("\\n").toLowerCase();
      return haystack.includes(state.search);
    }}

    function renderLegend() {{
      legendEl.innerHTML = data.codes.map((code) => `
        <article class="legend-item ${{state.selectedCode === code.code_id ? "is-selected" : ""}}" data-code-id="${{code.code_id}}">
          <div class="legend-top">
            <span class="swatch" style="background:${{code.display_color}}"></span>
            <div class="legend-name">${{escapeHtml(code.name)}}</div>
          </div>
          <div class="legend-meta">
            <span>${{fmtCount(code.annotation_count, "annotation", "annotations")}}</span>
            <span>${{fmtCount(code.document_annotation_count, "document tag", "document tags")}}</span>
          </div>
        </article>
      `).join("");
      for (const node of legendEl.querySelectorAll(".legend-item")) {{
        node.addEventListener("click", () => {{
          const codeId = node.dataset.codeId;
          state.selectedCode = state.selectedCode === codeId ? null : codeId;
          state.selectedAnnotation = null;
          render();
        }});
      }}
    }}

    function renderTags() {{
      const tags = data.document_annotations.filter((annotation) => !state.selectedCode || annotation.code_id === state.selectedCode);
      if (!tags.length) {{
        docTagsEl.innerHTML = `<div class="empty">No document-level codes visible.</div>`;
        return;
      }}
      docTagsEl.innerHTML = tags.map((annotation) => `
        <span class="tag" style="background:${{annotation.highlight_color}}">
          <span class="swatch" style="background:${{annotation.code_color}}"></span>
          ${{escapeHtml(annotation.code_name)}}
        </span>
      `).join("");
    }}

    function renderDocument() {{
      docTextEl.innerHTML = data.rendered_text;
      for (const node of docTextEl.querySelectorAll(".anno-segment")) {{
        const ids = (node.dataset.annotationIds || "").split(",").filter(Boolean);
        const shouldDim = ids.every((annotationId) => {{
          const annotation = data.annotation_index[annotationId];
          return !matchesAnnotation(annotation);
        }});
        node.classList.toggle("is-dim", shouldDim);
        node.classList.toggle("is-match", state.selectedAnnotation && ids.includes(state.selectedAnnotation));
      }}
    }}

    function renderAnnotationList() {{
      const items = data.span_annotations.filter(matchesAnnotation);
      if (!items.length) {{
        annotationListEl.innerHTML = `<div class="empty">No span annotations match the current filters.</div>`;
        return;
      }}
      annotationListEl.innerHTML = items.map((annotation) => `
        <article class="annotation-chip ${{state.selectedAnnotation === annotation.annotation_id ? "is-selected" : ""}}" data-annotation-id="${{annotation.annotation_id}}">
          <strong>${{escapeHtml(annotation.code_name)}}</strong> · lines ${{annotation.start_line}}-${{annotation.end_line}} · ${{escapeHtml(annotation.anchor_status)}}<br>
          ${{annotation.memo ? escapeHtml(annotation.memo) + "<br>" : ""}}
          <span>${{escapeHtml(annotation.exact_text || "")}}</span>
        </article>
      `).join("");
      for (const node of annotationListEl.querySelectorAll(".annotation-chip")) {{
        node.addEventListener("click", () => {{
          const annotationId = node.dataset.annotationId;
          state.selectedAnnotation = state.selectedAnnotation === annotationId ? null : annotationId;
          renderDocument();
          renderAnnotationList();
          const match = docTextEl.querySelector(`[data-annotation-ids*="${{annotationId}}"]`);
          if (match) {{
            match.scrollIntoView({{ behavior: "smooth", block: "center" }});
          }}
        }});
      }}
    }}

    function render() {{
      document.getElementById("show-all").classList.toggle("is-active", !state.selectedCode);
      renderLegend();
      renderTags();
      renderDocument();
      renderAnnotationList();
      docPathEl.textContent = data.document_path;
      docMetaEl.textContent = `${{data.revision_id}} · ${{data.line_count}} lines · ${{data.byte_length}} bytes`;
      footerEl.textContent = `Generated from ${{data.project_root}} on ${{data.generated_at}}`;
    }}

    searchEl.addEventListener("input", (event) => {{
      state.search = event.target.value.trim().toLowerCase();
      state.selectedAnnotation = null;
      render();
    }});

    document.getElementById("show-all").addEventListener("click", () => {{
      state.selectedCode = null;
      state.selectedAnnotation = null;
      render();
    }});

    document.getElementById("clear-focus").addEventListener("click", () => {{
      state.selectedCode = null;
      state.selectedAnnotation = null;
      state.search = "";
      searchEl.value = "";
      render();
    }});

    renderSummary();
    render();
  </script>
</body>
</html>
"""
