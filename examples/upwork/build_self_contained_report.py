#!/usr/bin/env python3

from __future__ import annotations

import csv
import html
import re
import sqlite3
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parent
EXPORTS = ROOT / "exports"
CORPUS = ROOT / "corpus"
REPORT_MD = EXPORTS / "upwork-research-report.md"
OUTPUT_HTML = EXPORTS / "upwork-research-report-self-contained.html"
TABLES_DIR = EXPORTS / "analysis-tables"
DB_PATH = ROOT / ".bewley" / "index" / "bewley.sqlite"


def slugify(path: Path) -> str:
    return re.sub(r"[^a-z0-9]+", "-", path.stem.lower()).strip("-")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def as_int(value: str) -> int:
    return int(value) if value else 0


def pandoc_fragment(markdown_path: Path) -> str:
    result = subprocess.run(
        ["pandoc", str(markdown_path), "--from", "gfm", "--to", "html5"],
        text=True,
        capture_output=True,
        check=True,
    )
    return result.stdout


def rewrite_internal_links(fragment: str) -> str:
    pattern = re.compile(
        r'href="[^"]*/corpus/([^"/]+)\.md(?:#L(\d+))?"'
    )

    def repl(match: re.Match[str]) -> str:
        stem = match.group(1)
        line = match.group(2)
        anchor = f"transcript-{slugify(Path(stem + '.md'))}"
        if line:
            anchor += f"-L{line}"
        return f'href="#{anchor}"'

    return pattern.sub(repl, fragment)


def build_top_codes_chart(code_summary: list[dict[str, str]]) -> str:
    rows = code_summary[:8]
    max_count = max((as_int(row["annotation_count"]) for row in rows), default=1)
    bars = []
    for row in rows:
        count = as_int(row["annotation_count"])
        width = (count / max_count) * 100
        bars.append(
            f"""
            <div class="chart-row">
              <div class="chart-label">{html.escape(row['code_name'])}</div>
              <div class="chart-bar"><span style="width:{width:.2f}%"></span></div>
              <div class="chart-value">{count}</div>
            </div>
            """
        )
    return "\n".join(bars)


def build_heatmap(matrix_rows: list[dict[str, str]]) -> str:
    if not matrix_rows:
        return "<p>No matrix data available.</p>"
    cols = [c for c in matrix_rows[0].keys() if c != "document_path"][:8]
    header = "<tr><th>Document</th>" + "".join(f"<th>{html.escape(col)}</th>" for col in cols) + "</tr>"
    body = []
    for row in matrix_rows:
        tds = [f"<th>{html.escape(row['document_path'].replace('corpus/', ''))}</th>"]
        for col in cols:
            value = as_int(row[col])
            color = "rgba(202, 88, 47, 0.78)" if value else "rgba(202, 88, 47, 0.08)"
            tds.append(f'<td style="background:{color}">{value}</td>')
        body.append("<tr>" + "".join(tds) + "</tr>")
    return f"<table class='heatmap'>{header}{''.join(body)}</table>"


def build_cooccurrence_chart(rows: list[dict[str, str]]) -> str:
    top = rows[:10]
    max_count = max((as_int(row["shared_document_count"]) for row in top), default=1)
    items = []
    for row in top:
        count = as_int(row["shared_document_count"])
        width = (count / max_count) * 100
        label = f"{row['code_a']} × {row['code_b']}"
        items.append(
            f"""
            <div class="chart-row">
              <div class="chart-label">{html.escape(label)}</div>
              <div class="chart-bar secondary"><span style="width:{width:.2f}%"></span></div>
              <div class="chart-value">{count}</div>
            </div>
            """
        )
    return "\n".join(items)


def build_focused_evidence_cards() -> str:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT c.canonical_name, d.current_path, a.start_line, a.end_line, a.exact_text
            FROM annotations a
            JOIN codes c ON c.code_id = a.code_id
            JOIN documents d ON d.document_id = a.document_id
            WHERE a.is_active = 1
              AND a.scope_type = 'span'
              AND a.memo = 'focused report evidence'
            ORDER BY c.canonical_name, d.current_path, a.start_line
            """
        ).fetchall()
    cards = []
    for row in rows[:12]:
        slug = slugify(Path(row["current_path"]))
        href = f"#transcript-{slug}-L{row['start_line']}"
        excerpt = html.escape((row["exact_text"] or "").strip())
        cards.append(
            f"""
            <article class="evidence-card">
              <div class="evidence-code">{html.escape(row['canonical_name'])}</div>
              <div class="evidence-doc"><a href="{href}">{html.escape(row['current_path'].replace('corpus/', ''))}:{row['start_line']}</a></div>
              <p>{excerpt}</p>
            </article>
            """
        )
    return "\n".join(cards)


def inject_inline_visuals(
    report_body: str,
    code_summary: list[dict[str, str]],
    matrix_rows: list[dict[str, str]],
    cooccurrence: list[dict[str, str]],
) -> str:
    sections = [
        (
            '<h2 id="corpus-and-analysis-basis">Corpus And Analysis Basis</h2>',
            f"""
            <section class="figure-block" aria-labelledby="figure-code-coverage">
              <h3 id="figure-code-coverage">Figure 1. Top Code Coverage</h3>
              {build_top_codes_chart(code_summary)}
              <p class="figure-note">This is orientation only: broad first-pass code counts showing which topics dominate the corpus at a high level.</p>
            </section>
            """,
        ),
        (
            '<h2 id="current-users-vs-prospects">Current Users Vs. Prospects</h2>',
            f"""
            <section class="figure-block" aria-labelledby="figure-heatmap">
              <h3 id="figure-heatmap">Figure 2. Code Presence Heatmap</h3>
              {build_heatmap(matrix_rows)}
              <p class="figure-note">This view makes the subgroup comparison concrete by showing which top codes appear across documents, rather than leaving the comparison purely narrative.</p>
            </section>
            """,
        ),
        (
            '<h2 id="tensions-and-counterevidence">Tensions And Counterevidence</h2>',
            f"""
            <section class="figure-block" aria-labelledby="figure-cooccurrence">
              <h3 id="figure-cooccurrence">Figure 3. Top Co-occurrence Pairs</h3>
              {build_cooccurrence_chart(cooccurrence)}
              <p class="figure-note">Co-occurrence is not proof, but it is useful for surfacing repeated tensions and higher-order themes that deserve closer reading.</p>
            </section>
            """,
        ),
        (
            '<h2 id="implications-for-upwork">Implications For Upwork</h2>',
            f"""
            <section class="figure-block" aria-labelledby="figure-focused-evidence">
              <h3 id="figure-focused-evidence">Figure 4. Focused Evidence Layer</h3>
              <div class="evidence-grid">
                {build_focused_evidence_cards()}
              </div>
              <p class="figure-note">These excerpts come from the tighter second-pass coding layer and show the report's higher-confidence evidence base.</p>
            </section>
            """,
        ),
    ]
    for heading, block in sections:
        report_body = report_body.replace(heading, heading + block, 1)
    return report_body


def strip_frontmatter(text: str) -> str:
    lines = text.splitlines()
    if len(lines) >= 2 and lines[0].strip() == "---":
        for idx in range(1, len(lines)):
            if lines[idx].strip() == "---":
                return "\n".join(lines[idx + 1 :])
    return text


def build_transcript_appendix() -> str:
    sections = []
    for path in sorted(CORPUS.glob("*.md")):
        slug = slugify(path)
        body = strip_frontmatter(path.read_text(encoding="utf-8"))
        lines = body.splitlines()
        rendered_lines = []
        for i, line in enumerate(lines, start=1):
            content = "&nbsp;" if line == "" else html.escape(line)
            rendered_lines.append(
                f'<div class="tline" id="transcript-{slug}-L{i}"><a class="ln" href="#transcript-{slug}-L{i}">L{i}</a><span>{content}</span></div>'
            )
        sections.append(
            f"""
            <details class="transcript" id="transcript-{slug}">
              <summary>{html.escape(path.name)}</summary>
              <div class="transcript-body">
                {''.join(rendered_lines)}
              </div>
            </details>
            """
        )
    return "\n".join(sections)


def build_html(report_body: str, code_summary: list[dict[str, str]], matrix_rows: list[dict[str, str]], cooccurrence: list[dict[str, str]]) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Upwork PM Spotlight Research Report</title>
  <style>
    :root {{
      --bg: #f5efe6;
      --paper: #fffaf3;
      --ink: #1d1714;
      --muted: #6f645b;
      --line: #d7c6b5;
      --accent: #b4472a;
      --accent-2: #446b7a;
    }}
    * {{ box-sizing: border-box; }}
    html {{ scroll-behavior: smooth; }}
    body {{
      margin: 0;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(180,71,42,.12), transparent 26rem),
        linear-gradient(180deg, #f7f1e8, #efe5d8 40%, #f6efe6);
      font-family: Georgia, "Iowan Old Style", "Palatino Linotype", serif;
      line-height: 1.65;
    }}
    .shell {{
      max-width: 980px;
      margin: 0 auto;
      padding: 32px 20px 60px;
    }}
    .hero {{
      background: rgba(255,250,243,.84);
      border: 1px solid var(--line);
      border-radius: 22px;
      padding: 28px;
      margin-bottom: 18px;
    }}
    .hero h1 {{
      margin: 0 0 8px;
      font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
      font-size: clamp(2.4rem, 5vw, 4.5rem);
      line-height: .94;
      letter-spacing: -.03em;
    }}
    .hero p {{
      margin: 0;
      color: var(--muted);
      font-size: 1.05rem;
    }}
    .panel {{
      background: rgba(255,250,243,.82);
      border: 1px solid var(--line);
      border-radius: 22px;
      padding: 24px;
    }}
    .panel h2, .panel h3 {{
      margin-top: 0;
      font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
      letter-spacing: -.02em;
    }}
    .report h1, .report h2, .report h3 {{
      font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
      letter-spacing: -.02em;
      line-height: 1.08;
    }}
    .report h1 {{ font-size: 2.5rem; }}
    .report h2 {{
      margin-top: 2.5rem;
      padding-top: 1rem;
      border-top: 1px solid var(--line);
      font-size: 1.65rem;
    }}
    .report h3 {{ font-size: 1.12rem; margin-top: 1.7rem; }}
    .report a {{ color: var(--accent); }}
    .report > :first-child {{ margin-top: 0; }}
    .report code {{
      font-family: "SFMono-Regular", Menlo, Consolas, monospace;
      background: rgba(120, 87, 54, 0.08);
      padding: .1em .35em;
      border-radius: 4px;
      font-size: .92em;
    }}
    .figure-block {{
      margin: 1.1rem 0 2rem;
      padding: 18px 20px;
      border: 1px solid var(--line);
      border-radius: 18px;
      background: rgba(255, 248, 240, 0.78);
    }}
    .figure-block h3 {{
      margin-top: 0;
      margin-bottom: .8rem;
      font-size: 1.05rem;
      text-transform: none;
    }}
    .figure-note {{
      color: var(--muted);
      font-size: .92rem;
      margin: 10px 0 0;
    }}
    .chart-row {{
      display: grid;
      grid-template-columns: minmax(140px, 240px) 1fr 40px;
      gap: 10px;
      align-items: center;
      margin-top: 10px;
    }}
    .chart-label, .chart-value {{
      font-size: .92rem;
    }}
    .chart-bar {{
      height: 12px;
      border-radius: 999px;
      background: #eaded3;
      overflow: hidden;
    }}
    .chart-bar span {{
      display: block;
      height: 100%;
      background: linear-gradient(90deg, var(--accent), #dc8d71);
    }}
    .chart-bar.secondary span {{
      background: linear-gradient(90deg, var(--accent-2), #8db2bf);
    }}
    .heatmap {{
      width: 100%;
      border-collapse: collapse;
      font-size: .84rem;
      margin-top: 10px;
    }}
    .heatmap th, .heatmap td {{
      border: 1px solid var(--line);
      padding: 6px 8px;
    }}
    .heatmap td {{ text-align: center; }}
    .evidence-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
      margin-top: 12px;
    }}
    .evidence-card {{
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 14px;
      background: var(--paper);
    }}
    .evidence-code {{
      font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
      font-size: .82rem;
      text-transform: uppercase;
      letter-spacing: .06em;
      color: var(--accent);
      margin-bottom: 4px;
    }}
    .evidence-doc {{
      font-size: .88rem;
      margin-bottom: 8px;
    }}
    .appendix {{
      margin-top: 18px;
    }}
    .transcript {{
      border: 1px solid var(--line);
      border-radius: 16px;
      background: rgba(255,250,243,.78);
      margin-top: 10px;
      overflow: hidden;
    }}
    .transcript summary {{
      cursor: pointer;
      padding: 14px 16px;
      font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
      font-weight: 600;
      background: rgba(230, 217, 203, .45);
    }}
    .transcript-body {{
      padding: 10px 0 14px;
      max-height: 70vh;
      overflow: auto;
      background: var(--paper);
    }}
    .tline {{
      display: grid;
      grid-template-columns: 66px 1fr;
      gap: 10px;
      padding: 1px 16px;
      font-family: "SFMono-Regular", Menlo, Consolas, monospace;
      font-size: 12px;
      line-height: 1.5;
      border-top: 1px solid rgba(215,198,181,.28);
    }}
    .ln {{
      color: var(--muted);
      text-decoration: none;
      position: sticky;
      left: 0;
      background: var(--paper);
    }}
    .note {{
      color: var(--muted);
      font-size: .9rem;
      margin-top: 10px;
    }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <h1>Upwork PM Spotlight Research Report</h1>
      <p>Self-contained case report with tighter higher-order evidence, inline visuals, and full transcript appendix.</p>
    </section>

    <article class="panel report">
      {report_body}
    </article>

    <section class="panel appendix" id="appendix">
      <h2>Transcript Appendix</h2>
      <p class="note">All report citations jump to exact line anchors below. This appendix is embedded so the document remains fully self-contained.</p>
      {build_transcript_appendix()}
    </section>
  </div>
</body>
</html>
"""


def main() -> int:
    code_summary = read_csv(TABLES_DIR / "code_summary.csv")
    matrix_rows = read_csv(TABLES_DIR / "code_document_matrix.csv")
    cooccurrence = read_csv(TABLES_DIR / "code_cooccurrence.csv")
    report_body = rewrite_internal_links(pandoc_fragment(REPORT_MD))
    report_body = inject_inline_visuals(report_body, code_summary, matrix_rows, cooccurrence)
    OUTPUT_HTML.write_text(build_html(report_body, code_summary, matrix_rows, cooccurrence), encoding="utf-8")
    print(OUTPUT_HTML)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
