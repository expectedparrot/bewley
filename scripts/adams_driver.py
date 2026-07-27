#!/usr/bin/env python3
"""Build the Adams-letters worked run for the bewley tutorial.

Runs every tutorial command against the real corpus, uses a deterministic
fixture Results object in place of the external `ep run` (no model calls),
captures each JSON envelope to build/tutorial/captures/, and regenerates
the explorer + plots into the repo's docs/.

Usage (from the repo root, with dev dependencies installed):

    python scripts/adams_driver.py   # rerun the worked run, refresh captures
    python scripts/build_index.py    # render docs/index.html from captures
"""
from __future__ import annotations

import csv
import json
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SRC = str(REPO / "src")
TUTORIAL = REPO / "build" / "tutorial"
RUN = TUTORIAL / "adams-letters"
CAPTURES = TUTORIAL / "captures"

# (code, filename, anchor phrase) — the reviewer-kept candidate codes.
PLANNED = [
    ("political_voice", "1776-march-31-abigail-adams.txt", "remember the ladies"),
    ("political_voice", "1776-april-14-john-adams.txt", "cannot but laugh"),
    ("political_voice", "1775-december-10-abigail-adams.txt", "angry with your House of Assembly"),
    ("public_duty", "1776-july-03-john-adams.txt", "memorable epocha"),
    ("public_duty", "1775-september-17-john-adams.txt", "Henry is made a General"),
    ("war_and_danger", "1775-june-15-abigail-adams.txt", "continual expectation of alarms"),
    ("war_and_danger", "1776-march-17-john-adams.txt", "cannonade was heard"),
    ("health_and_scarcity", "1775-june-15-abigail-adams.txt", "bundle of pins"),
    ("health_and_scarcity", "1776-july-13-abigail-adams.txt", "inoculated for the small-pox"),
    ("household_responsibility", "1776-june-03-abigail-adams.txt", "merchant complains of the farmer"),
    ("information_and_delay", "1776-march-31-abigail-adams.txt", "letter half as long"),
    ("information_and_delay", "1775-may-04-abigail-adams.txt", "I want very much to hear from you"),
    ("separation_and_affection", "1775-may-04-abigail-adams.txt", "heart of lead"),
    ("separation_and_affection", "1775-september-17-john-adams.txt", "tenderest language"),
]
REJECT_CODES = ["daily_minutiae", "travel_logistics", "weather_report"]


def bewley_human(*args: str, capture: str, cwd: Path | None = None) -> str:
    """Run a command with --human and capture the rendered text."""
    env = os.environ.copy()
    env["PYTHONPATH"] = SRC
    env.pop("BEWLEY_HUMAN_OUTPUT", None)
    completed = subprocess.run(
        [sys.executable, "-m", "bewley", *args, "--human"],
        cwd=cwd or RUN, env=env, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    assert completed.returncode == 0, f"failed: {args}\n{completed.stdout}\n{completed.stderr}"
    (CAPTURES / f"{capture}.txt").write_text(
        "$ bewley " + shlex.join(args) + " --human\n" + completed.stdout
    )
    return completed.stdout


def bewley(*args: str, expect_fail: bool = False, capture: str | None = None, cwd: Path | None = None) -> dict:
    env = os.environ.copy()
    env["PYTHONPATH"] = SRC
    env.pop("BEWLEY_HUMAN_OUTPUT", None)
    completed = subprocess.run(
        [sys.executable, "-m", "bewley", *args],
        cwd=cwd or RUN, env=env, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if expect_fail:
        assert completed.returncode != 0, f"expected failure: {args}\n{completed.stdout}"
    else:
        assert completed.returncode == 0, f"failed: {args}\n{completed.stdout}\n{completed.stderr}"
    payload = json.loads(completed.stdout)
    if capture:
        argv_display = "bewley " + shlex.join(args)
        (CAPTURES / f"{capture}.json").write_text(
            json.dumps({"argv_display": argv_display, "payload": payload}, indent=2, ensure_ascii=False)
        )
    return payload


def sentence_containing(text: str, phrase: str) -> str:
    """Extract the exact sentence (verbatim substring) containing the phrase."""
    lowered = text.lower()
    at = lowered.find(phrase.lower())
    assert at >= 0, f"phrase not found: {phrase}"
    start = at
    while start > 0 and text[start - 1] not in ".!?\n":
        start -= 1
    while start < len(text) and text[start] in " \"'":
        start += 1
    end = at
    while end < len(text) and text[end] not in ".!?":
        end += 1
    end = min(end + 1, len(text))
    sentence = text[start:end].strip()
    assert sentence in text and len(sentence) > 20, f"bad sentence for {phrase!r}: {sentence!r}"
    return sentence


def first_body_sentence(text: str) -> str:
    for line in text.splitlines():
        if line.strip() and not any(line.startswith(prefix) for prefix in ("Title:", "Author:", "Recipient:", "Date:", "Source")):
            return sentence_containing(line, line.strip().split()[0])
    raise AssertionError("no body")


def line_of(text: str, phrase: str) -> int:
    for number, line in enumerate(text.splitlines(), 1):
        if phrase.lower() in line.lower():
            return number
    raise AssertionError(phrase)


def main() -> None:
    if TUTORIAL.exists():
        shutil.rmtree(TUTORIAL)
    CAPTURES.mkdir(parents=True)

    # ── Chapter: start the project ─────────────────────────────────────────
    version_envelope = bewley("version", capture="01-version", cwd=TUTORIAL)
    import tomllib
    declared = tomllib.loads((REPO / "pyproject.toml").read_text(encoding="utf-8"))["project"]["version"]
    reported = version_envelope["data"]["version"]
    assert reported == declared, (
        f"docs would embed version {reported} but pyproject declares {declared}; "
        "refresh the environment with `pip install -e .` before regenerating"
    )
    bewley("example", "fetch", "adams-letters", capture="01b-fetch", cwd=TUTORIAL)
    texts = {p.name: p.read_text(encoding="utf-8") for p in sorted((RUN / "corpus").glob("*.txt"))}
    bewley("init", capture="02-init")
    bewley("study", "set", "--method", "grounded-theory", "--unit", "document",
           capture="02b-study-set")
    bewley("question", "add",
           "How did John and Abigail Adams negotiate public duty, household responsibility, "
           "political voice, danger, and emotional intimacy during the American Revolution?",
           capture="02c-question-add")
    bewley_human("study", "show", capture="02h-study")
    bewley("add", "corpus/1775-april-30-john-adams.txt", capture="03-add")
    for name in sorted(texts):
        if name != "1775-april-30-john-adams.txt":
            bewley("add", f"corpus/{name}")
    bewley("list", "documents", capture="04-list-documents")
    bewley_human("list", "documents", capture="04h-list-documents")

    # ── Cases: who the letters belong to ───────────────────────────────────
    bewley("case", "create", "Abigail Adams", "--type", "person", capture="04b-case-create")
    bewley("case", "create", "John Adams", "--type", "person")
    bewley("attribute", "define", "role", "--type", "categorical",
           "--values", "home-front,congressional-delegate", capture="04c-attribute-define")
    bewley("case", "set", "Abigail Adams", "role", "home-front")
    bewley("case", "set", "John Adams", "role", "congressional-delegate")
    bewley("case", "link", "Abigail Adams", "corpus/1775-may-04-abigail-adams.txt",
           "--as", "author", capture="04d-case-link")
    for name in sorted(texts):
        if name == "1775-may-04-abigail-adams.txt":
            continue
        author = "Abigail Adams" if "abigail" in name else "John Adams"
        bewley("case", "link", author, f"corpus/{name}", "--as", "author")
    bewley_human("case", "list", capture="04h-cases")

    bewley("status", capture="05-status")
    bewley("next", capture="06-next")

    # ── Chapter: model-assisted open coding ────────────────────────────────
    bewley("open-coding", "jobs", "--output", "jobs.ep", "--model", "gpt-4.1-mini", capture="07-jobs")

    # Fixture Results standing in for the external `ep run` (deterministic;
    # answer text is what varies in a real run).
    sys.path.insert(0, SRC)
    from edsl import Agent, Jobs, Model, Results, Scenario, Survey
    from edsl.results import Result

    jobs = Jobs.git.load(RUN / "jobs.ep")
    planned_by_file: dict[str, list[tuple[str, str]]] = {}
    for code, filename, phrase in PLANNED:
        quote = sentence_containing(texts[filename], phrase)
        planned_by_file.setdefault(filename, []).append((code, quote))

    def result_for(scenario_data: dict, answer_entries: list[dict], model_name: str = "gpt-4.1-mini") -> Result:
        return Result(
            agent=Agent(), scenario=Scenario(scenario_data), model=Model(model_name),
            iteration=0, answer={"open_coding": json.dumps(answer_entries)},
        )

    descriptions = {
        "political_voice": "Claims to political standing or representation by or for those excluded from it.",
        "public_duty": "Framing congressional or military service as obligation to the country.",
        "war_and_danger": "Direct experience or anticipation of military violence.",
        "health_and_scarcity": "Illness, inoculation, or shortages of goods and money.",
        "household_responsibility": "Managing the farm, prices, and family economy alone.",
        "information_and_delay": "The struggle to get timely news and letters.",
        "separation_and_affection": "Longing, tenderness, and the cost of being apart.",
    }
    results_rows = []
    reject_cycle = 0
    for item in jobs.scenarios:
        scenario_data = dict(item)
        filename = Path(scenario_data["document_path"]).name
        entries = [
            {"code": code, "description": descriptions[code], "quote": quote}
            for code, quote in planned_by_file.get(filename, [])
        ]
        if not entries:
            entries = [{
                "code": REJECT_CODES[reject_cycle % len(REJECT_CODES)],
                "description": "Routine detail without analytic weight.",
                "quote": first_body_sentence(texts[filename]),
            }]
            reject_cycle += 1
        results_rows.append(result_for(scenario_data, entries))
    Results(survey=Survey([]), data=results_rows).git.save(RUN / "results.ep")

    bewley("open-coding", "ingest", "results.ep", "--jobs", "jobs.ep", capture="08-ingest")
    bewley("open-coding", "candidates", capture="08b-candidates")
    bewley_human("open-coding", "candidates", capture="08h-candidates")

    # ── Chapter: review and apply ──────────────────────────────────────────
    candidates_path = RUN / "qualitative-analysis" / "candidate_codes.csv"
    with candidates_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
        fieldnames = handle.name and list(rows[0].keys())
    kept = [row for row in rows if row["code_name"] in descriptions]
    with candidates_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(kept)
    (CAPTURES / "review-note.json").write_text(json.dumps({
        "candidates": len(rows), "kept": len(kept), "rejected": len(rows) - len(kept),
        "rejected_codes": sorted({r["code_name"] for r in rows if r["code_name"] not in descriptions}),
    }, indent=2))

    bewley("open-coding", "apply", "--dry-run", capture="09-apply-dry-run")
    bewley("open-coding", "apply", capture="10-apply")
    bewley("show", "snippets", "--code", "political_voice", capture="11-snippets")
    bewley_human("show", "snippets", "--code", "political_voice", capture="11h-snippets")
    bewley_human("code", "list", capture="11h-codes")

    # ── Chapter: recovering from a failed run (separate artifacts) ─────────
    bewley("open-coding", "jobs", "--output", "pilot.jobs.ep", "--pilot", "2",
           "--model", "gpt-4.1-mini", "--force", capture="12-pilot-jobs")
    pilot_jobs = Jobs.git.load(RUN / "pilot.jobs.ep")
    pilot_scenarios = [dict(item) for item in pilot_jobs.scenarios]
    good = pilot_scenarios[0]
    bad = pilot_scenarios[1]
    good_name = Path(good["document_path"]).name
    Results(survey=Survey([]), data=[
        result_for(good, [{
            "code": "war_and_danger", "description": descriptions["war_and_danger"],
            "quote": first_body_sentence(texts[good_name]),
        }]),
        result_for(bad, "this run was truncated and is not a JSON array"),
    ]).git.save(RUN / "run1.results.ep")
    bewley("open-coding", "ingest", "run1.results.ep", "--jobs", "pilot.jobs.ep",
           "--output", "retry-demo.csv", expect_fail=True, capture="13-ingest-fails")
    bewley("open-coding", "jobs", "--from-failures", "run1.results.ep", "--jobs", "pilot.jobs.ep",
           "--output", "retry.jobs.ep", "--model", "gpt-4.1-mini", "--force", capture="14-from-failures")
    bad_name = Path(bad["document_path"]).name
    Results(survey=Survey([]), data=[
        result_for(bad, [{
            "code": "public_duty", "description": descriptions["public_duty"],
            "quote": first_body_sentence(texts[bad_name]),
        }]),
    ]).git.save(RUN / "run2.results.ep")
    bewley("open-coding", "ingest", "run1.results.ep", "run2.results.ep", "--jobs", "pilot.jobs.ep",
           "--output", "retry-demo.csv", capture="15-merged-ingest")

    # ── Chapter: refine the codebook ───────────────────────────────────────
    bewley("code", "create", "waiting_for_news",
           "--description", "Waiting anxiously for letters that do not come")
    may04 = texts["1775-may-04-abigail-adams.txt"]
    waiting_quote = (
        "I want very much to hear from you, how you stood your journey, and in what "
        "state you find yourself now. I felt very anxious about you; though I "
        "endeavored to be very insensible and heroic, yet my heart felt like a heart "
        "of lead."
    )
    assert waiting_quote in may04
    bewley("annotate", "apply", "waiting_for_news", "corpus/1775-may-04-abigail-adams.txt",
           "--quote", waiting_quote, capture="16-annotate-quote")
    bewley("code", "merge", "waiting_for_news", "--into", "information_and_delay", capture="17-merge")
    bewley("code", "show", "information_and_delay", capture="18-code-show")
    bewley("code", "create", "home_front", "--description", "The war as lived at home")
    bewley("code", "set-parent", "household_responsibility", "home_front")
    bewley("code", "set-parent", "health_and_scarcity", "home_front")
    bewley("code", "link", "separation_and_affection", "information_and_delay", "intensified_by",
           "--memo", "Delayed letters repeatedly sharpen the pain of separation.")
    bewley("code", "set-core", "separation_and_affection")
    bewley("code", "list", "--tree", capture="19-code-tree")
    bewley_human("code", "list", "--tree", capture="19h-code-tree")
    bewley("code", "coverage", "home_front", "--breakdown", capture="20-coverage")
    bewley_human("case", "show", "Abigail Adams", capture="20h-case-show")

    # ── Chapter: compare and memo ──────────────────────────────────────────
    bewley("query", "public_duty & separation_and_affection", capture="21-query-document")
    bewley("query", "information_and_delay & separation_and_affection", "--mode", "annotation",
           capture="22-query-annotation")
    bewley_human("query", "information_and_delay & separation_and_affection", "--mode", "annotation",
                 capture="22h-query-annotation")
    bewley("memo", "add", "--code", "separation_and_affection",
           "Affection and complaint travel together: nearly every tender passage sits beside a demand for more letters.",
           capture="23-memo")

    # ── Chapter: export ────────────────────────────────────────────────────
    bewley("export", "quotes", "--code", "political_voice", "--format", "jsonl", capture="24-export-quotes")
    bewley("export", "html", "--output", "adams-report.html",
           "--title", "Adams letters — coded corpus", capture="25-export-html")
    bewley("export", "theory", "--format", "mermaid", "--output", "theory.mmd", capture="26-export-theory")
    bewley("export", "plots", "--output-dir", "plots", capture="27-export-plots")

    # ── Chapter: integrity ────────────────────────────────────────────────
    bewley("fsck", capture="28-fsck")
    bewley("history", capture="29-history")
    bewley("next", capture="30-next-final")

    # ── Chapter: interviews and speakers (separate mini-project) ───────────
    IRUN = TUTORIAL / "adams-interviews"
    bewley("example", "fetch", "adams-interviews", capture="31-iv-fetch", cwd=TUTORIAL)
    bewley("init", cwd=IRUN)
    for path in sorted((IRUN / "corpus").glob("*.txt")):
        bewley("add", f"corpus/{path.name}", cwd=IRUN)
    bewley("speakers", "detect", "corpus/interview-01-abigail-adams.txt",
           capture="32-iv-detect", cwd=IRUN)
    bewley("speakers", "detect", "corpus/interview-02-john-adams.txt", cwd=IRUN)
    bewley("speakers", "detect", "corpus/interview-03-joint.txt", cwd=IRUN)
    bewley("next", capture="32b-iv-next", cwd=IRUN)
    bewley("speakers", "set-role", "INTERVIEWER", "interviewer",
           capture="33-iv-role", cwd=IRUN)
    bewley("speakers", "set-role", "ABIGAIL ADAMS", "participant", cwd=IRUN)
    bewley("speakers", "set-role", "JOHN ADAMS", "participant", cwd=IRUN)
    bewley("case", "create", "Abigail Adams", "--type", "person", cwd=IRUN)
    bewley("case", "create", "John Adams", "--type", "person", cwd=IRUN)
    bewley("speakers", "link-case", "corpus/interview-03-joint.txt", "ABIGAIL ADAMS",
           "Abigail Adams", capture="34-iv-link-case", cwd=IRUN)
    bewley("speakers", "link-case", "corpus/interview-03-joint.txt", "JOHN ADAMS",
           "John Adams", cwd=IRUN)
    bewley_human("speakers", "list", "corpus/interview-03-joint.txt",
                 capture="34h-iv-speakers", cwd=IRUN)
    bewley("code", "create", "political_voice",
           "--description", descriptions["political_voice"], cwd=IRUN)
    bewley("code", "create", "war_and_danger",
           "--description", descriptions["war_and_danger"], cwd=IRUN)
    bewley("annotate", "apply", "political_voice", "corpus/interview-01-abigail-adams.txt",
           "--quote", 'you asked him to "remember the ladies"',
           expect_fail=True, capture="35-iv-blocked", cwd=IRUN)
    bewley("annotate", "apply", "political_voice", "corpus/interview-01-abigail-adams.txt",
           "--quote", "I expected exactly what I received",
           capture="36-iv-quote", cwd=IRUN)
    bewley("annotate", "apply", "war_and_danger", "corpus/interview-01-abigail-adams.txt",
           "--turn", "8", capture="37-iv-turn", cwd=IRUN)

    # Refresh the shipped example artifacts from THIS run.
    report = (RUN / "adams-report.html").read_text(encoding="utf-8")
    (REPO / "docs" / "adams-report.html").write_text(
        report.replace(str(RUN), "."), encoding="utf-8"
    )
    for plot in (RUN / "plots").iterdir():
        shutil.copyfile(plot, REPO / "docs" / "plots" / plot.name)

    print("captures:", len(list(CAPTURES.glob("*.json"))))
    print("kept candidates:", len(kept), "of", len(rows))


if __name__ == "__main__":
    main()
