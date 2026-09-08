"""Immutable released codebooks used as fixed model-coding contracts."""

import hashlib
import json
import sqlite3

from .exceptions import BewleyError


def released_codebook(project, reference):
    with project.connect() as conn:
        row = conn.execute(
            "SELECT * FROM codebook_releases WHERE name=? OR release_id=?",
            (reference, reference),
        ).fetchone()
    if row is None:
        raise BewleyError(
            "Unknown codebook release.",
            code="NOT_FOUND",
            context={"release": reference},
        )
    codes = json.loads(row["snapshot"])
    # Old releases stored names only. Resolve historical IDs by replaying to
    # that release, never by guessing against today's potentially renamed codes.
    if any("code_id" not in code for code in codes):
        with sqlite3.connect(":memory:") as conn:
            conn.row_factory = sqlite3.Row
            project._init_connection(conn)
            for event in project.all_events():
                project.apply_event(conn, event)
                if (
                    event["event_type"] == "codebook_released"
                    and event["payload"]["release_id"] == row["release_id"]
                ):
                    break
            historical = {
                r["canonical_name"]: r["code_id"]
                for r in conn.execute("SELECT * FROM codes")
            }
            for code in codes:
                code["code_id"] = historical[code["canonical_name"]]
    return {
        "name": row["name"],
        "release_id": row["release_id"],
        "codes": codes,
        "fingerprint": hashlib.sha256(
            json.dumps(codes, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest(),
    }


CODING_PROMPT = """Apply this released codebook to the supplied document.
Use only the supplied code_id values. Respect each definition, inclusion and
exclusion criteria. Return an empty array if no code applies. Do not invent codes.
Return only a JSON array of objects with "code" (the code_id), "description"
(the supplied definition), and "quote" (an exact contiguous source quotation).
Anchor transcript evidence in participant answers, not interviewer questions.

Released codebook:
{{ codebook_json }}

Document:
{{ document_text_numbered }}
"""
