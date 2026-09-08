"""Case- and speaker-scoped evidence retrieval and explicit matrix denominators."""

from __future__ import annotations

from collections import defaultdict

from .exceptions import BewleyError
from .query_engine import ExprParser
from .util import annotation_overlap


def selected_cases(project, conn, case=None, attributes=()):
    ids = {
        row["case_id"]
        for row in conn.execute("SELECT case_id FROM cases WHERE status='active'")
    }
    if case:
        ids &= {project.resolve_case(conn, case)["case_id"]}
    for predicate in attributes:
        name, separator, value = predicate.partition("=")
        if not separator:
            raise BewleyError("Attribute filters use NAME=VALUE.", code="INVALID_INPUT")
        definition = conn.execute(
            "SELECT * FROM attribute_definitions WHERE name=?", (name,)
        ).fetchone()
        if definition is None:
            raise BewleyError(
                "Unknown attribute filter.",
                code="NOT_FOUND",
                context={"attribute": name},
            )
        value = project._validate_attribute_value(definition, value)
        ids &= {
            row["case_id"]
            for row in conn.execute(
                "SELECT case_id FROM attribute_values WHERE attribute_id=? AND value=? AND special IS NULL",
                (definition["attribute_id"], value),
            )
        }
    return ids


def case_scopes(conn, ids):
    documents, speakers = defaultdict(set), defaultdict(lambda: defaultdict(set))
    for link in conn.execute("SELECT * FROM entity_links WHERE is_active=1"):
        if (
            link["source_kind"] == "case"
            and link["source_id"] in ids
            and link["target_kind"] == "document"
        ):
            documents[link["source_id"]].add(link["target_id"])
        elif (
            link["source_kind"] == "speaker"
            and link["target_kind"] == "case"
            and link["target_id"] in ids
        ):
            doc, label = link["source_id"].split(":", 1)
            speakers[link["target_id"]][doc].add(label)
            documents[link["target_id"]].add(doc)
    return documents, speakers


def metadata_documents(conn, predicates):
    if not predicates:
        return None
    selected = None
    for predicate in predicates:
        field, separator, value = predicate.partition("=")
        if not separator or field not in {
            "author",
            "recipient",
            "date",
            "document_type",
            "language",
            "collection",
        }:
            raise BewleyError(
                "Metadata filters use a supported FIELD=VALUE.", code="INVALID_INPUT"
            )
        ids = {
            row["document_id"]
            for row in conn.execute(
                "SELECT document_id FROM document_metadata WHERE field=? AND review_status='accepted' AND COALESCE(normalized,original)=?",
                (field, value),
            )
        }
        selected = ids if selected is None else selected & ids
    return selected


def scoped_annotations(
    project, conn, *, case=None, attributes=(), speaker=None, metadata=()
):
    metadata_ids = metadata_documents(conn, metadata)
    cases = selected_cases(project, conn, case, attributes)
    documents, speakers = case_scopes(conn, cases)
    turns = defaultdict(list)
    roles = {
        row["label"]: row["role"] for row in conn.execute("SELECT * FROM speaker_roles")
    }
    for row in conn.execute("SELECT * FROM speaker_turns"):
        turns[(row["document_id"], row["revision_id"])].append(row)
    output = []
    for row in conn.execute(
        "SELECT a.*,c.canonical_name,d.current_path FROM annotations a JOIN codes c USING(code_id) JOIN documents d USING(document_id) WHERE a.is_active=1 AND d.archived_at IS NULL ORDER BY d.current_path,a.start_byte,a.annotation_id"
    ):
        # A conflicted anchor remains inspectable in ordinary query/history, but
        # it cannot contribute to a scoped matrix as current accepted evidence.
        if metadata_ids is not None and row["document_id"] not in metadata_ids:
            continue
        if row["anchor_status"] == "conflicted":
            continue
        overlaps = {
            turn["label"]
            for turn in turns[(row["document_id"], row["document_revision_id"])]
            if row["scope_type"] == "span"
            and turn["start_byte"] < row["end_byte"]
            and turn["end_byte"] > row["start_byte"]
        }
        if speaker and not any(
            label == speaker or roles.get(label) == speaker for label in overlaps
        ):
            continue
        if case or attributes:
            matches = False
            for case_id in cases:
                labels = speakers[case_id].get(row["document_id"], set())
                if labels:
                    matches |= bool(labels & overlaps)
                else:
                    matches |= row["document_id"] in documents[case_id]
            if not matches:
                continue
        output.append(row)
    return output


def query_scoped(
    project, expression, mode, *, case=None, attributes=(), speaker=None, metadata=()
):
    if mode not in {"document", "annotation"}:
        raise BewleyError(
            "Query mode must be document or annotation.", code="INVALID_INPUT"
        )
    expr = ExprParser(expression).parse()
    with project.connect() as conn:
        rows = scoped_annotations(
            project,
            conn,
            case=case,
            attributes=attributes,
            speaker=speaker,
            metadata=metadata,
        )
        effective = project._effective_code_names(conn)
        groups = defaultdict(list)
        for row in rows:
            groups[row["document_id"]].append(row)
        if mode == "document":
            # Include eligible uncoded documents, so NOT retains its meaning.
            ids = selected_cases(project, conn, case, attributes)
            scoped_docs, _ = case_scopes(conn, ids)
            eligible = (
                set().union(*scoped_docs.values()) if (case or attributes) else None
            )
            metadata_ids = metadata_documents(conn, metadata)
            speaker_documents = None
            if speaker:
                speaker_documents = {
                    row["document_id"]
                    for row in conn.execute(
                        """SELECT DISTINCT t.document_id FROM speaker_turns t
                        JOIN document_revisions r ON r.revision_id=t.revision_id
                        LEFT JOIN speaker_roles s ON s.label=t.label
                        WHERE r.is_current=1 AND (t.label=? OR s.role=?)""",
                        (speaker, speaker),
                    )
                }
            output = []
            for doc in conn.execute(
                "SELECT * FROM documents WHERE archived_at IS NULL ORDER BY current_path"
            ):
                if eligible is not None and doc["document_id"] not in eligible:
                    continue
                if metadata_ids is not None and doc["document_id"] not in metadata_ids:
                    continue
                if (
                    speaker_documents is not None
                    and doc["document_id"] not in speaker_documents
                ):
                    continue
                names = set().union(
                    *(
                        effective.get(r["code_id"], set())
                        for r in groups[doc["document_id"]]
                    )
                )
                if expr.evaluate(names):
                    output.append(
                        {
                            "document_id": doc["document_id"],
                            "current_path": doc["current_path"],
                        }
                    )
            return output
        output = []
        for group in groups.values():
            for row in group:
                names = set().union(
                    *(
                        effective.get(r["code_id"], set())
                        for r in group
                        if annotation_overlap(row, r)
                    )
                )
                if expr.evaluate(names):
                    output.append(
                        {
                            "annotation_id": row["annotation_id"],
                            "document_id": row["document_id"],
                            "canonical_name": row["canonical_name"],
                            "current_path": row["current_path"],
                            "start_line": row["start_line"],
                            "end_line": row["end_line"],
                            "anchor_status": row["anchor_status"],
                            "text": row["exact_text"]
                            if row["scope_type"] == "span"
                            else "<document>",
                        }
                    )
        return output


def code_case_matrix(project, attributes=()):
    with project.connect() as conn:
        ids = selected_cases(project, conn, attributes=attributes)
        documents, _ = case_scopes(conn, ids)
        active_documents = {
            row["document_id"]
            for row in conn.execute(
                "SELECT document_id FROM documents WHERE archived_at IS NULL"
            )
        }
        codes = [
            dict(row)
            for row in conn.execute(
                "SELECT code_id,canonical_name,parent_code_id FROM codes WHERE status='active' ORDER BY canonical_name"
            )
        ]
        parents = {code["code_id"]: code["parent_code_id"] for code in codes}
        resolution = project.merge_resolution_map(conn)
        cases = [
            dict(row)
            for row in conn.execute(
                "SELECT case_id,name FROM cases WHERE status='active' ORDER BY name"
            )
            if row["case_id"] in ids
        ]
        cells = []
        for case in cases:
            rows = scoped_annotations(project, conn, case=case["case_id"])
            grouped = defaultdict(dict)
            for row in rows:
                code_id = resolution.get(row["code_id"], row["code_id"])
                visited = set()
                while code_id and code_id not in visited:
                    visited.add(code_id)
                    grouped[code_id][row["annotation_id"]] = row
                    code_id = parents.get(code_id)
            denominator = len(documents[case["case_id"]] & active_documents)
            for code in codes:
                evidence = list(grouped[code["code_id"]].values())
                count = len({row["document_id"] for row in evidence})
                cells.append(
                    {
                        "case_id": case["case_id"],
                        "code_id": code["code_id"],
                        "annotation_count": len(evidence),
                        "coded_document_count": count,
                        "eligible_document_count": denominator,
                        "document_share": count / denominator if denominator else None,
                    }
                )
    return {
        "cases": cases,
        "codes": codes,
        "cells": cells,
        "denominator": "Distinct active documents explicitly linked to the case, including speaker-case links.",
        "evidence_policy": "Active non-conflicted annotations; speaker-linked cases use overlapping speaker turns. Parent codes include descendants, deduplicated by annotation ID.",
    }
