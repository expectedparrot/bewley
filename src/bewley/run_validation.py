"""Bind returned scenarios to the originating Jobs before interpreting answers."""

from __future__ import annotations

import json
from typing import Any

from .exceptions import BewleyError


def scenario_identity(scenario: Any) -> str:
    return json.dumps(
        dict(scenario), sort_keys=True, ensure_ascii=False, separators=(",", ":")
    )


def trusted_scenario(result: Any, scenarios: Any) -> dict:
    returned = dict(result["scenario"])
    signature = scenario_identity(returned)
    for scenario in scenarios:
        if scenario_identity(scenario) == signature:
            return dict(scenario)
    raise BewleyError(
        "Returned scenario does not match the originating Jobs package.",
        code="INCOMPLETE_RESULTS",
        context={
            "document_id": returned.get("document_id"),
            "revision_id": returned.get("revision_id"),
        },
        hint="Use Results from these Jobs; preserve the original scenario fields unchanged.",
    )


def validate_result_scenarios(results: Any, scenarios: Any) -> list:
    expected = {scenario_identity(scenario) for scenario in scenarios}
    rows = list(results)
    for result in rows:
        if scenario_identity(result["scenario"]) not in expected:
            raise BewleyError(
                "Results contain a changed or unexpected scenario.",
                code="INCOMPLETE_RESULTS",
            )
    return rows
