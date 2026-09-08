"""Versioned run evidence stored immutably and registered in the event log."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .exceptions import BewleyError
from .integrity import file_digest
from .util import atomic_create_bytes, sha256_bytes


def registered_input(project, path: Path) -> bytes | None:
    """Read the latest registered input, rejecting altered working copies.

    Older, unregistered runs can still be imported from their original files.
    A deleted working manifest is recoverable from its immutable snapshot.
    """
    resolved = path.resolve()
    locator = (
        resolved.relative_to(project.root.resolve()).as_posix()
        if resolved.is_relative_to(project.root.resolve())
        else str(resolved)
    )
    with project.connect() as conn:
        record = conn.execute(
            "SELECT sha256 FROM artifact_versions WHERE path=? ORDER BY sequence_number DESC LIMIT 1",
            (locator,),
        ).fetchone()
    if record is None:
        return path.read_bytes() if path.exists() else None
    snapshot = project.meta / "objects" / "artifacts" / record["sha256"]
    content = snapshot.read_bytes()
    if sha256_bytes(content) != record["sha256"] or (
        path.exists() and file_digest(path) != record["sha256"]
    ):
        raise BewleyError(
            "Run input differs from its registered evidence.",
            code="INTEGRITY_ERROR",
            context={"path": str(path)},
        )
    return content


def register_files(project, command: str, paths: list[Path], roles=None) -> list[dict]:
    records = []
    directory = project.meta / "objects" / "artifacts"
    directory.mkdir(parents=True, exist_ok=True)
    with project.connect() as conn:
        known = {
            (row["path"], row["sha256"])
            for row in conn.execute("SELECT path,sha256 FROM artifact_versions")
        }
    for path in sorted(set(paths)):
        path = path if path.is_absolute() else project.root / path
        if not path.is_file() or path.is_symlink() or path.name.startswith(".env"):
            continue
        resolved = path.resolve()
        if resolved.is_relative_to(project.meta.resolve()):
            continue
        content = path.read_bytes()
        digest = sha256_bytes(content)
        relative = (
            resolved.relative_to(project.root.resolve()).as_posix()
            if resolved.is_relative_to(project.root.resolve())
            else None
        )
        locator = relative or str(resolved)
        if (locator, digest) in known:
            continue
        destination = directory / digest
        if destination.exists():
            if file_digest(destination) != digest:
                raise BewleyError(
                    "Registered artifact object is corrupt.", code="INTEGRITY_ERROR"
                )
        else:
            try:
                atomic_create_bytes(destination, content)
            except FileExistsError:
                if file_digest(destination) != digest:
                    raise BewleyError(
                        "Artifact object conflict.", code="INTEGRITY_ERROR"
                    )
        known.add((locator, digest))
        role = (
            "sidecar"
            if "log" in path.stem
            else "results"
            if "result" in path.stem
            else "models"
            if "model" in path.stem
            else "jobs"
            if "job" in path.stem
            else "artifact"
        )
        role = (roles or {}).get(str(resolved), role)
        records.append(
            {
                "path": locator,
                "relative_path": relative,
                "sha256": digest,
                "size": len(content),
                "role": role,
            }
        )
    if records:
        project.append_event(
            "artifacts_registered", {"command": command, "artifacts": records}
        )
    return records


def record_command_artifacts(project, command: str, namespace: dict[str, Any]) -> None:
    """Capture the explicit file inputs/outputs of a successful workflow command.

    Only Path values held by the command are considered; no directory crawl or
    credential/configuration inspection is performed. JSON data supplies paths
    returned by helper functions (e.g. model manifests).
    """
    paths = []

    def collect(value, *, strings=False):
        if isinstance(value, Path):
            paths.append(value)
        elif isinstance(value, dict):
            for item in value.values():
                collect(item, strings=strings)
        elif isinstance(value, (list, tuple)):
            for item in value:
                collect(item, strings=strings)
        elif (
            strings
            and isinstance(value, str)
            and "\n" not in value
            and len(value) < 4096
        ):
            candidate = Path(value)
            if candidate.suffix in {".ep", ".json", ".jsonl", ".csv", ".html", ".md"}:
                paths.append(candidate)

    for name, value in namespace.items():
        if isinstance(value, Path) or (
            name in {"sources", "results_paths"} and isinstance(value, list)
        ):
            collect(value)
    data = namespace.get("data", {})
    if isinstance(data, dict):
        for key in {"output", "models", "manifest", "ingest_log", "apply_log"}:
            collect(data.get(key), strings=True)
    roles = {}
    for name, role in {
        "jobs_path": "jobs",
        "jobs_source": "jobs",
        "jobs_denominator": "jobs",
        "result_path": "results",
        "results": "results",
        "models_target": "models",
        "model_target": "models",
    }.items():
        value = namespace.get(name)
        if isinstance(value, Path):
            roles[
                str((value if value.is_absolute() else project.root / value).resolve())
            ] = role
    if command.endswith((" jobs", "-jobs")) and isinstance(
        namespace.get("target"), Path
    ):
        roles[str(namespace["target"].resolve())] = "jobs"
    register_files(project, command, paths, roles)
