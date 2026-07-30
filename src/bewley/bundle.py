"""Portable, integrity-checked Bewley project bundles."""
from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any

from bewley import __version__

from .exceptions import BewleyError
from .project import Project
from .util import utcnow

BUNDLE_FORMAT = "bewley-project"
BUNDLE_VERSION = 1
MANIFEST_NAME = "manifest.json"
EVIDENCE_NAMES = {
    "jobs.ep",
    "models.ep",
    "results.ep",
    "candidate_codes.csv",
    "ingest_log.jsonl",
    "apply_log.jsonl",
}
EXCLUDED_PARTS = {".git", "__pycache__", ".pytest_cache", ".DS_Store"}
EXCLUDED_NAMES = {".env", "write.lock", "bewley.sqlite"}


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _safe_member(name: str) -> PurePosixPath:
    path = PurePosixPath(name)
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise BewleyError(
            "Bundle contains an unsafe path.",
            code="INVALID_BUNDLE",
            context={"path": name},
        )
    return path


def _bundle_files(project: Project, output: Path) -> dict[str, Path]:
    files: dict[str, Path] = {}
    for path in project.meta.rglob("*"):
        if not path.is_file() or path.is_symlink():
            continue
        relative = path.relative_to(project.root)
        if any(part in EXCLUDED_PARTS for part in relative.parts):
            continue
        if path.name in EXCLUDED_NAMES or relative.parts[:3] == (".bewley", "index", "bewley.sqlite"):
            continue
        files[relative.as_posix()] = path

    # Materialize current document revisions at their logical working paths.
    with project.connect() as conn:
        rows = conn.execute(
            """
            SELECT d.document_id, d.current_path, r.content_sha256
            FROM documents d
            JOIN document_revisions r ON r.document_id = d.document_id AND r.is_current = 1
            WHERE d.archived_at IS NULL
            """
        ).fetchall()
    for row in rows:
        relative = _safe_member(row["current_path"])
        files[relative.as_posix()] = project.objects_dir / row["content_sha256"]

    # Preserve known open-coding run evidence wherever it lives in the project.
    for path in project.root.rglob("*"):
        if not path.is_file() or path.is_symlink() or path.resolve() == output.resolve():
            continue
        relative = path.relative_to(project.root)
        if any(part in EXCLUDED_PARTS for part in relative.parts) or path.name in EXCLUDED_NAMES:
            continue
        if path.name in EVIDENCE_NAMES or (
            path.suffix == ".ep"
            and any(token in path.name.lower() for token in ("jobs", "models", "results"))
        ):
            files[relative.as_posix()] = path
    return files


def pack_project(project: Project, output: Path) -> dict[str, Any]:
    """Write a validated project snapshot to a portable .bewley container."""
    problems = project.fsck()
    if problems:
        raise BewleyError(
            "Refusing to pack a project that fails integrity checks.",
            code="INTEGRITY_ERROR",
            context={"problems": problems},
        )
    output = output.resolve()
    if output.exists():
        raise BewleyError(
            "Output bundle already exists.",
            code="ALREADY_EXISTS",
            context={"path": str(output)},
            hint="Choose a new output path; bundles are never overwritten.",
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    files = _bundle_files(project, output)
    members = []
    payloads: dict[str, bytes] = {}
    for name, path in sorted(files.items()):
        data = path.read_bytes()
        payloads[name] = data
        members.append({"path": name, "size": len(data), "sha256": _digest(data)})
    manifest = {
        "format": BUNDLE_FORMAT,
        "format_version": BUNDLE_VERSION,
        "created_at": utcnow(),
        "bewley_version": __version__,
        "members": members,
    }
    with zipfile.ZipFile(output, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(MANIFEST_NAME, json.dumps(manifest, indent=2) + "\n")
        for name, data in payloads.items():
            archive.writestr(name, data)
    return {
        "path": str(output),
        "format": BUNDLE_FORMAT,
        "format_version": BUNDLE_VERSION,
        "member_count": len(members),
        "sha256": _digest(output.read_bytes()),
    }


def _validated_archive(bundle: Path) -> tuple[dict[str, Any], dict[str, bytes]]:
    try:
        with zipfile.ZipFile(bundle, "r") as archive:
            names = archive.namelist()
            if len(names) != len(set(names)) or MANIFEST_NAME not in names:
                raise BewleyError("Bundle manifest is missing or paths are duplicated.", code="INVALID_BUNDLE")
            manifest = json.loads(archive.read(MANIFEST_NAME))
            if manifest.get("format") != BUNDLE_FORMAT or manifest.get("format_version") != BUNDLE_VERSION:
                raise BewleyError(
                    "Unsupported Bewley bundle format.",
                    code="UNSUPPORTED_BUNDLE_VERSION",
                    context={
                        "format": manifest.get("format"),
                        "format_version": manifest.get("format_version"),
                    },
                )
            declared = {row["path"]: row for row in manifest.get("members", [])}
            actual = set(names) - {MANIFEST_NAME}
            if set(declared) != actual:
                raise BewleyError("Bundle members do not match its manifest.", code="INVALID_BUNDLE")
            payloads = {}
            for name, row in declared.items():
                _safe_member(name)
                data = archive.read(name)
                if len(data) != row.get("size") or _digest(data) != row.get("sha256"):
                    raise BewleyError(
                        "Bundle member failed its integrity check.",
                        code="BUNDLE_HASH_MISMATCH",
                        context={"path": name},
                    )
                payloads[name] = data
            return manifest, payloads
    except (zipfile.BadZipFile, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise BewleyError(
            "File is not a valid Bewley project bundle.",
            code="INVALID_BUNDLE",
            context={"path": str(bundle), "detail": str(exc)},
        ) from exc


def unpack_project(bundle: Path, destination: Path) -> dict[str, Any]:
    """Validate and restore a bundle into a new project directory."""
    bundle = bundle.resolve()
    if not bundle.is_file():
        raise BewleyError("Bundle does not exist.", code="NOT_FOUND", context={"path": str(bundle)})
    destination = destination.resolve()
    if destination.exists():
        raise BewleyError(
            "Destination already exists.",
            code="ALREADY_EXISTS",
            context={"path": str(destination)},
            hint="Choose a new directory; unpack never merges or overwrites projects.",
        )
    manifest, payloads = _validated_archive(bundle)
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".bewley-unpack-", dir=destination.parent))
    try:
        for name, data in payloads.items():
            target = staging.joinpath(*PurePosixPath(name).parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        project = Project(staging)
        if not project.config_path.exists() or not project.events_dir.exists():
            raise BewleyError("Bundle does not contain a Bewley project store.", code="INVALID_BUNDLE")
        project.db_path.parent.mkdir(parents=True, exist_ok=True)
        project.rebuild_index()
        problems = project.fsck()
        if problems:
            raise BewleyError(
                "Restored project failed integrity checks.",
                code="INTEGRITY_ERROR",
                context={"problems": problems},
            )
        staging.rename(destination)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {
        "path": str(destination),
        "format": manifest["format"],
        "format_version": manifest["format_version"],
        "member_count": len(payloads),
        "integrity": "ok",
    }
