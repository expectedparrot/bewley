"""Pure helpers: hashing, atomic writes, text/byte ranges, colors, time."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import sqlite3
import tempfile
import tomllib
from pathlib import Path
from typing import Any

from .exceptions import BewleyError


def utcnow() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n"


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        handle.write(content)
        temp_name = handle.name
    os.replace(temp_name, path)


def load_toml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return tomllib.loads(path.read_text(encoding="utf-8"))


def ensure_utf8_bytes(path: Path) -> bytes:
    data = path.read_bytes()
    try:
        data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise BewleyError(f"{path} is not valid UTF-8: {exc}") from exc
    return data


def count_lines(text: str) -> int:
    if not text:
        return 0
    return text.count("\n") + (0 if text.endswith("\n") else 1)


def line_offsets(text: str) -> list[int]:
    starts = [0]
    running = 0
    for line in text.splitlines(keepends=True):
        running += len(line.encode("utf-8"))
        starts.append(running)
    return starts


def byte_to_line_range(text: str, start_byte: int, end_byte: int) -> tuple[int, int]:
    starts = line_offsets(text)
    start_line = 1
    end_line = 1
    for idx, offset in enumerate(starts[:-1], start=1):
        next_offset = starts[idx]
        if offset <= start_byte < next_offset:
            start_line = idx
        if offset < end_byte <= next_offset:
            end_line = idx
            break
        if end_byte == offset and idx > 1:
            end_line = idx - 1
    else:
        if starts:
            end_line = max(1, len(starts) - 1)
    return start_line, max(start_line, end_line)


def lines_to_byte_range(text: str, start_line: int, end_line: int) -> tuple[int, int]:
    if start_line < 1 or end_line < start_line:
        raise BewleyError("invalid line range", code="INVALID_INPUT")
    starts = line_offsets(text)
    last_line = max(1, len(starts) - 1)
    if end_line > last_line:
        raise BewleyError(f"line range exceeds document length ({last_line} lines)", code="INVALID_INPUT")
    start_byte = starts[start_line - 1]
    end_byte = starts[end_line]
    return start_byte, end_byte


def safe_decode(data: bytes) -> str:
    return data.decode("utf-8", errors="replace")


def format_timestamp(seconds: Any) -> str:
    try:
        total = max(0.0, float(seconds))
    except (TypeError, ValueError):
        return "00:00.00"
    minutes = int(total // 60)
    remainder = total - (minutes * 60)
    return f"{minutes:02d}:{remainder:05.2f}"


def annotation_overlap(a: sqlite3.Row, b: sqlite3.Row) -> bool:
    if a["scope_type"] == "document" and b["scope_type"] == "document":
        return True
    if a["scope_type"] == "document" or b["scope_type"] == "document":
        return True
    return not (a["end_byte"] <= b["start_byte"] or b["end_byte"] <= a["start_byte"])


def parse_byte_range(spec: str) -> tuple[int, int]:
    try:
        start_text, end_text = spec.split(":", 1)
        start = int(start_text)
        end = int(end_text)
    except ValueError as exc:
        raise BewleyError("expected range format START:END") from exc
    return start, end


def default_code_color(name: str) -> str:
    digest = sha256_text(name)
    hue = int(digest[:6], 16) % 360
    return f"hsl({hue} 68% 52%)"


def coerce_code_color(color: str | None, name: str) -> str:
    if color and color.strip():
        return color.strip()
    return default_code_color(name)


def soft_color(color: str, alpha: float) -> str:
    value = color.strip()
    if value.startswith("#"):
        hex_part = value[1:]
        if len(hex_part) == 3:
            hex_part = "".join(ch * 2 for ch in hex_part)
        if len(hex_part) == 6:
            red = int(hex_part[0:2], 16)
            green = int(hex_part[2:4], 16)
            blue = int(hex_part[4:6], 16)
            return f"rgba({red}, {green}, {blue}, {alpha})"
    if value.startswith("hsl(") and value.endswith(")"):
        return f"hsl({value[4:-1]} / {alpha})"
    if value.startswith("rgb(") and value.endswith(")"):
        return f"rgb({value[4:-1]} / {alpha})"
    return value


def byte_to_char_index_map(text: str) -> dict[int, int]:
    mapping = {0: 0}
    byte_offset = 0
    for index, char in enumerate(text, start=1):
        byte_offset += len(char.encode("utf-8"))
        mapping[byte_offset] = index
    return mapping
