"""The Bewley error type."""
from __future__ import annotations



class BewleyError(Exception):
    def __init__(self, message: str, code: str = "ERROR", context: dict | None = None, hint: str = ""):
        super().__init__(message)
        self.code = code
        self.message = message
        self.context = context or {}
        self.hint = hint
