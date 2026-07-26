"""Boolean code-expression parser and evaluator."""
from __future__ import annotations

from dataclasses import dataclass

from .exceptions import BewleyError


class BoolExpr:
    def evaluate(self, names: set[str]) -> bool:
        raise NotImplementedError


@dataclass
class Term(BoolExpr):
    value: str

    def evaluate(self, names: set[str]) -> bool:
        return self.value in names


@dataclass
class Not(BoolExpr):
    expr: BoolExpr

    def evaluate(self, names: set[str]) -> bool:
        return not self.expr.evaluate(names)


@dataclass
class BinOp(BoolExpr):
    left: BoolExpr
    right: BoolExpr
    kind: str

    def evaluate(self, names: set[str]) -> bool:
        if self.kind == "AND":
            return self.left.evaluate(names) and self.right.evaluate(names)
        if self.kind == "OR":
            return self.left.evaluate(names) or self.right.evaluate(names)
        raise ValueError(self.kind)


class ExprParser:
    def __init__(self, text: str) -> None:
        self.tokens = self.tokenize(text)
        self.index = 0

    @staticmethod
    def tokenize(text: str) -> list[str]:
        tokens: list[str] = []
        i = 0
        while i < len(text):
            ch = text[i]
            if ch.isspace():
                i += 1
                continue
            if ch in "()":
                tokens.append(ch)
                i += 1
                continue
            if ch in "&|":
                # Accept & / && as AND and | / || as OR.
                tokens.append("AND" if ch == "&" else "OR")
                i += 2 if i + 1 < len(text) and text[i + 1] == ch else 1
                continue
            if ch == "!":
                tokens.append("NOT")
                i += 1
                continue
            if ch in "\"'":
                quote = ch
                i += 1
                start = i
                while i < len(text) and text[i] != quote:
                    i += 1
                if i >= len(text):
                    raise BewleyError("unterminated quoted token in query", code="INVALID_INPUT")
                tokens.append(text[start:i])
                i += 1
                continue
            start = i
            while i < len(text) and (not text[i].isspace()) and text[i] not in "()&|!":
                i += 1
            tokens.append(text[start:i])
        return tokens

    def current(self) -> str | None:
        if self.index >= len(self.tokens):
            return None
        return self.tokens[self.index]

    def consume(self, expected: str | None = None) -> str:
        token = self.current()
        if token is None:
            raise BewleyError("unexpected end of query", code="INVALID_INPUT")
        if expected is not None and token != expected:
            raise BewleyError(f"expected {expected!r}, got {token!r}", code="INVALID_INPUT")
        self.index += 1
        return token

    def parse(self) -> BoolExpr:
        expr = self.parse_or()
        if self.current() is not None:
            raise BewleyError(f"unexpected token {self.current()!r}", code="INVALID_INPUT")
        return expr

    def parse_or(self) -> BoolExpr:
        left = self.parse_and()
        while (token := self.current()) and token.upper() == "OR":
            self.consume()
            left = BinOp(left=left, right=self.parse_and(), kind="OR")
        return left

    def parse_and(self) -> BoolExpr:
        left = self.parse_not()
        while (token := self.current()) and token.upper() == "AND":
            self.consume()
            left = BinOp(left=left, right=self.parse_not(), kind="AND")
        return left

    def parse_not(self) -> BoolExpr:
        token = self.current()
        if token and token.upper() == "NOT":
            self.consume()
            return Not(self.parse_not())
        return self.parse_primary()

    def parse_primary(self) -> BoolExpr:
        token = self.current()
        if token == "(":
            self.consume("(")
            expr = self.parse_or()
            self.consume(")")
            return expr
        if token is None:
            raise BewleyError("unexpected end of query", code="INVALID_INPUT")
        self.consume()
        return Term(token)
