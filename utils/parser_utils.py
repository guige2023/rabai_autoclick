"""
Parser combinator and text parsing utilities.

Provides regex parsers, tokenizer, JSON path query,
CSV parsing utilities, and simple parser combinators.
"""

from __future__ import annotations

import re
import csv
import io
from typing import Any, Callable, Optional


class Token:
    """Token for tokenization."""
    def __init__(self, type: str, value: str, pos: int = 0):
        self.type = type
        self.value = value
        self.pos = pos

    def __repr__(self) -> str:
        return f"Token({self.type!r}, {self.value!r})"


class Lexer:
    """Simple lexer/tokenizer."""

    def __init__(self, rules: list[tuple[str, str]]):
        """
        Args:
            rules: List of (token_type, pattern) tuples
        """
        self.rules = sorted(rules, key=lambda x: -len(x[1]))
        self.pattern = re.compile('|'.join(f"(?P<{t}>{p})" for t, p in self.rules))

    def tokenize(self, text: str) -> list[Token]:
        tokens: list[Token] = []
        pos = 0
        for match in self.pattern.finditer(text):
            for name, value in match.groupdict().items():
                if value is not None:
                    if name != "SKIP":
                        tokens.append(Token(name, value, pos))
                    pos = match.end()
                    break
        return tokens


class Parser:
    """Simple recursive descent parser."""

    def __init__(self):
        self._tokens: list[Token] = []
        self._pos = 0

    def parse(self, tokens: list[Token]) -> Any:
        self._tokens = tokens
        self._pos = 0
        return self._parse_expr()

    def _peek(self) -> Token | None:
        return self._tokens[self._pos] if self._pos < len(self._tokens) else None

    def _consume(self, expected_type: str | None = None) -> Token:
        token = self._peek()
        if token is None:
            raise Exception("Unexpected end of input")
        if expected_type and token.type != expected_type:
            raise Exception(f"Expected {expected_type}, got {token.type}")
        self._pos += 1
        return token

    def _parse_expr(self) -> Any:
        return self._parse_add()

    def _parse_add(self) -> Any:
        left = self._parse_mul()
        while True:
            token = self._peek()
            if token and token.type == "PLUS":
                self._consume("PLUS")
                right = self._parse_mul()
                left = left + right
            elif token and token.type == "MINUS":
                self._consume("MINUS")
                right = self._parse_mul()
                left = left - right
            else:
                break
        return left

    def _parse_mul(self) -> Any:
        left = self._parse_unary()
        while True:
            token = self._peek()
            if token and token.type == "MUL":
                self._consume("MUL")
                right = self._parse_unary()
                left = left * right
            elif token and token.type == "DIV":
                self._consume("DIV")
                right = self._parse_unary()
                left = left / right
            else:
                break
        return left

    def _parse_unary(self) -> Any:
        token = self._peek()
        if token and token.type == "MINUS":
            self._consume("MINUS")
            return -self._parse_primary()
        return self._parse_primary()

    def _parse_primary(self) -> Any:
        token = self._consume()
        if token.type == "NUMBER":
            return float(token.value)
        elif token.type == "LPAREN":
            result = self._parse_expr()
            self._consume("RPAREN")
            return result
        raise Exception(f"Unexpected token: {token.type}")


def tokenize_python_code(code: str) -> list[Token]:
    """Tokenize Python source code (simplified)."""
    rules = [
        ("NUMBER", r"\d+\.?\d*"),
        ("STRING", r"\"[^\"]*\"|'[^']*'"),
        ("NAME", r"[a-zA-Z_][a-zA-Z0-9_]*"),
        ("PLUS", r"\+"),
        ("MINUS", r"-"),
        ("MUL", r"\*"),
        ("DIV", r"/"),
        ("LPAREN", r"\("),
        ("RPAREN", r"\)"),
        ("WS", r"\s+"),
    ]
    lexer = Lexer(rules)
    return lexer.tokenize(code)


def json_path_query(obj: Any, path: str) -> list[Any]:
    """
    Simple JSONPath-like query.

    Supports:
        $.key - child
        $[i] - array index
        ..key - recursive descent
        [*] - wildcard

    Args:
        obj: JSON object
        path: JSONPath expression

    Returns:
        List of matching values.
    """
    results: list[Any] = []
    if not path:
        return [obj]

    parts = re.split(r'\.(?![^\[]*\])', path)
    current = [obj]

    for part in parts:
        next_current: list[Any] = []
        if part == "*":
            for item in current:
                if isinstance(item, list):
                    next_current.extend(item)
                elif isinstance(item, dict):
                    next_current.extend(item.values())
        elif part.startswith("[") and part.endswith("]"):
            # Array index or wildcard
            if part == "[*]":
                for item in current:
                    if isinstance(item, list):
                        next_current.extend(item)
            else:
                indices = re.findall(r'\[(\d+)\]', part)
                for item in current:
                    if isinstance(item, list):
                        for idx in indices:
                            idx = int(idx)
                            if 0 <= idx < len(item):
                                next_current.append(item[idx])
        elif part.startswith(".."):
            # Recursive descent
            key = part[2:]
            next_current = _recursive_search(current, key)
        else:
            # Child
            for item in current:
                if isinstance(item, dict) and key in item:
                    next_current.append(item[key])
        current = next_current

    return current


def _recursive_search(items: list, key: str) -> list:
    results: list = []
    for item in items:
        if isinstance(item, dict):
            if key in item:
                results.append(item[key])
            results.extend(_recursive_search(list(item.values()), key))
        elif isinstance(item, list):
            results.extend(_recursive_search(item, key))
    return results


def parse_csv(
    text: str,
    delimiter: str = ",",
    quotechar: str = '"',
    skip_header: bool = True,
) -> tuple[list[str], list[list[str]]]:
    """
    Parse CSV text.

    Args:
        text: CSV content
        delimiter: Field delimiter
        quotechar: Quote character
        skip_header: If True, first row is header

    Returns:
        Tuple of (headers, rows).
    """
    reader = csv.reader(io.StringIO(text), delimiter=delimiter, quotechar=quotechar)
    rows = list(reader)
    if not rows:
        return [], []
    if skip_header:
        headers = rows[0]
        data = rows[1:]
    else:
        headers = [f"col{i}" for i in range(len(rows[0]))]
        data = rows
    return headers, data


def parse_tsv(text: str, skip_header: bool = True) -> tuple[list[str], list[list[str]]]:
    """Parse TSV (tab-separated values)."""
    return parse_csv(text, delimiter="\t", skip_header=skip_header)


def parse_query_string(qs: str) -> dict[str, str]:
    """
    Parse URL query string.

    Args:
        qs: Query string (e.g., "a=1&b=2")

    Returns:
        Dictionary of parameters.
    """
    result: dict[str, str] = {}
    if not qs:
        return result
    for pair in qs.split("&"):
        if "=" in pair:
            key, value = pair.split("=", 1)
            result[key] = value
    return result


def build_query_string(params: dict[str, Any]) -> str:
    """Build URL query string."""
    from urllib.parse import urlencode
    return urlencode(params)


def regex_extract_all(pattern: str, text: str, group: int = 0) -> list[str]:
    """Extract all regex matches."""
    return [m.group(group) for m in re.finditer(pattern, text)]


def regex_replace(
    pattern: str,
    text: str,
    replacement: str | Callable[[re.Match], str],
) -> str:
    """Replace regex matches."""
    return re.sub(pattern, replacement, text)


def split_sentences(text: str) -> list[str]:
    """Split text into sentences."""
    sentence_end = r'[.!?]+[\s]+'
    sentences = re.split(sentence_end, text)
    return [s.strip() for s in sentences if s.strip()]


def split_words(text: str) -> list[str]:
    """Split text into words."""
    return re.findall(r'\b\w+\b', text.lower())


def word_count(text: str) -> dict[str, int]:
    """Count word frequencies."""
    words = split_words(text)
    counts: dict[str, int] = {}
    for w in words:
        counts[w] = counts.get(w, 0) + 1
    return counts


def parse_ini_file(text: str) -> dict[str, dict[str, str]]:
    """
    Parse INI file format.

    Returns:
        Nested dict of {section: {key: value}}.
    """
    result: dict[str, dict[str, str]] = {}
    current_section = ""
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith(";"):
            continue
        if line.startswith("[") and line.endswith("]"):
            current_section = line[1:-1]
            result[current_section] = {}
        elif "=" in line:
            key, value = line.split("=", 1)
            section = result.get(current_section, {})
            section[key.strip()] = value.strip()
            result[current_section] = section
    return result
