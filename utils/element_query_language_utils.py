"""Element Query Language Utilities.

Provides a mini query language for finding UI elements.

Example:
    >>> from element_query_language_utils import ElementQuery
    >>> query = ElementQuery()
    >>> results = query.execute('button[name="OK"]', elements)
    >>> results = query.execute('input[type="text"][enabled=true]', elements)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Callable, List, Optional


class TokenType(Enum):
    """Query token types."""
    TAG = auto()
    ATTR = auto()
    VALUE = auto()
    OPERATOR = auto()
    LBRACKET = auto()
    RBRACKET = auto()
    EOF = auto()


@dataclass
class Token:
    """A query token."""
    type: TokenType
    value: str


class QueryLexer:
    """Lexes element query strings."""

    ATTR_PATTERN = re.compile(r'\[([^\]]+)\]')
    TAG_PATTERN = re.compile(r'^([a-zA-Z][a-zA-Z0-9_-]*)')

    def tokenize(self, query: str) -> List[Token]:
        """Tokenize a query string.

        Args:
            query: Query string.

        Returns:
            List of tokens.
        """
        tokens = []
        tag_match = self.TAG_PATTERN.match(query)
        if tag_match:
            tokens.append(Token(TokenType.TAG, tag_match.group(1)))
            query = query[tag_match.end():]

        for match in self.ATTR_PATTERN.finditer(query):
            attr_str = match.group(1)
            if "=" in attr_str:
                attr, val = attr_str.split("=", 1)
                tokens.append(Token(TokenType.ATTR, attr.strip()))
                tokens.append(Token(TokenType.OPERATOR, "="))
                tokens.append(Token(TokenType.VALUE, val.strip().strip('"')))
            else:
                tokens.append(Token(TokenType.ATTR, attr_str.strip()))

        return tokens


class ElementQuery:
    """Query engine for UI elements."""

    def __init__(self):
        """Initialize query engine."""
        self._lexer = QueryLexer()
        self._elements: List[Any] = []

    def execute(self, query: str, elements: List[Any]) -> List[Any]:
        """Execute a query against elements.

        Args:
            query: Query string.
            elements: Elements to search.

        Returns:
            Matching elements.
        """
        tokens = self._lexer.tokenize(query)
        tag = None
        attrs = {}

        for i, token in enumerate(tokens):
            if token.type == TokenType.TAG:
                tag = token.value.lower()
            elif token.type == TokenType.ATTR:
                op = "exists"
                val = True
                if i + 2 < len(tokens):
                    if tokens[i + 1].type == TokenType.OPERATOR:
                        op = tokens[i + 1].value
                        val = tokens[i + 2].value
                        if val.lower() == "true":
                            val = True
                        elif val.lower() == "false":
                            val = False
                attrs[token.value] = (op, val)

        return self._filter_elements(elements, tag, attrs)

    def _filter_elements(
        self, elements: List[Any], tag: Optional[str], attrs: dict
    ) -> List[Any]:
        """Filter elements by tag and attributes."""
        results = []
        for elem in elements:
            elem_tag = (getattr(elem, "tag", "") or getattr(elem, "role", "")).lower()
            if tag and elem_tag != tag:
                continue

            match = True
            for attr_name, (op, expected) in attrs.items():
                actual = getattr(elem, attr_name, None)
                if actual is None:
                    actual = self._get_attr(elem, attr_name)
                if actual is None:
                    match = False
                    break
                if op == "=" and str(actual) != str(expected):
                    match = False
                    break
                elif op == "!=" and str(actual) == str(expected):
                    match = False
                    break
                elif op == "exists" and not actual:
                    match = False
                    break
            if match:
                results.append(elem)
        return results

    def _get_attr(self, elem: Any, name: str) -> Any:
        """Get element attribute safely."""
        if hasattr(elem, "properties"):
            props = elem.properties
            if isinstance(props, dict):
                return props.get(name)
        return None
