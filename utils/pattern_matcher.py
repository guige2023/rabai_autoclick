"""
Pattern Matcher Utility

Matches UI elements by patterns (regex, wildcards, XPath).
Provides flexible element lookup beyond exact matching.

Example:
    >>> matcher = PatternMatcher()
    >>> results = matcher.find_by_pattern("button_*", elements, key="role")
    >>> print(results)
"""

from __future__ import annotations

import re
from typing import Any, Callable, Optional


class PatternMatcher:
    """
    Pattern-based element matching for automation.

    Supports:
        - Wildcard patterns (* and ?)
        - Regular expressions
        - XPath-style paths
        - Custom matcher functions
    """

    WILDCARD_SEP = "*"

    def __init__(self) -> None:
        self._custom_matchers: dict[str, Callable[[str, str], bool]] = {}

    def register_matcher(
        self,
        name: str,
        matcher: Callable[[str, str], bool],
    ) -> None:
        """
        Register a custom pattern matcher.

        Args:
            name: Matcher identifier.
            matcher: Function(pattern, value) -> bool.
        """
        self._custom_matchers[name] = matcher

    def match(
        self,
        pattern: str,
        value: str,
        matcher_type: str = "wildcard",
    ) -> bool:
        """
        Check if a value matches a pattern.

        Args:
            pattern: Pattern string.
            value: Value to match against.
            matcher_type: Type of matching ("wildcard", "regex", "exact", "contains").

        Returns:
            True if value matches pattern.
        """
        if matcher_type == "exact":
            return pattern == value

        if matcher_type == "contains":
            return pattern.lower() in value.lower()

        if matcher_type == "wildcard":
            return self._match_wildcard(pattern, value)

        if matcher_type == "regex":
            return self._match_regex(pattern, value)

        if matcher_type in self._custom_matchers:
            return self._custom_matchers[matcher_type](pattern, value)

        return False

    def _match_wildcard(self, pattern: str, value: str) -> bool:
        """
        Match using wildcard patterns.

        * matches any characters
        ? matches single character
        """
        # Convert wildcard pattern to regex
        regex_pattern = ""
        i = 0
        while i < len(pattern):
            c = pattern[i]
            if c == "*":
                regex_pattern += ".*"
            elif c == "?":
                regex_pattern += "."
            elif c == ".":
                regex_pattern += r"\."
            else:
                regex_pattern += re.escape(c)
            i += 1

        try:
            return bool(re.fullmatch(regex_pattern, value, re.IGNORECASE))
        except re.error:
            return False

    def _match_regex(self, pattern: str, value: str) -> bool:
        """Match using regular expression."""
        try:
            return bool(re.search(pattern, value, re.IGNORECASE))
        except re.error:
            return False

    def find_by_pattern(
        self,
        pattern: str,
        elements: list[dict],
        key: str = "name",
        matcher_type: str = "wildcard",
    ) -> list[dict]:
        """
        Find all elements matching a pattern.

        Args:
            pattern: Pattern to match.
            elements: List of element dicts.
            key: Element attribute to match against.
            matcher_type: Matching algorithm.

        Returns:
            List of matching elements.
        """
        results = []
        for element in elements:
            value = element.get(key, "")
            if isinstance(value, str) and self.match(pattern, value, matcher_type):
                results.append(element)
        return results

    def find_by_patterns(
        self,
        patterns: dict[str, str],
        elements: list[dict],
        match_all: bool = False,
    ) -> list[dict]:
        """
        Find elements matching multiple patterns.

        Args:
            patterns: Dict of key -> pattern.
            elements: List of element dicts.
            match_all: If True, all patterns must match. If False, any.

        Returns:
            List of matching elements.
        """
        results = []
        for element in elements:
            matches = []
            for key, pattern in patterns.items():
                value = element.get(key, "")
                if isinstance(value, str):
                    matches.append(self._match_wildcard(pattern, value))
                else:
                    matches.append(False)

            if match_all and all(matches):
                results.append(element)
            elif not match_all and any(matches):
                results.append(element)

        return results

    def filter_by_role_pattern(
        self,
        pattern: str,
        elements: list[dict],
    ) -> list[dict]:
        """Filter elements by role matching a wildcard pattern."""
        return self.find_by_pattern(pattern, elements, key="role", matcher_type="wildcard")

    def filter_by_name_pattern(
        self,
        pattern: str,
        elements: list[dict],
    ) -> list[dict]:
        """Filter elements by name matching a wildcard pattern."""
        return self.find_by_pattern(pattern, elements, key="name", matcher_type="wildcard")

    def xpath_select(
        self,
        elements: list[dict],
        xpath_expr: str,
    ) -> list[dict]:
        """
        Simple XPath-like selection on flat element lists.

        Supports:
            /role[.='name']  - role equals name
            /role[contains(.,'partial')]  - role contains text
            //*[@attribute='value']  - attribute equals value

        Args:
            elements: List of element dicts.
            xpath_expr: XPath-like expression.

        Returns:
            List of matching elements.
        """
        results = list(elements)

        # Simple path resolution
        if xpath_expr.startswith("//"):
            # Descendant-or-self
            expr = xpath_expr[2:]
        elif xpath_expr.startswith("/"):
            expr = xpath_expr[1:]
        else:
            expr = xpath_expr

        # Handle predicate [condition]
        predicate_match = re.search(r"\[(.*?)\]", expr)
        if predicate_match:
            predicate = predicate_match.group(1)
            expr = expr[:predicate_match.start()]

            # @attribute='value'
            attr_match = re.search(r"@(\w+)=['\"](.*?)['\"]", predicate)
            if attr_match:
                attr, val = attr_match.group(1), attr_match.group(2)
                results = [e for e in results if e.get(attr) == val]

            # contains(.,'text')
            contains_match = re.search(r"contains\(\.,['\"](.*?)['\"]\)", predicate)
            if contains_match:
                text = contains_match.group(1)
                results = [e for e in results if text.lower() in str(e.get("text", "").get("name", "")).lower()]

        # Role filter
        if expr and expr != "*":
            results = [e for e in results if e.get("role", "").lower() == expr.lower()]

        return results
