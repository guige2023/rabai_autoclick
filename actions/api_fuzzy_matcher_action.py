"""API Fuzzy Matcher Action Module.

Provides fuzzy matching capabilities for API request/response handling,
including string similarity, pattern matching, and approximate lookups.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class MatchStrategy(Enum):
    EXACT = "exact"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    FUZZY = "fuzzy"
    REGEX = "regex"


@dataclass
class MatchResult:
    matched: bool
    score: float
    matched_value: Optional[str] = None
    matched_index: Optional[int] = None
    matched_key: Optional[str] = None


@dataclass
class FuzzyMatcherConfig:
    threshold: float = 0.8
    case_sensitive: bool = False
    strategy: MatchStrategy = MatchStrategy.FUZZY
    max_results: int = 10
    normalize: bool = True


def normalize_string(s: str) -> str:
    return re.sub(r'\s+', ' ', s.lower().strip())


def fuzzy_score(s1: str, s2: str, normalize_str: bool = True) -> float:
    if normalize_str:
        s1, s2 = normalize_string(s1), normalize_string(s2)
    if not s1 or not s2:
        return 0.0
    return SequenceMatcher(None, s1, s2).ratio()


def match_value(
    value: str,
    pattern: str,
    config: FuzzyMatcherConfig,
) -> MatchResult:
    if config.strategy == MatchStrategy.EXACT:
        matched = (value == pattern) if config.case_sensitive else (value.lower() == pattern.lower())
        return MatchResult(matched=matched, score=1.0 if matched else 0.0, matched_value=value)

    elif config.strategy == MatchStrategy.CONTAINS:
        p = pattern if config.case_sensitive else pattern.lower()
        v = value if config.case_sensitive else value.lower()
        found = p in v
        return MatchResult(matched=found, score=1.0 if found else 0.0, matched_value=value)

    elif config.strategy == MatchStrategy.STARTS_WITH:
        p = pattern if config.case_sensitive else pattern.lower()
        v = value if config.case_sensitive else value.lower()
        found = v.startswith(p)
        return MatchResult(matched=found, score=1.0 if found else 0.0, matched_value=value)

    elif config.strategy == MatchStrategy.ENDS_WITH:
        p = pattern if config.case_sensitive else pattern.lower()
        v = value if config.case_sensitive else value.lower()
        found = v.endswith(p)
        return MatchResult(matched=found, score=1.0 if found else 0.0, matched_value=value)

    elif config.strategy == MatchStrategy.REGEX:
        flags = 0 if config.case_sensitive else re.IGNORECASE
        try:
            match = re.search(pattern, value, flags)
            return MatchResult(
                matched=bool(match),
                score=match.end() - match.start() / len(value) if match else 0.0,
                matched_value=match.group(0) if match else None,
            )
        except re.error as e:
            return MatchResult(matched=False, score=0.0, matched_value=None)

    elif config.strategy == MatchStrategy.FUZZY:
        score = fuzzy_score(value, pattern, config.normalize)
        return MatchResult(
            matched=score >= config.threshold,
            score=score,
            matched_value=value,
        )

    return MatchResult(matched=False, score=0.0)


def match_in_list(
    values: List[str],
    pattern: str,
    config: FuzzyMatcherConfig,
) -> List[MatchResult]:
    results = []
    for i, value in enumerate(values):
        result = match_value(value, pattern, config)
        result.matched_index = i
        results.append(result)

    results.sort(key=lambda r: r.score, reverse=True)
    return results[:config.max_results]


def match_in_dict(
    data: Dict[str, Any],
    pattern: str,
    config: FuzzyMatcherConfig,
    keys_only: bool = True,
) -> List[MatchResult]:
    results = []
    for key, val in data.items():
        result = match_value(str(key), pattern, config)
        result.matched_key = key
        results.append(result)

        if not keys_only and not isinstance(val, dict):
            result = match_value(str(val), pattern, config)
            result.matched_key = key
            results.append(result)

    results.sort(key=lambda r: r.score, reverse=True)
    return results[:config.max_results]


class FuzzyMatcher:
    def __init__(self, config: Optional[FuzzyMatcherConfig] = None):
        self.config = config or FuzzyMatcherConfig()
        self._rules: Dict[str, Callable[[str], bool]] = {}

    def add_rule(self, name: str, matcher: Callable[[str], bool]) -> None:
        self._rules[name] = matcher

    def match_value(self, value: str, pattern: str) -> MatchResult:
        return match_value(value, pattern, self.config)

    def match_list(self, values: List[str], pattern: str) -> List[MatchResult]:
        return match_in_list(values, pattern, self.config)

    def match_dict(self, data: Dict[str, Any], pattern: str) -> List[MatchResult]:
        return match_in_dict(data, pattern, self.config)

    def find_best_match(self, values: List[str], pattern: str) -> Optional[MatchResult]:
        results = self.match_list(values, pattern)
        return results[0] if results and results[0].matched else None


def fuzzy_search(
    items: List[str],
    query: str,
    threshold: float = 0.6,
) -> List[Tuple[str, float]]:
    scored = [(item, fuzzy_score(item, query)) for item in items]
    return [(item, score) for item, score in scored if score >= threshold]


def match_api_response(
    response: Dict[str, Any],
    expected: Dict[str, Any],
    strict: bool = False,
) -> Tuple[bool, List[str]]:
    differences = []

    for key, expected_val in expected.items():
        if key not in response:
            differences.append(f"Missing key: {key}")
            continue

        actual_val = response[key]
        if isinstance(expected_val, dict) and isinstance(actual_val, dict):
            matched, diffs = match_api_response(actual_val, expected_val, strict)
            differences.extend(diffs)
        elif expected_val != actual_val:
            if strict:
                differences.append(f"Key '{key}': expected {expected_val!r}, got {actual_val!r}")
            else:
                score = fuzzy_score(str(expected_val), str(actual_val))
                if score < 0.8:
                    differences.append(f"Key '{key}' mismatch: {expected_val!r} vs {actual_val!r}")

    return len(differences) == 0, differences
