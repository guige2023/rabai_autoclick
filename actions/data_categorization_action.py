"""Data Categorization Action Module.

Categorizes and tags data records based on configurable rules,
keyword matching, and pattern recognition.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
import re
import logging

logger = logging.getLogger(__name__)


@dataclass
class CategoryRule:
    """A rule for categorizing data."""
    name: str
    tags: List[str]
    priority: int = 0
    enabled: bool = True


@dataclass
class CategoryMatch:
    """Result of categorizing a record."""
    record_id: Any
    matched_rules: List[str]
    tags: Set[str]
    confidence: float


class DataCategorizationAction:
    """Categorizes and tags records based on rules and patterns.
    
    Supports keyword matching, regex patterns, and custom
    classifier functions for multi-label categorization.
    """

    def __init__(self) -> None:
        self._rules: List[CategoryRule] = []
        self._keyword_index: Dict[str, List[CategoryRule]] = {}
        self._regex_index: List[Tuple[re.Pattern[str], CategoryRule]] = []
        self._classifier_fn: Optional[Callable[..., List[str]]] = None
        self._stats: Dict[str, int] = {"total": 0, "matched": 0}

    def add_rule(
        self,
        name: str,
        tags: List[str],
        keywords: Optional[List[str]] = None,
        pattern: Optional[str] = None,
        priority: int = 0,
    ) -> None:
        """Add a categorization rule.
        
        Args:
            name: Unique rule name.
            tags: Tags to assign when rule matches.
            keywords: List of keywords that trigger this rule.
            pattern: Optional regex pattern (with .search semantics).
            priority: Higher priority rules are evaluated first.
        """
        rule = CategoryRule(name=name, tags=tags, priority=priority)
        self._rules.append(rule)
        self._rules.sort(key=lambda r: -r.priority)

        if keywords:
            for kw in keywords:
                lower = kw.lower()
                if lower not in self._keyword_index:
                    self._keyword_index[lower] = []
                self._keyword_index[lower].append(rule)

        if pattern:
            compiled = re.compile(pattern, re.IGNORECASE)
            self._regex_index.append((compiled, rule))

    def set_classifier(self, fn: Callable[..., List[str]]) -> None:
        """Set a custom ML/classifier function.
        
        Args:
            fn: Function that takes a record and returns list of tag names.
        """
        self._classifier_fn = fn

    def categorize(
        self,
        record: Dict[str, Any],
        record_id: Optional[Any] = None,
    ) -> CategoryMatch:
        """Categorize a single record.
        
        Args:
            record: Record dictionary to categorize.
            record_id: Optional record identifier for tracking.
        
        Returns:
            CategoryMatch with matched rules and tags.
        """
        self._stats["total"] += 1
        matched_rules: List[str] = []
        tags: Set[str] = set()
        record_text = self._record_text(record)

        # Keyword matching
        for kw, rules in self._keyword_index.items():
            if kw in record_text.lower():
                for rule in rules:
                    if rule.enabled and rule.name not in matched_rules:
                        matched_rules.append(rule.name)
                        tags.update(rule.tags)

        # Regex matching
        for pattern, rule in self._regex_index:
            if rule.enabled and rule.name not in matched_rules:
                if pattern.search(record_text):
                    matched_rules.append(rule.name)
                    tags.update(rule.tags)

        # Custom classifier
        if self._classifier_fn:
            try:
                extra_tags = self._classifier_fn(record)
                if extra_tags:
                    tags.update(extra_tags)
            except Exception as exc:  # pragma: no cover
                logger.warning("Classifier function error: %s", exc)

        confidence = min(len(matched_rules) / 3.0, 1.0) if matched_rules else 0.0
        if matched_rules:
            self._stats["matched"] += 1

        return CategoryMatch(
            record_id=record_id,
            matched_rules=matched_rules,
            tags=tags,
            confidence=confidence,
        )

    def categorize_batch(
        self,
        records: List[Dict[str, Any]],
        id_fn: Optional[Callable[[Dict[str, Any]], Any]] = None,
    ) -> List[CategoryMatch]:
        """Categorize multiple records.
        
        Args:
            records: List of record dictionaries.
            id_fn: Optional function to extract record ID.
        
        Returns:
            List of CategoryMatch in input order.
        """
        results: List[CategoryMatch] = []
        for i, record in enumerate(records):
            rid = id_fn(record) if id_fn else i
            results.append(self.categorize(record, record_id=rid))
        return results

    def _record_text(self, record: Dict[str, Any]) -> str:
        """Flatten a record into searchable text."""
        parts: List[str] = []
        for val in record.values():
            if isinstance(val, str):
                parts.append(val)
            elif val is not None:
                parts.append(str(val))
        return " ".join(parts)

    def get_stats(self) -> Dict[str, Any]:
        """Get categorization statistics."""
        total = self._stats["total"]
        matched = self._stats["matched"]
        return {
            "total": total,
            "matched": matched,
            "match_rate": round(matched / total, 4) if total > 0 else 0.0,
            "rule_count": len(self._rules),
        }
