"""
Data Classification Action - Classifies data into categories.

This module provides data classification capabilities including
rule-based classification, pattern matching, and multi-label categorization.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar
from enum import Enum


T = TypeVar("T")


class ClassificationMethod(Enum):
    """Method used for classification."""
    RULE_BASED = "rule_based"
    PATTERN = "pattern"
    KEYWORD = "keyword"
    CUSTOM = "custom"


@dataclass
class Category:
    """A classification category."""
    category_id: str
    name: str
    description: str = ""
    parent_id: str | None = None
    priority: int = 0


@dataclass
class ClassificationRule:
    """A rule for classifying data."""
    rule_id: str
    category_id: str
    method: ClassificationMethod
    conditions: list[dict[str, Any]] = field(default_factory=list)
    priority: int = 0
    confidence: float = 1.0


@dataclass
class ClassificationResult:
    """Result of data classification."""
    record_id: Any
    categories: list[str]
    confidence: float
    matched_rules: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class BatchClassificationResult:
    """Results of batch classification."""
    total: int
    classified: int
    unclassified: int
    results: list[ClassificationResult]
    category_counts: dict[str, int] = field(default_factory=dict)


class RuleClassifier:
    """Rule-based data classifier."""
    
    def __init__(self) -> None:
        self._rules: list[ClassificationRule] = []
        self._categories: dict[str, Category] = {}
    
    def add_category(
        self,
        category_id: str,
        name: str,
        description: str = "",
        parent_id: str | None = None,
    ) -> None:
        """Add a classification category."""
        self._categories[category_id] = Category(
            category_id=category_id,
            name=name,
            description=description,
            parent_id=parent_id,
        )
    
    def add_rule(
        self,
        rule_id: str,
        category_id: str,
        method: ClassificationMethod,
        conditions: list[dict[str, Any]],
        priority: int = 0,
        confidence: float = 1.0,
    ) -> None:
        """Add a classification rule."""
        rule = ClassificationRule(
            rule_id=rule_id,
            category_id=category_id,
            method=method,
            conditions=conditions,
            priority=priority,
            confidence=confidence,
        )
        self._rules.append(rule)
        self._rules.sort(key=lambda r: r.priority, reverse=True)
    
    def classify(self, record: dict[str, Any]) -> ClassificationResult:
        """Classify a single record."""
        matched_categories: list[str] = []
        matched_rules: list[str] = []
        total_confidence = 0.0
        match_count = 0
        
        for rule in self._rules:
            if self._evaluate_rule(rule, record):
                matched_categories.append(rule.category_id)
                matched_rules.append(rule.rule_id)
                total_confidence += rule.confidence
                match_count += 1
        
        if not matched_categories:
            return ClassificationResult(
                record_id=record.get("id", id(record)),
                categories=[],
                confidence=0.0,
            )
        
        avg_confidence = total_confidence / match_count
        
        return ClassificationResult(
            record_id=record.get("id", id(record)),
            categories=matched_categories,
            confidence=avg_confidence,
            matched_rules=matched_rules,
        )
    
    def _evaluate_rule(self, rule: ClassificationRule, record: dict[str, Any]) -> bool:
        """Evaluate if a rule matches a record."""
        if rule.method == ClassificationMethod.RULE_BASED:
            return self._evaluate_conditions(rule.conditions, record)
        elif rule.method == ClassificationMethod.PATTERN:
            return self._evaluate_patterns(rule.conditions, record)
        elif rule.method == ClassificationMethod.KEYWORD:
            return self._evaluate_keywords(rule.conditions, record)
        elif rule.method == ClassificationMethod.CUSTOM:
            return self._evaluate_conditions(rule.conditions, record)
        return False
    
    def _evaluate_conditions(
        self,
        conditions: list[dict[str, Any]],
        record: dict[str, Any],
    ) -> bool:
        """Evaluate conditions against a record."""
        if not conditions:
            return True
        
        for condition in conditions:
            field_name = condition.get("field")
            operator = condition.get("operator", "eq")
            value = condition.get("value")
            
            field_value = self._get_nested(record, field_name)
            
            if operator == "eq":
                if field_value != value:
                    return False
            elif operator == "ne":
                if field_value == value:
                    return False
            elif operator == "gt":
                if not (field_value is not None and field_value > value):
                    return False
            elif operator == "lt":
                if not (field_value is not None and field_value < value):
                    return False
            elif operator == "gte":
                if not (field_value is not None and field_value >= value):
                    return False
            elif operator == "lte":
                if not (field_value is not None and field_value <= value):
                    return False
            elif operator == "contains":
                if value not in str(field_value):
                    return False
            elif operator == "in":
                if field_value not in value:
                    return False
        
        return True
    
    def _evaluate_patterns(
        self,
        conditions: list[dict[str, Any]],
        record: dict[str, Any],
    ) -> bool:
        """Evaluate regex patterns against a record."""
        for condition in conditions:
            field_name = condition.get("field")
            pattern = condition.get("pattern")
            
            if not field_name or not pattern:
                continue
            
            field_value = self._get_nested(record, field_name)
            if field_value is None:
                return False
            
            if not re.search(pattern, str(field_value)):
                return False
        
        return True
    
    def _evaluate_keywords(
        self,
        conditions: list[dict[str, Any]],
        record: dict[str, Any],
    ) -> bool:
        """Evaluate keyword presence in a record."""
        for condition in conditions:
            field_name = condition.get("field")
            keywords = condition.get("keywords", [])
            match_all = condition.get("match_all", False)
            
            field_value = self._get_nested(record, field_name)
            if field_value is None:
                return False
            
            text = str(field_value).lower()
            
            if match_all:
                if not all(kw.lower() in text for kw in keywords):
                    return False
            else:
                if not any(kw.lower() in text for kw in keywords):
                    return False
        
        return True
    
    def _get_nested(self, data: dict[str, Any], path: str) -> Any:
        """Get nested value from dict using dot notation."""
        keys = path.split(".")
        current = data
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key)
            else:
                return None
        return current


class DataClassificationAction:
    """
    Data classification action for automation workflows.
    
    Example:
        action = DataClassificationAction()
        action.add_category("spam", "Spam", "Unwanted messages")
        action.add_rule(
            "spam_keywords",
            "spam",
            ClassificationMethod.KEYWORD,
            [{"field": "content", "keywords": ["buy", "free", "winner"]}]
        )
        result = await action.classify_records(records)
    """
    
    def __init__(self) -> None:
        self.classifier = RuleClassifier()
    
    def add_category(
        self,
        category_id: str,
        name: str,
        description: str = "",
    ) -> None:
        """Add a classification category."""
        self.classifier.add_category(category_id, name, description)
    
    def add_rule(
        self,
        rule_id: str,
        category_id: str,
        method: ClassificationMethod,
        conditions: list[dict[str, Any]],
        priority: int = 0,
    ) -> None:
        """Add a classification rule."""
        self.classifier.add_rule(
            rule_id, category_id, method, conditions, priority
        )
    
    async def classify_records(
        self,
        records: list[dict[str, Any]],
    ) -> BatchClassificationResult:
        """Classify multiple records."""
        results = []
        category_counts: dict[str, int] = {}
        classified = 0
        unclassified = 0
        
        for record in records:
            result = self.classifier.classify(record)
            results.append(result)
            
            if result.categories:
                classified += 1
                for cat in result.categories:
                    category_counts[cat] = category_counts.get(cat, 0) + 1
            else:
                unclassified += 1
        
        return BatchClassificationResult(
            total=len(records),
            classified=classified,
            unclassified=unclassified,
            results=results,
            category_counts=category_counts,
        )
    
    def classify_single(
        self,
        record: dict[str, Any],
    ) -> ClassificationResult:
        """Classify a single record."""
        return self.classifier.classify(record)


# Export public API
__all__ = [
    "ClassificationMethod",
    "Category",
    "ClassificationRule",
    "ClassificationResult",
    "BatchClassificationResult",
    "RuleClassifier",
    "DataClassificationAction",
]
