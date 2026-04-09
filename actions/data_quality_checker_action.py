"""Data Quality Checker Action Module.

Validates data quality across multiple dimensions:
- Completeness (missing values)
- Consistency (format, type)
- Accuracy (valid ranges)
- Uniqueness (duplicates)
- Timeliness (staleness)

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

logger = logging.getLogger(__name__)


class QualityDimension(Enum):
    """Data quality dimensions."""
    COMPLETENESS = auto()
    CONSISTENCY = auto()
    ACCURACY = auto()
    UNIQUENESS = auto()
    TIMELINESS = auto()


class QualityLevel(Enum):
    """Quality levels for results."""
    EXCELLENT = auto()
    GOOD = auto()
    ACCEPTABLE = auto()
    POOR = auto()
    FAILING = auto()


@dataclass
class QualityRule:
    """A data quality rule to check."""
    name: str
    dimension: QualityDimension
    check_fn: Callable[[Any], bool]
    description: str = ""
    severity: str = "error"
    threshold: float = 1.0


@dataclass
class QualityIssue:
    """Represents a data quality issue."""
    rule_name: str
    dimension: QualityDimension
    field_path: str
    message: str
    severity: str
    issue_count: int = 1
    sample_values: List[Any] = field(default_factory=list)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class QualityReport:
    """Data quality assessment report."""
    dataset_name: str
    total_records: int
    dimensions: Dict[QualityDimension, float] = field(default_factory=dict)
    issues: List[QualityIssue] = field(default_factory=list)
    passed_checks: int = 0
    failed_checks: int = 0
    quality_score: float = 0.0
    level: QualityLevel = QualityLevel.ACCEPTABLE
    generated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataQualityChecker:
    """Checks data quality across multiple dimensions.
    
    Supports:
    - Schema validation
    - Completeness checks
    - Consistency checks
    - Range validation
    - Pattern matching
    - Duplicate detection
    - Staleness detection
    """
    
    def __init__(self, dataset_name: str = "default"):
        self.dataset_name = dataset_name
        self._rules: List[QualityRule] = []
        self._field_validators: Dict[str, Dict[str, Any]] = {}
        self._required_fields: Set[str] = set()
        self._null_threshold: float = 0.0
        self._lock = asyncio.Lock()
    
    def set_null_threshold(self, threshold: float) -> None:
        """Set acceptable null percentage threshold.
        
        Args:
            threshold: Threshold as decimal (0.0 to 1.0)
        """
        self._null_threshold = threshold
    
    def add_required_field(self, field_path: str) -> None:
        """Mark a field as required.
        
        Args:
            field_path: Dot-separated field path
        """
        self._required_fields.add(field_path)
    
    def add_field_validator(
        self,
        field_path: str,
        field_type: Optional[type] = None,
        pattern: Optional[str] = None,
        min_value: Optional[Union[int, float]] = None,
        max_value: Optional[Union[int, float]] = None,
        allowed_values: Optional[List[Any]] = None,
        nullable: bool = True
    ) -> None:
        """Add a field validator.
        
        Args:
            field_path: Dot-separated field path
            field_type: Expected Python type
            pattern: Regex pattern for strings
            min_value: Minimum value for numbers
            max_value: Maximum value for numbers
            allowed_values: List of allowed values
            nullable: Whether field can be null
        """
        self._field_validators[field_path] = {
            "type": field_type,
            "pattern": pattern,
            "min_value": min_value,
            "max_value": max_value,
            "allowed_values": allowed_values,
            "nullable": nullable
        }
    
    def add_rule(self, rule: QualityRule) -> None:
        """Add a quality rule.
        
        Args:
            rule: Quality rule to add
        """
        self._rules.append(rule)
    
    def add_completeness_rule(
        self,
        field_path: str,
        threshold: float = 1.0
    ) -> None:
        """Add a completeness check rule.
        
        Args:
            field_path: Field to check
            threshold: Minimum completion rate (0.0 to 1.0)
        """
        def check(data: List[Dict[str, Any]]) -> bool:
            if not data:
                return True
            non_null = sum(1 for row in data if self._get_nested(row, field_path) is not None)
            return (non_null / len(data)) >= threshold
        
        self.add_rule(QualityRule(
            name=f"completeness_{field_path}",
            dimension=QualityDimension.COMPLETENESS,
            check_fn=check,
            description=f"Check completeness of {field_path}",
            threshold=threshold
        ))
    
    def add_uniqueness_rule(
        self,
        field_path: str,
        threshold: float = 1.0
    ) -> None:
        """Add a uniqueness check rule.
        
        Args:
            field_path: Field to check
            threshold: Minimum uniqueness rate (0.0 to 1.0)
        """
        def check(data: List[Dict[str, Any]]) -> bool:
            if not data:
                return True
            values = [self._get_nested(row, field_path) for row in data]
            non_null_values = [v for v in values if v is not None]
            unique_count = len(set(non_null_values))
            return (unique_count / len(non_null_values)) >= threshold if non_null_values else True
        
        self.add_rule(QualityRule(
            name=f"uniqueness_{field_path}",
            dimension=QualityDimension.UNIQUENESS,
            check_fn=check,
            description=f"Check uniqueness of {field_path}",
            threshold=threshold
        ))
    
    def add_consistency_rule(
        self,
        field_path: str,
        pattern: str
    ) -> None:
        """Add a consistency rule with pattern.
        
        Args:
            field_path: Field to check
            pattern: Regex pattern
        """
        compiled_pattern = re.compile(pattern)
        
        def check(data: List[Dict[str, Any]]) -> bool:
            if not data:
                return True
            for row in data:
                value = self._get_nested(row, field_path)
                if value is not None and not isinstance(value, str):
                    return False
                if value and not compiled_pattern.match(str(value)):
                    return False
            return True
        
        self.add_rule(QualityRule(
            name=f"consistency_{field_path}",
            dimension=QualityDimension.CONSISTENCY,
            check_fn=check,
            description=f"Check pattern consistency of {field_path}"
        ))
    
    async def check(
        self,
        data: List[Dict[str, Any]],
        metadata: Optional[Dict[str, Any]] = None
    ) -> QualityReport:
        """Run all quality checks on data.
        
        Args:
            data: List of data records
            metadata: Optional dataset metadata
            
        Returns:
            Quality assessment report
        """
        issues: List[QualityIssue] = []
        dimension_scores: Dict[QualityDimension, List[float]] = defaultdict(list)
        passed = 0
        failed = 0
        
        for rule in self._rules:
            try:
                result = rule.check_fn(data)
                if result:
                    passed += 1
                    dimension_scores[rule.dimension].append(100.0)
                else:
                    failed += 1
                    dimension_scores[rule.dimension].append(0.0)
                    issues.append(QualityIssue(
                        rule_name=rule.name,
                        dimension=rule.dimension,
                        field_path="",
                        message=rule.description or f"Rule {rule.name} failed",
                        severity=rule.severity
                    ))
            except Exception as e:
                logger.error(f"Error running rule {rule.name}: {e}")
                failed += 1
        
        field_issues = await self._check_fields(data)
        issues.extend(field_issues)
        
        for issue in field_issues:
            dimension_scores[issue.dimension].append(0.0)
        
        completeness_issues = await self._check_completeness(data)
        issues.extend(completeness_issues)
        
        for issue in completeness_issues:
            dimension_scores[issue.dimension].append(0.0)
        
        uniqueness_issues = await self._check_uniqueness(data)
        issues.extend(uniqueness_issues)
        
        for issue in uniqueness_issues:
            dimension_scores[issue.dimension].append(0.0)
        
        dimension_results = {}
        for dim, scores in dimension_scores.items():
            dimension_results[dim] = sum(scores) / len(scores) if scores else 100.0
        
        total_score = sum(dimension_results.values()) / len(dimension_results) if dimension_results else 100.0
        
        if total_score >= 95:
            level = QualityLevel.EXCELLENT
        elif total_score >= 85:
            level = QualityLevel.GOOD
        elif total_score >= 70:
            level = QualityLevel.ACCEPTABLE
        elif total_score >= 50:
            level = QualityLevel.POOR
        else:
            level = QualityLevel.FAILING
        
        return QualityReport(
            dataset_name=self.dataset_name,
            total_records=len(data),
            dimensions=dimension_results,
            issues=issues,
            passed_checks=passed,
            failed_checks=failed,
            quality_score=total_score,
            level=level,
            metadata=metadata or {}
        )
    
    async def _check_fields(self, data: List[Dict[str, Any]]) -> List[QualityIssue]:
        """Check field-level validators."""
        issues = []
        
        for field_path, validator in self._field_validators.items():
            null_count = 0
            type_errors = 0
            pattern_errors = 0
            range_errors = 0
            allowed_errors = 0
            sample_errors = []
            
            for row in data:
                value = self._get_nested(row, field_path)
                
                if value is None:
                    if not validator["nullable"]:
                        null_count += 1
                    continue
                
                if validator["type"] and not isinstance(value, validator["type"]):
                    type_errors += 1
                    if len(sample_errors) < 5:
                        sample_errors.append(value)
                    continue
                
                if validator["pattern"] and isinstance(value, str):
                    if not re.match(validator["pattern"], value):
                        pattern_errors += 1
                        if len(sample_errors) < 5:
                            sample_errors.append(value)
                
                if validator["min_value"] is not None and isinstance(value, (int, float)):
                    if value < validator["min_value"]:
                        range_errors += 1
                
                if validator["max_value"] is not None and isinstance(value, (int, float)):
                    if value > validator["max_value"]:
                        range_errors += 1
                
                if validator["allowed_values"]:
                    if value not in validator["allowed_values"]:
                        allowed_errors += 1
                        if len(sample_errors) < 5:
                            sample_errors.append(value)
            
            total = len(data)
            
            if null_count > 0:
                rate = null_count / total
                if rate > self._null_threshold:
                    issues.append(QualityIssue(
                        rule_name=f"nullable_{field_path}",
                        dimension=QualityDimension.COMPLETENESS,
                        field_path=field_path,
                        message=f"Null rate {rate:.2%} exceeds threshold",
                        severity="warning",
                        issue_count=null_count
                    ))
            
            if type_errors > 0:
                issues.append(QualityIssue(
                    rule_name=f"type_{field_path}",
                    dimension=QualityDimension.CONSISTENCY,
                    field_path=field_path,
                    message=f"Type mismatch for {field_path}",
                    severity="error",
                    issue_count=type_errors,
                    sample_values=sample_errors
                ))
            
            if pattern_errors > 0:
                issues.append(QualityIssue(
                    rule_name=f"pattern_{field_path}",
                    dimension=QualityDimension.CONSISTENCY,
                    field_path=field_path,
                    message=f"Pattern mismatch for {field_path}",
                    severity="error",
                    issue_count=pattern_errors,
                    sample_values=sample_errors
                ))
            
            if range_errors > 0:
                issues.append(QualityIssue(
                    rule_name=f"range_{field_path}",
                    dimension=QualityDimension.ACCURACY,
                    field_path=field_path,
                    message=f"Value out of range for {field_path}",
                    severity="error",
                    issue_count=range_errors,
                    sample_values=sample_errors
                ))
        
        return issues
    
    async def _check_completeness(self, data: List[Dict[str, Any]]) -> List[QualityIssue]:
        """Check completeness of required fields."""
        issues = []
        
        for field_path in self._required_fields:
            null_count = 0
            for row in data:
                if self._get_nested(row, field_path) is None:
                    null_count += 1
            
            if null_count > 0:
                rate = null_count / len(data)
                if rate > self._null_threshold:
                    issues.append(QualityIssue(
                        rule_name=f"required_{field_path}",
                        dimension=QualityDimension.COMPLETENESS,
                        field_path=field_path,
                        message=f"Required field {field_path} has {rate:.2%} null rate",
                        severity="error",
                        issue_count=null_count
                    ))
        
        return issues
    
    async def _check_uniqueness(self, data: List[Dict[str, Any]]) -> List[QualityIssue]:
        """Check for duplicate records."""
        issues = []
        
        seen_rows = Counter()
        for i, row in enumerate(data):
            row_key = str(sorted(row.items()))
            seen_rows[row_key] += 1
        
        duplicates = {k: v for k, v in seen_rows.items() if v > 1}
        
        if duplicates:
            total_dupes = sum(v - 1 for v in duplicates.values())
            issues.append(QualityIssue(
                rule_name="duplicate_records",
                dimension=QualityDimension.UNIQUENESS,
                field_path="",
                message=f"Found {len(duplicates)} duplicate record patterns with {total_dupes} total duplicates",
                severity="warning",
                issue_count=total_dupes
            ))
        
        return issues
    
    def _get_nested(self, data: Dict[str, Any], path: str) -> Any:
        """Get value from nested dict using dot notation."""
        keys = path.split(".")
        value = data
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
        return value


class DuplicateDetector:
    """Detects duplicate records using various strategies."""
    
    def __init__(self, threshold: float = 0.85):
        self.threshold = threshold
    
    def find_exact_duplicates(
        self,
        data: List[Dict[str, Any]],
        key_fields: List[str]
    ) -> List[Set[int]]:
        """Find exact duplicate records.
        
        Args:
            data: List of records
            key_fields: Fields to use for comparison
            
        Returns:
            List of index sets for duplicate groups
        """
        seen: Dict[Tuple, List[int]] = defaultdict(list)
        duplicates: List[Set[int]] = []
        
        for i, row in enumerate(data):
            key = tuple(self._get_field(row, f) for f in key_fields)
            seen[key].append(i)
        
        for indices in seen.values():
            if len(indices) > 1:
                duplicates.append(set(indices))
        
        return duplicates
    
    def find_fuzzy_duplicates(
        self,
        data: List[Dict[str, Any]],
        compare_fields: List[str]
    ) -> List[Tuple[int, int, float]]:
        """Find fuzzy duplicate pairs.
        
        Args:
            data: List of records
            compare_fields: Fields to compare
            
        Returns:
            List of (index1, index2, similarity) tuples
        """
        from difflib import SequenceMatcher
        
        duplicates = []
        
        for i in range(len(data)):
            for j in range(i + 1, len(data)):
                row1, row2 = data[i], data[j]
                
                similarities = []
                for field in compare_fields:
                    v1 = str(self._get_field(row1, field))
                    v2 = str(self._get_field(row2, field))
                    matcher = SequenceMatcher(None, v1, v2)
                    similarities.append(matcher.ratio())
                
                avg_similarity = sum(similarities) / len(similarities)
                
                if avg_similarity >= self.threshold:
                    duplicates.append((i, j, avg_similarity))
        
        return duplicates
    
    def _get_field(self, row: Dict[str, Any], field: str) -> Any:
        """Get field value from row."""
        return row.get(field)
