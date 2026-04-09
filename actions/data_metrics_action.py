"""Data Metrics Action.

Computes and tracks data quality metrics including completeness,
consistency, validity, and timeliness across data pipelines.
"""
from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple


class MetricCategory(Enum):
    """Categories of data quality metrics."""
    COMPLETENESS = "completeness"
    CONSISTENCY = "consistency"
    VALIDITY = "validity"
    TIMELINESS = "timeliness"
    UNIQUENESS = "uniqueness"
    INTEGRITY = "integrity"


@dataclass
class MetricValue:
    """A single metric value."""
    name: str
    category: MetricCategory
    value: float
    threshold: Optional[float] = None
    passed: bool = True
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=datetime.now().timestamp)


@dataclass
class DataMetricsReport:
    """Comprehensive data quality metrics report."""
    dataset_name: str
    total_records: int
    total_fields: int
    metrics: List[MetricValue] = field(default_factory=list)
    overall_score: float = 0.0
    passed_checks: int = 0
    failed_checks: int = 0
    generated_at: datetime = field(default_factory=datetime.now)


class DataMetricsAction:
    """Computes and tracks data quality metrics."""

    def __init__(self, dataset_name: str = "dataset") -> None:
        self.dataset_name = dataset_name
        self._metric_history: Dict[str, List[MetricValue]] = defaultdict(list)
        self._max_history = 1000

    def compute_completeness(
        self,
        data: List[Dict[str, Any]],
    ) -> List[MetricValue]:
        """Compute completeness metrics."""
        metrics = []

        if not data:
            return metrics

        fields = set()
        for record in data:
            fields.update(record.keys())

        total_records = len(data)
        total_cells = total_records * len(fields)
        null_cells = 0

        for record in data:
            for field_name in fields:
                if record.get(field_name) is None or record.get(field_name) == "":
                    null_cells += 1

        completeness_ratio = (total_cells - null_cells) / total_cells if total_cells > 0 else 0

        metrics.append(MetricValue(
            name="overall_completeness",
            category=MetricCategory.COMPLETENESS,
            value=completeness_ratio,
            threshold=0.95,
            passed=completeness_ratio >= 0.95,
            details={"total_records": total_records, "total_cells": total_cells, "null_cells": null_cells},
        ))

        for field_name in fields:
            null_count = sum(1 for r in data if r.get(field_name) is None or r.get(field_name) == "")
            field_completeness = (total_records - null_count) / total_records if total_records > 0 else 0

            metrics.append(MetricValue(
                name=f"field_completeness_{field_name}",
                category=MetricCategory.COMPLETENESS,
                value=field_completeness,
                details={"field": field_name, "null_count": null_count, "total": total_records},
            ))

        return metrics

    def compute_uniqueness(
        self,
        data: List[Dict[str, Any]],
        key_fields: Optional[List[str]] = None,
    ) -> List[MetricValue]:
        """Compute uniqueness metrics."""
        metrics = []

        if not data:
            return metrics

        if key_fields:
            keys = [tuple(r.get(f) for f in key_fields) for r in data]
            unique_keys = len(set(keys))
            uniqueness_ratio = unique_keys / len(data) if data else 0

            metrics.append(MetricValue(
                name="key_uniqueness",
                category=MetricCategory.UNIQUENESS,
                value=uniqueness_ratio,
                threshold=1.0,
                passed=uniqueness_ratio >= 1.0,
                details={"key_fields": key_fields, "unique_count": unique_keys, "total": len(data)},
            ))

        all_fields = set()
        for record in data:
            all_fields.update(record.keys())

        for field_name in all_fields:
            values = [r.get(field_name) for r in data if field_name in r]
            unique_values = len(set(values))
            uniqueness_ratio = unique_values / len(values) if values else 0

            metrics.append(MetricValue(
                name=f"field_uniqueness_{field_name}",
                category=MetricCategory.UNIQUENESS,
                value=uniqueness_ratio,
                details={"field": field_name, "unique_values": unique_values, "total": len(values)},
            ))

        return metrics

    def compute_validity(
        self,
        data: List[Dict[str, Any]],
        rules: Optional[Dict[str, Callable]] = None,
    ) -> List[MetricValue]:
        """Compute validity metrics based on rules."""
        metrics = []

        if not data:
            return metrics

        rules = rules or {}

        for field_name, rule_fn in rules.items():
            valid_count = 0
            invalid_examples = []

            for record in data:
                if field_name not in record:
                    continue
                value = record[field_name]
                try:
                    if rule_fn(value):
                        valid_count += 1
                    else:
                        if len(invalid_examples) < 5:
                            invalid_examples.append(value)
                except Exception:
                    pass

            total_with_field = sum(1 for r in data if field_name in r)
            validity_ratio = valid_count / total_with_field if total_with_field > 0 else 0

            metrics.append(MetricValue(
                name=f"field_validity_{field_name}",
                category=MetricCategory.VALIDITY,
                value=validity_ratio,
                threshold=0.99,
                passed=validity_ratio >= 0.99,
                details={
                    "field": field_name,
                    "valid_count": valid_count,
                    "invalid_count": total_with_field - valid_count,
                    "examples": invalid_examples,
                },
            ))

        return metrics

    def compute_consistency(
        self,
        data: List[Dict[str, Any]],
        field_pairs: Optional[List[Tuple[str, str]]] = None,
    ) -> List[MetricValue]:
        """Compute consistency metrics between field pairs."""
        metrics = []

        if not data or not field_pairs:
            return metrics

        for field_a, field_b in field_pairs:
            consistent_count = 0
            total = 0

            for record in data:
                if field_a in record and field_b in record:
                    val_a = record[field_a]
                    val_b = record[field_b]
                    total += 1
                    if val_a == val_b:
                        consistent_count += 1

            if total > 0:
                consistency_ratio = consistent_count / total
                metrics.append(MetricValue(
                    name=f"consistency_{field_a}_vs_{field_b}",
                    category=MetricCategory.CONSISTENCY,
                    value=consistency_ratio,
                    threshold=0.95,
                    passed=consistency_ratio >= 0.95,
                    details={"field_a": field_a, "field_b": field_b, "consistent": consistent_count, "total": total},
                ))

        return metrics

    def compute_timeliness(
        self,
        data: List[Dict[str, Any]],
        timestamp_field: str = "updated_at",
        max_age_seconds: float = 86400,
    ) -> List[MetricValue]:
        """Compute timeliness metrics."""
        metrics = []
        now = datetime.now().timestamp()

        if not data:
            return metrics

        timestamps = [r.get(timestamp_field) for r in data if timestamp_field in r]
        if not timestamps:
            return metrics

        timestamps = [t for t in timestamps if isinstance(t, (int, float))]

        if timestamps:
            latest = max(timestamps)
            age_seconds = now - latest

            freshness_ratio = max(0, 1 - (age_seconds / max_age_seconds))

            metrics.append(MetricValue(
                name="data_timeliness",
                category=MetricCategory.TIMELINESS,
                value=freshness_ratio,
                threshold=0.8,
                passed=freshness_ratio >= 0.8,
                details={
                    "latest_timestamp": latest,
                    "age_seconds": age_seconds,
                    "max_age_seconds": max_age_seconds,
                },
            ))

        return metrics

    def compute_integrity(
        self,
        data: List[Dict[str, Any]],
        referential_rules: Optional[List[Dict[str, Any]]] = None,
    ) -> List[MetricValue]:
        """Compute referential integrity metrics."""
        metrics = []

        if not data:
            return metrics

        referential_rules = referential_rules or []

        for rule in referential_rules:
            source_field = rule.get("source_field")
            foreign_key = rule.get("foreign_key")
            parent_values = rule.get("parent_values", set())

            if not source_field or not foreign_key or not parent_values:
                continue

            valid_count = 0
            invalid_count = 0
            invalid_examples = []

            for record in data:
                if source_field in record:
                    value = record[source_field]
                    if value in parent_values:
                        valid_count += 1
                    else:
                        invalid_count += 1
                        if len(invalid_examples) < 5:
                            invalid_examples.append(value)

            total = valid_count + invalid_count
            integrity_ratio = valid_count / total if total > 0 else 0

            metrics.append(MetricValue(
                name=f"integrity_{source_field}",
                category=MetricCategory.INTEGRITY,
                value=integrity_ratio,
                threshold=1.0,
                passed=integrity_ratio >= 1.0,
                details={
                    "source_field": source_field,
                    "foreign_key": foreign_key,
                    "valid_count": valid_count,
                    "invalid_count": invalid_count,
                    "examples": invalid_examples,
                },
            ))

        return metrics

    def compute_all(
        self,
        data: List[Dict[str, Any]],
        dataset_name: Optional[str] = None,
        rules: Optional[Dict[str, Callable]] = None,
        key_fields: Optional[List[str]] = None,
        timestamp_field: Optional[str] = None,
    ) -> DataMetricsReport:
        """Compute all data quality metrics."""
        all_metrics: List[MetricValue] = []

        all_metrics.extend(self.compute_completeness(data))
        all_metrics.extend(self.compute_uniqueness(data, key_fields))
        all_metrics.extend(self.compute_validity(data, rules))

        passed = sum(1 for m in all_metrics if m.passed)
        failed = len(all_metrics) - passed
        overall_score = passed / len(all_metrics) if all_metrics else 0

        for metric in all_metrics:
            metric_name = f"{metric.category.value}_{metric.name}"
            self._record_metric(metric_name, metric)

        return DataMetricsReport(
            dataset_name=dataset_name or self.dataset_name,
            total_records=len(data),
            total_fields=len(set().union(*[set(r.keys()) for r in data])) if data else 0,
            metrics=all_metrics,
            overall_score=overall_score,
            passed_checks=passed,
            failed_checks=failed,
        )

    def _record_metric(self, name: str, metric: MetricValue) -> None:
        """Record a metric in history."""
        if name not in self._metric_history:
            self._metric_history[name] = []

        self._metric_history[name].append(metric)

        if len(self._metric_history[name]) > self._max_history:
            self._metric_history[name] = self._metric_history[name][-self._max_history // 2:]

    def get_metric_trend(
        self,
        metric_name: str,
        limit: int = 30,
    ) -> List[MetricValue]:
        """Get historical trend for a metric."""
        return self._metric_history.get(metric_name, [])[-limit:]

    def get_metric_dashboard(self) -> Dict[str, Any]:
        """Get a summary dashboard of all metrics."""
        latest_metrics: Dict[str, float] = {}
        threshold_breaches: List[str] = []

        for metric_name, history in self._metric_history.items():
            if history:
                latest = history[-1]
                latest_metrics[metric_name] = latest.value
                if not latest.passed:
                    threshold_breaches.append(metric_name)

        return {
            "latest_metrics": latest_metrics,
            "threshold_breaches": threshold_breaches,
            "total_metrics_tracked": len(self._metric_history),
            "categories": {
                cat.value: len([m for name, history in self._metric_history.items()
                               for m in history if m.category.value == cat.value])
                for cat in MetricCategory
            },
        }
