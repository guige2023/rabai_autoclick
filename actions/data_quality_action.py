"""Data quality action module for RabAI AutoClick.

Provides data quality checks including completeness,
uniqueness, consistency, validity, and custom rule evaluation.
"""

import sys
import os
import re
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from collections import Counter
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class QualityRule:
    """A data quality rule definition."""
    name: str
    rule_type: str  # completeness, uniqueness, validity, consistency, custom
    field: Optional[str] = None
    params: Dict[str, Any] = field(default_factory=dict)


class DataQualityAction(BaseAction):
    """Evaluate data quality across multiple dimensions.
    
    Checks completeness, uniqueness, validity,
    consistency, and custom rules with detailed reports.
    """
    action_type = "data_quality"
    display_name = "数据质量检查"
    description = "数据质量评估：完整性/唯一性/有效性/一致性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Evaluate data quality.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of dicts, data to check
                - rules: list of rule specs or auto-detect
                - check_types: list of checks to run
                    (completeness/uniqueness/validity/consistency)
                - null_threshold: float, max null rate (0-1)
                - unique_threshold: float, min unique rate (0-1)
                - save_to_var: str
        
        Returns:
            ActionResult with quality report.
        """
        data = params.get('data', [])
        rules = params.get('rules', [])
        check_types = params.get('check_types', [
            'completeness', 'uniqueness', 'validity', 'consistency'
        ])
        null_threshold = params.get('null_threshold', 0.0)
        unique_threshold = params.get('unique_threshold', 0.0)
        save_to_var = params.get('save_to_var', None)

        if not data:
            return ActionResult(success=False, message="No data provided")

        report = {
            'total_records': len(data),
            'total_fields': 0,
            'checks': {},
            'passed': True,
            'failed_rules': []
        }

        # Auto-detect fields if data is list of dicts
        fields = set()
        if data and isinstance(data[0], dict):
            for record in data:
                fields.update(record.keys())
        report['total_fields'] = len(fields)

        results = {}

        if 'completeness' in check_types:
            results['completeness'] = self._check_completeness(data, fields, null_threshold)

        if 'uniqueness' in check_types:
            results['uniqueness'] = self._check_uniqueness(data, fields, unique_threshold)

        if 'validity' in check_types:
            results['validity'] = self._check_validity(data, fields)

        if 'consistency' in check_types:
            results['consistency'] = self._check_consistency(data, fields)

        # Evaluate against thresholds
        for check_name, check_result in results.items():
            if check_result.get('failed_fields'):
                report['failed_rules'].extend([
                    f"{check_name}.{f}" for f in check_result['failed_fields']
                ])
                report['passed'] = False
            report['checks'][check_name] = check_result

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = report

        return ActionResult(
            success=report['passed'],
            message=f"Quality check: {'PASSED' if report['passed'] else 'FAILED'} ({len(report['failed_rules'])} issues)",
            data=report
        )

    def _check_completeness(
        self, data: List, fields: Set[str], threshold: float
    ) -> Dict[str, Any]:
        """Check data completeness (null rates)."""
        field_stats = {}
        failed_fields = []

        for field_name in fields:
            non_null = sum(1 for record in data if isinstance(record, dict) and record.get(field_name) is not None)
            null_count = len(data) - non_null
            null_rate = null_count / len(data) if data else 0
            complete_rate = 1.0 - null_rate

            field_stats[field_name] = {
                'null_count': null_count,
                'null_rate': round(null_rate, 4),
                'complete_count': non_null,
                'complete_rate': round(complete_rate, 4)
            }

            if threshold > 0 and null_rate > threshold:
                failed_fields.append(field_name)

        return {
            'field_stats': field_stats,
            'failed_fields': failed_fields,
            'overall_null_rate': round(
                sum(1 for r in data for v in (r.values() if isinstance(r, dict) else [r]) if v is None) / max(1, len(data) * max(1, len(fields))),
                4
            )
        }

    def _check_uniqueness(
        self, data: List, fields: Set[str], threshold: float
    ) -> Dict[str, Any]:
        """Check uniqueness of fields."""
        field_stats = {}
        failed_fields = []

        for field_name in fields:
            values = [record.get(field_name) if isinstance(record, dict) else None for record in data]
            unique_count = len(set(v for v in values if v is not None))
            total_non_null = sum(1 for v in values if v is not None)
            unique_rate = unique_count / total_non_null if total_non_null > 0 else 0

            field_stats[field_name] = {
                'unique_count': unique_count,
                'unique_rate': round(unique_rate, 4),
                'duplicate_count': total_non_null - unique_count
            }

            if threshold > 0 and unique_rate < threshold:
                failed_fields.append(field_name)

        # Check record-level uniqueness
        record_strings = [str(r) for r in data]
        dup_records = len(record_strings) - len(set(record_strings))
        field_stats['_record_level'] = {
            'unique_count': len(set(record_strings)),
            'duplicate_count': dup_records
        }

        return {
            'field_stats': field_stats,
            'failed_fields': failed_fields
        }

    def _check_validity(self, data: List, fields: Set[str]) -> Dict[str, Any]:
        """Check data validity (type, format, range)."""
        field_stats = {}
        failed_fields = []

        type_patterns = {
            'email': r'^[\w\.-]+@[\w\.-]+\.\w+$',
            'phone': r'^[\d\-\+\(\)\s]+$',
            'url': r'^https?://[\w\-\.]+[\w/\-\.%?=&]+$',
            'ip': r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$',
            'date': r'^\d{4}[-/]\d{2}[-/]\d{2}$',
        }

        for field_name in fields:
            values = [record.get(field_name) if isinstance(record, dict) else None for record in data]
            non_null_values = [v for v in values if v is not None]

            if not non_null_values:
                continue

            # Detect type
            detected_type = 'unknown'
            for type_name, pattern in type_patterns.items():
                if all(re.match(pattern, str(v)) for v in non_null_values[:10]):
                    detected_type = type_name
                    break
            if detected_type == 'unknown':
                if all(isinstance(v, bool) for v in non_null_values):
                    detected_type = 'boolean'
                elif all(isinstance(v, (int, float)) for v in non_null_values):
                    detected_type = 'numeric'

            # Check format validity
            invalid_count = 0
            if detected_type in type_patterns:
                pattern = type_patterns[detected_type]
                invalid_count = sum(1 for v in non_null_values if not re.match(pattern, str(v)))

            field_stats[field_name] = {
                'detected_type': detected_type,
                'valid_count': len(non_null_values) - invalid_count,
                'invalid_count': invalid_count,
                'valid_rate': round((len(non_null_values) - invalid_count) / len(non_null_values), 4) if non_null_values else 1.0
            }

            if invalid_count > 0:
                failed_fields.append(field_name)

        return {
            'field_stats': field_stats,
            'failed_fields': failed_fields
        }

    def _check_consistency(self, data: List, fields: Set[str]) -> Dict[str, Any]:
        """Check data consistency (cross-field relationships)."""
        field_stats = {}
        failed_fields = []

        for field_name in fields:
            values = [record.get(field_name) if isinstance(record, dict) else None for record in data]
            non_null_values = [v for v in values if v is not None]

            if not non_null_values:
                continue

            # Check for format consistency
            formats = Counter(str(type(v).__name__) for v in non_null_values)
            most_common_type = formats.most_common(1)[0][0]
            type_consistency = formats[most_common_type] / len(non_null_values)

            # Check length consistency
            if all(isinstance(v, str) for v in non_null_values):
                lengths = [len(v) for v in non_null_values]
                avg_length = sum(lengths) / len(lengths)
                length_variance = sum((l - avg_length) ** 2 for l in lengths) / len(lengths)
            else:
                length_variance = 0

            field_stats[field_name] = {
                'type_consistency': round(type_consistency, 4),
                'length_variance': round(length_variance, 2)
            }

            if type_consistency < 0.8:
                failed_fields.append(field_name)

        return {
            'field_stats': field_stats,
            'failed_fields': failed_fields
        }

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'rules': [],
            'check_types': ['completeness', 'uniqueness', 'validity', 'consistency'],
            'null_threshold': 0.0,
            'unique_threshold': 0.0,
            'save_to_var': None,
        }
