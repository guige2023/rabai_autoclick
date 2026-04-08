"""Data Quality action module for RabAI AutoClick.

Provides data quality operations:
- DataQualityCheckAction: Check data quality rules
- DataProfilingAction: Profile data statistics
- DataValidationAction: Validate data schema
- DataCleansingAction: Cleanse data issues
"""

from __future__ import annotations

import sys
import os
import re
from typing import Any, Dict, List, Optional
from collections import Counter

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataQualityCheckAction(BaseAction):
    """Check data quality rules."""
    action_type = "data_quality_check"
    display_name = "数据质量检查"
    description = "检查数据质量规则"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute quality check."""
        data = params.get('data', [])
        rules = params.get('rules', [])
        output_var = params.get('output_var', 'quality_result')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_rules = context.resolve_value(rules) if context else rules

            results = []
            passed = 0
            failed = 0

            for rule in resolved_rules:
                rule_name = rule.get('name', 'unnamed')
                rule_type = rule.get('type', 'completeness')
                field = rule.get('field', '')

                if rule_type == 'completeness':
                    total = len(resolved_data)
                    non_null = sum(1 for r in resolved_data if r.get(field) is not None and r.get(field) != '')
                    rate = non_null / total if total > 0 else 0
                    rule_passed = rate >= rule.get('threshold', 0.95)
                    results.append({
                        'rule': rule_name,
                        'type': rule_type,
                        'passed': rule_passed,
                        'value': rate,
                        'threshold': rule.get('threshold', 0.95),
                    })

                elif rule_type == 'uniqueness':
                    values = [r.get(field) for r in resolved_data if r.get(field) is not None]
                    unique_count = len(set(values))
                    total = len(values)
                    rate = unique_count / total if total > 0 else 0
                    rule_passed = rate >= rule.get('threshold', 0.95)
                    results.append({
                        'rule': rule_name,
                        'type': rule_type,
                        'passed': rule_passed,
                        'value': rate,
                        'threshold': rule.get('threshold', 0.95),
                    })

                elif rule_type == 'validity':
                    pattern = rule.get('pattern', '')
                    valid_count = 0
                    for r in resolved_data:
                        val = r.get(field, '')
                        if val and re.match(pattern, str(val)):
                            valid_count += 1
                    total = len([r for r in resolved_data if r.get(field) is not None])
                    rate = valid_count / total if total > 0 else 0
                    rule_passed = rate >= rule.get('threshold', 0.95)
                    results.append({
                        'rule': rule_name,
                        'type': rule_type,
                        'passed': rule_passed,
                        'value': rate,
                        'threshold': rule.get('threshold', 0.95),
                    })

                if results[-1]['passed']:
                    passed += 1
                else:
                    failed += 1

            result = {
                'results': results,
                'total_rules': len(results),
                'passed': passed,
                'failed': failed,
                'pass_rate': passed / len(results) if results else 0,
            }

            return ActionResult(
                success=failed == 0,
                data={output_var: result},
                message=f"Quality check: {passed}/{len(results)} rules passed"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Quality check error: {e}")


class DataProfilingAction(BaseAction):
    """Profile data statistics."""
    action_type = "data_profiling"
    display_name = "数据统计"
    description = "数据统计分析"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute data profiling."""
        data = params.get('data', [])
        fields = params.get('fields', [])
        output_var = params.get('output_var', 'profile_result')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_fields = context.resolve_value(fields) if context else fields

            if not resolved_fields:
                if resolved_data:
                    resolved_fields = list(resolved_data[0].keys())

            profile = {
                'record_count': len(resolved_data),
                'field_count': len(resolved_fields),
                'fields': {},
            }

            for field in resolved_fields:
                values = [r.get(field) for r in resolved_data if r.get(field) is not None]
                numeric_values = [v for v in values if isinstance(v, (int, float))]

                field_profile = {
                    'count': len(values),
                    'null_count': len(resolved_data) - len(values),
                    'unique_count': len(set(values)),
                }

                if numeric_values:
                    field_profile.update({
                        'min': min(numeric_values),
                        'max': max(numeric_values),
                        'mean': sum(numeric_values) / len(numeric_values),
                        'type': 'numeric',
                    })
                else:
                    field_profile['type'] = 'string'
                    if values:
                        most_common = Counter(values).most_common(1)[0]
                        field_profile['most_common'] = {'value': most_common[0], 'count': most_common[1]}

                profile['fields'][field] = field_profile

            return ActionResult(
                success=True,
                data={output_var: profile},
                message=f"Profiled {len(resolved_fields)} fields"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data profiling error: {e}")


class DataValidationAction(BaseAction):
    """Validate data schema."""
    action_type = "data_validation"
    display_name = "数据验证"
    description = "验证数据schema"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute data validation."""
        data = params.get('data', {})
        schema = params.get('schema', {})
        strict = params.get('strict', False)
        output_var = params.get('output_var', 'validation_result')

        if not data or not schema:
            return ActionResult(success=False, message="data and schema are required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_schema = context.resolve_value(schema) if context else schema

            errors = []

            for field, field_schema in resolved_schema.items():
                field_type = field_schema.get('type', 'string')
                required = field_schema.get('required', False)

                if required and field not in resolved_data:
                    errors.append(f"Missing required field: {field}")
                    continue

                if field in resolved_data:
                    value = resolved_data[field]

                    if value is None:
                        continue

                    expected_types = field_type.split('|')

                    def check_type(v, t):
                        if t == 'string':
                            return isinstance(v, str)
                        elif t == 'integer':
                            return isinstance(v, int) and not isinstance(v, bool)
                        elif t == 'number':
                            return isinstance(v, (int, float)) and not isinstance(v, bool)
                        elif t == 'boolean':
                            return isinstance(v, bool)
                        elif t == 'array':
                            return isinstance(v, list)
                        elif t == 'object':
                            return isinstance(v, dict)
                        return True

                    if not any(check_type(value, t) for t in expected_types):
                        errors.append(f"Field '{field}' has invalid type. Expected {field_type}, got {type(value).__name__}")

                    if 'min' in field_schema and isinstance(value, (int, float)):
                        if value < field_schema['min']:
                            errors.append(f"Field '{field}' below minimum: {value} < {field_schema['min']}")

                    if 'max' in field_schema and isinstance(value, (int, float)):
                        if value > field_schema['max']:
                            errors.append(f"Field '{field}' above maximum: {value} > {field_schema['max']}")

                    if 'pattern' in field_schema and isinstance(value, str):
                        if not re.match(field_schema['pattern'], value):
                            errors.append(f"Field '{field}' does not match pattern: {field_schema['pattern']}")

                    if 'enum' in field_schema:
                        if value not in field_schema['enum']:
                            errors.append(f"Field '{field}' not in enum: {field_schema['enum']}")

            if strict:
                extra_fields = set(resolved_data.keys()) - set(resolved_schema.keys())
                for field in extra_fields:
                    errors.append(f"Unknown field in strict mode: {field}")

            result = {
                'valid': len(errors) == 0,
                'errors': errors,
                'error_count': len(errors),
            }

            return ActionResult(
                success=len(errors) == 0,
                data={output_var: result},
                message=f"Validation {'passed' if not errors else f'failed with {len(errors)} errors'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data validation error: {e}")


class DataCleansingAction(BaseAction):
    """Cleanse data issues."""
    action_type = "data_cleansing"
    display_name = "数据清洗"
    description = "清洗数据问题"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute data cleansing."""
        data = params.get('data', [])
        operations = params.get('operations', [])
        output_var = params.get('output_var', 'cleansed_data')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_ops = context.resolve_value(operations) if context else operations

            cleansed = resolved_data
            changes = []

            for op in resolved_ops:
                operation = op.get('operation', '')
                field = op.get('field', '')

                if operation == 'trim':
                    for record in cleansed:
                        if field in record and isinstance(record[field], str):
                            record[field] = record[field].strip()
                    changes.append(f"Trimmed {field}")

                elif operation == 'uppercase':
                    for record in cleansed:
                        if field in record and isinstance(record[field], str):
                            record[field] = record[field].upper()
                    changes.append(f"Uppercased {field}")

                elif operation == 'lowercase':
                    for record in cleansed:
                        if field in record and isinstance(record[field], str):
                            record[field] = record[field].lower()
                    changes.append(f"Lowercased {field}")

                elif operation == 'remove_nulls':
                    original_count = len(cleansed)
                    cleansed = [r for r in cleansed if r.get(field) is not None and r.get(field) != '']
                    removed = original_count - len(cleansed)
                    changes.append(f"Removed {removed} nulls from {field}")

                elif operation == 'fill_default':
                    default = op.get('default', '')
                    for record in cleansed:
                        if field not in record or record[field] is None or record[field] == '':
                            record[field] = default
                    changes.append(f"Filled default for {field}")

                elif operation == 'remove_duplicates':
                    seen = set()
                    original_count = len(cleansed)
                    cleansed = []
                    for record in cleansed:
                        key = record.get(field, '')
                        if key not in seen:
                            seen.add(key)
                            cleansed.append(record)
                    removed = original_count - len(cleansed)
                    changes.append(f"Removed {removed} duplicates based on {field}")

            result = {
                'data': cleansed,
                'record_count': len(cleansed),
                'changes': changes,
                'operations_applied': len(resolved_ops),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Cleansed: {len(changes)} operations applied"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data cleansing error: {e}")
