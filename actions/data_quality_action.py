"""Data Quality Action Module.

Provides data quality assessment and validation capabilities
including completeness checks, consistency validation, and quality scoring.
"""

import sys
import os
import re
from typing import Any, Dict, List, Optional
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataQualityCheckerAction(BaseAction):
    """Check data quality across multiple dimensions.
    
    Supports completeness, consistency, accuracy, and timeliness checks.
    """
    action_type = "data_quality_checker"
    display_name = "数据质量检查"
    description = "跨多个维度检查数据质量"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Check data quality.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data list.
                - checks: List of quality checks to perform.
                - rules: Custom quality rules.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with quality check result or error.
        """
        data = params.get('data', [])
        checks = params.get('checks', ['completeness', 'consistency'])
        rules = params.get('rules', {})
        output_var = params.get('output_var', 'quality_result')

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list for data, got {type(data).__name__}"
            )

        try:
            results = {}

            if 'completeness' in checks:
                results['completeness'] = self._check_completeness(data)

            if 'consistency' in checks:
                results['consistency'] = self._check_consistency(data)

            if 'uniqueness' in checks:
                results['uniqueness'] = self._check_uniqueness(data)

            if 'validity' in checks:
                results['validity'] = self._check_validity(data, rules)

            # Calculate overall score
            scores = [r.get('score', 0) for r in results.values() if 'score' in r]
            overall_score = sum(scores) / len(scores) if scores else 0

            result = {
                'overall_score': overall_score,
                'checks': results,
                'record_count': len(data),
                'passed': overall_score >= 0.8
            }

            context.variables[output_var] = result
            return ActionResult(
                success=result['passed'],
                data=result,
                message=f"Data quality score: {overall_score:.2%}"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Data quality check failed: {str(e)}"
            )

    def _check_completeness(self, data: List) -> Dict:
        """Check data completeness (missing values)."""
        if not data:
            return {'score': 0, 'details': 'No data'}

        total_fields = 0
        missing_fields = 0

        for item in data:
            if isinstance(item, dict):
                for key, value in item.items():
                    total_fields += 1
                    if value is None or value == '':
                        missing_fields += 1

        completeness = 1 - (missing_fields / total_fields) if total_fields > 0 else 0

        return {
            'score': completeness,
            'total_fields': total_fields,
            'missing_fields': missing_fields,
            'complete_records': sum(1 for item in data if isinstance(item, dict) and all(v is not None and v != '' for v in item.values()))
        }

    def _check_consistency(self, data: List) -> Dict:
        """Check data consistency."""
        if not data:
            return {'score': 0, 'details': 'No data'}

        # Check for type consistency
        field_types = {}
        inconsistencies = 0

        for item in data:
            if isinstance(item, dict):
                for key, value in item.items():
                    if key not in field_types:
                        field_types[key] = type(value)
                    elif field_types[key] != type(value) and value is not None:
                        inconsistencies += 1

        total_values = sum(len(item) for item in data if isinstance(item, dict))
        consistency = 1 - (inconsistencies / total_values) if total_values > 0 else 0

        return {
            'score': consistency,
            'inconsistencies': inconsistencies,
            'field_types': {k: str(v.__name__) for k, v in field_types.items()}
        }

    def _check_uniqueness(self, data: List) -> Dict:
        """Check data uniqueness."""
        if not data:
            return {'score': 0, 'details': 'No data'}

        # Convert items to hashable form
        hashable = []
        for item in data:
            if isinstance(item, dict):
                hashable.append(json.dumps(item, sort_keys=True))
            else:
                hashable.append(str(item))

        total = len(hashable)
        unique = len(set(hashable))
        duplicates = total - unique

        uniqueness = unique / total if total > 0 else 0

        return {
            'score': uniqueness,
            'total_records': total,
            'unique_records': unique,
            'duplicates': duplicates
        }

    def _check_validity(self, data: List, rules: Dict) -> Dict:
        """Check data validity against rules."""
        if not rules:
            return {'score': 1.0, 'details': 'No rules defined'}

        valid_count = 0
        invalid_count = 0
        violations = []

        for item in data:
            if isinstance(item, dict):
                is_valid = True
                for field, rule in rules.items():
                    value = item.get(field)
                    if not self._validate_field(value, rule):
                        is_valid = False
                        violations.append({'field': field, 'value': value, 'rule': rule})

                if is_valid:
                    valid_count += 1
                else:
                    invalid_count += 1

        total = valid_count + invalid_count
        validity = valid_count / total if total > 0 else 0

        return {
            'score': validity,
            'valid_records': valid_count,
            'invalid_records': invalid_count,
            'violations': violations[:10]  # Limit to first 10
        }

    def _validate_field(self, value: Any, rule: Dict) -> bool:
        """Validate a single field against a rule."""
        rule_type = rule.get('type')

        if rule_type == 'range':
            min_val = rule.get('min')
            max_val = rule.get('max')
            if min_val is not None and value < min_val:
                return False
            if max_val is not None and value > max_val:
                return False

        elif rule_type == 'pattern':
            pattern = rule.get('pattern')
            if pattern and not re.match(pattern, str(value)):
                return False

        elif rule_type == 'enum':
            allowed = rule.get('values', [])
            if value not in allowed:
                return False

        elif rule_type == 'type':
            expected = rule.get('expected')
            if expected == 'string' and not isinstance(value, str):
                return False
            elif expected == 'number' and not isinstance(value, (int, float)):
                return False
            elif expected == 'boolean' and not isinstance(value, bool):
                return False

        return True


import json


class DataProfilerAction(BaseAction):
    """Profile data to generate statistics and insights.
    
    Supports column profiling, distribution analysis, and pattern detection.
    """
    action_type = "data_profiler"
    display_name = "数据剖析"
    description = "生成数据统计和分析"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Profile data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data list.
                - profile_fields: Fields to profile.
                - include_distributions: Include value distributions.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with profiling result or error.
        """
        data = params.get('data', [])
        profile_fields = params.get('profile_fields', [])
        include_distributions = params.get('include_distributions', True)
        output_var = params.get('output_var', 'profile_result')

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list for data, got {type(data).__name__}"
            )

        try:
            # Auto-detect fields if not specified
            if not profile_fields and data:
                if isinstance(data[0], dict):
                    profile_fields = list(data[0].keys())

            profiles = {}
            for field in profile_fields:
                profiles[field] = self._profile_field(data, field, include_distributions)

            result = {
                'profiles': profiles,
                'record_count': len(data),
                'field_count': len(profile_fields)
            }

            context.variables[output_var] = result
            return ActionResult(
                success=True,
                data=result,
                message=f"Profiled {len(profile_fields)} fields from {len(data)} records"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Data profiling failed: {str(e)}"
            )

    def _profile_field(
        self, data: List, field: str, include_distributions: bool
    ) -> Dict:
        """Profile a single field."""
        values = []
        for item in data:
            if isinstance(item, dict) and field in item:
                values.append(item[field])

        if not values:
            return {'type': 'empty'}

        # Determine type
        type_counts = Counter(type(v).__name__ for v in values)
        primary_type = type_counts.most_common(1)[0][0]

        profile = {
            'type': primary_type,
            'count': len(values),
            'null_count': sum(1 for v in values if v is None),
            'unique_count': len(set(str(v) for v in values))
        }

        # Numeric stats
        numeric_values = [v for v in values if isinstance(v, (int, float)) and v is not None]
        if numeric_values:
            profile['min'] = min(numeric_values)
            profile['max'] = max(numeric_values)
            profile['mean'] = sum(numeric_values) / len(numeric_values)

        # String stats
        string_values = [v for v in values if isinstance(v, str)]
        if string_values:
            profile['min_length'] = min(len(v) for v in string_values)
            profile['max_length'] = max(len(v) for v in string_values)
            profile['avg_length'] = sum(len(v) for v in string_values) / len(string_values)

        # Distribution
        if include_distributions:
            if len(set(str(v) for v in values)) <= 20:
                profile['distribution'] = dict(Counter(str(v) for v in values).most_common(10))

        return profile


class Data cleanserAction(BaseAction):
    """Cleanse data by fixing common quality issues.
    
    Supports missing value imputation, outlier handling, and standardization.
    """
    action_type = "data_cleanser"
    display_name = "数据清洗"
    description = "修复常见数据质量问题"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Cleanse data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data list.
                - operations: List of cleansing operations.
                - field: Field to cleanse.
                - strategy: Cleansing strategy.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with cleansing result or error.
        """
        data = params.get('data', [])
        operations = params.get('operations', ['trim_strings'])
        field = params.get('field', '')
        strategy = params.get('strategy', 'default')
        output_var = params.get('output_var', 'cleansed')

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list for data, got {type(data).__name__}"
            )

        try:
            cleansed = [dict(item) if isinstance(item, dict) else item for item in data]
            operations_log = []

            for operation in operations:
                if operation == 'trim_strings':
                    count = self._trim_strings(cleansed, field)
                    operations_log.append({'operation': 'trim_strings', 'affected': count})
                elif operation == 'remove_duplicates':
                    count = self._remove_duplicates(cleansed)
                    operations_log.append({'operation': 'remove_duplicates', 'affected': count})
                elif operation == 'fill_nulls':
                    count = self._fill_nulls(cleansed, field, strategy)
                    operations_log.append({'operation': 'fill_nulls', 'affected': count, 'strategy': strategy})
                elif operation == 'standardize_case':
                    count = self._standardize_case(cleansed, field, strategy)
                    operations_log.append({'operation': 'standardize_case', 'affected': count})

            result = {
                'cleansed': cleansed,
                'original_count': len(data),
                'cleansed_count': len(cleansed),
                'operations': operations_log
            }

            context.variables[output_var] = result
            return ActionResult(
                success=True,
                data=result,
                message=f"Data cleansing completed: {len(operations_log)} operations applied"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Data cleansing failed: {str(e)}"
            )

    def _trim_strings(self, data: List, field: str) -> int:
        """Trim whitespace from strings."""
        count = 0
        for item in data:
            if isinstance(item, dict):
                if field:
                    if isinstance(item.get(field), str):
                        item[field] = item[field].strip()
                        count += 1
                else:
                    for k, v in item.items():
                        if isinstance(v, str):
                            item[k] = v.strip()
                            count += 1
        return count

    def _remove_duplicates(self, data: List) -> int:
        """Remove duplicate items."""
        original_len = len(data)
        seen = []
        unique = []
        for item in data:
            key = json.dumps(item, sort_keys=True) if isinstance(item, dict) else str(item)
            if key not in seen:
                seen.append(key)
                unique.append(item)
        data[:] = unique
        return original_len - len(unique)

    def _fill_nulls(self, data: List, field: str, strategy: str) -> int:
        """Fill null values."""
        count = 0
        for item in data:
            if isinstance(item, dict) and field in item and item[field] is None:
                if strategy == 'zero':
                    item[field] = 0
                elif strategy == 'empty_string':
                    item[field] = ''
                elif strategy == 'default':
                    item[field] = 'N/A'
                count += 1
        return count

    def _standardize_case(self, data: List, field: str, case_type: str) -> int:
        """Standardize string case."""
        count = 0
        for item in data:
            if isinstance(item, dict) and field in item:
                value = item.get(field)
                if isinstance(value, str):
                    if case_type == 'lower':
                        item[field] = value.lower()
                    elif case_type == 'upper':
                        item[field] = value.upper()
                    elif case_type == 'title':
                        item[field] = value.title()
                    count += 1
        return count
