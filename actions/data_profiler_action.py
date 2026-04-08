"""Data profiler action module for RabAI AutoClick.

Provides data profiling actions for analyzing data quality,
statistics, and schema information.
"""

import sys
import os
import json
from typing import Any, Dict, List, Optional, Union
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataProfilerAction(BaseAction):
    """Profile dataset and generate statistics.
    
    Analyzes data structure, types, and distributions.
    """
    action_type = "data_profiler"
    display_name = "数据画像"
    description = "生成数据统计画像"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Profile data.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, include_histograms,
                   include_top_values, sample_size.
        
        Returns:
            ActionResult with profile statistics.
        """
        data = params.get('data', [])
        include_histograms = params.get('include_histograms', False)
        include_top_values = params.get('include_top_values', True)
        sample_size = params.get('sample_size', 10000)

        if not data:
            return ActionResult(success=False, message="data is required")

        if not isinstance(data, list):
            data = [data]

        sample = data[:sample_size] if len(data) > sample_size else data
        
        profile = {
            'record_count': len(data),
            'sample_count': len(sample),
            'fields': {}
        }

        if not sample:
            return ActionResult(success=True, message="No data to profile", data=profile)

        field_names = set()
        for record in sample:
            if isinstance(record, dict):
                field_names.update(record.keys())

        for field in field_names:
            values = [r.get(field) for r in sample if isinstance(r, dict) and field in r]
            non_null = [v for v in values if v is not None]
            
            field_stats = {
                'type': self._infer_type(non_null) if non_null else 'null',
                'count': len(values),
                'null_count': len(values) - len(non_null),
                'null_pct': round((len(values) - len(non_null)) / len(values) * 100, 2) if values else 0
            }

            if non_null:
                if all(isinstance(v, (int, float)) for v in non_null):
                    numeric = [v for v in non_null if isinstance(v, (int, float))]
                    if numeric:
                        field_stats['min'] = min(numeric)
                        field_stats['max'] = max(numeric)
                        field_stats['avg'] = sum(numeric) / len(numeric)
                        field_stats['sum'] = sum(numeric)
                        
                        if include_histograms:
                            field_stats['histogram'] = self._compute_histogram(numeric)
                
                if include_top_values:
                    counter = Counter(str(v) for v in non_null)
                    field_stats['top_values'] = counter.most_common(10)

            profile['fields'][field] = field_stats

        return ActionResult(
            success=True,
            message=f"Profiled {profile['record_count']} records, {len(field_names)} fields",
            data=profile
        )

    def _infer_type(self, values: List) -> str:
        """Infer data type from values."""
        if not values:
            return 'unknown'
        
        type_counts = Counter(type(v).__name__ for v in values)
        most_common = type_counts.most_common(1)[0][0]
        
        type_map = {
            'str': 'string',
            'int': 'integer',
            'float': 'float',
            'bool': 'boolean',
            'list': 'array',
            'dict': 'object'
        }
        
        return type_map.get(most_common, most_common)

    def _compute_histogram(self, values: List, bins: int = 10) -> Dict:
        """Compute histogram for numeric values."""
        if not values:
            return {}
        
        min_val = min(values)
        max_val = max(values)
        range_val = max_val - min_val if max_val != min_val else 1
        bin_width = range_val / bins
        
        histogram = {}
        for i in range(bins):
            bin_start = min_val + i * bin_width
            bin_end = bin_start + bin_width
            bin_label = f"{bin_start:.2f}-{bin_end:.2f}"
            count = sum(1 for v in values if bin_start <= v < bin_end)
            histogram[bin_label] = count
        
        return histogram


class DataQualityCheckAction(BaseAction):
    """Perform data quality checks.
    
    Validates data against quality rules.
    """
    action_type = "data_quality_check"
    display_name = "数据质量检查"
    description = "数据质量规则检查"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Check data quality.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, rules, fail_fast.
                   rules: list of {name, field, check, threshold}.
        
        Returns:
            ActionResult with quality check results.
        """
        data = params.get('data', [])
        rules = params.get('rules', [])
        fail_fast = params.get('fail_fast', False)

        if not data:
            return ActionResult(success=False, message="data is required")

        if not isinstance(data, list):
            data = [data]

        results = []
        passed = 0
        failed = 0

        for rule in rules:
            name = rule.get('name', 'unnamed')
            field = rule.get('field', '')
            check_type = rule.get('check', 'completeness')
            threshold = rule.get('threshold', 0)

            rule_result = self._check_rule(data, field, check_type, threshold)
            rule_result['name'] = name
            
            results.append(rule_result)
            
            if rule_result['passed']:
                passed += 1
            else:
                failed += 1
                if fail_fast:
                    break

        return ActionResult(
            success=failed == 0,
            message=f"Quality check: {passed} passed, {failed} failed",
            data={
                'passed': passed,
                'failed': failed,
                'total': len(rules),
                'results': results
            }
        )

    def _check_rule(self, data: List, field: str, check_type: str, threshold: float) -> Dict:
        """Check a single rule."""
        if check_type == 'completeness':
            non_null = sum(1 for r in data if isinstance(r, dict) and r.get(field) is not None)
            score = non_null / len(data) if data else 0
            return {'passed': score >= threshold, 'score': round(score, 4), 'type': 'completeness'}
        
        elif check_type == 'uniqueness':
            values = [r.get(field) for r in data if isinstance(r, dict) and field in r]
            unique = len(set(values))
            score = unique / len(values) if values else 0
            return {'passed': score >= threshold, 'score': round(score, 4), 'type': 'uniqueness'}
        
        elif check_type == 'validity':
            valid = sum(1 for r in data if isinstance(r, dict) and r.get(field) is not None)
            score = valid / len(data) if data else 0
            return {'passed': score >= threshold, 'score': round(score, 4), 'type': 'validity'}
        
        elif check_type == 'consistency':
            score = 1.0
            return {'passed': score >= threshold, 'score': round(score, 4), 'type': 'consistency'}
        
        return {'passed': False, 'score': 0, 'type': check_type, 'error': 'unknown check type'}


class DataSchemaInferAction(BaseAction):
    """Infer schema from data.
    
    Generates schema definition from data samples.
    """
    action_type = "data_schema_infer"
    display_name = "推断Schema"
    description = "从数据推断Schema"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Infer schema.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, strict, sample_size.
        
        Returns:
            ActionResult with inferred schema.
        """
        data = params.get('data', [])
        strict = params.get('strict', False)
        sample_size = params.get('sample_size', 1000)

        if not data:
            return ActionResult(success=False, message="data is required")

        if not isinstance(data, list):
            data = [data]

        sample = data[:sample_size] if len(data) > sample_size else data
        
        schema = {
            'type': 'array',
            'items': {'type': 'object', 'properties': {}},
            'record_count': len(data)
        }

        for record in sample:
            if not isinstance(record, dict):
                continue
            
            for field, value in record.items():
                if field not in schema['items']['properties']:
                    schema['items']['properties'][field] = {
                        'type': self._get_type(value),
                        'nullable': True,
                        'samples': []
                    }
                
                prop = schema['items']['properties'][field]
                prop['type'] = self._merge_types(prop['type'], self._get_type(value))
                
                if len(prop['samples']) < 5:
                    prop['samples'].append(value)

        if strict:
            for field in schema['items']['properties']:
                schema['items']['properties'][field]['nullable'] = False

        return ActionResult(
            success=True,
            message=f"Inferred schema with {len(schema['items']['properties'])} fields",
            data={'schema': schema}
        )

    def _get_type(self, value: Any) -> str:
        """Get JSON schema type."""
        if value is None:
            return 'null'
        elif isinstance(value, bool):
            return 'boolean'
        elif isinstance(value, int):
            return 'integer'
        elif isinstance(value, float):
            return 'number'
        elif isinstance(value, str):
            return 'string'
        elif isinstance(value, list):
            return 'array'
        elif isinstance(value, dict):
            return 'object'
        return 'string'

    def _merge_types(self, type1: str, type2: str) -> str:
        """Merge two types."""
        if type1 == type2:
            return type1
        if 'null' in (type1, type2):
            return type2 if type1 == 'null' else type1
        return 'string'


class DataAnomalyDetectAction(BaseAction):
    """Detect anomalies in data.
    
    Identifies outliers and unusual patterns.
    """
    action_type = "data_anomaly_detect"
    display_name = "异常检测"
    description = "检测数据异常值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Detect anomalies.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, field, method,
                   threshold, z_threshold.
        
        Returns:
            ActionResult with detected anomalies.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        method = params.get('method', 'zscore')
        threshold = params.get('threshold', 3)
        z_threshold = params.get('z_threshold', 3)

        if not data:
            return ActionResult(success=False, message="data is required")

        if not field:
            return ActionResult(success=False, message="field is required")

        numeric_values = []
        for i, record in enumerate(data):
            if isinstance(record, dict) and field in record:
                value = record[field]
                if isinstance(value, (int, float)):
                    numeric_values.append((i, value, record))

        if not numeric_values:
            return ActionResult(success=True, message="No numeric values found", data={'anomalies': []})

        values = [v[1] for v in numeric_values]
        
        if method == 'zscore':
            mean = sum(values) / len(values)
            variance = sum((v - mean) ** 2 for v in values) / len(values)
            std = variance ** 0.5
            
            anomalies = []
            for idx, value, record in numeric_values:
                if std > 0:
                    zscore = abs((value - mean) / std)
                    if zscore > z_threshold:
                        anomalies.append({
                            'index': idx,
                            'value': value,
                            'zscore': round(zscore, 3),
                            'record': record
                        })
        
        elif method == 'iqr':
            sorted_vals = sorted(values)
            n = len(sorted_vals)
            q1 = sorted_vals[n // 4]
            q3 = sorted_vals[3 * n // 4]
            iqr = q3 - q1
            
            lower = q1 - threshold * iqr
            upper = q3 + threshold * iqr
            
            anomalies = []
            for idx, value, record in numeric_values:
                if value < lower or value > upper:
                    anomalies.append({
                        'index': idx,
                        'value': value,
                        'bounds': (lower, upper),
                        'record': record
                    })
        
        else:
            return ActionResult(success=False, message=f"Unknown method: {method}")

        return ActionResult(
            success=True,
            message=f"Detected {len(anomalies)} anomalies using {method}",
            data={
                'anomalies': anomalies,
                'count': len(anomalies),
                'method': method
            }
        )
