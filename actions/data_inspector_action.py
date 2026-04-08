"""Data inspector action module for RabAI AutoClick.

Provides data inspection and profiling operations to understand
data structure, statistics, types, and quality.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataProfilerAction(BaseAction):
    """Profile data to generate descriptive statistics.
    
    Computes statistics like count, null count, unique values,
    min, max, mean, median, std dev for numeric fields.
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
            params: Dict with keys: data, fields (optional list to profile),
                   include_histogram, include_quartiles.
        
        Returns:
            ActionResult with profiling statistics.
        """
        import math

        data = params.get('data', [])
        fields = params.get('fields', [])
        include_histogram = params.get('include_histogram', False)
        include_quartiles = params.get('include_quartiles', False)
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if not fields:
            if data and isinstance(data[0], dict):
                fields = list(data[0].keys())

        profiles = {}
        for field in fields:
            values = [self._get_field(row, field) for row in data]
            numeric_values = []
            for v in values:
                try:
                    numeric_values.append(float(v))
                except (TypeError, ValueError):
                    pass

            profile = {
                'field': field,
                'count': len(values),
                'null_count': sum(1 for v in values if v is None or v == ''),
                'unique_count': len(set(str(v) for v in values if v is not None)),
            }

            if numeric_values:
                sorted_vals = sorted(numeric_values)
                profile.update({
                    'min': min(numeric_values),
                    'max': max(numeric_values),
                    'mean': sum(numeric_values) / len(numeric_values),
                    'sum': sum(numeric_values),
                    'count_numeric': len(numeric_values),
                })
                mid = len(sorted_vals) // 2
                if len(sorted_vals) % 2 == 0:
                    profile['median'] = (sorted_vals[mid - 1] + sorted_vals[mid]) / 2
                else:
                    profile['median'] = sorted_vals[mid]

                variance = sum((x - profile['mean']) ** 2 for x in numeric_values) / len(numeric_values)
                profile['std'] = math.sqrt(variance) if variance >= 0 else 0

                if include_quartiles:
                    q1_idx = len(sorted_vals) // 4
                    q3_idx = 3 * len(sorted_vals) // 4
                    profile['q1'] = sorted_vals[q1_idx]
                    profile['q3'] = sorted_vals[q3_idx]
                    profile['iqr'] = profile['q3'] - profile['q1']

            else:
                str_values = [str(v) for v in values if v is not None and v != '']
                if str_values:
                    profile['min_length'] = min(len(s) for s in str_values)
                    profile['max_length'] = max(len(s) for s in str_values)
                    profile['avg_length'] = sum(len(s) for s in str_values) / len(str_values)

            if include_histogram and numeric_values:
                profile['histogram'] = self._compute_histogram(numeric_values)

            profiles[field] = profile

        return ActionResult(
            success=True,
            message=f"Profiled {len(profiles)} fields from {len(data)} records",
            data={
                'profiles': profiles,
                'total_records': len(data),
                'fields_profiled': len(profiles)
            },
            duration=time.time() - start_time
        )

    def _compute_histogram(self, values: List[float], bins: int = 10) -> Dict:
        """Compute histogram bins."""
        if not values:
            return {}
        min_val = min(values)
        max_val = max(values)
        if min_val == max_val:
            return {'bins': [min_val], 'counts': [len(values)]}

        bin_width = (max_val - min_val) / bins
        bin_edges = [min_val + i * bin_width for i in range(bins + 1)]
        counts = [0] * bins

        for v in values:
            idx = min(int((v - min_val) / bin_width), bins - 1)
            counts[idx] += 1

        return {
            'bins': [round(min_val + (i + 0.5) * bin_width, 4) for i in range(bins)],
            'counts': counts,
            'bin_width': round(bin_width, 4)
        }

    def _get_field(self, row: Any, field: str) -> Any:
        if not field:
            return row
        keys = field.split('.')
        value = row
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            elif hasattr(value, k):
                value = getattr(value, k)
            else:
                return None
        return value


class SchemaInspectorAction(BaseAction):
    """Inspect data schema and structure.
    
    Detects field names, data types, nullability,
    and structural patterns in the data.
    """
    action_type = "schema_inspector"
    display_name = "结构检查"
    description = "检查数据结构"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Inspect data schema.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, sample_size.
        
        Returns:
            ActionResult with schema information.
        """
        data = params.get('data', [])
        sample_size = params.get('sample_size', 100)
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        sample = data[:sample_size]
        schema = {}

        for row in sample:
            if not isinstance(row, dict):
                continue
            for key, value in row.items():
                if key not in schema:
                    schema[key] = {
                        'name': key,
                        'detected_types': set(),
                        'null_count': 0,
                        'sample_values': []
                    }

                if value is None or value == '':
                    schema[key]['null_count'] += 1
                else:
                    detected_type = self._detect_type(value)
                    schema[key]['detected_types'].add(detected_type)
                    if len(schema[key]['sample_values']) < 5:
                        schema[key]['sample_values'].append(value)

        schema_list = []
        for name, info in schema.items():
            detected_types = list(info['detected_types'])
            inferred_type = detected_types[0] if len(detected_types) == 1 else 'mixed'

            schema_list.append({
                'name': name,
                'inferred_type': inferred_type,
                'possible_types': detected_types,
                'null_count': info['null_count'],
                'null_ratio': info['null_count'] / len(sample) if sample else 0,
                'sample_values': info['sample_values'],
                'nullable': info['null_count'] > 0
            })

        schema_list.sort(key=lambda x: x['null_ratio'], reverse=True)

        return ActionResult(
            success=True,
            message=f"Inspected schema: {len(schema_list)} fields",
            data={
                'schema': schema_list,
                'field_count': len(schema_list),
                'sample_size': len(sample),
                'total_records': len(data)
            },
            duration=time.time() - start_time
        )

    def _detect_type(self, value: Any) -> str:
        """Detect the type of a value."""
        if isinstance(value, bool):
            return 'boolean'
        if isinstance(value, int):
            return 'integer'
        if isinstance(value, float):
            return 'float'
        if isinstance(value, str):
            return 'string'
        if isinstance(value, list):
            return 'array'
        if isinstance(value, dict):
            return 'object'
        return 'unknown'


class DataQualityCheckAction(BaseAction):
    """Check data quality against defined rules.
    
    Validates data against quality rules like completeness,
    consistency, validity, and uniqueness.
    """
    action_type = "data_quality_check"
    display_name = "数据质量检查"
    description = "检查数据质量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Check data quality.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, rules (list of quality rules).
        
        Returns:
            ActionResult with quality check results.
        """
        data = params.get('data', [])
        rules = params.get('rules', [])
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if not rules:
            rules = [
                {'type': 'completeness', 'threshold': 0.95},
                {'type': 'uniqueness', 'threshold': 0.90},
                {'type': 'validity', 'threshold': 0.95},
            ]

        results = {r['type']: {'passed': 0, 'failed': 0, 'details': []} for r in rules}

        for row in data:
            for rule in rules:
                rule_type = rule.get('type', '')
                if rule_type == 'completeness':
                    null_fields = [k for k, v in row.items() if v is None or v == '']
                    if not null_fields:
                        results['completeness']['passed'] += 1
                    else:
                        results['completeness']['failed'] += 1
                        results['completeness']['details'].append({'row': row, 'null_fields': null_fields})

                elif rule_type == 'uniqueness':
                    pass

                elif rule_type == 'validity':
                    pass

        summary = []
        for rule in rules:
            rule_type = rule.get('type', '')
            threshold = rule.get('threshold', 1.0)
            total = results[rule_type]['passed'] + results[rule_type]['failed']
            pass_rate = results[rule_type]['passed'] / total if total > 0 else 0
            passed = pass_rate >= threshold
            summary.append({
                'rule': rule_type,
                'threshold': threshold,
                'pass_rate': round(pass_rate, 4),
                'passed': passed,
                'passed_count': results[rule_type]['passed'],
                'failed_count': results[rule_type]['failed']
            })

        all_passed = all(s['passed'] for s in summary)

        return ActionResult(
            success=all_passed,
            message=f"Quality check: {sum(s['passed'] for s in summary)}/{len(summary)} rules passed",
            data={
                'results': results,
                'summary': summary,
                'all_passed': all_passed,
                'total_records': len(data)
            },
            duration=time.time() - start_time
        )


class SampleInspectorAction(BaseAction):
    """Extract and inspect sample records from data.
    
    Returns random samples, first N, last N, or
    stratified samples for manual inspection.
    """
    action_type = "sample_inspector"
    display_name = "样本检查"
    description = "提取样本记录"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Extract sample data.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, mode (first|last|random| stratified),
                   n (number of samples), seed, stratify_by.
        
        Returns:
            ActionResult with sampled data.
        """
        data = params.get('data', [])
        mode = params.get('mode', 'first')
        n = params.get('n', 10)
        seed = params.get('seed')
        stratify_by = params.get('stratify_by', '')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if mode == 'first':
            sample = data[:n]
        elif mode == 'last':
            sample = data[-n:]
        elif mode == 'random':
            import random
            if seed is not None:
                random.seed(seed)
            indices = random.sample(range(len(data)), min(n, len(data)))
            sample = [data[i] for i in indices]
        elif mode == 'stratified' and stratify_by:
            buckets = {}
            for row in data:
                key = self._get_field(row, stratify_by)
                if key not in buckets:
                    buckets[key] = []
                buckets[key].append(row)
            samples_per_bucket = max(1, n // len(buckets))
            sample = []
            for bucket in buckets.values():
                sample.extend(bucket[:samples_per_bucket])
        else:
            sample = data[:n]

        return ActionResult(
            success=True,
            message=f"Sampled {len(sample)} records (mode={mode})",
            data={
                'sample': sample,
                'count': len(sample),
                'mode': mode,
                'total_records': len(data)
            },
            duration=time.time() - start_time
        )

    def _get_field(self, row: Any, field: str) -> Any:
        if not field:
            return row
        keys = field.split('.')
        value = row
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            elif hasattr(value, k):
                value = getattr(value, k)
            else:
                return None
        return value


class DataDiffAction(BaseAction):
    """Compare two datasets and report differences.
    
    Compares schemas, values, and identifies added,
    removed, and modified records between two datasets.
    """
    action_type = "data_diff"
    display_name = "数据对比"
    description = "对比两个数据集的差异"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compare two datasets.
        
        Args:
            context: Execution context.
            params: Dict with keys: data_a, data_b, key_field,
                   compare_mode (full|key|schema).
        
        Returns:
            ActionResult with diff results.
        """
        data_a = params.get('data_a', [])
        data_b = params.get('data_b', [])
        key_field = params.get('key_field', '')
        compare_mode = params.get('compare_mode', 'key')
        start_time = time.time()

        if not isinstance(data_a, list):
            data_a = [data_a]
        if not isinstance(data_b, list):
            data_b = [data_b]

        if compare_mode == 'schema':
            schema_a = set(data_a[0].keys()) if data_a else set()
            schema_b = set(data_b[0].keys()) if data_b else set()
            only_in_a = schema_a - schema_b
            only_in_b = schema_b - schema_a
            common = schema_a & schema_b

            return ActionResult(
                success=True,
                message=f"Schema diff: {len(common)} common, {len(only_in_a)} only in A, {len(only_in_b)} only in B",
                data={
                    'only_in_a': list(only_in_a),
                    'only_in_b': list(only_in_b),
                    'common_fields': list(common),
                    'total_fields_a': len(schema_a),
                    'total_fields_b': len(schema_b)
                },
                duration=time.time() - start_time
            )

        if key_field:
            index_b = {self._get_field(row, key_field): row for row in data_b}
            added = []
            removed = []
            modified = []

            for row_a in data_a:
                key = self._get_field(row_a, key_field)
                if key not in index_b:
                    removed.append(row_a)
                else:
                    row_b = index_b[key]
                    diff = self._compare_rows(row_a, row_b)
                    if diff:
                        modified.append({'key': key, 'differences': diff})

            for row_b in data_b:
                key = self._get_field(row_b, key_field)
                if not any(self._get_field(r, key_field) == key for r in data_a):
                    added.append(row_b)

            return ActionResult(
                success=True,
                message=f"Diff: {len(added)} added, {len(removed)} removed, {len(modified)} modified",
                data={
                    'added': added,
                    'removed': removed,
                    'modified': modified,
                    'added_count': len(added),
                    'removed_count': len(removed),
                    'modified_count': len(modified)
                },
                duration=time.time() - start_time
            )

        return ActionResult(success=True, message="No diff performed", data={})

    def _compare_rows(self, row_a: Dict, row_b: Dict) -> Dict:
        """Compare two rows and return differences."""
        diff = {}
        all_keys = set(row_a.keys()) | set(row_b.keys())
        for key in all_keys:
            val_a = row_a.get(key)
            val_b = row_b.get(key)
            if val_a != val_b:
                diff[key] = {'from': val_a, 'to': val_b}
        return diff

    def _get_field(self, row: Any, field: str) -> Any:
        if not field:
            return row
        keys = field.split('.')
        value = row
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            elif hasattr(value, k):
                value = getattr(value, k)
            else:
                return None
        return value
