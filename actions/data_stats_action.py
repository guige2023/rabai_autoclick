"""Data Stats Action.

Computes statistical summaries of data with descriptive statistics,
distribution analysis, correlation computation, and percentile calculations.
"""

import sys
import os
import math
from typing import Any, Dict, List, Optional
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataStatsAction(BaseAction):
    """Compute statistical summaries of data.
    
    Calculates descriptive statistics, distributions, correlations,
    and percentiles for numerical and categorical data.
    """
    action_type = "data_stats"
    display_name = "数据统计"
    description = "计算数据统计摘要，包括描述统计、分布、相关性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Compute statistics.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Data to analyze.
                - fields: Fields to analyze (default: all numeric).
                - stats: List of stats to compute ('mean', 'median', 'std', etc.).
                - percentiles: List of percentiles to compute.
                - group_by: Field to group statistics by.
                - compute_correlation: Compute correlations between fields.
                - save_to_var: Variable name for result.
        
        Returns:
            ActionResult with statistics.
        """
        try:
            data = params.get('data')
            fields = params.get('fields')
            stats = params.get('stats', ['count', 'mean', 'median', 'std', 'min', 'max'])
            percentiles = params.get('percentiles', [25, 50, 75, 90, 95, 99])
            group_by = params.get('group_by')
            compute_correlation = params.get('compute_correlation', False)
            save_to_var = params.get('save_to_var', 'stats_result')

            if data is None:
                data = context.get_variable(params.get('use_var', 'input_data'))

            if not data:
                return ActionResult(success=False, message="No data provided")

            if not isinstance(data, list):
                return ActionResult(success=False, message="Data must be a list")

            # Determine fields to analyze
            if not fields:
                fields = self._detect_numeric_fields(data)

            if group_by:
                result = self._compute_grouped_stats(data, fields, stats, percentiles, group_by)
            else:
                result = self._compute_stats(data, fields, stats, percentiles, compute_correlation)

            context.set_variable(save_to_var, result)
            return ActionResult(success=True, data=result,
                             message=f"Computed stats for {len(fields)} fields")

        except Exception as e:
            return ActionResult(success=False, message=f"Stats error: {e}")

    def _detect_numeric_fields(self, data: List) -> List[str]:
        """Detect numeric fields in data."""
        if not data:
            return []
        
        fields = set()
        for item in data:
            if isinstance(item, dict):
                for key, value in item.items():
                    if isinstance(value, (int, float)):
                        fields.add(key)
        
        return list(fields)

    def _compute_stats(self, data: List[Dict], fields: List[str], 
                     stats: List[str], percentiles: List[int],
                     compute_correlation: bool) -> Dict:
        """Compute statistics for specified fields."""
        result = {'fields': {}, 'row_count': len(data)}

        for field in fields:
            values = [item.get(field) for item in data if isinstance(item, dict) and isinstance(item.get(field), (int, float))]
            if not values:
                continue

            field_stats = {'count': len(values)}

            for stat in stats:
                if stat == 'count':
                    field_stats['count'] = len(values)
                elif stat == 'mean':
                    field_stats['mean'] = sum(values) / len(values)
                elif stat == 'sum':
                    field_stats['sum'] = sum(values)
                elif stat == 'median':
                    field_stats['median'] = self._median(values)
                elif stat == 'std' or stat == 'stdev':
                    field_stats['std'] = self._stdev(values)
                elif stat == 'min':
                    field_stats['min'] = min(values)
                elif stat == 'max':
                    field_stats['max'] = max(values)
                elif stat == 'range':
                    field_stats['range'] = max(values) - min(values)
                elif stat == 'variance':
                    field_stats['variance'] = self._variance(values)
                elif stat == 'cv' or stat == 'coef_variation':
                    mean = sum(values) / len(values)
                    if mean != 0:
                        field_stats['cv'] = self._stdev(values) / abs(mean)

            # Compute percentiles
            for p in percentiles:
                field_stats[f'p{p}'] = self._percentile(values, p)

            result['fields'][field] = field_stats

        # Compute correlations if requested
        if compute_correlation and len(fields) >= 2:
            result['correlations'] = self._compute_correlations(data, fields)

        return result

    def _compute_grouped_stats(self, data: List[Dict], fields: List[str],
                              stats: List[str], percentiles: List[int],
                              group_by: str) -> Dict:
        """Compute statistics grouped by a field."""
        groups = {}
        for item in data:
            if isinstance(item, dict):
                key = item.get(group_by)
                if key not in groups:
                    groups[key] = []
                groups[key].append(item)

        result = {'groups': {}, 'group_by': group_by}
        for key, group_data in groups.items():
            result['groups'][key] = self._compute_stats(group_data, fields, stats, percentiles, False)

        return result

    def _compute_correlations(self, data: List[Dict], fields: List[str]) -> Dict:
        """Compute pairwise correlations between fields."""
        correlations = {}
        
        for i, f1 in enumerate(fields):
            for f2 in fields[i+1:]:
                values1 = [item.get(f1) for item in data if isinstance(item, dict) and isinstance(item.get(f1), (int, float))]
                values2 = [item.get(f2) for item in data if isinstance(item, dict) and isinstance(item.get(f2), (int, float))]
                
                if len(values1) != len(values2) or len(values1) < 2:
                    continue

                corr = self._pearson_correlation(values1, values2)
                correlations[f'{f1}_vs_{f2}'] = corr

        return correlations

    def _median(self, values: List[float]) -> float:
        """Compute median."""
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        if n % 2 == 0:
            return (sorted_vals[n//2 - 1] + sorted_vals[n//2]) / 2
        return sorted_vals[n//2]

    def _stdev(self, values: List[float]) -> float:
        """Compute standard deviation."""
        return math.sqrt(self._variance(values))

    def _variance(self, values: List[float]) -> float:
        """Compute variance."""
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        return sum((x - mean) ** 2 for x in values) / (len(values) - 1)

    def _percentile(self, values: List[float], p: int) -> float:
        """Compute percentile."""
        sorted_vals = sorted(values)
        idx = (p / 100) * (len(sorted_vals) - 1)
        lower = int(idx)
        upper = lower + 1
        if upper >= len(sorted_vals):
            return sorted_vals[-1]
        return sorted_vals[lower] + (idx - lower) * (sorted_vals[upper] - sorted_vals[lower])

    def _pearson_correlation(self, x: List[float], y: List[float]) -> float:
        """Compute Pearson correlation coefficient."""
        n = len(x)
        if n < 2:
            return 0.0
        
        mean_x = sum(x) / n
        mean_y = sum(y) / n
        
        numerator = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
        denom_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x))
        denom_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y))
        
        if denom_x == 0 or denom_y == 0:
            return 0.0
        
        return numerator / (denom_x * denom_y)
