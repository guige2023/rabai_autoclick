"""Data Aggregator Action Module.

Provides data aggregation capabilities including rollups,
time-windowed aggregations, and multi-dimensional grouping.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Callable
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataRollupAction(BaseAction):
    """Perform data rollup operations.
    
    Supports time-based and count-based rollups with various aggregation functions.
    """
    action_type = "data_rollup"
    display_name = "数据汇总"
    description = "执行数据汇总操作，支持时间窗口和计数窗口"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Perform data rollup.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data list.
                - group_by: Field(s) to group by.
                - rollup_field: Field to aggregate.
                - agg_func: Aggregation function.
                - bucket_size: Size of each bucket.
                - bucket_unit: 'seconds', 'minutes', 'hours', 'days'.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with rollup result or error.
        """
        data = params.get('data', [])
        group_by = params.get('group_by', [])
        rollup_field = params.get('rollup_field', 'value')
        agg_func = params.get('agg_func', 'sum')
        bucket_size = params.get('bucket_size', 1)
        bucket_unit = params.get('bucket_unit', 'hours')
        output_var = params.get('output_var', 'rollup')

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list for data, got {type(data).__name__}"
            )

        try:
            # Group data
            groups = defaultdict(list)
            for item in data:
                if isinstance(group_by, list):
                    key = tuple(item.get(g) for g in group_by)
                else:
                    key = item.get(group_by)
                groups[key].append(item)

            # Rollup each group
            results = []
            for key, items in groups.items():
                rolled = self._rollup_group(
                    items, rollup_field, agg_func, bucket_size, bucket_unit
                )
                if isinstance(group_by, list):
                    for i, g in enumerate(group_by):
                        rolled[g] = key[i]
                else:
                    rolled[group_by] = key
                results.append(rolled)

            context.variables[output_var] = results
            return ActionResult(
                success=True,
                data={'rollup': results, 'count': len(results)},
                message=f"Rolled up {len(data)} items into {len(results)} buckets"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Data rollup failed: {str(e)}"
            )

    def _rollup_group(
        self,
        items: List,
        field: str,
        agg_func: str,
        bucket_size: int,
        bucket_unit: str
    ) -> Dict:
        """Rollup a group of items."""
        values = [item.get(field, 0) for item in items]

        if agg_func == 'sum':
            result = sum(values)
        elif agg_func == 'avg':
            result = sum(values) / len(values) if values else 0
        elif agg_func == 'count':
            result = len(values)
        elif agg_func == 'min':
            result = min(values) if values else None
        elif agg_func == 'max':
            result = max(values) if values else None
        else:
            result = sum(values)

        return {'aggregated': result, 'count': len(items)}


class MultiDimensionalAggregatorAction(BaseAction):
    """Aggregate data across multiple dimensions.
    
    Supports cube and rollup operations for multidimensional analysis.
    """
    action_type = "multi_dimensional_aggregator"
    display_name = "多维聚合"
    description = "跨多个维度聚合数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Aggregate across multiple dimensions.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data list.
                - dimensions: List of dimension fields.
                - measures: List of measure fields with agg functions.
                - agg_type: 'cube', 'rollup', 'grouping_sets'.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with aggregation result or error.
        """
        data = params.get('data', [])
        dimensions = params.get('dimensions', [])
        measures = params.get('measures', [])
        agg_type = params.get('agg_type', 'grouping_sets')
        output_var = params.get('output_var', 'multi_agg')

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list for data, got {type(data).__name__}"
            )

        try:
            if agg_type == 'cube':
                results = self._compute_cube(data, dimensions, measures)
            elif agg_type == 'rollup':
                results = self._compute_rollup(data, dimensions, measures)
            else:
                results = self._compute_grouping_sets(data, dimensions, measures)

            context.variables[output_var] = results
            return ActionResult(
                success=True,
                data={'aggregated': results, 'count': len(results)},
                message=f"Multi-dimensional aggregation: {len(results)} groups"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Multi-dimensional aggregation failed: {str(e)}"
            )

    def _compute_cube(
        self, data: List, dimensions: List, measures: List
    ) -> List[Dict]:
        """Compute full cube (all combinations)."""
        from itertools import product

        # Generate all combinations
        all_combinations = list(product([True, False], repeat=len(dimensions)))

        results = []
        for combo in all_combinations:
            active_dims = [dimensions[i] for i in range(len(dimensions)) if combo[i]]

            # Filter data
            filtered = data
            for item in filtered:
                pass

            # Compute aggregations
            row = {}
            for dim in active_dims:
                row[dim] = 'ALL'
            for measure in measures:
                field = measure.get('field')
                func = measure.get('func', 'sum')
                values = [item.get(field, 0) for item in filtered]
                row[f'{field}_{func}'] = self._aggregate(values, func)
            results.append(row)

        return results

    def _compute_rollup(
        self, data: List, dimensions: List, measures: List
    ) -> List[Dict]:
        """Compute rollup (hierarchical levels)."""
        results = []

        # All dimensions
        results.append(self._compute_aggregation(data, dimensions, measures))

        # Gradually remove dimensions
        for i in range(len(dimensions) - 1, -1, -1):
            dims = dimensions[:i]
            results.append(self._compute_aggregation(data, dims, measures))

        return results

    def _compute_grouping_sets(
        self, data: List, dimensions: List, measures: List
    ) -> List[Dict]:
        """Compute grouping sets."""
        # Compute for each dimension combination
        results = []
        for dim in dimensions:
            results.append(self._compute_aggregation(data, [dim], measures))
        results.append(self._compute_aggregation(data, [], measures))
        return results

    def _compute_aggregation(
        self, data: List, dimensions: List, measures: List
    ) -> Dict:
        """Compute aggregation for a dimension set."""
        # Group by dimensions
        groups = defaultdict(list)
        for item in data:
            if dimensions:
                key = tuple(item.get(d) for d in dimensions)
            else:
                key = ('ALL',)
            groups[key].append(item)

        results = []
        for key, items in groups.items():
            row = {}
            for i, dim in enumerate(dimensions):
                row[dim] = key[i]
            for measure in measures:
                field = measure.get('field')
                func = measure.get('func', 'sum')
                values = [item.get(field, 0) for item in items]
                row[f'{field}_{func}'] = self._aggregate(values, func)
            results.append(row)

        return results[0] if len(results) == 1 else results

    def _aggregate(self, values: List, func: str) -> Any:
        """Perform aggregation."""
        if not values:
            return 0
        if func == 'sum':
            return sum(values)
        elif func == 'avg':
            return sum(values) / len(values)
        elif func == 'count':
            return len(values)
        elif func == 'min':
            return min(values)
        elif func == 'max':
            return max(values)
        return sum(values)


class TimeWindowAggregatorAction(BaseAction):
    """Aggregate data over time windows.
    
    Supports tumbling, sliding, and session windows with time-based aggregation.
    """
    action_type = "time_window_aggregator"
    display_name = "时间窗口聚合"
    description = "跨时间窗口聚合数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Aggregate over time windows.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data with timestamps.
                - timestamp_field: Field containing timestamps.
                - value_field: Field to aggregate.
                - window_size: Window size in seconds.
                - window_type: 'tumbling', 'sliding', 'session'.
                - agg_func: Aggregation function.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with time-windowed aggregation or error.
        """
        data = params.get('data', [])
        timestamp_field = params.get('timestamp_field', 'timestamp')
        value_field = params.get('value_field', 'value')
        window_size = params.get('window_size', 3600)
        window_type = params.get('window_type', 'tumbling')
        agg_func = params.get('agg_func', 'sum')
        output_var = params.get('output_var', 'time_window')

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list for data, got {type(data).__name__}"
            )

        try:
            # Sort by timestamp
            sorted_data = sorted(
                data,
                key=lambda x: x.get(timestamp_field, 0)
            )

            if window_type == 'tumbling':
                results = self._tumbling_window(
                    sorted_data, timestamp_field, value_field, window_size, agg_func
                )
            elif window_type == 'sliding':
                results = self._sliding_window(
                    sorted_data, timestamp_field, value_field, window_size, agg_func
                )
            else:
                results = self._session_window(
                    sorted_data, timestamp_field, value_field, agg_func
                )

            context.variables[output_var] = results
            return ActionResult(
                success=True,
                data={'windows': results, 'count': len(results)},
                message=f"Time window aggregation: {len(results)} windows"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Time window aggregation failed: {str(e)}"
            )

    def _tumbling_window(
        self,
        data: List,
        timestamp_field: str,
        value_field: str,
        window_size: int,
        agg_func: str
    ) -> List[Dict]:
        """Compute tumbling windows."""
        if not data:
            return []

        results = []
        start_time = data[0].get(timestamp_field, 0)
        window_data = []

        for item in data:
            item_time = item.get(timestamp_field, 0)
            window_start = start_time - (start_time % window_size)
            item_window = item_time - (item_time % window_size)

            if item_window == window_start:
                window_data.append(item.get(value_field, 0))
            else:
                if window_data:
                    results.append({
                        'window_start': window_start,
                        'window_end': window_start + window_size,
                        'aggregated': self._aggregate(window_data, agg_func),
                        'count': len(window_data)
                    })
                start_time = item_time
                window_data = [item.get(value_field, 0)]

        if window_data:
            results.append({
                'window_start': start_time - (start_time % window_size),
                'window_end': start_time + window_size,
                'aggregated': self._aggregate(window_data, agg_func),
                'count': len(window_data)
            })

        return results

    def _sliding_window(
        self,
        data: List,
        timestamp_field: str,
        value_field: str,
        window_size: int,
        agg_func: str
    ) -> List[Dict]:
        """Compute sliding windows."""
        if not data:
            return []

        results = []
        half_window = window_size // 2

        for i, item in enumerate(data):
            item_time = item.get(timestamp_field, 0)
            window_start = item_time - half_window
            window_end = item_time + half_window

            # Collect values in window
            window_values = []
            for j in range(max(0, i - len(data)), min(len(data), i + 1)):
                other_time = data[j].get(timestamp_field, 0)
                if window_start <= other_time <= window_end:
                    window_values.append(data[j].get(value_field, 0))

            results.append({
                'timestamp': item_time,
                'window_start': window_start,
                'window_end': window_end,
                'aggregated': self._aggregate(window_values, agg_func),
                'count': len(window_values)
            })

        return results

    def _session_window(
        self,
        data: List,
        timestamp_field: str,
        value_field: str,
        agg_func: str
    ) -> List[Dict]:
        """Compute session windows based on gaps."""
        if not data:
            return []

        gap_threshold = 300  # 5 minutes
        results = []
        current_window = []
        last_time = None

        for item in data:
            item_time = item.get(timestamp_field, 0)

            if last_time and (item_time - last_time) > gap_threshold:
                if current_window:
                    results.append(self._create_session_result(current_window, timestamp_field, value_field, agg_func))
                current_window = []

            current_window.append(item)
            last_time = item_time

        if current_window:
            results.append(self._create_session_result(current_window, timestamp_field, value_field, agg_func))

        return results

    def _create_session_result(
        self,
        items: List,
        timestamp_field: str,
        value_field: str,
        agg_func: str
    ) -> Dict:
        """Create session window result."""
        values = [item.get(value_field, 0) for item in items]
        times = [item.get(timestamp_field, 0) for item in items]

        return {
            'session_start': min(times),
            'session_end': max(times),
            'duration': max(times) - min(times),
            'aggregated': self._aggregate(values, agg_func),
            'count': len(items)
        }

    def _aggregate(self, values: List, func: str) -> Any:
        """Perform aggregation."""
        if not values:
            return 0
        if func == 'sum':
            return sum(values)
        elif func == 'avg':
            return sum(values) / len(values)
        elif func == 'count':
            return len(values)
        elif func == 'min':
            return min(values)
        elif func == 'max':
            return max(values)
        return sum(values)


class PercentileAggregatorAction(BaseAction):
    """Calculate percentiles and quantiles from data.
    
    Supports various percentile calculations and distribution analysis.
    """
    action_type = "percentile_aggregator"
    display_name = "百分位聚合"
    description = "从数据计算百分位数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Calculate percentiles.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data list.
                - field: Field to calculate percentiles for.
                - percentiles: List of percentiles (0-100).
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with percentile calculation or error.
        """
        data = params.get('data', [])
        field = params.get('field', 'value')
        percentiles = params.get('percentiles', [25, 50, 75, 90, 95, 99])
        output_var = params.get('output_var', 'percentiles')

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list for data, got {type(data).__name__}"
            )

        try:
            # Extract values
            values = []
            for item in data:
                if isinstance(item, dict):
                    values.append(item.get(field, 0))
                else:
                    values.append(item)

            if not values:
                return ActionResult(
                    success=False,
                    message="No values to calculate percentiles"
                )

            values.sort()
            n = len(values)

            # Calculate percentiles
            result = {}
            for p in percentiles:
                idx = int(n * p / 100)
                if idx >= n:
                    idx = n - 1
                result[f'p{p}'] = values[idx]

            # Also calculate basic stats
            result['min'] = min(values)
            result['max'] = max(values)
            result['mean'] = sum(values) / n
            result['count'] = n

            context.variables[output_var] = result
            return ActionResult(
                success=True,
                data=result,
                message=f"Calculated {len(percentiles)} percentiles"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Percentile calculation failed: {str(e)}"
            )
