"""Aggregation action module for RabAI AutoClick.

Provides data aggregation with group-by, window functions,
rollup, cube, and running total calculations.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict, OrderedDict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AggregationAction(BaseAction):
    """Aggregate data with group-by and window functions.
    
    Supports SUM, AVG, COUNT, MIN, MAX, running totals,
    lag/lead, ranking, rollup, and cube operations.
    """
    action_type = "aggregation"
    display_name = "数据聚合"
    description = "数据聚合：分组/窗口函数/滚动计算/排名/拉姆/立方"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Perform aggregation operations.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str (group_by/window/running/rank/rollup/cube)
                - data: list of dicts
                - group_by: list of field names
                - agg_fields: list of {field, func} specs
                - window_size: int, window size for running calculations
                - sort_by: list of {field, order} specs
                - save_to_var: str
        
        Returns:
            ActionResult with aggregated data.
        """
        operation = params.get('operation', 'group_by')
        data = params.get('data', [])
        save_to_var = params.get('save_to_var', None)

        if not data:
            return ActionResult(success=False, message="No data provided")

        try:
            if operation == 'group_by':
                result = self._group_by(data, params)
            elif operation == 'window':
                result = self._window_func(data, params)
            elif operation == 'running':
                result = self._running_total(data, params)
            elif operation == 'rank':
                result = self._ranking(data, params)
            elif operation == 'rollup':
                result = self._rollup(data, params)
            elif operation == 'cube':
                result = self._cube(data, params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

            if save_to_var and hasattr(context, 'vars'):
                context.vars[save_to_var] = result

            return ActionResult(
                success=True,
                message=f"Aggregated: {len(result)} groups",
                data=result
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Aggregation error: {e}")

    def _group_by(self, data: List[Dict], params: Dict) -> List[Dict]:
        """Group by and aggregate."""
        group_by = params.get('group_by', [])
        agg_fields = params.get('agg_fields', [])

        if not group_by or not agg_fields:
            return data

        groups = defaultdict(list)
        for record in data:
            key = tuple(record.get(g) for g in group_by)
            groups[key].append(record)

        results = []
        for key, records in groups.items():
            result_dict = dict(zip(group_by, key))
            for agg_spec in agg_fields:
                field_name = agg_spec.get('field', '')
                func = agg_spec.get('func', 'sum')
                output_name = agg_spec.get('alias', f'{field_name}_{func}')

                values = [r.get(field_name, 0) for r in records if field_name in r]
                nums = [v for v in values if isinstance(v, (int, float))]
                if func == 'sum':
                    result_dict[output_name] = sum(nums) if nums else 0
                elif func == 'avg':
                    result_dict[output_name] = sum(nums) / len(nums) if nums else 0
                elif func == 'count':
                    result_dict[output_name] = len(records)
                elif func == 'min':
                    result_dict[output_name] = min(nums) if nums else None
                elif func == 'max':
                    result_dict[output_name] = max(nums) if nums else None
                elif func == 'first':
                    result_dict[output_name] = values[0] if values else None
                elif func == 'last':
                    result_dict[output_name] = values[-1] if values else None
                elif func == 'count_distinct':
                    result_dict[output_name] = len(set(values))
            results.append(result_dict)

        return results

    def _window_func(self, data: List[Dict], params: Dict) -> List[Dict]:
        """Apply window functions."""
        agg_fields = params.get('agg_fields', [])
        sort_by = params.get('sort_by', [])
        window_size = params.get('window_size', 3)
        window_type = params.get('window_type', 'sliding')  # sliding or expanding

        # Sort data
        sorted_data = self._sort_data(data, sort_by)

        results = []
        for i, record in enumerate(sorted_data):
            new_record = dict(record)

            for agg_spec in agg_fields:
                field_name = agg_spec.get('field', '')
                func = agg_spec.get('func', 'sum')
                alias = agg_spec.get('alias', f'{field_name}_{func}_window')

                if window_type == 'sliding':
                    window_data = sorted_data[max(0, i - window_size + 1):i + 1]
                else:  # expanding
                    window_data = sorted_data[:i + 1]

                values = [r.get(field_name, 0) for r in window_data if field_name in r]
                nums = [v for v in values if isinstance(v, (int, float))]

                if func == 'sum':
                    new_record[alias] = sum(nums)
                elif func == 'avg':
                    new_record[alias] = sum(nums) / len(nums) if nums else 0
                elif func == 'min':
                    new_record[alias] = min(nums) if nums else None
                elif func == 'max':
                    new_record[alias] = max(nums) if nums else None
                elif func == 'count':
                    new_record[alias] = len(window_data)

            results.append(new_record)

        return results

    def _running_total(self, data: List[Dict], params: Dict) -> List[Dict]:
        """Calculate running totals."""
        field_name = params.get('field', '')
        sort_by = params.get('sort_by', [])

        if not field_name:
            return data

        sorted_data = self._sort_data(data, sort_by)
        running = 0
        results = []

        for record in sorted_data:
            new_record = dict(record)
            val = record.get(field_name, 0)
            if isinstance(val, (int, float)):
                running += val
            new_record['running_total'] = running
            results.append(new_record)

        return results

    def _ranking(self, data: List[Dict], params: Dict) -> List[Dict]:
        """Add ranking columns."""
        sort_by = params.get('sort_by', [])
        rank_field = params.get('rank_field', 'rank')
        partition_by = params.get('partition_by', [])

        if partition_by:
            return self._rank_partitioned(data, partition_by, sort_by, rank_field)
        else:
            sorted_data = self._sort_data(data, sort_by)
            results = []
            for i, record in enumerate(sorted_data):
                new_record = dict(record)
                new_record[rank_field] = i + 1
                results.append(new_record)
            return results

    def _rank_partitioned(
        self, data: List[Dict], partition_by: List[str],
        sort_by: List[Dict], rank_field: str
    ) -> List[Dict]:
        """Rank within partitions."""
        partitions = defaultdict(list)
        for record in data:
            key = tuple(record.get(p) for p in partition_by)
            partitions[key].append(record)

        results = []
        for key, partition in partitions.items():
            sorted_part = self._sort_data(partition, sort_by)
            for i, record in enumerate(sorted_part):
                new_record = dict(record)
                for p, v in zip(partition_by, key):
                    new_record[p] = v
                new_record[rank_field] = i + 1
                results.append(new_record)

        return results

    def _rollup(self, data: List[Dict], params: Dict) -> List[Dict]:
        """Generate rollup aggregation (hierarchical subtotals)."""
        group_by = params.get('group_by', [])
        agg_fields = params.get('agg_fields', [])

        if not group_by:
            return data

        results = []
        # All data total
        results.extend(self._group_by(data, {**params, 'group_by': []}))

        # Hierarchical subtotals
        for i in range(len(group_by)):
            level_groups = group_by[:len(group_by) - i - 1] if i > 0 else []
            if level_groups:
                results.extend(self._group_by(data, {**params, 'group_by': level_groups}))

        return results

    def _cube(self, data: List[Dict], params: Dict) -> List[Dict]:
        """Generate cube aggregation (all combinations)."""
        group_by = params.get('group_by', [])
        agg_fields = params.get('agg_fields', [])

        if not group_by:
            return data

        results = []
        from itertools import combinations
        n = len(group_by)

        # All data total
        results.extend(self._group_by(data, {**params, 'group_by': []}))

        # All subsets
        for r in range(1, n + 1):
            for combo in combinations(group_by, r):
                results.extend(self._group_by(data, {**params, 'group_by': list(combo)}))

        return results

    def _sort_data(self, data: List[Dict], sort_by: List[Dict]) -> List[Dict]:
        """Sort data by specified fields."""
        if not sort_by:
            return list(data)

        def sort_key(record):
            keys = []
            for spec in sort_by:
                field_name = spec.get('field', '')
                order = spec.get('order', 'asc')
                val = record.get(field_name)
                if order == 'desc':
                    val = -val if isinstance(val, (int, float)) else val
                keys.append(val)
            return tuple(keys)

        return sorted(list(data), key=sort_key)

    def get_required_params(self) -> List[str]:
        return ['operation', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'group_by': [],
            'agg_fields': [],
            'field': '',
            'sort_by': [],
            'window_size': 3,
            'window_type': 'sliding',
            'rank_field': 'rank',
            'partition_by': [],
            'save_to_var': None,
        }
