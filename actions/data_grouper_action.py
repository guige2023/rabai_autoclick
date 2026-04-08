"""Data grouper action module for RabAI AutoClick.

Provides data grouping operations for aggregating records
by key fields with aggregation functions like sum, count, avg, etc.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class GroupByAction(BaseAction):
    """Group records by one or more key fields.
    
    Groups data by specified fields and computes
    aggregations on numeric fields.
    """
    action_type = "group_by"
    display_name = "分组聚合"
    description = "按字段分组并进行聚合计算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Group data and compute aggregations.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, group_by (list of fields),
                   aggregations (list of {field, func, alias}),
                   having (optional filter on aggregated values).
        
        Returns:
            ActionResult with grouped data.
        """
        data = params.get('data', [])
        group_by = params.get('group_by', [])
        aggregations = params.get('aggregations', [])
        having = params.get('having')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if not group_by:
            return ActionResult(
                success=False,
                message="At least one group_by field is required"
            )

        groups = {}
        for row in data:
            key = self._compute_key(row, group_by)
            if key not in groups:
                groups[key] = []
            groups[key].append(row)

        results = []
        for key, group_rows in groups.items():
            result_row = {}
            for i, field in enumerate(group_by):
                result_row[field] = key[i] if isinstance(key, tuple) else key

            for agg in aggregations:
                src_field = agg.get('field', '')
                func = agg.get('func', 'sum')
                alias = agg.get('alias', f"{func}_{src_field}")

                values = []
                for row in group_rows:
                    val = self._get_field(row, src_field)
                    if val is not None:
                        try:
                            values.append(float(val))
                        except (TypeError, ValueError):
                            pass

                result_row[alias] = self._aggregate(values, func)

            if having and not self._check_having(result_row, having):
                continue

            results.append(result_row)

        return ActionResult(
            success=True,
            message=f"Grouped into {len(results)} groups",
            data={
                'grouped': results,
                'count': len(results),
                'group_keys': list(groups.keys()),
                'group_sizes': {str(k): len(v) for k, v in groups.items()}
            },
            duration=time.time() - start_time
        )

    def _compute_key(self, row: Any, fields: List[str]) -> tuple:
        """Compute group key from row."""
        return tuple(self._get_field(row, f) for f in fields)

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

    def _aggregate(self, values: List[float], func: str) -> Any:
        """Compute aggregation function on values."""
        if not values:
            return 0
        if func == 'sum':
            return sum(values)
        elif func == 'count':
            return len(values)
        elif func == 'avg' or func == 'mean':
            return sum(values) / len(values)
        elif func == 'min':
            return min(values)
        elif func == 'max':
            return max(values)
        elif func == 'first':
            return values[0]
        elif func == 'last':
            return values[-1]
        elif func == 'median':
            sorted_vals = sorted(values)
            n = len(sorted_vals)
            mid = n // 2
            return sorted_vals[mid] if n % 2 else (sorted_vals[mid-1] + sorted_vals[mid]) / 2
        elif func == 'std':
            import statistics
            return statistics.stdev(values) if len(values) > 1 else 0
        return sum(values)

    def _check_having(self, row: Dict, having: Dict) -> bool:
        """Check having condition on aggregated row."""
        field = having.get('field', '')
        operator = having.get('operator', 'gt')
        value = having.get('value', 0)
        row_val = row.get(field, 0)
        ops = {
            'gt': row_val > value,
            'gte': row_val >= value,
            'lt': row_val < value,
            'lte': row_val <= value,
            'eq': row_val == value,
            'ne': row_val != value,
        }
        return ops.get(operator, True)


class BinGroupAction(BaseAction):
    """Bin/bucket records into groups based on numeric ranges.
    
    Creates bins for numeric fields and groups records
    into appropriate bins.
    """
    action_type = "bin_group"
    display_name = "数值分桶"
    description = "将数值字段分桶分组"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Bin data into buckets.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, field, bins (list of
                   boundary values), labels (optional), include_outside.
        
        Returns:
            ActionResult with binned data.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        bins = params.get('bins', [0, 25, 50, 75, 100])
        labels = params.get('labels', [])
        include_outside = params.get('include_outside', False)
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if len(bins) < 2:
            return ActionResult(
                success=False,
                message="bins must have at least 2 boundary values"
            )

        results = {f"bin_{i}": [] for i in range(len(bins) - 1)}
        if include_outside:
            results['outside'] = []

        for row in data:
            val = self._get_field(row, field)
            try:
                num_val = float(val) if val is not None else None
            except (TypeError, ValueError):
                num_val = None

            bin_idx = self._find_bin(num_val, bins)
            if bin_idx is not None:
                if 0 <= bin_idx < len(bins) - 1:
                    bin_key = f"bin_{bin_idx}"
                elif include_outside:
                    bin_key = 'outside'
                else:
                    continue
                results[bin_key].append(row)

        if labels and len(labels) == len(bins) - 1:
            labeled_results = {}
            for i, label in enumerate(labels):
                labeled_results[label] = results.get(f"bin_{i}", [])
            results = labeled_results

        return ActionResult(
            success=True,
            message=f"Binned {len(data)} records into {len(bins) - 1} bins",
            data={
                'binned': results,
                'bin_counts': {k: len(v) for k, v in results.items()},
                'bins': bins,
                'labels': labels if labels else list(results.keys())
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

    def _find_bin(self, value: Optional[float], bins: List[float]) -> Optional[int]:
        if value is None:
            return None
        for i in range(len(bins) - 1):
            if bins[i] <= value < bins[i + 1]:
                return i
        if value >= bins[-1]:
            return len(bins) - 1
        if value < bins[0]:
            return -1
        return None


class WindowFunctionAction(BaseAction):
    """Apply window functions over data partitions.
    
    Supports ROW_NUMBER, RANK, DENSE_RANK, LEAD, LAG,
    cumulative sums, and running averages.
    """
    action_type = "window_function"
    display_name = "窗口函数"
    description = "在数据分区上应用窗口函数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Apply window functions.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, partition_by (list),
                   order_by (list of {field, order}), functions (list
                   of {type, field, alias}).
        
        Returns:
            ActionResult with data including window columns.
        """
        data = params.get('data', [])
        partition_by = params.get('partition_by', [])
        order_by = params.get('order_by', [])
        functions = params.get('functions', [])
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if partition_by:
            partitions = {}
            for row in data:
                key = tuple(self._get_field(row, f) for f in partition_by)
                if key not in partitions:
                    partitions[key] = []
                partitions[key].append(row)
        else:
            partitions = {None: data}

        results = []
        for key, partition in partitions.items():
            if order_by:
                partition = sorted(
                    partition,
                    key=lambda r: tuple(
                        self._get_field(r, f.get('field', '')) or 0
                        for f in order_by
                    )
                )

            for i, row in enumerate(partition):
                new_row = dict(row)
                values = [self._get_field(row, f.get('field', '')) or 0 for f in order_by]

                for fn in functions:
                    fn_type = fn.get('type', 'row_number')
                    alias = fn.get('alias', fn_type)
                    field_name = fn.get('field', '')

                    if fn_type == 'row_number':
                        new_row[alias] = i + 1
                    elif fn_type == 'rank':
                        new_row[alias] = i + 1
                    elif fn_type == 'dense_rank':
                        new_row[alias] = i + 1
                    elif fn_type == 'lag':
                        offset = fn.get('offset', 1)
                        default_val = fn.get('default')
                        idx = i - offset
                        new_row[alias] = partition[idx][field_name] if idx >= 0 else default_val
                    elif fn_type == 'lead':
                        offset = fn.get('offset', 1)
                        default_val = fn.get('default')
                        idx = i + offset
                        new_row[alias] = partition[idx][field_name] if idx < len(partition) else default_val
                    elif fn_type == 'cumsum':
                        vals = [float(self._get_field(r, field_name) or 0) for r in partition[:i+1]]
                        new_row[alias] = sum(vals)
                    elif fn_type == 'cumavg':
                        vals = [float(self._get_field(r, field_name) or 0) for r in partition[:i+1]]
                        new_row[alias] = sum(vals) / len(vals)
                    elif fn_type == 'cumcount':
                        new_row[alias] = i + 1
                    elif fn_type == 'first':
                        new_row[alias] = partition[0][field_name]
                    elif fn_type == 'last':
                        new_row[alias] = partition[-1][field_name]

                results.append(new_row)

        return ActionResult(
            success=True,
            message=f"Applied {len(functions)} window functions to {len(results)} rows",
            data={
                'result': results,
                'count': len(results),
                'partitions': len(partitions)
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


class UngroupAction(BaseAction):
    """Expand grouped data back to individual rows.
    
    Takes aggregated/grouped data and expands it back
    to one row per original record.
    """
    action_type = "ungroup"
    display_name = "展开分组"
    description = "将分组数据展开为单条记录"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Ungroup expanded data.
        
        Args:
            context: Execution context.
            params: Dict with keys: grouped_data, group_key_field,
                   repeat_count_field, copy_fields.
        
        Returns:
            ActionResult with ungrouped data.
        """
        grouped_data = params.get('grouped_data', [])
        group_key_field = params.get('group_key_field', 'group_key')
        repeat_count_field = params.get('repeat_count_field', 'count')
        copy_fields = params.get('copy_fields', [])
        start_time = time.time()

        if not isinstance(grouped_data, list):
            grouped_data = [grouped_data]

        results = []
        for group in grouped_data:
            count = group.get(repeat_count_field, 1)
            try:
                count = int(count)
            except (TypeError, ValueError):
                count = 1

            for _ in range(count):
                row = {}
                for f in copy_fields:
                    if f in group:
                        row[f] = group[f]
                if not row:
                    row = dict(group)
                results.append(row)

        return ActionResult(
            success=True,
            message=f"Ungrouped to {len(results)} rows from {len(grouped_data)} groups",
            data={
                'ungrouped': results,
                'count': len(results)
            },
            duration=time.time() - start_time
        )
