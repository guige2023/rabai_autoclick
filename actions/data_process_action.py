"""Data Process Action Module.

Provides data processing utilities including sorting,
grouping, deduplication, and advanced transformations.
"""

import sys
import os
import json
from typing import Any, Dict, List, Optional, Callable, Tuple
from collections import defaultdict, OrderedDict
from itertools import groupby

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataSorterAction(BaseAction):
    """Sort data by one or more fields.
    
    Supports ascending/descending order, multi-field sorting, and custom comparators.
    """
    action_type = "data_sorter"
    display_name = "数据排序"
    description = "按一个或多个字段对数据排序"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Sort data by fields.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data list.
                - sort_by: Field name or list of field names.
                - order: 'asc' or 'desc' (or list for mixed).
                - case_sensitive: Case-sensitive string comparison.
                - nulls_first: Place null values at the beginning.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with sorted data or error.
        """
        data = params.get('data', [])
        sort_by = params.get('sort_by', [])
        order = params.get('order', 'asc')
        case_sensitive = params.get('case_sensitive', False)
        nulls_first = params.get('nulls_first', False)
        output_var = params.get('output_var', 'sorted')

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list for data, got {type(data).__name__}"
            )

        try:
            # Normalize sort_by to list
            if isinstance(sort_by, str):
                sort_by = [sort_by]

            # Normalize order to list
            if isinstance(order, str):
                order = [order] * len(sort_by)
            else:
                order = list(order)

            # Pad order list if needed
            while len(order) < len(sort_by):
                order.append('asc')

            # Sort data
            sorted_data = sorted(
                data,
                key=lambda x: self._get_sort_key(x, sort_by, case_sensitive, nulls_first),
                reverse=False
            )

            # Apply order (multi-level sort is already handled by key function)
            if all(o == 'desc' for o in order):
                sorted_data = list(reversed(sorted_data))

            context.variables[output_var] = sorted_data
            return ActionResult(
                success=True,
                data={'sorted': sorted_data, 'count': len(sorted_data)},
                message=f"Sorted {len(data)} items by {len(sort_by)} fields"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Data sort failed: {str(e)}"
            )

    def _get_sort_key(
        self,
        item: Any,
        sort_by: List[str],
        case_sensitive: bool,
        nulls_first: bool
    ) -> Tuple:
        """Generate sort key for an item."""
        keys = []
        for field in sort_by:
            if isinstance(item, dict):
                value = item.get(field)
            else:
                value = getattr(item, field, None)

            # Handle None values
            if value is None:
                keys.append((0 if nulls_first else 1, ''))
            else:
                # Normalize string case
                if isinstance(value, str) and not case_sensitive:
                    value = value.lower()
                keys.append((0, value))

        return tuple(keys)


class DataGrouperAction(BaseAction):
    """Group data by one or more fields.
    
    Supports aggregation during grouping and nested grouping.
    """
    action_type = "data_grouper"
    display_name = "数据分组"
    description = "按一个或多个字段对数据分组"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Group data by fields.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data list.
                - group_by: Field name or list of field names.
                - aggregations: Dict of field -> aggregation function.
                - preserve_order: Preserve original item order.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with grouped data or error.
        """
        data = params.get('data', [])
        group_by = params.get('group_by', [])
        aggregations = params.get('aggregations', {})
        preserve_order = params.get('preserve_order', False)
        output_var = params.get('output_var', 'grouped')

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list for data, got {type(data).__name__}"
            )

        try:
            # Normalize group_by to list
            if isinstance(group_by, str):
                group_by = [group_by]

            # Group data
            groups = defaultdict(list)
            for item in data:
                key = self._get_group_key(item, group_by)
                groups[key].append(item)

            # Apply aggregations
            result = []
            for key, items in groups.items():
                group_result = self._create_group_result(key, items, group_by, aggregations)
                result.append(group_result)

            # Sort by key if preserving order
            if preserve_order:
                result.sort(key=lambda x: x['_key'])

            context.variables[output_var] = result
            return ActionResult(
                success=True,
                data={'groups': result, 'group_count': len(result)},
                message=f"Grouped {len(data)} items into {len(result)} groups"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Data grouping failed: {str(e)}"
            )

    def _get_group_key(self, item: Any, group_by: List[str]) -> Tuple:
        """Get grouping key for an item."""
        key_parts = []
        for field in group_by:
            if isinstance(item, dict):
                key_parts.append(item.get(field))
            else:
                key_parts.append(getattr(item, field, None))
        return tuple(key_parts)

    def _create_group_result(
        self,
        key: Tuple,
        items: List,
        group_by: List[str],
        aggregations: Dict
    ) -> Dict:
        """Create result for a group."""
        result = {
            '_key': key,
            '_count': len(items),
            '_items': items
        }

        # Add key fields
        for i, field in enumerate(group_by):
            result[field] = key[i]

        # Apply aggregations
        for field, agg_func in aggregations.items():
            result[f'{field}_{agg_func}'] = self._aggregate_field(items, field, agg_func)

        return result

    def _aggregate_field(
        self, items: List, field: str, agg_func: str
    ) -> Any:
        """Aggregate a field across items."""
        values = []
        for item in items:
            if isinstance(item, dict) and field in item:
                values.append(item[field])
            elif hasattr(item, field):
                values.append(getattr(item, field))

        if not values:
            return None

        if agg_func == 'sum':
            return sum(values)
        elif agg_func == 'avg':
            return sum(values) / len(values)
        elif agg_func == 'count':
            return len(values)
        elif agg_func == 'min':
            return min(values)
        elif agg_func == 'max':
            return max(values)
        elif agg_func == 'first':
            return values[0]
        elif agg_func == 'last':
            return values[-1]
        elif agg_func == 'list':
            return values

        return values


class DataDeduplicatorAction(BaseAction):
    """Remove duplicate items from data.
    
    Supports exact and fuzzy deduplication based on field selection.
    """
    action_type = "data_deduplicator"
    display_name = "数据去重"
    description = "从数据中移除重复项"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Deduplicate data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data list.
                - dedupe_by: Fields to consider for deduplication.
                - strategy: 'exact', 'first', 'last', 'keep_all'.
                - fuzzy: Enable fuzzy matching.
                - threshold: Fuzzy match threshold.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with deduplicated data or error.
        """
        data = params.get('data', [])
        dedupe_by = params.get('dedupe_by', [])
        strategy = params.get('strategy', 'first')
        fuzzy = params.get('fuzzy', False)
        threshold = params.get('threshold', 0.9)
        output_var = params.get('output_var', 'deduped')

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list for data, got {type(data).__name__}"
            )

        try:
            if fuzzy:
                result = self._fuzzy_dedupe(data, dedupe_by, threshold)
            else:
                result = self._exact_dedupe(data, dedupe_by, strategy)

            context.variables[output_var] = result
            return ActionResult(
                success=True,
                data={'deduped': result, 'original_count': len(data), 'dedup_count': len(data) - len(result)},
                message=f"Deduplicated {len(data)} items to {len(result)} unique items"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Data deduplication failed: {str(e)}"
            )

    def _exact_dedupe(
        self, data: List, dedupe_by: List[str], strategy: str
    ) -> List:
        """Perform exact deduplication."""
        seen = OrderedDict()

        for item in data:
            if dedupe_by:
                key = self._get_item_key(item, dedupe_by)
            else:
                key = json.dumps(item, sort_keys=True) if isinstance(item, (dict, list)) else item

            if strategy == 'keep_all':
                if key not in seen:
                    seen[key] = []
                seen[key].append(item)
            else:
                if key not in seen:
                    seen[key] = item

        if strategy == 'keep_all':
            result = []
            for items in seen.values():
                result.extend(items)
            return result
        elif strategy == 'last':
            return list(seen.values())
        else:
            return list(seen.values())

    def _fuzzy_dedupe(
        self, data: List, dedupe_by: List[str], threshold: float
    ) -> List:
        """Perform fuzzy deduplication."""
        result = []
        result_keys = []

        for item in data:
            item_key = self._get_item_key(item, dedupe_by) if dedupe_by else str(item)

            is_duplicate = False
            for existing_key in result_keys:
                similarity = self._calculate_similarity(item_key, existing_key)
                if similarity >= threshold:
                    is_duplicate = True
                    break

            if not is_duplicate:
                result.append(item)
                result_keys.append(item_key)

        return result

    def _get_item_key(self, item: Any, fields: List[str]) -> Tuple:
        """Get deduplication key for an item."""
        key_parts = []
        for field in fields:
            if isinstance(item, dict):
                key_parts.append(item.get(field))
            else:
                key_parts.append(getattr(item, field, None))
        return tuple(key_parts)

    def _calculate_similarity(self, s1: str, s2: str) -> float:
        """Calculate string similarity."""
        if s1 == s2:
            return 1.0

        # Simple Jaccard similarity based on character n-grams
        def get_ngrams(s, n=2):
            return set(s[i:i+n] for i in range(len(s) - n + 1))

        if len(s1) < 2 or len(s2) < 2:
            return 0.0

        ngrams1 = get_ngrams(s1.lower())
        ngrams2 = get_ngrams(s2.lower())

        if not ngrams1 or not ngrams2:
            return 0.0

        intersection = len(ngrams1 & ngrams2)
        union = len(ngrams1 | ngrams2)

        return intersection / union if union > 0 else 0.0


class DataPivoterAction(BaseAction):
    """Pivot data from rows to columns.
    
    Supports aggregation during pivot and multiple value columns.
    """
    action_type = "data_pivoter"
    display_name = "数据透视"
    description = "将数据从行透视到列"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Pivot data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data list.
                - index: Field(s) to use as index.
                - columns: Field to pivot on.
                - values: Field to aggregate.
                - aggfunc: Aggregation function.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with pivoted data or error.
        """
        data = params.get('data', [])
        index = params.get('index', [])
        columns = params.get('columns', '')
        values = params.get('values', '')
        aggfunc = params.get('aggfunc', 'sum')
        output_var = params.get('output_var', 'pivoted')

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list for data, got {type(data).__name__}"
            )

        try:
            # Group by index
            groups = defaultdict(list)
            for item in data:
                if isinstance(index, list):
                    key = tuple(item.get(i) for i in index)
                else:
                    key = item.get(index)
                groups[key].append(item)

            # Pivot
            pivoted = []
            column_values = set()

            for key, items in groups.items():
                row = {}
                if isinstance(index, list):
                    for i, idx_field in enumerate(index):
                        row[idx_field] = key[i]
                else:
                    row[index] = key

                for item in items:
                    col_val = item.get(columns)
                    val = item.get(values)
                    column_values.add(col_val)

                    col_key = f"{col_val}"
                    if aggfunc == 'sum':
                        row[col_key] = row.get(col_key, 0) + (val or 0)
                    elif aggfunc == 'count':
                        row[col_key] = row.get(col_key, 0) + 1
                    elif aggfunc == 'first':
                        if col_key not in row:
                            row[col_key] = val
                    elif aggfunc == 'last':
                        row[col_key] = val
                    elif aggfunc == 'avg':
                        if col_key not in row:
                            row[col_key] = {'sum': 0, 'count': 0}
                        row[col_key]['sum'] += (val or 0)
                        row[col_key]['count'] += 1

                # Process avg
                for col_key in list(row.keys()):
                    if isinstance(row[col_key], dict) and 'sum' in row[col_key]:
                        row[col_key] = row[col_key]['sum'] / row[col_key]['count']

                pivoted.append(row)

            result = {
                'pivoted': pivoted,
                'columns': sorted(column_values),
                'row_count': len(pivoted)
            }

            context.variables[output_var] = result
            return ActionResult(
                success=True,
                data=result,
                message=f"Pivoted {len(data)} items into {len(pivoted)} rows and {len(column_values)} columns"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Data pivot failed: {str(e)}"
            )


class DataMergerAction(BaseAction):
    """Merge multiple data sources.
    
    Supports inner, left, right, and full outer joins.
    """
    action_type = "data_merger"
    display_name = "数据合并"
    description = "合并多个数据源"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Merge data sources.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - left: Left data source.
                - right: Right data source.
                - left_key: Key field in left source.
                - right_key: Key field in right source.
                - join_type: 'inner', 'left', 'right', 'full'.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with merged data or error.
        """
        left = params.get('left', [])
        right = params.get('right', [])
        left_key = params.get('left_key', 'id')
        right_key = params.get('right_key', 'id')
        join_type = params.get('join_type', 'inner')
        output_var = params.get('output_var', 'merged')

        if not isinstance(left, list) or not isinstance(right, list):
            return ActionResult(
                success=False,
                message="Both 'left' and 'right' must be lists"
            )

        try:
            # Index right by key
            right_index = {}
            for item in right:
                key = item.get(right_key) if isinstance(item, dict) else getattr(item, right_key, None)
                if key not in right_index:
                    right_index[key] = []
                right_index[key].append(item)

            # Perform join
            merged = []
            left_unmatched = []

            for left_item in left:
                left_key_val = left_item.get(left_key) if isinstance(left_item, dict) else getattr(left_item, left_key, None)
                right_items = right_index.get(left_key_val, [])

                if right_items:
                    for right_item in right_items:
                        merged.append(self._merge_items(left_item, right_item))
                else:
                    left_unmatched.append(left_item)
                    if join_type in ('left', 'full'):
                        merged.append(self._merge_items(left_item, {}))

            # Handle right unmatched for full/right joins
            if join_type in ('right', 'full'):
                right_key_vals = set(item.get(right_key) if isinstance(item, dict) else getattr(item, right_key, None) for item in right)
                for left_item in left:
                    left_key_val = left_item.get(left_key) if isinstance(left_item, dict) else getattr(left_item, left_key, None)
                    if left_key_val not in right_key_vals:
                        merged.append(self._merge_items({}, left_item))

            context.variables[output_var] = merged
            return ActionResult(
                success=True,
                data={'merged': merged, 'count': len(merged)},
                message=f"Merged {len(left)} left and {len(right)} right items into {len(merged)} results"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Data merge failed: {str(e)}"
            )

    def _merge_items(self, left: Dict, right: Dict) -> Dict:
        """Merge two items."""
        result = {}
        result.update(left)
        for key, value in right.items():
            if key not in result:
                result[key] = value
            elif key.endswith('_right'):
                result[key] = value
            else:
                result[f'{key}_right'] = value
        return result
