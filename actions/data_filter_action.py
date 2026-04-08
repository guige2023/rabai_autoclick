"""Data filter action module for RabAI AutoClick.

Provides data filtering operations including field filtering,
row filtering, conditional filtering, deduplication, and
advanced query-like filtering operations.
"""

import time
import re
import sys
import os
from typing import Any, Dict, List, Optional, Union, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RowFilterAction(BaseAction):
    """Filter rows/records based on field conditions.
    
    Supports equality, comparison, contains, regex,
    and custom filter functions.
    """
    action_type = "row_filter"
    display_name = "行过滤"
    description = "根据字段条件过滤行/记录"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Filter rows based on conditions.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, filters (list of filter dicts),
                   filter_mode (all|any), invert.
        
        Returns:
            ActionResult with filtered data.
        """
        data = params.get('data', [])
        filters = params.get('filters', [])
        filter_mode = params.get('filter_mode', 'all')
        invert = params.get('invert', False)
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if not filters:
            return ActionResult(
                success=True,
                message="No filters applied, returning all data",
                data={'filtered': data, 'count': len(data), 'removed': 0}
            )

        filtered = []
        removed = 0

        for row in data:
            matches = self._row_matches_filters(row, filters, filter_mode)
            if invert:
                matches = not matches

            if matches:
                filtered.append(row)
            else:
                removed += 1

        return ActionResult(
            success=True,
            message=f"Filtered: {len(filtered)} rows retained, {removed} removed",
            data={
                'filtered': filtered,
                'count': len(filtered),
                'removed': removed,
                'original_count': len(data)
            },
            duration=time.time() - start_time
        )

    def _row_matches_filters(
        self,
        row: Any,
        filters: List[Dict[str, Any]],
        mode: str
    ) -> bool:
        """Check if row matches filter conditions."""
        results = []
        for f in filters:
            result = self._matches_filter(row, f)
            results.append(result)

        if mode == 'all':
            return all(results)
        else:
            return any(results)

    def _matches_filter(self, row: Any, filter_def: Dict[str, Any]) -> bool:
        """Check if single row matches one filter definition."""
        field = filter_def.get('field', '')
        operator = filter_def.get('operator', 'eq')
        value = filter_def.get('value')
        case_sensitive = filter_def.get('case_sensitive', True)

        field_value = self._get_field(row, field)

        if operator == 'eq':
            return self._compare_eq(field_value, value, case_sensitive)
        elif operator == 'ne':
            return not self._compare_eq(field_value, value, case_sensitive)
        elif operator == 'gt':
            return field_value > value if field_value is not None else False
        elif operator == 'gte':
            return field_value >= value if field_value is not None else False
        elif operator == 'lt':
            return field_value < value if field_value is not None else False
        elif operator == 'lte':
            return field_value <= value if field_value is not None else False
        elif operator == 'in':
            return field_value in value if field_value is not None else False
        elif operator == 'not_in':
            return field_value not in value if field_value is not None else True
        elif operator == 'contains':
            return str(value) in str(field_value) if field_value is not None else False
        elif operator == 'not_contains':
            return str(value) not in str(field_value) if field_value is not None else True
        elif operator == 'starts_with':
            s = str(field_value or '')
            v = str(value or '')
            return s.startswith(v) if case_sensitive else s.lower().startswith(v.lower())
        elif operator == 'ends_with':
            s = str(field_value or '')
            v = str(value or '')
            return s.endswith(v) if case_sensitive else s.lower().endswith(v.lower())
        elif operator == 'regex':
            try:
                flags = 0 if case_sensitive else re.IGNORECASE
                return bool(re.search(str(value), str(field_value or ''), flags))
            except:
                return False
        elif operator == 'is_null':
            return field_value is None or field_value == ''
        elif operator == 'is_not_null':
            return field_value is not None and field_value != ''
        elif operator == 'exists':
            return field_value is not None
        return True

    def _compare_eq(self, a: Any, b: Any, case_sensitive: bool) -> bool:
        """Compare two values for equality."""
        if a is None:
            return b is None or b == ''
        if isinstance(a, str) and isinstance(b, str):
            return a == b if case_sensitive else a.lower() == b.lower()
        return a == b

    def _get_field(self, row: Any, field: str) -> Any:
        """Get field value from row using dot notation."""
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


class ColumnFilterAction(BaseAction):
    """Filter/select specific columns from records.
    
    Supports include columns, exclude columns, and
    rename columns during filtering.
    """
    action_type = "column_filter"
    display_name = "列过滤"
    description = "选择或排除特定列"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Filter columns from records.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, include (list),
                   exclude (list), rename (dict).
        
        Returns:
            ActionResult with column-filtered data.
        """
        data = params.get('data', [])
        include = params.get('include', [])
        exclude = params.get('exclude', [])
        rename = params.get('rename', {})
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if not include and not exclude and not rename:
            return ActionResult(
                success=True,
                message="No column operations specified",
                data={'filtered': data, 'columns': list(data[0].keys()) if data and isinstance(data[0], dict) else []}
            )

        result = []
        all_columns = set()

        for row in data:
            if not isinstance(row, dict):
                continue

            new_row = {}
            columns = include if include else row.keys()

            for col in columns:
                if col in exclude:
                    continue
                if col in row:
                    new_col = rename.get(col, col)
                    new_row[new_col] = row[col]
                    all_columns.add(new_col)

            result.append(new_row)

        return ActionResult(
            success=True,
            message=f"Column filter: {len(all_columns)} columns in result",
            data={
                'filtered': result,
                'columns': list(all_columns),
                'count': len(result)
            },
            duration=time.time() - start_time
        )


class DeduplicateFilterAction(BaseAction):
    """Remove duplicate records from data.
    
    Supports deduplication by specific fields or all fields,
    with options for keeping first or last occurrence.
    """
    action_type = "deduplicate_filter"
    display_name = "去重过滤"
    description = "移除重复记录"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Deduplicate data.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, fields (list of field names
                   for comparison), keep (first|last), case_sensitive.
        
        Returns:
            ActionResult with deduplicated data.
        """
        data = params.get('data', [])
        fields = params.get('fields', [])
        keep = params.get('keep', 'first')
        case_sensitive = params.get('case_sensitive', True)
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if not data:
            return ActionResult(
                success=True,
                message="No data to deduplicate",
                data={'deduplicated': [], 'count': 0, 'removed': 0}
            )

        seen = []
        result = []
        removed = 0

        if keep == 'last':
            data = list(reversed(data))

        for row in data:
            key = self._compute_key(row, fields, case_sensitive)
            if key not in seen:
                seen.append(key)
                result.append(row)
            else:
                removed += 1

        if keep == 'last':
            result = list(reversed(result))

        return ActionResult(
            success=True,
            message=f"Deduplicated: {len(result)} unique records, {removed} duplicates removed",
            data={
                'deduplicated': result,
                'count': len(result),
                'removed': removed,
                'original_count': len(data)
            },
            duration=time.time() - start_time
        )

    def _compute_key(
        self,
        row: Any,
        fields: List[str],
        case_sensitive: bool
    ) -> tuple:
        """Compute deduplication key from row."""
        if not fields:
            if isinstance(row, dict):
                items = sorted(row.items())
                if not case_sensitive:
                    items = [(k.lower() if isinstance(k, str) else k, v) for k, v in items]
                return tuple(items)
            return tuple(sorted(str(row).split())) if isinstance(row, (list, str)) else (row,)

        key_parts = []
        for f in fields:
            val = self._get_field(row, f)
            if not case_sensitive and isinstance(val, str):
                val = val.lower()
            key_parts.append(val)
        return tuple(key_parts)

    def _get_field(self, row: Any, field: str) -> Any:
        """Get field value from row using dot notation."""
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


class RangeFilterAction(BaseAction):
    """Filter data by numeric or date range.
    
    Keeps records where field value falls within
    specified min/max range.
    """
    action_type = "range_filter"
    display_name = "范围过滤"
    description = "按数值或日期范围过滤"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Filter data by range.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, field, min, max, inclusive.
        
        Returns:
            ActionResult with range-filtered data.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        min_val = params.get('min')
        max_val = params.get('max')
        inclusive = params.get('inclusive', True)
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        filtered = []
        for row in data:
            value = self._get_field(row, field)
            if value is None:
                continue

            try:
                val_num = float(value)
                min_ok = (val_num >= min_val) if min_val is not None else True
                max_ok = (val_num <= max_val) if max_val is not None else True
                if inclusive:
                    if min_ok and max_ok:
                        filtered.append(row)
                else:
                    if val_num > min_val and val_num < max_val:
                        filtered.append(row)
            except (TypeError, ValueError):
                continue

        return ActionResult(
            success=True,
            message=f"Range filter: {len(filtered)}/{len(data)} records in range",
            data={
                'filtered': filtered,
                'count': len(filtered),
                'removed': len(data) - len(filtered)
            },
            duration=time.time() - start_time
        )

    def _get_field(self, row: Any, field: str) -> Any:
        """Get field value from row using dot notation."""
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


class CustomFilterAction(BaseAction):
    """Apply custom filter function to data.
    
    Accepts a Python lambda or function reference to
    apply custom filtering logic.
    """
    action_type = "custom_filter"
    display_name = "自定义过滤"
    description = "使用自定义函数过滤数据"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Apply custom filter.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, filter_func (code string
                   or callable), invert.
        
        Returns:
            ActionResult with filtered data.
        """
        data = params.get('data', [])
        filter_code = params.get('filter_func', '')
        invert = params.get('invert', False)
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if not filter_code and not callable(filter_code):
            return ActionResult(
                success=False,
                message="filter_func is required"
            )

        filtered = []
        removed = 0

        try:
            if isinstance(filter_code, str):
                filter_fn = eval(filter_code)
            else:
                filter_fn = filter_code

            for row in data:
                try:
                    matches = bool(filter_fn(row))
                    if invert:
                        matches = not matches
                    if matches:
                        filtered.append(row)
                    else:
                        removed += 1
                except Exception:
                    continue
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Custom filter error: {str(e)}"
            )

        return ActionResult(
            success=True,
            message=f"Custom filter: {len(filtered)} retained, {removed} removed",
            data={
                'filtered': filtered,
                'count': len(filtered),
                'removed': removed
            },
            duration=time.time() - start_time
        )


class TopNFilterAction(BaseAction):
    """Filter to top N or bottom N records.
    
    Sorts by specified field and returns top/bottom N records.
    """
    action_type = "top_n_filter"
    display_name = "TopN过滤"
    description = "返回排序后的前N条或后N条记录"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Filter to top N or bottom N.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, field, n, order (asc|desc),
                   keep_bottom.
        
        Returns:
            ActionResult with filtered data.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        n = params.get('n', 10)
        order = params.get('order', 'desc')
        keep_bottom = params.get('keep_bottom', False)
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if not field:
            return ActionResult(
                success=False,
                message="field is required for TopN filter"
            )

        reverse = (order == 'desc') ^ keep_bottom
        sorted_data = sorted(data, key=lambda x: self._get_field(x, field) or 0, reverse=reverse)
        result = sorted_data[:n]

        return ActionResult(
            success=True,
            message=f"TopN filter: {len(result)} records (order={order}, n={n})",
            data={
                'filtered': result,
                'count': len(result),
                'n': n,
                'order': order
            },
            duration=time.time() - start_time
        )

    def _get_field(self, row: Any, field: str) -> Any:
        """Get field value from row using dot notation."""
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


class NullFilterAction(BaseAction):
    """Filter records based on null/empty field values.
    
    Keeps or removes records that have null/empty values
    in specified fields.
    """
    action_type = "null_filter"
    display_name = "空值过滤"
    description = "根据空值过滤记录"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Filter by null values.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, fields (list),
                   mode (remove_nulls|keep_nulls|keep_non_nulls).
        
        Returns:
            ActionResult with filtered data.
        """
        data = params.get('data', [])
        fields = params.get('fields', [])
        mode = params.get('mode', 'remove_nulls')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        filtered = []
        removed = 0

        for row in data:
            has_null = any(self._is_null(self._get_field(row, f)) for f in fields)

            if mode == 'remove_nulls' and has_null:
                removed += 1
                continue
            if mode == 'keep_nulls' and not has_null:
                removed += 1
                continue
            if mode == 'keep_non_nulls' and has_null:
                removed += 1
                continue

            filtered.append(row)

        return ActionResult(
            success=True,
            message=f"Null filter ({mode}): {len(filtered)} retained, {removed} removed",
            data={
                'filtered': filtered,
                'count': len(filtered),
                'removed': removed
            },
            duration=time.time() - start_time
        )

    def _is_null(self, value: Any) -> bool:
        """Check if value is null or empty."""
        return value is None or value == '' or value == []

    def _get_field(self, row: Any, field: str) -> Any:
        """Get field value from row using dot notation."""
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
