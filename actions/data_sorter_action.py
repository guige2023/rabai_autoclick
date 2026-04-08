"""Data sorter action module for RabAI AutoClick.

Provides data sorting operations for records and arrays including
multi-field sorting, ascending/descending order, and custom sort keys.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional, Union, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class MultiFieldSortAction(BaseAction):
    """Sort data by multiple fields with direction control.
    
    Sorts records by multiple fields in priority order,
    each with its own ascending/descending direction.
    """
    action_type = "multi_field_sort"
    display_name = "多字段排序"
    description = "按多个字段优先级排序"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Sort data by multiple fields.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, fields (list of
                   {field, order (asc|desc), type (str|int|float|date)}),
                   nulls_position (first|last).
        
        Returns:
            ActionResult with sorted data.
        """
        data = params.get('data', [])
        fields = params.get('fields', [])
        nulls_position = params.get('nulls_position', 'last')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if not fields:
            return ActionResult(
                success=False,
                message="At least one sort field is required"
            )

        def sort_key(row):
            keys = []
            for f in fields:
                field_name = f.get('field', '')
                order = f.get('order', 'asc')
                val = self._get_field(row, field_name)
                if val is None:
                    keys.append((0 if nulls_position == 'first' else 1, ''))
                else:
                    try:
                        if order == 'desc':
                            keys.append((0, self._to_sortable(val)))
                        else:
                            keys.append((0, self._to_sortable(val)))
                    except:
                        keys.append((1, ''))
            return keys

        sorted_data = sorted(data, key=sort_key)
        return ActionResult(
            success=True,
            message=f"Sorted {len(sorted_data)} records by {len(fields)} fields",
            data={
                'sorted': sorted_data,
                'count': len(sorted_data),
                'sort_fields': [f.get('field') for f in fields]
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

    def _to_sortable(self, value: Any) -> Any:
        if isinstance(value, str):
            return value.lower()
        return value


class NumericSortAction(BaseAction):
    """Sort data by numeric field values.
    
    Sorts records in ascending or descending order
    based on a numeric field.
    """
    action_type = "numeric_sort"
    display_name = "数值排序"
    description = "按数值字段排序"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Sort data numerically.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, field, order (asc|desc).
        
        Returns:
            ActionResult with sorted data.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        order = params.get('order', 'asc')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        def numeric_key(row):
            val = self._get_field(row, field)
            try:
                return float(val) if val is not None else float('-inf')
            except (TypeError, ValueError):
                return float('-inf')

        reverse = order == 'desc'
        sorted_data = sorted(data, key=numeric_key, reverse=reverse)

        return ActionResult(
            success=True,
            message=f"Numeric sort: {len(sorted_data)} records (order={order})",
            data={
                'sorted': sorted_data,
                'count': len(sorted_data),
                'field': field,
                'order': order
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


class AlphaSortAction(BaseAction):
    """Sort data alphabetically by string field.
    
    Sorts records in alphabetical (lexicographic) order
    with case-insensitive option.
    """
    action_type = "alpha_sort"
    display_name = "字母排序"
    description = "按字母顺序排序"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Sort data alphabetically.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, field, order (asc|desc),
                   case_sensitive.
        
        Returns:
            ActionResult with sorted data.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        order = params.get('order', 'asc')
        case_sensitive = params.get('case_sensitive', False)
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        def alpha_key(row):
            val = self._get_field(row, field)
            if val is None:
                return ''
            s = str(val)
            return s if case_sensitive else s.lower()

        reverse = order == 'desc'
        sorted_data = sorted(data, key=alpha_key, reverse=reverse)

        return ActionResult(
            success=True,
            message=f"Alphabetical sort: {len(sorted_data)} records",
            data={
                'sorted': sorted_data,
                'count': len(sorted_data),
                'field': field,
                'order': order
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


class DateSortAction(BaseAction):
    """Sort data by date/datetime field.
    
    Parses date strings and sorts chronologically
    in ascending or descending order.
    """
    action_type = "date_sort"
    display_name = "日期排序"
    description = "按日期字段排序"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Sort data by date.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, field, order (asc|desc),
                   date_format, parse_mode (auto|strptime).
        
        Returns:
            ActionResult with sorted data.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        order = params.get('order', 'asc')
        date_format = params.get('date_format', '%Y-%m-%d')
        parse_mode = params.get('parse_mode', 'auto')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        import datetime

        def date_key(row):
            val = self._get_field(row, field)
            if val is None:
                return datetime.datetime.min
            if isinstance(val, datetime.datetime):
                return val
            if isinstance(val, (int, float)):
                return datetime.datetime.fromtimestamp(val)
            try:
                if parse_mode == 'strptime':
                    return datetime.datetime.strptime(str(val), date_format)
                return self._parse_flexible_date(str(val))
            except:
                return datetime.datetime.min

        reverse = order == 'desc'
        sorted_data = sorted(data, key=date_key, reverse=reverse)

        return ActionResult(
            success=True,
            message=f"Date sort: {len(sorted_data)} records",
            data={
                'sorted': sorted_data,
                'count': len(sorted_data),
                'field': field,
                'order': order
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

    def _parse_flexible_date(self, s: str):
        import datetime
        formats = [
            '%Y-%m-%d', '%Y/%m/%d', '%d-%m-%Y', '%d/%m/%Y',
            '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M:%S.%f',
        ]
        for fmt in formats:
            try:
                return datetime.datetime.strptime(s, fmt)
            except ValueError:
                pass
        return datetime.datetime.min


class CustomSortAction(BaseAction):
    """Sort data using custom sort function.
    
    Accepts a Python lambda or function to define
    custom sorting logic.
    """
    action_type = "custom_sort"
    display_name = "自定义排序"
    description = "使用自定义函数排序"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Sort data with custom function.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, sort_func (code string
                   or callable), reverse.
        
        Returns:
            ActionResult with sorted data.
        """
        data = params.get('data', [])
        sort_code = params.get('sort_func', '')
        reverse = params.get('reverse', False)
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        try:
            if isinstance(sort_code, str):
                sort_fn = eval(sort_code)
            else:
                sort_fn = sort_code

            sorted_data = sorted(data, key=sort_fn, reverse=reverse)
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Custom sort error: {str(e)}"
            )

        return ActionResult(
            success=True,
            message=f"Custom sort: {len(sorted_data)} records",
            data={
                'sorted': sorted_data,
                'count': len(sorted_data)
            },
            duration=time.time() - start_time
        )


class ShuffleAction(BaseAction):
    """Randomly shuffle data order.
    
    Randomizes the order of records in the dataset
    with optional seed for reproducibility.
    """
    action_type = "shuffle"
    display_name = "随机打乱"
    description = "随机打乱数据顺序"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Shuffle data randomly.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, seed (optional int).
        
        Returns:
            ActionResult with shuffled data.
        """
        import random

        data = params.get('data', [])
        seed = params.get('seed')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if seed is not None:
            random.seed(seed)
        else:
            random.seed()

        shuffled = list(data)
        random.shuffle(shuffled)

        return ActionResult(
            success=True,
            message=f"Shuffled {len(shuffled)} records" + (f" (seed={seed})" if seed is not None else ""),
            data={
                'shuffled': shuffled,
                'count': len(shuffled),
                'seed': seed
            },
            duration=time.time() - start_time
        )
