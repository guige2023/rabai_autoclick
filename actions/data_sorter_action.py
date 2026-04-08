"""
Data Sorter Action Module.

Sorts data by single or multiple columns with support for
ascending/descending order, natural sorting, and case-insensitive sorting.

Author: RabAi Team
"""

from __future__ import annotations

import sys
import os
import time
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SortOrder(Enum):
    """Sort order."""
    ASC = "asc"
    DESC = "desc"


@dataclass
class SortKey:
    """A sort key definition."""
    field: str
    order: SortOrder = SortOrder.ASC
    type: str = "auto"
    nulls_first: bool = False


class DataSorterAction(BaseAction):
    """Data sorter action.
    
    Sorts data by single or multiple columns with various
    sorting strategies and null handling.
    """
    action_type = "data_sorter"
    display_name = "数据排序"
    description = "多字段数据排序"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Sort data.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - data: Data to sort
                - operation: sort/top/bottom/rank
                - sort_by: Field name or list of SortKey dicts
                - order: asc or desc (for single field)
                - case_sensitive: Case sensitive string comparison
                - natural_sort: Enable natural sorting (1, 2, 10 instead of 1, 10, 2)
                - nulls_first: Put null values first
                - limit: Number of top/bottom records to return
                
        Returns:
            ActionResult with sorted data.
        """
        start_time = time.time()
        
        operation = params.get("operation", "sort")
        data = params.get("data", [])
        sort_by = params.get("sort_by", [])
        order = params.get("order", "asc")
        case_sensitive = params.get("case_sensitive", False)
        natural_sort = params.get("natural_sort", False)
        nulls_first = params.get("nulls_first", False)
        limit = params.get("limit")
        
        try:
            if operation == "sort":
                result = self._sort_data(data, sort_by, order, case_sensitive, natural_sort, nulls_first, start_time)
            elif operation == "top":
                result = self._top_records(data, sort_by, order, limit, case_sensitive, natural_sort, nulls_first, start_time)
            elif operation == "bottom":
                result = self._bottom_records(data, sort_by, order, limit, case_sensitive, natural_sort, nulls_first, start_time)
            elif operation == "rank":
                result = self._rank_data(data, sort_by, order, case_sensitive, start_time)
            elif operation == "shuffle":
                result = self._shuffle_data(data, start_time)
            elif operation == "reverse":
                result = self._reverse_data(data, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
            
            return result
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Sort failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _parse_sort_keys(self, sort_by: Any, default_order: str) -> List[SortKey]:
        """Parse sort_by into list of SortKey objects."""
        sort_keys = []
        
        if isinstance(sort_by, str):
            sort_keys.append(SortKey(
                field=sort_by,
                order=SortOrder(default_order)
            ))
        elif isinstance(sort_by, list):
            for item in sort_by:
                if isinstance(item, dict):
                    sort_keys.append(SortKey(
                        field=item.get("field", ""),
                        order=SortOrder(item.get("order", default_order)),
                        type=item.get("type", "auto"),
                        nulls_first=item.get("nulls_first", False)
                    ))
                elif isinstance(item, str):
                    sort_keys.append(SortKey(field=item, order=SortOrder(default_order)))
        elif isinstance(sort_by, dict):
            sort_keys.append(SortKey(
                field=sort_by.get("field", ""),
                order=SortOrder(sort_by.get("order", default_order)),
                type=sort_by.get("type", "auto"),
                nulls_first=sort_by.get("nulls_first", False)
            ))
        
        return sort_keys
    
    def _natural_key(self, s: str) -> List:
        """Generate a key for natural sorting."""
        def convert(text):
            return int(text) if text.isdigit() else text.lower()
        return [convert(c) for c in re.split(r'(\d+)', str(s))]
    
    def _get_value(self, record: Any, field: str) -> Any:
        """Get value from record by field path."""
        if field is None or field == "":
            return record
        
        if isinstance(record, dict):
            parts = field.split(".")
            value = record
            for part in parts:
                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    return None
            return value
        elif isinstance(record, (list, tuple)) and field.isdigit():
            idx = int(field)
            return record[idx] if 0 <= idx < len(record) else None
        else:
            return getattr(record, field, None) if hasattr(record, field) else None
    
    def _compare_values(self, v1: Any, v2: Any, field_type: str, case_sensitive: bool) -> int:
        """Compare two values for sorting."""
        if v1 is None and v2 is None:
            return 0
        if v1 is None:
            return -1
        if v2 is None:
            return 1
        
        if field_type == "number":
            try:
                n1, n2 = float(v1), float(v2)
                return (n1 > n2) - (n1 < n2)
            except (ValueError, TypeError):
                return (str(v1) > str(v2)) - (str(v1) < str(v2))
        
        elif field_type == "string":
            s1 = str(v1) if case_sensitive else str(v1).lower()
            s2 = str(v2) if case_sensitive else str(v2).lower()
            return (s1 > s2) - (s1 < s2)
        
        elif field_type == "date":
            try:
                t1 = time.mktime(time.strptime(str(v1), "%Y-%m-%d"))
                t2 = time.mktime(time.strptime(str(v2), "%Y-%m-%d"))
                return (t1 > t2) - (t1 < t2)
            except Exception:
                return 0
        
        else:
            if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                return (v1 > v2) - (v1 < v2)
            s1 = str(v1) if case_sensitive else str(v1).lower()
            s2 = str(v2) if case_sensitive else str(v2).lower()
            return (s1 > s2) - (s1 < s2)
    
    def _sort_data(
        self, data: List, sort_by: Any, default_order: str,
        case_sensitive: bool, natural_sort: bool, nulls_first: bool, start_time: float
    ) -> ActionResult:
        """Sort data by specified keys."""
        if not data:
            return ActionResult(
                success=True,
                message="No data to sort",
                data={"data": []},
                duration=time.time() - start_time
            )
        
        sort_keys = self._parse_sort_keys(sort_by, default_order)
        
        if not sort_keys:
            return ActionResult(
                success=True,
                message="No sort keys specified",
                data={"data": list(data)},
                duration=time.time() - start_time
            )
        
        def sort_key(record: Any) -> Tuple:
            keys = []
            for sk in sort_keys:
                value = self._get_value(record, sk.field)
                
                if nulls_first:
                    if value is None:
                        keys.append((True, 0))
                        continue
                
                if natural_sort and isinstance(value, str):
                    keys.append((False, self._natural_key(value)))
                else:
                    normalized = value
                    if sk.order == SortOrder.DESC and value is not None:
                        if isinstance(value, str):
                            normalized = value.lower() if not case_sensitive else value
                        elif isinstance(value, (int, float)):
                            normalized = -value
                    
                    keys.append((False, normalized))
            
            return tuple(keys)
        
        sorted_data = sorted(data, key=sort_key)
        
        sort_fields = [sk.field for sk in sort_keys]
        sort_orders = [sk.order.value for sk in sort_keys]
        
        return ActionResult(
            success=True,
            message=f"Sorted {len(sorted_data)} records",
            data={
                "data": sorted_data,
                "sort_by": sort_fields,
                "order": sort_orders,
                "natural_sort": natural_sort,
                "nulls_first": nulls_first
            },
            duration=time.time() - start_time
        )
    
    def _top_records(
        self, data: List, sort_by: Any, default_order: str,
        limit: Optional[int], case_sensitive: bool, natural_sort: bool,
        nulls_first: bool, start_time: float
    ) -> ActionResult:
        """Get top N records by sort order."""
        sort_result = self._sort_data(data, sort_by, "desc", case_sensitive, natural_sort, nulls_first, start_time)
        
        if not sort_result.success:
            return sort_result
        
        sorted_data = sort_result.data["data"]
        
        if limit:
            sorted_data = sorted_data[:limit]
        
        return ActionResult(
            success=True,
            message=f"Returned top {len(sorted_data)} records",
            data={"data": sorted_data, "limit": limit},
            duration=time.time() - start_time
        )
    
    def _bottom_records(
        self, data: List, sort_by: Any, default_order: str,
        limit: Optional[int], case_sensitive: bool, natural_sort: bool,
        nulls_first: bool, start_time: float
    ) -> ActionResult:
        """Get bottom N records by sort order."""
        sort_result = self._sort_data(data, sort_by, "asc", case_sensitive, natural_sort, nulls_first, start_time)
        
        if not sort_result.success:
            return sort_result
        
        sorted_data = sort_result.data["data"]
        
        if limit:
            sorted_data = sorted_data[:limit]
        
        return ActionResult(
            success=True,
            message=f"Returned bottom {len(sorted_data)} records",
            data={"data": sorted_data, "limit": limit},
            duration=time.time() - start_time
        )
    
    def _rank_data(
        self, data: List, sort_by: Any, default_order: str,
        case_sensitive: bool, start_time: float
    ) -> ActionResult:
        """Add rank column to data."""
        if not data:
            return ActionResult(success=True, message="No data", data={"data": []}, duration=time.time() - start_time)
        
        sort_keys = self._parse_sort_keys(sort_by, default_order)
        
        sorted_copy = sorted(data, key=lambda r: [
            self._get_value(r, sk.field) if sk.order == SortOrder.ASC else None
            for sk in sort_keys
        ])
        
        ranked = []
        current_rank = 1
        
        for i, record in enumerate(sorted_copy):
            new_record = dict(record)
            new_record["_rank"] = current_rank
            
            if i > 0:
                prev_record = sorted_copy[i - 1]
                same = all(
                    self._get_value(record, sk.field) == self._get_value(prev_record, sk.field)
                    for sk in sort_keys
                )
                if same:
                    new_record["_rank"] = ranked[-1]["_rank"]
                    new_record["_rank_group"] = ranked[-1]["_rank_group"]
                else:
                    current_rank = i + 1
                    new_record["_rank"] = current_rank
                    new_record["_rank_group"] = current_rank
            else:
                new_record["_rank_group"] = 1
            
            ranked.append(new_record)
        
        return ActionResult(
            success=True,
            message=f"Ranked {len(ranked)} records",
            data={"data": ranked, "sort_by": [sk.field for sk in sort_keys]},
            duration=time.time() - start_time
        )
    
    def _shuffle_data(self, data: List, start_time: float) -> ActionResult:
        """Shuffle data randomly."""
        import random
        shuffled = list(data)
        random.shuffle(shuffled)
        
        return ActionResult(
            success=True,
            message=f"Shuffled {len(shuffled)} records",
            data={"data": shuffled},
            duration=time.time() - start_time
        )
    
    def _reverse_data(self, data: List, start_time: float) -> ActionResult:
        """Reverse data order."""
        reversed_data = list(reversed(data))
        
        return ActionResult(
            success=True,
            message=f"Reversed {len(reversed_data)} records",
            data={"data": reversed_data},
            duration=time.time() - start_time
        )
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate sorter parameters."""
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return []
