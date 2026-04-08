"""Data sorter action module for RabAI AutoClick.

Provides data sorting:
- DataSorter: General data sorter
- MultiKeySorter: Sort by multiple keys
- CustomSorter: Custom sort function
- StableSorter: Stable sorting
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SortOrder(Enum):
    """Sort order."""
    ASC = "asc"
    DESC = "desc"


@dataclass
class SortConfig:
    """Sort configuration."""
    field: str
    order: SortOrder = SortOrder.ASC
    numeric: bool = False
    nulls_first: bool = False


class DataSorter:
    """General data sorter."""

    def sort(
        self,
        data: List[Any],
        configs: List[SortConfig],
    ) -> List[Any]:
        """Sort data by configurations."""
        if not data or not configs:
            return list(data)

        def sort_key(item):
            values = []
            for config in configs:
                value = item.get(config.field) if isinstance(item, dict) else getattr(item, config.field, None)

                if value is None:
                    if config.nulls_first:
                        values.append(float("-inf") if config.order == SortOrder.ASC else float("inf"))
                    else:
                        values.append(float("inf") if config.order == SortOrder.ASC else float("-inf"))
                elif config.numeric and isinstance(value, (int, float)):
                    values.append(value)
                elif isinstance(value, str):
                    try:
                        values.append(float(value))
                    except ValueError:
                        values.append(value.lower() if config.order == SortOrder.ASC else (-len(value), value))
                else:
                    values.append(value)

            return tuple(values)

        reverse = configs[0].order == SortOrder.DESC
        return sorted(data, key=sort_key, reverse=reverse)


class MultiKeySorter:
    """Sort by multiple keys."""

    def sort(
        self,
        data: List[Dict],
        keys: List[str],
        orders: Optional[List[SortOrder]] = None,
    ) -> List[Dict]:
        """Sort by multiple keys."""
        if not data:
            return data

        orders = orders or [SortOrder.ASC] * len(keys)

        def multi_key(item):
            result = []
            for key, order in zip(keys, orders):
                value = item.get(key)
                if value is None:
                    result.append(("" if order == SortOrder.ASC else None, ""))
                elif isinstance(value, (int, float)):
                    result.append((0, value if order == SortOrder.ASC else -value))
                else:
                    result.append((str(value).lower(), ""))
            return result

        return sorted(data, key=multi_key)


class CustomSorter:
    """Custom sort function."""

    def sort(
        self,
        data: List[Any],
        sort_fn: Callable[[Any, Any], int],
    ) -> List[Any]:
        """Sort with custom function."""
        from functools import cmp_to_key
        return sorted(data, key=cmp_to_key(sort_fn))


class DataSorterAction(BaseAction):
    """Data sorter action."""
    action_type = "data_sorter"
    display_name = "数据排序器"
    description = "数据排序"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "sort")
            data = params.get("data", [])

            if operation == "sort":
                return self._sort(data, params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Sort error: {str(e)}")

    def _sort(self, data: List[Dict], params: Dict) -> ActionResult:
        """Sort data."""
        keys = params.get("keys", [])
        orders_str = params.get("orders", [])

        orders = []
        for o in orders_str:
            try:
                orders.append(SortOrder[o.upper()])
            except KeyError:
                orders.append(SortOrder.ASC)

        if not keys:
            return ActionResult(success=False, message="keys is required")

        if len(keys) == 1:
            config = SortConfig(field=keys[0], order=orders[0] if orders else SortOrder.ASC)
            sorter = DataSorter()
            sorted_data = sorter.sort(data, [config])
        else:
            sorter = MultiKeySorter()
            sorted_data = sorter.sort(data, keys, orders)

        return ActionResult(
            success=True,
            message=f"Sorted by {keys}",
            data={"data": sorted_data, "count": len(sorted_data)},
        )
