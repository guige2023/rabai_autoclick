"""Strategy action module for RabAI AutoClick.

Provides strategy pattern implementation:
- Strategy: Abstract strategy interface
- Context: Context using strategies
- ConcreteStrategy: Specific algorithm implementations
"""

from typing import Any, Callable, Dict, List, Optional, TypeVar, Generic
from abc import ABC, abstractmethod
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


T = TypeVar("T")
R = TypeVar("R")


class Strategy(ABC, Generic[T, R]):
    """Abstract strategy interface."""

    @abstractmethod
    def execute(self, data: T) -> R:
        """Execute the strategy."""
        pass

    def get_name(self) -> str:
        """Get strategy name."""
        return self.__class__.__name__


class SortStrategy(Strategy[List[Any], List[Any]]):
    """Sorting strategy."""

    def execute(self, data: List[Any]) -> List[Any]:
        """Sort the data."""
        return sorted(data)


class BubbleSortStrategy(SortStrategy):
    """Bubble sort implementation."""

    def execute(self, data: List[Any]) -> List[Any]:
        """Bubble sort."""
        arr = data.copy()
        n = len(arr)
        for i in range(n):
            for j in range(0, n - i - 1):
                if arr[j] > arr[j + 1]:
                    arr[j], arr[j + 1] = arr[j + 1], arr[j]
        return arr


class QuickSortStrategy(SortStrategy):
    """Quick sort implementation."""

    def execute(self, data: List[Any]) -> List[Any]:
        """Quick sort."""
        if len(data) <= 1:
            return data.copy()
        pivot = data[len(data) // 2]
        left = [x for x in data if x < pivot]
        middle = [x for x in data if x == pivot]
        right = [x for x in data if x > pivot]
        return self.execute(left) + middle + self.execute(right)


class MergeSortStrategy(SortStrategy):
    """Merge sort implementation."""

    def execute(self, data: List[Any]) -> List[Any]:
        """Merge sort."""
        if len(data) <= 1:
            return data.copy()

        mid = len(data) // 2
        left = self.execute(data[:mid])
        right = self.execute(data[mid:])

        return self._merge(left, right)

    def _merge(self, left: List, right: List) -> List:
        """Merge two sorted lists."""
        result = []
        i = j = 0
        while i < len(left) and j < len(right):
            if left[i] <= right[j]:
                result.append(left[i])
                i += 1
            else:
                result.append(right[j])
                j += 1
        result.extend(left[i:])
        result.extend(right[j:])
        return result


class FilterStrategy(Strategy[List[Dict], List[Dict]]):
    """Filtering strategy."""

    def execute(self, data: List[Dict]) -> List[Dict]:
        """Filter data."""
        return data


class RangeFilterStrategy(FilterStrategy):
    """Filter by numeric range."""

    def __init__(self, field: str, min_val: Optional[float] = None, max_val: Optional[float] = None):
        self.field = field
        self.min_val = min_val
        self.max_val = max_val

    def execute(self, data: List[Dict]) -> List[Dict]:
        """Filter by range."""
        result = []
        for item in data:
            val = item.get(self.field)
            if val is None:
                continue
            try:
                num_val = float(val)
                if self.min_val is not None and num_val < self.min_val:
                    continue
                if self.max_val is not None and num_val > self.max_val:
                    continue
                result.append(item)
            except (TypeError, ValueError):
                pass
        return result


class KeywordFilterStrategy(FilterStrategy):
    """Filter by keyword."""

    def __init__(self, field: str, keyword: str, case_sensitive: bool = False):
        self.field = field
        self.keyword = keyword
        self.case_sensitive = case_sensitive

    def execute(self, data: List[Dict]) -> List[Dict]:
        """Filter by keyword."""
        result = []
        keyword = self.keyword if self.case_sensitive else self.keyword.lower()
        for item in data:
            val = item.get(self.field, "")
            check_val = val if self.case_sensitive else val.lower()
            if keyword in check_val:
                result.append(item)
        return result


class SearchStrategy(Strategy[Dict, Optional[Any]]):
    """Search strategy."""

    def execute(self, data: Dict) -> Optional[Any]:
        """Search in data."""
        return None


class BinarySearchStrategy(SearchStrategy):
    """Binary search (requires sorted data)."""

    def __init__(self, key: str = "value"):
        self.key = key

    def execute(self, data: Dict) -> Optional[Any]:
        """Binary search."""
        sorted_data = data.get("sorted_data", [])
        target = data.get("target")

        if not sorted_data or target is None:
            return None

        left, right = 0, len(sorted_data) - 1
        while left <= right:
            mid = (left + right) // 2
            val = sorted_data[mid] if isinstance(sorted_data[mid], (int, float)) else sorted_data[mid].get(self.key)
            if val == target:
                return {"found": True, "index": mid, "value": sorted_data[mid]}
            elif val < target:
                left = mid + 1
            else:
                right = mid - 1

        return {"found": False, "index": -1}


class LinearSearchStrategy(SearchStrategy):
    """Linear search."""

    def __init__(self, key: str = "value"):
        self.key = key

    def execute(self, data: Dict) -> Optional[Any]:
        """Linear search."""
        items = data.get("items", [])
        target = data.get("target")

        if target is None:
            return None

        for i, item in enumerate(items):
            val = item if isinstance(item, (int, float, str)) else item.get(self.key)
            if val == target:
                return {"found": True, "index": i, "value": item}

        return {"found": False, "index": -1}


class Context:
    """Context using strategies."""

    def __init__(self, strategy: Optional[Strategy] = None):
        self._strategy = strategy

    def set_strategy(self, strategy: Strategy) -> None:
        """Set the strategy."""
        self._strategy = strategy

    def get_strategy(self) -> Optional[Strategy]:
        """Get the strategy."""
        return self._strategy

    def execute_strategy(self, data: Any) -> Any:
        """Execute current strategy."""
        if self._strategy is None:
            raise RuntimeError("No strategy set")
        return self._strategy.execute(data)


class StrategyRegistry:
    """Registry for strategies."""

    def __init__(self):
        self._strategies: Dict[str, Strategy] = {}

    def register(self, name: str, strategy: Strategy) -> None:
        """Register a strategy."""
        self._strategies[name] = strategy

    def get(self, name: str) -> Optional[Strategy]:
        """Get a strategy."""
        return self._strategies.get(name)

    def list_strategies(self) -> List[str]:
        """List all strategy names."""
        return list(self._strategies.keys())

    def unregister(self, name: str) -> bool:
        """Unregister a strategy."""
        if name in self._strategies:
            del self._strategies[name]
            return True
        return False


class StrategyAction(BaseAction):
    """Strategy pattern action."""
    action_type = "strategy"
    display_name = "策略模式"
    description = "算法策略切换"

    def __init__(self):
        super().__init__()
        self._context = Context()
        self._registry = StrategyRegistry()
        self._register_defaults()

    def _register_defaults(self) -> None:
        """Register default strategies."""
        self._registry.register("bubble_sort", BubbleSortStrategy())
        self._registry.register("quick_sort", QuickSortStrategy())
        self._registry.register("merge_sort", MergeSortStrategy())
        self._registry.register("binary_search", BinarySearchStrategy())
        self._registry.register("linear_search", LinearSearchStrategy())

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "execute")

            if operation == "execute":
                return self._execute(params)
            elif operation == "set_strategy":
                return self._set_strategy(params)
            elif operation == "register":
                return self._register_strategy(params)
            elif operation == "list":
                return self._list_strategies()
            elif operation == "sort":
                return self._sort(params)
            elif operation == "filter":
                return self._filter(params)
            elif operation == "search":
                return self._search(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Strategy error: {str(e)}")

    def _execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute with current strategy."""
        data = params.get("data")

        if data is None:
            return ActionResult(success=False, message="data is required")

        try:
            result = self._context.execute_strategy(data)
            return ActionResult(success=True, message="Strategy executed", data={"result": result})
        except Exception as e:
            return ActionResult(success=False, message=f"Execution failed: {e}")

    def _set_strategy(self, params: Dict[str, Any]) -> ActionResult:
        """Set the active strategy."""
        strategy_name = params.get("strategy_name")

        if not strategy_name:
            return ActionResult(success=False, message="strategy_name is required")

        strategy = self._registry.get(strategy_name)
        if not strategy:
            return ActionResult(success=False, message=f"Strategy not found: {strategy_name}")

        self._context.set_strategy(strategy)
        return ActionResult(success=True, message=f"Strategy set: {strategy_name}")

    def _register_strategy(self, params: Dict[str, Any]) -> ActionResult:
        """Register a custom strategy."""
        name = params.get("name")
        strategy_type = params.get("type")
        config = params.get("config", {})

        if not name or not strategy_type:
            return ActionResult(success=False, message="name and type are required")

        strategy: Optional[Strategy] = None

        if strategy_type == "bubble_sort":
            strategy = BubbleSortStrategy()
        elif strategy_type == "quick_sort":
            strategy = QuickSortStrategy()
        elif strategy_type == "merge_sort":
            strategy = MergeSortStrategy()
        elif strategy_type == "range_filter":
            strategy = RangeFilterStrategy(
                field=config.get("field", "value"),
                min_val=config.get("min_val"),
                max_val=config.get("max_val"),
            )
        elif strategy_type == "keyword_filter":
            strategy = KeywordFilterStrategy(
                field=config.get("field", "text"),
                keyword=config.get("keyword", ""),
                case_sensitive=config.get("case_sensitive", False),
            )
        elif strategy_type == "binary_search":
            strategy = BinarySearchStrategy(key=config.get("key", "value"))
        elif strategy_type == "linear_search":
            strategy = LinearSearchStrategy(key=config.get("key", "value"))
        else:
            return ActionResult(success=False, message=f"Unknown strategy type: {strategy_type}")

        self._registry.register(name, strategy)
        return ActionResult(success=True, message=f"Strategy registered: {name}")

    def _list_strategies(self) -> ActionResult:
        """List all strategies."""
        strategies = self._registry.list_strategies()
        current = self._context.get_strategy()
        current_name = current.get_name() if current else None

        return ActionResult(
            success=True,
            message=f"{len(strategies)} strategies available",
            data={"strategies": strategies, "current": current_name},
        )

    def _sort(self, params: Dict[str, Any]) -> ActionResult:
        """Sort data."""
        data = params.get("data", [])
        sort_type = params.get("sort_type", "quick_sort")

        sort_map = {
            "bubble_sort": BubbleSortStrategy,
            "quick_sort": QuickSortStrategy,
            "merge_sort": MergeSortStrategy,
        }

        if sort_type not in sort_map:
            return ActionResult(success=False, message=f"Unknown sort type: {sort_type}")

        strategy = sort_map[sort_type]()
        result = strategy.execute(data)

        return ActionResult(success=True, message=f"Sorted with {sort_type}", data={"result": result})

    def _filter(self, params: Dict[str, Any]) -> ActionResult:
        """Filter data."""
        data = params.get("data", [])
        filter_type = params.get("filter_type")
        config = params.get("config", {})

        if filter_type == "range":
            strategy = RangeFilterStrategy(
                field=config.get("field", "value"),
                min_val=config.get("min_val"),
                max_val=config.get("max_val"),
            )
        elif filter_type == "keyword":
            strategy = KeywordFilterStrategy(
                field=config.get("field", "text"),
                keyword=config.get("keyword", ""),
                case_sensitive=config.get("case_sensitive", False),
            )
        else:
            return ActionResult(success=False, message=f"Unknown filter type: {filter_type}")

        result = strategy.execute(data)
        return ActionResult(success=True, message=f"Filtered: {len(result)}/{len(data)} items", data={"result": result, "count": len(result)})

    def _search(self, params: Dict[str, Any]) -> ActionResult:
        """Search data."""
        search_type = params.get("search_type", "linear")
        search_data = params.get("search_data", {})

        if search_type == "binary":
            strategy = BinarySearchStrategy(key=search_data.get("key", "value"))
        else:
            strategy = LinearSearchStrategy(key=search_data.get("key", "value"))

        result = strategy.execute(search_data)
        return ActionResult(success=True, message="Search completed", data={"result": result})
