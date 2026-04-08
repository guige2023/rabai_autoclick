"""Reducer action module for RabAI AutoClick.

Provides reducer pattern for data processing:
- Reducer: Base reducer interface
- Accumulator: State accumulator
- GroupReducer: Group and reduce data
- PipelineReducer: Chain reducers
"""

from typing import Any, Callable, Dict, List, Optional, TypeVar, Generic
from abc import ABC, abstractmethod
from dataclasses import dataclass
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


T = TypeVar("T")
R = TypeVar("R")


class Reducer(ABC, Generic[T, R]):
    """Abstract reducer interface."""

    @abstractmethod
    def reduce(self, items: List[T]) -> R:
        """Reduce items to single value."""
        pass

    def get_name(self) -> str:
        """Get reducer name."""
        return self.__class__.__name__


class SumReducer(Reducer[T, R]):
    """Sum reducer."""

    def __init__(self, key: Optional[str] = None):
        self.key = key

    def reduce(self, items: List[Dict]) -> float:
        """Sum values."""
        total = 0.0
        for item in items:
            if self.key:
                val = item.get(self.key, 0)
            else:
                val = item
            try:
                total += float(val)
            except (TypeError, ValueError):
                pass
        return total


class CountReducer(Reducer[T, int]):
    """Count reducer."""

    def reduce(self, items: List[Any]) -> int:
        """Count items."""
        return len(items)


class AverageReducer(Reducer[T, float]):
    """Average reducer."""

    def __init__(self, key: Optional[str] = None):
        self.key = key

    def reduce(self, items: List[Dict]) -> float:
        """Calculate average."""
        if not items:
            return 0.0
        total = 0.0
        count = 0
        for item in items:
            if self.key:
                val = item.get(self.key, 0)
            else:
                val = item
            try:
                total += float(val)
                count += 1
            except (TypeError, ValueError):
                pass
        return total / count if count > 0 else 0.0


class MinReducer(Reducer[T, Any]):
    """Minimum reducer."""

    def __init__(self, key: Optional[str] = None):
        self.key = key

    def reduce(self, items: List[Dict]) -> Any:
        """Find minimum."""
        if not items:
            return None
        if self.key is None:
            try:
                return min(float(item) for item in items if item is not None)
            except (TypeError, ValueError):
                return None
        try:
            return min(float(item.get(self.key, float("inf"))) for item in items)
        except (TypeError, ValueError):
            return None


class MaxReducer(Reducer[T, Any]):
    """Maximum reducer."""

    def __init__(self, key: Optional[str] = None):
        self.key = key

    def reduce(self, items: List[Dict]) -> Any:
        """Find maximum."""
        if not items:
            return None
        if self.key is None:
            try:
                return max(float(item) for item in items if item is not None)
            except (TypeError, ValueError):
                return None
        try:
            return max(float(item.get(self.key, float("-inf"))) for item in items)
        except (TypeError, ValueError):
            return None


class CollectReducer(Reducer[T, List[Any]]):
    """Collect reducer."""

    def __init__(self, key: Optional[str] = None):
        self.key = key

    def reduce(self, items: List[Dict]) -> List[Any]:
        """Collect values."""
        if self.key is None:
            return list(items)
        return [item.get(self.key) for item in items if self.key in item]


class DistinctReducer(Reducer[T, List[Any]]):
    """Distinct reducer."""

    def __init__(self, key: Optional[str] = None):
        self.key = key

    def reduce(self, items: List[Any]) -> List[Any]:
        """Get distinct values."""
        seen = set()
        result = []
        for item in items:
            val = item.get(self.key) if isinstance(item, dict) else item
            if val not in seen:
                seen.add(val)
                result.append(val)
        return result


@dataclass
class AccumulatorState:
    """Accumulator state."""
    value: Any
    count: int = 0
    metadata: Dict[str, Any] = None


class Accumulator(Generic[T, R]):
    """Stateful accumulator."""

    def __init__(self, initial_value: R, reducer: Callable[[R, T], R]):
        self._value = initial_value
        self._reducer = reducer
        self._count = 0

    @property
    def value(self) -> R:
        """Get current value."""
        return self._value

    @property
    def count(self) -> int:
        """Get item count."""
        return self._count

    def accumulate(self, item: T) -> None:
        """Accumulate an item."""
        self._value = self._reducer(self._value, item)
        self._count += 1

    def reset(self, initial_value: Optional[R] = None) -> None:
        """Reset accumulator."""
        if initial_value is not None:
            self._value = initial_value
        self._count = 0


class GroupReducer:
    """Group data and apply reducers."""

    def __init__(self, group_key: str):
        self.group_key = group_key
        self._reducers: Dict[str, Reducer] = {}

    def add_reducer(self, name: str, reducer: Reducer) -> None:
        """Add a reducer."""
        self._reducers[name] = reducer

    def reduce(self, items: List[Dict]) -> Dict[str, Any]:
        """Group and reduce."""
        groups: Dict[Any, List[Dict]] = {}

        for item in items:
            key_val = item.get(self.group_key)
            if key_val not in groups:
                groups[key_val] = []
            groups[key_val].append(item)

        results = {}
        for group_key, group_items in groups.items():
            group_result = {"_count": len(group_items)}
            for name, reducer in self._reducers.items():
                try:
                    group_result[name] = reducer.reduce(group_items)
                except Exception:
                    group_result[name] = None
            results[group_key] = group_result

        return results


class PipelineReducer:
    """Chain reducers in a pipeline."""

    def __init__(self):
        self._stages: List[Callable[[List], List]] = []

    def add_stage(self, stage_fn: Callable[[List], List]) -> "PipelineReducer":
        """Add a pipeline stage."""
        self._stages.append(stage_fn)
        return self

    def pipe(self, items: List[Any]) -> List[Any]:
        """Process through pipeline."""
        result = items
        for stage in self._stages:
            result = stage(result)
        return result


class ReducerAction(BaseAction):
    """Reducer pattern action."""
    action_type = "reducer"
    display_name = "归约模式"
    description = "数据归约操作"

    def __init__(self):
        super().__init__()
        self._accumulators: Dict[str, Accumulator] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "reduce")

            if operation == "reduce":
                return self._reduce(params)
            elif operation == "group":
                return self._group_reduce(params)
            elif operation == "accumulate":
                return self._accumulate(params)
            elif operation == "pipeline":
                return self._pipeline(params)
            elif operation == "reset":
                return self._reset_accumulator(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Reducer error: {str(e)}")

    def _reduce(self, params: Dict[str, Any]) -> ActionResult:
        """Reduce data."""
        items = params.get("items", [])
        reduce_type = params.get("type", "sum")
        key = params.get("key")

        if reduce_type == "sum":
            reducer = SumReducer(key=key)
        elif reduce_type == "count":
            reducer = CountReducer()
        elif reduce_type == "average":
            reducer = AverageReducer(key=key)
        elif reduce_type == "min":
            reducer = MinReducer(key=key)
        elif reduce_type == "max":
            reducer = MaxReducer(key=key)
        elif reduce_type == "collect":
            reducer = CollectReducer(key=key)
        elif reduce_type == "distinct":
            reducer = DistinctReducer(key=key)
        else:
            return ActionResult(success=False, message=f"Unknown reduce type: {reduce_type}")

        try:
            result = reducer.reduce(items)
            return ActionResult(success=True, message=f"{reduce_type} reduced", data={"result": result})
        except Exception as e:
            return ActionResult(success=False, message=f"Reduce failed: {e}")

    def _group_reduce(self, params: Dict[str, Any]) -> ActionResult:
        """Group and reduce."""
        items = params.get("items", [])
        group_key = params.get("group_key")
        reducers_config = params.get("reducers", {})

        if not group_key:
            return ActionResult(success=False, message="group_key is required")

        grouper = GroupReducer(group_key=group_key)

        for name, config in reducers_config.items():
            reduce_type = config.get("type", "sum")
            key = config.get("key")

            if reduce_type == "sum":
                grouper.add_reducer(name, SumReducer(key=key))
            elif reduce_type == "count":
                grouper.add_reducer(name, CountReducer())
            elif reduce_type == "average":
                grouper.add_reducer(name, AverageReducer(key=key))
            elif reduce_type == "min":
                grouper.add_reducer(name, MinReducer(key=key))
            elif reduce_type == "max":
                grouper.add_reducer(name, MaxReducer(key=key))
            elif reduce_type == "collect":
                grouper.add_reducer(name, CollectReducer(key=key))

        try:
            result = grouper.reduce(items)
            return ActionResult(success=True, message=f"Grouped into {len(result)} groups", data={"groups": result})
        except Exception as e:
            return ActionResult(success=False, message=f"Group reduce failed: {e}")

    def _accumulate(self, params: Dict[str, Any]) -> ActionResult:
        """Accumulate values."""
        accumulator_id = params.get("accumulator_id", "default")
        items = params.get("items", [])
        initial_value = params.get("initial_value", 0)
        reducer_fn = params.get("reducer_fn")

        if accumulator_id not in self._accumulators:
            if reducer_fn:
                self._accumulators[accumulator_id] = Accumulator(initial_value, reducer_fn)
            else:
                def sum_fn(acc, val):
                    try:
                        return acc + float(val)
                    except (TypeError, ValueError):
                        return acc
                self._accumulators[accumulator_id] = Accumulator(initial_value, sum_fn)

        acc = self._accumulators[accumulator_id]

        for item in items:
            acc.accumulate(item)

        return ActionResult(
            success=True,
            message=f"Accumulated {acc.count} items",
            data={"value": acc.value, "count": acc.count, "accumulator_id": accumulator_id},
        )

    def _reset_accumulator(self, params: Dict[str, Any]) -> ActionResult:
        """Reset an accumulator."""
        accumulator_id = params.get("accumulator_id", "default")
        initial_value = params.get("initial_value")

        if accumulator_id not in self._accumulators:
            return ActionResult(success=False, message=f"Accumulator not found: {accumulator_id}")

        acc = self._accumulators[accumulator_id]
        acc.reset(initial_value)

        return ActionResult(success=True, message=f"Accumulator reset: {accumulator_id}")

    def _pipeline(self, params: Dict[str, Any]) -> ActionResult:
        """Execute a pipeline."""
        items = params.get("items", [])
        stages = params.get("stages", [])

        pipeline = PipelineReducer()

        for stage_config in stages:
            stage_type = stage_config.get("type")

            if stage_type == "filter":
                key = stage_config.get("key")
                operator = stage_config.get("operator", "eq")
                value = stage_config.get("value")

                def make_filter(k, op, v):
                    def filter_fn(data):
                        result = []
                        for item in data:
                            item_val = item.get(k) if isinstance(item, dict) else item
                            if op == "eq" and item_val == v:
                                result.append(item)
                            elif op == "ne" and item_val != v:
                                result.append(item)
                            elif op == "gt" and item_val > v:
                                result.append(item)
                            elif op == "lt" and item_val < v:
                                result.append(item)
                        return result
                    return filter_fn

                pipeline.add_stage(make_filter(key, operator, value))

            elif stage_type == "map":
                key = stage_config.get("key")
                fn = stage_config.get("fn", lambda x: x)

                def make_map(k, f):
                    def map_fn(data):
                        result = []
                        for item in data:
                            item_val = item.get(k) if isinstance(item, dict) else item
                            result.append(f(item_val))
                        return result
                    return map_fn

                pipeline.add_stage(make_map(key, fn))

            elif stage_type == "sort":
                key = stage_config.get("key")
                reverse = stage_config.get("reverse", False)

                def make_sort(k, rev):
                    def sort_fn(data):
                        return sorted(data, key=lambda x: x.get(k) if isinstance(x, dict) else x, reverse=rev)
                    return sort_fn

                pipeline.add_stage(make_sort(key, reverse))

        try:
            result = pipeline.pipe(items)
            return ActionResult(success=True, message=f"Pipeline processed {len(result)} items", data={"result": result, "count": len(result)})
        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline failed: {e}")
