"""Stream processing action module for RabAI AutoClick.

Provides stream processing operations:
- StreamSourceAction: Create data streams from various sources
- StreamFilterAction: Filter stream elements
- StreamMapAction: Transform stream elements
- StreamAggregateAction: Aggregate stream data
"""

import threading
import time
from typing import Any, Callable, Dict, Generator, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
import queue


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class StreamElement:
    """Represents an element in a stream."""
    value: Any
    timestamp: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataStream:
    """In-memory data stream."""
    def __init__(self, name: str = "unnamed"):
        self.name = name
        self._queue: queue.Queue = queue.Queue()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._source: Optional[Callable] = None
        self._filters: List[Callable] = []
        self._mappers: List[Callable] = []
        self._elements: List[StreamElement] = []
        self._max_elements = 10000

    def set_source(self, source: Callable[[], Any]) -> None:
        self._source = source

    def add_filter(self, filter_fn: Callable[[Any], bool]) -> None:
        self._filters.append(filter_fn)

    def add_mapper(self, mapper: Callable[[Any], Any]) -> None:
        self._mappers.append(mapper)

    def push(self, value: Any, metadata: Optional[Dict[str, Any]] = None) -> None:
        element = StreamElement(value=value, metadata=metadata or {})
        for f in self._filters:
            if not f(element):
                return
        for m in self._mappers:
            element.value = m(element.value)
        self._elements.append(element)
        if len(self._elements) > self._max_elements:
            self._elements = self._elements[-self._max_elements:]
        self._queue.put(element)

    def consume(self, count: int = 1, timeout: Optional[float] = None) -> List[StreamElement]:
        elements = []
        for _ in range(count):
            try:
                elem = self._queue.get(timeout=timeout)
                elements.append(elem)
            except queue.Empty:
                break
        return elements

    def get_elements(self, limit: int = 100) -> List[StreamElement]:
        return self._elements[-limit:]


_streams: Dict[str, DataStream] = {}


class StreamSourceAction(BaseAction):
    """Create and configure data streams."""
    action_type = "stream_source"
    display_name = "创建数据流"
    description = "从各种数据源创建数据流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            source_type = params.get("source_type", "memory")
            source_data = params.get("source_data", [])
            auto_push = params.get("auto_push", False)
            push_interval = params.get("push_interval", 1.0)

            if not name:
                return ActionResult(success=False, message="name is required")

            stream = DataStream(name=name)
            _streams[name] = stream

            if source_type == "memory":
                if isinstance(source_data, list):
                    for item in source_data:
                        stream.push(item)
                else:
                    stream.push(source_data)

            elif source_type == "generator":
                gen_ref = params.get("generator_ref", None)
                if gen_ref:
                    stream.set_source(gen_ref)

            return ActionResult(
                success=True,
                message=f"Stream '{name}' created with {len(stream.get_elements())} elements",
                data={
                    "name": name,
                    "source_type": source_type,
                    "element_count": len(stream.get_elements()),
                    "auto_push": auto_push
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Stream source failed: {str(e)}")


class StreamFilterAction(BaseAction):
    """Filter stream elements."""
    action_type = "stream_filter"
    display_name = "流过滤"
    description = "过滤数据流中的元素"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            filter_type = params.get("filter_type", "value")
            condition = params.get("condition", {})
            limit = params.get("limit", 100)

            if not name:
                return ActionResult(success=False, message="name is required")

            if name not in _streams:
                return ActionResult(success=False, message=f"Stream '{name}' not found")

            stream = _streams[name]
            elements = stream.get_elements(limit=10000)

            filtered = []
            for elem in elements:
                if filter_type == "value":
                    column = condition.get("column", "")
                    operator = condition.get("operator", "==")
                    value = condition.get("value", None)
                    if column:
                        val = elem.value.get(column) if isinstance(elem.value, dict) else getattr(elem.value, column, None)
                        if operator == "==":
                            match = val == value
                        elif operator == "!=":
                            match = val != value
                        elif operator == ">":
                            match = val > value
                        elif operator == "<":
                            match = val < value
                        elif operator == "contains":
                            match = str(value) in str(val)
                        else:
                            match = True
                        if match:
                            filtered.append(elem)
                    else:
                        filtered.append(elem)
                elif filter_type == "time":
                    pass
                elif filter_type == "metadata":
                    key = condition.get("key", "")
                    value = condition.get("value", None)
                    if key in elem.metadata and elem.metadata[key] == value:
                        filtered.append(elem)

            return ActionResult(
                success=True,
                message=f"Filtered {len(filtered)}/{len(elements)} elements",
                data={
                    "filtered_count": len(filtered),
                    "total_count": len(elements),
                    "filter_type": filter_type
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Stream filter failed: {str(e)}")


class StreamMapAction(BaseAction):
    """Transform stream elements."""
    action_type = "stream_map"
    display_name = "流映射"
    description = "转换数据流中的元素"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            field_mappings = params.get("field_mappings", {})
            computed_fields = params.get("computed_fields", [])
            limit = params.get("limit", 100)

            if not name:
                return ActionResult(success=False, message="name is required")

            if name not in _streams:
                return ActionResult(success=False, message=f"Stream '{name}' not found")

            stream = _streams[name]
            elements = stream.get_elements(limit=limit)
            mapped = []

            for elem in elements:
                value = elem.value
                if isinstance(value, dict):
                    new_value = dict(value)
                    for old_field, new_field in field_mappings.items():
                        if old_field in new_value:
                            new_value[new_field] = new_value.pop(old_field)
                    for cf in computed_fields:
                        field_name = cf.get("field", "")
                        expression = cf.get("expression", "")
                        try:
                            new_value[field_name] = eval(expression, {"__builtins__": {}}, {"v": new_value})
                        except Exception:
                            new_value[field_name] = None
                    mapped.append(new_value)
                else:
                    mapped.append(value)

            return ActionResult(
                success=True,
                message=f"Mapped {len(mapped)} elements",
                data={"mapped": mapped, "count": len(mapped)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Stream map failed: {str(e)}")


class StreamAggregateAction(BaseAction):
    """Aggregate stream data."""
    action_type = "stream_aggregate"
    display_name = "流聚合"
    description = "聚合数据流中的数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            agg_type = params.get("agg_type", "count")
            group_by = params.get("group_by", None)
            field_name = params.get("field", None)
            window_size = params.get("window_size", None)

            if not name:
                return ActionResult(success=False, message="name is required")

            if name not in _streams:
                return ActionResult(success=False, message=f"Stream '{name}' not found")

            stream = _streams[name]
            elements = stream.get_elements(limit=10000)

            if window_size:
                elements = elements[-window_size:]

            values = []
            for elem in elements:
                if isinstance(elem.value, dict):
                    if field_name and field_name in elem.value:
                        values.append(elem.value[field_name])
                    else:
                        values.append(elem.value)
                else:
                    values.append(elem.value)

            result = {}
            if agg_type == "count":
                result = {"count": len(values)}
            elif agg_type == "sum":
                try:
                    result = {"sum": sum(float(v) for v in values if v is not None)}
                except (ValueError, TypeError):
                    result = {"sum": 0}
            elif agg_type == "avg":
                try:
                    nums = [float(v) for v in values if v is not None]
                    result = {"avg": sum(nums) / len(nums) if nums else 0}
                except (ValueError, TypeError):
                    result = {"avg": 0}
            elif agg_type == "min":
                try:
                    result = {"min": min(float(v) for v in values if v is not None)}
                except (ValueError, TypeError):
                    result = {"min": None}
            elif agg_type == "max":
                try:
                    result = {"max": max(float(v) for v in values if v is not None)}
                except (ValueError, TypeError):
                    result = {"max": None}
            elif agg_type == "collect":
                result = {"values": values}

            return ActionResult(
                success=True,
                message=f"Aggregated {len(values)} elements: {agg_type}",
                data={**result, "element_count": len(values), "agg_type": agg_type}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Stream aggregate failed: {str(e)}")
