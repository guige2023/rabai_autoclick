"""
Data pipeline stream action for continuous data processing.

Provides stream processing with windowing, aggregation, and transformation.
"""

from typing import Any, Callable, Optional
import time
from collections import deque


class DataPipelineStreamAction:
    """Stream processing pipeline with windowing support."""

    def __init__(
        self,
        window_size: float = 60.0,
        window_type: str = "tumbling",
        buffer_size: int = 1000,
    ) -> None:
        """
        Initialize data pipeline stream.

        Args:
            window_size: Window size in seconds
            window_type: 'tumbling', 'sliding', 'session'
            buffer_size: Maximum buffer size
        """
        self.window_size = window_size
        self.window_type = window_type
        self.buffer_size = buffer_size
        self._buffer: deque[dict[str, Any]] = deque(maxlen=buffer_size)
        self._windows: list[dict[str, Any]] = []
        self._processors: list[Callable] = []

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Process data through pipeline.

        Args:
            params: Dictionary containing:
                - operation: 'emit', 'process', 'aggregate', 'flush'
                - data: Data to process
                - key: Optional grouping key

        Returns:
            Dictionary with processing result
        """
        operation = params.get("operation", "process")

        if operation == "emit":
            return self._emit_data(params)
        elif operation == "process":
            return self._process_data(params)
        elif operation == "aggregate":
            return self._aggregate_window(params)
        elif operation == "flush":
            return self._flush_buffer(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _emit_data(self, params: dict[str, Any]) -> dict[str, Any]:
        """Emit data into stream."""
        data = params.get("data", {})
        key = params.get("key", "default")

        record = {
            "data": data,
            "key": key,
            "timestamp": time.time(),
            "sequence": len(self._buffer),
        }

        self._buffer.append(record)
        return {
            "success": True,
            "buffer_size": len(self._buffer),
            "sequence": record["sequence"],
        }

    def _process_data(self, params: dict[str, Any]) -> dict[str, Any]:
        """Process data through pipeline stages."""
        data = params.get("data", {})
        stages = params.get("stages", ["filter", "transform", "map"])

        result = data
        for stage in stages:
            if stage == "filter":
                result = self._filter_stage(result, params)
            elif stage == "transform":
                result = self._transform_stage(result, params)
            elif stage == "map":
                result = self._map_stage(result, params)

        return {"success": True, "result": result, "stages_applied": len(stages)}

    def _filter_stage(self, data: Any, params: dict[str, Any]) -> Any:
        """Apply filter condition."""
        condition = params.get("filter_condition", lambda x: True)
        if callable(condition):
            return data if condition(data) else None
        return data

    def _transform_stage(self, data: Any, params: dict[str, Any]) -> Any:
        """Apply transformation."""
        transform_fn = params.get("transform", lambda x: x)
        if callable(transform_fn):
            return transform_fn(data)
        return data

    def _map_stage(self, data: Any, params: dict[str, Any]) -> Any:
        """Apply map operation."""
        map_fn = params.get("map_fn", lambda x: x)
        if callable(map_fn):
            return map_fn(data)
        return data

    def _aggregate_window(self, params: dict[str, Any]) -> dict[str, Any]:
        """Aggregate current window."""
        now = time.time()
        cutoff = now - self.window_size

        if self.window_type == "tumbling":
            window_data = list(self._buffer)
            self._buffer.clear()
        elif self.window_type == "sliding":
            window_data = [r for r in self._buffer if r["timestamp"] > cutoff]
        else:
            window_data = list(self._buffer)

        aggregation_fn = params.get("aggregation", "count")
        result = self._compute_aggregation(window_data, aggregation_fn)

        window = {
            "window_id": len(self._windows),
            "start_time": window_data[0]["timestamp"] if window_data else now,
            "end_time": now,
            "record_count": len(window_data),
            "aggregation": result,
        }
        self._windows.append(window)

        return {"success": True, "window": window}

    def _compute_aggregation(
        self, data: list[dict[str, Any]], aggregation: str
    ) -> Any:
        """Compute aggregation on window data."""
        if not data:
            return None

        if aggregation == "count":
            return len(data)
        elif aggregation == "sum":
            values = [r["data"].get("value", 0) for r in data]
            return sum(values)
        elif aggregation == "avg":
            values = [r["data"].get("value", 0) for r in data]
            return sum(values) / len(values) if values else 0
        elif aggregation == "min":
            values = [r["data"].get("value", 0) for r in data]
            return min(values) if values else None
        elif aggregation == "max":
            values = [r["data"].get("value", 0) for r in data]
            return max(values) if values else None
        else:
            return len(data)

    def _flush_buffer(self, params: dict[str, Any]) -> dict[str, Any]:
        """Flush buffer and return all records."""
        records = list(self._buffer)
        self._buffer.clear()
        return {"success": True, "flushed_count": len(records), "records": records}

    def get_buffer_status(self) -> dict[str, Any]:
        """Get current buffer status."""
        return {
            "buffer_size": len(self._buffer),
            "buffer_capacity": self.buffer_size,
            "window_type": self.window_type,
            "window_size": self.window_size,
            "windows_processed": len(self._windows),
        }
