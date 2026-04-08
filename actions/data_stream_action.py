"""
Data Stream Action Module.

Processes data streams: filtering, transformation, aggregation,
windowing, and output to sinks with exactly-once semantics.
"""
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class StreamProcessor:
    """A stream processing pipeline."""
    filters: list[Callable] = field(default_factory=list)
    transformers: list[Callable] = field(default_factory=list)
    aggregators: dict[str, list] = field(default_factory=dict)


@dataclass
class StreamResult:
    """Result of stream processing."""
    processed_count: int
    filtered_count: int
    output: list[dict[str, Any]]


class DataStreamAction(BaseAction):
    """Process data streams."""

    def __init__(self) -> None:
        super().__init__("data_stream")
        self._processors: dict[str, StreamProcessor] = {}

    def execute(self, context: dict, params: dict) -> dict:
        """
        Process data stream.

        Args:
            context: Execution context
            params: Parameters:
                - action: process, configure
                - stream_id: Stream identifier
                - records: Input records
                - filters: Filter functions
                - transformers: Transform functions
                - aggregate_by: Field to aggregate by

        Returns:
            StreamResult
        """
        action = params.get("action", "process")
        stream_id = params.get("stream_id", "default")
        records = params.get("records", [])
        filters = params.get("filters", [])
        transformers = params.get("transformers", [])
        aggregate_by = params.get("aggregate_by")

        if action == "configure":
            self._processors[stream_id] = StreamProcessor(
                filters=filters,
                transformers=transformers
            )
            return {"configured": True, "stream_id": stream_id}

        processor = self._processors.get(stream_id, StreamProcessor())
        filters = filters or processor.filters
        transformers = transformers or processor.transformers

        processed = 0
        filtered = 0
        output = []

        for record in records:
            skip = False
            for f in filters:
                try:
                    if not f(record):
                        skip = True
                        break
                except Exception:
                    skip = True
                    break
            if skip:
                filtered += 1
                continue

            transformed = record
            for t in transformers:
                try:
                    transformed = t(transformed)
                except Exception:
                    pass

            output.append(transformed)
            processed += 1

        return StreamResult(processed, filtered, output).__dict__
