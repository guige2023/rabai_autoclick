"""
Event Transformation Utilities for UI Automation.

This module provides utilities for transforming and normalizing
events in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TypeVar, Generic
from enum import Enum


T = TypeVar("T")


class TransformationType(Enum):
    """Types of event transformations."""
    MAP = "map"
    FILTER = "filter"
    REDUCE = "reduce"
    FLATTEN = "flatten"
    ENRICH = "enrich"


@dataclass
class EventTransformer:
    """
    Transform events with configurable operations.
    """
    transformation_type: TransformationType
    func: Callable
    metadata: Dict[str, Any] = field(default_factory=dict)


class EventPipeline:
    """
    Pipeline for chaining event transformations.
    """

    def __init__(self):
        """Initialize event pipeline."""
        self._transformers: List[EventTransformer] = []

    def add_map(self, func: Callable[[Any], Any]) -> 'EventPipeline':
        """Add a map transformation."""
        self._transformers.append(
            EventTransformer(TransformationType.MAP, func)
        )
        return self

    def add_filter(self, predicate: Callable[[Any], bool]) -> 'EventPipeline':
        """Add a filter transformation."""
        self._transformers.append(
            EventTransformer(TransformationType.FILTER, predicate)
        )
        return self

    def add_flatten(self) -> 'EventPipeline':
        """Add a flatten transformation."""
        def flatten_func(events):
            result = []
            for event in events:
                if isinstance(event, (list, tuple)):
                    result.extend(event)
                else:
                    result.append(event)
            return result
        self._transformers.append(
            EventTransformer(TransformationType.FLATTEN, flatten_func)
        )
        return self

    def transform(self, event: Any) -> Any:
        """Transform an event through the pipeline."""
        result = event
        for transformer in self._transformers:
            if transformer.transformation_type == TransformationType.MAP:
                result = transformer.func(result)
            elif transformer.transformation_type == TransformationType.FILTER:
                if not transformer.func(result):
                    return None
            elif transformer.transformation_type == TransformationType.FLATTEN:
                if isinstance(result, list):
                    result = transformer.func(result)
        return result

    def transform_many(self, events: List[Any]) -> List[Any]:
        """Transform multiple events."""
        results = []
        for event in events:
            result = self.transform(event)
            if result is not None:
                if isinstance(result, list):
                    results.extend(result)
                else:
                    results.append(result)
        return results


def normalize_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize event structure.

    Args:
        event: Raw event dictionary

    Returns:
        Normalized event
    """
    normalized = {
        "type": event.get("type", event.get("eventType", "unknown")),
        "timestamp": event.get("timestamp", event.get("time", 0)),
        "source": event.get("source", event.get("sourceApp", "unknown")),
        "data": event.get("data", event.get("payload", {})),
    }

    if "element" in event:
        normalized["element"] = event["element"]

    if "value" in event:
        normalized["value"] = event["value"]

    return normalized


def enrich_event(
    event: Dict[str, Any],
    enricher: Callable[[Dict[str, Any]], Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Enrich an event with additional data.

    Args:
        event: Event to enrich
        enricher: Function that adds enrichment data

    Returns:
        Enriched event
    """
    enriched = event.copy()
    enrichment = enricher(event)
    enriched["enrichment"] = enrichment
    return enriched


def extract_event_metadata(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract metadata from an event.

    Args:
        event: Event dictionary

    Returns:
        Metadata dictionary
    """
    return {
        "type": event.get("type"),
        "timestamp": event.get("timestamp"),
        "source": event.get("source"),
        "has_element": "element" in event,
        "has_value": "value" in event,
        "keys": list(event.keys()),
    }


def group_events_by_type(
    events: List[Dict[str, Any]]
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Group events by their type.

    Args:
        events: List of events

    Returns:
        Dictionary mapping type to events
    """
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for event in events:
        event_type = event.get("type", "unknown")
        if event_type not in grouped:
            grouped[event_type] = []
        grouped[event_type].append(event)
    return grouped
