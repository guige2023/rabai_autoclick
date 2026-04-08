"""
Event Serializer Utilities

Provides utilities for serializing and deserializing
events in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any
from dataclasses import dataclass
import json
from datetime import datetime


@dataclass
class SerializedEvent:
    """Represents a serialized event."""
    type: str
    data: dict[str, Any]
    timestamp: str
    version: str = "1.0"


class EventSerializer:
    """
    Serializes and deserializes events.
    
    Supports JSON serialization with type
    information preservation.
    """

    def __init__(self, version: str = "1.0") -> None:
        self._version = version

    def serialize(self, event_type: str, data: dict[str, Any]) -> str:
        """
        Serialize an event to JSON string.
        
        Args:
            event_type: Type of event.
            data: Event data.
            
        Returns:
            JSON string representation.
        """
        serialized = SerializedEvent(
            type=event_type,
            data=data,
            timestamp=datetime.now().isoformat(),
            version=self._version,
        )
        return json.dumps(serialized.__dict__, default=str)

    def deserialize(self, json_str: str) -> tuple[str, dict[str, Any], str]:
        """
        Deserialize a JSON string to event components.
        
        Args:
            json_str: JSON string to deserialize.
            
        Returns:
            Tuple of (event_type, data, timestamp).
        """
        obj = json.loads(json_str)
        return obj["type"], obj["data"], obj["timestamp"]

    def serialize_batch(
        self,
        events: list[tuple[str, dict[str, Any]]],
    ) -> str:
        """
        Serialize multiple events.
        
        Args:
            events: List of (event_type, data) tuples.
            
        Returns:
            JSON string of all events.
        """
        serialized = [
            {
                "type": event_type,
                "data": data,
                "timestamp": datetime.now().isoformat(),
                "version": self._version,
            }
            for event_type, data in events
        ]
        return json.dumps(serialized, default=str)

    def deserialize_batch(
        self,
        json_str: str,
    ) -> list[tuple[str, dict[str, Any]]]:
        """
        Deserialize multiple events.
        
        Args:
            json_str: JSON string of events.
            
        Returns:
            List of (event_type, data) tuples.
        """
        objs = json.loads(json_str)
        return [(obj["type"], obj["data"]) for obj in objs]


def create_serializer(version: str = "1.0") -> EventSerializer:
    """Create a new EventSerializer instance."""
    return EventSerializer(version=version)
