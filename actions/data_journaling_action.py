"""Data Journaling Action.

Provides append-only journaling for data changes and audit trails.
"""
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
import time
import uuid
import json


@dataclass
class JournalEntry:
    entry_id: str
    timestamp: float
    action: str
    entity_type: str
    entity_id: str
    payload: Dict[str, Any]
    user: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "timestamp": self.timestamp,
            "timestamp_iso": datetime.fromtimestamp(self.timestamp).isoformat(),
            "action": self.action,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "payload": self.payload,
            "user": self.user,
            "metadata": self.metadata,
        }


class DataJournalingAction:
    """Append-only journal for tracking data changes."""

    def __init__(self, entity_types: Optional[List[str]] = None) -> None:
        self.entries: List[JournalEntry] = []
        self.entity_types = set(entity_types or [])

    def append(
        self,
        action: str,
        entity_type: str,
        entity_id: str,
        payload: Optional[Dict[str, Any]] = None,
        user: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> JournalEntry:
        entry = JournalEntry(
            entry_id=uuid.uuid4().hex,
            timestamp=time.time(),
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            payload=payload or {},
            user=user,
            metadata=metadata or {},
        )
        self.entries.append(entry)
        return entry

    def get_entries(
        self,
        entity_type: Optional[str] = None,
        entity_id: Optional[str] = None,
        action: Optional[str] = None,
        since: Optional[float] = None,
        limit: int = 100,
    ) -> List[JournalEntry]:
        result = self.entries
        if entity_type:
            result = [e for e in result if e.entity_type == entity_type]
        if entity_id:
            result = [e for e in result if e.entity_id == entity_id]
        if action:
            result = [e for e in result if e.action == action]
        if since:
            result = [e for e in result if e.timestamp >= since]
        return result[-limit:]

    def get_timeline(
        self,
        entity_type: str,
        entity_id: str,
    ) -> List[Dict[str, Any]]:
        return [e.to_dict() for e in self.entries
                if e.entity_type == entity_type and e.entity_id == entity_id]

    def replay(
        self,
        entity_type: str,
        entity_id: str,
    ) -> List[Dict[str, Any]]:
        timeline = self.get_timeline(entity_type, entity_id)
        state: Dict[str, Any] = {}
        replay_log = []
        for entry in timeline:
            op = entry["action"]
            payload = entry["payload"]
            if op == "create":
                state.update(payload)
            elif op == "update":
                state.update(payload)
            elif op == "delete":
                state = {}
            replay_log.append({"step": entry, "state": dict(state)})
        return replay_log
