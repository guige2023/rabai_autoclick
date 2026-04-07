"""Audit trail utilities: track and log changes to entities with immutable history."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "AuditEntry",
    "AuditTrail",
    "auditable",
]


@dataclass
class AuditEntry:
    """An immutable audit trail entry."""

    id: str
    entity_type: str
    entity_id: str
    action: str
    changes: dict[str, Any]
    actor: str = "system"
    timestamp: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        entity_type: str,
        entity_id: str,
        action: str,
        changes: dict[str, Any],
        actor: str = "system",
        **kwargs: Any,
    ) -> "AuditEntry":
        return cls(
            id=uuid.uuid4().hex,
            entity_type=entity_type,
            entity_id=entity_id,
            action=action,
            changes=changes,
            actor=actor,
            **kwargs,
        )


class AuditTrail:
    """Immutable audit trail for tracking entity changes."""

    def __init__(self) -> None:
        self._entries: list[AuditEntry] = []

    def record(
        self,
        entity_type: str,
        entity_id: str,
        action: str,
        changes: dict[str, Any],
        actor: str = "system",
        **kwargs: Any,
    ) -> AuditEntry:
        entry = AuditEntry.create(
            entity_type=entity_type,
            entity_id=entity_id,
            action=action,
            changes=changes,
            actor=actor,
            **kwargs,
        )
        self._entries.append(entry)
        return entry

    def get_for_entity(
        self,
        entity_type: str,
        entity_id: str,
    ) -> list[AuditEntry]:
        return [
            e for e in self._entries
            if e.entity_type == entity_type and e.entity_id == entity_id
        ]

    def get_by_actor(self, actor: str) -> list[AuditEntry]:
        return [e for e in self._entries if e.actor == actor]

    def get_by_action(self, action: str) -> list[AuditEntry]:
        return [e for e in self._entries if e.action == action]

    def export(self) -> list[dict[str, Any]]:
        return [
            {
                "id": e.id,
                "entity_type": e.entity_type,
                "entity_id": e.entity_id,
                "action": e.action,
                "changes": e.changes,
                "actor": e.actor,
                "timestamp": e.timestamp,
                "metadata": e.metadata,
            }
            for e in self._entries
        ]


def auditable(cls: type) -> type:
    """Class decorator to add audit trail support."""
    original_init = cls.__init__

    def new_init(self, *args: Any, **kwargs: Any) -> None:
        original_init(self, *args, **kwargs)
        if not hasattr(self, "_audit_trail"):
            self._audit_trail = AuditTrail()

    cls.__init__ = new_init
    return cls
