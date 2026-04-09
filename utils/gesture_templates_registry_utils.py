"""
Gesture Templates Registry Utilities

Manage a registry of gesture templates for matching and recognition.
Supports CRUD operations, template versioning, and similarity lookup.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import uuid
import time
import json
from dataclasses import dataclass, field
from typing import Optional, List, Callable


@dataclass
class GestureTemplate:
    """A registered gesture template."""
    template_id: str
    name: str
    gesture_type: str  # 'swipe', 'tap', 'long_press', 'pinch', etc.
    points: List[tuple[float, float]]  # normalized coordinates
    metadata: dict = field(default_factory=dict)
    version: int = 1
    created_at_ms: float = field(default_factory=lambda: time.time() * 1000)
    updated_at_ms: float = field(default_factory=lambda: time.time() * 1000)


class GestureTemplatesRegistry:
    """Registry for managing gesture templates."""

    def __init__(self):
        self._templates: dict[str, GestureTemplate] = {}
        self._name_index: dict[str, str] = {}  # name -> template_id
        self._type_index: dict[str, list[str]] = {}  # gesture_type -> [template_ids]

    def register(
        self,
        name: str,
        gesture_type: str,
        points: List[tuple[float, float]],
        metadata: Optional[dict] = None,
    ) -> GestureTemplate:
        """Register a new gesture template."""
        template_id = str(uuid.uuid4())[:8]
        template = GestureTemplate(
            template_id=template_id,
            name=name,
            gesture_type=gesture_type,
            points=points,
            metadata=metadata or {},
        )
        self._templates[template_id] = template
        self._name_index[name] = template_id
        self._type_index.setdefault(gesture_type, []).append(template_id)
        return template

    def get(self, template_id: str) -> Optional[GestureTemplate]:
        """Get a template by ID."""
        return self._templates.get(template_id)

    def get_by_name(self, name: str) -> Optional[GestureTemplate]:
        """Get a template by name."""
        tid = self._name_index.get(name)
        return self._templates.get(tid) if tid else None

    def get_by_type(self, gesture_type: str) -> List[GestureTemplate]:
        """Get all templates of a given gesture type."""
        tids = self._type_index.get(gesture_type, [])
        return [self._templates[tid] for tid in tids if tid in self._templates]

    def update(
        self,
        template_id: str,
        points: Optional[List[tuple[float, float]]] = None,
        metadata: Optional[dict] = None,
    ) -> Optional[GestureTemplate]:
        """Update an existing template."""
        template = self._templates.get(template_id)
        if not template:
            return None
        if points is not None:
            template.points = points
        if metadata is not None:
            template.metadata.update(metadata)
        template.version += 1
        template.updated_at_ms = time.time() * 1000
        return template

    def delete(self, template_id: str) -> bool:
        """Delete a template from the registry."""
        template = self._templates.pop(template_id, None)
        if not template:
            return False
        self._name_index.pop(template.name, None)
        if template.gesture_type in self._type_index:
            self._type_index[template.gesture_type].remove(template_id)
        return True

    def list_all(self) -> List[GestureTemplate]:
        """List all registered templates."""
        return list(self._templates.values())

    def export_to_json(self) -> str:
        """Export all templates as JSON."""
        return json.dumps(
            [t.__dict__ for t in self._templates.values()],
            ensure_ascii=False,
        )
