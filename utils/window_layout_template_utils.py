"""Window Layout Template Utilities.

Manages window layout templates for predefined arrangements.

Example:
    >>> from window_layout_template_utils import WindowLayoutTemplate
    >>> tmpl = WindowLayoutTemplate.load("dual_monitor")
    >>> tmpl.apply()
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class LayoutSlot:
    """A slot in a layout template."""
    index: int
    x: int
    y: int
    width: int
    height: int
    app_hint: Optional[str] = None


@dataclass
class WindowLayoutTemplate:
    """A window layout template."""
    name: str
    display_count: int = 1
    slots: List[LayoutSlot] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def load(cls, name: str, path: Optional[Path] = None) -> WindowLayoutTemplate:
        """Load a template from disk.

        Args:
            name: Template name.
            path: Optional path to templates directory.

        Returns:
            WindowLayoutTemplate instance.
        """
        p = (path or Path("~/.window_layouts")).expanduser() / f"{name}.json"
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        slots = [LayoutSlot(**s) for s in data.get("slots", [])]
        return cls(
            name=data["name"],
            display_count=data.get("display_count", 1),
            slots=slots,
            metadata=data.get("metadata", {}),
        )

    def save(self, path: Optional[Path] = None) -> None:
        """Save template to disk.

        Args:
            path: Optional path to templates directory.
        """
        p = (path or Path("~/.window_layouts")).expanduser()
        p.mkdir(parents=True, exist_ok=True)
        filepath = p / f"{self.name}.json"
        data = {
            "name": self.name,
            "display_count": self.display_count,
            "slots": [
                {"index": s.index, "x": s.x, "y": s.y, "width": s.width, "height": s.height, "app_hint": s.app_hint}
                for s in self.slots
            ],
            "metadata": self.metadata,
        }
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def get_slot(self, index: int) -> Optional[LayoutSlot]:
        """Get a slot by index."""
        for slot in self.slots:
            if slot.index == index:
                return slot
        return None
