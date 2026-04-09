"""
Palm Rejection Utilities for UI Automation.

This module provides utilities for detecting and filtering
palm/wrist contacts during touch input in UI automation.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Set, Tuple
from enum import Enum


class ContactType(Enum):
    """Types of touch contact."""
    FINGER = "finger"
    THUMB = "thumb"
    PALM = "palm"
    STYLUS = "stylus"
    UNKNOWN = "unknown"


@dataclass
class TouchContact:
    """Represents a touch contact point."""
    id: int
    x: float
    y: float
    major_axis: float = 10.0
    minor_axis: float = 10.0
    angle: float = 0.0
    pressure: float = 0.5
    timestamp: float = 0.0
    contact_type: ContactType = ContactType.UNKNOWN
    rejected: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RejectionConfig:
    """Configuration for palm rejection."""
    max_major_axis: float = 60.0
    max_contact_area: float = 2000.0
    min_pressure: float = 0.05
    max_angle_deviation: float = 45.0
    palm_rejection_enabled: bool = True
    thumb_detection_enabled: bool = True


class PalmRejectionFilter:
    """Filters out palm and accidental touches from touch input."""

    def __init__(self, config: Optional[RejectionConfig] = None) -> None:
        self._config = config or RejectionConfig()
        self._contact_history: Dict[int, List[TouchContact]] = {}
        self._rejected_ids: Set[int] = set()
        self._active_contacts: Dict[int, TouchContact] = {}

    def set_config(self, config: RejectionConfig) -> None:
        """Update the rejection configuration."""
        self._config = config

    def get_config(self) -> RejectionConfig:
        """Get the current rejection configuration."""
        return self._config

    def process_contacts(
        self,
        contacts: List[TouchContact],
    ) -> List[TouchContact]:
        """Process a list of contacts and return filtered results."""
        filtered: List[TouchContact] = []

        for contact in contacts:
            if self._should_reject(contact):
                contact.rejected = True
                self._rejected_ids.add(contact.id)
            else:
                contact.rejected = False

            filtered.append(contact)
            self._active_contacts[contact.id] = contact

            if contact.id not in self._contact_history:
                self._contact_history[contact.id] = []
            self._contact_history[contact.id].append(contact)

        return filtered

    def _should_reject(self, contact: TouchContact) -> bool:
        """Determine if a contact should be rejected as palm/wrist."""
        if not self._config.palm_rejection_enabled:
            return False

        if contact.id in self._rejected_ids:
            return True

        if contact.major_axis > self._config.max_major_axis:
            return True

        area = contact.major_axis * contact.minor_axis * 3.14159
        if area > self._config.max_contact_area:
            return True

        if contact.pressure < self._config.min_pressure:
            return True

        return False

    def is_rejected(self, contact_id: int) -> bool:
        """Check if a contact ID has been rejected."""
        return contact_id in self._rejected_ids

    def get_active_contact_ids(self) -> Set[int]:
        """Get IDs of all currently active contacts."""
        return set(self._active_contacts.keys())

    def remove_contact(self, contact_id: int) -> None:
        """Remove a contact from active tracking."""
        self._active_contacts.pop(contact_id, None)

    def reset(self) -> None:
        """Reset all tracking state."""
        self._active_contacts.clear()
        self._rejected_ids.clear()
        self._contact_history.clear()

    def get_contact_history(self, contact_id: int) -> List[TouchContact]:
        """Get the full history for a contact ID."""
        return list(self._contact_history.get(contact_id, []))

    def estimate_contact_type(
        self,
        contact: TouchContact,
    ) -> ContactType:
        """Estimate the type of contact based on geometry."""
        area = contact.major_axis * contact.minor_axis * 3.14159

        if area > self._config.max_contact_area * 0.8:
            return ContactType.PALM
        if area > self._config.max_major_axis * 3:
            return ContactType.PALM

        if contact.major_axis < 15 and contact.pressure > 0.3:
            return ContactType.THUMB

        return ContactType.FINGER


def create_touch_contact(
    contact_id: int,
    x: float,
    y: float,
    **kwargs: Any,
) -> TouchContact:
    """Create a touch contact with the specified parameters."""
    return TouchContact(id=contact_id, x=x, y=y, **kwargs)
