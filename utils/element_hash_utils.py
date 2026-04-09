"""
Element Hash Utilities

Generate stable, content-based hashes for UI elements from their
accessibility properties. Used for element-level caching and deduplication.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Optional


@dataclass
class ElementHash:
    """A stable hash for a UI element."""
    full_hash: str  # 32-char hex
    role_hash: str  # 8-char hex
    label_hash: str  # 8-char hex
    value_hash: str  # 8-char hex
    bounds_hash: str  # 8-char hex

    @property
    def short_hash(self) -> str:
        """Return a shortened 8-char hash for display."""
        return self.full_hash[:8]


def hash_string(value: str, prefix: str = "") -> str:
    """Compute a SHA256 hash of a string, returning first 8 chars."""
    data = (prefix + value).encode("utf-8")
    return hashlib.sha256(data).hexdigest()[:8]


def compute_element_hash(
    role: str,
    label: str = "",
    value: str = "",
    bounds: Optional[tuple[float, float, float, float]] = None,
    attributes: Optional[dict] = None,
) -> ElementHash:
    """
    Compute a stable multi-part hash for a UI element.

    The hash is deterministic: the same element always produces the same hash
    regardless of runtime environment or transient properties.

    Args:
        role: Accessibility role (e.g., 'button', 'textfield').
        label: Accessibility label / name.
        value: Current value of the element.
        bounds: Bounding box (x, y, width, height).
        attributes: Additional attributes for hashing.

    Returns:
        ElementHash with full and component hashes.
    """
    role_hash = hash_string(role, prefix="role:")
    label_hash = hash_string(label, prefix="label:")
    value_hash = hash_string(value, prefix="value:")

    if bounds:
        bounds_str = f"bounds:{bounds[0]:.1f},{bounds[1]:.1f},{bounds[2]:.1f},{bounds[3]:.1f}"
    else:
        bounds_str = "bounds:None"
    bounds_hash = hashlib.sha256(bounds_str.encode()).hexdigest()[:8]

    if attributes:
        sorted_attrs = sorted(attributes.items())
        attr_str = "|".join(f"{k}={v}" for k, v in sorted_attrs if v)
        extra_hash = hash_string(attr_str, prefix="attr:")
    else:
        extra_hash = "0" * 8

    combined = f"{role_hash}:{label_hash}:{value_hash}:{bounds_hash}:{extra_hash}"
    full = hashlib.sha256(combined.encode()).hexdigest()

    return ElementHash(
        full_hash=full,
        role_hash=role_hash,
        label_hash=label_hash,
        value_hash=value_hash,
        bounds_hash=bounds_hash,
    )


def are_elements_equivalent(hash_a: ElementHash, hash_b: ElementHash) -> bool:
    """Check if two element hashes refer to the same element class."""
    return (hash_a.role_hash == hash_b.role_hash
            and hash_a.label_hash == hash_b.label_hash)
