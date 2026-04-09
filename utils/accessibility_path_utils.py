"""
Accessibility path utilities.

This module provides utilities for working with macOS Accessibility (AX) paths,
including path parsing, building, and comparison.
"""

from __future__ import annotations

import re
from typing import List, Optional, Tuple, Dict, Any
from dataclasses import dataclass, field


# Type aliases
AXPathComponent = Tuple[str, int]  # (role, index)
AXPath = List[AXPathComponent]


@dataclass
class ParsedAXPath:
    """A parsed accessibility element path."""
    components: AXPath = field(default_factory=list)
    raw_string: str = ""

    def __str__(self) -> str:
        """Return string representation of the path."""
        if not self.components:
            return ""
        return "/" + "/".join(f"{role}[{idx}]" for role, idx in self.components)

    def __len__(self) -> int:
        return len(self.components)

    def __getitem__(self, index: int) -> AXPathComponent:
        return self.components[index]


def parse_ax_path(path_str: str) -> ParsedAXPath:
    """
    Parse an accessibility path string.

    Args:
        path_str: Path string like "/AXApplication[0]/AXWindow[0]/AXButton[2]"

    Returns:
        ParsedAXPath object.

    Example:
        >>> path = parse_ax_path("/AXApplication[0]/AXWindow[0]/AXButton[2]")
        >>> len(path)
        3
    """
    if not path_str:
        return ParsedAXPath(raw_string=path_str)

    components: AXPath = []
    # Match role[index] patterns
    pattern = re.compile(r"/([A-Za-z]+)\[(\d+)\]")
    matches = pattern.findall(path_str)

    for role, idx_str in matches:
        try:
            idx = int(idx_str)
            components.append((role, idx))
        except ValueError:
            pass

    return ParsedAXPath(components=components, raw_string=path_str)


def build_ax_path(components: AXPath) -> str:
    """
    Build an accessibility path string from components.

    Args:
        components: List of (role, index) tuples.

    Returns:
        Path string like "/AXApplication[0]/AXWindow[0]/AXButton[2]".
    """
    if not components:
        return ""
    return "/" + "/".join(f"{role}[{idx}]" for role, idx in components)


def get_ax_role(path_str: str) -> Optional[str]:
    """
    Extract the role from the last component of a path.

    Args:
        path_str: Accessibility path string.

    Returns:
        Role string or None if path is empty/invalid.
    """
    parsed = parse_ax_path(path_str)
    if not parsed.components:
        return None
    return parsed.components[-1][0]


def get_ax_index(path_str: str) -> Optional[int]:
    """
    Extract the index from the last component of a path.

    Args:
        path_str: Accessibility path string.

    Returns:
        Index integer or None if path is empty/invalid.
    """
    parsed = parse_ax_path(path_str)
    if not parsed.components:
        return None
    return parsed.components[-1][1]


def get_parent_path(path_str: str) -> str:
    """
    Get the parent path (all but the last component).

    Args:
        path_str: Accessibility path string.

    Returns:
        Parent path string.
    """
    parsed = parse_ax_path(path_str)
    if len(parsed.components) <= 1:
        return ""
    parent_components = parsed.components[:-1]
    return build_ax_path(parent_components)


def append_to_path(path_str: str, role: str, index: int) -> str:
    """
    Append a new component to an accessibility path.

    Args:
        path_str: Existing path string.
        role: Role of the new component.
        index: Index of the new component.

    Returns:
        Extended path string.
    """
    if not path_str:
        return f"/{role}[{index}]"
    return f"{path_str}/{role}[{index}]"


def common_path_prefix(path1: str, path2: str) -> str:
    """
    Find the common prefix path between two accessibility paths.

    Args:
        path1: First path string.
        path2: Second path string.

    Returns:
        Common prefix path.
    """
    p1 = parse_ax_path(path1)
    p2 = parse_ax_path(path2)
    common: AXPath = []

    for c1, c2 in zip(p1.components, p2.components):
        if c1 == c2:
            common.append(c1)
        else:
            break

    return build_ax_path(common)


def path_depth(path_str: str) -> int:
    """
    Get the depth (number of components) of a path.

    Args:
        path_str: Accessibility path string.

    Returns:
        Number of path components.
    """
    return len(parse_ax_path(path_str).components)


def path_equals(path1: str, path2: str) -> bool:
    """
    Check if two accessibility paths are equal.

    Args:
        path1: First path string.
        path2: Second path string.

    Returns:
        True if paths are identical.
    """
    return parse_ax_path(path1).components == parse_ax_path(path2).components


def normalize_role(role: str) -> str:
    """
    Normalize an accessibility role string.

    Args:
        role: Role string (case-insensitive).

    Returns:
        Normalized role with AX prefix.
    """
    role = role.strip()
    if not role.startswith("AX"):
        role = "AX" + role
    return role
