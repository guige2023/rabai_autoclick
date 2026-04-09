"""Hasher utilities for generating and verifying element hashes.

Provides consistent hashing for UI elements to support
caching, deduplication, and change detection in
automation workflows.

Example:
    >>> from utils.hasher_utils import hash_element, hash_position, incremental_hash
    >>> h = hash_element(element, include_children=True)
    >>> incremental_hash(prev_hash, "click", position=(100, 200))
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional, Tuple, Union


def hash_bytes(data: bytes) -> str:
    """Generate SHA256 hash of bytes.

    Args:
        data: Bytes to hash.

    Returns:
        Hex digest string.
    """
    return hashlib.sha256(data).hexdigest()


def hash_string(s: str) -> str:
    """Generate hash of a string.

    Args:
        s: String to hash.

    Returns:
        Hex digest string.
    """
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def hash_dict(
    d: Dict[str, Any],
    *,
    keys: Optional[List[str]] = None,
    sorted_keys: bool = True,
) -> str:
    """Generate deterministic hash of a dict.

    Args:
        d: Dict to hash.
        keys: Specific keys to include (None = all).
        sorted_keys: Sort keys for deterministic output.

    Returns:
        Hex digest string.
    """
    if keys:
        d = {k: d.get(k) for k in keys if k in d}
    else:
        d = dict(d)

    json_bytes = json.dumps(d, sort_keys=sorted_keys, default=str).encode("utf-8")
    return hashlib.sha256(json_bytes).hexdigest()


def hash_element(
    element: Dict[str, Any],
    *,
    include_children: bool = False,
    include_position: bool = False,
) -> str:
    """Generate a stable hash for a UI element.

    Args:
        element: Element dict.
        include_children: Include children in hash.
        include_position: Include position info (makes hash unstable).

    Returns:
        Element hash hex string.
    """
    parts: Dict[str, Any] = {}

    for key in ["role", "title", "description", "value", "enabled"]:
        if key in element:
            parts[key] = element[key]

    if include_position:
        for key in ["position", "frame", "bounds", "x", "y"]:
            if key in element:
                parts[key] = element[key]

    if include_children and "children" in element:
        parts["children"] = [
            hash_element(child, include_position=include_position)
            for child in element["children"]
        ]

    return hash_dict(parts, sorted_keys=True)


def hash_position(
    x: int,
    y: int,
    screen_w: Optional[int] = None,
    screen_h: Optional[int] = None,
) -> str:
    """Generate a hash for a screen position.

    Args:
        x: X coordinate.
        y: Y coordinate.
        screen_w: Optional screen width for normalization.
        screen_h: Optional screen height for normalization.

    Returns:
        Position hash string.
    """
    data = {"x": x, "y": y}
    if screen_w:
        data["nx"] = x / screen_w
    if screen_h:
        data["ny"] = y / screen_h

    return hash_dict(data)


def incremental_hash(
    previous: Optional[str],
    action: str,
    **params: Any,
) -> str:
    """Create an incremental hash incorporating previous state.

    Args:
        previous: Previous hash (or None for initial).
        action: Action type string.
        **params: Action parameters.

    Returns:
        New hash incorporating previous state.
    """
    data = {
        "prev": previous or "",
        "action": action,
        "params": params,
    }
    return hash_dict(data)


def hash_tree(
    elements: List[Dict[str, Any]],
    root_id: Optional[str] = None,
) -> str:
    """Generate a hash representing an entire element tree.

    Args:
        elements: List of all elements in tree.
        root_id: ID of root element (None = use first no-parent element).

    Returns:
        Tree hash string.
    """
    if not elements:
        return hash_string("")

    if root_id is None:
        for e in elements:
            if not e.get("parent_id"):
                root_id = e.get("id")
                break
        if root_id is None:
            root_id = elements[0].get("id")

    element_map = {e.get("id"): e for e in elements}

    def tree_hash(eid: str) -> str:
        if eid not in element_map:
            return ""
        e = element_map[eid]
        children_ids = [cid for cid in element_map if element_map[cid].get("parent_id") == eid]
        child_hashes = sorted(tree_hash(cid) for cid in children_ids)
        return hash_dict({
            "self": hash_element(e, include_position=False),
            "children": child_hashes,
        })

    return tree_hash(root_id)


def similarity_hash(
    h1: str,
    h2: str,
    *,
    prefix_match: bool = True,
) -> float:
    """Calculate similarity between two hashes.

    Args:
        h1: First hash.
        h2: Second hash.
        prefix_match: Use prefix matching vs byte comparison.

    Returns:
        Similarity score 0.0 to 1.0.
    """
    if h1 == h2:
        return 1.0
    if not h1 or not h2:
        return 0.0

    if prefix_match:
        common_len = 0
        for c1, c2 in zip(h1, h2):
            if c1 == c2:
                common_len += 1
            else:
                break
        return common_len / max(len(h1), len(h2), 1)

    common = sum(1 for a, b in zip(h1, h2) if a == b)
    return common / max(len(h1), len(h2), 1)


def rolling_hash(
    items: List[Any],
    window_size: int = 3,
) -> List[str]:
    """Generate rolling hashes over a sequence.

    Args:
        items: Sequence to hash.
        window_size: Size of rolling window.

    Returns:
        List of hashes, one per window position.
    """
    results: List[str] = []
    for i in range(len(items) - window_size + 1):
        window = items[i : i + window_size]
        results.append(hash_dict({"items": window, "pos": i}))
    return results
