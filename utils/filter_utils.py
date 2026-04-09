"""Filter utilities for selecting and filtering UI elements.

Provides filtering, sorting, and ranking functions for
automation element selection based on properties,
visibility, and affinity scores.

Example:
    >>> from utils.filter_utils import filter_by, rank_elements, dedupe_elements
    >>> visible = filter_by(elements, visible=True, enabled=True)
    >>> ranked = rank_elements(visible, affinity_fn=score_element)
"""

from __future__ import annotations

from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
)

T = TypeVar("T")


def filter_by(
    items: List[Dict[str, Any]],
    *,
    visible: Optional[bool] = None,
    enabled: Optional[bool] = None,
    role: Optional[str] = None,
    text_contains: Optional[str] = None,
    text_matches: Optional[str] = None,
    custom: Optional[Callable[[Dict[str, Any]], bool]] = None,
) -> List[Dict[str, Any]]:
    """Filter elements by common properties.

    Args:
        items: List of element dicts.
        visible: Filter by visibility.
        enabled: Filter by enabled state.
        role: Filter by accessibility role.
        text_contains: Element text must contain this substring.
        text_matches: Element text must match this regex pattern.
        custom: Custom filter function.

    Returns:
        Filtered list of elements.
    """
    import re

    results = list(items)

    if visible is not None:
        results = [e for e in results if e.get("visible", True) == visible]

    if enabled is not None:
        results = [e for e in results if e.get("enabled", True) == enabled]

    if role is not None:
        results = [e for e in results if e.get("role", "").lower() == role.lower()]

    if text_contains is not None:
        results = [
            e for e in results
            if text_contains.lower() in e.get("title", "").lower()
            or text_contains.lower() in e.get("description", "").lower()
        ]

    if text_matches is not None:
        pattern = re.compile(text_matches, re.IGNORECASE)
        results = [
            e for e in results
            if pattern.search(e.get("title", "") or "")
            or pattern.search(e.get("description", "") or "")
        ]

    if custom is not None:
        results = [e for e in results if custom(e)]

    return results


def rank_elements(
    items: List[Dict[str, Any]],
    *,
    affinity_fn: Optional[Callable[[Dict[str, Any]], float]] = None,
    top_k: Optional[int] = None,
) -> List[Tuple[Dict[str, Any], float]]:
    """Rank elements by affinity score.

    Args:
        items: List of element dicts.
        affinity_fn: Function returning score (higher = better).
        top_k: Return only top K elements.

    Returns:
        List of (element, score) tuples sorted by score descending.
    """
    if affinity_fn is None:
        affinity_fn = lambda e: e.get("score", 0.0)

    scored = [(item, affinity_fn(item)) for item in items]
    scored.sort(key=lambda x: -x[1])

    if top_k is not None:
        scored = scored[:top_k]

    return scored


def sort_elements(
    items: List[Dict[str, Any]],
    *,
    key: str,
    reverse: bool = False,
    nulls_last: bool = True,
) -> List[Dict[str, Any]]:
    """Sort elements by a property.

    Args:
        items: List of element dicts.
        key: Property name to sort by.
        reverse: Sort descending.
        nulls_last: Put None values at end.

    Returns:
        Sorted list.
    """
    def sort_key(e: Dict[str, Any]) -> Tuple[bool, Any]:
        val = e.get(key)
        if val is None:
            return (nulls_last, "")
        return (False, val)

    return sorted(items, key=sort_key, reverse=reverse)


def dedupe_elements(
    items: List[Dict[str, Any]],
    *,
    key: Optional[Callable[[Dict[str, Any]], Any]] = None,
    strategy: str = "first",
) -> List[Dict[str, Any]]:
    """Remove duplicate elements.

    Args:
        items: List of element dicts.
        key: Key function for identity (default: element's "id" or "hash").
        strategy: "first" keeps first occurrence, "last" keeps last.

    Returns:
        Deduplicated list.
    """
    if key is None:
        key = lambda e: (e.get("id"), e.get("hash"))

    seen: Dict[Any, Dict[str, Any]] = {}
    for item in items:
        k = key(item)
        if strategy == "first":
            if k not in seen:
                seen[k] = item
        else:
            seen[k] = item

    return list(seen.values())


def find_ancestor(
    element: Dict[str, Any],
    elements: List[Dict[str, Any]],
    *,
    role: Optional[str] = None,
    max_depth: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """Find an ancestor element matching criteria.

    Args:
        element: Starting element.
        elements: All elements (to search in).
        role: Required ancestor role.
        max_depth: Maximum depth to search.

    Returns:
        Matching ancestor or None.
    """
    parent_id = element.get("parent_id")
    if not parent_id:
        return None

    depth = 0
    current_id = parent_id

    while current_id and (max_depth is None or depth < max_depth):
        for e in elements:
            if e.get("id") == current_id:
                if role is None or e.get("role", "").lower() == role.lower():
                    return e
                current_id = e.get("parent_id")
                depth += 1
                break
        else:
            break

    return None


def find_children(
    element: Dict[str, Any],
    elements: List[Dict[str, Any]],
    *,
    role: Optional[str] = None,
    recursive: bool = False,
) -> List[Dict[str, Any]]:
    """Find child elements matching criteria.

    Args:
        element: Parent element.
        elements: All elements.
        role: Filter by role.
        recursive: Search recursively.

    Returns:
        List of matching child elements.
    """
    eid = element.get("id")
    children: List[Dict[str, Any]] = []

    for e in elements:
        if e.get("parent_id") == eid:
            if role is None or e.get("role", "").lower() == role.lower():
                children.append(e)
            if recursive:
                children.extend(find_children(e, elements, role=role, recursive=True))

    return children


def nearest_to_point(
    items: List[Dict[str, Any]],
    point: Tuple[int, int],
    *,
    key: str = "position",
) -> Optional[Dict[str, Any]]:
    """Find element nearest to a point.

    Args:
        items: Elements with position info.
        point: (x, y) reference point.
        key: Key containing position tuple in element.

    Returns:
        Nearest element or None.
    """
    px, py = point
    best: Optional[Dict[str, Any]] = None
    best_dist = float("inf")

    for item in items:
        pos = item.get(key)
        if not pos:
            continue
        if isinstance(pos, (list, tuple)) and len(pos) >= 2:
            ex, ey = pos[0], pos[1]
            dist = ((ex - px) ** 2 + (ey - py) ** 2) ** 0.5
            if dist < best_dist:
                best_dist = dist
                best = item

    return best


def within_bounds(
    element: Dict[str, Any],
    bounds: Tuple[int, int, int, int],
) -> bool:
    """Check if element is within screen bounds.

    Args:
        element: Element with "position" or "frame" key.
        bounds: (x, y, width, height) bounding rect.

    Returns:
        True if element is fully within bounds.
    """
    bx, by, bw, bh = bounds
    pos = element.get("position") or element.get("frame")
    if not pos or len(pos) < 4:
        return False

    ex, ey, ew, eh = pos[0], pos[1], pos[2], pos[3]
    return (
        ex >= bx
        and ey >= by
        and (ex + ew) <= (bx + bw)
        and (ey + eh) <= (by + bh)
    )
