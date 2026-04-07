"""Merger utilities for RabAI AutoClick.

Provides:
- Dictionary merging
- List merging
- Record merging
- Deep merging
- Conflict resolution strategies
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Set, Union


def merge_dicts(
    *dicts: Dict[str, Any],
    strategy: str = "override",
) -> Dict[str, Any]:
    """Merge multiple dictionaries.

    Args:
        *dicts: Dictionaries to merge.
        strategy: "override", "keep", "combine".

    Returns:
        Merged dictionary.
    """
    result: Dict[str, Any] = {}
    for d in dicts:
        for key, value in d.items():
            if key in result:
                if strategy == "override":
                    result[key] = value
                elif strategy == "keep":
                    pass
                elif strategy == "combine":
                    if isinstance(result[key], list) and isinstance(value, list):
                        result[key] = result[key] + value
                    else:
                        result[key] = value
            else:
                result[key] = value
    return result


def deep_merge(
    *dicts: Dict[str, Any],
    conflict_strategy: str = "override",
) -> Dict[str, Any]:
    """Deep merge dictionaries.

    Args:
        *dicts: Dictionaries to merge.
        conflict_strategy: "override", "keep", "combine", "custom".

    Returns:
        Deep merged dictionary.
    """
    result: Dict[str, Any] = {}

    for d in dicts:
        for key, value in d.items():
            if key in result:
                if isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = deep_merge(result[key], value, conflict_strategy)
                elif isinstance(result[key], list) and isinstance(value, list):
                    result[key] = result[key] + value
                elif conflict_strategy == "override":
                    result[key] = value
                elif conflict_strategy == "keep":
                    pass
            else:
                result[key] = value

    return result


def merge_lists(
    *lists: List[Any],
    unique: bool = False,
    preserve_order: bool = True,
) -> List[Any]:
    """Merge multiple lists.

    Args:
        *lists: Lists to merge.
        unique: Remove duplicates.
        preserve_order: Maintain insertion order.

    Returns:
        Merged list.
    """
    result: List[Any] = []
    for lst in lists:
        for item in lst:
            if not unique or item not in result:
                result.append(item)
    return result


def merge_records(
    *records: Dict[str, Any],
    key_field: str = "id",
    merge_strategy: str = "override",
) -> List[Dict[str, Any]]:
    """Merge list of records by key field.

    Args:
        *records: Lists of record dictionaries.
        key_field: Field to use as key for merging.
        merge_strategy: "override", "keep", "combine".

    Returns:
        Merged list of records.
    """
    merged: Dict[Any, Dict[str, Any]] = {}

    for record_list in records:
        for record in record_list:
            if key_field not in record:
                continue
            key = record[key_field]
            if key not in merged:
                merged[key] = dict(record)
            else:
                if merge_strategy == "override":
                    merged[key].update(record)
                elif merge_strategy == "combine":
                    for k, v in record.items():
                        if k != key_field:
                            if k in merged[key]:
                                if isinstance(merged[key][k], list) and isinstance(v, list):
                                    merged[key][k] = merged[key][k] + v
                                elif merged[key][k] != v:
                                    merged[key][k] = [merged[key][k], v]
                            else:
                                merged[key][k] = v

    return list(merged.values())


def merge_by_key(
    left: List[Dict[str, Any]],
    right: List[Dict[str, Any]],
    key: str,
    how: str = "left",
    merge_strategy: str = "override",
) -> List[Dict[str, Any]]:
    """Merge two lists of dictionaries by key (SQL-like join).

    Args:
        left: Left list of records.
        right: Right list of records.
        key: Key field for joining.
        how: "left", "right", "inner", "outer".
        merge_strategy: "override", "keep", "combine".

    Returns:
        Merged list of records.
    """
    right_index = {r[key]: r for r in right if key in r}
    result: List[Dict[str, Any]] = []
    left_keys = set()
    right_keys = set()

    for l in left:
        if key not in l:
            continue
        left_keys.add(l[key])
        r = right_index.get(l[key])
        if r is not None:
            right_keys.add(l[key])
            merged = dict(l)
            if merge_strategy == "override":
                merged.update(r)
            else:
                for k, v in r.items():
                    if k != key:
                        if k in merged:
                            if isinstance(merged[k], list) and isinstance(v, list):
                                merged[k] = merged[k] + v
                            else:
                                merged[k] = v
                        else:
                            merged[k] = v
            result.append(merged)
        elif how in ("left", "outer"):
            result.append(dict(l))

    if how in ("right", "outer"):
        for r in right:
            if key not in r:
                continue
            if r[key] not in left_keys:
                result.append(dict(r))

    return result


def reconcile(
    source: Dict[str, Any],
    target: Dict[str, Any],
) -> tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """Compare two dictionaries and find differences.

    Args:
        source: Source dictionary.
        target: Target dictionary.

    Returns:
        Tuple of (added, removed, changed).
    """
    added = {k: target[k] for k in target if k not in source}
    removed = {k: source[k] for k in source if k not in target}
    changed = {
        k: (source[k], target[k])
        for k in source
        if k in target and source[k] != target[k]
    }
    return added, removed, changed
