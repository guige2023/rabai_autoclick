"""
Data class merge and diff utilities.

Provides deep merging, field-level diffing,
and patch application for dataclasses.
"""

from __future__ import annotations

import dataclasses
from typing import Any, TypeVar


T = TypeVar("T")


def deep_merge(base: dict, override: dict) -> dict:
    """
    Deep merge two dictionaries.

    Args:
        base: Base dictionary
        override: Override dictionary

    Returns:
        Merged dictionary (new dict, original unchanged)
    """
    result = dict(base)
    for key, value in override.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def merge_dataclasses(base: T, override: dict[str, Any]) -> T:
    """
    Merge override dict into dataclass instance.

    Args:
        base: Base dataclass instance
        override: Field overrides

    Returns:
        New instance with merged values
    """
    if not dataclasses.is_dataclass(base):
        raise TypeError("base must be a dataclass")
    kwargs = {f.name: getattr(base, f.name) for f in dataclasses.fields(base)}
    for key, value in override.items():
        if key in kwargs:
            if isinstance(kwargs[key], dict) and isinstance(value, dict):
                kwargs[key] = deep_merge(kwargs[key], value)
            else:
                kwargs[key] = value
    return type(base)(**kwargs)


@dataclass
class DiffEntry:
    """Single field diff."""
    field: str
    old_value: Any
    new_value: Any
    change_type: str = "modified"


@dataclass
class DiffResult:
    """Result of diffing two objects."""
    changed: bool
    entries: list[DiffEntry]
    added: list[str]
    removed: list[str]


def diff_dicts(
    old: dict[str, Any],
    new: dict[str, Any],
) -> DiffResult:
    """
    Compute field-level diff between two dicts.

    Args:
        old: Original dict
        new: Updated dict

    Returns:
        DiffResult with all changes
    """
    entries = []
    added = []
    removed = []

    all_keys = set(old.keys()) | set(new.keys())
    for key in all_keys:
        if key not in old:
            added.append(key)
        elif key not in new:
            removed.append(key)
        elif old[key] != new[key]:
            entries.append(DiffEntry(
                field=key,
                old_value=old[key],
                new_value=new[key],
            ))

    return DiffResult(
        changed=bool(entries or added or removed),
        entries=entries,
        added=added,
        removed=removed,
    )


def diff_dataclasses(old: T, new: T) -> DiffResult:
    """
    Diff two dataclass instances.

    Args:
        old: Original instance
        new: Updated instance

    Returns:
        DiffResult
    """
    old_dict = {f.name: getattr(old, f.name) for f in dataclasses.fields(old)}
    new_dict = {f.name: getattr(new, f.name) for f in dataclasses.fields(new)}
    return diff_dicts(old_dict, new_dict)


def apply_patch(base: dict, patch: dict) -> dict:
    """
    Apply patch dict to base dict.

    Args:
        base: Base dictionary
        patch: Patch with changes

    Returns:
        Patched dictionary
    """
    return deep_merge(base, patch)


def apply_diff(base: dict, diff: DiffResult) -> dict:
    """
    Apply DiffResult to base dictionary.

    Args:
        base: Base dictionary
        diff: DiffResult

    Returns:
        Modified dictionary
    """
    result = dict(base)
    for entry in diff.entries:
        result[entry.field] = entry.new_value
    for field in diff.added:
        if field in diff.entries:
            continue
    for field in diff.removed:
        result.pop(field, None)
    return result


def reverse_patch(patch: dict) -> dict:
    """
    Create reverse of a patch dict.

    Args:
        patch: Forward patch

    Returns:
        Reverse patch
    """
    reversed_patch: dict[str, Any] = {}
    for key, value in patch.items():
        if isinstance(value, dict):
            reversed_patch[key] = reverse_patch(value)
        else:
            reversed_patch[key] = "__original_value_needed__"
    return reversed_patch


def is_superset(superset: dict, subset: dict) -> bool:
    """
    Check if superset contains all keys/values of subset.

    Args:
        superset: Larger dict
        subset: Smaller dict

    Returns:
        True if superset contains subset
    """
    for key, value in subset.items():
        if key not in superset:
            return False
        if isinstance(value, dict) and isinstance(superset[key], dict):
            if not is_superset(superset[key], value):
                return False
        elif superset[key] != value:
            return False
    return True
