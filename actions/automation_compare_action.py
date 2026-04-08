"""Automation Compare Action.

Compares automation configurations, states, and outputs.
"""
from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass


@dataclass
class DiffEntry:
    path: str
    change_type: str
    old_value: Any = None
    new_value: Any = None


@dataclass
class DiffResult:
    diffs: List[DiffEntry]
    added_count: int = 0
    removed_count: int = 0
    changed_count: int = 0

    def __post_init__(self):
        self.added_count = sum(1 for d in self.diffs if d.change_type == "added")
        self.removed_count = sum(1 for d in self.diffs if d.change_type == "removed")
        self.changed_count = sum(1 for d in self.diffs if d.change_type == "changed")

    @property
    def has_differences(self) -> bool:
        return len(self.diffs) > 0

    def summary(self) -> str:
        return f"+{self.added_count} -{self.removed_count} ~{self.changed_count}"


class AutomationCompareAction:
    """Compares automation configs, states, and outputs."""

    def __init__(
        self,
        ignore_keys: Optional[Set[str]] = None,
        deep_compare: bool = True,
    ) -> None:
        self.ignore_keys = ignore_keys or set()
        self.deep_compare = deep_compare

    def diff_dict(
        self,
        old: Dict[str, Any],
        new: Dict[str, Any],
        path: str = "",
    ) -> DiffResult:
        diffs: List[DiffEntry] = []
        all_keys = set(old.keys()) | set(new.keys())
        for key in all_keys:
            if key in self.ignore_keys:
                continue
            current_path = f"{path}.{key}" if path else key
            if key not in old:
                diffs.append(DiffEntry(path=current_path, change_type="added", new_value=new[key]))
            elif key not in new:
                diffs.append(DiffEntry(path=current_path, change_type="removed", old_value=old[key]))
            elif self.deep_compare and isinstance(old[key], dict) and isinstance(new[key], dict):
                sub_diff = self.diff_dict(old[key], new[key], current_path)
                diffs.extend(sub_diff.diffs)
            elif old[key] != new[key]:
                diffs.append(DiffEntry(path=current_path, change_type="changed", old_value=old[key], new_value=new[key]))
        return DiffResult(diffs=diffs)

    def diff(
        self,
        old: Any,
        new: Any,
    ) -> DiffResult:
        if isinstance(old, dict) and isinstance(new, dict):
            return self.diff_dict(old, new)
        else:
            if old == new:
                return DiffResult(diffs=[])
            return DiffResult(diffs=[DiffEntry(path="<root>", change_type="changed", old_value=old, new_value=new)])

    def apply_patch(
        self,
        target: Dict[str, Any],
        diffs: List[DiffEntry],
    ) -> Dict[str, Any]:
        result = dict(target)
        for d in diffs:
            keys = d.path.split(".")
            current = result
            for key in keys[:-1]:
                if key not in current:
                    current[key] = {}
                current = current[key]
            final_key = keys[-1]
            if d.change_type == "added":
                current[final_key] = d.new_value
            elif d.change_type == "removed":
                current.pop(final_key, None)
            elif d.change_type == "changed":
                current[final_key] = d.new_value
        return result
