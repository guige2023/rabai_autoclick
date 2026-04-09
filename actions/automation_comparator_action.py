"""Automation Comparator Action Module.

Provides comparison and diff capabilities for automation workflows,
including structural comparison, semantic diff, and patch generation.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class DiffType(Enum):
    """Types of differences."""
    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"
    UNCHANGED = "unchanged"


@dataclass
class DiffResult:
    """Result of a comparison."""
    equal: bool
    diffs: List[Dict[str, Any]] = field(default_factory=list)
    summary: Dict[str, int] = field(default_factory=dict)


@dataclass
class ComparisonConfig:
    """Configuration for comparisons."""
    ignore_keys: List[str] = field(default_factory=list)
    tolerance: float = 0.0
    deep_compare: bool = True
    case_sensitive: bool = True


class DictComparator:
    """Compares dictionaries with detailed diff."""

    def __init__(self, config: Optional[ComparisonConfig] = None):
        self._config = config or ComparisonConfig()

    def compare(
        self,
        left: Dict[str, Any],
        right: Dict[str, Any],
        path: str = ""
    ) -> DiffResult:
        """Compare two dictionaries."""
        diffs = []

        # Check for removed keys
        for key in left:
            if key in self._config.ignore_keys:
                continue
            current_path = f"{path}.{key}" if path else key

            if key not in right:
                diffs.append({
                    "type": DiffType.REMOVED.value,
                    "path": current_path,
                    "value": left[key]
                })
            elif self._config.deep_compare and isinstance(left[key], dict) and isinstance(right[key], dict):
                sub_result = DictComparator(self._config).compare(left[key], right[key], current_path)
                diffs.extend(sub_result.diffs)
            else:
                if not self._values_equal(left[key], right[key]):
                    diffs.append({
                        "type": DiffType.MODIFIED.value,
                        "path": current_path,
                        "old_value": left[key],
                        "new_value": right[key]
                    })

        # Check for added keys
        for key in right:
            if key in self._config.ignore_keys or key in left:
                continue
            current_path = f"{path}.{key}" if path else key
            diffs.append({
                "type": DiffType.ADDED.value,
                "path": current_path,
                "value": right[key]
            })

        summary = {
            "added": len([d for d in diffs if d["type"] == DiffType.ADDED.value]),
            "removed": len([d for d in diffs if d["type"] == DiffType.REMOVED.value]),
            "modified": len([d for d in diffs if d["type"] == DiffType.MODIFIED.value])
        }

        return DiffResult(equal=len(diffs) == 0, diffs=diffs, summary=summary)

    def _values_equal(self, left: Any, right: Any) -> bool:
        """Check if two values are equal."""
        if isinstance(left, (int, float)) and isinstance(right, (int, float)):
            if self._config.tolerance > 0:
                return abs(left - right) <= self._config.tolerance
            return left == right

        if not self._config.case_sensitive and isinstance(left, str) and isinstance(right, str):
            return left.lower() == right.lower()

        return left == right


class ListComparator:
    """Compares lists with order-aware and order-agnostic modes."""

    def __init__(self, order_matters: bool = False):
        self._order_matters = order_matters

    def compare(
        self,
        left: List[Any],
        right: List[Any]
    ) -> DiffResult:
        """Compare two lists."""
        if not self._order_matters:
            return self._compare_set(left, right)
        return self._compare_sequential(left, right)

    def _compare_set(
        self,
        left: List[Any],
        right: List[Any]
    ) -> DiffResult:
        """Compare lists as sets (order-agnostic)."""
        left_set = set(str(x) for x in left)
        right_set = set(str(x) for x in right)

        added = right_set - left_set
        removed = left_set - right_set

        diffs = [
            {"type": DiffType.ADDED.value, "value": x}
            for x in added
        ] + [
            {"type": DiffType.REMOVED.value, "value": x}
            for x in removed
        ]

        summary = {
            "added": len(added),
            "removed": len(removed),
            "modified": 0
        }

        return DiffResult(equal=len(diffs) == 0, diffs=diffs, summary=summary)

    def _compare_sequential(
        self,
        left: List[Any],
        right: List[Any]
    ) -> DiffResult:
        """Compare lists sequentially with index tracking."""
        diffs = []
        max_len = max(len(left), len(right))

        for i in range(max_len):
            if i >= len(left):
                diffs.append({
                    "type": DiffType.ADDED.value,
                    "index": i,
                    "value": right[i]
                })
            elif i >= len(right):
                diffs.append({
                    "type": DiffType.REMOVED.value,
                    "index": i,
                    "value": left[i]
                })
            elif left[i] != right[i]:
                diffs.append({
                    "type": DiffType.MODIFIED.value,
                    "index": i,
                    "old_value": left[i],
                    "new_value": right[i]
                })

        summary = {
            "added": len([d for d in diffs if d["type"] == DiffType.ADDED.value]),
            "removed": len([d for d in diffs if d["type"] == DiffType.REMOVED.value]),
            "modified": len([d for d in diffs if d["type"] == DiffType.MODIFIED.value])
        }

        return DiffResult(equal=len(diffs) == 0, diffs=diffs, summary=summary)


class SemanticComparer:
    """Performs semantic comparison beyond exact matching."""

    def __init__(self):
        self._custom_comparators: Dict[str, Callable] = {}

    def register_comparator(
        self,
        data_type: str,
        comparator: Callable[[Any, Any], bool]
    ) -> None:
        """Register a custom comparator for a data type."""
        self._custom_comparators[data_type] = comparator

    def compare(
        self,
        left: Any,
        right: Any
    ) -> bool:
        """Compare two values semantically."""
        # Check custom comparators
        left_type = type(left).__name__
        if left_type in self._custom_comparators:
            return self._custom_comparators[left_type](left, right)

        # Default comparison
        return left == right


class PatchGenerator:
    """Generates patches for applying differences."""

    @staticmethod
    def generate_patch(
        source: Dict[str, Any],
        diffs: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Generate a patch from source and diffs."""
        patch = []

        for diff in diffs:
            if diff["type"] == DiffType.ADDED.value:
                patch.append({
                    "op": "add",
                    "path": diff.get("path", ""),
                    "value": diff.get("value")
                })
            elif diff["type"] == DiffType.REMOVED.value:
                patch.append({
                    "op": "remove",
                    "path": diff.get("path", "")
                })
            elif diff["type"] == DiffType.MODIFIED.value:
                patch.append({
                    "op": "replace",
                    "path": diff.get("path", ""),
                    "value": diff.get("new_value")
                })

        return patch

    @staticmethod
    def apply_patch(
        source: Dict[str, Any],
        patch: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Apply a patch to source data."""
        result = dict(source)

        for op in patch:
            path = op.get("path", "")
            keys = path.split(".") if path else []

            if op["op"] == "add":
                if keys:
                    target = result
                    for key in keys[:-1]:
                        target = target.setdefault(key, {})
                    target[keys[-1]] = op["value"]
                else:
                    result = op["value"]

            elif op["op"] == "remove":
                if keys:
                    target = result
                    for key in keys[:-1]:
                        target = target.get(key, {})
                    target.pop(keys[-1], None)

            elif op["op"] == "replace":
                if keys:
                    target = result
                    for key in keys[:-1]:
                        target = target.setdefault(key, {})
                    target[keys[-1]] = op["value"]
                else:
                    result = op["value"]

        return result


class AutomationComparatorAction:
    """Main action class for automation comparison."""

    def __init__(self):
        self._dict_comparator = DictComparator()
        self._list_comparator = ListComparator()
        self._semantic_comparer = SemanticComparer()

    async def execute(
        self,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the automation comparator action.

        Args:
            context: Dictionary containing:
                - operation: Operation to perform
                - Other operation-specific fields

        Returns:
            Dictionary with comparison results.
        """
        operation = context.get("operation", "compare_dict")

        if operation == "compare_dict":
            left = context.get("left", {})
            right = context.get("right", {})

            config = ComparisonConfig(
                ignore_keys=context.get("ignore_keys", []),
                tolerance=context.get("tolerance", 0.0),
                deep_compare=context.get("deep_compare", True)
            )
            comparator = DictComparator(config)
            result = comparator.compare(left, right)

            return {
                "success": True,
                "equal": result.equal,
                "diffs": result.diffs,
                "summary": result.summary
            }

        elif operation == "compare_list":
            left = context.get("left", [])
            right = context.get("right", [])
            order_matters = context.get("order_matters", False)

            comparator = ListComparator(order_matters)
            result = comparator.compare(left, right)

            return {
                "success": True,
                "equal": result.equal,
                "diffs": result.diffs,
                "summary": result.summary
            }

        elif operation == "compare_semantic":
            left = context.get("left")
            right = context.get("right")

            equal = self._semantic_comparer.compare(left, right)
            return {
                "success": True,
                "equal": equal
            }

        elif operation == "generate_patch":
            source = context.get("source", {})
            diffs = context.get("diffs", [])

            patch = PatchGenerator.generate_patch(source, diffs)
            return {
                "success": True,
                "patch": patch
            }

        elif operation == "apply_patch":
            source = context.get("source", {})
            patch = context.get("patch", [])

            result = PatchGenerator.apply_patch(source, patch)
            return {
                "success": True,
                "result": result
            }

        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
