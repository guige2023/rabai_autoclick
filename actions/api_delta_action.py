"""
API Delta Action Module.

Handles incremental updates and delta synchronization for API responses,
tracking changes between states and enabling efficient partial updates.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
import hashlib
import json
import logging

logger = logging.getLogger(__name__)


class DeltaType(Enum):
    """Types of delta changes."""
    ADDED = auto()
    REMOVED = auto()
    MODIFIED = auto()
    UNCHANGED = auto()


@dataclass
class Delta:
    """Represents a single change in a delta update."""
    path: str
    delta_type: DeltaType
    old_value: Any = None
    new_value: Any = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        """Convert delta to dictionary representation."""
        return {
            "path": self.path,
            "delta_type": self.delta_type.name,
            "old_value": self.old_value,
            "new_value": self.new_value,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class DeltaResult:
    """Result of a delta computation."""
    deltas: List[Delta]
    added_count: int = 0
    removed_count: int = 0
    modified_count: int = 0
    unchanged_count: int = 0

    def __post_init__(self):
        """Calculate delta counts after initialization."""
        for delta in self.deltas:
            if delta.delta_type == DeltaType.ADDED:
                self.added_count += 1
            elif delta.delta_type == DeltaType.REMOVED:
                self.removed_count += 1
            elif delta.delta_type == DeltaType.MODIFIED:
                self.modified_count += 1
            elif delta.delta_type == DeltaType.UNCHANGED:
                self.unchanged_count += 1

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "deltas": [d.to_dict() for d in self.deltas],
            "summary": {
                "added": self.added_count,
                "removed": self.removed_count,
                "modified": self.modified_count,
                "unchanged": self.unchanged_count,
            },
        }

    def has_changes(self) -> bool:
        """Check if there are any changes."""
        return bool(self.deltas)


class ApiDeltaAction:
    """
    Handles API delta synchronization and incremental updates.

    This action computes differences between API states and generates
    efficient delta representations for partial updates.

    Example:
        >>> action = ApiDeltaAction()
        >>> old_state = {"users": [{"id": 1, "name": "Alice"}]}
        >>> new_state = {"users": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]}
        >>> result = action.compute_delta(old_state, new_state)
        >>> print(result.added_count)
        1
    """

    def __init__(
        self,
        ignore_keys: Optional[Set[str]] = None,
        deep_compare: bool = True,
        hash_values: bool = False,
    ):
        """
        Initialize the API Delta Action.

        Args:
            ignore_keys: Set of keys to ignore during comparison.
            deep_compare: Whether to perform deep nested comparison.
            hash_values: Whether to hash values for comparison efficiency.
        """
        self.ignore_keys = ignore_keys or set()
        self.deep_compare = deep_compare
        self.hash_values = hash_values
        self._cache: Dict[str, str] = {}

    def compute_delta(
        self,
        old_state: Dict[str, Any],
        new_state: Dict[str, Any],
        path: str = "",
    ) -> DeltaResult:
        """
        Compute delta between two states.

        Args:
            old_state: The previous state.
            new_state: The current state.
            path: Current path in the state tree (for nested comparison).

        Returns:
            DeltaResult containing all computed deltas.
        """
        deltas: List[Delta] = []
        all_keys = set(old_state.keys()) | set(new_state.keys())

        for key in sorted(all_keys):
            if key in self.ignore_keys:
                continue

            current_path = f"{path}.{key}" if path else key
            old_val = old_state.get(key, ...)
            new_val = new_state.get(key, ...)

            delta = self._compare_values(current_path, old_val, new_val)
            if delta:
                deltas.append(delta)

        return DeltaResult(deltas=deltas)

    def _compare_values(
        self,
        path: str,
        old_val: Any,
        new_val: Any,
    ) -> Optional[Delta]:
        """Compare two values and return delta if changed."""
        if old_val is ... and new_val is ...:
            return None

        if old_val is ...:
            return Delta(
                path=path,
                delta_type=DeltaType.ADDED,
                new_value=new_val,
            )

        if new_val is ...:
            return Delta(
                path=path,
                delta_type=DeltaType.REMOVED,
                old_value=old_val,
            )

        if self._values_equal(old_val, new_val):
            return Delta(
                path=path,
                delta_type=DeltaType.UNCHANGED,
                old_value=old_val,
                new_value=new_val,
            )

        return Delta(
            path=path,
            delta_type=DeltaType.MODIFIED,
            old_value=old_val,
            new_value=new_val,
        )

    def _values_equal(self, old_val: Any, new_val: Any) -> bool:
        """Check if two values are equal."""
        if type(old_val) != type(new_val):
            return False

        if isinstance(old_val, dict):
            if not self.deep_compare:
                return old_val == new_val
            return self._compare_dicts(old_val, new_val)

        if isinstance(old_val, (list, tuple)):
            return self._compare_lists(old_val, new_val)

        if self.hash_values:
            return self._hash_value(old_val) == self._hash_value(new_val)

        return old_val == new_val

    def _compare_dicts(self, old_dict: Dict, new_dict: Dict) -> bool:
        """Compare two dictionaries."""
        if set(old_dict.keys()) != set(new_dict.keys()):
            return False
        return all(self._values_equal(old_dict[k], new_dict[k]) for k in old_dict)

    def _compare_lists(self, old_list: List, new_list: List) -> bool:
        """Compare two lists."""
        if len(old_list) != len(new_list):
            return False
        return all(self._values_equal(o, n) for o, n in zip(old_list, new_list))

    def _hash_value(self, value: Any) -> str:
        """Hash a value for efficient comparison."""
        try:
            serialized = json.dumps(value, sort_keys=True, default=str)
            return hashlib.sha256(serialized.encode()).hexdigest()
        except (TypeError, ValueError):
            return str(value)

    def apply_delta(
        self,
        state: Dict[str, Any],
        delta: Delta,
    ) -> Dict[str, Any]:
        """
        Apply a single delta to a state.

        Args:
            state: Current state to modify.
            delta: Delta to apply.

        Returns:
            Modified state.
        """
        result = state.copy()
        keys = delta.path.split(".")
        current = result

        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]

        final_key = keys[-1]
        if delta.delta_type == DeltaType.REMOVED:
            current.pop(final_key, None)
        else:
            current[final_key] = delta.new_value

        return result

    def apply_deltas(
        self,
        state: Dict[str, Any],
        deltas: List[Delta],
    ) -> Dict[str, Any]:
        """
        Apply multiple deltas to a state.

        Args:
            state: Current state to modify.
            deltas: List of deltas to apply.

        Returns:
            Modified state after all deltas applied.
        """
        result = state.copy()
        for delta in deltas:
            if delta.delta_type != DeltaType.UNCHANGED:
                result = self.apply_delta(result, delta)
        return result

    def create_patch(
        self,
        old_state: Dict[str, Any],
        new_state: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Create a JSON patch compatible list of operations.

        Args:
            old_state: Original state.
            new_state: Modified state.

        Returns:
            List of JSON Patch operations.
        """
        result = self.compute_delta(old_state, new_state)
        patch = []

        for delta in result.deltas:
            if delta.delta_type == DeltaType.ADDED:
                patch.append({
                    "op": "add",
                    "path": f"/{delta.path}",
                    "value": delta.new_value,
                })
            elif delta.delta_type == DeltaType.REMOVED:
                patch.append({
                    "op": "remove",
                    "path": f"/{delta.path}",
                })
            elif delta.delta_type == DeltaType.MODIFIED:
                patch.append({
                    "op": "replace",
                    "path": f"/{delta.path}",
                    "value": delta.new_value,
                })

        return patch

    def merge_deltas(
        self,
        delta1: Delta,
        delta2: Delta,
    ) -> Optional[Delta]:
        """
        Merge two deltas for the same path.

        Args:
            delta1: First delta.
            delta2: Second delta.

        Returns:
            Merged delta or None if incompatible.
        """
        if delta1.path != delta2.path:
            return None

        if delta2.delta_type == DeltaType.REMOVED:
            return delta2

        if delta2.delta_type == DeltaType.ADDED and delta1.delta_type == DeltaType.MODIFIED:
            return Delta(
                path=delta1.path,
                delta_type=DeltaType.ADDED,
                old_value=delta1.old_value,
                new_value=delta2.new_value,
            )

        return Delta(
            path=delta1.path,
            delta_type=delta2.delta_type,
            old_value=delta1.old_value,
            new_value=delta2.new_value,
        )

    def filter_deltas(
        self,
        deltas: List[Delta],
        delta_type: Optional[DeltaType] = None,
        path_prefix: Optional[str] = None,
    ) -> List[Delta]:
        """
        Filter deltas by type and/or path prefix.

        Args:
            deltas: List of deltas to filter.
            delta_type: Optional type to filter by.
            path_prefix: Optional path prefix to filter by.

        Returns:
            Filtered list of deltas.
        """
        result = deltas

        if delta_type is not None:
            result = [d for d in result if d.delta_type == delta_type]

        if path_prefix is not None:
            result = [d for d in result if d.path.startswith(path_prefix)]

        return result

    def reverse_delta(self, delta: Delta) -> Delta:
        """
        Reverse a delta to undo the change.

        Args:
            delta: Delta to reverse.

        Returns:
            Reverse delta.
        """
        if delta.delta_type == DeltaType.ADDED:
            return Delta(
                path=delta.path,
                delta_type=DeltaType.REMOVED,
                old_value=delta.new_value,
            )
        elif delta.delta_type == DeltaType.REMOVED:
            return Delta(
                path=delta.path,
                delta_type=DeltaType.ADDED,
                new_value=delta.old_value,
            )
        else:
            return Delta(
                path=delta.path,
                delta_type=DeltaType.MODIFIED,
                old_value=delta.new_value,
                new_value=delta.old_value,
            )


def create_delta_action(**kwargs) -> ApiDeltaAction:
    """Factory function to create an ApiDeltaAction instance."""
    return ApiDeltaAction(**kwargs)
