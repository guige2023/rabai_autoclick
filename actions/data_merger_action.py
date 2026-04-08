"""Data Merger Action Module.

Provides deep merging of nested dictionaries, list concatenation
with deduplication, and conflict resolution strategies.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Set, Union
from copy import deepcopy
import logging

logger = logging.getLogger(__name__)


class ConflictStrategy(Enum):
    """Conflict resolution strategy."""
    LEFT_WINS = "left_wins"
    RIGHT_WINS = "right_wins"
    PREFER_NON_EMPTY = "prefer_non_empty"
    PREFER_NON_NONE = "prefer_non_none"
    CUSTOM = "custom"


from enum import Enum


class DataMergerAction:
    """Data merger with conflict resolution.

    Example:
        merger = DataMergerAction()

        result = merger.merge(
            {"a": 1, "b": {"c": 2}},
            {"b": {"d": 3}, "e": 4},
            conflict_strategy=ConflictStrategy.RIGHT_WINS
        )
        # result = {"a": 1, "b": {"c": 2, "d": 3}, "e": 4}
    """

    def __init__(
        self,
        default_conflict_strategy: ConflictStrategy = ConflictStrategy.RIGHT_WINS,
    ) -> None:
        self.default_conflict_strategy = default_conflict_strategy
        self._custom_resolvers: Dict[str, Callable] = {}

    def register_resolver(
        self,
        key: str,
        resolver: Callable[[Any, Any], Any],
    ) -> None:
        """Register custom conflict resolver for key."""
        self._custom_resolvers[key] = resolver

    def merge(
        self,
        *dicts: Dict[str, Any],
        conflict_strategy: Optional[ConflictStrategy] = None,
    ) -> Dict[str, Any]:
        """Merge multiple dictionaries deeply.

        Args:
            *dicts: Dictionaries to merge
            conflict_strategy: Conflict resolution strategy

        Returns:
            Merged dictionary
        """
        if not dicts:
            return {}

        result = deepcopy(dicts[0])
        strategy = conflict_strategy or self.default_conflict_strategy

        for other in dicts[1:]:
            result = self._merge_dicts(result, other, strategy)

        return result

    def _merge_dicts(
        self,
        left: Dict,
        right: Dict,
        strategy: ConflictStrategy,
    ) -> Dict:
        """Merge two dictionaries."""
        result = deepcopy(left)

        for key, value in right.items():
            if key in result:
                result[key] = self._resolve_conflict(
                    key, result[key], value, strategy
                )
            else:
                result[key] = deepcopy(value)

        return result

    def _resolve_conflict(
        self,
        key: str,
        left: Any,
        right: Any,
        strategy: ConflictStrategy,
    ) -> Any:
        """Resolve value conflict."""
        if key in self._custom_resolvers:
            return self._custom_resolvers[key](left, right)

        if isinstance(left, dict) and isinstance(right, dict):
            return self._merge_dicts(left, right, strategy)

        if isinstance(left, list) and isinstance(right, list):
            return self._merge_lists(left, right)

        if strategy == ConflictStrategy.LEFT_WINS:
            return left
        elif strategy == ConflictStrategy.RIGHT_WINS:
            return right
        elif strategy == ConflictStrategy.PREFER_NON_EMPTY:
            return left if left or not right else right
        elif strategy == ConflictStrategy.PREFER_NON_NONE:
            return left if left is not None else right

        return right

    def _merge_lists(
        self,
        left: List,
        right: List,
        deduplicate: bool = False,
    ) -> List:
        """Merge two lists."""
        if deduplicate:
            seen: Set = set()
            result = []

            for item in left + right:
                key = self._make_hashable(item)
                if key not in seen:
                    seen.add(key)
                    result.append(item)

            return result

        return deepcopy(left) + deepcopy(right)

    def _make_hashable(self, item: Any) -> Any:
        """Convert item to hashable form."""
        if isinstance(item, dict):
            return tuple(sorted(item.items()))
        elif isinstance(item, list):
            return tuple(self._make_hashable(i) for i in item)
        return item

    def merge_lists(
        self,
        *lists: List,
        deduplicate: bool = False,
    ) -> List:
        """Merge multiple lists."""
        if deduplicate:
            seen: Set = set()
            result = []

            for lst in lists:
                for item in lst:
                    key = self._make_hashable(item)
                    if key not in seen:
                        seen.add(key)
                        result.append(item)

            return result

        result = []
        for lst in lists:
            result.extend(deepcopy(lst))
        return result
