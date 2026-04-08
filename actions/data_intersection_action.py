"""Data Intersection Action.

Computes set intersections between data sources.
"""
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar
from dataclasses import dataclass


T = TypeVar("T")


@dataclass
class IntersectionResult:
    intersection: List[Any]
    only_in_first: List[Any]
    coverage: Dict[str, float]


class DataIntersectionAction:
    """Computes intersections between data sources."""

    def __init__(self, key_fn: Optional[Callable[[T], Any]] = None) -> None:
        self.key_fn = key_fn or (lambda x: x)

    def intersect(
        self,
        a: List[T],
        b: List[T],
    ) -> IntersectionResult:
        a_keys = {self.key_fn(x) for x in a}
        b_keys = {self.key_fn(x) for x in b}
        common_keys = a_keys & b_keys
        intersection = [x for x in a if self.key_fn(x) in common_keys]
        only_in_first = [x for x in a if self.key_fn(x) not in b_keys]
        coverage = {
            "a_coverage": len(common_keys) / len(a_keys) if a_keys else 0.0,
            "b_coverage": len(common_keys) / len(b_keys) if b_keys else 0.0,
        }
        return IntersectionResult(
            intersection=intersection,
            only_in_first=only_in_first,
            coverage=coverage,
        )

    def intersect_all(
        self,
        sources: Dict[str, List[T]],
    ) -> List[Any]:
        if not sources:
            return []
        keys_sets: List[Set] = []
        for items in sources.values():
            keys_sets.append({self.key_fn(x) for x in items})
        common_keys = keys_sets[0]
        for key_set in keys_sets[1:]:
            common_keys &= key_set
        result = []
        for items in sources.values():
            for item in items:
                if self.key_fn(item) in common_keys:
                    result.append(item)
                    common_keys.discard(self.key_fn(item))
                    break
        return result
