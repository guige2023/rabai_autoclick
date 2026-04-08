"""Data Difference Action.

Computes set differences between data sources.
"""
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar
from dataclasses import dataclass


T = TypeVar("T")


@dataclass
class DifferenceResult:
    only_in_a: List[Any]
    only_in_b: List[Any]
    in_both: List[Any]
    symmetric_difference: List[Any]


class DataDifferenceAction:
    """Computes differences between data sources."""

    def __init__(self, key_fn: Optional[Callable[[T], Any]] = None) -> None:
        self.key_fn = key_fn or (lambda x: x)

    def diff(
        self,
        a: List[T],
        b: List[T],
    ) -> DifferenceResult:
        a_keys = {self.key_fn(x) for x in a}
        b_keys = {self.key_fn(x) for x in b}
        only_a = [x for x in a if self.key_fn(x) not in b_keys]
        only_b = [x for x in b if self.key_fn(x) not in a_keys]
        in_both = [x for x in a if self.key_fn(x) in b_keys]
        sym_diff = only_a + only_b
        return DifferenceResult(
            only_in_a=only_a,
            only_in_b=only_b,
            in_both=in_both,
            symmetric_difference=sym_diff,
        )

    def diff_multiple(
        self,
        sources: Dict[str, List[T]],
    ) -> Dict[str, List[T]]:
        if not sources:
            return {}
        all_keys: Set = set()
        for items in sources.values():
            for item in items:
                all_keys.add(self.key_fn(item))
        result = {}
        for name, items in sources.items():
            keys = {self.key_fn(x) for x in items}
            other_keys = all_keys - keys
            result[name] = [x for x in items if self.key_fn(x) in other_keys]
        return result
