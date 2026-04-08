"""Data Filtering Action.

Filters data based on predicates, schemas, and data quality rules.
"""
from typing import Any, Callable, Dict, List, Optional, TypeVar, Generic
from dataclasses import dataclass


T = TypeVar("T")


@dataclass
class FilterRule:
    name: str
    predicate: Callable[[T], bool]
    description: str = ""
    drop_on_match: bool = False


class DataFilteringAction(Generic[T]):
    """Filters data using predicates and rules."""

    def __init__(self) -> None:
        self.rules: List[FilterRule] = []
        self._stats = {"passed": 0, "dropped": 0}

    def add_rule(
        self,
        name: str,
        predicate: Callable[[T], bool],
        description: str = "",
        drop_on_match: bool = False,
    ) -> "DataFilteringAction":
        self.rules.append(FilterRule(
            name=name,
            predicate=predicate,
            description=description,
            drop_on_match=drop_on_match,
        ))
        return self

    def filter(self, item: T) -> bool:
        for rule in self.rules:
            matches = rule.predicate(item)
            if matches:
                if rule.drop_on_match:
                    self._stats["dropped"] += 1
                    return False
            else:
                if not rule.drop_on_match:
                    self._stats["dropped"] += 1
                    return False
        self._stats["passed"] += 1
        return True

    def filter_list(self, items: List[T]) -> List[T]:
        return [item for item in items if self.filter(item)]

    def get_stats(self) -> Dict[str, int]:
        return dict(self._stats)
