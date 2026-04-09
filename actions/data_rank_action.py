"""Data Rank Action Module.

Provides ranking and ordering operations for data analysis including
primary rankings, dense/sparse ranks, fractional ranks, and
percentile calculations with configurable tie-handling.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class RankMethod(Enum):
    """Ranking method types."""
    STANDARD = "standard"
    DENSE = "dense"
    ORDINAL = "ordinal"
    FRACTIONAL = "fractional"
    PERCENTILE = "percentile"


class SortOrder(Enum):
    """Sort order direction."""
    ASCENDING = "ascending"
    DESCENDING = "descending"


@dataclass
class RankResult:
    """Result of a ranking operation."""
    item: Any
    rank: float
    original_index: int
    group: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RankConfig:
    """Configuration for ranking operations."""
    method: RankMethod = RankMethod.STANDARD
    order: SortOrder = SortOrder.DESCENDING
    tie_breaker: str = "average"
    handle_missing: str = "bottom"
    zero_indexed: bool = False
    assign_groups: bool = False


class RankCalculator:
    """Calculate various ranking methods."""

    @staticmethod
    def standard_rank(values: List[Tuple[Any, float]]) -> List[RankResult]:
        """Calculate standard rank with average tie handling."""
        sorted_values = sorted(values, key=lambda x: x[1], reverse=True)
        results = []
        current_rank = 1

        for i, (item, value) in enumerate(sorted_values):
            if i > 0 and value == sorted_values[i - 1][1]:
                rank = (current_rank + i) / 2.0
            else:
                rank = i + 1
            results.append(RankResult(
                item=item,
                rank=rank,
                original_index=values.index((item, value))
            ))
            current_rank = i + 1

        return results

    @staticmethod
    def dense_rank(values: List[Tuple[Any, float]]) -> List[RankResult]:
        """Calculate dense rank (no gaps)."""
        sorted_values = sorted(values, key=lambda x: x[1], reverse=True)
        results = []
        current_rank = 1
        prev_value = None

        for i, (item, value) in enumerate(sorted_values):
            if prev_value is not None and value != prev_value:
                current_rank = i + 1
            results.append(RankResult(
                item=item,
                rank=current_rank,
                original_index=values.index((item, value))
            ))
            prev_value = value

        return results

    @staticmethod
    def ordinal_rank(values: List[Tuple[Any, float]]) -> List[RankResult]:
        """Calculate ordinal rank (1, 2, 3, ... no ties)."""
        sorted_values = sorted(values, key=lambda x: x[1], reverse=True)
        return [
            RankResult(
                item=item,
                rank=i + 1,
                original_index=values.index((item, value))
            )
            for i, (item, value) in enumerate(sorted_values)
        ]

    @staticmethod
    def fractional_rank(values: List[Tuple[Any, float]]) -> List[RankResult]:
        """Calculate fractional rank for tied values."""
        from statistics import mean

        sorted_values = sorted(values, key=lambda x: x[1], reverse=True)
        results = []
        i = 0

        while i < len(sorted_values):
            j = i
            while j < len(sorted_values) - 1 and sorted_values[j][1] == sorted_values[j + 1][1]:
                j += 1

            ranks = list(range(i + 1, j + 2))
            avg_rank = mean(ranks)

            for k in range(i, j + 1):
                item, value = sorted_values[k]
                results.append(RankResult(
                    item=item,
                    rank=avg_rank,
                    original_index=values.index((item, value))
                ))

            i = j + 1

        return results

    @staticmethod
    def percentile_rank(values: List[Tuple[Any, float]]) -> List[RankResult]:
        """Calculate percentile rank."""
        sorted_values = sorted(values, key=lambda x: x[1], reverse=True)
        n = len(sorted_values)

        return [
            RankResult(
                item=item,
                rank=((n - i) / n) * 100,
                original_index=values.index((item, value))
            )
            for i, (item, value) in enumerate(sorted_values)
        ]


class DataRankAction(BaseAction):
    """Action for ranking operations on data."""

    def __init__(self):
        super().__init__(name="data_rank")
        self._config = RankConfig()
        self._rank_history: List[Dict[str, Any]] = []

    def configure(self, config: RankConfig):
        """Configure ranking settings."""
        self._config = config

    def rank(
        self,
        items: List[Any],
        key: Optional[Callable[[Any], float]] = None,
        values: Optional[List[float]] = None
    ) -> List[RankResult]:
        """Rank items based on their values."""
        if values:
            item_value_pairs = list(zip(items, values))
        elif key:
            item_value_pairs = [(item, key(item)) for item in items]
        else:
            raise ValueError("Either values or key function must be provided")

        order_descending = self._config.order == SortOrder.DESCENDING
        item_value_pairs.sort(key=lambda x: x[1], reverse=order_descending)

        if self._config.method == RankMethod.STANDARD:
            results = RankCalculator.standard_rank(item_value_pairs)
        elif self._config.method == RankMethod.DENSE:
            results = RankCalculator.dense_rank(item_value_pairs)
        elif self._config.method == RankMethod.ORDINAL:
            results = RankCalculator.ordinal_rank(item_value_pairs)
        elif self._config.method == RankMethod.FRACTIONAL:
            results = RankCalculator.fractional_rank(item_value_pairs)
        elif self._config.method == RankMethod.PERCENTILE:
            results = RankCalculator.percentile_rank(item_value_pairs)
        else:
            results = RankCalculator.standard_rank(item_value_pairs)

        if self._config.zero_indexed:
            for r in results:
                r.rank -= 1

        if self._config.assign_groups:
            results = self._assign_rank_groups(results)

        return results

    def _assign_rank_groups(self, results: List[RankResult]) -> List[RankResult]:
        """Assign groups to ranked results based on rank thresholds."""
        groups = ["top", "high", "medium", "low", "bottom"]
        thresholds = [90, 75, 50, 25]

        for r in results:
            assigned = False
            for i, threshold in enumerate(thresholds):
                if r.rank >= threshold:
                    r.group = groups[i]
                    assigned = True
                    break
            if not assigned:
                r.group = groups[-1]

        return results

    def rank_with_groups(
        self,
        items: List[Any],
        values: List[float],
        group_key: Callable[[Any], str]
    ) -> Dict[str, List[RankResult]]:
        """Rank items within groups."""
        groups: Dict[str, List[Tuple[Any, float]]] = defaultdict(list)

        for item, value in zip(items, values):
            group = group_key(item)
            groups[group].append((item, value))

        group_ranks: Dict[str, List[RankResult]] = {}
        for group_name, group_items in groups.items():
            ranks = self.rank(
                [item for item, _ in group_items],
                values=[value for _, value in group_items]
            )
            for r in ranks:
                r.group = group_name
            group_ranks[group_name] = ranks

        return group_ranks

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute ranking action."""
        try:
            items = params.get("items", [])
            values = params.get("values")
            key = params.get("key")

            if key and callable(key):
                results = self.rank(items, key=key)
            elif values:
                results = self.rank(items, values=values)
            else:
                return ActionResult(success=False, error="Either values or key must be provided")

            return ActionResult(
                success=True,
                data={
                    "method": self._config.method.value,
                    "order": self._config.order.value,
                    "results": [
                        {
                            "item": str(r.item),
                            "rank": r.rank,
                            "group": r.group
                        }
                        for r in results[:100]
                    ]
                }
            )
        except Exception as e:
            logger.exception("Ranking failed")
            return ActionResult(success=False, error=str(e))
