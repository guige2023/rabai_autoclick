"""
Ranking module for ordering and scoring elements.

Provides ranking, sorting, and tie-breaking strategies for
scoring systems, leaderboards, and priority queues.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class RankMethod(Enum):
    """Ranking method types."""
    STANDARD = auto()    # 1, 2, 3, 3, 5 (average for ties)
    DENSE = auto()       # 1, 2, 3, 3, 4
    ORDINAL = auto()     # 1, 2, 3, 4, 5 (no ties)
    PERCENTILE = auto()  # Percentile rank 0-100
    FRACTIONAL = auto()  # 0.0 to 1.0 normalized


@dataclass
class RankedItem:
    """An item with its rank and score."""
    index: int
    value: Any
    score: float
    rank: float
    rank_method: RankMethod
    tie_group: Optional[int] = None


@dataclass 
class RankingResult:
    """Complete ranking analysis result."""
    ranked_items: list[RankedItem]
    top_item: RankedItem
    bottom_item: RankedItem
    method: RankMethod
    total_items: int
    unique_ranks: int
    tie_groups: int


class RankingEngine:
    """
    Ranks items using various ranking strategies.
    
    Example:
        engine = RankingEngine()
        items = [('a', 85), ('b', 92), ('c', 85), ('d', 78)]
        result = engine.rank(items, key=lambda x: x[1], method=RankMethod.STANDARD)
    """

    def __init__(self) -> None:
        """Initialize ranking engine."""
        self._cache: dict[str, list[RankedItem]] = {}

    def rank(
        self,
        items: list[Any],
        key: Optional[Callable[[Any], float]] = None,
        method: RankMethod = RankMethod.STANDARD,
        ascending: bool = False
    ) -> RankingResult:
        """
        Rank items based on scores.
        
        Args:
            items: List of items to rank.
            key: Function to extract score from each item.
            method: Ranking method to use.
            ascending: If True, lower scores get higher ranks.
            
        Returns:
            RankingResult with all ranked items and statistics.
            
        Raises:
            ValueError: If items is empty.
        """
        if not items:
            raise ValueError("Items list cannot be empty")

        # Extract scores
        if key is None:
            def default_key(x: Any) -> float:
                if isinstance(x, (int, float)):
                    return float(x)
                raise ValueError("key function required for non-numeric items")
            key = default_key

        scored_items = [(i, item, key(item)) for i, item in enumerate(items)]

        # Sort by score
        sorted_items = sorted(
            scored_items,
            key=lambda x: x[2],
            reverse=not ascending
        )

        # Assign ranks based on method
        ranked_items = self._assign_ranks(sorted_items, method)

        total_items = len(ranked_items)
        unique_ranks = len(set(r.rank for r in ranked_items))
        tie_groups = len(set(r.tie_group for r in ranked_items if r.tie_group is not None))

        return RankingResult(
            ranked_items=ranked_items,
            top_item=ranked_items[0],
            bottom_item=ranked_items[-1],
            method=method,
            total_items=total_items,
            unique_ranks=unique_ranks,
            tie_groups=tie_groups
        )

    def _assign_ranks(
        self,
        sorted_items: list[tuple[int, Any, float]],
        method: RankMethod
    ) -> list[RankedItem]:
        """Assign ranks based on the specified method."""
        if method == RankMethod.ORDINAL:
            return self._ordinal_ranks(sorted_items)
        elif method == RankMethod.STANDARD:
            return self._standard_ranks(sorted_items)
        elif method == RankMethod.DENSE:
            return self._dense_ranks(sorted_items)
        elif method == RankMethod.PERCENTILE:
            return self._percentile_ranks(sorted_items)
        elif method == RankMethod.FRACTIONAL:
            return self._fractional_ranks(sorted_items)
        return self._standard_ranks(sorted_items)

    def _ordinal_ranks(self, sorted_items: list[tuple[int, Any, float]]) -> list[RankedItem]:
        """Assign sequential ordinal ranks (no ties)."""
        return [
            RankedItem(
                index=orig_idx,
                value=item,
                score=score,
                rank=float(i + 1),
                rank_method=RankMethod.ORDINAL,
                tie_group=None
            )
            for i, (orig_idx, item, score) in enumerate(sorted_items)
        ]

    def _standard_ranks(self, sorted_items: list[tuple[int, Any, float]]) -> list[RankedItem]:
        """Assign standard ranks with average for ties."""
        n = len(sorted_items)
        ranks: list[RankedItem] = []
        i = 0

        while i < n:
            current_score = sorted_items[i][2]
            tie_start = i

            # Find all items with the same score
            while i < n and sorted_items[i][2] == current_score:
                i += 1

            tie_end = i
            tie_count = tie_end - tie_start
            avg_rank = (tie_start + 1 + tie_end) / 2.0  # Average of ranks

            for j in range(tie_start, tie_end):
                orig_idx, item, score = sorted_items[j]
                ranks.append(RankedItem(
                    index=orig_idx,
                    value=item,
                    score=score,
                    rank=avg_rank,
                    rank_method=RankMethod.STANDARD,
                    tie_group=tie_start
                ))

        return ranks

    def _dense_ranks(self, sorted_items: list[tuple[int, Any, float]]) -> list[RankedItem]:
        """Assign dense ranks with no gaps after ties."""
        n = len(sorted_items)
        ranks: list[RankedItem] = []
        i = 0
        current_rank = 1

        while i < n:
            current_score = sorted_items[i][2]
            tie_start = i

            while i < n and sorted_items[i][2] == current_score:
                i += 1

            for j in range(tie_start, i):
                orig_idx, item, score = sorted_items[j]
                ranks.append(RankedItem(
                    index=orig_idx,
                    value=item,
                    score=score,
                    rank=float(current_rank),
                    rank_method=RankMethod.DENSE,
                    tie_group=current_rank
                ))

            current_rank += 1

        return ranks

    def _percentile_ranks(self, sorted_items: list[tuple[int, Any, float]]) -> list[RankedItem]:
        """Assign percentile ranks (0-100)."""
        n = len(sorted_items)
        ranks: list[RankedItem] = []
        i = 0

        while i < n:
            current_score = sorted_items[i][2]
            tie_start = i

            while i < n and sorted_items[i][2] == current_score:
                i += 1

            tie_end = i
            # Percentile: position in sorted order
            percentile = ((tie_start + 1) / n) * 100

            for j in range(tie_start, tie_end):
                orig_idx, item, score = sorted_items[j]
                ranks.append(RankedItem(
                    index=orig_idx,
                    value=item,
                    score=score,
                    rank=percentile,
                    rank_method=RankMethod.PERCENTILE,
                    tie_group=tie_start
                ))

        return ranks

    def _fractional_ranks(self, sorted_items: list[tuple[int, Any, float]]) -> list[RankedItem]:
        """Assign fractional ranks (0.0-1.0)."""
        n = len(sorted_items)
        ranks: list[RankedItem] = []
        i = 0

        while i < n:
            current_score = sorted_items[i][2]
            tie_start = i

            while i < n and sorted_items[i][2] == current_score:
                i += 1

            tie_end = i
            fractional = tie_start / n

            for j in range(tie_start, tie_end):
                orig_idx, item, score = sorted_items[j]
                ranks.append(RankedItem(
                    index=orig_idx,
                    value=item,
                    score=score,
                    rank=fractional,
                    rank_method=RankMethod.FRACTIONAL,
                    tie_group=tie_start
                ))

        return ranks

    def get_top_k(
        self,
        items: list[Any],
        k: int,
        key: Optional[Callable[[Any], float]] = None,
        ascending: bool = False
    ) -> list[Any]:
        """
        Get top K items by score.
        
        Args:
            items: List of items.
            k: Number of top items to return.
            key: Score extraction function.
            ascending: If True, return lowest scores.
            
        Returns:
            List of top K items.
        """
        result = self.rank(items, key, RankMethod.ORDINAL, ascending)
        return [ri.value for ri in result.ranked_items[:min(k, len(items))]]

    def get_percentile_bracket(
        self,
        items: list[Any],
        value: Any,
        key: Optional[Callable[[Any], float]] = None
    ) -> str:
        """
        Get the percentile bracket for a value.
        
        Args:
            items: List of items.
            value: Item to find bracket for.
            key: Score extraction function.
            
        Returns:
            Bracket label (e.g., 'top 1%', 'top 5%', 'top 10%', etc.).
        """
        if key is None:
            key = lambda x: float(x) if isinstance(x, (int, float)) else 0.0

        result = self.rank(items, key, RankMethod.PERCENTILE)
        
        for ranked in result.ranked_items:
            if ranked.value == value:
                pct = ranked.rank
                if pct >= 99:
                    return "top 1%"
                elif pct >= 95:
                    return "top 5%"
                elif pct >= 90:
                    return "top 10%"
                elif pct >= 75:
                    return "top 25%"
                elif pct >= 50:
                    return "top 50%"
                else:
                    return "bottom 50%"

        return "unranked"

    def rerank_with_boost(
        self,
        items: list[Any],
        boost_indices: list[int],
        boost_factor: float,
        key: Optional[Callable[[Any], float]] = None,
        method: RankMethod = RankMethod.STANDARD
    ) -> RankingResult:
        """
        Re-rank items with a boost applied to specific indices.
        
        Args:
            items: List of items to rank.
            boost_indices: Indices of items to boost.
            boost_factor: Multiplicative boost factor.
            key: Score extraction function.
            method: Ranking method.
            
        Returns:
            New ranking result with boosted scores.
        """
        if key is None:
            key = lambda x: float(x) if isinstance(x, (int, float)) else 0.0

        boosted_scores = []
        for i, item in enumerate(items):
            score = key(item)
            if i in set(boost_indices):
                score *= boost_factor
            boosted_scores.append((i, item, score))

        sorted_items = sorted(boosted_scores, key=lambda x: x[2], reverse=True)
        ranked_items = self._assign_ranks(sorted_items, method)

        total_items = len(ranked_items)
        unique_ranks = len(set(r.rank for r in ranked_items))
        tie_groups = len(set(r.tie_group for r in ranked_items if r.tie_group is not None))

        return RankingResult(
            ranked_items=ranked_items,
            top_item=ranked_items[0],
            bottom_item=ranked_items[-1],
            method=method,
            total_items=total_items,
            unique_ranks=unique_ranks,
            tie_groups=tie_groups
        )
