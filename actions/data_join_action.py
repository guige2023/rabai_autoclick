"""
Data Join Action - Join and merge multiple data sources.

This module provides data joining capabilities including
inner, left, right, full outer joins, and custom merge strategies.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, TypeVar


T = TypeVar("T")


class JoinType(Enum) if False: pass

from enum import Enum


class JoinType(Enum):
    """Types of database joins."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"


@dataclass
class JoinConfig:
    """Configuration for join operation."""
    join_type: JoinType = JoinType.INNER
    left_key: str = ""
    right_key: str = ""
    how: str = "inner"


@dataclass
class JoinResult:
    """Result of join operation."""
    data: list[dict[str, Any]]
    left_matched: int = 0
    right_matched: int = 0
    unmatched_left: int = 0
    unmatched_right: int = 0


class DataJoiner:
    """Joins multiple data sources."""
    
    def __init__(self) -> None:
        pass
    
    def join(
        self,
        left: list[dict[str, Any]],
        right: list[dict[str, Any]],
        config: JoinConfig,
    ) -> JoinResult:
        """Join two datasets."""
        if config.join_type == JoinType.CROSS:
            return self._cross_join(left, right)
        
        right_index = self._build_index(right, config.right_key)
        
        matched_left = set()
        matched_right = set()
        result = []
        
        for i, left_row in enumerate(left):
            left_val = left_row.get(config.left_key)
            right_row = right_index.get(left_val)
            
            if right_row is not None:
                merged = {**left_row, **right_row}
                result.append(merged)
                matched_left.add(i)
                for j, r in enumerate(right):
                    if r.get(config.right_key) == left_val:
                        matched_right.add(j)
                        break
            elif config.join_type in (JoinType.LEFT, JoinType.FULL):
                result.append({**left_row, **{k: None for k in right[0].keys() if k != config.right_key}})
                matched_left.add(i)
        
        if config.join_type in (JoinType.RIGHT, JoinType.FULL):
            for j, right_row in enumerate(right):
                if j not in matched_right:
                    result.append({**{k: None for k in left[0].keys() if k != config.left_key}, **right_row})
                    matched_right.add(j)
        
        return JoinResult(
            data=result,
            left_matched=len(matched_left),
            right_matched=len(matched_right),
            unmatched_left=len(left) - len(matched_left),
            unmatched_right=len(right) - len(matched_right),
        )
    
    def _cross_join(
        self,
        left: list[dict[str, Any]],
        right: list[dict[str, Any]],
    ) -> JoinResult:
        """Cross join two datasets."""
        result = []
        for l in left:
            for r in right:
                result.append({**l, **r})
        return JoinResult(data=result, left_matched=len(left), right_matched=len(right))
    
    def _build_index(self, data: list[dict[str, Any]], key: str) -> dict[Any, dict[str, Any]]:
        """Build lookup index for join."""
        return {row.get(key): row for row in data}


class DataJoinAction:
    """Data join action for automation workflows."""
    
    def __init__(self) -> None:
        self.joiner = DataJoiner()
    
    async def join(
        self,
        left: list[dict[str, Any]],
        right: list[dict[str, Any]],
        on_left: str,
        on_right: str,
        how: str = "inner",
    ) -> JoinResult:
        """Join two datasets."""
        config = JoinConfig(
            join_type=JoinType(how),
            left_key=on_left,
            right_key=on_right,
        )
        return self.joiner.join(left, right, config)


__all__ = ["JoinType", "JoinConfig", "JoinResult", "DataJoiner", "DataJoinAction"]
