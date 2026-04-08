"""
Data Fuse Action Module.

Fuses multiple data sources with conflict resolution,
merge strategies, and priority-based composition.
"""

from __future__ import annotations

from typing import Any, Callable, Optional
from dataclasses import dataclass
from enum import Enum
import logging
import copy

logger = logging.getLogger(__name__)


class MergeStrategy(Enum):
    """Strategy for resolving conflicts during merge."""
    PREFER_FIRST = "prefer_first"
    PREFER_LAST = "prefer_last"
    PREFER_NON_NULL = "prefer_non_null"
    PREFER_NON_EMPTY = "prefer_non_empty"
    CONCATENATE = "concatenate"
    CUSTOM = "custom"


@dataclass
class FuseConfig:
    """Configuration for data fusion."""
    strategy: MergeStrategy = MergeStrategy.PREFER_LAST
    custom_resolver: Optional[Callable[[Any, Any], Any]] = None
    deep_merge: bool = True
    concat_separator: str = ", "


class DataFuseAction:
    """
    Fuses multiple data sources with configurable merge strategies.

    Supports shallow/deep merge, conflict resolution,
    and priority-based value selection.

    Example:
        fused = DataFuseAction().fuse(
            source_a,
            source_b,
            strategy=MergeStrategy.PREFER_NON_NULL
        )
    """

    def __init__(self, config: Optional[FuseConfig] = None) -> None:
        self.config = config or FuseConfig()

    def fuse(
        self,
        *sources: Any,
        config: Optional[FuseConfig] = None,
    ) -> Any:
        """Fuse multiple data sources into one."""
        cfg = config or self.config
        if not sources:
            return None

        if len(sources) == 1:
            return copy.deepcopy(sources[0])

        result: Any = None

        for source in sources:
            if source is None:
                continue

            if result is None:
                result = copy.deepcopy(source)
                continue

            if cfg.deep_merge and isinstance(result, dict) and isinstance(source, dict):
                result = self._deep_merge(result, source, cfg)
            else:
                result = self._shallow_merge_two(result, source, cfg)

        return result

    def fuse_priority(
        self,
        *sources: Any,
        priorities: list[int],
    ) -> Any:
        """Fuse sources with explicit priority ordering."""
        if len(sources) != len(priorities):
            raise ValueError("sources and priorities must have same length")

        indexed = list(zip(priorities, sources))
        indexed.sort(key=lambda x: x[0])

        _, sorted_sources = zip(*indexed)
        return self.fuse(*sorted_sources)

    def _deep_merge(
        self,
        base: dict,
        overlay: dict,
        cfg: FuseConfig,
    ) -> dict:
        """Deep merge two dicts."""
        result = copy.deepcopy(base)

        for key, value in overlay.items():
            if key in result:
                existing = result[key]

                if isinstance(existing, dict) and isinstance(value, dict):
                    result[key] = self._deep_merge(existing, value, cfg)
                elif isinstance(existing, list) and isinstance(value, list):
                    result[key] = self._merge_lists(existing, value, cfg)
                else:
                    result[key] = self._resolve_conflict(existing, value, cfg)
            else:
                result[key] = copy.deepcopy(value)

        return result

    def _shallow_merge_two(
        self,
        base: Any,
        overlay: Any,
        cfg: FuseConfig,
    ) -> Any:
        """Shallow merge two values."""
        return self._resolve_conflict(base, overlay, cfg)

    def _merge_lists(
        self,
        base: list,
        overlay: list,
        cfg: FuseConfig,
    ) -> list:
        """Merge two lists based on strategy."""
        if cfg.strategy == MergeStrategy.CONCATENATE:
            return base + overlay
        elif cfg.strategy == MergeStrategy.PREFER_LAST:
            return overlay
        else:
            return base if base else overlay

    def _resolve_conflict(
        self,
        first: Any,
        second: Any,
        cfg: FuseConfig,
    ) -> Any:
        """Resolve a merge conflict between two values."""
        if cfg.custom_resolver:
            return cfg.custom_resolver(first, second)

        strategy = cfg.strategy

        if strategy == MergeStrategy.PREFER_FIRST:
            return first
        elif strategy == MergeStrategy.PREFER_LAST:
            return second
        elif strategy == MergeStrategy.PREFER_NON_NULL:
            return second if second is not None else first
        elif strategy == MergeStrategy.PREFER_NON_EMPTY:
            if not first and first != 0 and first != False:
                return second
            return first
        elif strategy == MergeStrategy.CONCATENATE:
            if isinstance(first, str) and isinstance(second, str):
                return first + cfg.concat_separator + second
            return second

        return second
