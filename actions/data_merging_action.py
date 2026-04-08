"""Data Merging Action.

Merges multiple data sources with conflict resolution strategies.
"""
from typing import Any, Callable, Dict, List, Literal
from dataclasses import dataclass, field
from enum import Enum


class MergeStrategy(Enum):
    PREFER_SOURCE = "prefer_source"
    PREFER_TARGET = "prefer_target"
    CONFLICT_ERROR = "conflict_error"
    CUSTOM = "custom"


@dataclass
class MergeResult:
    merged: Dict[str, Any]
    conflicts: List[Dict[str, Any]]
    strategy: MergeStrategy


@dataclass
class MergeRule:
    field: str
    strategy: MergeStrategy
    custom_resolver: Optional[Callable[[Any, Any], Any]] = None


class DataMergingAction:
    """Merges data from multiple sources."""

    def __init__(self, default_strategy: MergeStrategy = MergeStrategy.PREFER_SOURCE) -> None:
        self.default_strategy = default_strategy
        self.rules: Dict[str, MergeRule] = {}

    def add_rule(
        self,
        field: str,
        strategy: MergeStrategy,
        custom_resolver: Optional[Callable[[Any, Any], Any]] = None,
    ) -> None:
        self.rules[field] = MergeRule(
            field=field,
            strategy=strategy,
            custom_resolver=custom_resolver,
        )

    def merge_two(
        self,
        source: Dict[str, Any],
        target: Dict[str, Any],
        source_priority: bool = True,
    ) -> MergeResult:
        merged = dict(target)
        conflicts = []
        all_keys = set(source.keys()) | set(target.keys())
        for key in all_keys:
            src_val = source.get(key)
            tgt_val = target.get(key)
            if src_val == tgt_val:
                merged[key] = src_val
            elif src_val is None:
                merged[key] = tgt_val
            elif tgt_val is None:
                merged[key] = src_val
            else:
                rule = self.rules.get(key)
                strategy = rule.strategy if rule else (self.default_strategy if source_priority else MergeStrategy.PREFER_TARGET)
                if rule and rule.custom_resolver:
                    merged[key] = rule.custom_resolver(src_val, tgt_val)
                elif strategy == MergeStrategy.PREFER_SOURCE:
                    merged[key] = src_val
                elif strategy == MergeStrategy.PREFER_TARGET:
                    merged[key] = tgt_val
                elif strategy == MergeStrategy.CONFLICT_ERROR:
                    conflicts.append({"field": key, "source": src_val, "target": tgt_val})
                else:
                    merged[key] = src_val
        return MergeResult(merged=merged, conflicts=conflicts, strategy=self.default_strategy)

    def merge_multiple(
        self,
        sources: List[Dict[str, Any]],
    ) -> MergeResult:
        if not sources:
            return MergeResult(merged={}, conflicts=[], strategy=self.default_strategy)
        result = sources[0]
        all_conflicts = []
        for source in sources[1:]:
            merge_result = self.merge_two(source, result)
            result = merge_result.merged
            all_conflicts.extend(merge_result.conflicts)
        return MergeResult(merged=result, conflicts=all_conflicts, strategy=self.default_strategy)
