"""Data transformer action module for RabAI AutoClick.

Provides data transformation operations:
- DataMapperAction: Map and transform data fields
- DataAggregatorAction: Aggregate data from multiple sources
- DataSplitterAction: Split data into partitions
- DataMergerAction: Merge multiple data sources
"""

import sys
import os
import logging
from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass, field
from datetime import datetime

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult

logger = logging.getLogger(__name__)


@dataclass
class FieldMapping:
    """A single field mapping definition."""
    source_field: str
    target_field: str
    transform: Optional[Callable[[Any], Any]] = None
    default: Any = None


class DataMapper:
    """Maps and transforms data fields based on mapping rules."""

    def __init__(self, mappings: List[FieldMapping]) -> None:
        self.mappings = mappings

    def map(self, data: Dict[str, Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for mapping in self.mappings:
            value = data.get(mapping.source_field, mapping.default)
            if mapping.transform and value is not None:
                try:
                    value = mapping.transform(value)
                except Exception as e:
                    logger.warning(f"Transform failed for {mapping.source_field}: {e}")
                    value = mapping.default or value
            result[mapping.target_field] = value
        return result

    def map_batch(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [self.map(record) for record in records]


class DataAggregator:
    """Aggregates data using various strategies."""

    def __init__(self) -> None:
        self.strategies = {
            "sum": lambda values: sum(v for v in values if v is not None),
            "avg": lambda values: sum(v for v in values if v is not None) / len([v for v in values if v is not None]) if any(v is not None for v in values) else 0,
            "min": lambda values: min((v for v in values if v is not None), default=None),
            "max": lambda values: max((v for v in values if v is not None), default=None),
            "count": lambda values: len([v for v in values if v is not None]),
            "first": lambda values: next((v for v in values if v is not None), None),
            "last": lambda values: next((v for v in reversed(values) if v is not None), None),
            "concat": lambda values: ",".join(str(v) for v in values if v is not None),
        }

    def aggregate(
        self,
        data: List[Dict[str, Any]],
        group_by: str,
        field_name: str,
        strategy: str = "sum"
    ) -> List[Dict[str, Any]]:
        groups: Dict[Any, List[Any]] = {}
        for record in data:
            key = record.get(group_by)
            if key not in groups:
                groups[key] = []
            groups[key].append(record.get(field_name))

        if strategy not in self.strategies:
            raise ValueError(f"Unknown strategy: {strategy}")

        strategy_func = self.strategies[strategy]
        result = []
        for key, values in groups.items():
            aggregated = strategy_func(values)
            result.append({group_by: key, field_name: aggregated, "_count": len(values)})

        return result


class DataSplitter:
    """Splits data into partitions."""

    def split_by_size(self, data: List[Any], chunk_size: int) -> List[List[Any]]:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

    def split_by_count(self, data: List[Any], num_partitions: int) -> List[List[Any]]:
        if num_partitions <= 0:
            raise ValueError("num_partitions must be positive")
        chunk_size = max(1, len(data) // num_partitions)
        return self.split_by_size(data, chunk_size)

    def split_by_key(self, data: List[Dict[str, Any]], key: str) -> Dict[Any, List[Dict[str, Any]]]:
        result: Dict[Any, List[Dict[str, Any]]] = {}
        for record in data:
            partition_key = record.get(key)
            if partition_key not in result:
                result[partition_key] = []
            result[partition_key].append(record)
        return result

    def split_by_range(
        self,
        data: List[Union[int, float]],
        num_ranges: int
    ) -> List[List[Union[int, float]]]:
        if not data:
            return []
        min_val = min(data)
        max_val = max(data)
        if min_val == max_val:
            return [data]
        range_size = (max_val - min_val) / num_ranges
        ranges: List[List[Union[int, float]]] = [[] for _ in range(num_partitions := num_ranges)]
        for value in data:
            idx = min(int((value - min_val) / range_size), num_ranges - 1)
            ranges[idx].append(value)
        return ranges


class DataMerger:
    """Merges multiple data sources."""

    def merge_inner(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        left_key: str,
        right_key: str
    ) -> List[Dict[str, Any]]:
        right_index = {r[right_key]: r for r in right}
        result = []
        for l in left:
            r = right_index.get(l.get(left_key))
            if r:
                merged = {**l, **r}
                result.append(merged)
        return result

    def merge_left(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        left_key: str,
        right_key: str
    ) -> List[Dict[str, Any]]:
        right_index = {r[right_key]: r for r in right}
        result = []
        for l in left:
            r = right_index.get(l.get(left_key))
            merged = {**l}
            if r:
                for k, v in r.items():
                    if k != right_key:
                        merged[k] = v
            else:
                merged["_merged"] = None
            result.append(merged)
        return result

    def merge_concat(self, *datasets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        for dataset in datasets:
            result.extend(dataset)
        return result


class DataMapperAction(BaseAction):
    """Map and transform data fields."""
    action_type = "data_mapper"
    display_name = "数据字段映射"
    description = "根据映射规则转换数据字段"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        data = params.get("data", [])
        mappings_config = params.get("mappings", [])

        if not isinstance(data, list):
            return ActionResult(success=False, message="data必须是列表")

        mappings = []
        for m in mappings_config:
            mapping = FieldMapping(
                source_field=m.get("source_field", ""),
                target_field=m.get("target_field", ""),
                default=m.get("default")
            )
            mappings.append(mapping)

        mapper = DataMapper(mappings)
        result = mapper.map_batch(data)

        return ActionResult(
            success=True,
            message=f"映射完成，{len(result)} 条记录",
            data={"records": result, "count": len(result)}
        )


class DataAggregatorAction(BaseAction):
    """Aggregate data from multiple sources."""
    action_type = "data_aggregator"
    display_name = "数据聚合"
    description = "使用各种策略聚合数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        data = params.get("data", [])
        group_by = params.get("group_by", "")
        field_name = params.get("field_name", "")
        strategy = params.get("strategy", "sum")

        if not isinstance(data, list):
            return ActionResult(success=False, message="data必须是列表")

        if not group_by or not field_name:
            return ActionResult(success=False, message="group_by和field_name是必需的")

        valid, msg = self.validate_in(
            strategy,
            ["sum", "avg", "min", "max", "count", "first", "last", "concat"],
            "strategy"
        )
        if not valid:
            return ActionResult(success=False, message=msg)

        aggregator = DataAggregator()
        try:
            result = aggregator.aggregate(data, group_by, field_name, strategy)
            return ActionResult(
                success=True,
                message=f"聚合完成，{len(result)} 个分组",
                data={"groups": result, "count": len(result)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"聚合失败: {e}")


class DataSplitterAction(BaseAction):
    """Split data into partitions."""
    action_type = "data_splitter"
    display_name = "数据分割"
    description = "将数据分割为多个分区"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        data = params.get("data", [])
        split_type = params.get("split_type", "size")
        key = params.get("key", "")
        chunk_size = params.get("chunk_size", 10)
        num_partitions = params.get("num_partitions", 3)

        if not isinstance(data, list):
            return ActionResult(success=False, message="data必须是列表")

        splitter = DataSplitter()

        try:
            if split_type == "size":
                result = splitter.split_by_size(data, chunk_size)
            elif split_type == "count":
                result = splitter.split_by_count(data, num_partitions)
            elif split_type == "key":
                if not key:
                    return ActionResult(success=False, message="split_type为key时key参数是必需的")
                result_dict = splitter.split_by_key(data, key)
                result = [{"key": k, "records": v} for k, v in result_dict.items()]
            else:
                return ActionResult(success=False, message=f"未知split_type: {split_type}")

            return ActionResult(
                success=True,
                message=f"分割完成，{len(result)} 个分区",
                data={"partitions": result, "count": len(result)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"分割失败: {e}")


class DataMergerAction(BaseAction):
    """Merge multiple data sources."""
    action_type = "data_merger"
    display_name = "数据合并"
    description = "合并多个数据源"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        left = params.get("left", [])
        right = params.get("right", [])
        merge_type = params.get("merge_type", "concat")
        left_key = params.get("left_key", "")
        right_key = params.get("right_key", "")

        if not isinstance(left, list) or not isinstance(right, list):
            return ActionResult(success=False, message="left和right必须是列表")

        merger = DataMerger()

        if merge_type == "concat":
            result = merger.merge_concat(left, right)
            return ActionResult(
                success=True,
                message=f"合并完成，{len(result)} 条记录",
                data={"records": result, "count": len(result)}
            )

        if merge_type in ("inner", "left"):
            if not left_key or not right_key:
                return ActionResult(success=False, message="inner和left合并需要left_key和right_key")

            if merge_type == "inner":
                result = merger.merge_inner(left, right, left_key, right_key)
            else:
                result = merger.merge_left(left, right, left_key, right_key)

            return ActionResult(
                success=True,
                message=f"{merge_type}合并完成，{len(result)} 条记录",
                data={"records": result, "count": len(result)}
            )

        return ActionResult(success=False, message=f"未知merge_type: {merge_type}")
