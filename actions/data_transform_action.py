"""Data transform action module for RabAI AutoClick.

Provides data transformation operations:
- DataTransformer: General data transformations
- SchemaMapper: Map data between schemas
- DataAggregator: Aggregate and group data
- DataFilter: Filter and select data
- DataSorter: Sort data by multiple keys
- DataJoiner: Join multiple data sources
"""

import time
from typing import Any, Callable, Dict, List, Optional, Set, Union
from dataclasses import dataclass
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TransformType(Enum):
    """Transformation types."""
    MAP = "map"
    FILTER = "filter"
    REDUCE = "reduce"
    FLATTEN = "flatten"
    GROUP = "group"
    SORT = "sort"
    JOIN = "join"
    PIVOT = "pivot"
    UNPIVOT = "unpivot"


@dataclass
class TransformConfig:
    """Configuration for transformations."""
    transform_type: TransformType
    source_field: Optional[str] = None
    target_field: Optional[str] = None
    expression: Optional[str] = None
    keys: List[str] = None
    ascending: bool = True


class DataTransformer:
    """General data transformer."""

    def __init__(self):
        self._transforms: List[TransformConfig] = []

    def add_transform(self, config: TransformConfig) -> "DataTransformer":
        """Add a transformation."""
        self._transforms.append(config)
        return self

    def apply(self, data: Any) -> Any:
        """Apply all transformations."""
        result = data
        for transform in self._transforms:
            result = self._apply_single(result, transform)
        return result

    def _apply_single(self, data: Any, config: TransformConfig) -> Any:
        """Apply single transformation."""
        if config.transform_type == TransformType.MAP:
            return self._map(data, config)
        elif config.transform_type == TransformType.FILTER:
            return self._filter(data, config)
        elif config.transform_type == TransformType.REDUCE:
            return self._reduce(data, config)
        elif config.transform_type == TransformType.FLATTEN:
            return self._flatten(data)
        elif config.transform_type == TransformType.GROUP:
            return self._group(data, config)
        elif config.transform_type == TransformType.SORT:
            return self._sort(data, config)
        return data

    def _map(self, data: List[Dict], config: TransformConfig) -> List[Dict]:
        """Apply mapping transformation."""
        if not data:
            return data

        source = config.source_field or "value"
        target = config.target_field or source
        expr = config.expression

        if expr:
            try:
                compiled = eval(f"lambda x: {expr}")
                return [{**item, target: compiled(item.get(source))} for item in data]
            except Exception:
                return data
        return [{**item, target: item.get(source)} for item in data]

    def _filter(self, data: List[Dict], config: TransformConfig) -> List[Dict]:
        """Apply filter transformation."""
        if not data:
            return data

        expr = config.expression
        if not expr:
            return data

        try:
            compiled = eval(f"lambda x: {expr}")
            return [item for item in data if compiled(item)]
        except Exception:
            return data

    def _reduce(self, data: List[Any], config: TransformConfig) -> Any:
        """Apply reduce transformation."""
        if not data:
            return None

        expr = config.expression
        if not expr:
            return data

        try:
            compiled = eval(f"lambda acc, x: {expr}")
            result = data[0]
            for item in data[1:]:
                result = compiled(result, item)
            return result
        except Exception:
            return data

    def _flatten(self, data: Any) -> List[Any]:
        """Flatten nested structure."""
        if isinstance(data, list):
            result = []
            for item in data:
                result.extend(self._flatten(item))
            return result
        elif isinstance(data, dict):
            result = []
            for value in data.values():
                result.extend(self._flatten(value))
            return result
        else:
            return [data]

    def _group(self, data: List[Dict], config: TransformConfig) -> Dict[str, List[Dict]]:
        """Group data by keys."""
        if not data or not config.keys:
            return {}

        keys = config.keys
        result = {}

        for item in data:
            key_values = tuple(item.get(k) for k in keys)
            key_str = str(key_values)

            if key_str not in result:
                result[key_str] = []
            result[key_str].append(item)

        return result

    def _sort(self, data: List[Dict], config: TransformConfig) -> List[Dict]:
        """Sort data."""
        if not data:
            return data

        keys = config.keys or ["value"]
        ascending = config.ascending

        try:
            return sorted(data, key=lambda x: tuple(x.get(k) for k in keys), reverse=not ascending)
        except Exception:
            return data


class SchemaMapper:
    """Map data between schemas."""

    def __init__(self, schema: Dict[str, str]):
        """Initialize with schema mapping.

        Args:
            schema: Dict mapping target field to source expression
        """
        self.schema = schema

    def map(self, data: Dict) -> Dict:
        """Map data according to schema."""
        result = {}
        for target, source in self.schema.items():
            try:
                if isinstance(source, str) and source.startswith("="):
                    result[target] = eval(f"lambda x: {source[1:]}", {"x": data})
                else:
                    result[target] = data.get(source, source if isinstance(source, str) else None)
            except Exception:
                result[target] = None
        return result

    def map_list(self, data: List[Dict]) -> List[Dict]:
        """Map list of data."""
        return [self.map(item) for item in data]


class DataAggregator:
    """Aggregate data."""

    @staticmethod
    def count(data: List[Dict], field: str) -> int:
        """Count non-null values."""
        return sum(1 for item in data if item.get(field) is not None)

    @staticmethod
    def sum(data: List[Dict], field: str) -> Union[int, float]:
        """Sum values."""
        return sum(item.get(field, 0) for item in data if item.get(field) is not None)

    @staticmethod
    def average(data: List[Dict], field: str) -> float:
        """Calculate average."""
        values = [item.get(field) for item in data if item.get(field) is not None]
        return sum(values) / len(values) if values else 0

    @staticmethod
    def min(data: List[Dict], field: str) -> Any:
        """Get minimum value."""
        values = [item.get(field) for item in data if item.get(field) is not None]
        return min(values) if values else None

    @staticmethod
    def max(data: List[Dict], field: str) -> Any:
        """Get maximum value."""
        values = [item.get(field) for item in data if item.get(field) is not None]
        return max(values) if values else None

    @staticmethod
    def group_aggregate(
        data: List[Dict],
        group_by: List[str],
        aggregates: Dict[str, str],
    ) -> List[Dict]:
        """Group and aggregate."""
        groups = {}
        for item in data:
            key = tuple(item.get(k) for k in group_by)
            if key not in groups:
                groups[key] = []
            groups[key].append(item)

        results = []
        for key, items in groups.items():
            result = dict(zip(group_by, key))
            for field, agg_func in aggregates.items():
                if agg_func == "count":
                    result[field] = len(items)
                elif agg_func == "sum":
                    result[field] = DataAggregator.sum(items, field)
                elif agg_func == "avg":
                    result[field] = DataAggregator.average(items, field)
                elif agg_func == "min":
                    result[field] = DataAggregator.min(items, field)
                elif agg_func == "max":
                    result[field] = DataAggregator.max(items, field)
            results.append(result)

        return results


class DataJoiner:
    """Join multiple data sources."""

    @staticmethod
    def inner_join(
        left: List[Dict],
        right: List[Dict],
        left_key: str,
        right_key: str,
    ) -> List[Dict]:
        """Inner join on key."""
        right_index = {item[right_key]: item for item in right if right_key in item}
        results = []

        for left_item in left:
            key_val = left_item.get(left_key)
            if key_val in right_index:
                results.append({**left_item, **right_index[key_val]})

        return results

    @staticmethod
    def left_join(
        left: List[Dict],
        right: List[Dict],
        left_key: str,
        right_key: str,
    ) -> List[Dict]:
        """Left join on key."""
        right_index = {item[right_key]: item for item in right if right_key in item}
        results = []

        for left_item in left:
            key_val = left_item.get(left_key)
            if key_val in right_index:
                results.append({**left_item, **right_index[key_val]})
            else:
                results.append(left_item)

        return results

    @staticmethod
    def full_outer_join(
        left: List[Dict],
        right: List[Dict],
        left_key: str,
        right_key: str,
    ) -> List[Dict]:
        """Full outer join on key."""
        right_index = {item[right_key]: item for item in right if right_key in item}
        left_keys = set(item.get(left_key) for item in left)

        results = list(left)
        for right_item in right:
            key_val = right_item.get(right_key)
            if key_val not in left_keys:
                results.append(right_item)
            else:
                for i, left_item in enumerate(results):
                    if left_item.get(left_key) == key_val:
                        results[i] = {**left_item, **right_item}
                        break

        return results


class DataTransformAction(BaseAction):
    """Data transformation action."""
    action_type = "data_transform"
    display_name = "数据转换"
    description = "数据转换和映射"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "transform")
            data = params.get("data", [])

            if operation == "transform":
                return self._transform(data, params)
            elif operation == "map_schema":
                return self._map_schema(data, params)
            elif operation == "aggregate":
                return self._aggregate(data, params)
            elif operation == "join":
                return self._join(data, params)
            elif operation == "filter":
                return self._filter(data, params)
            elif operation == "sort":
                return self._sort(data, params)
            elif operation == "group":
                return self._group(data, params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Transform error: {str(e)}")

    def _transform(self, data: List[Dict], params: Dict) -> ActionResult:
        """Apply transformations."""
        transformer = DataTransformer()

        transforms = params.get("transforms", [])
        for t in transforms:
            ttype = TransformType[t.get("type", "MAP").upper()]
            config = TransformConfig(
                transform_type=ttype,
                source_field=t.get("source_field"),
                target_field=t.get("target_field"),
                expression=t.get("expression"),
                keys=t.get("keys", []),
                ascending=t.get("ascending", True),
            )
            transformer.add_transform(config)

        result = transformer.apply(data)
        return ActionResult(success=True, message=f"Transformed {len(data)} items", data={"result": result})

    def _map_schema(self, data: List[Dict], params: Dict) -> ActionResult:
        """Map data to schema."""
        schema = params.get("schema", {})
        mapper = SchemaMapper(schema)

        if isinstance(data, list):
            result = mapper.map_list(data)
        else:
            result = mapper.map(data)

        return ActionResult(success=True, message="Schema mapped", data={"result": result})

    def _aggregate(self, data: List[Dict], params: Dict) -> ActionResult:
        """Aggregate data."""
        group_by = params.get("group_by", [])
        aggregates = params.get("aggregates", {})

        if group_by:
            result = DataAggregator.group_aggregate(data, group_by, aggregates)
        else:
            result = {}
            for field, agg_func in aggregates.items():
                if agg_func == "count":
                    result[field] = DataAggregator.count(data, field)
                elif agg_func == "sum":
                    result[field] = DataAggregator.sum(data, field)
                elif agg_func == "avg":
                    result[field] = DataAggregator.average(data, field)
                elif agg_func == "min":
                    result[field] = DataAggregator.min(data, field)
                elif agg_func == "max":
                    result[field] = DataAggregator.max(data, field)

        return ActionResult(success=True, message="Aggregated", data={"result": result})

    def _join(self, data: List[Dict], params: Dict) -> ActionResult:
        """Join data sources."""
        left = data
        right = params.get("right", [])
        join_type = params.get("join_type", "inner")
        left_key = params.get("left_key", "id")
        right_key = params.get("right_key", "id")

        if join_type == "inner":
            result = DataJoiner.inner_join(left, right, left_key, right_key)
        elif join_type == "left":
            result = DataJoiner.left_join(left, right, left_key, right_key)
        elif join_type == "full":
            result = DataJoiner.full_outer_join(left, right, left_key, right_key)
        else:
            return ActionResult(success=False, message=f"Unknown join type: {join_type}")

        return ActionResult(success=True, message=f"{join_type} join: {len(result)} results", data={"result": result})

    def _filter(self, data: List[Dict], params: Dict) -> ActionResult:
        """Filter data."""
        expr = params.get("expression", "True")

        try:
            compiled = eval(f"lambda x: {expr}")
            result = [item for item in data if compiled(item)]
            return ActionResult(success=True, message=f"Filtered to {len(result)} items", data={"result": result})
        except Exception as e:
            return ActionResult(success=False, message=f"Filter error: {str(e)}")

    def _sort(self, data: List[Dict], params: Dict) -> ActionResult:
        """Sort data."""
        keys = params.get("keys", [])
        ascending = params.get("ascending", True)

        if not keys:
            return ActionResult(success=False, message="keys is required")

        try:
            result = sorted(data, key=lambda x: tuple(x.get(k) for k in keys), reverse=not ascending)
            return ActionResult(success=True, message=f"Sorted by {keys}", data={"result": result})
        except Exception as e:
            return ActionResult(success=False, message=f"Sort error: {str(e)}")

    def _group(self, data: List[Dict], params: Dict) -> ActionResult:
        """Group data."""
        keys = params.get("keys", [])

        if not keys:
            return ActionResult(success=False, message="keys is required")

        result = DataAggregator.group_aggregate(data, keys, {"_count": "count"})
        return ActionResult(success=True, message=f"Grouped by {keys}", data={"result": result})
