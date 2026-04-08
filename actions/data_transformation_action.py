"""
Data Transformation Action Module

Provides data transformation, mapping, and conversion operations.
"""
from typing import Any, Optional, Callable, TypeVar
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio


T = TypeVar('T')


class TransformType(Enum):
    """Transformation types."""
    MAP = "map"
    FILTER = "filter"
    REDUCE = "reduce"
    FLATTEN = "flatten"
    PIVOT = "pivot"
    GROUP = "group"
    SORT = "sort"
    DEDUPE = "dedupe"
    MERGE = "merge"


@dataclass
class TransformRule:
    """A transformation rule."""
    rule_id: str
    transform_type: TransformType
    field: Optional[str] = None
    expression: Optional[str] = None
    target_field: Optional[str] = None
    priority: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TransformConfig:
    """Configuration for transformation."""
    rules: list[TransformRule]
    parallel: bool = True
    error_handling: str = "skip"  # skip, fail, collect
    max_workers: int = 10


@dataclass
class TransformResult:
    """Result of transformation."""
    success: bool
    output: list[dict]
    errors: list[dict]
    transformed_count: int
    error_count: int
    duration_ms: float


class DataTransformationAction:
    """Main data transformation action handler."""
    
    def __init__(self):
        self._custom_transforms: dict[str, Callable] = {}
        self._stats: dict[str, Any] = {}
    
    def register_transform(
        self,
        name: str,
        transform_fn: Callable[[Any], Any]
    ) -> "DataTransformationAction":
        """Register a custom transform function."""
        self._custom_transforms[name] = transform_fn
        return self
    
    async def transform(
        self,
        data: list[dict],
        config: TransformConfig
    ) -> TransformResult:
        """
        Apply transformations to data.
        
        Args:
            data: List of records to transform
            config: Transformation configuration
            
        Returns:
            TransformResult with transformed data
        """
        start_time = datetime.now()
        output = list(data)
        errors = []
        
        # Sort rules by priority
        rules = sorted(config.rules, key=lambda r: r.priority)
        
        for rule in rules:
            try:
                if config.parallel and len(output) > 100:
                    output = await self._transform_parallel(output, rule, config)
                else:
                    output = await self._transform_sequential(output, rule, config)
            except Exception as e:
                if config.error_handling == "fail":
                    return TransformResult(
                        success=False,
                        output=[],
                        errors=[{"rule": rule.rule_id, "error": str(e)}],
                        transformed_count=0,
                        error_count=1,
                        duration_ms=0
                    )
                elif config.error_handling == "collect":
                    errors.append({"rule": rule.rule_id, "error": str(e)})
        
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        return TransformResult(
            success=len(errors) == 0,
            output=output,
            errors=errors,
            transformed_count=len(output),
            error_count=len(errors),
            duration_ms=duration_ms
        )
    
    async def _transform_parallel(
        self,
        data: list[dict],
        rule: TransformRule,
        config: TransformConfig
    ) -> list[dict]:
        """Apply transform in parallel."""
        semaphore = asyncio.Semaphore(config.max_workers)
        
        async def transform_item(item):
            async with semaphore:
                return await self._apply_rule(item, rule)
        
        tasks = [transform_item(item) for item in data]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        return [r for r in results if not isinstance(r, Exception)]
    
    async def _transform_sequential(
        self,
        data: list[dict],
        rule: TransformRule,
        config: TransformConfig
    ) -> list[dict]:
        """Apply transform sequentially."""
        results = []
        
        for item in data:
            try:
                result = await self._apply_rule(item, rule)
                results.append(result)
            except Exception:
                if config.error_handling == "skip":
                    continue
                elif config.error_handling == "collect":
                    results.append(item)  # Keep original
        
        return results
    
    async def _apply_rule(self, record: dict, rule: TransformRule) -> dict:
        """Apply a single transformation rule."""
        result = dict(record)
        
        if rule.transform_type == TransformType.MAP:
            result = await self._transform_map(result, rule)
        elif rule.transform_type == TransformType.FILTER:
            result = await self._transform_filter(result, rule)
        elif rule.transform_type == TransformType.FLATTEN:
            result = await self._transform_flatten(result, rule)
        elif rule.transform_type == TransformType.SORT:
            result = await self._transform_sort(result, rule)
        elif rule.transform_type == TransformType.DEDUPE:
            result = await self._transform_dedupe(result, rule)
        
        return result
    
    async def _transform_map(
        self,
        record: dict,
        rule: TransformRule
    ) -> dict:
        """Apply map transformation."""
        result = dict(record)
        
        if rule.expression and rule.target_field:
            # Evaluate expression
            value = await self._evaluate_expression(rule.expression, record)
            result[rule.target_field] = value
        
        return result
    
    async def _transform_filter(
        self,
        record: dict,
        rule: TransformRule
    ) -> dict:
        """Apply filter transformation - returns None to filter out."""
        if rule.expression:
            keep = await self._evaluate_condition(rule.expression, record)
            if not keep:
                raise ValueError("Filtered out")
        
        return record
    
    async def _transform_flatten(
        self,
        record: dict,
        rule: TransformRule
    ) -> dict:
        """Flatten nested structures."""
        result = {}
        
        def flatten_recursive(obj, prefix=""):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    new_key = f"{prefix}{key}" if prefix else key
                    flatten_recursive(value, f"{new_key}.")
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    new_key = f"{prefix}{i}"
                    flatten_recursive(item, f"{new_key}.")
            else:
                result[prefix[:-1] if prefix.endswith('.') else prefix] = obj
        
        flatten_recursive(record)
        return result
    
    async def _transform_sort(
        self,
        records: list[dict],
        rule: TransformRule
    ) -> list[dict]:
        """Sort records."""
        if rule.field:
            reverse = rule.metadata.get("reverse", False)
            return sorted(
                records,
                key=lambda r: r.get(rule.field, ""),
                reverse=reverse
            )
        return records
    
    async def _transform_dedupe(
        self,
        records: list[dict],
        rule: TransformRule
    ) -> list[dict]:
        """Remove duplicates."""
        seen = set()
        result = []
        
        for record in records:
            key_value = record.get(rule.field, str(record))
            
            if key_value not in seen:
                seen.add(key_value)
                result.append(record)
        
        return result
    
    async def _evaluate_expression(
        self,
        expression: str,
        record: dict
    ) -> Any:
        """Evaluate a transformation expression."""
        # Support simple field references
        if expression.startswith("${") and expression.endswith("}"):
            field_name = expression[2:-1]
            return record.get(field_name)
        
        # Support simple arithmetic
        if "+" in expression:
            parts = expression.split("+")
            values = [record.get(p.strip().replace("${", "").replace("}", ""), 0) for p in parts]
            return sum(values)
        
        return expression
    
    async def _evaluate_condition(
        self,
        condition: str,
        record: dict
    ) -> bool:
        """Evaluate a filter condition."""
        # Simple equality check
        if "==" in condition:
            field, value = condition.split("==")
            field = field.strip().replace("${", "").replace("}", "")
            return record.get(field.strip()) == value.strip()
        
        return True
    
    async def map_fields(
        self,
        data: list[dict],
        field_mapping: dict[str, str]
    ) -> list[dict]:
        """
        Map/rename fields in data.
        
        Args:
            data: List of records
            field_mapping: Dict of {source_field: target_field}
            
        Returns:
            Transformed records
        """
        result = []
        
        for record in data:
            new_record = {}
            for source, target in field_mapping.items():
                if source in record:
                    new_record[target] = record[source]
            # Keep unmapped fields
            for key, value in record.items():
                if key not in field_mapping:
                    new_record[key] = value
            result.append(new_record)
        
        return result
    
    async def filter_records(
        self,
        data: list[dict],
        predicate: Callable[[dict], bool]
    ) -> list[dict]:
        """Filter records using a predicate function."""
        return [record for record in data if predicate(record)]
    
    async def merge_records(
        self,
        left: list[dict],
        right: list[dict],
        on: str,
        how: str = "inner"  # inner, left, right, outer
    ) -> list[dict]:
        """Merge two lists of records on a key field."""
        right_index = {r.get(on): r for r in right}
        result = []
        
        for left_record in left:
            right_record = right_index.get(left_record.get(on))
            
            if right_record:
                merged = {**left_record, **right_record}
                result.append(merged)
            elif how in ["left", "outer"]:
                result.append(left_record)
        
        if how in ["right", "outer"]:
            for right_record in right:
                if right_record.get(on) not in [r.get(on) for r in result]:
                    result.append(right_record)
        
        return result
    
    async def pivot_data(
        self,
        data: list[dict],
        index: str,
        columns: str,
        values: str,
        aggfunc: str = "sum"
    ) -> list[dict]:
        """Pivot data from long to wide format."""
        pivot: dict[str, dict] = {}
        
        for record in data:
            idx_value = record.get(index)
            col_value = record.get(columns)
            val = record.get(values)
            
            if idx_value not in pivot:
                pivot[idx_value] = {"_index": idx_value}
            
            pivot[idx_value][col_value] = val
        
        return list(pivot.values())
    
    async def unpivot_data(
        self,
        data: list[dict],
        id_columns: list[str],
        value_columns: list[str]
    ) -> list[dict]:
        """Unpivot data from wide to long format."""
        result = []
        
        for record in data:
            base = {col: record[col] for col in id_columns if col in record}
            
            for col in value_columns:
                if col in record:
                    new_record = {**base, "column": col, "value": record[col]}
                    result.append(new_record)
        
        return result
    
    def get_stats(self) -> dict[str, Any]:
        """Get transformation statistics."""
        return dict(self._stats)
