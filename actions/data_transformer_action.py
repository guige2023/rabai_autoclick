"""
Data Transformer Action Module.

Transforms data between different schemas and formats with
 field mapping, type conversion, and complex transformations.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class TransformType(Enum):
    """Type of transformation."""
    DIRECT = "direct"
    RENAME = "rename"
    MAP = "map"
    COMBINE = "combine"
    SPLIT = "split"
    EXTRACT = "extract"
    CONVERT = "convert"
    CUSTOM = "custom"


@dataclass
class FieldMapping:
    """A mapping from source to target field."""
    source: str
    target: str
    transform_type: TransformType = TransformType.DIRECT
    transform_func: Optional[Callable[[Any], Any]] = None
    default_value: Any = None
    required: bool = False


@dataclass
class TransformConfig:
    """Configuration for a transformation pipeline."""
    mappings: list[FieldMapping]
    skip_nulls: bool = False
    skip_errors: bool = True


@dataclass
class TransformResult:
    """Result of a transformation operation."""
    success: bool
    data: Optional[dict[str, Any]] = None
    errors: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


class DataTransformerAction:
    """
    Data transformation engine with field mapping and type conversion.

    Transforms data between different schemas using configurable
    field mappings and transformation functions.

    Example:
        transformer = DataTransformerAction()
        transformer.add_mapping("user_name", "displayName", TransformType.RENAME)
        transformer.add_mapping("email", "emailHash", TransformType.CUSTOM,
            func=lambda x: hash(x))
        result = transformer.transform(source_data, config)
    """

    def __init__(self) -> None:
        self._mappings: list[FieldMapping] = []
        self._custom_transforms: dict[str, Callable] = {}

    def add_mapping(
        self,
        source: str,
        target: str,
        transform_type: TransformType = TransformType.DIRECT,
        func: Optional[Callable[[Any], Any]] = None,
        default_value: Any = None,
        required: bool = False,
    ) -> "DataTransformerAction":
        """Add a field mapping."""
        mapping = FieldMapping(
            source=source,
            target=target,
            transform_type=transform_type,
            transform_func=func,
            default_value=default_value,
            required=required,
        )
        self._mappings.append(mapping)
        return self

    def add_rename(self, source: str, target: str) -> "DataTransformerAction":
        """Add a simple rename mapping."""
        return self.add_mapping(source, target, TransformType.RENAME)

    def add_field_map(
        self,
        source: str,
        target: str,
        mapping_dict: dict[Any, Any],
        default: Any = None,
    ) -> "DataTransformerAction":
        """Add a value mapping."""
        def map_func(value: Any) -> Any:
            return mapping_dict.get(value, default)

        return self.add_mapping(source, target, TransformType.MAP, func=map_func)

    def add_combination(
        self,
        target: str,
        sources: list[str],
        separator: str = " ",
        func: Optional[Callable[[list], Any]] = None,
    ) -> "DataTransformerAction":
        """Add a field that combines multiple sources."""
        def combine_func(data: dict[str, Any]) -> Any:
            values = []
            for src in sources:
                if src in data and data[src] is not None:
                    values.append(str(data[src]))

            if func:
                return func(values)

            return separator.join(values) if values else None

        combined_mapping = FieldMapping(
            source="__combined__",
            target=target,
            transform_type=TransformType.COMBINE,
            transform_func=combine_func,
        )
        self._mappings.append(combined_mapping)
        return self

    def add_split(
        self,
        source: str,
        targets: list[str],
        delimiter: str = ",",
    ) -> "DataTransformerAction":
        """Add a split transformation."""
        def split_func(value: Any) -> list[str]:
            if value is None:
                return [""] * len(targets)
            return str(value).split(delimiter)

        for i, target in enumerate(targets):
            def make_index_func(idx: int) -> Callable[[Any], Any]:
                def index_func(value: Any) -> Any:
                    parts = str(value).split(delimiter) if value else []
                    return parts[idx] if idx < len(parts) else None
                return index_func

            self.add_mapping(
                source=source,
                target=target,
                transform_type=TransformType.SPLIT,
                func=make_index_func(i),
            )
        return self

    def add_extraction(
        self,
        source: str,
        target: str,
        pattern: str,
        group: int = 0,
    ) -> "DataTransformerAction":
        """Add regex extraction transformation."""
        import re
        compiled = re.compile(pattern)

        def extract_func(value: Any) -> Any:
            if value is None:
                return None
            match = compiled.search(str(value))
            if match:
                return match.group(group) if group < len(match.groups()) + 1 else match.group(0)
            return None

        return self.add_mapping(source, target, TransformType.EXTRACT, func=extract_func)

    def add_custom_transform(
        self,
        name: str,
        func: Callable[[Any], Any],
    ) -> "DataTransformerAction":
        """Register a custom named transform function."""
        self._custom_transforms[name] = func
        return self

    def transform(
        self,
        data: dict[str, Any],
        config: Optional[TransformConfig] = None,
    ) -> TransformResult:
        """Transform data according to configured mappings."""
        config = config or TransformConfig(mappings=self._mappings)
        errors: list[dict[str, Any]] = []
        warnings: list[str] = []

        result: dict[str, Any] = {}

        for mapping in config.mappings:
            try:
                if mapping.transform_type == TransformType.COMBINE:
                    value = mapping.transform_func(data)
                else:
                    value = data.get(mapping.source)

                    if value is None:
                        if mapping.required:
                            errors.append({
                                "field": mapping.source,
                                "error": "Required field is missing",
                            })
                            continue
                        elif mapping.default_value is not None:
                            value = mapping.default_value
                        elif config.skip_nulls:
                            warnings.append(f"Skipping null value for {mapping.source}")
                            continue
                        else:
                            value = None

                    if mapping.transform_func:
                        value = mapping.transform_func(value)
                    elif mapping.transform_type == TransformType.RENAME:
                        pass
                    elif mapping.transform_type == TransformType.CONVERT:
                        value = self._convert_type(value)

                result[mapping.target] = value

            except Exception as e:
                error_entry = {
                    "field": mapping.source,
                    "error": str(e),
                    "target": mapping.target,
                }
                errors.append(error_entry)

                if config.skip_errors:
                    if mapping.default_value is not None:
                        result[mapping.target] = mapping.default_value
                    continue

                return TransformResult(
                    success=False,
                    data=None,
                    errors=errors,
                    warnings=warnings,
                )

        return TransformResult(
            success=True,
            data=result,
            errors=errors,
            warnings=warnings,
        )

    def transform_batch(
        self,
        data: list[dict[str, Any]],
        config: Optional[TransformConfig] = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Transform a batch of records."""
        results: list[dict[str, Any]] = []
        all_errors: list[dict[str, Any]] = []

        for idx, record in enumerate(data):
            result = self.transform(record, config)
            if result.success and result.data:
                results.append(result.data)
            if result.errors:
                for err in result.errors:
                    err["record_index"] = idx
                all_errors.extend(result.errors)

        return results, all_errors

    def _convert_type(self, value: Any) -> Any:
        """Attempt to convert a value to appropriate type."""
        if value is None:
            return None

        if isinstance(value, str):
            if value.lower() in ("true", "false"):
                return value.lower() == "true"
            try:
                if "." in value:
                    return float(value)
                return int(value)
            except ValueError:
                return value

        return value

    def get_mapping_stats(self) -> dict[str, int]:
        """Get statistics about configured mappings."""
        return {
            "total_mappings": len(self._mappings),
            "direct": sum(1 for m in self._mappings if m.transform_type == TransformType.DIRECT),
            "renames": sum(1 for m in self._mappings if m.transform_type == TransformType.RENAME),
            "custom": sum(1 for m in self._mappings if m.transform_func is not None),
        }
