"""API request/response transformation and mapping.

This module provides request and response transformation capabilities:
- Request envelope/wrapper creation
- Response normalization
- Field mapping and renaming
- Data type conversion

Example:
    >>> from actions.api_transform_action import RequestTransformer, ResponseNormalizer
    >>> transformer = RequestTransformer()
    >>> normalized = transformer.normalize_response(raw_data, schema)
"""

from __future__ import annotations

import logging
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class TransformType(Enum):
    """Type of transformation to apply."""
    RENAME = "rename"
    TYPE_CONVERT = "type_convert"
    COMPUTE = "compute"
    OMIT = "omit"
    ADD = "add"


@dataclass
class FieldMapping:
    """Mapping configuration for a field."""
    source: str
    target: str
    transform_type: TransformType = TransformType.RENAME
    default: Any = None
    converter: Optional[Callable[[Any], Any]] = None


@dataclass
class TransformSchema:
    """Schema for request/response transformation."""
    field_mappings: list[FieldMapping] = field(default_factory=list)
    root_key: Optional[str] = None
    envelope_key: Optional[str] = None


class RequestTransformer:
    """Transform API requests before sending.

    Applies field mappings, type conversions, and envelope wrapping.
    """

    def __init__(self, schema: Optional[TransformSchema] = None) -> None:
        self.schema = schema

    def transform(
        self,
        data: dict[str, Any],
        schema: Optional[TransformSchema] = None,
    ) -> dict[str, Any]:
        """Transform request data.

        Args:
            data: Input request data.
            schema: Optional transformation schema.

        Returns:
            Transformed request data.
        """
        schema = schema or self.schema
        if not schema:
            return data

        result = {}
        data_keys = set(data.keys())
        mapping_dict = {m.source: m for m in schema.field_mappings}

        for key in data_keys:
            mapping = mapping_dict.get(key)
            if mapping and mapping.transform_type == TransformType.OMIT:
                continue
            if mapping:
                result[mapping.target] = self._apply_transform(
                    data[key], mapping
                )
            else:
                result[key] = data[key]

        for mapping in schema.field_mappings:
            if mapping.target not in result:
                result[mapping.target] = mapping.default

        if schema.envelope_key:
            result = {schema.envelope_key: result}

        return result

    def _apply_transform(self, value: Any, mapping: FieldMapping) -> Any:
        """Apply a single field transformation."""
        if mapping.converter:
            try:
                return mapping.converter(value)
            except Exception as e:
                logger.warning(f"Converter failed for {mapping.source}: {e}")
                return value
        if mapping.transform_type == TransformType.TYPE_CONVERT:
            return self._convert_type(value, mapping.default)
        return value

    def _convert_type(self, value: Any, target_type: Any) -> Any:
        """Convert value to target type."""
        if target_type is bool:
            return bool(value)
        elif target_type is int:
            return int(value)
        elif target_type is float:
            return float(value)
        elif target_type is str:
            return str(value)
        return value


class ResponseNormalizer:
    """Normalize API responses to a standard format.

    Flattens nested structures, renames fields, and handles pagination.
    """

    def __init__(
        self,
        schema: Optional[TransformSchema] = None,
        flatten: bool = True,
        separator: str = ".",
    ) -> None:
        self.schema = schema
        self.flatten = flatten
        self.separator = separator

    def normalize(
        self,
        response: Any,
        schema: Optional[TransformSchema] = None,
    ) -> dict[str, Any]:
        """Normalize a response.

        Args:
            response: Raw API response.
            schema: Optional transformation schema.

        Returns:
            Normalized response data.
        """
        schema = schema or self.schema
        if isinstance(response, dict):
            data = response
        elif isinstance(response, list):
            data = {"items": response, "count": len(response)}
        else:
            data = {"value": response}

        if self.flatten:
            data = self._flatten_dict(data)

        if schema:
            data = self._apply_schema(data, schema)

        return data

    def _flatten_dict(
        self,
        data: dict[str, Any],
        parent_key: str = "",
        sep: str = ".",
    ) -> dict[str, Any]:
        """Flatten nested dictionary."""
        items: list[tuple[str, Any]] = []
        for key, value in data.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            if isinstance(value, dict):
                items.extend(self._flatten_dict(value, new_key, sep).items())
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        items.extend(
                            self._flatten_dict(item, f"{new_key}[{i}]", sep).items()
                        )
                    else:
                        items.append((f"{new_key}[{i}]", item))
            else:
                items.append((new_key, value))
        return dict(items)

    def _apply_schema(
        self,
        data: dict[str, Any],
        schema: TransformSchema,
    ) -> dict[str, Any]:
        """Apply transformation schema to data."""
        mapping_dict = {m.source: m for m in schema.field_mappings}
        result = {}
        for key, value in data.items():
            mapping = mapping_dict.get(key)
            if mapping:
                if mapping.transform_type == TransformType.OMIT:
                    continue
                result[mapping.target] = self._apply_transform(value, mapping)
            else:
                result[key] = value
        return result

    def _apply_transform(self, value: Any, mapping: FieldMapping) -> Any:
        """Apply field transformation."""
        if mapping.transform_type == TransformType.COMPUTE and mapping.converter:
            return mapping.converter(value)
        return value


class PaginationTransformer:
    """Handle pagination in API responses.

    Supports cursor-based and offset-based pagination.
    """

    def __init__(self, page_size: int = 20) -> None:
        self.page_size = page_size

    def extract_pagination_info(self, response: dict[str, Any]) -> dict[str, Any]:
        """Extract pagination metadata from response.

        Args:
            response: API response with pagination info.

        Returns:
            Pagination metadata.
        """
        return {
            "has_more": response.get("has_more", response.get("hasNextPage", False)),
            "next_cursor": response.get("next_cursor", response.get("cursor", None)),
            "total": response.get("total", response.get("total_count", None)),
            "page_size": self.page_size,
        }

    def get_page(
        self,
        items: list[Any],
        page: int,
    ) -> list[Any]:
        """Get a specific page of items.

        Args:
            items: All items.
            page: Page number (1-indexed).

        Returns:
            Items for the requested page.
        """
        start = (page - 1) * self.page_size
        end = start + self.page_size
        return items[start:end]

    def merge_pages(
        self,
        page1: list[Any],
        page2: list[Any],
    ) -> list[Any]:
        """Merge two pages of items."""
        seen = set()
        result = []
        for item in page1 + page2:
            item_id = id(item)
            if item_id not in seen:
                seen.add(item_id)
                result.append(item)
        return result
