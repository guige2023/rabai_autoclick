"""
Data Convert Action - Converts data between formats.

This module provides data conversion capabilities for
transforming between different data formats.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class ConversionResult:
    """Result of data conversion."""
    success: bool
    data: Any
    format: str


class DataConverter:
    """Converts data between formats."""
    
    def __init__(self) -> None:
        pass
    
    def to_dict_list(self, data: Any) -> list[dict[str, Any]]:
        """Convert data to list of dicts."""
        if isinstance(data, list):
            return [dict(r) if isinstance(r, dict) else r for r in data]
        return [dict(data)] if isinstance(data, dict) else []
    
    def to_flat_dict(self, data: dict[str, Any], separator: str = "_") -> dict[str, Any]:
        """Flatten nested dict."""
        result = {}
        for key, value in data.items():
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    result[f"{key}{separator}{sub_key}"] = sub_value
            else:
                result[key] = value
        return result


class DataConvertAction:
    """Data conversion action for automation workflows."""
    
    def __init__(self) -> None:
        self.converter = DataConverter()
    
    async def convert(self, data: Any, target: str = "dict_list") -> ConversionResult:
        """Convert data to target format."""
        try:
            if target == "dict_list":
                converted = self.converter.to_dict_list(data)
            elif target == "flat_dict":
                converted = self.converter.to_flat_dict(data) if isinstance(data, dict) else {}
            else:
                converted = data
            return ConversionResult(success=True, data=converted, format=target)
        except Exception as e:
            return ConversionResult(success=False, data=None, format=target)


__all__ = ["ConversionResult", "DataConverter", "DataConvertAction"]
