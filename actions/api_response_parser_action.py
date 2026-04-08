"""
API Response Parser Action Module.

Parses, transforms, and validates API responses with support
for JSONPath, XML, CSV, and custom transformation pipelines.

Author: RabAi Team
"""

from __future__ import annotations

import json
import re
import sys
import os
import csv
import io
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ResponseFormat(Enum):
    """Supported response formats."""
    JSON = "json"
    XML = "xml"
    CSV = "csv"
    TEXT = "text"
    HTML = "html"
    BINARY = "binary"


class ParseMode(Enum):
    """Parsing modes."""
    AUTO = "auto"
    STRICT = "strict"
    LENIENT = "lenient"
    SILENT = "silent"


@dataclass
class TransformRule:
    """A single transformation rule."""
    field_path: str
    transform_type: str
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationRule:
    """A single validation rule."""
    field_path: str
    rule_type: str
    params: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None


@dataclass
class ParseResult:
    """Result of parsing an API response."""
    success: bool
    parsed_data: Any
    format_detected: ResponseFormat
    transformations_applied: List[str]
    validation_errors: List[Dict[str, Any]]
    metadata: Dict[str, Any]


class JsonPathParser:
    """Simple JSONPath-like parser for extracting data from JSON."""
    
    @staticmethod
    def get(data: Any, path: str, default: Any = None) -> Any:
        """Get value from data using dot-notation path."""
        if not path or path == "$":
            return data
        
        parts = path.replace("[", ".").replace("]", "").split(".")
        current = data
        
        for part in parts:
            if part.isdigit():
                idx = int(part)
                if isinstance(current, list) and 0 <= idx < len(current):
                    current = current[idx]
                else:
                    return default
            elif isinstance(current, dict):
                current = current.get(part, default)
            else:
                return default
        
        return current
    
    @staticmethod
    def set(data: Any, path: str, value: Any) -> Any:
        """Set value in data using dot-notation path."""
        if isinstance(data, dict) and "." in path:
            first, rest = path.split(".", 1)
            if first not in data:
                data[first] = {}
            data[first] = JsonPathParser.set(data[first], rest, value)
            return data
        elif isinstance(data, dict):
            data[path] = value
            return data
        elif isinstance(data, list) and path.isdigit():
            idx = int(path)
            if 0 <= idx < len(data):
                data[idx] = value
        return data


class XmlParser:
    """XML parser and extractor."""
    
    @staticmethod
    def to_dict(element: ET.Element) -> Dict[str, Any]:
        """Convert XML element to dictionary."""
        result: Dict[str, Any] = {}
        
        if element.attrib:
            result["@attributes"] = element.attrib
        
        if element.text and element.text.strip():
            if len(element) == 0:
                return element.text.strip()
            result["_text"] = element.text.strip()
        
        for child in element:
            child_data = XmlParser.to_dict(child)
            
            if child.tag in result:
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                result[child.tag] = child_data
        
        return result
    
    @staticmethod
    def extract(xml_string: str, xpath: str) -> List[str]:
        """Extract values using XPath."""
        try:
            root = ET.fromstring(xml_string)
            namespaces = {
                "re": "http://exslt.org/regular-expressions"
            }
            elements = root.findall(xpath)
            return [el.text for el in elements if el.text]
        except Exception:
            return []


class ApiResponseParserAction(BaseAction):
    """API response parser action.
    
    Parses and transforms API responses with extraction,
    validation, and data transformation capabilities.
    """
    action_type = "api_response_parser"
    display_name = "API响应解析"
    description = "解析和转换API响应数据"
    
    def __init__(self):
        super().__init__()
        self._transform_functions: Dict[str, Callable] = {
            "uppercase": lambda x, **kw: str(x).upper() if x else "",
            "lowercase": lambda x, **kw: str(x).lower() if x else "",
            "trim": lambda x, **kw: str(x).strip() if x else "",
            "strip_tags": lambda x, **kw: re.sub(r"<[^>]+>", "", str(x)) if x else "",
            "to_int": lambda x, **kw: int(float(x)) if x else 0,
            "to_float": lambda x, **kw: float(x) if x else 0.0,
            "to_string": lambda x, **kw: str(x) if x is not None else "",
            "to_bool": lambda x, **kw: str(x).lower() in ("true", "1", "yes", "on") if x else False,
            "to_json": lambda x, **kw: json.dumps(x) if isinstance(x, (dict, list)) else x,
            "from_json": lambda x, **kw: json.loads(x) if isinstance(x, str) else x,
            "hash": lambda x, **kw: str(hash(str(x))) if x else "",
            "slice": lambda x, start=0, end=None, **kw: str(x)[start:end] if x else "",
            "replace": lambda x, pattern="", replacement="", **kw: re.sub(pattern, replacement, str(x)) if x else "",
            "default": lambda x, default="", **kw: x if x else default,
            "coalesce": lambda x, *args, **kw: x if x else (args[0] if args else ""),
        }
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Parse and transform an API response.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - response: The API response (dict, string, or raw)
                - format: Response format hint (json/xml/csv/auto)
                - extract: JSONPath to extract specific data
                - transforms: List of transform rules
                - validations: List of validation rules
                - flatten: Whether to flatten nested structures
                - mode: Parse mode (auto/strict/lenient/silent)
                
        Returns:
            ActionResult with parsed and transformed data.
        """
        import time
        start_time = time.time()
        
        response = params.get("response")
        format_hint = params.get("format", "auto")
        extract_path = params.get("extract")
        transform_rules = params.get("transforms", [])
        validation_rules = params.get("validations", [])
        flatten = params.get("flatten", False)
        mode = ParseMode(params.get("mode", "auto"))
        
        if response is None:
            return ActionResult(
                success=False,
                message="Missing required parameter: response",
                duration=time.time() - start_time
            )
        
        try:
            detected_format = self._detect_format(response, format_hint)
            
            parsed = self._parse_response(response, detected_format, mode)
            
            if extract_path:
                parsed = JsonPathParser.get(parsed, extract_path, [])
                if parsed is None:
                    parsed = []
                elif not isinstance(parsed, list):
                    parsed = [parsed]
            
            transformations_applied = []
            
            for rule in transform_rules:
                parsed = self._apply_transform(parsed, rule)
                transformations_applied.append(f"{rule['field_path']}:{rule['transform_type']}")
            
            validation_errors = []
            for rule in validation_rules:
                errors = self._validate(parsed, rule)
                validation_errors.extend(errors)
            
            if flatten:
                parsed = self._flatten(parsed)
            
            return ActionResult(
                success=len(validation_errors) == 0,
                message=f"Parsed {detected_format.value} with {len(transformations_applied)} transforms",
                data={
                    "data": parsed,
                    "format": detected_format.value,
                    "transformations_applied": transformations_applied,
                    "validation_errors": validation_errors,
                    "metadata": {
                        "total_items": len(parsed) if isinstance(parsed, list) else 1,
                        "flattened": flatten
                    }
                },
                duration=time.time() - start_time
            )
            
        except Exception as e:
            if mode == ParseMode.SILENT:
                return ActionResult(
                    success=True,
                    message=f"Parse failed silently: {str(e)}",
                    data={"data": response, "format": "raw"},
                    duration=time.time() - start_time
                )
            return ActionResult(
                success=False,
                message=f"Parse failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _detect_format(self, response: Any, hint: str) -> ResponseFormat:
        """Detect response format."""
        if hint != "auto":
            return ResponseFormat(hint)
        
        if isinstance(response, (dict, list)):
            return ResponseFormat.JSON
        elif isinstance(response, str):
            stripped = response.strip()
            if stripped.startswith("{") or stripped.startswith("["):
                return ResponseFormat.JSON
            elif stripped.startswith("<"):
                return ResponseFormat.XML
            elif "," in stripped and "\n" in stripped:
                return ResponseFormat.CSV
            elif "<html" in stripped.lower() or "</html>" in stripped.lower():
                return ResponseFormat.HTML
            else:
                return ResponseFormat.TEXT
        elif isinstance(response, bytes):
            return ResponseFormat.BINARY
        
        return ResponseFormat.JSON
    
    def _parse_response(self, response: Any, format: ResponseFormat, mode: ParseMode) -> Any:
        """Parse response based on format."""
        if format == ResponseFormat.JSON:
            if isinstance(response, (dict, list)):
                return response
            elif isinstance(response, str):
                try:
                    return json.loads(response)
                except json.JSONDecodeError as e:
                    if mode == ParseMode.STRICT:
                        raise
                    return response
            return response
        
        elif format == ResponseFormat.XML:
            if isinstance(response, str):
                try:
                    root = ET.fromstring(response)
                    return XmlParser.to_dict(root)
                except ET.ParseError as e:
                    if mode == ParseMode.STRICT:
                        raise
                    return {"raw": response}
            return response
        
        elif format == ResponseFormat.CSV:
            if isinstance(response, str):
                try:
                    reader = csv.DictReader(io.StringIO(response))
                    return list(reader)
                except Exception:
                    return []
            elif isinstance(response, list):
                return response
            return []
        
        else:
            return response
    
    def _apply_transform(self, data: Any, rule: Dict[str, Any]) -> Any:
        """Apply a single transformation rule."""
        field_path = rule.get("field_path", "$")
        transform_type = rule.get("transform_type", "default")
        transform_params = rule.get("params", {})
        
        if isinstance(data, list):
            return [
                self._apply_transform_to_item(item, field_path, transform_type, transform_params)
                for item in data
            ]
        else:
            return self._apply_transform_to_item(data, field_path, transform_type, transform_params)
    
    def _apply_transform_to_item(
        self, item: Any, field_path: str, transform_type: str, params: Dict[str, Any]
    ) -> Any:
        """Apply transform to a single item."""
        if field_path != "$":
            value = JsonPathParser.get(item, field_path)
        else:
            value = item
        
        transform_fn = self._transform_functions.get(transform_type)
        if transform_fn:
            try:
                new_value = transform_fn(value, **params)
            except Exception:
                new_value = value
        else:
            new_value = value
        
        if field_path != "$" and isinstance(item, dict):
            JsonPathParser.set(item, field_path, new_value)
            return item
        
        return new_value
    
    def _validate(self, data: Any, rule: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Validate data against a rule."""
        field_path = rule.get("field_path", "$")
        rule_type = rule.get("rule_type", "required")
        error_msg = rule.get("error_message", f"Validation failed for {field_path}")
        params = rule.get("params", {})
        
        errors = []
        
        if isinstance(data, list):
            for i, item in enumerate(data):
                item_errors = self._validate_item(item, field_path, rule_type, params, error_msg, i)
                errors.extend(item_errors)
        else:
            errors = self._validate_item(data, field_path, rule_type, params, error_msg)
        
        return errors
    
    def _validate_item(
        self, item: Any, field_path: str, rule_type: str, params: Dict[str, Any], error_msg: str, index: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Validate a single item against a rule."""
        errors = []
        value = JsonPathParser.get(item, field_path) if field_path != "$" else item
        
        if rule_type == "required":
            if value is None or value == "":
                prefix = f"[{index}] " if index is not None else ""
                errors.append({"field": field_path, "rule": rule_type, "message": f"{prefix}{error_msg}"})
        
        elif rule_type == "type":
            expected_type = params.get("expected_type", "string")
            type_map = {"string": str, "int": int, "float": float, "bool": bool, "list": list, "dict": dict}
            expected = type_map.get(expected_type)
            if expected and not isinstance(value, expected):
                prefix = f"[{index}] " if index is not None else ""
                errors.append({"field": field_path, "rule": rule_type, "message": f"{prefix}Expected {expected_type}"})
        
        elif rule_type == "min":
            min_val = params.get("min")
            if min_val is not None and value is not None and float(value) < float(min_val):
                errors.append({"field": field_path, "rule": rule_type, "message": f"{error_msg}: min={min_val}"})
        
        elif rule_type == "max":
            max_val = params.get("max")
            if max_val is not None and value is not None and float(value) > float(max_val):
                errors.append({"field": field_path, "rule": rule_type, "message": f"{error_msg}: max={max_val}"})
        
        elif rule_type == "pattern":
            pattern = params.get("pattern")
            if pattern and value and not re.match(pattern, str(value)):
                errors.append({"field": field_path, "rule": rule_type, "message": error_msg})
        
        elif rule_type == "enum":
            allowed = params.get("values", [])
            if value and value not in allowed:
                errors.append({"field": field_path, "rule": rule_type, "message": error_msg})
        
        return errors
    
    def _flatten(self, data: Any, parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
        """Flatten nested dictionaries."""
        items: List[Tuple[str, Any]] = []
        
        if isinstance(data, dict):
            for k, v in data.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.extend(self._flatten(v, new_key, sep).items())
                elif isinstance(v, list):
                    for i, item in enumerate(v):
                        if isinstance(item, dict):
                            items.extend(self._flatten(item, f"{new_key}[{i}]", sep).items())
                        else:
                            items.append((f"{new_key}[{i}]", item))
                else:
                    items.append((new_key, v))
        elif isinstance(data, list):
            for i, item in enumerate(data):
                items.extend(self._flatten(item, f"{parent_key}[{i}]", sep).items())
        else:
            items.append((parent_key, data))
        
        return dict(items)
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate parser parameters."""
        if "response" not in params:
            return False, "Missing required parameter: response"
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return ["response"]
