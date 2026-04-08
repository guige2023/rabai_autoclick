"""API Transformation Action Module.

Provides request/response transformation, mapping,
and data conversion for API integrations.
"""

from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
import json
import re
from datetime import datetime


class TransformDirection(Enum):
    """Transformation direction."""
    REQUEST = "request"
    RESPONSE = "response"


@dataclass
class FieldMapping:
    """Maps a source field to a target field."""
    source_path: str
    target_path: str
    transform_fn: Optional[Callable[[Any], Any]] = None
    default_value: Any = None
    required: bool = False


@dataclass
class TransformRule:
    """Defines a transformation rule."""
    name: str
    direction: TransformDirection
    mappings: List[FieldMapping]
    conditions: Optional[Callable[[Dict], bool]] = None
    post_processors: List[Callable[[Any], Any]] = field(default_factory=list)


class DataMapper:
    """Maps data between different structures."""

    def __init__(self):
        self._mappings: List[FieldMapping] = []

    def add_mapping(self, mapping: FieldMapping):
        """Add a field mapping."""
        self._mappings.append(mapping)

    def add_mappings(self, mappings: List[FieldMapping]):
        """Add multiple field mappings."""
        self._mappings.extend(mappings)

    def map_data(
        self,
        source: Dict[str, Any],
        skip_missing: bool = True,
    ) -> Dict[str, Any]:
        """Map source data to target structure."""
        result = {}

        for mapping in self._mappings:
            value = self._get_nested_value(source, mapping.source_path)

            if value is None and mapping.default_value is not None:
                value = mapping.default_value

            if value is None and mapping.required:
                raise ValueError(f"Required field {mapping.source_path} is missing")

            if value is not None or not skip_missing:
                if mapping.transform_fn:
                    try:
                        value = mapping.transform_fn(value)
                    except Exception:
                        pass

                self._set_nested_value(result, mapping.target_path, value)

        return result

    def _get_nested_value(
        self,
        data: Dict[str, Any],
        path: str,
    ) -> Any:
        """Get value using dot notation path."""
        parts = path.split(".")
        value = data

        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            elif isinstance(value, list):
                try:
                    index = int(part)
                    value = value[index] if 0 <= index < len(value) else None
                except ValueError:
                    return None
            else:
                return None

            if value is None:
                return None

        return value

    def _set_nested_value(
        self,
        data: Dict[str, Any],
        path: str,
        value: Any,
    ):
        """Set value using dot notation path."""
        parts = path.split(".")
        current = data

        for i, part in enumerate(parts[:-1]):
            if part not in current:
                current[part] = {}
            current = current[part]

        current[parts[-1]] = value


class JSONPathExtractor:
    """Extracts data using JSONPath-like expressions."""

    JSONPATH_PATTERN = re.compile(r'\$\.([^[]+)(?:\[(\d+)\])?(?:\.|$)')

    def extract(self, data: Any, path: str) -> List[Any]:
        """Extract values using path pattern."""
        results = []
        parts = path.split(".")

        def traverse(obj: Any, part_idx: int):
            if part_idx >= len(parts):
                results.append(obj)
                return

            part = parts[part_idx]

            if part == "*":
                if isinstance(obj, list):
                    for item in obj:
                        traverse(item, part_idx + 1)
                elif isinstance(obj, dict):
                    for value in obj.values():
                        traverse(value, part_idx + 1)

            elif part.startswith("[") and part.endswith("]"):
                try:
                    index = int(part[1:-1])
                    if isinstance(obj, list) and 0 <= index < len(obj):
                        traverse(obj[index], part_idx + 1)
                except ValueError:
                    pass

            elif isinstance(obj, dict):
                if part in obj:
                    traverse(obj[part], part_idx + 1)

            elif isinstance(obj, list):
                try:
                    index = int(part)
                    if 0 <= index < len(obj):
                        traverse(obj[index], part_idx + 1)
                except ValueError:
                    pass

        traverse(data, 0)
        return results

    def set_value(self, data: Dict, path: str, value: Any):
        """Set value at path."""
        parts = path.split(".")
        current = data

        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]

        current[parts[-1]] = value


class TemplateRenderer:
    """Renders string templates with data."""

    def __init__(self):
        self._pattern = re.compile(r'\{\{([^}]+)\}\}')
        self._extractor = JSONPathExtractor()

    def render(
        self,
        template: str,
        data: Dict[str, Any],
        default_value: str = "",
    ) -> str:
        """Render a template string."""

        def replace(match):
            path = match.group(1).strip()
            values = self._extractor.extract(data, path)
            if values:
                return str(values[0])
            return default_value

        return self._pattern.sub(replace, template)

    def render_dict(
        self,
        template_dict: Dict[str, str],
        data: Dict[str, Any],
    ) -> Dict[str, str]:
        """Render all template strings in a dictionary."""
        return {
            key: self.render(value, data)
            for key, value in template_dict.items()
        }


class ValueTransformer:
    """Transforms individual values."""

    def __init__(self):
        self._transformers: Dict[str, Callable] = {}

    def register(self, name: str, transformer: Callable):
        """Register a named transformer."""
        self._transformers[name] = transformer

    def transform(
        self,
        value: Any,
        transform_type: str,
        **options,
    ) -> Any:
        """Transform a value."""
        if transform_type in self._transformers:
            return self._transformers[transform_type](value, **options)

        if transform_type == "uppercase":
            return str(value).upper()
        elif transform_type == "lowercase":
            return str(value).lower()
        elif transform_type == "titlecase":
            return str(value).title()
        elif transform_type == "strip":
            return str(value).strip()
        elif transform_type == "int":
            return int(value)
        elif transform_type == "float":
            return float(value)
        elif transform_type == "string":
            return str(value)
        elif transform_type == "bool":
            return bool(value)
        elif transform_type == "json":
            if isinstance(value, str):
                return json.loads(value)
            return value
        elif transform_type == "base64_encode":
            import base64
            return base64.b64encode(str(value).encode()).decode()
        elif transform_type == "base64_decode":
            import base64
            return base64.b64decode(str(value).encode()).decode()
        elif transform_type == "url_encode":
            import urllib.parse
            return urllib.parse.quote(str(value))
        elif transform_type == "url_decode":
            import urllib.parse
            return urllib.parse.unquote(str(value))
        elif transform_type == "default":
            return value if value is not None else options.get("default_value", "")

        return value


class RequestTransformer:
    """Transforms API requests."""

    def __init__(
        self,
        mapper: Optional[DataMapper] = None,
        template_renderer: Optional[TemplateRenderer] = None,
        transformer: Optional[ValueTransformer] = None,
    ):
        self.mapper = mapper or DataMapper()
        self.template_renderer = template_renderer or TemplateRenderer()
        self.value_transformer = transformer or ValueTransformer()

    def transform(
        self,
        request: Dict[str, Any],
        rules: List[TransformRule],
    ) -> Dict[str, Any]:
        """Transform a request according to rules."""
        result = request.copy()

        for rule in rules:
            if rule.direction != TransformDirection.REQUEST:
                continue

            if rule.conditions and not rule.conditions(result):
                continue

            for mapping in rule.mappings:
                self.mapper.add_mapping(mapping)

            mapped = self.mapper.map_data(result)

            for key, value in mapped.items():
                if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                    result[key].update(value)
                else:
                    result[key] = value

            for post_processor in rule.post_processors:
                result = post_processor(result)

        return result


class ResponseTransformer:
    """Transforms API responses."""

    def __init__(
        self,
        mapper: Optional[DataMapper] = None,
        template_renderer: Optional[TemplateRenderer] = None,
        transformer: Optional[ValueTransformer] = None,
    ):
        self.mapper = mapper or DataMapper()
        self.template_renderer = template_renderer or TemplateRenderer()
        self.value_transformer = transformer or ValueTransformer()

    def transform(
        self,
        response: Dict[str, Any],
        rules: List[TransformRule],
    ) -> Dict[str, Any]:
        """Transform a response according to rules."""
        result = response.copy()

        for rule in rules:
            if rule.direction != TransformDirection.RESPONSE:
                continue

            if rule.conditions and not rule.conditions(result):
                continue

            mapped = self.mapper.map_data(result)

            for key, value in mapped.items():
                result[key] = value

            for post_processor in rule.post_processors:
                result = post_processor(result)

        return result


class DataConverter:
    """Converts data between different formats."""

    def to_json(self, data: Any) -> str:
        """Convert data to JSON string."""
        return json.dumps(data, default=str, ensure_ascii=False)

    def from_json(self, json_str: str) -> Any:
        """Parse JSON string to data."""
        return json.loads(json_str)

    def to_query_string(
        self,
        data: Dict[str, Any],
        encode: bool = True,
    ) -> str:
        """Convert dictionary to query string."""
        import urllib.parse
        parts = []
        for key, value in data.items():
            if isinstance(value, list):
                for v in value:
                    parts.append(f"{key}={v}")
            else:
                parts.append(f"{key}={value}")

        query = "&".join(parts)
        if encode:
            query = urllib.parse.quote(query)
        return query

    def from_query_string(
        self,
        query: str,
        decode: bool = True,
    ) -> Dict[str, Any]:
        """Parse query string to dictionary."""
        import urllib.parse
        if decode:
            query = urllib.parse.unquote(query)
        result = {}
        pairs = query.split("&")
        for pair in pairs:
            if "=" in pair:
                key, value = pair.split("=", 1)
                result[key] = value
        return result


class APITransformationAction:
    """High-level API transformation action."""

    def __init__(
        self,
        request_transformer: Optional[RequestTransformer] = None,
        response_transformer: Optional[ResponseTransformer] = None,
        converter: Optional[DataConverter] = None,
    ):
        self.request_transformer = request_transformer or RequestTransformer()
        self.response_transformer = response_transformer or ResponseTransformer()
        self.converter = converter or DataConverter()

    def create_mapping(
        self,
        source_path: str,
        target_path: str,
        transform_fn: Optional[Callable] = None,
    ) -> FieldMapping:
        """Create a field mapping."""
        return FieldMapping(
            source_path=source_path,
            target_path=target_path,
            transform_fn=transform_fn,
        )

    def create_rule(
        self,
        name: str,
        direction: str,
        mappings: List[FieldMapping],
    ) -> TransformRule:
        """Create a transformation rule."""
        return TransformRule(
            name=name,
            direction=TransformDirection(direction),
            mappings=mappings,
        )

    def transform_request(
        self,
        request: Dict[str, Any],
        rules: List[TransformRule],
    ) -> Dict[str, Any]:
        """Transform a request."""
        return self.request_transformer.transform(request, rules)

    def transform_response(
        self,
        response: Dict[str, Any],
        rules: List[TransformRule],
    ) -> Dict[str, Any]:
        """Transform a response."""
        return self.response_transformer.transform(response, rules)

    def render_template(
        self,
        template: str,
        data: Dict[str, Any],
    ) -> str:
        """Render a template string."""
        return self.template_renderer.render(template, data)

    def convert_format(
        self,
        data: Any,
        from_format: str,
        to_format: str,
    ) -> Any:
        """Convert between formats."""
        if from_format == "json" and to_format == "query":
            return self.converter.to_query_string(data)
        elif from_format == "query" and to_format == "json":
            return self.converter.from_query_string(data)
        elif from_format == "dict" and to_format == "json":
            return self.converter.to_json(data)
        elif from_format == "json" and to_format == "dict":
            return self.converter.from_json(data)
        return data


# Module exports
__all__ = [
    "APITransformationAction",
    "RequestTransformer",
    "ResponseTransformer",
    "DataMapper",
    "DataConverter",
    "ValueTransformer",
    "TemplateRenderer",
    "JSONPathExtractor",
    "FieldMapping",
    "TransformRule",
    "TransformDirection",
]
