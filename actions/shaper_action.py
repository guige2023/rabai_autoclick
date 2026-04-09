"""
Shaper Action Module.

Provides data shaping and transformation capabilities for
converting between different data formats and structures.
"""

import json
import csv
import io
from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field, asdict
from enum import Enum
import xml.etree.ElementTree as ET


class ShaperMode(Enum):
    """Data shaping modes."""
    EXPAND = "expand"
    COLLAPSE = "collapse"
    RESHAPE = "reshape"
    NORMALIZE = "normalize"
    DENORMALIZE = "denormalize"


@dataclass
class FieldMapping:
    """Mapping definition for field transformation."""
    source: str
    target: str
    transform: Optional[Callable] = None
    default: Any = None


@dataclass
class ShapeConfig:
    """Configuration for data shaping."""
    mode: ShaperMode = ShaperMode.RESHAPE
    field_mappings: List[FieldMapping] = field(default_factory=list)
    ignore_missing: bool = True
    deep_copy: bool = True


class DataShaper:
    """Core data shaping logic."""

    @staticmethod
    def expand(data: Dict, separator: str = ".") -> Dict:
        """Flatten nested dict into dot-notation keys."""
        result = {}

        def _expand_recursive(obj: Any, prefix: str = "") -> None:
            if isinstance(obj, dict):
                for key, value in obj.items():
                    new_key = f"{prefix}{separator}{key}" if prefix else key
                    _expand_recursive(value, new_key)
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    new_key = f"{prefix}[{i}]"
                    _expand_recursive(item, new_key)
            else:
                result[prefix] = obj

        _expand_recursive(data)
        return result

    @staticmethod
    def collapse(data: Dict, separator: str = ".") -> Dict:
        """Convert dot-notation keys back to nested dict."""
        result = {}

        for key, value in data.items():
            parts = key.replace("][", "]").replace("[", ".[").split(".")
            current = result

            for i, part in enumerate(parts):
                if part.endswith("]"):
                    part = part[:-1]
                    is_array = True
                else:
                    is_array = False

                if i == len(parts) - 1:
                    current[part] = value
                else:
                    if part not in current:
                        next_part = parts[i + 1]
                        if next_part.startswith("["):
                            current[part] = []
                        else:
                            current[part] = {}
                    current = current[part]

        return result

    @staticmethod
    def normalize(data: List[Dict]) -> List[Dict]:
        """Normalize list of dicts to have consistent keys."""
        all_keys = set()
        for item in data:
            all_keys.update(item.keys())

        normalized = []
        for item in data:
            normalized_item = {}
            for key in all_keys:
                normalized_item[key] = item.get(key)
            normalized.append(normalized_item)

        return normalized

    @staticmethod
    def denormalize(data: List[Dict], key_field: str) -> Dict:
        """Convert normalized dicts back to nested structure."""
        result = {}
        for item in data:
            keys = key_field.split(".")
            current = result

            for i, k in enumerate(keys):
                if i == len(keys) - 1:
                    current[k] = item
                else:
                    if k not in current:
                        current[k] = {}
                    current = current[k]

        return result


class ShaperAction:
    """
    Action for shaping and transforming data.

    Example:
        shaper = ShaperAction("data_transformer")
        shaper.add_mapping("name", "full_name", str.upper)
        result = shaper.transform({"name": "john"})
    """

    def __init__(
        self,
        name: str,
        config: Optional[ShapeConfig] = None,
    ):
        self.name = name
        self.config = config or ShapeConfig()
        self._mappings: List[FieldMapping] = []

    def add_mapping(
        self,
        source: str,
        target: str,
        transform: Optional[Callable] = None,
        default: Any = None,
    ) -> "ShaperAction":
        """Add a field mapping."""
        self._mappings.append(
            FieldMapping(
                source=source,
                target=target,
                transform=transform,
                default=default,
            )
        )
        return self

    def apply_mappings(self, data: Dict) -> Dict:
        """Apply all field mappings to data."""
        result = {}

        for mapping in self._mappings:
            value = data.get(mapping.source, mapping.default)

            if mapping.transform and value is not None:
                try:
                    value = mapping.transform(value)
                except Exception:
                    value = mapping.default if mapping.default else None

            result[mapping.target] = value

        return result

    def transform(self, data: Any) -> Any:
        """Transform data according to config and mappings."""
        if isinstance(data, dict):
            result = dict(data) if self.config.deep_copy else data

            result = self.apply_mappings(result)

            if self.config.mode == ShaperMode.EXPAND:
                return DataShaper.expand(result)
            elif self.config.mode == ShaperMode.COLLAPSE:
                return DataShaper.collapse(result)
            elif self.config.mode == ShaperMode.NORMALIZE:
                return [result]
            elif self.config.mode == ShaperMode.DENORMALIZE:
                return DataShaper.denormalize([result], "id")

            return result

        elif isinstance(data, list):
            if self.config.mode == ShaperMode.NORMALIZE:
                return DataShaper.normalize(data)

            return [self.transform(item) for item in data]

        return data

    def transform_dict_to_object(
        self,
        data: Dict,
        class_type: type,
    ) -> Any:
        """Convert dict to dataclass or object."""
        try:
            if hasattr(class_type, "__dataclass_fields__"):
                field_names = set(class_type.__dataclass_fields__.keys())
                filtered = {k: v for k, v in data.items() if k in field_names}
                return class_type(**filtered)
            return class_type(data)
        except Exception as e:
            raise ValueError(f"Failed to transform to {class_type}: {e}")

    def transform_to_csv(
        self,
        data: List[Dict],
        headers: Optional[List[str]] = None,
    ) -> str:
        """Transform list of dicts to CSV string."""
        if not data:
            return ""

        headers = headers or list(data[0].keys())
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)
        return output.getvalue()

    def transform_from_csv(self, csv_string: str) -> List[Dict]:
        """Parse CSV string to list of dicts."""
        input_io = io.StringIO(csv_string)
        reader = csv.DictReader(input_io)
        return list(reader)

    def transform_to_xml(
        self,
        data: Dict,
        root_tag: str = "root",
    ) -> str:
        """Transform dict to XML string."""
        root = ET.Element(root_tag)

        def _build_xml(parent: ET.Element, data: Any) -> None:
            if isinstance(data, dict):
                for key, value in data.items():
                    child = ET.SubElement(parent, key)
                    _build_xml(child, value)
            elif isinstance(data, list):
                for item in data:
                    child = ET.SubElement(parent, "item")
                    _build_xml(child, item)
            else:
                parent.text = str(data)

        _build_xml(root, data)
        return ET.tostring(root, encoding="unicode")

    def transform_from_xml(self, xml_string: str) -> Dict:
        """Parse XML string to dict."""
        root = ET.fromstring(xml_string)

        def _parse_xml(element: ET.Element) -> Any:
            if len(element) == 0:
                return element.text
            result = {}
            for child in element:
                value = _parse_xml(child)
                if child.tag in result:
                    if not isinstance(result[child.tag], list):
                        result[child.tag] = [result[child.tag]]
                    result[child.tag].append(value)
                else:
                    result[child.tag] = value
            return result

        return {root.tag: _parse_xml(root)}

    def transform_json(self, data: Any, indent: Optional[int] = None) -> str:
        """Serialize data to JSON string."""
        return json.dumps(data, indent=indent, default=str)

    def transform_from_json(self, json_string: str) -> Any:
        """Parse JSON string to data."""
        return json.loads(json_string)

    def reshape(
        self,
        data: Any,
        schema: Dict[str, Any],
    ) -> Dict:
        """Reshape data according to a schema definition."""
        result = {}

        for target_field, source_path in schema.items():
            if isinstance(source_path, str):
                value = self._get_nested(data, source_path)
                result[target_field] = value
            elif callable(source_path):
                try:
                    result[target_field] = source_path(data)
                except Exception:
                    result[target_field] = None
            else:
                result[target_field] = source_path

        return result

    def _get_nested(self, data: Dict, path: str) -> Any:
        """Get nested value using dot notation."""
        keys = path.split(".")
        current = data

        for key in keys:
            if isinstance(current, dict):
                current = current.get(key)
            else:
                return None

            if current is None:
                return None

        return current

    def set_nested(self, data: Dict, path: str, value: Any) -> None:
        """Set nested value using dot notation."""
        keys = path.split(".")
        current = data

        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]

        current[keys[-1]] = value
