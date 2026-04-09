"""
Data Transformer Action Module

Data transformation pipeline with field mapping, type conversion,
and conditional transformations. Supports chained operations.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class TransformType(Enum):
    """Types of transformations."""
    
    MAP_FIELD = "map_field"
    RENAME_FIELD = "rename_field"
    DELETE_FIELD = "delete_field"
    TYPE_CONVERT = "type_convert"
    COMPUTE = "compute"
    CONDITIONAL = "conditional"
    FLATTEN = "flatten"
    UNFLATTEN = "unflatten"
    TEMPLATE = "template"
    CUSTOM = "custom"


@dataclass
class FieldMapping:
    """Mapping from source to destination field."""
    
    source: str
    destination: str
    transform: Optional[str] = None


@dataclass
class TransformRule:
    """A single transformation rule."""
    
    type: TransformType
    config: Dict[str, Any]
    condition: Optional[str] = None
    priority: int = 0


@dataclass
class TransformConfig:
    """Configuration for transformation behavior."""
    
    rules: List[TransformRule] = field(default_factory=list)
    fail_on_error: bool = False
    strict_mode: bool = False
    preserve_original: bool = False


@dataclass
class TransformResult:
    """Result of transformation."""
    
    success: bool
    data: Any = None
    transformed_fields: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class FieldMapper:
    """Maps and transforms fields."""
    
    def __init__(self, mappings: List[FieldMapping]):
        self.mappings = mappings
    
    def apply(self, data: Dict) -> Dict:
        """Apply field mappings to data."""
        result = {}
        
        for mapping in self.mappings:
            value = self._get_nested(data, mapping.source)
            if value is not None:
                self._set_nested(result, mapping.destination, value)
        
        return result
    
    def _get_nested(self, data: Dict, path: str) -> Any:
        """Get nested value using dot notation."""
        keys = path.split(".")
        value = data
        
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        
        return value
    
    def _set_nested(self, data: Dict, path: str, value: Any) -> None:
        """Set nested value using dot notation."""
        keys = path.split(".")
        current = data
        
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        
        current[keys[-1]] = value


class TypeConverter:
    """Converts between data types."""
    
    TYPE_CONVERTERS: Dict[str, Callable] = {}
    
    @classmethod
    def register(cls, type_name: str, converter: Callable) -> None:
        """Register a type converter."""
        cls.TYPE_CONVERTERS[type_name] = converter
    
    @classmethod
    def convert(cls, value: Any, to_type: str) -> Any:
        """Convert value to specified type."""
        if to_type == "int":
            return int(value)
        elif to_type == "float":
            return float(value)
        elif to_type == "str":
            return str(value)
        elif to_type == "bool":
            return bool(value)
        elif to_type == "list":
            return list(value) if not isinstance(value, list) else value
        elif to_type == "dict":
            return dict(value) if not isinstance(value, dict) else value
        
        converter = cls.TYPE_CONVERTERS.get(to_type)
        if converter:
            return converter(value)
        
        return value


TypeConverter.register("datetime", lambda v: datetime.fromisoformat(str(v)))
TypeConverter.register("json", lambda v: json.loads(v) if isinstance(v, str) else v)


class TemplateEngine:
    """Simple template engine for transformations."""
    
    def __init__(self, template: str):
        self.template = template
        self._pattern = re.compile(r'\$\{(\w+)\}|\$\{(\w+(?:\.\w+)*)\}')
    
    def render(self, context: Dict) -> str:
        """Render template with context."""
        def replacer(match):
            path = match.group(1) or match.group(2)
            value = self._get_nested(context, path)
            return str(value) if value is not None else ""
        
        return self._pattern.sub(replacer, self.template)
    
    def _get_nested(self, data: Dict, path: str) -> Any:
        """Get nested value using dot notation."""
        keys = path.split(".")
        value = data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value


class DataTransformerAction:
    """
    Main data transformer action handler.
    
    Provides a pipeline for transforming data with field mapping,
    type conversion, computed fields, and conditional logic.
    """
    
    def __init__(self, config: Optional[TransformConfig] = None):
        self.config = config or TransformConfig()
        self._custom_transforms: Dict[str, Callable] = {}
        self._stats = {
            "total_transformations": 0,
            "successful": 0,
            "failed": 0
        }
    
    def add_rule(self, rule: TransformRule) -> None:
        """Add a transformation rule."""
        self.config.rules.append(rule)
        self.config.rules.sort(key=lambda r: r.priority)
    
    def add_mapping(self, mapping: FieldMapping) -> None:
        """Add a field mapping."""
        rule = TransformRule(
            type=TransformType.MAP_FIELD,
            config={"mapping": mapping}
        )
        self.add_rule(rule)
    
    def register_transform(self, name: str, func: Callable) -> None:
        """Register a custom transform function."""
        self._custom_transforms[name] = func
    
    def transform(
        self,
        data: Any,
        rules: Optional[List[TransformRule]] = None
    ) -> TransformResult:
        """Transform data using configured or provided rules."""
        self._stats["total_transformations"] += 1
        
        if self.config.preserve_original:
            data = self._deep_copy(data)
        
        if rules is None:
            rules = self.config.rules
        
        transformed_fields = []
        errors = []
        warnings = []
        
        for rule in rules:
            try:
                if rule.condition and not self._evaluate_condition(rule.condition, data):
                    continue
                
                data, fields = self._apply_rule(rule, data)
                transformed_fields.extend(fields)
            
            except Exception as e:
                error_msg = f"Rule {rule.type.value} failed: {str(e)}"
                if self.config.fail_on_error:
                    errors.append(error_msg)
                    self._stats["failed"] += 1
                    return TransformResult(
                        success=False,
                        data=data,
                        errors=errors
                    )
                warnings.append(error_msg)
        
        self._stats["successful"] += 1
        
        return TransformResult(
            success=True,
            data=data,
            transformed_fields=transformed_fields,
            errors=errors,
            warnings=warnings
        )
    
    def _apply_rule(
        self,
        rule: TransformRule,
        data: Any
    ) -> tuple[Any, List[str]]:
        """Apply a single transformation rule."""
        fields = []
        
        if rule.type == TransformType.MAP_FIELD:
            mapping = FieldMapping(**rule.config.get("mapping", {}))
            mapper = FieldMapper([mapping])
            data = mapper.apply(data) if isinstance(data, dict) else data
            fields.append(f"{mapping.source} -> {mapping.destination}")
        
        elif rule.type == TransformType.RENAME_FIELD:
            source = rule.config.get("source")
            dest = rule.config.get("destination")
            if isinstance(data, dict) and source in data:
                data[dest] = data.pop(source)
                fields.append(f"{source} -> {dest}")
        
        elif rule.type == TransformType.DELETE_FIELD:
            field_path = rule.config.get("field")
            if isinstance(data, dict):
                data.pop(field_path, None)
                fields.append(f"deleted: {field_path}")
        
        elif rule.type == TransformType.TYPE_CONVERT:
            field_name = rule.config.get("field")
            to_type = rule.config.get("to_type")
            if isinstance(data, dict) and field_name in data:
                data[field_name] = TypeConverter.convert(data[field_name], to_type)
                fields.append(f"{field_name} ({to_type})")
        
        elif rule.type == TransformType.COMPUTE:
            field_name = rule.config.get("field")
            expression = rule.config.get("expression")
            if expression in self._custom_transforms:
                data[field_name] = self._custom_transforms[expression](data)
            fields.append(f"computed: {field_name}")
        
        elif rule.type == TransformType.TEMPLATE:
            field_name = rule.config.get("field")
            template_str = rule.config.get("template")
            engine = TemplateEngine(template_str)
            data[field_name] = engine.render(data) if isinstance(data, dict) else data
            fields.append(f"template: {field_name}")
        
        elif rule.type == TransformType.CUSTOM:
            transform_name = rule.config.get("name")
            if transform_name in self._custom_transforms:
                data = self._custom_transforms[transform_name](data)
                fields.append(f"custom: {transform_name}")
        
        elif rule.type == TransformType.FLATTEN:
            data = self._flatten(data)
            fields.append("flattened")
        
        elif rule.type == TransformType.UNFLATTEN:
            data = self._unflatten(data)
            fields.append("unflattened")
        
        return data, fields
    
    def _flatten(self, data: Dict, parent_key: str = "", sep: str = ".") -> Dict:
        """Flatten nested dictionary."""
        items = []
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten(v, new_key, sep).items())
            else:
                items.append((new_key, v))
        return dict(items)
    
    def _unflatten(self, data: Dict, sep: str = ".") -> Dict:
        """Unflatten dictionary to nested."""
        result = {}
        for key, value in data.items():
            parts = key.split(sep)
            current = result
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            current[parts[-1]] = value
        return result
    
    def _evaluate_condition(self, condition: str, data: Dict) -> bool:
        """Evaluate a simple condition against data."""
        if condition.startswith("exists:"):
            field = condition.split(":", 1)[1]
            return field in data and data[field] is not None
        
        if condition.startswith("eq:"):
            field, value = condition.split(":", 1)[1].split("=", 1)
            return data.get(field) == value
        
        return True
    
    def _deep_copy(self, data: Any) -> Any:
        """Deep copy data."""
        import copy
        return copy.deepcopy(data)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get transformation statistics."""
        return {
            **self._stats,
            "rules_count": len(self.config.rules),
            "custom_transforms": list(self._custom_transforms.keys())
        }


import json
