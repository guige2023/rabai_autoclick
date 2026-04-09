"""
API Response Transformation Action Module.

Provides response transformation capabilities including JSON reshaping,
field mapping, filtering, and format conversion for API responses.

Author: RabAI Team
"""

from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
import json
import base64
from datetime import datetime


class OutputFormat(Enum):
    """Supported output formats."""
    JSON = "json"
    XML = "xml"
    CSV = "csv"
    YAML = "yaml"
    TEXT = "text"


@dataclass
class FieldMapping:
    """Defines a field mapping operation."""
    source: str
    target: str
    transform: Optional[Callable[[Any], Any]] = None


@dataclass
class TransformationPipeline:
    """A pipeline of transformation steps."""
    steps: List[Callable[[Any], Any]] = field(default_factory=list)
    
    def add_step(self, fn: Callable[[Any], Any]) -> "TransformationPipeline":
        """Add a transformation step."""
        self.steps.append(fn)
        return self
    
    def execute(self, data: Any) -> Any:
        """Execute all transformation steps."""
        result = data
        for step in self.steps:
            result = step(result)
        return result


class ResponseReshaper:
    """
    Reshapes API responses according to specifications.
    
    Example:
        reshaper = ResponseReshaper()
        reshaper.add_mapping("user.name", "full_name")
        reshaper.add_mapping("user.email", "contact_email")
        result = reshaper.reshape({"user": {"name": "John", "email": "john@example.com"}})
    """
    
    def __init__(self):
        self.mappings: List[FieldMapping] = []
        self.exclude_fields: List[str] = []
        self.include_only: List[str] = []
        self.defaults: Dict[str, Any] = {}
    
    def add_mapping(
        self,
        source: str,
        target: str,
        transform: Optional[Callable] = None
    ) -> "ResponseReshaper":
        """Add a field mapping."""
        self.mappings.append(FieldMapping(source, target, transform))
        return self
    
    def exclude(self, *fields: str) -> "ResponseReshaper":
        """Exclude specific fields from output."""
        self.exclude_fields.extend(fields)
        return self
    
    def include_only(self, *fields: str) -> "ResponseReshaper":
        """Only include specified fields."""
        self.include_only.extend(fields)
        return self
    
    def set_defaults(self, **defaults) -> "ResponseReshaper":
        """Set default values for missing fields."""
        self.defaults.update(defaults)
        return self
    
    def reshape(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Reshape the response data."""
        if not isinstance(data, dict):
            return data
        
        result = {}
        
        # Apply field mappings
        for mapping in self.mappings:
            value = self._get_nested(data, mapping.source)
            if value is not None:
                transformed = value
                if mapping.transform:
                    transformed = mapping.transform(value)
                result[mapping.target] = transformed
        
        # Copy unmapped fields
        for key, value in data.items():
            if key not in [m.source for m in self.mappings]:
                if not self._should_exclude(key):
                    result[key] = value
        
        # Apply defaults
        for key, default in self.defaults.items():
            if key not in result:
                result[key] = default
        
        # Apply include_only filter
        if self.include_only:
            result = {k: v for k, v in result.items() if k in self.include_only}
        
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
    
    def _should_exclude(self, field: str) -> bool:
        """Check if field should be excluded."""
        if self.include_only:
            return field not in self.include_only
        return field in self.exclude_fields


class ResponseFilter:
    """Filters response data based on criteria."""
    
    def __init__(self):
        self.filters: List[Callable[[Dict], bool]] = []
    
    def add_filter(self, filter_fn: Callable[[Dict], bool]) -> "ResponseFilter":
        """Add a filter function."""
        self.filters.append(filter_fn)
        return self
    
    def filter_by_field(
        self,
        field: str,
        operator: str,
        value: Any
    ) -> "ResponseFilter":
        """Add a field-based filter."""
        def filter_fn(item: Dict) -> bool:
            field_value = item.get(field)
            if operator == "eq":
                return field_value == value
            elif operator == "ne":
                return field_value != value
            elif operator == "gt":
                return field_value > value
            elif operator == "ge":
                return field_value >= value
            elif operator == "lt":
                return field_value < value
            elif operator == "le":
                return field_value <= value
            elif operator == "in":
                return field_value in value
            elif operator == "contains":
                return value in str(field_value)
            return False
        self.filters.append(filter_fn)
        return self
    
    def filter_list(self, items: List[Dict]) -> List[Dict]:
        """Apply filters to a list of items."""
        result = items
        for filter_fn in self.filters:
            result = [item for item in result if filter_fn(item)]
        return result


class FormatConverter:
    """Converts response data between formats."""
    
    @staticmethod
    def to_json(data: Any, pretty: bool = False) -> str:
        """Convert to JSON string."""
        if pretty:
            return json.dumps(data, indent=2, ensure_ascii=False)
        return json.dumps(data, ensure_ascii=False)
    
    @staticmethod
    def to_csv(data: List[Dict]) -> str:
        """Convert list of dicts to CSV string."""
        if not data:
            return ""
        
        headers = list(data[0].keys())
        lines = [",".join(headers)]
        
        for row in data:
            values = [str(row.get(h, "")) for h in headers]
            # Escape values containing commas or quotes
            escaped = []
            for v in values:
                if "," in v or '"' in v:
                    escaped.append(f'"{v.replace('"', '""')}"')
                else:
                    escaped.append(v)
            lines.append(",".join(escaped))
        
        return "\n".join(lines)
    
    @staticmethod
    def from_json(json_str: str) -> Any:
        """Parse JSON string."""
        return json.loads(json_str)
    
    @staticmethod
    def flatten(data: Dict, parent_key: str = "", sep: str = ".") -> Dict:
        """Flatten nested dictionary."""
        items = []
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(FormatConverter.flatten(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        items.extend(
                            FormatConverter.flatten(item, f"{new_key}[{i}]", sep=sep).items()
                        )
                    else:
                        items.append((f"{new_key}[{i}]", item))
            else:
                items.append((new_key, v))
        return dict(items)


class BaseAction:
    """Base class for all actions."""
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Any:
        raise NotImplementedError


class APIResponseTransformationAction(BaseAction):
    """
    Transforms API responses according to specifications.
    
    Parameters:
        mappings: Field mappings (source -> target)
        exclude: Fields to exclude
        format: Output format (json/csv/text)
    
    Example:
        action = APIResponseTransformationAction()
        result = action.execute({}, {
            "mappings": {"user.name": "full_name"},
            "data": {"user": {"name": "John"}},
            "format": "json"
        })
    """
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute response transformation."""
        data = params.get("data", {})
        mappings = params.get("mappings", {})
        exclude = params.get("exclude", [])
        include_only = params.get("include_only", [])
        format_out = params.get("format", "json")
        defaults = params.get("defaults", {})
        
        # Setup reshaper
        reshaper = ResponseReshaper()
        for source, target in mappings.items():
            reshaper.add_mapping(source, target)
        
        if exclude:
            reshaper.exclude(*exclude)
        
        if include_only:
            reshaper.include_only(*include_only)
        
        if defaults:
            reshaper.set_defaults(**defaults)
        
        # Apply transformations
        result = reshaper.reshape(data) if isinstance(data, dict) else data
        
        # Convert format if needed
        if format_out == "json":
            output = FormatConverter.to_json(result, pretty=True)
        elif format_out == "csv" and isinstance(result, list):
            output = FormatConverter.to_csv(result)
        elif format_out == "flatten":
            output = FormatConverter.to_json(FormatConverter.flatten(result))
        else:
            output = str(result)
        
        return {
            "data": output,
            "format": format_out,
            "transformed_at": datetime.now().isoformat()
        }
