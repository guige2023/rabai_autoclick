"""Data normalizer action module for RabAI AutoClick.

Provides data normalization with schema mapping, type coercion,
and format standardization.
"""

import sys
import os
import re
from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataType(Enum):
    """Target data types for normalization."""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    LIST = "list"
    DICT = "dict"


@dataclass
class NormalizationRule:
    """A normalization rule."""
    source_field: str
    target_field: str
    target_type: DataType
    default: Any = None
    transform: Optional[Callable[[Any], Any]] = None
    required: bool = False
    pattern: Optional[str] = None


class DataNormalizerAction(BaseAction):
    """Normalize data to standard formats and types.
    
    Supports field mapping, type coercion, format standardization,
    and custom transformation functions.
    """
    action_type = "data_normalizer"
    display_name = "数据标准化"
    description = "数据类型转换和格式标准化"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data normalization.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data dict
                - rules: List of NormalizationRule dicts
                - schema: Target schema dict
                - strict: Fail on missing required fields
        
        Returns:
            ActionResult with normalized data.
        """
        data = params.get('data', {})
        rules = params.get('rules', [])
        schema = params.get('schema', {})
        strict = params.get('strict', False)
        
        if not isinstance(data, dict):
            return ActionResult(success=False, message="data must be a dict")
        
        # Convert rules
        norm_rules = []
        for rule in rules:
            if isinstance(rule, dict):
                norm_rules.append(NormalizationRule(
                    source_field=rule.get('source', rule.get('source_field', '')),
                    target_field=rule.get('target', rule.get('target_field', '')),
                    target_type=DataType(rule.get('type', 'string')),
                    default=rule.get('default'),
                    transform=rule.get('transform'),
                    required=rule.get('required', False),
                    pattern=rule.get('pattern')
                ))
        
        # Apply schema-based rules if no explicit rules
        if not norm_rules and schema:
            for field_name, field_spec in schema.items():
                if isinstance(field_spec, dict):
                    norm_rules.append(NormalizationRule(
                        source_field=field_name,
                        target_field=field_name,
                        target_type=DataType(field_spec.get('type', 'string')),
                        default=field_spec.get('default'),
                        required=field_spec.get('required', False)
                    ))
        
        # Normalize
        result = {}
        errors = []
        
        for rule in norm_rules:
            value = data.get(rule.source_field)
            
            # Handle missing values
            if value is None:
                if rule.required:
                    errors.append(f"Required field '{rule.source_field}' is missing")
                if rule.default is not None:
                    value = rule.default
                else:
                    continue
            
            # Pattern validation
            if rule.pattern and value is not None:
                if not re.match(rule.pattern, str(value)):
                    errors.append(
                        f"Field '{rule.source_field}' does not match pattern"
                    )
                    continue
            
            # Apply custom transform
            if rule.transform and callable(rule.transform):
                try:
                    value = rule.transform(value)
                except Exception as e:
                    errors.append(f"Transform failed for '{rule.source_field}': {e}")
                    continue
            
            # Type coercion
            try:
                value = self._coerce_type(value, rule.target_type)
            except (ValueError, TypeError) as e:
                errors.append(f"Type coercion failed for '{rule.source_field}': {e}")
                continue
            
            result[rule.target_field] = value
        
        # Copy unmapped fields if not strict
        if not strict:
            mapped_sources = {r.source_field for r in norm_rules}
            for key, value in data.items():
                if key not in mapped_sources:
                    result[key] = value
        
        return ActionResult(
            success=len(errors) == 0 or not strict,
            message=f"Normalized with {len(errors)} errors" if errors else "Normalized successfully",
            data={
                'result': result,
                'errors': errors if strict else [],
                'fields_normalized': len(result)
            }
        )
    
    def _coerce_type(self, value: Any, target_type: DataType) -> Any:
        """Coerce value to target type."""
        if value is None:
            return None
        
        if target_type == DataType.STRING:
            if isinstance(value, bool):
                return str(value).lower()
            return str(value)
        
        elif target_type == DataType.INTEGER:
            if isinstance(value, str):
                value = value.strip().replace(',', '')
            return int(float(value))
        
        elif target_type == DataType.FLOAT:
            if isinstance(value, str):
                value = value.strip().replace(',', '')
            return float(value)
        
        elif target_type == DataType.BOOLEAN:
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            return bool(value)
        
        elif target_type == DataType.DATETIME:
            if isinstance(value, datetime):
                return value.isoformat()
            if isinstance(value, str):
                # Try ISO format first
                try:
                    return datetime.fromisoformat(value.replace('Z', '+00:00')).isoformat()
                except ValueError:
                    pass
                # Try common formats
                for fmt in ('%Y-%m-%d', '%Y/%m/%d', '%d-%m-%Y', '%m/%d/%Y'):
                    try:
                        return datetime.strptime(value, fmt).isoformat()
                    except ValueError:
                        continue
            return str(value)
        
        elif target_type == DataType.LIST:
            if isinstance(value, (list, tuple)):
                return list(value)
            if isinstance(value, str):
                return [v.strip() for v in value.split(',')]
            return [value]
        
        elif target_type == DataType.DICT:
            if isinstance(value, dict):
                return value
            return {'value': value}
        
        return value


class FormatStandardizerAction(BaseAction):
    """Standardize data formats across different sources."""
    action_type = "format_standardizer"
    display_name = "格式标准化"
    description = "统一不同数据源的格式"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute format standardization.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data
                - source_format: Source format identifier
                - target_format: Target format identifier
        
        Returns:
            ActionResult with standardized data.
        """
        data = params.get('data')
        source_format = params.get('source_format', 'auto')
        target_format = params.get('target_format', 'standard')
        
        if data is None:
            return ActionResult(success=False, message="data is required")
        
        # Auto-detect source format
        if source_format == 'auto':
            source_format = self._detect_format(data)
        
        # Apply standardization
        standardized = self._standardize(data, source_format, target_format)
        
        return ActionResult(
            success=True,
            message=f"Standardized from {source_format} to {target_format}",
            data={'result': standardized, 'source_format': source_format}
        )
    
    def _detect_format(self, data: Any) -> str:
        """Auto-detect data format."""
        if isinstance(data, dict):
            if 'id' in data and 'name' in data:
                return 'api_response'
            return 'dict'
        elif isinstance(data, list):
            return 'list'
        elif isinstance(data, str):
            return 'string'
        return 'unknown'
    
    def _standardize(
        self,
        data: Any,
        source: str,
        target: str
    ) -> Dict:
        """Apply standardization rules."""
        # Placeholder for actual standardization logic
        return {
            'data': data,
            'source_format': source,
            'target_format': target
        }


class FieldMapperAction(BaseAction):
    """Map fields between different schemas."""
    action_type = "field_mapper"
    display_name = "字段映射"
    description = "不同schema之间的字段映射"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute field mapping.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data
                - mapping: Dict of target_field -> source_field
                - prefix: Optional prefix for target fields
        
        Returns:
            ActionResult with mapped data.
        """
        data = params.get('data', {})
        mapping = params.get('mapping', {})
        prefix = params.get('prefix', '')
        
        if not isinstance(data, dict):
            return ActionResult(success=False, message="data must be a dict")
        
        result = {}
        
        for target, source in mapping.items():
            if isinstance(source, str):
                # Direct field mapping
                if '.' in source:
                    # Nested path
                    result[f"{prefix}{target}"] = self._get_nested(data, source)
                else:
                    result[f"{prefix}{target}"] = data.get(source)
            elif callable(source):
                # Function mapping
                result[f"{prefix}{target}"] = source(data)
        
        return ActionResult(
            success=True,
            message=f"Mapped {len(mapping)} fields",
            data={'result': result, 'count': len(result)}
        )
    
    def _get_nested(self, data: Dict, path: str) -> Any:
        """Get nested value using dot notation."""
        parts = path.split('.')
        current = data
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        return current
