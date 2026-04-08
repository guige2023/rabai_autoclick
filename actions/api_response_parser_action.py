"""API response parser action module for RabAI AutoClick.

Provides API response parsing with support for JSONPath,
field extraction, transformation, and validation.
"""

import sys
import os
import json
import re
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class FieldMapping:
    """Field mapping definition."""
    source: str
    target: str
    transform: Optional[str] = None
    default: Any = None


class ApiResponseParserAction(BaseAction):
    """API response parser action for extracting and transforming responses.
    
    Supports JSONPath extraction, field mapping, response
    validation, and data transformation.
    """
    action_type = "api_response_parser"
    display_name = "API响应解析器"
    description = "API响应解析与字段映射"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute parsing operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                data: Response data to parse
                extraction: Extraction rules
                mappings: Field mappings
                validation: Validation rules
                transform: Transformation rules.
        
        Returns:
            ActionResult with parsed data.
        """
        data = params.get('data')
        extraction = params.get('extraction')
        mappings = params.get('mappings', [])
        validation = params.get('validation')
        transform = params.get('transform')
        
        if data is None:
            return ActionResult(success=False, message="No data provided")
        
        parsed = data
        
        if isinstance(data, str):
            try:
                parsed = json.loads(data)
            except json.JSONDecodeError:
                return ActionResult(
                    success=False,
                    message="Failed to parse JSON",
                    data={'raw': data}
                )
        
        result = {'_original': parsed}
        
        if extraction:
            extracted = self._extract_fields(parsed, extraction)
            result.update(extracted)
        
        if mappings:
            result = self._apply_mappings(result, mappings)
        
        if validation:
            validation_result = self._validate(result, validation)
            result['_validation'] = validation_result
        
        if transform:
            result = self._apply_transforms(result, transform)
        
        return ActionResult(
            success=True,
            message="Response parsed",
            data=result
        )
    
    def _extract_fields(
        self,
        data: Any,
        extraction: Union[Dict, List, str]
    ) -> Dict[str, Any]:
        """Extract fields from data using JSONPath-like syntax."""
        result = {}
        
        if isinstance(extraction, str):
            value = self._jsonpath(data, extraction)
            return {'result': value}
        
        if isinstance(extraction, list):
            for field_path in extraction:
                if isinstance(field_path, str):
                    result[field_path] = self._jsonpath(data, field_path)
            return result
        
        if isinstance(extraction, dict):
            for target_name, source_path in extraction.items():
                if isinstance(source_path, str):
                    result[target_name] = self._jsonpath(data, source_path)
            return result
        
        return result
    
    def _jsonpath(self, data: Any, path: str) -> Any:
        """Extract value using JSONPath-like notation."""
        if path.startswith('$.'):
            path = path[2:]
        
        parts = path.split('.')
        current = data
        
        for part in parts:
            if part == '*':
                if isinstance(current, list):
                    return [self._jsonpath(item, '.'.join(parts[parts.index(part) + 1:])) for item in current]
                return current
            
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list):
                try:
                    idx = int(part)
                    current = current[idx]
                except (ValueError, IndexError):
                    return None
            else:
                return None
        
        return current
    
    def _apply_mappings(
        self,
        data: Dict,
        mappings: List[Dict]
    ) -> Dict:
        """Apply field mappings to data."""
        result = dict(data)
        
        for mapping in mappings:
            source = mapping.get('source')
            target = mapping.get('target', source)
            transform = mapping.get('transform')
            default = mapping.get('default')
            
            value = self._get_nested(data, source) if '.' in source else data.get(source)
            
            if value is None:
                value = default
            
            if transform and value is not None:
                value = self._apply_transform(value, transform)
            
            self._set_nested(result, target, value)
        
        return result
    
    def _apply_transform(self, value: Any, transform: str) -> Any:
        """Apply single transform to value."""
        if transform == 'upper':
            return str(value).upper() if value else value
        elif transform == 'lower':
            return str(value).lower() if value else value
        elif transform == 'strip':
            return str(value).strip() if value else value
        elif transform == 'int':
            try:
                return int(value)
            except (ValueError, TypeError):
                return value
        elif transform == 'float':
            try:
                return float(value)
            except (ValueError, TypeError):
                return value
        elif transform == 'str':
            return str(value) if value is not None else value
        elif transform == 'bool':
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            return bool(value)
        elif transform == 'len':
            return len(value) if value is not None else 0
        elif transform == 'json':
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return value
            return value
        
        return value
    
    def _apply_transforms(
        self,
        data: Dict,
        transforms: List[Dict]
    ) -> Dict:
        """Apply multiple transforms to data."""
        result = dict(data)
        
        for transform_def in transforms:
            field = transform_def.get('field')
            operation = transform_def.get('operation')
            
            if not field or not operation:
                continue
            
            value = result.get(field)
            result[field] = self._apply_transform(value, operation)
        
        return result
    
    def _validate(
        self,
        data: Dict,
        validation: Dict
    ) -> Dict:
        """Validate parsed data."""
        errors = []
        warnings = []
        
        for rule in validation.get('required', []):
            field = rule.get('field')
            if field not in data or data[field] is None:
                errors.append(f"Required field '{field}' is missing")
        
        for rule in validation.get('types', []):
            field = rule.get('field')
            expected = rule.get('expected')
            value = data.get(field)
            
            if value is not None:
                actual_type = type(value).__name__
                if actual_type != expected:
                    errors.append(f"Field '{field}' should be {expected}, got {actual_type}")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings
        }
    
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
    
    def _set_nested(self, data: Dict, path: str, value: Any) -> None:
        """Set nested value using dot notation."""
        parts = path.split('.')
        current = data
        
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        
        current[parts[-1]] = value
