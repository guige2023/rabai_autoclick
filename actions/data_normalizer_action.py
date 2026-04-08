"""Data normalizer action module for RabAI AutoClick.

Provides data normalization with schema validation,
type conversion, format standardization, and outlier handling.
"""

import re
import sys
import os
import json
from typing import Any, Dict, List, Optional, Union, Callable
from datetime import datetime
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataNormalizerAction(BaseAction):
    """Normalize data to standard formats and schemas.
    
    Supports type coercion, format standardization,
    schema validation, and outlier handling.
    """
    action_type = "data_normalizer"
    display_name = "数据规范化"
    description = "数据规范化，类型转换和格式标准化"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data normalization.
        
        Args:
            context: Execution context.
            params: Dict with keys: records, schema, normalize_types,
                   handle_nulls, remove_duplicates.
        
        Returns:
            ActionResult with normalized records.
        """
        records = params.get('records', [])
        if not records:
            return ActionResult(success=False, message="No records provided")
        
        if not isinstance(records, list):
            if isinstance(records, dict):
                records = [records]
            else:
                return ActionResult(success=False, message="records must be a list or dict")
        
        schema = params.get('schema', {})
        normalize_types = params.get('normalize_types', True)
        handle_nulls = params.get('handle_nulls', 'keep')
        remove_duplicates = params.get('remove_duplicates', False)
        
        normalized = []
        errors = []
        
        for idx, record in enumerate(records):
            try:
                norm_record = self._normalize_record(
                    record, schema, normalize_types, handle_nulls
                )
                normalized.append(norm_record)
            except Exception as e:
                errors.append({'index': idx, 'error': str(e)})
        
        if remove_duplicates:
            original_count = len(normalized)
            normalized = self._remove_duplicates(normalized)
            dup_removed = original_count - len(normalized)
        else:
            dup_removed = 0
        
        return ActionResult(
            success=len(errors) == 0,
            message=f"Normalized {len(normalized)} records, {len(errors)} errors",
            data={
                'total': len(records),
                'normalized': len(normalized),
                'duplicates_removed': dup_removed,
                'errors': errors[:100]
            }
        )
    
    def _normalize_record(
        self,
        record: Dict[str, Any],
        schema: Dict[str, Any],
        normalize_types: bool,
        handle_nulls: str
    ) -> Dict[str, Any]:
        """Normalize a single record."""
        result = {}
        
        if schema:
            for field, field_schema in schema.items():
                value = record.get(field)
                result[field] = self._normalize_field(
                    value, field_schema, handle_nulls
                )
        else:
            for key, value in record.items():
                result[key] = value
        
        if normalize_types:
            for key, value in result.items():
                result[key] = self._coerce_type(value)
        
        return result
    
    def _normalize_field(
        self,
        value: Any,
        field_schema: Dict[str, Any],
        handle_nulls: str
    ) -> Any:
        """Normalize a single field based on schema."""
        field_type = field_schema.get('type')
        default = field_schema.get('default')
        required = field_schema.get('required', False)
        pattern = field_schema.get('pattern')
        min_val = field_schema.get('min')
        max_val = field_schema.get('max')
        
        if value is None or value == '':
            if required and default is None:
                return default
            if handle_nulls == 'remove':
                return None
            elif handle_nulls == 'default':
                return default
            else:
                return value
        
        if field_type == 'string':
            value = str(value).strip()
            if pattern:
                if not re.match(pattern, value):
                    value = None
        elif field_type == 'integer':
            try:
                value = int(float(value))
            except (ValueError, TypeError):
                value = None
        elif field_type == 'float':
            try:
                value = float(value)
            except (ValueError, TypeError):
                value = None
        elif field_type == 'boolean':
            if isinstance(value, bool):
                pass
            elif isinstance(value, str):
                value = value.lower() in ('true', '1', 'yes', 'on')
            else:
                value = bool(value)
        elif field_type == 'email':
            value = str(value).strip().lower()
            if not re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', value):
                value = None
        elif field_type == 'phone':
            value = re.sub(r'[^\d+]', '', str(value))
        elif field_type == 'date':
            value = self._normalize_date(value)
        elif field_type == 'url':
            value = str(value).strip()
            if not value.startswith(('http://', 'https://')):
                value = None
        
        if min_val is not None and isinstance(value, (int, float)):
            if value < min_val:
                value = min_val
        
        if max_val is not None and isinstance(value, (int, float)):
            if value > max_val:
                value = max_val
        
        return value
    
    def _coerce_type(self, value: Any) -> Any:
        """Attempt to coerce value to appropriate type."""
        if value is None:
            return None
        
        if isinstance(value, (int, float, bool)):
            return value
        
        if isinstance(value, str):
            str_val = value.strip()
            
            if str_val.lower() == 'true':
                return True
            elif str_val.lower() == 'false':
                return False
            elif str_val.lower() == 'null' or str_val == '':
                return None
            
            try:
                if '.' in str_val:
                    return float(str_val)
                return int(str_val)
            except ValueError:
                return str_val
        
        return str(value)
    
    def _normalize_date(self, value: Any) -> Optional[str]:
        """Normalize date to ISO format."""
        if isinstance(value, datetime):
            return value.isoformat()
        
        if isinstance(value, str):
            formats = [
                '%Y-%m-%d',
                '%Y-%m-%d %H:%M:%S',
                '%Y/%m/%d',
                '%m/%d/%Y',
                '%d/%m/%Y',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%dT%H:%M:%S.%fZ'
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(value, fmt)
                    return dt.isoformat()
                except ValueError:
                    continue
            
            return value
        
        return None
    
    def _remove_duplicates(
        self,
        records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Remove duplicate records."""
        seen = set()
        unique = []
        
        for record in records:
            try:
                record_key = json.dumps(record, sort_keys=True, default=str)
                if record_key not in seen:
                    seen.add(record_key)
                    unique.append(record)
            except Exception:
                unique.append(record)
        
        return unique
