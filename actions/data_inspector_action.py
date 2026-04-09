"""Data Inspector Action Module.

Provides data inspection and quality assessment capabilities.
"""

import time
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Union, Tuple
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataInspectorAction(BaseAction):
    """Inspect data structure and content.
    
    Analyzes data to understand schema, types, and basic statistics.
    """
    action_type = "data_inspector"
    display_name = "数据检查"
    description = "检查数据结构、内容和类型"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data inspection.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, deep_scan, include_samples.
        
        Returns:
            ActionResult with inspection results.
        """
        data = params.get('data', [])
        deep_scan = params.get('deep_scan', False)
        include_samples = params.get('include_samples', True)
        
        if not data:
            return ActionResult(
                success=False,
                data=None,
                error="No data to inspect"
            )
        
        try:
            # Basic inspection
            inspection = {
                'record_count': len(data) if isinstance(data, list) else 1,
                'data_type': self._get_data_type(data),
                'schema': self._extract_schema(data),
            }
            
            if include_samples:
                inspection['samples'] = self._get_samples(data, 5)
            
            if deep_scan:
                inspection['statistics'] = self._compute_statistics(data)
                inspection['quality'] = self._assess_quality(data)
            
            return ActionResult(
                success=True,
                data=inspection,
                error=None
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                data=None,
                error=f"Inspection failed: {str(e)}"
            )
    
    def _get_data_type(self, data: Any) -> str:
        """Get the data type."""
        if isinstance(data, list):
            return 'array'
        elif isinstance(data, dict):
            return 'object'
        elif isinstance(data, str):
            return 'string'
        elif isinstance(data, int):
            return 'integer'
        elif isinstance(data, float):
            return 'float'
        elif isinstance(data, bool):
            return 'boolean'
        else:
            return 'unknown'
    
    def _extract_schema(self, data: Any) -> Dict:
        """Extract schema from data."""
        schema = {}
        
        if isinstance(data, list) and len(data) > 0:
            sample = data[0]
            if isinstance(sample, dict):
                for key, value in sample.items():
                    schema[key] = {
                        'type': self._get_data_type(value),
                        'nullable': False
                    }
                    # Check if nullable
                    for item in data[1:]:
                        if key not in item:
                            schema[key]['nullable'] = True
                            break
        elif isinstance(data, dict):
            for key, value in data.items():
                schema[key] = {
                    'type': self._get_data_type(value),
                    'nullable': False
                }
        
        return schema
    
    def _get_samples(self, data: Any, count: int) -> List:
        """Get sample records from data."""
        if isinstance(data, list):
            return data[:count]
        else:
            return [data]
    
    def _compute_statistics(self, data: Any) -> Dict:
        """Compute basic statistics."""
        stats = {
            'field_stats': {}
        }
        
        if not isinstance(data, list):
            return stats
        
        # Collect all field names
        all_fields = set()
        for item in data:
            if isinstance(item, dict):
                all_fields.update(item.keys())
        
        # Compute stats per field
        for field in all_fields:
            values = [item.get(field) for item in data if isinstance(item, dict) and field in item]
            numeric_values = [v for v in values if isinstance(v, (int, float))]
            
            field_stats = {
                'count': len(values),
                'null_count': sum(1 for v in values if v is None),
                'unique_count': len(set(str(v) for v in values))
            }
            
            if numeric_values:
                field_stats.update({
                    'min': min(numeric_values),
                    'max': max(numeric_values),
                    'mean': sum(numeric_values) / len(numeric_values)
                })
            
            stats['field_stats'][field] = field_stats
        
        return stats
    
    def _assess_quality(self, data: Any) -> Dict:
        """Assess data quality."""
        if not isinstance(data, list):
            return {'quality_score': 100, 'issues': []}
        
        issues = []
        total_cells = 0
        empty_cells = 0
        
        for item in data:
            if isinstance(item, dict):
                for key, value in item.items():
                    total_cells += 1
                    if value is None or value == '':
                        empty_cells += 1
        
        empty_ratio = empty_cells / total_cells if total_cells > 0 else 0
        quality_score = max(0, (1 - empty_ratio) * 100)
        
        if empty_ratio > 0.1:
            issues.append(f"High empty value ratio: {empty_ratio:.1%}")
        
        return {
            'quality_score': quality_score,
            'issues': issues,
            'empty_cells': empty_cells,
            'total_cells': total_cells
        }


class SchemaValidatorAction(BaseAction):
    """Validate data against a schema.
    
    Checks if data conforms to expected schema and types.
    """
    action_type = "schema_validator"
    display_name = "模式验证"
    description = "验证数据是否符合预期模式"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute schema validation.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, schema, strict.
        
        Returns:
            ActionResult with validation results.
        """
        data = params.get('data', [])
        schema = params.get('schema', {})
        strict = params.get('strict', False)
        
        if not schema:
            return ActionResult(
                success=False,
                data=None,
                error="No schema provided"
            )
        
        if not isinstance(data, list):
            data = [data]
        
        validation_results = []
        all_valid = True
        
        for i, record in enumerate(data):
            result = self._validate_record(record, schema, strict)
            if not result['valid']:
                all_valid = False
            validation_results.append(result)
        
        return ActionResult(
            success=all_valid,
            data={
                'total_records': len(data),
                'valid_records': sum(1 for r in validation_results if r['valid']),
                'invalid_records': sum(1 for r in validation_results if not r['valid']),
                'results': validation_results
            },
            error=None if all_valid else "Some records failed validation"
        )
    
    def _validate_record(self, record: Dict, schema: Dict, strict: bool) -> Dict:
        """Validate a single record against schema."""
        if not isinstance(record, dict):
            return {'valid': False, 'errors': ['Record is not an object']}
        
        errors = []
        
        # Check required fields
        for field, field_schema in schema.items():
            if field_schema.get('required', False):
                if field not in record:
                    errors.append(f"Missing required field: {field}")
        
        # Check field types
        for field, value in record.items():
            if field in schema:
                expected_type = schema[field].get('type', 'any')
                actual_type = self._get_type_name(value)
                
                if not self._types_compatible(actual_type, expected_type):
                    errors.append(f"Field '{field}': expected {expected_type}, got {actual_type}")
        
        # In strict mode, check for unexpected fields
        if strict:
            for field in record:
                if field not in schema:
                    errors.append(f"Unexpected field: {field}")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors
        }
    
    def _get_type_name(self, value: Any) -> str:
        """Get type name for a value."""
        if value is None:
            return 'null'
        elif isinstance(value, bool):
            return 'boolean'
        elif isinstance(value, int):
            return 'integer'
        elif isinstance(value, float):
            return 'float'
        elif isinstance(value, str):
            return 'string'
        elif isinstance(value, list):
            return 'array'
        elif isinstance(value, dict):
            return 'object'
        else:
            return 'unknown'
    
    def _types_compatible(self, actual: str, expected: str) -> bool:
        """Check if actual type is compatible with expected."""
        if expected == 'any' or expected == actual:
            return True
        
        # Numeric compatibility
        if expected in ('number', 'numeric') and actual in ('integer', 'float'):
            return True
        
        return False


class DataProfilerAction(BaseAction):
    """Profile data in depth.
    
    Generates comprehensive data profiling report.
    """
    action_type = "data_profiler"
    display_name = "数据深度分析"
    description = "生成全面的数据深度分析报告"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data profiling.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, profile_level.
        
        Returns:
            ActionResult with profiling report.
        """
        data = params.get('data', [])
        profile_level = params.get('profile_level', 'standard')
        
        if not data:
            return ActionResult(
                success=False,
                data=None,
                error="No data to profile"
            )
        
        if not isinstance(data, list):
            data = [data]
        
        profile = {
            'basic': self._basic_profile(data),
            'structure': self._structure_profile(data),
            'content': self._content_profile(data)
        }
        
        if profile_level == 'comprehensive':
            profile['advanced'] = self._advanced_profile(data)
        
        return ActionResult(
            success=True,
            data=profile,
            error=None
        )
    
    def _basic_profile(self, data: List) -> Dict:
        """Basic data profile."""
        return {
            'record_count': len(data),
            'is_empty': len(data) == 0,
            'has_duplicates': len(data) != len(set(str(d) for d in data))
        }
    
    def _structure_profile(self, data: List) -> Dict:
        """Structure profile."""
        field_counts = Counter()
        
        for record in data:
            if isinstance(record, dict):
                field_counts.update(record.keys())
        
        return {
            'field_count': len(field_counts),
            'fields': dict(field_counts.most_common()),
            'uniform_schema': all(isinstance(r, dict) and set(r.keys()) == set(data[0].keys()) for r in data if isinstance(r, dict))
        }
    
    def _content_profile(self, data: List) -> Dict:
        """Content profile."""
        all_values = []
        for record in data:
            if isinstance(record, dict):
                all_values.extend(record.values())
            else:
                all_values.append(record)
        
        value_types = Counter(self._get_type_name(v) for v in all_values)
        
        return {
            'total_values': len(all_values),
            'type_distribution': dict(value_types),
            'null_count': sum(1 for v in all_values if v is None),
            'empty_string_count': sum(1 for v in all_values if v == '')
        }
    
    def _advanced_profile(self, data: List) -> Dict:
        """Advanced profiling with pattern analysis."""
        patterns = {
            'email': 0,
            'phone': 0,
            'url': 0,
            'date': 0
        }
        
        import re
        for record in data:
            if isinstance(record, dict):
                for value in record.values():
                    if isinstance(value, str):
                        if re.match(r'^[\w.-]+@[\w.-]+\.\w+$', value):
                            patterns['email'] += 1
                        elif re.match(r'^\+?[\d\s\-\(\)]{10,}$', value):
                            patterns['phone'] += 1
                        elif value.startswith(('http://', 'https://')):
                            patterns['url'] += 1
        
        return {'patterns': patterns}
    
    def _get_type_name(self, value: Any) -> str:
        """Get type name for value."""
        if value is None:
            return 'null'
        elif isinstance(value, bool):
            return 'boolean'
        elif isinstance(value, int):
            return 'integer'
        elif isinstance(value, float):
            return 'float'
        elif isinstance(value, str):
            return 'string'
        elif isinstance(value, list):
            return 'array'
        elif isinstance(value, dict):
            return 'object'
        else:
            return 'unknown'


def register_actions():
    """Register all Data Inspector actions."""
    return [
        DataInspectorAction,
        SchemaValidatorAction,
        DataProfilerAction,
    ]
