"""Data transform action module for RabAI AutoClick.

Provides comprehensive data transformation capabilities including
mapping, reshaping, conversion, and enrichment operations.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class FieldMapping:
    """Field mapping definition."""
    source: str
    target: str
    transform: Optional[str] = None
    default: Any = None


class DataTransformAction(BaseAction):
    """Data transform action for reshaping and converting data.
    
    Supports field mapping, nested data extraction, type conversion,
    data enrichment, and custom transformation functions.
    """
    action_type = "data_transform"
    display_name = "数据转换"
    description = "数据映射、转换与转换"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data transformation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                operation: map|reshape|convert|enrich|pivot|unpivot
                data: Input data to transform
                mappings: List of field mappings (for map)
                schema: Target schema (for reshape)
                conversions: Type conversions (for convert)
                enrichments: Data to merge (for enrich).
        
        Returns:
            ActionResult with transformed data.
        """
        operation = params.get('operation', 'map')
        data = params.get('data')
        
        if data is None:
            return ActionResult(success=False, message="No data provided")
        
        if operation == 'map':
            return self._map_fields(data, params)
        elif operation == 'reshape':
            return self._reshape(data, params)
        elif operation == 'convert':
            return self._convert(data, params)
        elif operation == 'enrich':
            return self._enrich(data, params)
        elif operation == 'pivot':
            return self._pivot(data, params)
        elif operation == 'unpivot':
            return self._unpivot(data, params)
        elif operation == 'merge':
            return self._merge(data, params)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")
    
    def _map_fields(self, data: Any, params: Dict[str, Any]) -> ActionResult:
        """Map fields from source to target."""
        mappings = params.get('mappings', [])
        drop_nulls = params.get('drop_nulls', False)
        
        if isinstance(data, list):
            output = []
            for item in data:
                if isinstance(item, dict):
                    mapped = self._apply_mappings(item, mappings, drop_nulls)
                    output.append(mapped)
                else:
                    output.append(item)
            
            return ActionResult(
                success=True,
                message=f"Mapped {len(output)} items",
                data={'items': output, 'count': len(output)}
            )
        
        if isinstance(data, dict):
            mapped = self._apply_mappings(data, mappings, drop_nulls)
            return ActionResult(
                success=True,
                message="Mapped fields",
                data={'item': mapped}
            )
        
        return ActionResult(success=False, message="Data must be dict or list of dicts")
    
    def _apply_mappings(
        self,
        item: Dict,
        mappings: List[Union[Dict, str]],
        drop_nulls: bool
    ) -> Dict:
        """Apply field mappings to a single item."""
        result = {}
        
        for mapping in mappings:
            if isinstance(mapping, str):
                source = target = mapping
                transform = None
                default = None
            else:
                source = mapping['source']
                target = mapping.get('target', source)
                transform = mapping.get('transform')
                default = mapping.get('default')
            
            value = self._get_nested(item, source, default)
            
            if drop_nulls and value is None:
                continue
            
            if transform:
                value = self._apply_transform(value, transform)
            
            result[target] = value
        
        return result
    
    def _get_nested(self, data: Dict, path: str, default: Any = None) -> Any:
        """Get nested value using dot notation."""
        parts = path.split('.')
        current = data
        
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
                if current is None:
                    return default
            else:
                return default
        
        return current if current is not None else default
    
    def _apply_transform(self, value: Any, transform: str) -> Any:
        """Apply transformation function to value."""
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
        elif transform == 'json':
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return value
            return value
        elif transform == 'len':
            return len(value) if value is not None else 0
        elif transform.startswith('default:'):
            default_val = transform.split(':', 1)[1]
            return value if value is not None else default_val
        
        return value
    
    def _reshape(self, data: Any, params: Dict[str, Any]) -> ActionResult:
        """Reshape data according to target schema."""
        schema = params.get('schema', {})
        output = []
        
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    reshaped = self._apply_schema(item, schema)
                    output.append(reshaped)
        elif isinstance(data, dict):
            output = self._apply_schema(data, schema)
        else:
            return ActionResult(success=False, message="Data must be dict or list of dicts")
        
        return ActionResult(
            success=True,
            message=f"Reshaped to {len(output) if isinstance(output, list) else 1} items",
            data={'items': output if isinstance(output, list) else [output]}
        )
    
    def _apply_schema(self, item: Dict, schema: Dict[str, Any]) -> Dict:
        """Apply schema to item."""
        result = {}
        
        for target_field, definition in schema.items():
            if isinstance(definition, str):
                result[target_field] = self._get_nested(item, definition)
            elif isinstance(definition, dict):
                source = definition.get('source')
                transform = definition.get('transform')
                default = definition.get('default')
                
                value = self._get_nested(item, source, default) if source else item.get(target_field)
                
                if transform:
                    value = self._apply_transform(value, transform)
                
                result[target_field] = value
            else:
                result[target_field] = item.get(target_field)
        
        return result
    
    def _convert(self, data: Any, params: Dict[str, Any]) -> ActionResult:
        """Convert data types."""
        conversions = params.get('conversions', {})
        output = []
        
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    converted = {}
                    for field, target_type in conversions.items():
                        value = item.get(field)
                        converted[field] = self._convert_value(value, target_type)
                    for k, v in item.items():
                        if k not in conversions:
                            converted[k] = v
                    output.append(converted)
                else:
                    output.append(item)
        else:
            return ActionResult(success=False, message="Convert requires list input")
        
        return ActionResult(
            success=True,
            message=f"Converted {len(output)} items",
            data={'items': output}
        )
    
    def _convert_value(self, value: Any, target_type: str) -> Any:
        """Convert value to target type."""
        if value is None:
            return None
        
        type_map = {
            'int': int,
            'float': float,
            'str': str,
            'bool': bool,
            'json': lambda v: json.dumps(v) if isinstance(v, (dict, list)) else v
        }
        
        converter = type_map.get(target_type)
        if converter:
            try:
                return converter(value)
            except (ValueError, TypeError):
                return value
        
        return value
    
    def _enrich(self, data: Any, params: Dict[str, Any]) -> ActionResult:
        """Enrich data with additional fields."""
        enrichments = params.get('enrichments', {})
        join_key = params.get('join_key')
        output = []
        
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    enriched = dict(item)
                    
                    if join_key:
                        join_value = item.get(join_key)
                        for enrichment_key, enrichment_data in enrichments.items():
                            if isinstance(enrichment_data, dict):
                                enriched[enrichment_key] = enrichment_data.get(join_value)
                            elif isinstance(enrichment_data, list):
                                for entry in enrichment_data:
                                    if isinstance(entry, dict) and entry.get(join_key) == join_value:
                                        enriched.update({k: v for k, v in entry.items() if k != join_key})
                                        break
                    else:
                        enriched.update(enrichments)
                    
                    output.append(enriched)
                else:
                    output.append(item)
        elif isinstance(data, dict) and not join_key:
            enriched = dict(data)
            enriched.update(enrichments)
            output = enriched
        else:
            output = data
        
        return ActionResult(
            success=True,
            message=f"Enriched data",
            data={'items': output if isinstance(output, list) else [output]}
        )
    
    def _pivot(self, data: Any, params: Dict[str, Any]) -> ActionResult:
        """Pivot data from rows to columns."""
        index = params.get('index')
        columns = params.get('columns')
        values = params.get('values')
        aggfunc = params.get('aggfunc', 'sum')
        
        if not isinstance(data, list) or not index or not columns:
            return ActionResult(success=False, message="Pivot requires list data, index and columns")
        
        pivot: Dict[Any, Dict[Any, List]] = {}
        
        for item in data:
            if isinstance(item, dict):
                row_key = item.get(index)
                col_key = item.get(columns)
                val = item.get(values) if values else 1
                
                if row_key not in pivot:
                    pivot[row_key] = {}
                if col_key not in pivot[row_key]:
                    pivot[row_key][col_key] = []
                pivot[row_key][col_key].append(val)
        
        output = []
        all_cols = set()
        for row_data in pivot.values():
            all_cols.update(row_data.keys())
        
        for row_key, row_data in pivot.items():
            output_row = {index: row_key}
            for col in all_cols:
                vals = row_data.get(col, [])
                if not vals:
                    output_row[col] = None
                elif aggfunc == 'sum':
                    output_row[col] = sum(vals)
                elif aggfunc == 'avg':
                    output_row[col] = sum(vals) / len(vals)
                elif aggfunc == 'count':
                    output_row[col] = len(vals)
                elif aggfunc == 'min':
                    output_row[col] = min(vals)
                elif aggfunc == 'max':
                    output_row[col] = max(vals)
                elif aggfunc == 'first':
                    output_row[col] = vals[0]
                elif aggfunc == 'last':
                    output_row[col] = vals[-1]
            output.append(output_row)
        
        return ActionResult(
            success=True,
            message=f"Pivoted to {len(output)} rows",
            data={'items': output}
        )
    
    def _unpivot(self, data: Any, params: Dict[str, Any]) -> ActionResult:
        """Unpivot data from columns to rows."""
        id_vars = params.get('id_vars', [])
        value_vars = params.get('value_vars')
        
        if not isinstance(data, list):
            return ActionResult(success=False, message="Unpivot requires list data")
        
        output = []
        
        for item in data:
            if isinstance(item, dict):
                base = {k: item.get(k) for k in id_vars if k in item}
                
                if value_vars is None:
                    value_vars = [k for k in item.keys() if k not in id_vars]
                
                for var in value_vars:
                    row = dict(base)
                    row['variable'] = var
                    row['value'] = item.get(var)
                    output.append(row)
        
        return ActionResult(
            success=True,
            message=f"Unpivoted to {len(output)} rows",
            data={'items': output}
        )
    
    def _merge(self, data: Any, params: Dict[str, Any]) -> ActionResult:
        """Merge multiple data sources."""
        sources = params.get('sources', [])
        merge_type = params.get('merge_type', 'union')
        on = params.get('on')
        output = []
        
        all_items = list(data) if isinstance(data, list) else [data]
        
        for source in sources:
            source_data = source.get('data', [])
            if isinstance(source_data, list):
                all_items.extend(source_data)
        
        if on and isinstance(all_items[0], dict) if all_items else False:
            seen = set()
            output = []
            for item in all_items:
                if isinstance(item, dict):
                    key = item.get(on)
                    if key not in seen:
                        seen.add(key)
                        output.append(item)
        else:
            output = all_items
        
        return ActionResult(
            success=True,
            message=f"Merged {len(output)} items",
            data={'items': output, 'count': len(output)}
        )
