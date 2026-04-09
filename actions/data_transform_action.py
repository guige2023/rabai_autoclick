"""Data transformation action module for RabAI AutoClick.

Provides data transformation operations:
- RenameFieldsAction: Rename data fields
- TypeConversionAction: Convert data types
- NormalizeDataAction: Normalize data values
- EnrichDataAction: Enrich data with additional fields
- FlattenDataAction: Flatten nested data structures
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RenameFieldsAction(BaseAction):
    """Rename data fields."""
    action_type = "rename_fields"
    display_name = "重命名字段"
    description = "重命名数据字段"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            mapping = params.get("mapping", {})
            
            if isinstance(data, dict):
                result = self._rename_dict(data, mapping)
            elif isinstance(data, list):
                result = [self._rename_dict(item, mapping) if isinstance(item, dict) else item 
                         for item in data]
            else:
                return ActionResult(success=False, message="data must be dict or list")
            
            return ActionResult(
                success=True,
                message=f"Renamed {len(mapping)} fields",
                data={
                    "original_data": data,
                    "transformed_data": result,
                    "fields_renamed": len(mapping)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _rename_dict(self, d: Dict, mapping: Dict) -> Dict:
        result = {}
        for key, value in d.items():
            new_key = mapping.get(key, key)
            result[new_key] = value
        return result


class TypeConversionAction(BaseAction):
    """Convert data types."""
    action_type = "type_conversion"
    display_name = "类型转换"
    description = "转换数据类型"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data")
            conversions = params.get("conversions", {})
            
            result = {}
            errors = []
            
            for field, target_type in conversions.items():
                if isinstance(data, dict):
                    value = data.get(field)
                else:
                    value = data.get(field) if isinstance(data, dict) else None
                
                if value is None:
                    result[field] = None
                    continue
                
                try:
                    converted = self._convert_value(value, target_type)
                    result[field] = converted
                except Exception as e:
                    errors.append(f"{field}: {str(e)}")
                    result[field] = None
            
            if errors:
                return ActionResult(
                    success=False,
                    message=f"Conversion errors: {errors[0]}",
                    data={
                        "converted_data": result,
                        "errors": errors
                    }
                )
            
            return ActionResult(
                success=True,
                message="Type conversion complete",
                data={
                    "original_data": data,
                    "converted_data": result,
                    "conversions_applied": len(conversions)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _convert_value(self, value: Any, target_type: str) -> Any:
        target_type = target_type.lower()
        
        if target_type == "string" or target_type == "str":
            return str(value)
        elif target_type == "int" or target_type == "integer":
            if isinstance(value, str):
                return int(float(value))
            return int(value)
        elif target_type == "float" or target_type == "number":
            return float(value)
        elif target_type == "bool" or target_type == "boolean":
            if isinstance(value, str):
                return value.lower() in ("true", "1", "yes", "on")
            return bool(value)
        elif target_type == "list" or target_type == "array":
            if isinstance(value, str):
                return [v.strip() for v in value.split(",")]
            return list(value)
        elif target_type == "dict" or target_type == "object":
            if isinstance(value, str):
                import json
                return json.loads(value)
            return dict(value)
        else:
            raise ValueError(f"Unknown target type: {target_type}")


class NormalizeDataAction(BaseAction):
    """Normalize data values."""
    action_type = "normalize_data"
    display_name = "数据归一化"
    description = "归一化数据值"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            fields = params.get("fields", [])
            method = params.get("method", "minmax")
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not fields:
                return ActionResult(success=False, message="No fields specified")
            
            if method == "minmax":
                result = self._minmax_normalize(data, fields)
            elif method == "zscore":
                result = self._zscore_normalize(data, fields)
            elif method == "log":
                result = self._log_normalize(data, fields)
            else:
                return ActionResult(success=False, message=f"Unknown method: {method}")
            
            return ActionResult(
                success=True,
                message=f"Normalization complete using {method}",
                data={
                    "method": method,
                    "fields_normalized": fields,
                    "normalized_data": result[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _minmax_normalize(self, data: List[Dict], fields: List[str]) -> List[Dict]:
        result = []
        
        for field in fields:
            values = [item.get(field, 0) for item in data if isinstance(item, dict)]
            
            if not values:
                continue
            
            min_val = min(values)
            max_val = max(values)
            
            if min_val == max_val:
                continue
            
            for item in data:
                if isinstance(item, dict) and field in item:
                    item[field] = (item[field] - min_val) / (max_val - min_val)
        
        return data
    
    def _zscore_normalize(self, data: List[Dict], fields: List[str]) -> List[Dict]:
        for field in fields:
            values = [item.get(field, 0) for item in data if isinstance(item, dict)]
            
            if not values:
                continue
            
            mean = sum(values) / len(values)
            variance = sum((v - mean) ** 2 for v in values) / len(values)
            std = variance ** 0.5
            
            if std == 0:
                continue
            
            for item in data:
                if isinstance(item, dict) and field in item:
                    item[field] = (item[field] - mean) / std
        
        return data
    
    def _log_normalize(self, data: List[Dict], fields: List[str]) -> List[Dict]:
        import math
        
        for field in fields:
            for item in data:
                if isinstance(item, dict) and field in item:
                    value = item[field]
                    if isinstance(value, (int, float)) and value > 0:
                        item[field] = math.log(value)
        
        return data


class EnrichDataAction(BaseAction):
    """Enrich data with additional fields."""
    action_type = "enrich_data"
    display_name = "数据增强"
    description = "为数据添加额外字段"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            enrichments = params.get("enrichments", [])
            
            if isinstance(data, dict):
                data = [data]
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            enriched_count = 0
            
            for enrichment in enrichments:
                field_name = enrichment.get("field")
                source = enrichment.get("source")
                default = enrichment.get("default")
                
                if not field_name:
                    continue
                
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    
                    value = self._compute_enrichment(item, source, default)
                    item[field_name] = value
                    enriched_count += 1
            
            return ActionResult(
                success=True,
                message="Data enrichment complete",
                data={
                    "enrichment_count": enriched_count,
                    "enrichments_applied": len(enrichments),
                    "enriched_data": data[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _compute_enrichment(self, item: Dict, source: str, default: Any) -> Any:
        if source == "timestamp":
            return datetime.now().isoformat()
        elif source == "uuid":
            import uuid
            return str(uuid.uuid4())
        elif source.startswith("const:"):
            return source[6:]
        elif source.startswith("computed:"):
            computed_expr = source[9:]
            return self._evaluate_computed(item, computed_expr)
        else:
            return default
    
    def _evaluate_computed(self, item: Dict, expr: str) -> Any:
        try:
            return eval(expr, {"item": item})
        except Exception:
            return None


class FlattenDataAction(BaseAction):
    """Flatten nested data structures."""
    action_type = "flatten_data"
    display_name = "数据扁平化"
    description = "将嵌套数据结构扁平化"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            separator = params.get("separator", ".")
            max_depth = params.get("max_depth", 10)
            
            if isinstance(data, list):
                result = [self._flatten_dict(item, separator, 1, max_depth) 
                         if isinstance(item, dict) else item 
                         for item in data]
            elif isinstance(data, dict):
                result = self._flatten_dict(data, separator, 1, max_depth)
            else:
                return ActionResult(success=False, message="data must be dict or list")
            
            return ActionResult(
                success=True,
                message="Data flattening complete",
                data={
                    "original_data": data,
                    "flattened_data": result,
                    "separator": separator,
                    "max_depth": max_depth
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _flatten_dict(self, d: Dict, separator: str, depth: int, max_depth: int) -> Dict:
        if depth > max_depth:
            return d
        
        result = {}
        
        for key, value in d.items():
            if isinstance(value, dict):
                flattened = self._flatten_dict(value, separator, depth + 1, max_depth)
                for sub_key, sub_value in flattened.items():
                    result[f"{key}{separator}{sub_key}"] = sub_value
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        flattened = self._flatten_dict(item, separator, depth + 1, max_depth)
                        for sub_key, sub_value in flattened.items():
                            result[f"{key}{separator}{i}{separator}{sub_key}"] = sub_value
                    else:
                        result[f"{key}{separator}{i}"] = item
            else:
                result[key] = value
        
        return result
