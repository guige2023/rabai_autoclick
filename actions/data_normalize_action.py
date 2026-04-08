"""Data Normalize action module for RabAI AutoClick.

Provides data normalization operations:
- NormalizeScaleAction: Scale normalization
- NormalizeEncodeAction: Encode normalization
- NormalizeCleanAction: Clean normalization
- NormalizeReshapeAction: Reshape normalization
"""

from __future__ import annotations

import sys
import os
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class NormalizeScaleAction(BaseAction):
    """Scale normalization."""
    action_type = "normalize_scale"
    display_name = "缩放归一化"
    description = "缩放归一化"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute scale normalization."""
        data = params.get('data', [])
        fields = params.get('fields', [])
        method = params.get('method', 'minmax')
        output_var = params.get('output_var', 'normalized_data')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_fields = context.resolve_value(fields) if context else fields

            if not resolved_fields:
                if resolved_data and isinstance(resolved_data[0], dict):
                    resolved_fields = [k for k, v in resolved_data[0].items() if isinstance(v, (int, float))]

            normalized = []
            for record in resolved_data:
                new_record = record.copy()
                for field in resolved_fields:
                    if field in record and isinstance(record[field], (int, float)):
                        value = record[field]

                        if method == 'minmax':
                            min_val = min(r.get(field, 0) for r in resolved_data if isinstance(r.get(field), (int, float)))
                            max_val = max(r.get(field, 0) for r in resolved_data if isinstance(r.get(field), (int, float)))
                            if max_val > min_val:
                                new_record[field] = (value - min_val) / (max_val - min_val)

                        elif method == 'zscore':
                            values = [r.get(field, 0) for r in resolved_data if isinstance(r.get(field), (int, float))]
                            mean = sum(values) / len(values)
                            variance = sum((x - mean) ** 2 for x in values) / len(values)
                            std = variance ** 0.5
                            if std > 0:
                                new_record[field] = (value - mean) / std

                        elif method == 'robust':
                            values = sorted([r.get(field, 0) for r in resolved_data if isinstance(r.get(field), (int, float))])
                            n = len(values)
                            q1 = values[n // 4]
                            q3 = values[3 * n // 4]
                            iqr = q3 - q1
                            if iqr > 0:
                                new_record[field] = (value - q1) / iqr

                normalized.append(new_record)

            result = {
                'data': normalized,
                'record_count': len(normalized),
                'method': method,
                'fields_normalized': resolved_fields,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Normalized {len(resolved_fields)} fields using {method} method"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Normalize scale error: {e}")


class NormalizeEncodeAction(BaseAction):
    """Encode normalization."""
    action_type = "normalize_encode"
    display_name = "编码归一化"
    description = "编码归一化"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute encoding normalization."""
        data = params.get('data', [])
        field = params.get('field', '')
        encoding = params.get('encoding', 'onehot')
        output_var = params.get('output_var', 'encoded_data')

        if not data or not field:
            return ActionResult(success=False, message="data and field are required")

        try:
            resolved_data = context.resolve_value(data) if context else data

            unique_values = sorted(set(r.get(field, '') for r in resolved_data if r.get(field)))

            encoded = []
            encoding_map = {v: i for i, v in enumerate(unique_values)}

            for record in resolved_data:
                new_record = record.copy()

                if encoding == 'label':
                    new_record[f'{field}_encoded'] = encoding_map.get(record.get(field, ''), -1)
                elif encoding == 'onehot':
                    for val in unique_values:
                        new_record[f'{field}_{val}'] = 1 if record.get(field) == val else 0

                encoded.append(new_record)

            result = {
                'data': encoded,
                'record_count': len(encoded),
                'encoding': encoding,
                'unique_values': unique_values,
                'field': field,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Encoded {field} with {encoding}: {len(unique_values)} categories"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Normalize encode error: {e}")


class NormalizeCleanAction(BaseAction):
    """Clean normalization."""
    action_type = "normalize_clean"
    display_name = "清洗归一化"
    description = "数据清洗归一化"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute clean normalization."""
        data = params.get('data', [])
        operations = params.get('operations', [])
        output_var = params.get('output_var', 'cleaned_data')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_ops = context.resolve_value(operations) if context else operations

            cleaned = []
            for record in resolved_data:
                new_record = {}
                for key, value in record.items():
                    if isinstance(value, str):
                        new_val = value.strip()
                        if 'trim' in resolved_ops:
                            new_val = ' '.join(new_val.split())
                        if 'lowercase' in resolved_ops:
                            new_val = new_val.lower()
                        if 'remove_special' in resolved_ops:
                            import re
                            new_val = re.sub(r'[^\w\s]', '', new_val)
                        new_record[key] = new_val
                    else:
                        new_record[key] = value
                cleaned.append(new_record)

            result = {
                'data': cleaned,
                'record_count': len(cleaned),
                'operations_applied': len(resolved_ops),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Cleaned {len(cleaned)} records"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Normalize clean error: {e}")


class NormalizeReshapeAction(BaseAction):
    """Reshape normalization."""
    action_type = "normalize_reshape"
    display_name = "重塑归一化"
    description = "数据重塑归一化"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute reshape normalization."""
        data = params.get('data', [])
        reshape_type = params.get('reshape_type', 'transpose')
        output_var = params.get('output_var', 'reshaped_data')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data

            if reshape_type == 'transpose' and resolved_data and isinstance(resolved_data[0], dict):
                keys = list(resolved_data[0].keys())
                transposed = []
                for key in keys:
                    new_record = {'_field': key}
                    for i, record in enumerate(resolved_data):
                        new_record[f'_row_{i}'] = record.get(key)
                    transposed.append(new_record)
                result_data = transposed

            elif reshape_type == 'flatten':
                flattened = []
                for record in resolved_data:
                    flat_record = {}
                    for key, value in record.items():
                        if isinstance(value, dict):
                            for sub_key, sub_value in value.items():
                                flat_record[f'{key}_{sub_key}'] = sub_value
                        elif isinstance(value, list):
                            flat_record[f'{key}_count'] = len(value)
                        else:
                            flat_record[key] = value
                    flattened.append(flat_record)
                result_data = flattened

            else:
                result_data = resolved_data

            result = {
                'data': result_data,
                'record_count': len(result_data),
                'reshape_type': reshape_type,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Reshaped to {reshape_type}: {len(result_data)} records"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Normalize reshape error: {e}")
