"""Normalize action module for RabAI AutoClick.

Provides data normalization actions for standardizing data formats,
scales, and representations.
"""

import re
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class NormalizeNumbersAction(BaseAction):
    """Normalize numeric values to common scale.
    
    Supports min-max, z-score, and log normalization.
    """
    action_type = "normalize_numbers"
    display_name = "数字归一化"
    description = "数值标准化归一化"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Normalize numbers.
        
        Args:
            context: Execution context.
            params: Dict with keys: values, method, min_val, max_val,
                   scale_range.
        
        Returns:
            ActionResult with normalized values.
        """
        values = params.get('values', [])
        method = params.get('method', 'minmax')
        min_val = params.get('min_val', None)
        max_val = params.get('max_val', None)
        scale_range = params.get('scale_range', (0, 1))

        if not values:
            return ActionResult(success=False, message="values list is required")

        try:
            numeric_values = [v for v in values if isinstance(v, (int, float)) and v is not None]
            
            if not numeric_values:
                return ActionResult(success=False, message="No numeric values found")

            result = []
            
            if method == 'minmax':
                v_min = min_val if min_val is not None else min(numeric_values)
                v_max = max_val if max_val is not None else max(numeric_values)
                target_min, target_max = scale_range
                
                if v_max == v_min:
                    result = [target_min] * len(values)
                else:
                    for v in values:
                        if isinstance(v, (int, float)) and v is not None:
                            normalized = ((v - v_min) / (v_max - v_min)) * (target_max - target_min) + target_min
                            result.append(normalized)
                        else:
                            result.append(v)

            elif method == 'zscore':
                mean = sum(numeric_values) / len(numeric_values)
                variance = sum((x - mean) ** 2 for x in numeric_values) / len(numeric_values)
                std = variance ** 0.5
                
                for v in values:
                    if isinstance(v, (int, float)) and v is not None and std > 0:
                        result.append((v - mean) / std)
                    else:
                        result.append(v)

            elif method == 'log':
                for v in values:
                    if isinstance(v, (int, float)) and v is not None and v > 0:
                        import math
                        result.append(math.log(v))
                    else:
                        result.append(v)

            elif method == 'robust':
                sorted_vals = sorted(numeric_values)
                q1 = sorted_vals[len(sorted_vals) // 4]
                q3 = sorted_vals[3 * len(sorted_vals) // 4]
                iqr = q3 - q1
                
                for v in values:
                    if isinstance(v, (int, float)) and v is not None and iqr > 0:
                        result.append((v - q1) / iqr)
                    else:
                        result.append(v)

            return ActionResult(
                success=True,
                message=f"Normalized {len(result)} values using {method}",
                data={
                    'values': result,
                    'original_count': len(values),
                    'method': method
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Normalization failed: {str(e)}")


class NormalizeStringsAction(BaseAction):
    """Normalize string values.
    
    Handles case, whitespace, encoding, and format standardization.
    """
    action_type = "normalize_strings"
    display_name = "字符串标准化"
    description = "字符串格式标准化"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Normalize strings.
        
        Args:
            context: Execution context.
            params: Dict with keys: strings, lowercase, uppercase,
                   strip, remove_punctuation, normalize_unicode.
        
        Returns:
            ActionResult with normalized strings.
        """
        strings = params.get('strings', [])
        lowercase = params.get('lowercase', False)
        uppercase = params.get('uppercase', False)
        strip = params.get('strip', True)
        remove_punctuation = params.get('remove_punctuation', False)
        normalize_unicode = params.get('normalize_unicode', False)

        if not strings:
            return ActionResult(success=False, message="strings list is required")

        try:
            result = []

            for s in strings:
                if not isinstance(s, str):
                    result.append(s)
                    continue

                normalized = s

                if strip:
                    normalized = normalized.strip()

                if normalize_unicode:
                    import unicodedata
                    normalized = unicodedata.normalize('NFKC', normalized)

                if remove_punctuation:
                    normalized = re.sub(r'[^\w\s]', '', normalized)

                if lowercase:
                    normalized = normalized.lower()
                elif uppercase:
                    normalized = normalized.upper()

                normalized = re.sub(r'\s+', ' ', normalized)

                result.append(normalized)

            return ActionResult(
                success=True,
                message=f"Normalized {len(result)} strings",
                data={
                    'strings': result,
                    'original_count': len(strings)
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"String normalization failed: {str(e)}")


class NormalizeDatesAction(BaseAction):
    """Normalize date and time values.
    
    Converts various date formats to standard representation.
    """
    action_type = "normalize_dates"
    display_name = "日期标准化"
    description = "日期格式标准化"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Normalize dates.
        
        Args:
            context: Execution context.
            params: Dict with keys: dates, output_format, input_formats,
                   timezone.
        
        Returns:
            ActionResult with normalized dates.
        """
        dates = params.get('dates', [])
        output_format = params.get('output_format', '%Y-%m-%d %H:%M:%S')
        input_formats = params.get('input_formats', None)
        timezone = params.get('timezone', 'UTC')

        if not dates:
            return ActionResult(success=False, message="dates list is required")

        try:
            from datetime import datetime
            import dateutil.parser

            default_formats = [
                '%Y-%m-%d',
                '%Y-%m-%d %H:%M:%S',
                '%Y/%m/%d',
                '%d/%m/%Y',
                '%m/%d/%Y',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%dT%H:%M:%S.%fZ',
            ]

            formats = input_formats or default_formats
            result = []
            parsed_count = 0

            for d in dates:
                if not d:
                    result.append(None)
                    continue

                parsed = None

                if isinstance(d, (int, float)):
                    try:
                        parsed = datetime.fromtimestamp(d)
                        parsed_count += 1
                    except:
                        pass

                elif isinstance(d, str):
                    try:
                        parsed = dateutil.parser.parse(d)
                        parsed_count += 1
                    except:
                        for fmt in formats:
                            try:
                                parsed = datetime.strptime(d, fmt)
                                parsed_count += 1
                                break
                            except:
                                continue

                elif isinstance(d, datetime):
                    parsed = d
                    parsed_count += 1

                if parsed:
                    result.append(parsed.strftime(output_format))
                else:
                    result.append(str(d))

            return ActionResult(
                success=True,
                message=f"Normalized {parsed_count}/{len(dates)} dates",
                data={
                    'dates': result,
                    'original_count': len(dates),
                    'parsed_count': parsed_count,
                    'output_format': output_format
                }
            )

        except ImportError:
            return ActionResult(
                success=False,
                message="python-dateutil not installed. Run: pip install python-dateutil"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Date normalization failed: {str(e)}")


class NormalizeSchemaAction(BaseAction):
    """Normalize data schemas.
    
    Standardizes field names, types, and structures.
    """
    action_type = "normalize_schema"
    display_name = "Schema标准化"
    description = "数据结构标准化"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Normalize schema.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, field_mapping, required_fields,
                   default_values.
        
        Returns:
            ActionResult with normalized data.
        """
        data = params.get('data', [])
        field_mapping = params.get('field_mapping', {})
        required_fields = params.get('required_fields', [])
        default_values = params.get('default_values', {})

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            result = []

            for item in data:
                if not isinstance(item, dict):
                    continue

                normalized = {}

                for old_name, new_name in field_mapping.items():
                    if old_name in item:
                        normalized[new_name] = item[old_name]

                for key, value in item.items():
                    if key not in field_mapping:
                        normalized[key] = value

                for field in required_fields:
                    if field not in normalized:
                        normalized[field] = default_values.get(field)

                result.append(normalized)

            return ActionResult(
                success=True,
                message=f"Normalized {len(result)} records",
                data={
                    'data': result,
                    'original_count': len(data),
                    'normalized_count': len(result),
                    'field_mapping': field_mapping
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Schema normalization failed: {str(e)}")


class NormalizeJsonAction(BaseAction):
    """Normalize JSON data structures.
    
    Handles JSON path operations and structure standardization.
    """
    action_type = "normalize_json"
    display_name = "JSON标准化"
    description = "JSON数据标准化"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Normalize JSON.
        
        Args:
            context: Execution context.
            params: Dict with keys: json_data, compact, sort_keys,
                   ensure_ascii.
        
        Returns:
            ActionResult with normalized JSON.
        """
        json_data = params.get('json_data', '')
        compact = params.get('compact', False)
        sort_keys = params.get('sort_keys', False)
        ensure_ascii = params.get('ensure_ascii', False)

        if not json_data:
            return ActionResult(success=False, message="json_data is required")

        try:
            import json

            if isinstance(json_data, str):
                parsed = json.loads(json_data)
            else:
                parsed = json_data

            if compact:
                normalized = json.dumps(parsed, separators=(',', ':'), sort_keys=sort_keys)
            else:
                normalized = json.dumps(parsed, indent=2, sort_keys=sort_keys, ensure_ascii=ensure_ascii)

            return ActionResult(
                success=True,
                message="JSON normalized",
                data={
                    'json': normalized,
                    'original_size': len(str(json_data)),
                    'normalized_size': len(normalized)
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"JSON normalization failed: {str(e)}")
