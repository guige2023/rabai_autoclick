"""Data Format action module for RabAI AutoClick.

Provides data format conversion operations:
- FormatConvertAction: Convert between formats
- FormatSerializeAction: Serialize data
- FormatParseAction: Parse formatted data
- FormatValidateAction: Validate format
"""

from __future__ import annotations

import sys
import os
import json
import csv
import base64
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FormatConvertAction(BaseAction):
    """Convert between formats."""
    action_type = "format_convert"
    display_name = "格式转换"
    description = "数据格式转换"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute format conversion."""
        data = params.get('data', {})
        from_format = params.get('from_format', 'json')
        to_format = params.get('to_format', 'json')
        output_var = params.get('output_var', 'converted_data')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data

            if to_format == 'json':
                result_data = json.dumps(resolved_data, indent=2, ensure_ascii=False)
            elif to_format == 'csv':
                if isinstance(resolved_data, list) and resolved_data:
                    output = []
                    for record in resolved_data:
                        row = {}
                        for k, v in record.items():
                            row[k] = json.dumps(v) if isinstance(v, (dict, list)) else v
                        output.append(row)
                    result_data = output
                else:
                    result_data = [resolved_data]
            elif to_format == 'xml':
                result_data = self._dict_to_xml(resolved_data)
            elif to_format == 'yaml':
                try:
                    import yaml
                    result_data = yaml.dump(resolved_data, allow_unicode=True)
                except ImportError:
                    return ActionResult(success=False, message="pyyaml not installed")
            else:
                result_data = str(resolved_data)

            result = {
                'data': result_data,
                'from_format': from_format,
                'to_format': to_format,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Converted {from_format} to {to_format}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Format convert error: {e}")

    def _dict_to_xml(self, data, root_key='root'):
        """Convert dict to XML string."""
        xml = f'<{root_key}>'
        if isinstance(data, dict):
            for key, value in data.items():
                xml += self._dict_to_xml(value, key)
        elif isinstance(data, list):
            for item in data:
                xml += self._dict_to_xml(item, 'item')
        else:
            xml += str(data)
        xml += f'</{root_key}>'
        return xml


class FormatSerializeAction(BaseAction):
    """Serialize data."""
    action_type = "format_serialize"
    display_name = "数据序列化"
    description = "序列化数据"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute serialization."""
        data = params.get('data', {})
        format_type = params.get('format', 'json')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'serialized_data')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data

            if format_type == 'json':
                serialized = json.dumps(resolved_data, ensure_ascii=False)
            elif format_type == 'base64':
                json_str = json.dumps(resolved_data, ensure_ascii=False)
                serialized = base64.b64encode(json_str.encode(encoding)).decode(encoding)
            elif format_type == 'pickle':
                import pickle
                serialized = base64.b64encode(pickle.dumps(resolved_data)).decode(encoding)
            elif format_type == 'query':
                if isinstance(resolved_data, dict):
                    from urllib.parse import urlencode
                    serialized = urlencode(resolved_data)
                else:
                    serialized = str(resolved_data)
            else:
                serialized = str(resolved_data)

            result = {
                'serialized': serialized,
                'format': format_type,
                'encoding': encoding,
                'length': len(serialized),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Serialized to {format_type}: {len(serialized)} bytes"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Format serialize error: {e}")


class FormatParseAction(BaseAction):
    """Parse formatted data."""
    action_type = "format_parse"
    display_name = "格式解析"
    description = "解析格式化数据"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute parsing."""
        data = params.get('data', '')
        format_type = params.get('format', 'json')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'parsed_data')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data

            if format_type == 'json':
                parsed = json.loads(resolved_data) if isinstance(resolved_data, str) else resolved_data
            elif format_type == 'base64':
                decoded = base64.b64decode(resolved_data.encode(encoding)).decode(encoding)
                parsed = json.loads(decoded)
            elif format_type == 'pickle':
                import pickle
                decoded = base64.b64decode(resolved_data.encode(encoding))
                parsed = pickle.loads(decoded)
            elif format_type == 'csv':
                if isinstance(resolved_data, str):
                    lines = resolved_data.strip().split('\n')
                    reader = csv.DictReader(lines)
                    parsed = list(reader)
                else:
                    parsed = resolved_data
            elif format_type == 'query':
                from urllib.parse import parse_qs
                parsed = parse_qs(resolved_data)
            else:
                parsed = resolved_data

            result = {
                'parsed': parsed,
                'format': format_type,
                'type': type(parsed).__name__,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Parsed {format_type}: {type(parsed).__name__}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Format parse error: {e}")


class FormatValidateAction(BaseAction):
    """Validate format."""
    action_type = "format_validate"
    display_name = "格式验证"
    description = "验证数据格式"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute format validation."""
        data = params.get('data', '')
        format_type = params.get('format', 'json')
        schema = params.get('schema', None)
        output_var = params.get('output_var', 'validation_result')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            valid = True
            errors = []

            if format_type == 'json':
                if isinstance(resolved_data, str):
                    try:
                        json.loads(resolved_data)
                    except json.JSONDecodeError as e:
                        valid = False
                        errors.append(f"JSON parse error: {e}")

            elif format_type == 'csv':
                if isinstance(resolved_data, str):
                    lines = resolved_data.strip().split('\n')
                    if len(lines) < 2:
                        valid = False
                        errors.append("CSV must have header and at least one row")

            elif format_type == 'email':
                import re
                pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                if not re.match(pattern, resolved_data):
                    valid = False
                    errors.append("Invalid email format")

            elif format_type == 'url':
                import re
                pattern = r'^https?://[^\s/$.?#].[^\s]*$'
                if not re.match(pattern, resolved_data):
                    valid = False
                    errors.append("Invalid URL format")

            elif format_type == 'uuid':
                import re
                pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                if not re.match(pattern, str(resolved_data).lower()):
                    valid = False
                    errors.append("Invalid UUID format")

            result = {
                'valid': valid,
                'format': format_type,
                'errors': errors,
            }

            return ActionResult(
                success=valid,
                data={output_var: result},
                message="Valid" if valid else f"Invalid: {errors[0]}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Format validate error: {e}")
