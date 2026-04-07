"""JSONL (JSON Lines) action module for RabAI AutoClick.

Provides JSONL operations:
- JsonlReadAction: Read JSONL file
- JsonlWriteAction: Write JSONL file
- JsonlAppendAction: Append to JSONL file
- JsonlFilterAction: Filter JSONL lines
- JsonlStatsAction: Statistics on JSONL
"""

from __future__ import annotations

import json
import sys
import os
from typing import Any, Dict, List, Optional, Iterator

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class JsonlReadAction(BaseAction):
    """Read JSONL file."""
    action_type = "jsonl_read"
    display_name = "JSONL读取"
    description = "读取JSONL文件"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JSONL read."""
        file_path = params.get('file_path', '')
        limit = params.get('limit', None)
        skip = params.get('skip', 0)
        output_var = params.get('output_var', 'jsonl_data')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            resolved_path = context.resolve_value(file_path) if context else file_path
            resolved_skip = context.resolve_value(skip) if context else skip
            resolved_limit = context.resolve_value(limit) if context else limit

            lines = []
            with open(resolved_path, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if i < resolved_skip:
                        continue
                    line = line.strip()
                    if line:
                        lines.append(json.loads(line))
                    if resolved_limit and len(lines) >= resolved_limit:
                        break

            result = {'lines': lines, 'count': len(lines)}
            if context:
                context.set(output_var, lines)
                context.set(f"{output_var}_info", result)
            return ActionResult(success=True, message=f"Read {len(lines)} JSONL lines", data=result)
        except FileNotFoundError:
            return ActionResult(success=False, message=f"File not found: {resolved_path}")
        except json.JSONDecodeError as e:
            return ActionResult(success=False, message=f"Invalid JSON at line: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"JSONL read error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'limit': None, 'skip': 0, 'output_var': 'jsonl_data'}


class JsonlWriteAction(BaseAction):
    """Write JSONL file."""
    action_type = "jsonl_write"
    display_name = "JSONL写入"
    description = "写入JSONL文件"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JSONL write."""
        file_path = params.get('file_path', '')
        data = params.get('data', [])
        output_var = params.get('output_var', 'jsonl_write_result')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            resolved_path = context.resolve_value(file_path) if context else file_path
            resolved_data = context.resolve_value(data) if context else data

            _os.makedirs(_os.path.dirname(resolved_path) or '.', exist_ok=True)

            with open(resolved_path, 'w', encoding='utf-8') as f:
                for item in resolved_data:
                    f.write(json.dumps(item, ensure_ascii=False) + '\n')

            result = {'written': True, 'lines': len(resolved_data), 'path': resolved_path}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Wrote {len(resolved_data)} lines to {resolved_path}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"JSONL write error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'jsonl_write_result'}


class JsonlAppendAction(BaseAction):
    """Append to JSONL file."""
    action_type = "jsonl_append"
    display_name = "JSONL追加"
    description = "追加到JSONL文件"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JSONL append."""
        file_path = params.get('file_path', '')
        data = params.get('data', [])  # list of dicts
        output_var = params.get('output_var', 'jsonl_append_result')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            resolved_path = context.resolve_value(file_path) if context else file_path
            resolved_data = context.resolve_value(data) if context else data

            _os.makedirs(_os.path.dirname(resolved_path) or '.', exist_ok=True)

            with open(resolved_path, 'a', encoding='utf-8') as f:
                for item in resolved_data:
                    f.write(json.dumps(item, ensure_ascii=False) + '\n')

            result = {'appended': len(resolved_data), 'path': resolved_path}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Appended {len(resolved_data)} lines", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"JSONL append error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'jsonl_append_result'}


class JsonlFilterAction(BaseAction):
    """Filter JSONL lines."""
    action_type = "jsonl_filter"
    display_name = "JSONL过滤"
    description = "过滤JSONL行"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JSONL filter."""
        data = params.get('data', [])
        key = params.get('key', '')
        operator = params.get('operator', 'eq')
        value = params.get('value', None)
        output_var = params.get('output_var', 'filtered_jsonl')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_key = context.resolve_value(key) if context else key
            resolved_value = context.resolve_value(value) if context else value
            resolved_op = context.resolve_value(operator) if context else operator

            operators = {
                'eq': lambda a, b: a == b,
                'ne': lambda a, b: a != b,
                'gt': lambda a, b: float(a) > float(b) if (a is not None and b is not None) else False,
                'lt': lambda a, b: float(a) < float(b) if (a is not None and b is not None) else False,
                'ge': lambda a, b: float(a) >= float(b) if (a is not None and b is not None) else False,
                'le': lambda a, b: float(a) <= float(b) if (a is not None and b is not None) else False,
                'contains': lambda a, b: str(b) in str(a) if a is not None else False,
                'exists': lambda a, b: a is not None,
            }

            op_func = operators.get(resolved_op, operators['eq'])
            filtered = []

            for line in resolved_data:
                if isinstance(line, dict):
                    line_val = line.get(resolved_key)
                    if op_func(line_val, resolved_value):
                        filtered.append(line)

            result = {'filtered': filtered, 'count': len(filtered), 'original': len(resolved_data)}
            if context:
                context.set(output_var, filtered)
            return ActionResult(success=True, message=f"Filtered: {len(resolved_data)} -> {len(filtered)}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"JSONL filter error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'key': '', 'operator': 'eq', 'value': None, 'output_var': 'filtered_jsonl'}


class JsonlStatsAction(BaseAction):
    """Get statistics on JSONL data."""
    action_type = "jsonl_stats"
    display_name = "JSONL统计"
    description = "JSONL统计信息"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JSONL stats."""
        data = params.get('data', [])
        output_var = params.get('output_var', 'jsonl_stats')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data

            all_keys = set()
            for line in resolved_data:
                if isinstance(line, dict):
                    all_keys.update(line.keys())

            key_counts = {}
            for key in all_keys:
                key_counts[key] = sum(1 for line in resolved_data if isinstance(line, dict) and key in line)

            result = {
                'total_lines': len(resolved_data),
                'keys': list(all_keys),
                'key_counts': key_counts,
            }

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Stats: {len(resolved_data)} lines, {len(all_keys)} keys", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"JSONL stats error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'jsonl_stats'}
