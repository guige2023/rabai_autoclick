"""JSON action module for RabAI AutoClick.

Provides JSON operations:
- JsonParseAction: Parse JSON string
- JsonDumpAction: Dump to JSON string
- JsonReadAction: Read JSON file
- JsonWriteAction: Write JSON file
- JsonValidateAction: Validate JSON
- JsonMergeAction: Merge JSON objects
"""

import json
import os
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class JsonParseAction(BaseAction):
    """Parse JSON string."""
    action_type = "json_parse"
    display_name = "JSON解析"
    description = "解析JSON字符串"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute parse.

        Args:
            context: Execution context.
            params: Dict with json_string, output_var.

        Returns:
            ActionResult with parsed object.
        """
        json_string = params.get('json_string', '')
        output_var = params.get('output_var', 'json_parsed')

        valid, msg = self.validate_type(json_string, str, 'json_string')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_str = context.resolve_value(json_string)

            parsed = json.loads(resolved_str)
            context.set(output_var, parsed)

            return ActionResult(
                success=True,
                message=f"JSON已解析",
                data={'parsed': parsed, 'output_var': output_var}
            )
        except json.JSONDecodeError as e:
            return ActionResult(success=False, message=f"JSON解析失败: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"JSON解析失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['json_string']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'json_parsed'}


class JsonDumpAction(BaseAction):
    """Dump to JSON string."""
    action_type = "json_dump"
    display_name = "JSON序列化"
    description = "对象序列化为JSON"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dump.

        Args:
            context: Execution context.
            params: Dict with data, indent, output_var.

        Returns:
            ActionResult with JSON string.
        """
        data = params.get('data', {})
        indent = params.get('indent', 2)
        output_var = params.get('output_var', 'json_dumped')

        try:
            resolved_data = context.resolve_value(data)
            resolved_indent = context.resolve_value(indent)

            dumped = json.dumps(resolved_data, indent=resolved_indent, ensure_ascii=False)
            context.set(output_var, dumped)

            return ActionResult(
                success=True,
                message=f"JSON已序列化 ({len(dumped)} 字符)",
                data={'json': dumped, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"JSON序列化失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'indent': 2, 'output_var': 'json_dumped'}


class JsonReadAction(BaseAction):
    """Read JSON file."""
    action_type = "json_read"
    display_name = "JSON读取"
    description = "读取JSON文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute read.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with file content.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'json_data')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)

            if not os.path.exists(resolved_path):
                return ActionResult(success=False, message=f"文件不存在: {resolved_path}")

            with open(resolved_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            context.set(output_var, data)

            return ActionResult(
                success=True,
                message=f"JSON已读取: {resolved_path}",
                data={'data': data, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"JSON读取失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'json_data'}


class JsonWriteAction(BaseAction):
    """Write JSON file."""
    action_type = "json_write"
    display_name = "JSON写入"
    description = "写入JSON文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute write.

        Args:
            context: Execution context.
            params: Dict with file_path, data, indent.

        Returns:
            ActionResult indicating success.
        """
        file_path = params.get('file_path', '')
        data = params.get('data', {})
        indent = params.get('indent', 2)

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_data = context.resolve_value(data)
            resolved_indent = context.resolve_value(indent)

            os.makedirs(os.path.dirname(resolved_path) or '.', exist_ok=True)

            with open(resolved_path, 'w', encoding='utf-8') as f:
                json.dump(resolved_data, f, indent=resolved_indent, ensure_ascii=False)

            return ActionResult(
                success=True,
                message=f"JSON已写入: {resolved_path}",
                data={'file_path': resolved_path}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"JSON写入失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'indent': 2}


class JsonValidateAction(BaseAction):
    """Validate JSON."""
    action_type = "json_validate"
    display_name = "JSON验证"
    description = "验证JSON格式"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute validate.

        Args:
            context: Execution context.
            params: Dict with json_string, output_var.

        Returns:
            ActionResult with validation result.
        """
        json_string = params.get('json_string', '')
        output_var = params.get('output_var', 'json_valid')

        valid, msg = self.validate_type(json_string, str, 'json_string')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_str = context.resolve_value(json_string)

            json.loads(resolved_str)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message="JSON有效",
                data={'valid': True, 'output_var': output_var}
            )
        except json.JSONDecodeError:
            context.set(output_var, False)
            return ActionResult(
                success=True,
                message="JSON无效",
                data={'valid': False, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"JSON验证失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['json_string']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'json_valid'}


class JsonMergeAction(BaseAction):
    """Merge JSON objects."""
    action_type = "json_merge"
    display_name = "JSON合并"
    description = "合并JSON对象"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute merge.

        Args:
            context: Execution context.
            params: Dict with objects, output_var.

        Returns:
            ActionResult with merged object.
        """
        objects = params.get('objects', [])
        output_var = params.get('output_var', 'json_merged')

        valid, msg = self.validate_type(objects, list, 'objects')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_objects = context.resolve_value(objects)

            merged = {}
            for obj in resolved_objects:
                if isinstance(obj, dict):
                    merged.update(obj)

            context.set(output_var, merged)

            return ActionResult(
                success=True,
                message=f"JSON已合并: {len(merged)} 个键",
                data={'merged': merged, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"JSON合并失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['objects']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'json_merged'}
