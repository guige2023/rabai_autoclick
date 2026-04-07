"""Base64 action module for RabAI AutoClick.

Provides Base64 encoding/decoding operations:
- Base64EncodeAction: Encode to Base64
- Base64DecodeAction: Decode from Base64
"""

import base64
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class Base64EncodeAction(BaseAction):
    """Encode to Base64."""
    action_type = "base64_encode"
    display_name = "Base64编码"
    description = "将字符串编码为Base64"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Base64 encoding.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with encoded value.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'base64_result')

        try:
            resolved = context.resolve_value(value)

            if isinstance(resolved, str):
                result = base64.b64encode(resolved.encode()).decode()
            elif isinstance(resolved, bytes):
                result = base64.b64encode(resolved).decode()
            else:
                result = base64.b64encode(str(resolved).encode()).decode()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Base64编码完成: {len(result)} 字符",
                data={
                    'result': result,
                    'length': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base64编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'base64_result'}


class Base64DecodeAction(BaseAction):
    """Decode from Base64."""
    action_type = "base64_decode"
    display_name = "Base64解码"
    description = "将Base64字符串解码"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Base64 decoding.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with decoded value.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'base64_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)

            decoded = base64.b64decode(resolved.encode()).decode()
            context.set(output_var, decoded)

            return ActionResult(
                success=True,
                message=f"Base64解码完成: {len(decoded)} 字符",
                data={
                    'result': decoded,
                    'length': len(decoded),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base64解码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'base64_result'}


class Base64EncodeFileAction(BaseAction):
    """Encode file to Base64."""
    action_type = "base64_encode_file"
    display_name = "文件Base64编码"
    description = "将文件编码为Base64"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file Base64 encoding.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with encoded file content.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'base64_result')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"文件不存在: {resolved_path}"
                )

            with open(resolved_path, 'rb') as f:
                result = base64.b64encode(f.read()).decode()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"文件Base64编码完成: {len(result)} 字符",
                data={
                    'result': result,
                    'length': len(result),
                    'file_path': resolved_path,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"文件Base64编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'base64_result'}


class Base64DecodeToFileAction(BaseAction):
    """Decode Base64 to file."""
    action_type = "base64_decode_to_file"
    display_name = "Base64解码到文件"
    description = "将Base64数据解码并保存到文件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Base64 decoding to file.

        Args:
            context: Execution context.
            params: Dict with value, output_path.

        Returns:
            ActionResult indicating success.
        """
        value = params.get('value', '')
        output_path = params.get('output_path', '')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(output_path, str, 'output_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_value = context.resolve_value(value)
            resolved_output = context.resolve_value(output_path)

            decoded = base64.b64decode(resolved_value.encode())

            with open(resolved_output, 'wb') as f:
                f.write(decoded)

            return ActionResult(
                success=True,
                message=f"Base64已解码到文件: {resolved_output}",
                data={
                    'output_path': resolved_output,
                    'size': len(decoded)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base64解码到文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'output_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}