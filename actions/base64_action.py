"""Base64 action module for RabAI AutoClick.

Provides Base64 encoding/decoding operations:
- Base64EncodeAction: Encode to Base64
- Base64DecodeAction: Decode from Base64
- Base64UrlEncodeAction: URL-safe Base64 encode
- Base64UrlDecodeAction: URL-safe Base64 decode
- Base64FileEncodeAction: Encode file to Base64
- Base64FileDecodeAction: Decode Base64 to file
"""

import base64
import os
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class Base64EncodeAction(BaseAction):
    """Encode to Base64."""
    action_type = "base64_encode"
    display_name = "Base64编码"
    description = "字符串Base64编码"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute encode.

        Args:
            context: Execution context.
            params: Dict with data, encoding, output_var.

        Returns:
            ActionResult with encoded string.
        """
        data = params.get('data', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'base64_encoded')

        valid, msg = self.validate_type(data, str, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_enc = context.resolve_value(encoding)

            encoded = base64.b64encode(resolved_data.encode(resolved_enc)).decode('ascii')
            context.set(output_var, encoded)

            return ActionResult(
                success=True,
                message=f"Base64编码完成 ({len(encoded)} 字符)",
                data={'encoded': encoded, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Base64编码失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'output_var': 'base64_encoded'}


class Base64DecodeAction(BaseAction):
    """Decode from Base64."""
    action_type = "base64_decode"
    display_name = "Base64解码"
    description = "Base64解码"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute decode.

        Args:
            context: Execution context.
            params: Dict with data, encoding, output_var.

        Returns:
            ActionResult with decoded string.
        """
        data = params.get('data', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'base64_decoded')

        valid, msg = self.validate_type(data, str, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_enc = context.resolve_value(encoding)

            decoded = base64.b64decode(resolved_data.encode('ascii')).decode(resolved_enc)
            context.set(output_var, decoded)

            return ActionResult(
                success=True,
                message=f"Base64解码完成 ({len(decoded)} 字符)",
                data={'decoded': decoded, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Base64解码失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'output_var': 'base64_decoded'}


class Base64UrlEncodeAction(BaseAction):
    """URL-safe Base64 encode."""
    action_type = "base64_url_encode"
    display_name = "Base64 URL编码"
    description = "URL安全Base64编码"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute URL encode.

        Args:
            context: Execution context.
            params: Dict with data, encoding, output_var.

        Returns:
            ActionResult with encoded string.
        """
        data = params.get('data', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'base64_url_encoded')

        valid, msg = self.validate_type(data, str, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_enc = context.resolve_value(encoding)

            encoded = base64.urlsafe_b64encode(resolved_data.encode(resolved_enc)).decode('ascii')
            context.set(output_var, encoded)

            return ActionResult(
                success=True,
                message=f"Base64 URL编码完成 ({len(encoded)} 字符)",
                data={'encoded': encoded, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Base64 URL编码失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'output_var': 'base64_url_encoded'}


class Base64UrlDecodeAction(BaseAction):
    """URL-safe Base64 decode."""
    action_type = "base64_url_decode"
    display_name = "Base64 URL解码"
    description = "URL安全Base64解码"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute URL decode.

        Args:
            context: Execution context.
            params: Dict with data, encoding, output_var.

        Returns:
            ActionResult with decoded string.
        """
        data = params.get('data', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'base64_url_decoded')

        valid, msg = self.validate_type(data, str, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_enc = context.resolve_value(encoding)

            decoded = base64.urlsafe_b64decode(resolved_data.encode('ascii')).decode(resolved_enc)
            context.set(output_var, decoded)

            return ActionResult(
                success=True,
                message=f"Base64 URL解码完成 ({len(decoded)} 字符)",
                data={'decoded': decoded, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Base64 URL解码失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'output_var': 'base64_url_decoded'}


class Base64FileEncodeAction(BaseAction):
    """Encode file to Base64."""
    action_type = "base64_file_encode"
    display_name = "Base64文件编码"
    description = "文件Base64编码"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file encode.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with encoded string.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'base64_file_encoded')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)

            if not os.path.exists(resolved_path):
                return ActionResult(success=False, message=f"文件不存在: {resolved_path}")

            with open(resolved_path, 'rb') as f:
                encoded = base64.b64encode(f.read()).decode('ascii')

            context.set(output_var, encoded)

            return ActionResult(
                success=True,
                message=f"文件Base64编码完成 ({len(encoded)} 字符)",
                data={'size': len(encoded), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"文件Base64编码失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'base64_file_encoded'}


class Base64FileDecodeAction(BaseAction):
    """Decode Base64 to file."""
    action_type = "base64_file_decode"
    display_name = "Base64文件解码"
    description = "Base64解码到文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file decode.

        Args:
            context: Execution context.
            params: Dict with data, output_file.

        Returns:
            ActionResult indicating success.
        """
        data = params.get('data', '')
        output_file = params.get('output_file', '/tmp/decoded_file')

        valid, msg = self.validate_type(data, str, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(output_file, str, 'output_file')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_output = context.resolve_value(output_file)

            decoded = base64.b64decode(resolved_data.encode('ascii'))

            with open(resolved_output, 'wb') as f:
                f.write(decoded)

            return ActionResult(
                success=True,
                message=f"Base64解码到文件: {resolved_output}",
                data={'output_file': resolved_output, 'size': len(decoded)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Base64文件解码失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data', 'output_file']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}
