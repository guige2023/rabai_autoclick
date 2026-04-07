"""Encoding action module for RabAI AutoClick.

Provides encoding/decoding operations:
- EncodingEncodeAction: Encode string
- EncodingDecodeAction: Decode string
- EncodingDetectAction: Detect encoding
- EncodingConvertAction: Convert encoding
"""

import chardet
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EncodingEncodeAction(BaseAction):
    """Encode string."""
    action_type = "encoding_encode"
    display_name = "编码转换"
    description = "编码转换"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute encode.

        Args:
            context: Execution context.
            params: Dict with text, target_encoding, source_encoding, output_var.

        Returns:
            ActionResult with encoded string.
        """
        text = params.get('text', '')
        target_encoding = params.get('target_encoding', 'utf-8')
        source_encoding = params.get('source_encoding', 'utf-8')
        output_var = params.get('output_var', 'encoded_text')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_text = context.resolve_value(text)
            resolved_target = context.resolve_value(target_encoding)
            resolved_source = context.resolve_value(source_encoding)

            encoded = resolved_text.encode(resolved_target, errors='ignore').decode(resolved_target)

            context.set(output_var, encoded)

            return ActionResult(
                success=True,
                message=f"编码完成: {resolved_target}",
                data={'encoded': encoded, 'target_encoding': resolved_target, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"编码转换失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['text', 'target_encoding']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'source_encoding': 'utf-8', 'output_var': 'encoded_text'}


class EncodingDecodeAction(BaseAction):
    """Decode bytes."""
    action_type = "encoding_decode"
    display_name = "解码转换"
    description = "解码转换"
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
        output_var = params.get('output_var', 'decoded_text')

        valid, msg = self.validate_type(data, (str, bytes), 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_enc = context.resolve_value(encoding)

            if isinstance(resolved_data, str):
                decoded = resolved_data
            else:
                decoded = resolved_data.decode(resolved_enc, errors='ignore')

            context.set(output_var, decoded)

            return ActionResult(
                success=True,
                message=f"解码完成: {resolved_enc}",
                data={'decoded': decoded, 'encoding': resolved_enc, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"解码转换失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data', 'encoding']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'decoded_text'}


class EncodingDetectAction(BaseAction):
    """Detect encoding."""
    action_type = "encoding_detect"
    display_name = "检测编码"
    description = "检测字符编码"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute detect.

        Args:
            context: Execution context.
            params: Dict with data, output_var.

        Returns:
            ActionResult with detected encoding.
        """
        data = params.get('data', '')
        output_var = params.get('output_var', 'detected_encoding')

        valid, msg = self.validate_type(data, (str, bytes), 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)

            if isinstance(resolved_data, str):
                resolved_data = resolved_data.encode('utf-8')

            result = chardet.detect(resolved_data)
            encoding = result.get('encoding', 'unknown')
            confidence = result.get('confidence', 0)

            context.set(output_var, encoding)

            return ActionResult(
                success=True,
                message=f"检测到编码: {encoding} ({confidence:.0%})",
                data={'encoding': encoding, 'confidence': confidence, 'output_var': output_var}
            )
        except ImportError:
            return ActionResult(success=False, message="chardet未安装")
        except Exception as e:
            return ActionResult(success=False, message=f"编码检测失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'detected_encoding'}


class EncodingConvertAction(BaseAction):
    """Convert encoding."""
    action_type = "encoding_convert"
    display_name = "编码转换器"
    description = "转换字符编码"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute convert.

        Args:
            context: Execution context.
            params: Dict with data, from_encoding, to_encoding, output_var.

        Returns:
            ActionResult with converted string.
        """
        data = params.get('data', '')
        from_encoding = params.get('from_encoding', 'utf-8')
        to_encoding = params.get('to_encoding', 'utf-8')
        output_var = params.get('output_var', 'converted_text')

        valid, msg = self.validate_type(data, (str, bytes), 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_from = context.resolve_value(from_encoding)
            resolved_to = context.resolve_value(to_encoding)

            if isinstance(resolved_data, str):
                resolved_data = resolved_data.encode(resolved_from)

            decoded = resolved_data.decode(resolved_from, errors='ignore')
            converted = decoded.encode(resolved_to, errors='ignore').decode(resolved_to)

            context.set(output_var, converted)

            return ActionResult(
                success=True,
                message=f"编码转换: {resolved_from} -> {resolved_to}",
                data={'converted': converted, 'from': resolved_from, 'to': resolved_to, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"编码转换失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data', 'from_encoding', 'to_encoding']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'converted_text'}
