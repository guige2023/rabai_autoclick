"""Compress action module for RabAI AutoClick.

Provides compression operations:
- CompressListAction: Compress list using conditions
- CompressRunLengthAction: Run-length encoding
- CompressDeltaAction: Delta encoding
- DecompressRunLengthAction: Run-length decoding
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CompressListAction(BaseAction):
    """Compress list using conditions."""
    action_type = "compress_list"
    display_name = "压缩列表"
    description = "根据条件压缩列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute compress.

        Args:
            context: Execution context.
            params: Dict with list_var, condition, output_var.

        Returns:
            ActionResult with compressed list.
        """
        list_var = params.get('list_var', '')
        condition = params.get('condition', 'lambda x: x')
        output_var = params.get('output_var', 'compressed_list')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)
            resolved_cond = context.resolve_value(condition)

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是列表"
                )

            cond_fn = context.safe_exec(f"return_value = {resolved_cond}")
            result = [item for item in items if cond_fn(item)]
            context.set(output_var, result)

            compression_ratio = len(result) / len(items) if items else 1

            return ActionResult(
                success=True,
                message=f"压缩完成: {len(items)} -> {len(result)} ({compression_ratio:.2%})",
                data={
                    'original_count': len(items),
                    'compressed_count': len(result),
                    'compression_ratio': compression_ratio,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"压缩列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'condition': 'lambda x: x', 'output_var': 'compressed_list'}


class CompressRunLengthAction(BaseAction):
    """Run-length encoding."""
    action_type = "compress_rle"
    display_name = "游程编码"
    description = "游程编码压缩"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute RLE.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with RLE encoded list.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'rle_encoded')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                items = list(str(items))

            if len(items) == 0:
                context.set(output_var, [])
                return ActionResult(
                    success=True,
                    message="空列表",
                    data={'count': 0, 'output_var': output_var}
                )

            result = []
            current_item = items[0]
            current_count = 1

            for item in items[1:]:
                if item == current_item:
                    current_count += 1
                else:
                    result.append((current_item, current_count))
                    current_item = item
                    current_count = 1
            result.append((current_item, current_count))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"游程编码完成: {len(items)} -> {len(result)}",
                data={
                    'original_count': len(items),
                    'encoded_count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"游程编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'rle_encoded'}


class CompressDeltaAction(BaseAction):
    """Delta encoding."""
    action_type = "compress_delta"
    display_name = "增量编码"
    description = "增量编码压缩"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute delta.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with delta encoded list.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'delta_encoded')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                items = list(items)

            if len(items) == 0:
                context.set(output_var, [])
                return ActionResult(
                    success=True,
                    message="空列表",
                    data={'count': 0, 'output_var': output_var}
                )

            result = [items[0]]
            for i in range(1, len(items)):
                result.append(items[i] - items[i - 1])

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"增量编码完成: {len(items)} 项",
                data={
                    'original_count': len(items),
                    'encoded_count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"增量编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'delta_encoded'}


class DecompressRunLengthAction(BaseAction):
    """Run-length decoding."""
    action_type = "decompress_rle"
    display_name = "游程解码"
    description = "游程编码解码"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute RLE decode.

        Args:
            context: Execution context.
            params: Dict with encoded_var, output_var.

        Returns:
            ActionResult with decoded list.
        """
        encoded_var = params.get('encoded_var', '')
        output_var = params.get('output_var', 'rle_decoded')

        valid, msg = self.validate_type(encoded_var, str, 'encoded_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(encoded_var)

            encoded = context.get(resolved_var)
            if not isinstance(encoded, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是列表"
                )

            result = []
            for item, count in encoded:
                result.extend([item] * count)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"游程解码完成: {len(encoded)} -> {len(result)}",
                data={
                    'encoded_count': len(encoded),
                    'decoded_count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"游程解码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['encoded_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'rle_decoded'}
