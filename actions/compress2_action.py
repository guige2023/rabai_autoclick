"""Compress2 action module for RabAI AutoClick.

Provides additional compression operations:
- CompressGZIPAction: GZIP compress
- DecompressGZIPAction: GZIP decompress
- CompressZLIBAction: ZLIB compress
- DecompressZLIBAction: ZLIB decompress
- CompressLZMAction: LZMA compress
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CompressGZIPAction(BaseAction):
    """GZIP compress."""
    action_type = "compress2_gzip"
    display_name = "GZIP压缩"
    description = "GZIP压缩数据"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute gzip compress.

        Args:
            context: Execution context.
            params: Dict with data, output_var.

        Returns:
            ActionResult with compressed data.
        """
        data = params.get('data', '')
        output_var = params.get('output_var', 'compressed_data')

        try:
            import gzip

            resolved = context.resolve_value(data)

            if isinstance(resolved, str):
                resolved = resolved.encode('utf-8')

            compressed = gzip.compress(resolved)

            context.set(output_var, compressed.hex())

            return ActionResult(
                success=True,
                message=f"GZIP压缩成功: {len(compressed)}字节",
                data={
                    'original_size': len(resolved),
                    'compressed_size': len(compressed),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"GZIP压缩失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compressed_data'}


class DecompressGZIPAction(BaseAction):
    """GZIP decompress."""
    action_type = "compress2_gunzip"
    display_name = "GZIP解压"
    description = "GZIP解压数据"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute gzip decompress.

        Args:
            context: Execution context.
            params: Dict with data, output_var.

        Returns:
            ActionResult with decompressed data.
        """
        data = params.get('data', '')
        output_var = params.get('output_var', 'decompressed_data')

        try:
            import gzip

            resolved = context.resolve_value(data)

            if isinstance(resolved, str):
                resolved = bytes.fromhex(resolved)

            decompressed = gzip.decompress(resolved)

            context.set(output_var, decompressed.decode('utf-8'))

            return ActionResult(
                success=True,
                message=f"GZIP解压成功: {len(decompressed)}字节",
                data={
                    'compressed_size': len(resolved),
                    'decompressed_size': len(decompressed),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"GZIP解压失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'decompressed_data'}


class CompressZLIBAction(BaseAction):
    """ZLIB compress."""
    action_type = "compress2_zlib"
    display_name = "ZLIB压缩"
    description = "ZLIB压缩数据"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute zlib compress.

        Args:
            context: Execution context.
            params: Dict with data, level, output_var.

        Returns:
            ActionResult with compressed data.
        """
        data = params.get('data', '')
        level = params.get('level', 6)
        output_var = params.get('output_var', 'compressed_data')

        try:
            import zlib

            resolved = context.resolve_value(data)
            resolved_level = int(context.resolve_value(level)) if level else 6

            if isinstance(resolved, str):
                resolved = resolved.encode('utf-8')

            compressed = zlib.compress(resolved, level=resolved_level)

            context.set(output_var, compressed.hex())

            return ActionResult(
                success=True,
                message=f"ZLIB压缩成功: {len(compressed)}字节",
                data={
                    'original_size': len(resolved),
                    'compressed_size': len(compressed),
                    'level': resolved_level,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"ZLIB压缩失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'level': 6, 'output_var': 'compressed_data'}


class DecompressZLIBAction(BaseAction):
    """ZLIB decompress."""
    action_type = "compress2_unzlib"
    display_name = "ZLIB解压"
    description = "ZLIB解压数据"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute zlib decompress.

        Args:
            context: Execution context.
            params: Dict with data, output_var.

        Returns:
            ActionResult with decompressed data.
        """
        data = params.get('data', '')
        output_var = params.get('output_var', 'decompressed_data')

        try:
            import zlib

            resolved = context.resolve_value(data)

            if isinstance(resolved, str):
                resolved = bytes.fromhex(resolved)

            decompressed = zlib.decompress(resolved)

            context.set(output_var, decompressed.decode('utf-8'))

            return ActionResult(
                success=True,
                message=f"ZLIB解压成功: {len(decompressed)}字节",
                data={
                    'compressed_size': len(resolved),
                    'decompressed_size': len(decompressed),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"ZLIB解压失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'decompressed_data'}


class CompressLZMAction(BaseAction):
    """LZMA compress."""
    action_type = "compress2_lzma"
    display_name = "LZMA压缩"
    description = "LZMA压缩数据"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute lzma compress.

        Args:
            context: Execution context.
            params: Dict with data, output_var.

        Returns:
            ActionResult with compressed data.
        """
        data = params.get('data', '')
        output_var = params.get('output_var', 'compressed_data')

        try:
            import lzma

            resolved = context.resolve_value(data)

            if isinstance(resolved, str):
                resolved = resolved.encode('utf-8')

            compressed = lzma.compress(resolved)

            context.set(output_var, compressed.hex())

            return ActionResult(
                success=True,
                message=f"LZMA压缩成功: {len(compressed)}字节",
                data={
                    'original_size': len(resolved),
                    'compressed_size': len(compressed),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"LZMA压缩失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compressed_data'}