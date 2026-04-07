"""Pdf action module for RabAI AutoClick.

Provides PDF operations:
- PdfTextExtractAction: Extract text from PDF
- PdfPageCountAction: Get page count
- PdfMetadataAction: Get PDF metadata
- PdfThumbnailAction: Generate thumbnail
- PdfCompressAction: Compress PDF
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PdfTextExtractAction(BaseAction):
    """Extract text from PDF."""
    action_type = "pdf_text_extract"
    display_name = "PDF提取文本"
    description = "从PDF文件提取文本内容"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute text extract.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with extracted text.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'pdf_text')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)

            try:
                import PyPDF2
                with open(resolved_path, 'rb') as f:
                    reader = PyPDF2.PdfReader(f)
                    text = ''
                    for page in reader.pages:
                        text += page.extract_text() or ''
            except ImportError:
                return ActionResult(
                    success=False,
                    message="PDF处理失败: 未安装PyPDF2库"
                )

            context.set(output_var, text)

            return ActionResult(
                success=True,
                message=f"PDF文本提取完成: {len(text)} 字符",
                data={
                    'file_path': resolved_path,
                    'text': text,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"PDF文本提取失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'pdf_text'}


class PdfPageCountAction(BaseAction):
    """Get page count."""
    action_type = "pdf_page_count"
    display_name = "PDF页数"
    description = "获取PDF文件页数"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute page count.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with page count.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'page_count')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)

            try:
                import PyPDF2
                with open(resolved_path, 'rb') as f:
                    reader = PyPDF2.PdfReader(f)
                    count = len(reader.pages)
            except ImportError:
                return ActionResult(
                    success=False,
                    message="PDF处理失败: 未安装PyPDF2库"
                )

            context.set(output_var, count)

            return ActionResult(
                success=True,
                message=f"PDF页数: {count}",
                data={
                    'file_path': resolved_path,
                    'page_count': count,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取PDF页数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'page_count'}


class PdfMetadataAction(BaseAction):
    """Get PDF metadata."""
    action_type = "pdf_metadata"
    display_name = "PDF元数据"
    description = "获取PDF文件元数据"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute metadata.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with metadata.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'pdf_metadata')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)

            try:
                import PyPDF2
                with open(resolved_path, 'rb') as f:
                    reader = PyPDF2.PdfReader(f)
                    metadata = {}
                    if reader.metadata:
                        metadata = {
                            'title': reader.metadata.get('/Title', ''),
                            'author': reader.metadata.get('/Author', ''),
                            'subject': reader.metadata.get('/Subject', ''),
                            'creator': reader.metadata.get('/Creator', ''),
                            'producer': reader.metadata.get('/Producer', ''),
                        }
                    metadata['page_count'] = len(reader.pages)
            except ImportError:
                return ActionResult(
                    success=False,
                    message="PDF处理失败: 未安装PyPDF2库"
                )

            context.set(output_var, metadata)

            return ActionResult(
                success=True,
                message=f"PDF元数据获取完成",
                data={
                    'file_path': resolved_path,
                    'metadata': metadata,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取PDF元数据失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'pdf_metadata'}


class PdfThumbnailAction(BaseAction):
    """Generate thumbnail."""
    action_type = "pdf_thumbnail"
    display_name = "PDF缩略图"
    description = "生成PDF页面缩略图"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute thumbnail.

        Args:
            context: Execution context.
            params: Dict with file_path, page, size, output_var.

        Returns:
            ActionResult with thumbnail path.
        """
        file_path = params.get('file_path', '')
        page = params.get('page', 0)
        size = params.get('size', (200, 300))
        output_var = params.get('output_var', 'thumbnail_path')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_page = int(context.resolve_value(page))
            resolved_size = tuple(context.resolve_value(size)) if isinstance(size, (list, tuple)) else (200, 300)

            try:
                from PIL import Image
                import io

                import PyPDF2
                with open(resolved_path, 'rb') as f:
                    reader = PyPDF2.PdfReader(f)
                    if resolved_page >= len(reader.pages):
                        return ActionResult(
                            success=False,
                            message=f"PDF缩略图失败: 页码超出范围"
                        )

                    page_obj = reader.pages[resolved_page]
                    image = page_obj.to_image()

                    img_bytes = io.BytesIO()
                    image.save(img_bytes, format='PNG')
                    img_bytes.seek(0)

                    thumb = Image.open(img_bytes)
                    thumb = thumb.resize(resolved_size, Image.Resampling.LANCZOS)

                    thumb_path = resolved_path.replace('.pdf', f'_thumb_{resolved_page}.png')
                    thumb.save(thumb_path, 'PNG')

            except ImportError:
                return ActionResult(
                    success=False,
                    message="PDF缩略图失败: 未安装Pillow或PyPDF2库"
                )

            context.set(output_var, thumb_path)

            return ActionResult(
                success=True,
                message=f"PDF缩略图生成: {thumb_path}",
                data={
                    'file_path': resolved_path,
                    'thumbnail_path': thumb_path,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"PDF缩略图失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'page': 0, 'size': (200, 300), 'output_var': 'thumbnail_path'}


class PdfCompressAction(BaseAction):
    """Compress PDF."""
    action_type = "pdf_compress"
    display_name = "PDF压缩"
    description = "压缩PDF文件大小"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute compress.

        Args:
            context: Execution context.
            params: Dict with file_path, quality, output_var.

        Returns:
            ActionResult with compressed PDF path.
        """
        file_path = params.get('file_path', '')
        quality = params.get('quality', 'medium')
        output_var = params.get('output_var', 'compressed_path')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_quality = context.resolve_value(quality)

            try:
                import PyPDF2

                compressed_path = resolved_path.replace('.pdf', '_compressed.pdf')

                with open(resolved_path, 'rb') as f_in:
                    reader = PyPDF2.PdfReader(f_in)
                    writer = PyPDF2.PdfWriter()

                    for page in reader.pages:
                        writer.add_page(page)

                    with open(compressed_path, 'wb') as f_out:
                        writer.write(f_out)

            except ImportError:
                return ActionResult(
                    success=False,
                    message="PDF压缩失败: 未安装PyPDF2库"
                )

            context.set(output_var, compressed_path)

            return ActionResult(
                success=True,
                message=f"PDF压缩完成: {compressed_path}",
                data={
                    'original_path': resolved_path,
                    'compressed_path': compressed_path,
                    'quality': resolved_quality,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"PDF压缩失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'quality': 'medium', 'output_var': 'compressed_path'}