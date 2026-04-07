"""Archive2 action module for RabAI AutoClick.

Provides additional archive operations:
- ArchiveCreateZipAction: Create ZIP archive
- ArchiveExtractZipAction: Extract ZIP archive
- ArchiveListZipAction: List ZIP contents
- ArchiveCreateTarAction: Create TAR archive
"""

import zipfile
import tarfile
import os
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ArchiveCreateZipAction(BaseAction):
    """Create ZIP archive."""
    action_type = "archive2_create_zip"
    display_name = "创建ZIP"
    description = "创建ZIP压缩包"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create ZIP.

        Args:
            context: Execution context.
            params: Dict with files, output_path, output_var.

        Returns:
            ActionResult with create result.
        """
        files = params.get('files', [])
        output_path = params.get('output_path', '')
        output_var = params.get('output_var', 'archive_result')

        valid, msg = self.validate_type(output_path, str, 'output_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_files = context.resolve_value(files)
            resolved_output = context.resolve_value(output_path)

            if not isinstance(resolved_files, list):
                return ActionResult(
                    success=False,
                    message="files 必须是列表"
                )

            with zipfile.ZipFile(resolved_output, 'w', zipfile.ZIP_DEFLATED) as zf:
                for file_path in resolved_files:
                    if os.path.isfile(file_path):
                        zf.write(file_path, os.path.basename(file_path))

            context.set(output_var, resolved_output)

            return ActionResult(
                success=True,
                message=f"ZIP创建成功: {resolved_output}",
                data={
                    'output_path': resolved_output,
                    'files_count': len(resolved_files),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建ZIP失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['files', 'output_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'archive_result'}


class ArchiveExtractZipAction(BaseAction):
    """Extract ZIP archive."""
    action_type = "archive2_extract_zip"
    display_name = "解压ZIP"
    description = "解压ZIP压缩包"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute extract ZIP.

        Args:
            context: Execution context.
            params: Dict with archive_path, output_dir, output_var.

        Returns:
            ActionResult with extract result.
        """
        archive_path = params.get('archive_path', '')
        output_dir = params.get('output_dir', '')
        output_var = params.get('output_var', 'extract_result')

        valid, msg = self.validate_type(archive_path, str, 'archive_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_archive = context.resolve_value(archive_path)
            resolved_output = context.resolve_value(output_dir) if output_dir else os.path.dirname(resolved_archive)

            with zipfile.ZipFile(resolved_archive, 'r') as zf:
                zf.extractall(resolved_output)
                extracted_files = zf.namelist()

            context.set(output_var, extracted_files)

            return ActionResult(
                success=True,
                message=f"ZIP解压成功: {len(extracted_files)} 个文件",
                data={
                    'archive_path': resolved_archive,
                    'output_dir': resolved_output,
                    'extracted_files': extracted_files,
                    'count': len(extracted_files),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解压ZIP失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['archive_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_dir': '', 'output_var': 'extract_result'}


class ArchiveListZipAction(BaseAction):
    """List ZIP contents."""
    action_type = "archive2_list_zip"
    display_name = "列出ZIP内容"
    description = "列出ZIP压缩包内容"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list ZIP.

        Args:
            context: Execution context.
            params: Dict with archive_path, output_var.

        Returns:
            ActionResult with file list.
        """
        archive_path = params.get('archive_path', '')
        output_var = params.get('output_var', 'zip_contents')

        valid, msg = self.validate_type(archive_path, str, 'archive_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_archive = context.resolve_value(archive_path)

            with zipfile.ZipFile(resolved_archive, 'r') as zf:
                files = zf.namelist()

            context.set(output_var, files)

            return ActionResult(
                success=True,
                message=f"ZIP内容: {len(files)} 个文件",
                data={
                    'archive_path': resolved_archive,
                    'files': files,
                    'count': len(files),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列出ZIP内容失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['archive_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'zip_contents'}


class ArchiveCreateTarAction(BaseAction):
    """Create TAR archive."""
    action_type = "archive2_create_tar"
    display_name = "创建TAR"
    description = "创建TAR压缩包"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create TAR.

        Args:
            context: Execution context.
            params: Dict with files, output_path, compress_type, output_var.

        Returns:
            ActionResult with create result.
        """
        files = params.get('files', [])
        output_path = params.get('output_path', '')
        compress_type = params.get('compress_type', 'gz')
        output_var = params.get('output_var', 'archive_result')

        valid, msg = self.validate_type(output_path, str, 'output_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_files = context.resolve_value(files)
            resolved_output = context.resolve_value(output_path)
            resolved_compress = context.resolve_value(compress_type)

            mode = 'w:' + resolved_compress if resolved_compress else 'w'

            with tarfile.open(resolved_output, mode) as tf:
                for file_path in resolved_files:
                    if os.path.isfile(file_path):
                        tf.add(file_path, arcname=os.path.basename(file_path))

            context.set(output_var, resolved_output)

            return ActionResult(
                success=True,
                message=f"TAR创建成功: {resolved_output}",
                data={
                    'output_path': resolved_output,
                    'compress_type': resolved_compress,
                    'files_count': len(resolved_files),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建TAR失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['files', 'output_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'compress_type': 'gz', 'output_var': 'archive_result'}