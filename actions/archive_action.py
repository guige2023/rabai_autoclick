"""Archive action module for RabAI AutoClick.

Provides archive/compression operations:
- ArchiveZipAction: Create zip archive
- ArchiveUnzipAction: Extract zip archive
- ArchiveListAction: List archive contents
"""

import zipfile
import tarfile
import os
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ArchiveZipAction(BaseAction):
    """Create zip archive."""
    action_type = "archive_zip"
    display_name = "创建ZIP压缩包"
    description = "创建ZIP压缩包"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute creating zip archive.

        Args:
            context: Execution context.
            params: Dict with source_path, output_path.

        Returns:
            ActionResult indicating success.
        """
        source_path = params.get('source_path', '')
        output_path = params.get('output_path', '')

        valid, msg = self.validate_type(source_path, str, 'source_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(output_path, str, 'output_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_source = context.resolve_value(source_path)
            resolved_output = context.resolve_value(output_path)

            if not os.path.exists(resolved_source):
                return ActionResult(
                    success=False,
                    message=f"源路径不存在: {resolved_source}"
                )

            with zipfile.ZipFile(resolved_output, 'w', zipfile.ZIP_DEFLATED) as zipf:
                if os.path.isfile(resolved_source):
                    zipf.write(resolved_source, os.path.basename(resolved_source))
                else:
                    for root, dirs, files in os.walk(resolved_source):
                        for file in files:
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, os.path.dirname(resolved_source))
                            zipf.write(file_path, arcname)

            return ActionResult(
                success=True,
                message=f"ZIP压缩包已创建: {resolved_output}",
                data={
                    'output_path': resolved_output,
                    'source_path': resolved_source
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建ZIP失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['source_path', 'output_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class ArchiveUnzipAction(BaseAction):
    """Extract zip archive."""
    action_type = "archive_unzip"
    display_name = "解压ZIP文件"
    description = "解压ZIP压缩包"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute extracting zip archive.

        Args:
            context: Execution context.
            params: Dict with archive_path, output_dir.

        Returns:
            ActionResult indicating success.
        """
        archive_path = params.get('archive_path', '')
        output_dir = params.get('output_dir', '')

        valid, msg = self.validate_type(archive_path, str, 'archive_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(output_dir, str, 'output_dir')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_archive = context.resolve_value(archive_path)
            resolved_output = context.resolve_value(output_dir)

            if not os.path.exists(resolved_archive):
                return ActionResult(
                    success=False,
                    message=f"压缩包不存在: {resolved_archive}"
                )

            if not os.path.exists(resolved_output):
                os.makedirs(resolved_output)

            with zipfile.ZipFile(resolved_archive, 'r') as zipf:
                zipf.extractall(resolved_output)
                extracted_files = zipf.namelist()

            return ActionResult(
                success=True,
                message=f"解压成功: {len(extracted_files)} 个文件",
                data={
                    'output_dir': resolved_output,
                    'extracted_count': len(extracted_files)
                }
            )
        except zipfile.BadZipFile:
            return ActionResult(
                success=False,
                message="无效的ZIP文件"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解压失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['archive_path', 'output_dir']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class ArchiveListAction(BaseAction):
    """List archive contents."""
    action_type = "archive_list"
    display_name = "列出压缩包内容"
    description = "列出压缩包内的文件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute listing archive contents.

        Args:
            context: Execution context.
            params: Dict with archive_path, output_var.

        Returns:
            ActionResult with file list.
        """
        archive_path = params.get('archive_path', '')
        output_var = params.get('output_var', 'archive_contents')

        valid, msg = self.validate_type(archive_path, str, 'archive_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_archive = context.resolve_value(archive_path)

            if not os.path.exists(resolved_archive):
                return ActionResult(
                    success=False,
                    message=f"压缩包不存在: {resolved_archive}"
                )

            with zipfile.ZipFile(resolved_archive, 'r') as zipf:
                file_list = zipf.namelist()

            context.set(output_var, file_list)

            return ActionResult(
                success=True,
                message=f"压缩包包含 {len(file_list)} 个文件",
                data={
                    'files': file_list,
                    'count': len(file_list),
                    'output_var': output_var
                }
            )
        except zipfile.BadZipFile:
            return ActionResult(
                success=False,
                message="无效的ZIP文件"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列出内容失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['archive_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'archive_contents'}


class ArchiveTarAction(BaseAction):
    """Create tar archive."""
    action_type = "archive_tar"
    display_name = "创建TAR压缩包"
    description = "创建TAR压缩包"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute creating tar archive.

        Args:
            context: Execution context.
            params: Dict with source_path, output_path, mode.

        Returns:
            ActionResult indicating success.
        """
        source_path = params.get('source_path', '')
        output_path = params.get('output_path', '')
        mode = params.get('mode', 'w:gz')

        valid, msg = self.validate_type(source_path, str, 'source_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(output_path, str, 'output_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_source = context.resolve_value(source_path)
            resolved_output = context.resolve_value(output_path)

            if not os.path.exists(resolved_source):
                return ActionResult(
                    success=False,
                    message=f"源路径不存在: {resolved_source}"
                )

            with tarfile.open(resolved_output, mode) as tar:
                tar.add(resolved_source, arcname=os.path.basename(resolved_source))

            return ActionResult(
                success=True,
                message=f"TAR压缩包已创建: {resolved_output}",
                data={
                    'output_path': resolved_output,
                    'source_path': resolved_source
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建TAR失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['source_path', 'output_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'mode': 'w:gz'}


class ArchiveUntarAction(BaseAction):
    """Extract tar archive."""
    action_type = "archive_untar"
    display_name = "解压TAR文件"
    description = "解压TAR压缩包"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute extracting tar archive.

        Args:
            context: Execution context.
            params: Dict with archive_path, output_dir.

        Returns:
            ActionResult indicating success.
        """
        archive_path = params.get('archive_path', '')
        output_dir = params.get('output_dir', '')

        valid, msg = self.validate_type(archive_path, str, 'archive_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(output_dir, str, 'output_dir')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_archive = context.resolve_value(archive_path)
            resolved_output = context.resolve_value(output_dir)

            if not os.path.exists(resolved_archive):
                return ActionResult(
                    success=False,
                    message=f"压缩包不存在: {resolved_archive}"
                )

            if not os.path.exists(resolved_output):
                os.makedirs(resolved_output)

            with tarfile.open(resolved_archive, 'r:*') as tar:
                tar.extractall(resolved_output)
                extracted_files = tar.getnames()

            return ActionResult(
                success=True,
                message=f"解压成功: {len(extracted_files)} 个文件",
                data={
                    'output_dir': resolved_output,
                    'extracted_count': len(extracted_files)
                }
            )
        except tarfile.TarError as e:
            return ActionResult(
                success=False,
                message=f"无效的TAR文件: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解压失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['archive_path', 'output_dir']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}