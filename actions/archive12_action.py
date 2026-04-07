"""Archive12 action module for RabAI AutoClick.

Provides additional archive operations:
- ArchiveZipAction: Create zip archive
- ArchiveUnzipAction: Extract zip archive
- ArchiveTarAction: Create tar archive
- ArchiveuntarAction: Extract tar archive
- ArchiveListAction: List archive contents
- ArchiveIsArchiveAction: Check if file is archive
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ArchiveZipAction(BaseAction):
    """Create zip archive."""
    action_type = "archive12_zip"
    display_name = "创建ZIP压缩包"
    description = "创建ZIP压缩包"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute zip.

        Args:
            context: Execution context.
            params: Dict with source, destination, output_var.

        Returns:
            ActionResult with zip status.
        """
        source = params.get('source', '')
        destination = params.get('destination', '')
        output_var = params.get('output_var', 'zip_status')

        try:
            import zipfile
            import os

            resolved_source = context.resolve_value(source)
            resolved_dest = context.resolve_value(destination)

            with zipfile.ZipFile(resolved_dest, 'w', zipfile.ZIP_DEFLATED) as zipf:
                if os.path.isdir(resolved_source):
                    for root, dirs, files in os.walk(resolved_source):
                        for file in files:
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, os.path.dirname(resolved_source))
                            zipf.write(file_path, arcname)
                else:
                    zipf.write(resolved_source, os.path.basename(resolved_source))

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"创建ZIP: {resolved_dest}",
                data={
                    'source': resolved_source,
                    'destination': resolved_dest,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建ZIP失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['source', 'destination']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'zip_status'}


class ArchiveUnzipAction(BaseAction):
    """Extract zip archive."""
    action_type = "archive12_unzip"
    display_name = "解压ZIP压缩包"
    description = "解压ZIP压缩包"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute unzip.

        Args:
            context: Execution context.
            params: Dict with source, destination, output_var.

        Returns:
            ActionResult with unzip status.
        """
        source = params.get('source', '')
        destination = params.get('destination', '')
        output_var = params.get('output_var', 'unzip_status')

        try:
            import zipfile
            import os

            resolved_source = context.resolve_value(source)
            resolved_dest = context.resolve_value(destination) if destination else os.path.dirname(resolved_source)

            with zipfile.ZipFile(resolved_source, 'r') as zipf:
                zipf.extractall(resolved_dest)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"解压ZIP: {resolved_source}",
                data={
                    'source': resolved_source,
                    'destination': resolved_dest,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解压ZIP失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['source']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'destination': '', 'output_var': 'unzip_status'}


class ArchiveTarAction(BaseAction):
    """Create tar archive."""
    action_type = "archive12_tar"
    display_name = "创建TAR压缩包"
    description = "创建TAR压缩包"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute tar.

        Args:
            context: Execution context.
            params: Dict with source, destination, mode, output_var.

        Returns:
            ActionResult with tar status.
        """
        source = params.get('source', '')
        destination = params.get('destination', '')
        mode = params.get('mode', 'w')
        output_var = params.get('output_var', 'tar_status')

        try:
            import tarfile
            import os

            resolved_source = context.resolve_value(source)
            resolved_dest = context.resolve_value(destination)
            resolved_mode = context.resolve_value(mode) if mode else 'w'

            with tarfile.open(resolved_dest, f'{resolved_mode}:gz') as tar:
                tar.add(resolved_source, arcname=os.path.basename(resolved_source))

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"创建TAR: {resolved_dest}",
                data={
                    'source': resolved_source,
                    'destination': resolved_dest,
                    'mode': resolved_mode,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建TAR失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['source', 'destination']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'mode': 'w', 'output_var': 'tar_status'}


class ArchiveUntarAction(BaseAction):
    """Extract tar archive."""
    action_type = "archive12_untar"
    display_name = "解压TAR压缩包"
    description = "解压TAR压缩包"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute untar.

        Args:
            context: Execution context.
            params: Dict with source, destination, output_var.

        Returns:
            ActionResult with untar status.
        """
        source = params.get('source', '')
        destination = params.get('destination', '')
        output_var = params.get('output_var', 'untar_status')

        try:
            import tarfile
            import os

            resolved_source = context.resolve_value(source)
            resolved_dest = context.resolve_value(destination) if destination else os.path.dirname(resolved_source)

            with tarfile.open(resolved_source, 'r:*') as tar:
                tar.extractall(resolved_dest)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"解压TAR: {resolved_source}",
                data={
                    'source': resolved_source,
                    'destination': resolved_dest,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解压TAR失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['source']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'destination': '', 'output_var': 'untar_status'}


class ArchiveListAction(BaseAction):
    """List archive contents."""
    action_type = "archive12_list"
    display_name = "列出压缩包内容"
    description = "列出压缩包内容"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list.

        Args:
            context: Execution context.
            params: Dict with archive, output_var.

        Returns:
            ActionResult with archive contents.
        """
        archive = params.get('archive', '')
        output_var = params.get('output_var', 'archive_list')

        try:
            import zipfile
            import tarfile
            import os

            resolved_archive = context.resolve_value(archive)

            ext = os.path.splitext(resolved_archive)[1].lower()

            if ext == '.zip':
                with zipfile.ZipFile(resolved_archive, 'r') as zipf:
                    result = zipf.namelist()
            elif ext in ['.tar', '.gz', '.tgz', '.tar.gz']:
                with tarfile.open(resolved_archive, 'r:*') as tar:
                    result = tar.getnames()
            else:
                return ActionResult(
                    success=False,
                    message=f"不支持的压缩格式: {ext}"
                )

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列出内容: {len(result)}个文件",
                data={
                    'archive': resolved_archive,
                    'files': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列出内容失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['archive']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'archive_list'}


class ArchiveIsArchiveAction(BaseAction):
    """Check if file is archive."""
    action_type = "archive12_is_archive"
    display_name = "检查压缩包"
    description = "检查是否为压缩包"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is archive.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with is archive result.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'is_archive')

        try:
            import zipfile
            import tarfile
            import os

            resolved_path = context.resolve_value(path)

            ext = os.path.splitext(resolved_path)[1].lower()
            is_archive = ext in ['.zip', '.tar', '.gz', '.tgz', '.tar.gz', '.tar.bz2']

            context.set(output_var, is_archive)

            return ActionResult(
                success=True,
                message=f"是压缩包: {'是' if is_archive else '否'}",
                data={
                    'path': resolved_path,
                    'is_archive': is_archive,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查压缩包失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_archive'}