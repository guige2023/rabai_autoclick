"""Archive3 action module for RabAI AutoClick.

Provides additional archive operations:
- ArchiveZipAction: Create ZIP archive
- ArchiveUnzipAction: Extract ZIP archive
- ArchiveTarAction: Create TAR archive
- ArchiveUntarAction: Extract TAR archive
- ArchiveListAction: List archive contents
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


class ArchiveZipAction(BaseAction):
    """Create ZIP archive."""
    action_type = "archive3_zip"
    display_name = "创建ZIP压缩包"
    description = "创建ZIP压缩包"
    version = "3.0"

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
            ActionResult with archive status.
        """
        source = params.get('source', '')
        destination = params.get('destination', '')
        output_var = params.get('output_var', 'zip_status')

        valid, msg = self.validate_type(source, str, 'source')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(destination, str, 'destination')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_source = context.resolve_value(source)
            resolved_dest = context.resolve_value(destination)

            with zipfile.ZipFile(resolved_dest, 'w', zipfile.ZIP_DEFLATED) as zipf:
                if os.path.isfile(resolved_source):
                    zipf.write(resolved_source, os.path.basename(resolved_source))
                else:
                    for root, dirs, files in os.walk(resolved_source):
                        for file in files:
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, os.path.dirname(resolved_source))
                            zipf.write(file_path, arcname)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"ZIP压缩包创建完成: {resolved_dest}",
                data={
                    'source': resolved_source,
                    'destination': resolved_dest,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建ZIP压缩包失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['source', 'destination']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'zip_status'}


class ArchiveUnzipAction(BaseAction):
    """Extract ZIP archive."""
    action_type = "archive3_unzip"
    display_name = "解压ZIP文件"
    description = "解压ZIP压缩包"
    version = "3.0"

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
            ActionResult with extract status.
        """
        source = params.get('source', '')
        destination = params.get('destination', '')
        output_var = params.get('output_var', 'unzip_status')

        valid, msg = self.validate_type(source, str, 'source')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_source = context.resolve_value(source)
            resolved_dest = context.resolve_value(destination) if destination else os.path.dirname(resolved_source)

            with zipfile.ZipFile(resolved_source, 'r') as zipf:
                zipf.extractall(resolved_dest)
                extracted_files = zipf.namelist()

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"ZIP文件解压完成: {len(extracted_files)} 个文件",
                data={
                    'source': resolved_source,
                    'destination': resolved_dest,
                    'extracted_count': len(extracted_files),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解压ZIP文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['source']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'destination': '', 'output_var': 'unzip_status'}


class ArchiveTarAction(BaseAction):
    """Create TAR archive."""
    action_type = "archive3_tar"
    display_name = "创建TAR压缩包"
    description = "创建TAR压缩包"
    version = "3.0"

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
            ActionResult with archive status.
        """
        source = params.get('source', '')
        destination = params.get('destination', '')
        mode = params.get('mode', 'w')
        output_var = params.get('output_var', 'tar_status')

        valid, msg = self.validate_type(source, str, 'source')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(destination, str, 'destination')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_source = context.resolve_value(source)
            resolved_dest = context.resolve_value(destination)
            resolved_mode = context.resolve_value(mode) if mode else 'w'

            with tarfile.open(resolved_dest, resolved_mode) as tarf:
                if os.path.isfile(resolved_source):
                    tarf.add(resolved_source, arcname=os.path.basename(resolved_source))
                else:
                    tarf.add(resolved_source, arcname=os.path.basename(resolved_source))

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"TAR压缩包创建完成: {resolved_dest}",
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
                message=f"创建TAR压缩包失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['source', 'destination']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'mode': 'w', 'output_var': 'tar_status'}


class ArchiveUntarAction(BaseAction):
    """Extract TAR archive."""
    action_type = "archive3_untargz"
    display_name = "解压TAR文件"
    description = "解压TAR压缩包"
    version = "3.0"

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
            ActionResult with extract status.
        """
        source = params.get('source', '')
        destination = params.get('destination', '')
        output_var = params.get('output_var', 'untar_status')

        valid, msg = self.validate_type(source, str, 'source')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_source = context.resolve_value(source)
            resolved_dest = context.resolve_value(destination) if destination else os.path.dirname(resolved_source)

            with tarfile.open(resolved_source, 'r:*') as tarf:
                tarf.extractall(resolved_dest)
                extracted_members = tarf.getnames()

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"TAR文件解压完成: {len(extracted_members)} 个文件",
                data={
                    'source': resolved_source,
                    'destination': resolved_dest,
                    'extracted_count': len(extracted_members),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解压TAR文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['source']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'destination': '', 'output_var': 'untar_status'}


class ArchiveListAction(BaseAction):
    """List archive contents."""
    action_type = "archive3_list"
    display_name = "列出压缩包内容"
    description = "列出压缩包内的文件"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list.

        Args:
            context: Execution context.
            params: Dict with archive_path, output_var.

        Returns:
            ActionResult with archive contents.
        """
        archive_path = params.get('archive_path', '')
        output_var = params.get('output_var', 'archive_list')

        valid, msg = self.validate_type(archive_path, str, 'archive_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(archive_path)

            if resolved_path.endswith('.zip'):
                with zipfile.ZipFile(resolved_path, 'r') as zipf:
                    contents = zipf.namelist()
            elif resolved_path.endswith(('.tar', '.tar.gz', '.tgz', '.tar.bz2')):
                with tarfile.open(resolved_path, 'r:*') as tarf:
                    contents = tarf.getnames()
            else:
                return ActionResult(
                    success=False,
                    message=f"列出压缩包内容失败: 不支持的格式"
                )

            context.set(output_var, contents)

            return ActionResult(
                success=True,
                message=f"列出压缩包内容: {len(contents)} 个文件",
                data={
                    'archive_path': resolved_path,
                    'contents': contents,
                    'count': len(contents),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列出压缩包内容失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['archive_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'archive_list'}