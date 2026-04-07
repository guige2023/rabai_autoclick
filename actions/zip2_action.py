"""Zip2 action module for RabAI AutoClick.

Provides additional zip operations:
- ZipUnzipAction: Extract zip file
- ZipCreateZipAction: Create zip file
- ZipListContentsAction: List zip contents
- ZipAddToZipAction: Add file to zip
- ZipRemoveFromZipAction: Remove file from zip
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ZipUnzipAction(BaseAction):
    """Extract zip file."""
    action_type = "zip2_unzip"
    display_name = "解压文件"
    description = "解压ZIP文件"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute unzip.

        Args:
            context: Execution context.
            params: Dict with zip_path, extract_to, output_var.

        Returns:
            ActionResult with extraction status.
        """
        zip_path = params.get('zip_path', '')
        extract_to = params.get('extract_to', '')
        output_var = params.get('output_var', 'extraction_status')

        valid, msg = self.validate_type(zip_path, str, 'zip_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import zipfile

            resolved_zip = context.resolve_value(zip_path)
            resolved_dest = context.resolve_value(extract_to) if extract_to else os.path.dirname(resolved_zip)

            with zipfile.ZipFile(resolved_zip, 'r') as zip_ref:
                zip_ref.extractall(resolved_dest)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"解压成功: {resolved_zip}",
                data={
                    'zip_path': resolved_zip,
                    'extracted_to': resolved_dest,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解压失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['zip_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'extract_to': '', 'output_var': 'extraction_status'}


class ZipCreateZipAction(BaseAction):
    """Create zip file."""
    action_type = "zip2_create"
    display_name = "创建压缩文件"
    description = "创建ZIP压缩文件"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create zip.

        Args:
            context: Execution context.
            params: Dict with files, output_path, output_var.

        Returns:
            ActionResult with zip creation status.
        """
        files = params.get('files', [])
        output_path = params.get('output_path', '')
        output_var = params.get('output_var', 'zip_status')

        valid, msg = self.validate_type(output_path, str, 'output_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import zipfile

            resolved_files = context.resolve_value(files)
            resolved_output = context.resolve_value(output_path)

            if not isinstance(resolved_files, (list, tuple)):
                resolved_files = [resolved_files]

            with zipfile.ZipFile(resolved_output, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file in resolved_files:
                    if os.path.isfile(file):
                        zipf.write(file, os.path.basename(file))

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"创建压缩文件成功: {resolved_output}",
                data={
                    'files_count': len(resolved_files),
                    'output_path': resolved_output,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建压缩文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['files', 'output_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'zip_status'}


class ZipListContentsAction(BaseAction):
    """List zip contents."""
    action_type = "zip2_list"
    display_name = "列出压缩内容"
    description = "列出ZIP文件内容"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list contents.

        Args:
            context: Execution context.
            params: Dict with zip_path, output_var.

        Returns:
            ActionResult with list of contents.
        """
        zip_path = params.get('zip_path', '')
        output_var = params.get('output_var', 'zip_contents')

        valid, msg = self.validate_type(zip_path, str, 'zip_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import zipfile

            resolved_zip = context.resolve_value(zip_path)

            with zipfile.ZipFile(resolved_zip, 'r') as zipf:
                contents = zipf.namelist()

            context.set(output_var, contents)

            return ActionResult(
                success=True,
                message=f"ZIP内容: {len(contents)}个文件",
                data={
                    'zip_path': resolved_zip,
                    'contents': contents,
                    'count': len(contents),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列出压缩内容失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['zip_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'zip_contents'}


class ZipAddToZipAction(BaseAction):
    """Add file to zip."""
    action_type = "zip2_add"
    display_name = "添加文件到压缩"
    description = "添加文件到ZIP压缩文件"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute add to zip.

        Args:
            context: Execution context.
            params: Dict with zip_path, file_path, arcname, output_var.

        Returns:
            ActionResult with add status.
        """
        zip_path = params.get('zip_path', '')
        file_path = params.get('file_path', '')
        arcname = params.get('arcname', '')
        output_var = params.get('output_var', 'add_status')

        valid, msg = self.validate_type(zip_path, str, 'zip_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import zipfile

            resolved_zip = context.resolve_value(zip_path)
            resolved_file = context.resolve_value(file_path)
            resolved_arcname = context.resolve_value(arcname) if arcname else os.path.basename(resolved_file)

            with zipfile.ZipFile(resolved_zip, 'a', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(resolved_file, resolved_arcname)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"添加文件到压缩成功",
                data={
                    'zip_path': resolved_zip,
                    'file_path': resolved_file,
                    'arcname': resolved_arcname,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"添加文件到压缩失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['zip_path', 'file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'arcname': '', 'output_var': 'add_status'}


class ZipRemoveFromZipAction(BaseAction):
    """Remove file from zip."""
    action_type = "zip2_remove"
    display_name = "从压缩删除文件"
    description = "从ZIP压缩文件删除文件"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute remove from zip.

        Args:
            context: Execution context.
            params: Dict with zip_path, filename, output_var.

        Returns:
            ActionResult with remove status.
        """
        zip_path = params.get('zip_path', '')
        filename = params.get('filename', '')
        output_var = params.get('output_var', 'remove_status')

        valid, msg = self.validate_type(zip_path, str, 'zip_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import zipfile
            import tempfile
            import shutil

            resolved_zip = context.resolve_value(zip_path)
            resolved_filename = context.resolve_value(filename)

            temp_dir = tempfile.mkdtemp()
            temp_zip = os.path.join(temp_dir, 'temp.zip')

            with zipfile.ZipFile(resolved_zip, 'r') as zipf:
                with zipfile.ZipFile(temp_zip, 'w', zipfile.ZIP_DEFLATED) as new_zipf:
                    for item in zipf.namelist():
                        if item != resolved_filename:
                            new_zipf.writestr(zipf.getinfo(item), zipf.read(item))

            shutil.move(temp_zip, resolved_zip)
            shutil.rmtree(temp_dir)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"从压缩删除文件成功",
                data={
                    'zip_path': resolved_zip,
                    'removed': resolved_filename,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"从压缩删除文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['zip_path', 'filename']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'remove_status'}