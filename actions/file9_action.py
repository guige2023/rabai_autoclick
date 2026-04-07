"""File9 action module for RabAI AutoClick.

Provides additional file operations:
- FileReadAction: Read file
- FileWriteAction: Write file
- FileAppendAction: Append to file
- FileExistsAction: Check if file exists
- FileDeleteAction: Delete file
- FileCopyAction: Copy file
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FileReadAction(BaseAction):
    """Read file."""
    action_type = "file9_read"
    display_name = "读取文件"
    description = "读取文件内容"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file read.

        Args:
            context: Execution context.
            params: Dict with path, encoding, output_var.

        Returns:
            ActionResult with file content.
        """
        path = params.get('path', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'file_content')

        try:
            resolved_path = context.resolve_value(path)
            resolved_encoding = context.resolve_value(encoding) if encoding else 'utf-8'

            with open(resolved_path, 'r', encoding=resolved_encoding) as f:
                content = f.read()

            context.set(output_var, content)

            return ActionResult(
                success=True,
                message=f"读取文件: {len(content)}字符",
                data={
                    'path': resolved_path,
                    'content': content,
                    'size': len(content),
                    'output_var': output_var
                }
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message=f"文件未找到: {resolved_path}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"读取文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'output_var': 'file_content'}


class FileWriteAction(BaseAction):
    """Write file."""
    action_type = "file9_write"
    display_name = "写入文件"
    description = "写入文件内容"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file write.

        Args:
            context: Execution context.
            params: Dict with path, content, encoding, output_var.

        Returns:
            ActionResult with write status.
        """
        path = params.get('path', '')
        content = params.get('content', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'write_status')

        try:
            resolved_path = context.resolve_value(path)
            resolved_content = context.resolve_value(content) if content else ''
            resolved_encoding = context.resolve_value(encoding) if encoding else 'utf-8'

            with open(resolved_path, 'w', encoding=resolved_encoding) as f:
                f.write(resolved_content)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"写入文件: {len(resolved_content)}字符",
                data={
                    'path': resolved_path,
                    'bytes_written': len(resolved_content),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"写入文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path', 'content']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'output_var': 'write_status'}


class FileAppendAction(BaseAction):
    """Append to file."""
    action_type = "file9_append"
    display_name = "追加文件"
    description = "追加内容到文件"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file append.

        Args:
            context: Execution context.
            params: Dict with path, content, encoding, output_var.

        Returns:
            ActionResult with append status.
        """
        path = params.get('path', '')
        content = params.get('content', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'append_status')

        try:
            resolved_path = context.resolve_value(path)
            resolved_content = context.resolve_value(content) if content else ''
            resolved_encoding = context.resolve_value(encoding) if encoding else 'utf-8'

            with open(resolved_path, 'a', encoding=resolved_encoding) as f:
                f.write(resolved_content)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"追加文件: {len(resolved_content)}字符",
                data={
                    'path': resolved_path,
                    'bytes_appended': len(resolved_content),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"追加文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path', 'content']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'output_var': 'append_status'}


class FileExistsAction(BaseAction):
    """Check if file exists."""
    action_type = "file9_exists"
    display_name = "文件存在"
    description = "检查文件是否存在"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file exists.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with exists result.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'exists_result')

        try:
            resolved_path = context.resolve_value(path)

            result = os.path.exists(resolved_path)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"文件存在: {'是' if result else '否'}",
                data={
                    'path': resolved_path,
                    'exists': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查文件存在失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'exists_result'}


class FileDeleteAction(BaseAction):
    """Delete file."""
    action_type = "file9_delete"
    display_name = "删除文件"
    description = "删除文件"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file delete.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with delete status.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'delete_status')

        try:
            resolved_path = context.resolve_value(path)

            if os.path.exists(resolved_path):
                os.remove(resolved_path)
                context.set(output_var, True)
                return ActionResult(
                    success=True,
                    message=f"删除文件: {resolved_path}",
                    data={
                        'path': resolved_path,
                        'deleted': True,
                        'output_var': output_var
                    }
                )
            else:
                context.set(output_var, False)
                return ActionResult(
                    success=False,
                    message=f"文件不存在: {resolved_path}"
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"删除文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'delete_status'}


class FileCopyAction(BaseAction):
    """Copy file."""
    action_type = "file9_copy"
    display_name = "复制文件"
    description = "复制文件"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file copy.

        Args:
            context: Execution context.
            params: Dict with source, destination, output_var.

        Returns:
            ActionResult with copy status.
        """
        source = params.get('source', '')
        destination = params.get('destination', '')
        output_var = params.get('output_var', 'copy_status')

        try:
            import shutil

            resolved_source = context.resolve_value(source)
            resolved_destination = context.resolve_value(destination)

            shutil.copy2(resolved_source, resolved_destination)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"复制文件: {resolved_source} -> {resolved_destination}",
                data={
                    'source': resolved_source,
                    'destination': resolved_destination,
                    'copied': True,
                    'output_var': output_var
                }
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message=f"源文件不存在: {resolved_source}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"复制文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['source', 'destination']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'copy_status'}