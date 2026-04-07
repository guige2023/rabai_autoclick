"""File action module for RabAI AutoClick.

Provides file operations:
- ReadFileAction: Read file contents
- WriteFileAction: Write to file
- FileExistsAction: Check if file exists
- DeleteFileAction: Delete a file
- ListFilesAction: List directory contents
"""

import os
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ReadFileAction(BaseAction):
    """Read file contents."""
    action_type = "read_file"
    display_name = "读取文件"
    description = "读取文件内容"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute reading a file.

        Args:
            context: Execution context.
            params: Dict with path, encoding, output_var.

        Returns:
            ActionResult with file contents.
        """
        path = params.get('path', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'file_content')

        # Validate path
        if not path:
            return ActionResult(
                success=False,
                message="未指定文件路径"
            )
        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(encoding, str, 'encoding')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(path)

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"文件不存在: {resolved_path}"
                )

            if not os.path.isfile(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"路径不是文件: {resolved_path}"
                )

            with open(resolved_path, 'r', encoding=encoding) as f:
                content = f.read()

            # Store in context
            context.set(output_var, content)

            truncated = content[:100] + '...' if len(content) > 100 else content
            return ActionResult(
                success=True,
                message=f"已读取文件: {resolved_path}",
                data={
                    'path': resolved_path,
                    'content': content,
                    'length': len(content),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"读取文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'encoding': 'utf-8',
            'output_var': 'file_content'
        }


class WriteFileAction(BaseAction):
    """Write content to a file."""
    action_type = "write_file"
    display_name = "写入文件"
    description = "写入内容到文件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute writing to a file.

        Args:
            context: Execution context.
            params: Dict with path, content, encoding, append.

        Returns:
            ActionResult indicating success.
        """
        path = params.get('path', '')
        content = params.get('content', '')
        encoding = params.get('encoding', 'utf-8')
        append = params.get('append', False)

        # Validate path
        if not path:
            return ActionResult(
                success=False,
                message="未指定文件路径"
            )
        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        # Validate append
        valid, msg = self.validate_type(append, bool, 'append')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(path)
            resolved_content = context.resolve_value(content)

            # Create directory if needed
            parent_dir = os.path.dirname(resolved_path)
            if parent_dir:
                os.makedirs(parent_dir, exist_ok=True)

            mode = 'a' if append else 'w'
            with open(resolved_path, mode, encoding=encoding) as f:
                f.write(str(resolved_content))

            return ActionResult(
                success=True,
                message=f"已写入文件: {resolved_path} ({len(str(resolved_content))} bytes)",
                data={
                    'path': resolved_path,
                    'bytes_written': len(str(resolved_content)),
                    'append': append
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
        return {
            'encoding': 'utf-8',
            'append': False
        }


class FileExistsAction(BaseAction):
    """Check if a file or directory exists."""
    action_type = "file_exists"
    display_name = "检查文件存在"
    description = "检查文件或目录是否存在"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute checking file existence.

        Args:
            context: Execution context.
            params: Dict with path, check_type, output_var.

        Returns:
            ActionResult with existence status.
        """
        path = params.get('path', '')
        check_type = params.get('check_type', 'any')  # 'any', 'file', 'dir'
        output_var = params.get('output_var', 'file_exists')

        # Validate path
        if not path:
            return ActionResult(
                success=False,
                message="未指定路径"
            )
        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(check_type, str, 'check_type')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(path)

            exists = os.path.exists(resolved_path)
            is_file = os.path.isfile(resolved_path) if exists else False
            is_dir = os.path.isdir(resolved_path) if exists else False

            if check_type == 'file':
                result = is_file
            elif check_type == 'dir':
                result = is_dir
            else:
                result = exists

            # Store in context
            context.set(output_var, result)

            status = "存在" if result else "不存在"
            return ActionResult(
                success=True,
                message=f"{resolved_path}: {status}",
                data={
                    'path': resolved_path,
                    'exists': exists,
                    'is_file': is_file,
                    'is_dir': is_dir,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'check_type': 'any',  # 'any', 'file', 'dir'
            'output_var': 'file_exists'
        }


class DeleteFileAction(BaseAction):
    """Delete a file or directory."""
    action_type = "delete_file"
    display_name = "删除文件"
    description = "删除文件或目录"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute deleting a file.

        Args:
            context: Execution context.
            params: Dict with path, recursive.

        Returns:
            ActionResult indicating success.
        """
        path = params.get('path', '')
        recursive = params.get('recursive', False)

        # Validate path
        if not path:
            return ActionResult(
                success=False,
                message="未指定路径"
            )
        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(recursive, bool, 'recursive')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(path)

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=True,
                    message=f"路径不存在: {resolved_path}",
                    data={'deleted': False, 'reason': 'not_found'}
                )

            if os.path.isdir(resolved_path):
                if recursive:
                    import shutil
                    shutil.rmtree(resolved_path)
                else:
                    os.rmdir(resolved_path)
            else:
                os.remove(resolved_path)

            return ActionResult(
                success=True,
                message=f"已删除: {resolved_path}",
                data={'deleted': True, 'path': resolved_path}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"删除失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'recursive': False}


class ListFilesAction(BaseAction):
    """List files in a directory."""
    action_type = "list_files"
    display_name = "列出文件"
    description = "列出目录中的文件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute listing files.

        Args:
            context: Execution context.
            params: Dict with path, pattern, recursive, output_var.

        Returns:
            ActionResult with file list.
        """
        path = params.get('path', '.')
        pattern = params.get('pattern', '*')
        recursive = params.get('recursive', False)
        output_var = params.get('output_var', 'file_list')

        # Validate path
        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(pattern, str, 'pattern')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(recursive, bool, 'recursive')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(path)

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"路径不存在: {resolved_path}"
                )

            if not os.path.isdir(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"路径不是目录: {resolved_path}"
                )

            files = []
            if recursive:
                for root, dirs, filenames in os.walk(resolved_path):
                    for filename in filenames:
                        filepath = os.path.join(root, filename)
                        files.append(filepath)
            else:
                for item in os.listdir(resolved_path):
                    filepath = os.path.join(resolved_path, item)
                    if os.path.isfile(filepath):
                        files.append(filepath)

            # Filter by pattern
            if pattern and pattern != '*':
                import fnmatch
                files = [f for f in files if fnmatch.fnmatch(os.path.basename(f), pattern)]

            # Store in context
            context.set(output_var, files)

            return ActionResult(
                success=True,
                message=f"找到 {len(files)} 个文件",
                data={
                    'path': resolved_path,
                    'files': files,
                    'count': len(files),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列出文件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'path': '.',
            'pattern': '*',
            'recursive': False,
            'output_var': 'file_list'
        }