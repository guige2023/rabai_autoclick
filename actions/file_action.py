"""File action module for RabAI AutoClick.

Provides file operations:
- FileReadAction: Read file
- FileWriteAction: Write file
- FileAppendAction: Append to file
- FileExistsAction: Check if file exists
- FileDeleteAction: Delete file
- FileCopyAction: Copy file
- FileMoveAction: Move file
- FileSizeAction: Get file size
- FileListAction: List directory
"""

import os
import shutil
from pathlib import Path
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FileReadAction(BaseAction):
    """Read file."""
    action_type = "file_read"
    display_name = "读取文件"
    description = "读取文件内容"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute read.

        Args:
            context: Execution context.
            params: Dict with file_path, encoding, max_size, output_var.

        Returns:
            ActionResult with file content.
        """
        file_path = params.get('file_path', '')
        encoding = params.get('encoding', 'utf-8')
        max_size = params.get('max_size', 1024 * 1024)
        output_var = params.get('output_var', 'file_content')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_encoding = context.resolve_value(encoding)
            resolved_max = context.resolve_value(max_size)

            if not os.path.exists(resolved_path):
                return ActionResult(success=False, message=f"文件不存在: {resolved_path}")

            size = os.path.getsize(resolved_path)
            if size > resolved_max:
                return ActionResult(success=False, message=f"文件过大: {size} bytes (max: {resolved_max})")

            with open(resolved_path, 'r', encoding=resolved_encoding) as f:
                content = f.read()

            context.set(output_var, content)

            return ActionResult(
                success=True,
                message=f"已读取: {len(content)} 字符",
                data={'content': content, 'size': len(content), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"读取文件失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'max_size': 1048576, 'output_var': 'file_content'}


class FileWriteAction(BaseAction):
    """Write file."""
    action_type = "file_write"
    display_name = "写入文件"
    description = "写入文件内容"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute write.

        Args:
            context: Execution context.
            params: Dict with file_path, content, encoding.

        Returns:
            ActionResult indicating success.
        """
        file_path = params.get('file_path', '')
        content = params.get('content', '')
        encoding = params.get('encoding', 'utf-8')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_content = context.resolve_value(content)
            resolved_encoding = context.resolve_value(encoding)

            os.makedirs(os.path.dirname(resolved_path) or '.', exist_ok=True)

            with open(resolved_path, 'w', encoding=resolved_encoding) as f:
                f.write(resolved_content)

            return ActionResult(
                success=True,
                message=f"已写入: {resolved_path}",
                data={'file_path': resolved_path, 'size': len(resolved_content)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"写入文件失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path', 'content']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8'}


class FileAppendAction(BaseAction):
    """Append to file."""
    action_type = "file_append"
    display_name = "追加文件"
    description = "追加内容到文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute append.

        Args:
            context: Execution context.
            params: Dict with file_path, content, encoding.

        Returns:
            ActionResult indicating success.
        """
        file_path = params.get('file_path', '')
        content = params.get('content', '')
        encoding = params.get('encoding', 'utf-8')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_content = context.resolve_value(content)
            resolved_encoding = context.resolve_value(encoding)

            os.makedirs(os.path.dirname(resolved_path) or '.', exist_ok=True)

            with open(resolved_path, 'a', encoding=resolved_encoding) as f:
                f.write(resolved_content)

            return ActionResult(
                success=True,
                message=f"已追加: {resolved_path}",
                data={'file_path': resolved_path, 'size': len(resolved_content)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"追加文件失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path', 'content']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8'}


class FileExistsAction(BaseAction):
    """Check if file exists."""
    action_type = "file_exists"
    display_name = "文件存在"
    description = "检查文件是否存在"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute exists.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with exists flag.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'file_exists')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            exists = os.path.exists(resolved_path)

            context.set(output_var, exists)

            return ActionResult(
                success=True,
                message=f"文件{'存在' if exists else '不存在'}: {resolved_path}",
                data={'exists': exists, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"检查文件失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'file_exists'}


class FileDeleteAction(BaseAction):
    """Delete file."""
    action_type = "file_delete"
    display_name = "删除文件"
    description = "删除文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute delete.

        Args:
            context: Execution context.
            params: Dict with file_path.

        Returns:
            ActionResult indicating success.
        """
        file_path = params.get('file_path', '')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)

            if not os.path.exists(resolved_path):
                return ActionResult(success=False, message=f"文件不存在: {resolved_path}")

            os.remove(resolved_path)

            return ActionResult(
                success=True,
                message=f"已删除: {resolved_path}",
                data={'file_path': resolved_path}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"删除文件失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class FileCopyAction(BaseAction):
    """Copy file."""
    action_type = "file_copy"
    display_name = "复制文件"
    description = "复制文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute copy.

        Args:
            context: Execution context.
            params: Dict with source, destination.

        Returns:
            ActionResult indicating success.
        """
        source = params.get('source', '')
        destination = params.get('destination', '')

        valid, msg = self.validate_type(source, str, 'source')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(destination, str, 'destination')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_source = context.resolve_value(source)
            resolved_dest = context.resolve_value(destination)

            if not os.path.exists(resolved_source):
                return ActionResult(success=False, message=f"源文件不存在: {resolved_source}")

            os.makedirs(os.path.dirname(resolved_dest) or '.', exist_ok=True)
            shutil.copy2(resolved_source, resolved_dest)

            return ActionResult(
                success=True,
                message=f"已复制: {resolved_source} -> {resolved_dest}",
                data={'source': resolved_source, 'destination': resolved_dest}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"复制文件失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['source', 'destination']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class FileMoveAction(BaseAction):
    """Move file."""
    action_type = "file_move"
    display_name = "移动文件"
    description = "移动文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute move.

        Args:
            context: Execution context.
            params: Dict with source, destination.

        Returns:
            ActionResult indicating success.
        """
        source = params.get('source', '')
        destination = params.get('destination', '')

        valid, msg = self.validate_type(source, str, 'source')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(destination, str, 'destination')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_source = context.resolve_value(source)
            resolved_dest = context.resolve_value(destination)

            if not os.path.exists(resolved_source):
                return ActionResult(success=False, message=f"源文件不存在: {resolved_source}")

            os.makedirs(os.path.dirname(resolved_dest) or '.', exist_ok=True)
            shutil.move(resolved_source, resolved_dest)

            return ActionResult(
                success=True,
                message=f"已移动: {resolved_source} -> {resolved_dest}",
                data={'source': resolved_source, 'destination': resolved_dest}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"移动文件失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['source', 'destination']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class FileSizeAction(BaseAction):
    """Get file size."""
    action_type = "file_size"
    display_name = "文件大小"
    description = "获取文件大小"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute size.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with file size.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'file_size')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)

            if not os.path.exists(resolved_path):
                return ActionResult(success=False, message=f"文件不存在: {resolved_path}")

            size = os.path.getsize(resolved_path)

            context.set(output_var, size)

            return ActionResult(
                success=True,
                message=f"文件大小: {size} bytes",
                data={'size': size, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"获取文件大小失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'file_size'}


class FileListAction(BaseAction):
    """List directory."""
    action_type = "file_list"
    display_name = "列出文件"
    description = "列出目录文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list.

        Args:
            context: Execution context.
            params: Dict with directory, pattern, output_var.

        Returns:
            ActionResult with file list.
        """
        directory = params.get('directory', '.')
        pattern = params.get('pattern', '*')
        output_var = params.get('output_var', 'file_list')

        valid, msg = self.validate_type(directory, str, 'directory')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dir = context.resolve_value(directory)
            resolved_pattern = context.resolve_value(pattern)

            if not os.path.exists(resolved_dir):
                return ActionResult(success=False, message=f"目录不存在: {resolved_dir}")

            path = Path(resolved_dir)
            files = [str(f) for f in path.glob(resolved_pattern)]

            context.set(output_var, files)

            return ActionResult(
                success=True,
                message=f"文件列表: {len(files)} 个",
                data={'files': files, 'count': len(files), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"列出文件失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['directory']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'pattern': '*', 'output_var': 'file_list'}
