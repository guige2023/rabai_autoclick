"""File4 action module for RabAI AutoClick.

Provides additional file operations:
- FileReadLinesAction: Read file lines
- FileWriteLinesAction: Write lines to file
- FileAppendLinesAction: Append lines to file
- FileReadBinaryAction: Read file as binary
- FileWriteBinaryAction: Write binary to file
"""

import os
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FileReadLinesAction(BaseAction):
    """Read file lines."""
    action_type = "file4_read_lines"
    display_name = "读取文件行"
    description = "读取文件所有行"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute read lines.

        Args:
            context: Execution context.
            params: Dict with path, encoding, output_var.

        Returns:
            ActionResult with lines.
        """
        path = params.get('path', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'file_lines')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            resolved_encoding = context.resolve_value(encoding) if encoding else 'utf-8'

            if not os.path.exists(resolved):
                return ActionResult(
                    success=False,
                    message=f"文件不存在: {resolved}"
                )

            with open(resolved, 'r', encoding=resolved_encoding) as f:
                lines = f.readlines()

            context.set(output_var, lines)

            return ActionResult(
                success=True,
                message=f"读取文件行: {len(lines)} 行",
                data={
                    'path': resolved,
                    'line_count': len(lines),
                    'lines': lines,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"读取文件行失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'output_var': 'file_lines'}


class FileWriteLinesAction(BaseAction):
    """Write lines to file."""
    action_type = "file4_write_lines"
    display_name = "写入文件行"
    description = "写入多行到文件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute write lines.

        Args:
            context: Execution context.
            params: Dict with path, lines, encoding, output_var.

        Returns:
            ActionResult with write result.
        """
        path = params.get('path', '')
        lines = params.get('lines', [])
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'write_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            resolved_lines = context.resolve_value(lines)
            resolved_encoding = context.resolve_value(encoding) if encoding else 'utf-8'

            if not isinstance(resolved_lines, (list, tuple)):
                resolved_lines = [str(resolved_lines)]

            with open(resolved, 'w', encoding=resolved_encoding) as f:
                f.writelines(resolved_lines)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"写入文件行: {len(resolved_lines)} 行",
                data={
                    'path': resolved,
                    'lines_written': len(resolved_lines),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"写入文件行失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path', 'lines']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'output_var': 'write_result'}


class FileAppendLinesAction(BaseAction):
    """Append lines to file."""
    action_type = "file4_append_lines"
    display_name = "追加文件行"
    description = "追加多行到文件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute append lines.

        Args:
            context: Execution context.
            params: Dict with path, lines, encoding, output_var.

        Returns:
            ActionResult with append result.
        """
        path = params.get('path', '')
        lines = params.get('lines', [])
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'append_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            resolved_lines = context.resolve_value(lines)
            resolved_encoding = context.resolve_value(encoding) if encoding else 'utf-8'

            if not isinstance(resolved_lines, (list, tuple)):
                resolved_lines = [str(resolved_lines)]

            with open(resolved, 'a', encoding=resolved_encoding) as f:
                f.writelines(resolved_lines)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"追加文件行: {len(resolved_lines)} 行",
                data={
                    'path': resolved,
                    'lines_appended': len(resolved_lines),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"追加文件行失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path', 'lines']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'output_var': 'append_result'}


class FileReadBinaryAction(BaseAction):
    """Read file as binary."""
    action_type = "file4_read_binary"
    display_name = "读取二进制"
    description = "以二进制读取文件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute read binary.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with binary content.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'binary_content')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)

            if not os.path.exists(resolved):
                return ActionResult(
                    success=False,
                    message=f"文件不存在: {resolved}"
                )

            with open(resolved, 'rb') as f:
                content = f.read()

            context.set(output_var, content)

            return ActionResult(
                success=True,
                message=f"读取二进制: {len(content)} bytes",
                data={
                    'path': resolved,
                    'size': len(content),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"读取二进制失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'binary_content'}


class FileWriteBinaryAction(BaseAction):
    """Write binary to file."""
    action_type = "file4_write_binary"
    display_name = "写入二进制"
    description = "以二进制写入文件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute write binary.

        Args:
            context: Execution context.
            params: Dict with path, content, output_var.

        Returns:
            ActionResult with write result.
        """
        path = params.get('path', '')
        content = params.get('content', b'')
        output_var = params.get('output_var', 'write_binary_result')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(path)
            resolved_content = context.resolve_value(content)

            if isinstance(resolved_content, str):
                resolved_content = resolved_content.encode('utf-8')

            with open(resolved, 'wb') as f:
                f.write(resolved_content)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"写入二进制: {len(resolved_content)} bytes",
                data={
                    'path': resolved,
                    'bytes_written': len(resolved_content),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"写入二进制失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path', 'content']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'write_binary_result'}
