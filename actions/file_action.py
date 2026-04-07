"""File system action module for RabAI AutoClick.

Provides file operations:
- FileReadAction: Read file content
- FileWriteAction: Write content to file
- FileAppendAction: Append content to file
- FileCopyAction: Copy file
- FileMoveAction: Move file
- FileDeleteAction: Delete file
- FileExistsAction: Check if file exists
- FileSizeAction: Get file size
- FileListAction: List directory contents
- FileCreateDirAction: Create directory
- FileRemoveDirAction: Remove directory
- FileWalkAction: Walk directory tree
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


try:
    import shutil
    import glob as glob_module
    FS_AVAILABLE = True
except ImportError:
    FS_AVAILABLE = False


class FileReadAction(BaseAction):
    """Read file content."""
    action_type = "file_read"
    display_name = "读取文件"
    description = "读取文件内容"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute file read.

        Args:
            context: Execution context.
            params: Dict with path, encoding, max_size, output_var.

        Returns:
            ActionResult with file content.
        """
        if not FS_AVAILABLE:
            return ActionResult(success=False, message="文件系统库不可用")

        file_path = params.get('path', '')
        encoding = params.get('encoding', 'utf-8')
        max_size = params.get('max_size', 10 * 1024 * 1024)
        output_var = params.get('output_var', 'file_content')

        if not file_path:
            return ActionResult(success=False, message="文件路径不能为空")

        valid, msg = self.validate_type(file_path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            if not os.path.exists(file_path):
                return ActionResult(success=False, message=f"文件不存在: {file_path}")

            if not os.path.isfile(file_path):
                return ActionResult(success=False, message=f"路径不是文件: {file_path}")

            file_size = os.path.getsize(file_path)
            if file_size > max_size:
                return ActionResult(
                    success=False,
                    message=f"文件过大: {file_size} bytes, 限制: {max_size} bytes"
                )

            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                content = f.read()

            context.set(output_var, content)

            return ActionResult(
                success=True,
                message=f"读取成功: {len(content)} 字符",
                data={'path': file_path, 'size': file_size, 'content': content}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"读取文件失败: {str(e)}"
            )


class FileWriteAction(BaseAction):
    """Write content to file."""
    action_type = "file_write"
    display_name = "写入文件"
    description = "写入内容到文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute file write.

        Args:
            context: Execution context.
            params: Dict with path, content, encoding, create_dirs, output_var.

        Returns:
            ActionResult with write result.
        """
        if not FS_AVAILABLE:
            return ActionResult(success=False, message="文件系统库不可用")

        file_path = params.get('path', '')
        content = params.get('content', '')
        encoding = params.get('encoding', 'utf-8')
        create_dirs = params.get('create_dirs', True)
        output_var = params.get('output_var', 'write_result')

        if not file_path:
            return ActionResult(success=False, message="文件路径不能为空")

        try:
            if create_dirs:
                dir_path = os.path.dirname(file_path)
                if dir_path and not os.path.exists(dir_path):
                    os.makedirs(dir_path, exist_ok=True)

            bytes_written = 0
            with open(file_path, 'w', encoding=encoding) as f:
                bytes_written = f.write(content)

            context.set(output_var, {
                'path': file_path,
                'bytes_written': bytes_written,
                'success': True
            })

            return ActionResult(
                success=True,
                message=f"写入成功: {bytes_written} 字符",
                data={'path': file_path, 'bytes_written': bytes_written}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"写入文件失败: {str(e)}"
            )


class FileAppendAction(BaseAction):
    """Append content to file."""
    action_type = "file_append"
    display_name = "追加文件"
    description = "追加内容到文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute file append.

        Args:
            context: Execution context.
            params: Dict with path, content, encoding, output_var.

        Returns:
            ActionResult with append result.
        """
        if not FS_AVAILABLE:
            return ActionResult(success=False, message="文件系统库不可用")

        file_path = params.get('path', '')
        content = params.get('content', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'append_result')

        if not file_path:
            return ActionResult(success=False, message="文件路径不能为空")

        try:
            dir_path = os.path.dirname(file_path)
            if dir_path and not os.path.exists(dir_path):
                os.makedirs(dir_path, exist_ok=True)

            bytes_written = 0
            with open(file_path, 'a', encoding=encoding) as f:
                bytes_written = f.write(content)

            context.set(output_var, {
                'path': file_path,
                'bytes_written': bytes_written,
                'success': True
            })

            return ActionResult(
                success=True,
                message=f"追加成功: {bytes_written} 字符",
                data={'path': file_path, 'bytes_written': bytes_written}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"追加文件失败: {str(e)}"
            )


class FileCopyAction(BaseAction):
    """Copy file."""
    action_type = "file_copy"
    display_name = "复制文件"
    description = "复制文件到目标路径"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute file copy.

        Args:
            context: Execution context.
            params: Dict with src, dest, overwrite, output_var.

        Returns:
            ActionResult with copy result.
        """
        if not FS_AVAILABLE:
            return ActionResult(success=False, message="文件系统库不可用")

        src = params.get('src', '')
        dest = params.get('dest', '')
        overwrite = params.get('overwrite', False)
        output_var = params.get('output_var', 'copy_result')

        if not src or not dest:
            return ActionResult(success=False, message="源路径和目标路径都不能为空")

        try:
            if not os.path.exists(src):
                return ActionResult(success=False, message=f"源文件不存在: {src}")

            if os.path.exists(dest) and not overwrite:
                return ActionResult(success=False, message=f"目标文件已存在: {dest}")

            dest_dir = os.path.dirname(dest)
            if dest_dir and not os.path.exists(dest_dir):
                os.makedirs(dest_dir, exist_ok=True)

            shutil.copy2(src, dest)

            context.set(output_var, {
                'src': src,
                'dest': dest,
                'success': True
            })

            return ActionResult(
                success=True,
                message=f"复制成功: {src} -> {dest}",
                data={'src': src, 'dest': dest}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"复制文件失败: {str(e)}"
            )


class FileMoveAction(BaseAction):
    """Move file."""
    action_type = "file_move"
    display_name = "移动文件"
    description = "移动文件到目标路径"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute file move.

        Args:
            context: Execution context.
            params: Dict with src, dest, overwrite, output_var.

        Returns:
            ActionResult with move result.
        """
        if not FS_AVAILABLE:
            return ActionResult(success=False, message="文件系统库不可用")

        src = params.get('src', '')
        dest = params.get('dest', '')
        overwrite = params.get('overwrite', False)
        output_var = params.get('output_var', 'move_result')

        if not src or not dest:
            return ActionResult(success=False, message="源路径和目标路径都不能为空")

        try:
            if not os.path.exists(src):
                return ActionResult(success=False, message=f"源文件不存在: {src}")

            if os.path.exists(dest) and not overwrite:
                return ActionResult(success=False, message=f"目标文件已存在: {dest}")

            dest_dir = os.path.dirname(dest)
            if dest_dir and not os.path.exists(dest_dir):
                os.makedirs(dest_dir, exist_ok=True)

            shutil.move(src, dest)

            context.set(output_var, {
                'src': src,
                'dest': dest,
                'success': True
            })

            return ActionResult(
                success=True,
                message=f"移动成功: {src} -> {dest}",
                data={'src': src, 'dest': dest}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"移动文件失败: {str(e)}"
            )


class FileDeleteAction(BaseAction):
    """Delete file."""
    action_type = "file_delete"
    display_name = "删除文件"
    description = "删除指定文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute file delete.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with delete result.
        """
        if not FS_AVAILABLE:
            return ActionResult(success=False, message="文件系统库不可用")

        file_path = params.get('path', '')
        output_var = params.get('output_var', 'delete_result')

        if not file_path:
            return ActionResult(success=False, message="文件路径不能为空")

        try:
            if not os.path.exists(file_path):
                return ActionResult(success=False, message=f"文件不存在: {file_path}")

            if os.path.isdir(file_path):
                return ActionResult(success=False, message=f"路径是目录而非文件: {file_path}")

            os.remove(file_path)

            context.set(output_var, {
                'path': file_path,
                'success': True
            })

            return ActionResult(
                success=True,
                message=f"删除成功: {file_path}",
                data={'path': file_path}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"删除文件失败: {str(e)}"
            )


class FileExistsAction(BaseAction):
    """Check if file exists."""
    action_type = "file_exists"
    display_name = "文件是否存在"
    description = "检查文件是否存在"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute file exists check.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with exists result.
        """
        if not FS_AVAILABLE:
            return ActionResult(success=False, message="文件系统库不可用")

        file_path = params.get('path', '')
        output_var = params.get('output_var', 'exists_result')

        if not file_path:
            return ActionResult(success=False, message="文件路径不能为空")

        exists = os.path.exists(file_path)
        is_file = os.path.isfile(file_path) if exists else False
        is_dir = os.path.isdir(file_path) if exists else False

        context.set(output_var, exists)

        return ActionResult(
            success=True,
            message=f"文件{'存在' if exists else '不存在'}",
            data={'path': file_path, 'exists': exists, 'is_file': is_file, 'is_dir': is_dir}
        )


class FileSizeAction(BaseAction):
    """Get file size."""
    action_type = "file_size"
    display_name = "获取文件大小"
    description = "获取文件字节大小"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute file size check.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with file size.
        """
        if not FS_AVAILABLE:
            return ActionResult(success=False, message="文件系统库不可用")

        file_path = params.get('path', '')
        output_var = params.get('output_var', 'size_result')

        if not file_path:
            return ActionResult(success=False, message="文件路径不能为空")

        try:
            if not os.path.exists(file_path):
                return ActionResult(success=False, message=f"文件不存在: {file_path}")

            if not os.path.isfile(file_path):
                return ActionResult(success=False, message=f"路径不是文件: {file_path}")

            size = os.path.getsize(file_path)

            context.set(output_var, size)

            return ActionResult(
                success=True,
                message=f"文件大小: {size} bytes",
                data={'path': file_path, 'size': size, 'size_kb': size / 1024, 'size_mb': size / (1024 * 1024)}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取文件大小失败: {str(e)}"
            )


class FileListAction(BaseAction):
    """List directory contents."""
    action_type = "file_list"
    display_name = "列出目录"
    description = "列出目录中的文件和子目录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute directory listing.

        Args:
            context: Execution context.
            params: Dict with path, pattern, recursive, output_var.

        Returns:
            ActionResult with file list.
        """
        if not FS_AVAILABLE:
            return ActionResult(success=False, message="文件系统库不可用")

        dir_path = params.get('path', '')
        pattern = params.get('pattern', '*')
        recursive = params.get('recursive', False)
        output_var = params.get('output_var', 'list_result')

        if not dir_path:
            return ActionResult(success=False, message="目录路径不能为空")

        try:
            if not os.path.exists(dir_path):
                return ActionResult(success=False, message=f"目录不存在: {dir_path}")

            if not os.path.isdir(dir_path):
                return ActionResult(success=False, message=f"路径不是目录: {dir_path}")

            files = []
            dirs = []

            if recursive:
                for root, subdirs, filenames in os.walk(dir_path):
                    for filename in filenames:
                        full_path = os.path.join(root, filename)
                        rel_path = os.path.relpath(full_path, dir_path)
                        files.append(rel_path)
                    for subdir in subdirs:
                        full_path = os.path.join(root, subdir)
                        rel_path = os.path.relpath(full_path, dir_path)
                        dirs.append(rel_path)
            else:
                entries = os.listdir(dir_path)
                for entry in entries:
                    full_path = os.path.join(dir_path, entry)
                    if os.path.isfile(full_path):
                        files.append(entry)
                    elif os.path.isdir(full_path):
                        dirs.append(entry)

            if pattern and pattern != '*':
                import fnmatch
                files = [f for f in files if fnmatch.fnmatch(os.path.basename(f), pattern)]
                dirs = [d for d in dirs if fnmatch.fnmatch(os.path.basename(d), pattern)]

            context.set(output_var, {'files': files, 'dirs': dirs, 'path': dir_path})

            return ActionResult(
                success=True,
                message=f"列出成功: {len(files)} 文件, {len(dirs)} 目录",
                data={'path': dir_path, 'files': files, 'dirs': dirs, 'file_count': len(files), 'dir_count': len(dirs)}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列出目录失败: {str(e)}"
            )


class FileCreateDirAction(BaseAction):
    """Create directory."""
    action_type = "file_create_dir"
    display_name = "创建目录"
    description = "创建新目录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute directory creation.

        Args:
            context: Execution context.
            params: Dict with path, parents, output_var.

        Returns:
            ActionResult with create result.
        """
        if not FS_AVAILABLE:
            return ActionResult(success=False, message="文件系统库不可用")

        dir_path = params.get('path', '')
        parents = params.get('parents', True)
        output_var = params.get('output_var', 'mkdir_result')

        if not dir_path:
            return ActionResult(success=False, message="目录路径不能为空")

        try:
            if os.path.exists(dir_path):
                return ActionResult(success=False, message=f"目录已存在: {dir_path}")

            os.makedirs(dir_path, exist_ok=parents)

            context.set(output_var, {
                'path': dir_path,
                'success': True
            })

            return ActionResult(
                success=True,
                message=f"创建成功: {dir_path}",
                data={'path': dir_path}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建目录失败: {str(e)}"
            )


class FileRemoveDirAction(BaseAction):
    """Remove directory."""
    action_type = "file_remove_dir"
    display_name = "删除目录"
    description = "删除空目录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute directory removal.

        Args:
            context: Execution context.
            params: Dict with path, recursive, output_var.

        Returns:
            ActionResult with remove result.
        """
        if not FS_AVAILABLE:
            return ActionResult(success=False, message="文件系统库不可用")

        dir_path = params.get('path', '')
        recursive = params.get('recursive', False)
        output_var = params.get('output_var', 'rmdir_result')

        if not dir_path:
            return ActionResult(success=False, message="目录路径不能为空")

        try:
            if not os.path.exists(dir_path):
                return ActionResult(success=False, message=f"目录不存在: {dir_path}")

            if not os.path.isdir(dir_path):
                return ActionResult(success=False, message=f"路径不是目录: {dir_path}")

            if recursive:
                shutil.rmtree(dir_path)
            else:
                os.rmdir(dir_path)

            context.set(output_var, {
                'path': dir_path,
                'success': True
            })

            return ActionResult(
                success=True,
                message=f"删除成功: {dir_path}",
                data={'path': dir_path}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"删除目录失败: {str(e)}"
            )


class FileWalkAction(BaseAction):
    """Walk directory tree."""
    action_type = "file_walk"
    display_name = "遍历目录树"
    description = "遍历目录树返回所有路径"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute directory tree walk.

        Args:
            context: Execution context.
            params: Dict with path, topdown, output_var.

        Returns:
            ActionResult with tree structure.
        """
        if not FS_AVAILABLE:
            return ActionResult(success=False, message="文件系统库不可用")

        dir_path = params.get('path', '')
        topdown = params.get('topdown', True)
        output_var = params.get('output_var', 'walk_result')

        if not dir_path:
            return ActionResult(success=False, message="目录路径不能为空")

        try:
            if not os.path.exists(dir_path):
                return ActionResult(success=False, message=f"目录不存在: {dir_path}")

            tree = []
            total_files = 0
            total_dirs = 0

            for root, dirs, files in os.walk(dir_path, topdown=topdown):
                rel_root = os.path.relpath(root, dir_path)
                level = rel_root.count(os.sep) if rel_root != '.' else 0

                tree.append({
                    'root': root,
                    'rel_path': rel_root,
                    'level': level,
                    'dirs': dirs,
                    'files': files
                })

                total_files += len(files)
                total_dirs += len(dirs)

            context.set(output_var, tree)

            return ActionResult(
                success=True,
                message=f"遍历成功: {total_files} 文件, {total_dirs} 目录",
                data={'path': dir_path, 'tree': tree, 'total_files': total_files, 'total_dirs': total_dirs}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"遍历目录失败: {str(e)}"
            )
