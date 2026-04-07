"""Pathlib action module for RabAI AutoClick.

Provides pathlib extensions:
- PathExistsAction: Check if path exists
- PathIsFileAction: Check if path is file
- PathIsDirAction: Check if path is directory
- PathListDirAction: List directory contents
- PathGlobAction: Glob pattern matching
- PathResolveAction: Resolve path
- PathParentsAction: Get parent directories
- PathSuffixAction: Get file suffix
- PathStemAction: Get file stem
- PathCreateAction: Create file/directory
"""

from typing import Any, Dict, List, Optional, Union
import sys
import pathlib

_parent_dir = __import__('os').path.dirname(__import__('os').path.dirname(__import__('os').path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PathExistsAction(BaseAction):
    """Check if path exists."""
    action_type = "pathlib_exists"
    display_name = "路径存在"
    description = "检查路径是否存在"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute exists check."""
        path = params.get('path', '')
        output_var = params.get('output_var', 'exists_result')

        try:
            resolved_path = context.resolve_value(path) if isinstance(path, str) else path
            p = pathlib.Path(resolved_path)
            exists = p.exists()
            context.set_variable(output_var, exists)
            return ActionResult(success=True, message=f"exists: {exists}")
        except Exception as e:
            return ActionResult(success=False, message=f"exists check failed: {e}")


class PathIsFileAction(BaseAction):
    """Check if path is file."""
    action_type = "pathlib_is_file"
    display_name = "是文件"
    description = "检查路径是否是文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute is file check."""
        path = params.get('path', '')
        output_var = params.get('output_var', 'is_file_result')

        try:
            resolved_path = context.resolve_value(path) if isinstance(path, str) else path
            p = pathlib.Path(resolved_path)
            is_file = p.is_file()
            context.set_variable(output_var, is_file)
            return ActionResult(success=True, message=f"is_file: {is_file}")
        except Exception as e:
            return ActionResult(success=False, message=f"is_file check failed: {e}")


class PathIsDirAction(BaseAction):
    """Check if path is directory."""
    action_type = "pathlib_is_dir"
    display_name = "是目录"
    description = "检查路径是否是目录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute is dir check."""
        path = params.get('path', '')
        output_var = params.get('output_var', 'is_dir_result')

        try:
            resolved_path = context.resolve_value(path) if isinstance(path, str) else path
            p = pathlib.Path(resolved_path)
            is_dir = p.is_dir()
            context.set_variable(output_var, is_dir)
            return ActionResult(success=True, message=f"is_dir: {is_dir}")
        except Exception as e:
            return ActionResult(success=False, message=f"is_dir check failed: {e}")


class PathListDirAction(BaseAction):
    """List directory contents."""
    action_type = "pathlib_list_dir"
    display_name = "列出目录"
    description = "列出目录内容"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute list dir."""
        path = params.get('path', '.')
        pattern = params.get('pattern', '*')
        recursive = params.get('recursive', False)
        output_var = params.get('output_var', 'list_dir_result')

        try:
            resolved_path = context.resolve_value(path) if isinstance(path, str) else path
            p = pathlib.Path(resolved_path)
            
            if not p.is_dir():
                return ActionResult(success=False, message="path is not a directory")
            
            if recursive:
                files = [str(f) for f in p.rglob(pattern)]
            else:
                files = [str(f) for f in p.glob(pattern)]
            
            context.set_variable(output_var, files)
            return ActionResult(success=True, message=f"listed {len(files)} items")
        except Exception as e:
            return ActionResult(success=False, message=f"list_dir failed: {e}")


class PathGlobAction(BaseAction):
    """Glob pattern matching."""
    action_type = "pathlib_glob"
    display_name = "通配符匹配"
    description = "使用通配符模式匹配文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute glob."""
        path = params.get('path', '.')
        pattern = params.get('pattern', '*')
        recursive = params.get('recursive', False)
        output_var = params.get('output_var', 'glob_result')

        try:
            resolved_path = context.resolve_value(path) if isinstance(path, str) else path
            p = pathlib.Path(resolved_path)
            
            if recursive:
                matches = list(p.rglob(pattern))
            else:
                matches = list(p.glob(pattern))
            
            files = [str(m) for m in matches]
            context.set_variable(output_var, files)
            return ActionResult(success=True, message=f"glob matched {len(files)} files")
        except Exception as e:
            return ActionResult(success=False, message=f"glob failed: {e}")


class PathResolveAction(BaseAction):
    """Resolve path."""
    action_type = "pathlib_resolve"
    display_name = "解析路径"
    description = "解析绝对路径"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute resolve."""
        path = params.get('path', '')
        output_var = params.get('output_var', 'resolve_result')

        try:
            resolved_path = context.resolve_value(path) if isinstance(path, str) else path
            p = pathlib.Path(resolved_path)
            resolved = p.resolve()
            context.set_variable(output_var, str(resolved))
            return ActionResult(success=True, message=f"resolved: {resolved}")
        except Exception as e:
            return ActionResult(success=False, message=f"resolve failed: {e}")


class PathParentsAction(BaseAction):
    """Get parent directories."""
    action_type = "pathlib_parents"
    display_name = "父目录"
    description = "获取路径的所有父目录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute parents."""
        path = params.get('path', '')
        depth = params.get('depth', 1)
        output_var = params.get('output_var', 'parents_result')

        try:
            resolved_path = context.resolve_value(path) if isinstance(path, str) else path
            p = pathlib.Path(resolved_path)
            resolved_depth = context.resolve_value(depth) if isinstance(depth, str) else depth
            
            parents = []
            current = p.parent
            for _ in range(resolved_depth):
                if str(current) == current:
                    break
                parents.append(str(current))
                current = current.parent
                if len(parents) >= resolved_depth:
                    break
            
            context.set_variable(output_var, parents)
            return ActionResult(success=True, message=f"got {len(parents)} parents")
        except Exception as e:
            return ActionResult(success=False, message=f"parents failed: {e}")


class PathSuffixAction(BaseAction):
    """Get file suffix."""
    action_type = "pathlib_suffix"
    display_name = "文件后缀"
    description = "获取文件后缀名"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute suffix."""
        path = params.get('path', '')
        output_var = params.get('output_var', 'suffix_result')

        try:
            resolved_path = context.resolve_value(path) if isinstance(path, str) else path
            p = pathlib.Path(resolved_path)
            suffix = p.suffix
            context.set_variable(output_var, suffix)
            return ActionResult(success=True, message=f"suffix: {suffix}")
        except Exception as e:
            return ActionResult(success=False, message=f"suffix failed: {e}")


class PathStemAction(BaseAction):
    """Get file stem."""
    action_type = "pathlib_stem"
    display_name = "文件名主干"
    description = "获取文件名（不含后缀）"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute stem."""
        path = params.get('path', '')
        output_var = params.get('output_var', 'stem_result')

        try:
            resolved_path = context.resolve_value(path) if isinstance(path, str) else path
            p = pathlib.Path(resolved_path)
            stem = p.stem
            context.set_variable(output_var, stem)
            return ActionResult(success=True, message=f"stem: {stem}")
        except Exception as e:
            return ActionResult(success=False, message=f"stem failed: {e}")


class PathCreateAction(BaseAction):
    """Create file or directory."""
    action_type = "pathlib_create"
    display_name = "创建路径"
    description = "创建文件或目录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute create."""
        path = params.get('path', '')
        is_dir = params.get('is_dir', False)
        parents = params.get('parents', True)
        output_var = params.get('output_var', 'create_result')

        try:
            resolved_path = context.resolve_value(path) if isinstance(path, str) else path
            resolved_is_dir = context.resolve_value(is_dir) if isinstance(is_dir, str) else is_dir
            resolved_parents = context.resolve_value(parents) if isinstance(parents, str) else parents
            
            p = pathlib.Path(resolved_path)
            
            if resolved_is_dir:
                p.mkdir(parents=resolved_parents, exist_ok=True)
            else:
                p.parent.mkdir(parents=True, exist_ok=True)
                p.touch()
            
            context.set_variable(output_var, str(p))
            return ActionResult(success=True, message=f"created: {p}")
        except Exception as e:
            return ActionResult(success=False, message=f"create failed: {e}")


class PathReadWriteAction(BaseAction):
    """Read or write file."""
    action_type = "pathlib_read_write"
    display_name = "读写文件"
    description = "读取或写入文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute read/write."""
        path = params.get('path', '')
        mode = params.get('mode', 'read')
        content = params.get('content', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'read_write_result')

        try:
            resolved_path = context.resolve_value(path) if isinstance(path, str) else path
            resolved_mode = context.resolve_value(mode) if isinstance(mode, str) else mode
            resolved_content = context.resolve_value(content) if isinstance(content, str) else content
            resolved_encoding = context.resolve_value(encoding) if isinstance(encoding, str) else encoding
            
            p = pathlib.Path(resolved_path)
            
            if resolved_mode == 'read':
                result = p.read_text(encoding=resolved_encoding)
                context.set_variable(output_var, result)
                return ActionResult(success=True, message=f"read {len(result)} chars")
            elif resolved_mode == 'write':
                p.write_text(resolved_content, encoding=resolved_encoding)
                context.set_variable(output_var, len(resolved_content))
                return ActionResult(success=True, message=f"wrote {len(resolved_content)} chars")
            elif resolved_mode == 'append':
                p.append_text(resolved_content, encoding=resolved_encoding)
                context.set_variable(output_var, len(resolved_content))
                return ActionResult(success=True, message=f"appended {len(resolved_content)} chars")
            else:
                return ActionResult(success=False, message=f"unknown mode: {resolved_mode}")
        except Exception as e:
            return ActionResult(success=False, message=f"read/write failed: {e}")


class PathInfoAction(BaseAction):
    """Get path information."""
    action_type = "pathlib_info"
    display_name = "路径信息"
    description = "获取路径详细信息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute path info."""
        path = params.get('path', '')
        output_var = params.get('output_var', 'path_info_result')

        try:
            resolved_path = context.resolve_value(path) if isinstance(path, str) else path
            p = pathlib.Path(resolved_path)
            
            info = {
                "path": str(p),
                "name": p.name,
                "stem": p.stem,
                "suffix": p.suffix,
                "exists": p.exists(),
                "is_file": p.is_file() if p.exists() else False,
                "is_dir": p.is_dir() if p.exists() else False,
                "is_symlink": p.is_symlink(),
                "parent": str(p.parent),
            }
            
            if p.exists():
                stat = p.stat()
                info["size"] = stat.st_size
                info["modified"] = stat.st_mtime
            
            context.set_variable(output_var, info)
            return ActionResult(success=True, message=f"path info retrieved")
        except Exception as e:
            return ActionResult(success=False, message=f"path info failed: {e}")


class PathJoinAction(BaseAction):
    """Join path components."""
    action_type = "pathlib_join"
    display_name = "连接路径"
    description = "连接多个路径组件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute join."""
        parts = params.get('parts', [])
        output_var = params.get('output_var', 'join_result')

        try:
            resolved_parts = context.resolve_value(parts) if isinstance(parts, str) else parts
            
            if isinstance(resolved_parts, str):
                resolved_parts = resolved_parts.split('/')
            
            result = str(pathlib.Path(*resolved_parts))
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"joined: {result}")
        except Exception as e:
            return ActionResult(success=False, message=f"join failed: {e}")
