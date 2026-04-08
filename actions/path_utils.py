"""Path utilities action module for RabAI AutoClick.

Provides file system path manipulation operations
including path resolution, traversal, and validation.
"""

import os
import sys
import glob
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PathResolveAction(BaseAction):
    """Resolve and validate file system paths.
    
    Supports absolute path, symlink resolution,
    and path component extraction.
    """
    action_type = "path_resolve"
    display_name = "路径解析"
    description = "解析和验证文件系统路径"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Resolve path.
        
        Args:
            context: Execution context.
            params: Dict with keys: path, resolve_symlinks,
                   must_exist, save_to_var.
        
        Returns:
            ActionResult with resolved path.
        """
        path = params.get('path', '')
        resolve_symlinks = params.get('resolve_symlinks', False)
        must_exist = params.get('must_exist', False)
        save_to_var = params.get('save_to_var', None)

        if not path:
            return ActionResult(success=False, message="Path is required")

        try:
            p = Path(path)

            if must_exist and not p.exists():
                return ActionResult(
                    success=False,
                    message=f"Path does not exist: {path}"
                )

            if resolve_symlinks:
                resolved = p.resolve()
            else:
                resolved = p.absolute()

            result_data = {
                'path': str(resolved),
                'name': p.name,
                'stem': p.stem,
                'suffix': p.suffix,
                'parent': str(p.parent),
                'exists': p.exists(),
                'is_file': p.is_file() if p.exists() else None,
                'is_dir': p.is_dir() if p.exists() else None,
                'is_symlink': p.is_symlink()
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"路径解析: {result_data['path']}",
                data=result_data
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"路径解析失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'resolve_symlinks': False,
            'must_exist': False,
            'save_to_var': None
        }


class PathGlobAction(BaseAction):
    """Glob pattern matching for files.
    
    Supports recursive patterns, multiple extensions,
    and exclusion patterns.
    """
    action_type = "path_glob"
    display_name = "路径匹配"
    description = "使用glob模式匹配文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Glob files.
        
        Args:
            context: Execution context.
            params: Dict with keys: pattern, base_dir,
                   recursive, save_to_var.
        
        Returns:
            ActionResult with matched files.
        """
        pattern = params.get('pattern', '*')
        base_dir = params.get('base_dir', '.')
        recursive = params.get('recursive', False)
        save_to_var = params.get('save_to_var', None)

        if not pattern:
            return ActionResult(success=False, message="Pattern is required")

        try:
            if recursive:
                pattern = f"**/{pattern}"

            matches = glob.glob(
                pattern,
                rootdir=base_dir,
                recursive=recursive
            )

            # Filter to files only
            files = []
            for m in matches:
                full_path = os.path.join(base_dir, m)
                if os.path.isfile(full_path):
                    files.append(m)

            result_data = {
                'files': files,
                'count': len(files),
                'pattern': pattern,
                'base_dir': base_dir
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"匹配 {len(files)} 个文件: {pattern}",
                data=result_data
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Glob失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pattern']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'base_dir': '.',
            'recursive': False,
            'save_to_var': None
        }


class PathWalkAction(BaseAction):
    """Walk directory tree.
    
    Traverses directories recursively and collects
    file information.
    """
    action_type = "path_walk"
    display_name = "目录遍历"
    description = "递归遍历目录树"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Walk directory tree.
        
        Args:
            context: Execution context.
            params: Dict with keys: root_dir, max_depth,
                   include_files, include_dirs, save_to_var.
        
        Returns:
            ActionResult with directory tree.
        """
        root_dir = params.get('root_dir', '.')
        max_depth = params.get('max_depth', None)
        include_files = params.get('include_files', True)
        include_dirs = params.get('include_dirs', True)
        save_to_var = params.get('save_to_var', None)

        if not os.path.exists(root_dir):
            return ActionResult(success=False, message=f"Directory not found: {root_dir}")

        if not os.path.isdir(root_dir):
            return ActionResult(success=False, message=f"Not a directory: {root_dir}")

        tree = []
        file_count = 0
        dir_count = 0

        for dirpath, dirnames, filenames in os.walk(root_dir):
            depth = dirpath[len(root_dir):].count(os.sep)
            if max_depth and depth >= max_depth:
                dirnames.clear()
                continue

            rel_path = os.path.relpath(dirpath, root_dir)

            if include_dirs:
                tree.append({
                    'type': 'directory',
                    'path': rel_path,
                    'name': os.path.basename(dirpath) or root_dir
                })
                dir_count += 1

            if include_files:
                for f in filenames:
                    tree.append({
                        'type': 'file',
                        'path': os.path.join(rel_path, f),
                        'name': f,
                        'size': os.path.getsize(os.path.join(dirpath, f))
                    })
                    file_count += 1

        result_data = {
            'tree': tree,
            'file_count': file_count,
            'dir_count': dir_count,
            'root': root_dir
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"遍历完成: {dir_count} 目录, {file_count} 文件",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['root_dir']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'max_depth': None,
            'include_files': True,
            'include_dirs': True,
            'save_to_var': None
        }
