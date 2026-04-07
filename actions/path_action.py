"""Path and filesystem utilities action module for RabAI AutoClick.

Provides path operations:
- PathExistsAction: Check if path exists
- PathJoinAction: Join path components
- PathDirnameAction: Get directory name
- PathBasenameAction: Get file name
- PathSplitAction: Split path
- PathExpandAction: Expand user and variables
- PathNormalizeAction: Normalize path
- PathGlobAction: Glob pattern matching
"""

from __future__ import annotations

import os
import sys
import glob as glob_module
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PathExistsAction(BaseAction):
    """Check if path exists."""
    action_type = "path_exists"
    display_name = "路径存在"
    description = "检查路径是否存在"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute path exists check."""
        path = params.get('path', '')
        check_type = params.get('check_type', None)  # None, file, dir
        output_var = params.get('output_var', 'path_exists')

        if not path:
            return ActionResult(success=False, message="path is required")

        try:
            resolved_path = context.resolve_value(path) if context else path
            expanded = _os.path.expanduser(_os.path.expandvars(resolved_path))

            if check_type == 'file':
                exists = _os.path.isfile(expanded)
            elif check_type == 'dir':
                exists = _os.path.isdir(expanded)
            else:
                exists = _os.path.exists(expanded)

            result = {'exists': exists, 'path': resolved_path, 'type': check_type}
            if context:
                context.set(output_var, exists)
            return ActionResult(success=True, message=f"{resolved_path}: {'exists' if exists else 'not found'}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Path exists error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'check_type': None, 'output_var': 'path_exists'}


class PathJoinAction(BaseAction):
    """Join path components."""
    action_type = "path_join"
    display_name = "路径拼接"
    description = "拼接路径"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute path join."""
        parts = params.get('parts', [])
        output_var = params.get('output_var', 'joined_path')

        if not parts:
            return ActionResult(success=False, message="parts is required")

        try:
            resolved_parts = context.resolve_value(parts) if context else parts
            joined = _os.path.join(*resolved_parts)

            if context:
                context.set(output_var, joined)
            return ActionResult(success=True, message=joined, data={'path': joined})
        except Exception as e:
            return ActionResult(success=False, message=f"Path join error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['parts']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'joined_path'}


class PathDirnameAction(BaseAction):
    """Get directory name of path."""
    action_type = "path_dirname"
    display_name = "路径目录名"
    description = "获取路径的目录"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute dirname."""
        path = params.get('path', '')
        output_var = params.get('output_var', 'dirname')

        if not path:
            return ActionResult(success=False, message="path is required")

        try:
            resolved_path = context.resolve_value(path) if context else path
            dirname = _os.path.dirname(resolved_path)

            if context:
                context.set(output_var, dirname)
            return ActionResult(success=True, message=dirname, data={'dirname': dirname})
        except Exception as e:
            return ActionResult(success=False, message=f"Dirname error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dirname'}


class PathBasenameAction(BaseAction):
    """Get basename of path."""
    action_type = "path_basename"
    display_name = "路径文件名"
    description = "获取路径的文件名"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute basename."""
        path = params.get('path', '')
        output_var = params.get('output_var', 'basename')

        if not path:
            return ActionResult(success=False, message="path is required")

        try:
            resolved_path = context.resolve_value(path) if context else path
            basename = _os.path.basename(resolved_path)

            if context:
                context.set(output_var, basename)
            return ActionResult(success=True, message=basename, data={'basename': basename})
        except Exception as e:
            return ActionResult(success=False, message=f"Basename error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'basename'}


class PathSplitAction(BaseAction):
    """Split path into dirname and basename."""
    action_type = "path_split"
    display_name = "路径拆分"
    description = "拆分路径"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute path split."""
        path = params.get('path', '')
        output_var = params.get('output_var', 'split_path')

        if not path:
            return ActionResult(success=False, message="path is required")

        try:
            resolved_path = context.resolve_value(path) if context else path
            dirname, basename = _os.path.split(resolved_path)

            result = {'dirname': dirname, 'basename': basename}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"{dirname} / {basename}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Path split error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'split_path'}


class PathExpandAction(BaseAction):
    """Expand user and variables in path."""
    action_type = "path_expand"
    display_name = "路径展开"
    description = "展开~和环境变量"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute path expand."""
        path = params.get('path', '')
        output_var = params.get('output_var', 'expanded_path')

        if not path:
            return ActionResult(success=False, message="path is required")

        try:
            resolved_path = context.resolve_value(path) if context else path
            expanded = _os.path.expanduser(resolved_path)
            expanded = _os.path.expandvars(expanded)

            if context:
                context.set(output_var, expanded)
            return ActionResult(success=True, message=expanded, data={'path': expanded})
        except Exception as e:
            return ActionResult(success=False, message=f"Path expand error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'expanded_path'}


class PathNormalizeAction(BaseAction):
    """Normalize path."""
    action_type = "path_normalize"
    display_name = "路径标准化"
    description = "标准化路径"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute path normalize."""
        path = params.get('path', '')
        output_var = params.get('output_var', 'normalized_path')

        if not path:
            return ActionResult(success=False, message="path is required")

        try:
            resolved_path = context.resolve_value(path) if context else path
            expanded = _os.path.expanduser(resolved_path)
            normalized = _os.path.normpath(expanded)

            if context:
                context.set(output_var, normalized)
            return ActionResult(success=True, message=normalized, data={'path': normalized})
        except Exception as e:
            return ActionResult(success=False, message=f"Path normalize error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'normalized_path'}


class PathGlobAction(BaseAction):
    """Glob pattern matching."""
    action_type = "path_glob"
    display_name = "路径Glob匹配"
    description = "Glob模式匹配文件"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute glob."""
        pattern = params.get('pattern', '')
        base_dir = params.get('base_dir', None)
        recursive = params.get('recursive', False)
        output_var = params.get('output_var', 'glob_matches')

        if not pattern:
            return ActionResult(success=False, message="pattern is required")

        try:
            resolved_pattern = context.resolve_value(pattern) if context else pattern
            resolved_base = context.resolve_value(base_dir) if context else base_dir
            resolved_recursive = context.resolve_value(recursive) if context else recursive

            if resolved_base:
                full_pattern = _os.path.join(resolved_base, resolved_pattern)
            else:
                full_pattern = resolved_pattern

            if resolved_recursive and '**' not in full_pattern:
                full_pattern = _os.path.join(full_pattern, '**', resolved_pattern.split(_os.sep)[-1])

            matches = glob_module.glob(full_pattern, recursive=resolved_recursive)
            matches = sorted(matches)

            result = {'matches': matches, 'count': len(matches)}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Glob: {len(matches)} matches", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Glob error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['pattern']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'base_dir': None, 'recursive': False, 'output_var': 'glob_matches'}
