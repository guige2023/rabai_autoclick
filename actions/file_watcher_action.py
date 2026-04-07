"""File watcher action module for RabAI AutoClick.

Provides file system monitoring operations:
- FileWatchAction: Watch for file changes
- FileExistsAction: Check if file exists
- FileModifiedAction: Check if file was modified
- FileSizeAction: Get file size
- FileListAction: List files in directory
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FileWatchAction(BaseAction):
    """Watch for file changes."""
    action_type = "file_watch"
    display_name = "文件监控"
    description = "监控文件变化"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute file watch."""
        path = params.get('path', '')
        timeout = params.get('timeout', 30)  # seconds
        poll_interval = params.get('poll_interval', 1)
        output_var = params.get('output_var', 'watch_result')

        if not path:
            return ActionResult(success=False, message="path is required")

        try:
            import os as os_module

            resolved_path = context.resolve_value(path) if context else path
            resolved_timeout = context.resolve_value(timeout) if context else timeout
            resolved_poll = context.resolve_value(poll_interval) if context else poll_interval

            if not os_module.path.exists(resolved_path):
                return ActionResult(success=False, message=f"Path does not exist: {resolved_path}")

            mtime = os_module.path.getmtime(resolved_path)
            start_time = time.time()

            while time.time() - start_time < resolved_timeout:
                new_mtime = os_module.path.getmtime(resolved_path)
                if new_mtime != mtime:
                    result = {'changed': True, 'path': resolved_path, 'mtime': new_mtime}
                    if context:
                        context.set(output_var, result)
                    return ActionResult(success=True, message=f"File changed: {resolved_path}", data=result)
                time.sleep(resolved_poll)

            result = {'changed': False, 'path': resolved_path}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"No change detected in {resolved_timeout}s", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"File watch error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'timeout': 30, 'poll_interval': 1, 'output_var': 'watch_result'}


class FileExistsAction(BaseAction):
    """Check if file exists."""
    action_type = "file_exists"
    display_name = "文件存在检查"
    description = "检查文件是否存在"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute file exists check."""
        path = params.get('path', '')
        output_var = params.get('output_var', 'file_exists')

        if not path:
            return ActionResult(success=False, message="path is required")

        try:
            resolved = context.resolve_value(path) if context else path
            exists = os.path.exists(resolved)

            result = {'exists': exists, 'path': resolved}
            if context:
                context.set(output_var, exists)
            return ActionResult(success=True, message=f"{'Exists' if exists else 'Does not exist'}: {resolved}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"File exists error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'file_exists'}


class FileModifiedAction(BaseAction):
    """Check if file was modified since timestamp."""
    action_type = "file_modified"
    display_name = "文件修改检查"
    description = "检查文件是否已修改"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute file modified check."""
        path = params.get('path', '')
        since = params.get('since', None)  # timestamp
        output_var = params.get('output_var', 'file_modified')

        if not path:
            return ActionResult(success=False, message="path is required")

        try:
            resolved = context.resolve_value(path) if context else path
            resolved_since = context.resolve_value(since) if context else since

            if not os.path.exists(resolved):
                return ActionResult(success=False, message=f"Path does not exist: {resolved}")

            mtime = os.path.getmtime(resolved)
            since_ts = float(resolved_since) if resolved_since else time.time() - 86400

            modified = mtime > since_ts
            result = {'modified': modified, 'path': resolved, 'mtime': mtime, 'since': since_ts}

            if context:
                context.set(output_var, modified)
            return ActionResult(success=True, message=f"File {'modified' if modified else 'not modified'}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"File modified error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'since': None, 'output_var': 'file_modified'}


class FileSizeAction(BaseAction):
    """Get file size."""
    action_type = "file_size"
    display_name = "文件大小"
    description = "获取文件大小"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute file size."""
        path = params.get('path', '')
        unit = params.get('unit', 'bytes')  # bytes, kb, mb, gb
        output_var = params.get('output_var', 'file_size')

        if not path:
            return ActionResult(success=False, message="path is required")

        try:
            resolved = context.resolve_value(path) if context else path

            if not os.path.exists(resolved):
                return ActionResult(success=False, message=f"Path does not exist: {resolved}")

            size = os.path.getsize(resolved)
            unit_map = {'bytes': 1, 'kb': 1024, 'mb': 1024**2, 'gb': 1024**3}
            divisor = unit_map.get(unit, 1)
            size_formatted = size / divisor

            result = {'size': size, 'size_formatted': size_formatted, 'unit': unit, 'path': resolved}
            if context:
                context.set(output_var, size_formatted)
            return ActionResult(success=True, message=f"Size: {size_formatted:.2f} {unit}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"File size error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'unit': 'bytes', 'output_var': 'file_size'}


class FileListAction(BaseAction):
    """List files in directory."""
    action_type = "file_list"
    display_name = "文件列表"
    description = "列出目录文件"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute file list."""
        directory = params.get('directory', '.')
        pattern = params.get('pattern', '*')
        recursive = params.get('recursive', False)
        output_var = params.get('output_var', 'file_list')

        try:
            import glob as glob_module

            resolved_dir = context.resolve_value(directory) if context else directory
            resolved_pattern = context.resolve_value(pattern) if context else pattern
            resolved_recursive = context.resolve_value(recursive) if context else recursive

            if resolved_recursive:
                search_pattern = os.path.join(resolved_dir, '**', resolved_pattern)
                files = glob_module.glob(search_pattern, recursive=True)
            else:
                search_pattern = os.path.join(resolved_dir, resolved_pattern)
                files = glob_module.glob(search_pattern)

            files = [f for f in files if os.path.isfile(f)]
            result = {'files': files, 'count': len(files)}
            if context:
                context.set(output_var, files)
            return ActionResult(success=True, message=f"Found {len(files)} files", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"File list error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'directory': '.', 'pattern': '*', 'recursive': False, 'output_var': 'file_list'}
