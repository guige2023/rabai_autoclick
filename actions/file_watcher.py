"""File watcher action module for RabAI AutoClick.

Provides file system monitoring actions for watching file changes,
directory modifications, and file system events.
"""

import os
import sys
import time
import hashlib
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class WatchFileAction(BaseAction):
    """Watch a file for modifications.
    
    Monitors file modification time and content hash to detect changes.
    Supports polling-based monitoring with configurable interval.
    """
    action_type = "watch_file"
    display_name = "监控文件"
    description = "监控文件变化，支持修改时间检测和内容hash对比"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Watch a file for changes.
        
        Args:
            context: Execution context.
            params: Dict with keys: path, watch_mode, interval,
                   timeout, save_to_var.
        
        Returns:
            ActionResult with file status information.
        """
        path = params.get('path', '')
        watch_mode = params.get('watch_mode', 'mtime')
        interval = params.get('interval', 1.0)
        timeout = params.get('timeout', 30)
        save_to_var = params.get('save_to_var', None)

        if not path:
            return ActionResult(success=False, message="文件路径不能为空")

        if not os.path.exists(path):
            return ActionResult(success=False, message=f"文件不存在: {path}")

        if not os.path.isfile(path):
            return ActionResult(success=False, message=f"路径不是文件: {path}")

        try:
            # Get initial state
            initial_stat = os.stat(path)
            initial_mtime = initial_stat.st_mtime
            initial_size = initial_stat.st_size

            initial_hash = None
            if watch_mode == 'hash':
                with open(path, 'rb') as f:
                    initial_hash = hashlib.md5(f.read()).hexdigest()

            result_data = {
                'path': path,
                'initial_mtime': initial_mtime,
                'initial_size': initial_size,
                'initial_hash': initial_hash,
                'watch_mode': watch_mode,
                'changed': False
            }

            start_time = time.time()
            while time.time() - start_time < timeout:
                time.sleep(interval)
                
                if not os.path.exists(path):
                    result_data['changed'] = True
                    result_data['reason'] = 'deleted'
                    break

                current_stat = os.stat(path)
                current_mtime = current_stat.st_mtime
                current_size = current_stat.st_size

                if watch_mode == 'mtime':
                    if current_mtime > initial_mtime:
                        result_data['changed'] = True
                        result_data['reason'] = 'modified'
                        result_data['new_mtime'] = current_mtime
                        break
                elif watch_mode == 'size':
                    if current_size != initial_size:
                        result_data['changed'] = True
                        result_data['reason'] = 'size_changed'
                        result_data['new_size'] = current_size
                        break
                elif watch_mode == 'hash':
                    with open(path, 'rb') as f:
                        current_hash = hashlib.md5(f.read()).hexdigest()
                    if current_hash != initial_hash:
                        result_data['changed'] = True
                        result_data['reason'] = 'content_changed'
                        result_data['new_hash'] = current_hash
                        break

            result_data['elapsed'] = time.time() - start_time

            if save_to_var:
                context.variables[save_to_var] = result_data

            if result_data['changed']:
                return ActionResult(
                    success=True,
                    message=f"文件已变化: {result_data.get('reason')}",
                    data=result_data
                )
            else:
                return ActionResult(
                    success=True,
                    message=f"监控超时({timeout}s)未检测到变化",
                    data=result_data
                )

        except PermissionError:
            return ActionResult(
                success=False,
                message=f"无权限访问文件: {path}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"文件监控异常: {str(e)}",
                data={'error': str(e)}
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'watch_mode': 'mtime',
            'interval': 1.0,
            'timeout': 30,
            'save_to_var': None
        }


class WatchDirectoryAction(BaseAction):
    """Watch a directory for file changes.
    
    Monitors directory for new, modified, or deleted files.
    Supports recursive monitoring and file pattern filtering.
    """
    action_type = "watch_directory"
    display_name = "监控目录"
    description = "监控目录变化，支持新文件检测和模式过滤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Watch a directory for changes.
        
        Args:
            context: Execution context.
            params: Dict with keys: path, recursive, patterns,
                   timeout, save_to_var.
        
        Returns:
            ActionResult with directory change information.
        """
        path = params.get('path', '')
        recursive = params.get('recursive', False)
        patterns = params.get('patterns', ['*'])
        timeout = params.get('timeout', 30)
        save_to_var = params.get('save_to_var', None)

        if not path:
            return ActionResult(success=False, message="目录路径不能为空")

        if not os.path.exists(path):
            return ActionResult(success=False, message=f"目录不存在: {path}")

        if not os.path.isdir(path):
            return ActionResult(success=False, message=f"路径不是目录: {path}")

        def get_files(dir_path, recurse):
            """Get all files in directory."""
            files = {}
            try:
                for entry in os.scandir(dir_path):
                    if entry.is_file():
                        files[entry.path] = entry.stat().st_mtime
                    elif entry.is_dir() and recurse:
                        files.update(get_files(entry.path, recurse))
            except PermissionError:
                pass
            return files

        def matches_patterns(file_path, patterns):
            """Check if file matches any pattern."""
            import fnmatch
            filename = os.path.basename(file_path)
            for pattern in patterns:
                if fnmatch.fnmatch(filename, pattern):
                    return True
            return False

        # Get initial file list
        initial_files = {
            f: mtime for f, mtime in get_files(path, recursive).items()
            if matches_patterns(f, patterns)
        }

        result_data = {
            'path': path,
            'recursive': recursive,
            'patterns': patterns,
            'initial_count': len(initial_files),
            'changed': False,
            'changes': []
        }

        start_time = time.time()
        check_interval = 1.0

        while time.time() - start_time < timeout:
            time.sleep(check_interval)

            current_files = {
                f: mtime for f, mtime in get_files(path, recursive).items()
                if matches_patterns(f, patterns)
            }

            # Detect new files
            new_files = set(current_files.keys()) - set(initial_files.keys())
            for f in new_files:
                result_data['changed'] = True
                result_data['changes'].append({
                    'type': 'created',
                    'path': f,
                    'mtime': current_files[f]
                })

            # Detect deleted files
            deleted_files = set(initial_files.keys()) - set(current_files.keys())
            for f in deleted_files:
                result_data['changed'] = True
                result_data['changes'].append({
                    'type': 'deleted',
                    'path': f
                })

            # Detect modified files
            for f in set(current_files.keys()) & set(initial_files.keys()):
                if current_files[f] > initial_files[f]:
                    result_data['changed'] = True
                    result_data['changes'].append({
                        'type': 'modified',
                        'path': f,
                        'old_mtime': initial_files[f],
                        'new_mtime': current_files[f]
                    })

            if result_data['changed']:
                break

            # Update baseline for next check
            initial_files = current_files

        result_data['elapsed'] = time.time() - start_time

        if save_to_var:
            context.variables[save_to_var] = result_data

        if result_data['changed']:
            return ActionResult(
                success=True,
                message=f"检测到目录变化: {len(result_data['changes'])} 项",
                data=result_data
            )
        else:
            return ActionResult(
                success=True,
                message=f"监控超时({timeout}s)未检测到变化",
                data=result_data
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'recursive': False,
            'patterns': ['*'],
            'timeout': 30,
            'save_to_var': None
        }
