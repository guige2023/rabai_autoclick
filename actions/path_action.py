"""Path action module for RabAI AutoClick.

Provides path manipulation and directory management actions.
"""

import os
import shutil
import sys
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PathJoinAction(BaseAction):
    """Join path components.
    
    Combines path parts into a full path.
    """
    action_type = "path_join"
    display_name = "路径拼接"
    description = "拼接路径组件"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Join path.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: parts (list), base.
        
        Returns:
            ActionResult with joined path.
        """
        parts = params.get('parts', [])
        base = params.get('base', '')
        
        if not parts:
            return ActionResult(success=False, message="parts required")
        
        if base:
            parts.insert(0, base)
        
        joined = os.path.join(*parts)
        normalized = os.path.normpath(joined)
        
        return ActionResult(
            success=True,
            message=f"Joined: {normalized}",
            data={'path': normalized, 'parts': parts}
        )


class PathExistsAction(BaseAction):
    """Check if path exists.
    
    Tests for file or directory existence.
    """
    action_type = "path_exists"
    display_name = "路径存在检查"
    description = "检查路径是否存在"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Check path exists.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: path, check_type (any/file/dir).
        
        Returns:
            ActionResult with existence status.
        """
        path = params.get('path', '')
        check_type = params.get('check_type', 'any')
        
        if not path:
            return ActionResult(success=False, message="path required")
        
        exists = os.path.exists(path)
        is_file = os.path.isfile(path) if exists else False
        is_dir = os.path.isdir(path) if exists else False
        
        if check_type == 'file':
            result = is_file
        elif check_type == 'dir':
            result = is_dir
        else:
            result = exists
        
        return ActionResult(
            success=True,
            message=f"{'Exists' if result else 'Not found'}: {path}",
            data={
                'path': path,
                'exists': exists,
                'is_file': is_file,
                'is_dir': is_dir,
                'result': result
            }
        )


class PathListDirAction(BaseAction):
    """List directory contents.
    
    Returns files and directories in a path.
    """
    action_type = "path_listdir"
    display_name = "列出目录内容"
    description = "列出目录中的文件和文件夹"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """List directory.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: path, pattern, include_hidden.
        
        Returns:
            ActionResult with file list.
        """
        path = params.get('path', '.')
        pattern = params.get('pattern', None)
        include_hidden = params.get('include_hidden', False)
        
        if not os.path.exists(path):
            return ActionResult(success=False, message=f"Path not found: {path}")
        
        if not os.path.isdir(path):
            return ActionResult(success=False, message=f"Not a directory: {path}")
        
        try:
            items = os.listdir(path)
            
            if not include_hidden:
                items = [i for i in items if not i.startswith('.')]
            
            if pattern:
                import fnmatch
                items = [i for i in items if fnmatch.fnmatch(i, pattern)]
            
            # Separate files and dirs
            files = [i for i in items if os.path.isfile(os.path.join(path, i))]
            dirs = [i for i in items if os.path.isdir(os.path.join(path, i))]
            
            return ActionResult(
                success=True,
                message=f"Listed {len(items)} item(s)",
                data={
                    'items': items,
                    'files': files,
                    'dirs': dirs,
                    'count': len(items)
                }
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"List error: {e}",
                data={'error': str(e)}
            )


class PathMakeDirAction(BaseAction):
    """Create directory.
    
    Creates directory with optional parents.
    """
    action_type = "path_mkdir"
    display_name = "创建目录"
    description = "创建目录"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Create directory.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: path, parents.
        
        Returns:
            ActionResult with creation status.
        """
        path = params.get('path', '')
        parents = params.get('parents', True)
        
        if not path:
            return ActionResult(success=False, message="path required")
        
        try:
            os.makedirs(path, exist_ok=parents)
            
            return ActionResult(
                success=True,
                message=f"Directory created: {path}",
                data={'path': path}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"mkdir error: {e}",
                data={'error': str(e)}
            )


class PathResolveAction(BaseAction):
    """Resolve path to absolute form.
    
    Resolves relative paths and symlinks.
    """
    action_type = "path_resolve"
    display_name = "解析绝对路径"
    description = "将相对路径解析为绝对路径"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Resolve path.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: path, resolve_symlinks.
        
        Returns:
            ActionResult with resolved path.
        """
        path = params.get('path', '')
        resolve_symlinks = params.get('resolve_symlinks', True)
        
        if not path:
            return ActionResult(success=False, message="path required")
        
        try:
            if resolve_symlinks:
                resolved = os.path.realpath(path)
            else:
                resolved = os.path.abspath(path)
            
            return ActionResult(
                success=True,
                message=f"Resolved: {resolved}",
                data={
                    'original': path,
                    'resolved': resolved,
                    'exists': os.path.exists(resolved)
                }
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Resolve error: {e}",
                data={'error': str(e)}
            )
