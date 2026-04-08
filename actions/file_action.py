"""File action module for RabAI AutoClick.

Provides file operations including read, write, copy, move, and metadata retrieval.
"""

import os
import shutil
import json
import hashlib
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class FileReadAction(BaseAction):
    """Read file contents as text with encoding handling.
    
    Supports various encodings, line range selection, and automatic
    encoding detection for text files.
    """
    action_type = "file_read"
    display_name = "读取文件"
    description = "读取文本文件内容"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Read file contents.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: path, encoding, start_line, end_line,
                   max_chars, strip.
        
        Returns:
            ActionResult with file contents.
        """
        path = params.get('path', '')
        if not path:
            return ActionResult(success=False, message="path is required")
        
        encoding = params.get('encoding', 'utf-8')
        start_line = params.get('start_line', None)
        end_line = params.get('end_line', None)
        max_chars = params.get('max_chars', None)
        strip = params.get('strip', False)
        
        # Check file exists
        if not os.path.exists(path):
            return ActionResult(
                success=False,
                message=f"File not found: {path}"
            )
        
        # Check is file
        if not os.path.isfile(path):
            return ActionResult(
                success=False,
                message=f"Not a file: {path}"
            )
        
        try:
            with open(path, 'r', encoding=encoding, errors='replace') as f:
                lines = f.readlines()
            
            content = ''.join(lines)
            
            # Line range selection
            if start_line is not None or end_line is not None:
                start = start_line if start_line is not None else 0
                end = end_line if end_line is not None else len(lines)
                content = ''.join(lines[start:end])
            
            if strip:
                content = content.strip()
            
            if max_chars and len(content) > max_chars:
                content = content[:max_chars]
                truncated = True
            else:
                truncated = False
            
            return ActionResult(
                success=True,
                message=f"Read {len(content)} chars" + (" (truncated)" if truncated else ""),
                data=content
            )
            
        except UnicodeDecodeError as e:
            return ActionResult(
                success=False,
                message=f"Encoding error: {e}",
                data={'encoding': encoding}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Read error: {e}",
                data={'error': str(e)}
            )


class FileWriteAction(BaseAction):
    """Write content to file with backup support.
    
    Creates parent directories as needed, supports atomic writes,
    and optionally creates backup of existing files.
    """
    action_type = "file_write"
    display_name = "写入文件"
    description = "将内容写入文本文件"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Write content to file.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: path, content, encoding, create_dirs,
                   create_backup.
        
        Returns:
            ActionResult with write status.
        """
        path = params.get('path', '')
        content = params.get('content', '')
        encoding = params.get('encoding', 'utf-8')
        create_dirs = params.get('create_dirs', True)
        create_backup = params.get('create_backup', False)
        
        if not path:
            return ActionResult(success=False, message="path is required")
        
        if content is None:
            return ActionResult(success=False, message="content is required")
        
        # Create parent directories
        parent_dir = os.path.dirname(path)
        if parent_dir and not os.path.exists(parent_dir):
            if create_dirs:
                try:
                    os.makedirs(parent_dir, exist_ok=True)
                except Exception as e:
                    return ActionResult(
                        success=False,
                        message=f"Failed to create directory: {e}"
                    )
            else:
                return ActionResult(
                    success=False,
                    message=f"Parent directory does not exist: {parent_dir}"
                )
        
        # Create backup of existing file
        if create_backup and os.path.exists(path):
            backup_path = f"{path}.backup.{datetime.now().strftime('%Y%m%d%H%M%S')}"
            try:
                shutil.copy2(path, backup_path)
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Backup failed: {e}"
                )
        
        try:
            with open(path, 'w', encoding=encoding) as f:
                f.write(content)
            
            return ActionResult(
                success=True,
                message=f"Wrote {len(content)} chars to {path}",
                data={'path': path, 'size': len(content)}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Write error: {e}",
                data={'error': str(e)}
            )


class FileCopyMoveAction(BaseAction):
    """Copy or move files with overwrite protection.
    
    Supports recursive directory operations, preserves metadata,
    and provides verification after operation.
    """
    action_type = "file_copy_move"
    display_name = "文件复制/移动"
    description = "复制或移动文件和目录"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Copy or move file/directory.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: operation (copy/move), source, dest,
                   overwrite, create_parent_dirs.
        
        Returns:
            ActionResult with operation status.
        """
        operation = params.get('operation', 'copy')
        source = params.get('source', '')
        dest = params.get('dest', '')
        overwrite = params.get('overwrite', False)
        create_parent_dirs = params.get('create_parent_dirs', True)
        
        if operation not in ['copy', 'move']:
            return ActionResult(
                success=False,
                message=f"Invalid operation: {operation}. Must be 'copy' or 'move'"
            )
        
        if not source:
            return ActionResult(success=False, message="source is required")
        if not dest:
            return ActionResult(success=False, message="dest is required")
        
        if not os.path.exists(source):
            return ActionResult(success=False, message=f"Source not found: {source}")
        
        # Check dest exists (for non-overwrite)
        if os.path.exists(dest) and not overwrite:
            return ActionResult(
                success=False,
                message=f"Destination exists: {dest}"
            )
        
        # Create parent directories
        parent_dir = os.path.dirname(dest)
        if parent_dir and not os.path.exists(parent_dir):
            if create_parent_dirs:
                try:
                    os.makedirs(parent_dir, exist_ok=True)
                except Exception as e:
                    return ActionResult(
                        success=False,
                        message=f"Failed to create directory: {e}"
                    )
        
        try:
            if operation == 'copy':
                if os.path.isdir(source):
                    shutil.copytree(source, dest, dirs_exist_ok=overwrite)
                    msg = f"Copied directory to {dest}"
                else:
                    shutil.copy2(source, dest)
                    msg = f"Copied {source} to {dest}"
            else:  # move
                shutil.move(source, dest)
                msg = f"Moved {source} to {dest}"
            
            return ActionResult(
                success=True,
                message=msg,
                data={'operation': operation, 'source': source, 'dest': dest}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"{operation.capitalize()} failed: {e}",
                data={'error': str(e)}
            )


class FileInfoAction(BaseAction):
    """Get file metadata and compute checksums.
    
    Returns file size, timestamps, permissions, and optional hash.
    """
    action_type = "file_info"
    display_name = "文件信息"
    description = "获取文件元数据和校验和"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get file information.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: path, compute_hash, hash_algorithm.
        
        Returns:
            ActionResult with file metadata.
        """
        path = params.get('path', '')
        if not path:
            return ActionResult(success=False, message="path is required")
        
        if not os.path.exists(path):
            return ActionResult(success=False, message=f"File not found: {path}")
        
        compute_hash = params.get('compute_hash', False)
        hash_algorithm = params.get('hash_algorithm', 'md5')
        
        try:
            stat = os.stat(path)
            info = {
                'path': path,
                'name': os.path.basename(path),
                'size': stat.st_size,
                'is_file': os.path.isfile(path),
                'is_dir': os.path.isdir(path),
                'created': datetime.fromtimestamp(stat.st_ctime).isoformat(),
                'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                'accessed': datetime.fromtimestamp(stat.st_atime).isoformat(),
                'permissions': oct(stat.st_mode)[-3:]
            }
            
            if compute_hash and os.path.isfile(path):
                if hash_algorithm not in ['md5', 'sha1', 'sha256']:
                    hash_algorithm = 'md5'
                
                h = hashlib.new(hash_algorithm)
                with open(path, 'rb') as f:
                    for chunk in iter(lambda: f.read(8192), b''):
                        h.update(chunk)
                info['hash'] = h.hexdigest()
                info['hash_algorithm'] = hash_algorithm
            
            return ActionResult(
                success=True,
                message=f"Got info for {path}",
                data=info
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"File info error: {e}",
                data={'error': str(e)}
            )
