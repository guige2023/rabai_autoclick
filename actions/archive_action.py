"""Archive action module for RabAI AutoClick.

Provides archive operations including zip, tar, and unzip.
"""

import zipfile
import tarfile
import sys
import os
import shutil
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ArchiveCreateAction(BaseAction):
    """Create archive from files.
    
    Creates ZIP or TAR archives.
    """
    action_type = "archive_create"
    display_name = "创建压缩包"
    description = "创建ZIP或TAR压缩包"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Create archive.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: archive_path, files, base_dir,
                   compression, format.
        
        Returns:
            ActionResult with creation status.
        """
        archive_path = params.get('archive_path', '')
        files = params.get('files', [])
        base_dir = params.get('base_dir', None)
        compression = params.get('compression', 'deflate')
        format_type = params.get('format', 'zip')
        
        if not archive_path:
            return ActionResult(success=False, message="archive_path required")
        
        if not files:
            return ActionResult(success=False, message="files required")
        
        if format_type == 'zip':
            try:
                with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                    for file_path in files:
                        if os.path.exists(file_path):
                            arcname = os.path.basename(file_path)
                            if base_dir and file_path.startswith(base_dir):
                                arcname = os.path.relpath(file_path, base_dir)
                            zf.write(file_path, arcname)
                
                size = os.path.getsize(archive_path)
                return ActionResult(
                    success=True,
                    message=f"ZIP archive created: {len(files)} files",
                    data={'archive_path': archive_path, 'size': size, 'file_count': len(files)}
                )
                
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Archive error: {e}",
                    data={'error': str(e)}
                )
        
        elif format_type == 'tar':
            comp_mode = 'w:gz' if compression == 'gzip' else 'w'
            if compression == 'bzip2':
                comp_mode = 'w:bz2'
            
            try:
                with tarfile.open(archive_path, comp_mode) as tf:
                    for file_path in files:
                        if os.path.exists(file_path):
                            arcname = os.path.basename(file_path)
                            if base_dir and file_path.startswith(base_dir):
                                arcname = os.path.relpath(file_path, base_dir)
                            tf.add(file_path, arcname)
                
                size = os.path.getsize(archive_path)
                return ActionResult(
                    success=True,
                    message=f"TAR archive created: {len(files)} files",
                    data={'archive_path': archive_path, 'size': size, 'file_count': len(files)}
                )
                
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Archive error: {e}",
                    data={'error': str(e)}
                )
        
        return ActionResult(success=False, message=f"Unknown format: {format_type}")


class ArchiveExtractAction(BaseAction):
    """Extract archive contents.
    
    Extracts ZIP or TAR archives.
    """
    action_type = "archive_extract"
    display_name = "解压文件"
    description = "解压ZIP或TAR压缩包"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Extract archive.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: archive_path, dest_dir, members.
        
        Returns:
            ActionResult with extraction status.
        """
        archive_path = params.get('archive_path', '')
        dest_dir = params.get('dest_dir', '')
        members = params.get('members', None)
        
        if not archive_path:
            return ActionResult(success=False, message="archive_path required")
        
        if not os.path.exists(archive_path):
            return ActionResult(success=False, message=f"Archive not found: {archive_path}")
        
        if not dest_dir:
            dest_dir = os.path.dirname(archive_path)
        
        os.makedirs(dest_dir, exist_ok=True)
        
        try:
            if archive_path.endswith('.zip'):
                with zipfile.ZipFile(archive_path, 'r') as zf:
                    if members:
                        zf.extractall(dest_dir, members=members)
                    else:
                        zf.extractall(dest_dir)
                    extracted = len(zf.namelist())
            
            elif archive_path.endswith(('.tar', '.tar.gz', '.tgz', '.tar.bz2')):
                mode = 'r:*'
                if archive_path.endswith('.gz'):
                    mode = 'r:gz'
                elif archive_path.endswith('.bz2'):
                    mode = 'r:bz2'
                
                with tarfile.open(archive_path, mode) as tf:
                    if members:
                        for m in members:
                            tf.extract(m, dest_dir)
                    else:
                        tf.extractall(dest_dir)
                    extracted = len(tf.getnames())
            else:
                return ActionResult(
                    success=False,
                    message="Unsupported archive format"
                )
            
            return ActionResult(
                success=True,
                message=f"Extracted {extracted} file(s) to {dest_dir}",
                data={'dest_dir': dest_dir, 'extracted_count': extracted}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Extract error: {e}",
                data={'error': str(e)}
            )


class ArchiveListAction(BaseAction):
    """List archive contents.
    
    Shows files in archive without extracting.
    """
    action_type = "archive_list"
    display_name = "列出压缩包内容"
    description = "列出压缩包内的文件"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """List archive.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: archive_path, pattern.
        
        Returns:
            ActionResult with file list.
        """
        archive_path = params.get('archive_path', '')
        pattern = params.get('pattern', None)
        
        if not archive_path:
            return ActionResult(success=False, message="archive_path required")
        
        if not os.path.exists(archive_path):
            return ActionResult(success=False, message=f"Archive not found: {archive_path}")
        
        try:
            if archive_path.endswith('.zip'):
                with zipfile.ZipFile(archive_path, 'r') as zf:
                    files = zf.namelist()
            
            elif archive_path.endswith(('.tar', '.tar.gz', '.tgz', '.tar.bz2')):
                with tarfile.open(archive_path, 'r:*') as tf:
                    files = tf.getnames()
            else:
                return ActionResult(success=False, message="Unsupported archive format")
            
            if pattern:
                import fnmatch
                files = [f for f in files if fnmatch.fnmatch(f, pattern)]
            
            return ActionResult(
                success=True,
                message=f"Found {len(files)} file(s) in archive",
                data={'files': files, 'count': len(files)}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"List error: {e}",
                data={'error': str(e)}
            )
