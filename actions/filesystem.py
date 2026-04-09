"""Filesystem action module for RabAI AutoClick.

Provides filesystem operations:
- FileReadAction: Read file contents
- FileWriteAction: Write content to file
- FileExistsAction: Check if file exists
- FileDeleteAction: Delete file
- FileCopyAction: Copy file
- DirCreateAction: Create directory
- FileListAction: List directory contents
- FileMoveAction: Move file
"""

import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

from rabai_autoclick.core.base_action import BaseAction, ActionResult


class FileReadAction(BaseAction):
    """Read contents of a file."""
    action_type = "file_read"
    display_name = "读取文件"
    description = "读取文件内容"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file read.
        
        Args:
            context: Execution context.
            params: Dict with path, encoding.
            
        Returns:
            ActionResult with file contents.
        """
        path = params.get('path')
        encoding = params.get('encoding', 'utf-8')
        
        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(encoding, str, 'encoding')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            path_obj = Path(path)
            if not path_obj.exists():
                return ActionResult(success=False, message=f"文件不存在: {path}")
            
            if not path_obj.is_file():
                return ActionResult(success=False, message=f"路径不是文件: {path}")
            
            content = path_obj.read_text(encoding=encoding)
            
            return ActionResult(
                success=True,
                message=f"文件读取成功: {path}",
                data={'content': content, 'path': str(path_obj.absolute())}
            )
        except UnicodeDecodeError as e:
            return ActionResult(
                success=False,
                message=f"文件编码错误，请尝试其他编码: {str(e)}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"文件读取失败: {str(e)}")
    
    def get_required_params(self) -> List[str]:
        return ['path']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'encoding': 'utf-8'
        }


class FileWriteAction(BaseAction):
    """Write content to a file."""
    action_type = "file_write"
    display_name = "写入文件"
    description = "写入内容到文件"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file write.
        
        Args:
            context: Execution context.
            params: Dict with path, content, encoding, append.
            
        Returns:
            ActionResult indicating success or failure.
        """
        path = params.get('path')
        content = params.get('content', '')
        encoding = params.get('encoding', 'utf-8')
        append = params.get('append', False)
        
        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(content, str, 'content')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(append, bool, 'append')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            path_obj = Path(path)
            path_obj.parent.mkdir(parents=True, exist_ok=True)
            
            mode = 'a' if append else 'w'
            path_obj.write_text(content, encoding=encoding)
            
            return ActionResult(
                success=True,
                message=f"文件写入成功: {path}",
                data={'path': str(path_obj.absolute()), 'bytes_written': len(content.encode(encoding))}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"文件写入失败: {str(e)}")
    
    def get_required_params(self) -> List[str]:
        return ['path', 'content']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'encoding': 'utf-8',
            'append': False
        }


class FileExistsAction(BaseAction):
    """Check if a file or directory exists."""
    action_type = "file_exists"
    display_name = "文件存在检查"
    description = "检查文件或目录是否存在"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file existence check.
        
        Args:
            context: Execution context.
            params: Dict with path, output_var.
            
        Returns:
            ActionResult with exists status.
        """
        path = params.get('path')
        output_var = params.get('output_var', 'file_exists')
        
        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            path_obj = Path(path)
            exists = path_obj.exists()
            is_file = path_obj.is_file() if exists else False
            is_dir = path_obj.is_dir() if exists else False
            
            result_data = {
                'exists': exists,
                'is_file': is_file,
                'is_dir': is_dir,
                'path': str(path_obj.absolute())
            }
            
            context.set(output_var, exists)
            
            return ActionResult(
                success=True,
                message=f"{'存在' if exists else '不存在'}: {path}",
                data=result_data
            )
        except Exception as e:
            return ActionResult(success=False, message=f"检查失败: {str(e)}")
    
    def get_required_params(self) -> List[str]:
        return ['path']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'output_var': 'file_exists'
        }


class FileDeleteAction(BaseAction):
    """Delete a file."""
    action_type = "file_delete"
    display_name = "删除文件"
    description = "删除指定文件"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file deletion.
        
        Args:
            context: Execution context.
            params: Dict with path, must_exist.
            
        Returns:
            ActionResult indicating success or failure.
        """
        path = params.get('path')
        must_exist = params.get('must_exist', True)
        
        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            path_obj = Path(path)
            
            if not path_obj.exists():
                if must_exist:
                    return ActionResult(success=False, message=f"文件不存在: {path}")
                else:
                    return ActionResult(success=True, message=f"文件不存在，跳过删除: {path}")
            
            if not path_obj.is_file():
                return ActionResult(success=False, message=f"路径不是文件: {path}")
            
            path_obj.unlink()
            
            return ActionResult(
                success=True,
                message=f"文件已删除: {path}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"文件删除失败: {str(e)}")
    
    def get_required_params(self) -> List[str]:
        return ['path']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'must_exist': True
        }


class FileCopyAction(BaseAction):
    """Copy a file to a destination."""
    action_type = "file_copy"
    display_name = "复制文件"
    description = "复制文件到目标位置"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file copy.
        
        Args:
            context: Execution context.
            params: Dict with src, dest, overwrite.
            
        Returns:
            ActionResult indicating success or failure.
        """
        src = params.get('src')
        dest = params.get('dest')
        overwrite = params.get('overwrite', False)
        
        valid, msg = self.validate_type(src, str, 'src')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(dest, str, 'dest')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(overwrite, bool, 'overwrite')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            src_path = Path(src)
            dest_path = Path(dest)
            
            if not src_path.exists():
                return ActionResult(success=False, message=f"源文件不存在: {src}")
            
            if not src_path.is_file():
                return ActionResult(success=False, message=f"源路径不是文件: {src}")
            
            if dest_path.exists() and not overwrite:
                return ActionResult(success=False, message=f"目标文件已存在: {dest}")
            
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest)
            
            return ActionResult(
                success=True,
                message=f"文件已复制: {src} -> {dest}",
                data={'src': str(src_path.absolute()), 'dest': str(dest_path.absolute())}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"文件复制失败: {str(e)}")
    
    def get_required_params(self) -> List[str]:
        return ['src', 'dest']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'overwrite': False
        }


class DirCreateAction(BaseAction):
    """Create a directory."""
    action_type = "dir_create"
    display_name = "创建目录"
    description = "创建目录"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute directory creation.
        
        Args:
            context: Execution context.
            params: Dict with path, parents.
            
        Returns:
            ActionResult indicating success or failure.
        """
        path = params.get('path')
        parents = params.get('parents', True)
        
        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(parents, bool, 'parents')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            path_obj = Path(path)
            
            if path_obj.exists():
                return ActionResult(
                    success=True,
                    message=f"目录已存在: {path}",
                    data={'path': str(path_obj.absolute()), 'created': False}
                )
            
            path_obj.mkdir(parents=parents, exist_ok=True)
            
            return ActionResult(
                success=True,
                message=f"目录已创建: {path}",
                data={'path': str(path_obj.absolute()), 'created': True}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"目录创建失败: {str(e)}")
    
    def get_required_params(self) -> List[str]:
        return ['path']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'parents': True
        }


class FileListAction(BaseAction):
    """List contents of a directory."""
    action_type = "file_list"
    display_name = "列出目录"
    description = "列出目录内容和文件"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute directory listing.
        
        Args:
            context: Execution context.
            params: Dict with path, pattern, output_var.
            
        Returns:
            ActionResult with list of files/directories.
        """
        path = params.get('path')
        pattern = params.get('pattern', '*')
        output_var = params.get('output_var', 'file_list')
        
        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(pattern, str, 'pattern')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            path_obj = Path(path)
            
            if not path_obj.exists():
                return ActionResult(success=False, message=f"目录不存在: {path}")
            
            if not path_obj.is_dir():
                return ActionResult(success=False, message=f"路径不是目录: {path}")
            
            entries = []
            for entry in sorted(path_obj.glob(pattern)):
                entries.append({
                    'name': entry.name,
                    'path': str(entry.absolute()),
                    'is_file': entry.is_file(),
                    'is_dir': entry.is_dir(),
                    'size': entry.stat().st_size if entry.is_file() else None
                })
            
            context.set(output_var, entries)
            
            return ActionResult(
                success=True,
                message=f"列出目录成功: {path} ({len(entries)} 项)",
                data={'entries': entries, 'count': len(entries)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"列出目录失败: {str(e)}")
    
    def get_required_params(self) -> List[str]:
        return ['path']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'pattern': '*',
            'output_var': 'file_list'
        }


class FileMoveAction(BaseAction):
    """Move a file to a destination."""
    action_type = "file_move"
    display_name = "移动文件"
    description = "移动文件到目标位置"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute file move.
        
        Args:
            context: Execution context.
            params: Dict with src, dest, overwrite.
            
        Returns:
            ActionResult indicating success or failure.
        """
        src = params.get('src')
        dest = params.get('dest')
        overwrite = params.get('overwrite', False)
        
        valid, msg = self.validate_type(src, str, 'src')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(dest, str, 'dest')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(overwrite, bool, 'overwrite')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            src_path = Path(src)
            dest_path = Path(dest)
            
            if not src_path.exists():
                return ActionResult(success=False, message=f"源文件不存在: {src}")
            
            if not src_path.is_file():
                return ActionResult(success=False, message=f"源路径不是文件: {src}")
            
            if dest_path.exists() and not overwrite:
                return ActionResult(success=False, message=f"目标文件已存在: {dest}")
            
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(src, dest)
            
            return ActionResult(
                success=True,
                message=f"文件已移动: {src} -> {dest}",
                data={'src': str(src_path.absolute()), 'dest': str(dest_path.absolute())}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"文件移动失败: {str(e)}")
    
    def get_required_params(self) -> List[str]:
        return ['src', 'dest']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'overwrite': False
        }
