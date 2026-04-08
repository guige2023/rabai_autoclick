"""File watcher action module for RabAI AutoClick.

Provides file monitoring operations:
- FileMonitorAction: Monitor file changes
- FileCreateAction: Create new file
- FileDeleteAction: Delete file
- FileMoveAction: Move/rename file
- FileCopyAction: Copy file
- FileModifyAction: Modify file content
- FileBackupAction: Backup file
- FileRestoreAction: Restore from backup
"""

import hashlib
import json
import os
import shutil
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FileMonitorAction(BaseAction):
    """Monitor file for changes."""
    action_type = "file_monitor"
    display_name = "文件监控"
    description = "监控文件变化"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            file_path = params.get("file_path", "")
            watch_events = params.get("events", ["modify", "delete", "create"])
            timeout = params.get("timeout", 60)
            
            if not file_path:
                return ActionResult(success=False, message="file_path is required")
            
            if not os.path.exists(file_path):
                return ActionResult(success=False, message=f"File not found: {file_path}")
            
            mtime_before = os.path.getmtime(file_path)
            size_before = os.path.getsize(file_path)
            
            start_time = time.time()
            triggered = None
            
            while time.time() - start_time < timeout:
                if os.path.exists(file_path):
                    mtime_now = os.path.getmtime(file_path)
                    size_now = os.path.getsize(file_path)
                    
                    if mtime_now != mtime_before:
                        triggered = "modify"
                        break
                    if size_now != size_before:
                        triggered = "modify"
                        break
                else:
                    triggered = "delete"
                    break
                
                time.sleep(0.5)
            
            if triggered is None:
                return ActionResult(success=True, message="No changes detected within timeout", data={"timeout": timeout})
            
            return ActionResult(
                success=True,
                message=f"File event: {triggered}",
                data={"event": triggered, "file_path": file_path}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"File monitor failed: {str(e)}")


class FileCreateAction(BaseAction):
    """Create a new file."""
    action_type = "file_create"
    display_name = "创建文件"
    description = "创建新文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            file_path = params.get("file_path", "")
            content = params.get("content", "")
            encoding = params.get("encoding", "utf-8")
            
            if not file_path:
                return ActionResult(success=False, message="file_path is required")
            
            os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)
            
            with open(file_path, "w", encoding=encoding) as f:
                f.write(content)
            
            return ActionResult(
                success=True,
                message=f"Created file: {file_path}",
                data={"path": file_path, "size": len(content)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"File create failed: {str(e)}")


class FileDeleteAction(BaseAction):
    """Delete a file or directory."""
    action_type = "file_delete"
    display_name = "删除文件"
    description = "删除文件或目录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            file_path = params.get("file_path", "")
            recursive = params.get("recursive", False)
            
            if not file_path:
                return ActionResult(success=False, message="file_path is required")
            
            if not os.path.exists(file_path):
                return ActionResult(success=False, message=f"File not found: {file_path}")
            
            if os.path.isdir(file_path):
                if recursive:
                    shutil.rmtree(file_path)
                else:
                    os.rmdir(file_path)
            else:
                os.remove(file_path)
            
            return ActionResult(
                success=True,
                message=f"Deleted: {file_path}",
                data={"path": file_path, "type": "directory" if os.path.isdir(file_path) else "file"}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"File delete failed: {str(e)}")


class FileMoveAction(BaseAction):
    """Move or rename a file."""
    action_type = "file_move"
    display_name = "移动文件"
    description = "移动或重命名文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source = params.get("source", "")
            destination = params.get("destination", "")
            
            if not source or not destination:
                return ActionResult(success=False, message="source and destination required")
            
            if not os.path.exists(source):
                return ActionResult(success=False, message=f"Source not found: {source}")
            
            os.makedirs(os.path.dirname(destination) or ".", exist_ok=True)
            shutil.move(source, destination)
            
            return ActionResult(
                success=True,
                message=f"Moved {source} -> {destination}",
                data={"source": source, "destination": destination}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"File move failed: {str(e)}")


class FileCopyAction(BaseAction):
    """Copy a file or directory."""
    action_type = "file_copy"
    display_name = "复制文件"
    description = "复制文件或目录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source = params.get("source", "")
            destination = params.get("destination", "")
            overwrite = params.get("overwrite", True)
            
            if not source or not destination:
                return ActionResult(success=False, message="source and destination required")
            
            if not os.path.exists(source):
                return ActionResult(success=False, message=f"Source not found: {source}")
            
            if os.path.exists(destination) and not overwrite:
                return ActionResult(success=False, message="Destination exists, overwrite=False")
            
            os.makedirs(os.path.dirname(destination) or ".", exist_ok=True)
            
            if os.path.isdir(source):
                shutil.copytree(source, destination, dirs_exist_ok=overwrite)
            else:
                shutil.copy2(source, destination)
            
            return ActionResult(
                success=True,
                message=f"Copied {source} -> {destination}",
                data={"source": source, "destination": destination}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"File copy failed: {str(e)}")


class FileModifyAction(BaseAction):
    """Modify file content."""
    action_type = "file_modify"
    display_name = "修改文件"
    description = "修改文件内容"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            file_path = params.get("file_path", "")
            content = params.get("content", "")
            mode = params.get("mode", "replace")
            append = params.get("append", False)
            
            if not file_path:
                return ActionResult(success=False, message="file_path is required")
            
            os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)
            
            if append:
                with open(file_path, "a", encoding="utf-8") as f:
                    f.write(content)
                message = f"Appended to {file_path}"
            else:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(content)
                message = f"Replaced content in {file_path}"
            
            return ActionResult(
                success=True,
                message=message,
                data={"path": file_path, "size": len(content)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"File modify failed: {str(e)}")


class FileBackupAction(BaseAction):
    """Backup a file."""
    action_type = "file_backup"
    display_name = "文件备份"
    description = "备份文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            file_path = params.get("file_path", "")
            backup_dir = params.get("backup_dir", "/tmp/backups")
            suffix = params.get("suffix", ".bak")
            
            if not file_path:
                return ActionResult(success=False, message="file_path is required")
            
            if not os.path.exists(file_path):
                return ActionResult(success=False, message=f"File not found: {file_path}")
            
            os.makedirs(backup_dir, exist_ok=True)
            
            filename = os.path.basename(file_path)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"{filename}_{timestamp}{suffix}"
            backup_path = os.path.join(backup_dir, backup_name)
            
            shutil.copy2(file_path, backup_path)
            
            return ActionResult(
                success=True,
                message=f"Backed up to {backup_path}",
                data={"original": file_path, "backup": backup_path}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Backup failed: {str(e)}")


class FileRestoreAction(BaseAction):
    """Restore file from backup."""
    action_type = "file_restore"
    display_name = "文件恢复"
    description = "从备份恢复文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            backup_path = params.get("backup_path", "")
            restore_path = params.get("restore_path", "")
            
            if not backup_path:
                return ActionResult(success=False, message="backup_path is required")
            
            if not os.path.exists(backup_path):
                return ActionResult(success=False, message=f"Backup not found: {backup_path}")
            
            if not restore_path:
                restore_path = backup_path.rsplit("_", 2)[0]
            
            shutil.copy2(backup_path, restore_path)
            
            return ActionResult(
                success=True,
                message=f"Restored {backup_path} -> {restore_path}",
                data={"backup": backup_path, "restored": restore_path}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Restore failed: {str(e)}")
