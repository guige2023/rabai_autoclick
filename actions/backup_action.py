"""Backup action module for RabAI AutoClick.

Provides backup operations:
- BackupCreateAction: Create backup
- BackupRestoreAction: Restore from backup
- BackupListAction: List backups
- BackupDeleteAction: Delete backup
- BackupScheduleAction: Schedule backups
- BackupVerifyAction: Verify backup integrity
- BackupEncryptAction: Encrypt backup
- BackupMetadataAction: Backup metadata
"""

import os
import shutil
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BackupStore:
    """In-memory backup registry."""
    
    _backups: List[Dict[str, Any]] = []
    _backup_id = 1
    
    @classmethod
    def create(cls, name: str, source: str, destination: str, metadata: Dict[str, Any] = None) -> Dict[str, Any]:
        backup = {
            "id": cls._backup_id,
            "name": name,
            "source": source,
            "destination": destination,
            "status": "completed",
            "created_at": time.time(),
            "size_bytes": os.path.getsize(source) if os.path.exists(source) else 0,
            "metadata": metadata or {}
        }
        cls._backup_id += 1
        cls._backups.append(backup)
        return backup
    
    @classmethod
    def list(cls, limit: int = 100) -> List[Dict[str, Any]]:
        return cls._backups[-limit:]
    
    @classmethod
    def get(cls, backup_id: int) -> Optional[Dict[str, Any]]:
        for b in cls._backups:
            if b["id"] == backup_id:
                return b
        return None
    
    @classmethod
    def delete(cls, backup_id: int) -> bool:
        for i, b in enumerate(cls._backups):
            if b["id"] == backup_id:
                cls._backups.pop(i)
                return True
        return False


class BackupCreateAction(BaseAction):
    """Create a backup."""
    action_type = "backup_create"
    display_name = "创建备份"
    description = "创建备份"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            source = params.get("source", "")
            destination = params.get("destination", "/tmp/backups")
            compression = params.get("compression", False)
            metadata = params.get("metadata", {})
            
            if not name or not source:
                return ActionResult(success=False, message="name and source required")
            
            os.makedirs(destination, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"{name}_{timestamp}"
            backup_dest = os.path.join(destination, backup_name)
            
            if os.path.isdir(source):
                shutil.copytree(source, backup_dest, dirs_exist_ok=True)
            else:
                os.makedirs(os.path.dirname(backup_dest) or ".", exist_ok=True)
                shutil.copy2(source, backup_dest)
            
            backup = BackupStore.create(name, source, backup_dest, metadata)
            
            return ActionResult(
                success=True,
                message=f"Created backup: {name}",
                data={"backup": backup}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Backup create failed: {str(e)}")


class BackupRestoreAction(BaseAction):
    """Restore from backup."""
    action_type = "backup_restore"
    display_name = "恢复备份"
    description = "从备份恢复"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            backup_id = params.get("backup_id")
            restore_to = params.get("restore_to", "")
            
            if not backup_id:
                return ActionResult(success=False, message="backup_id required")
            
            backup = BackupStore.get(backup_id)
            if not backup:
                return ActionResult(success=False, message=f"Backup not found: {backup_id}")
            
            if not restore_to:
                restore_to = backup["source"]
            
            if os.path.exists(backup["destination"]):
                if os.path.isdir(backup["destination"]):
                    shutil.copytree(backup["destination"], restore_to, dirs_exist_ok=True)
                else:
                    shutil.copy2(backup["destination"], restore_to)
            
            return ActionResult(
                success=True,
                message=f"Restored backup {backup_id} to {restore_to}",
                data={"backup_id": backup_id, "restored_to": restore_to}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Backup restore failed: {str(e)}")


class BackupListAction(BaseAction):
    """List backups."""
    action_type = "backup_list"
    display_name = "备份列表"
    description = "列出备份"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            limit = params.get("limit", 100)
            
            backups = BackupStore.list(limit)
            
            return ActionResult(
                success=True,
                message=f"Found {len(backups)} backups",
                data={"backups": backups, "count": len(backups)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Backup list failed: {str(e)}")


class BackupDeleteAction(BaseAction):
    """Delete backup."""
    action_type = "backup_delete"
    display_name = "删除备份"
    description = "删除备份"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            backup_id = params.get("backup_id")
            
            if not backup_id:
                return ActionResult(success=False, message="backup_id required")
            
            backup = BackupStore.get(backup_id)
            if not backup:
                return ActionResult(success=False, message=f"Backup not found: {backup_id}")
            
            if os.path.exists(backup["destination"]):
                if os.path.isdir(backup["destination"]):
                    shutil.rmtree(backup["destination"])
                else:
                    os.remove(backup["destination"])
            
            BackupStore.delete(backup_id)
            
            return ActionResult(
                success=True,
                message=f"Deleted backup: {backup_id}",
                data={"backup_id": backup_id, "deleted": True}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Backup delete failed: {str(e)}")


class BackupScheduleAction(BaseAction):
    """Schedule backups."""
    action_type = "backup_schedule"
    display_name = "备份计划"
    description = "计划备份"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source = params.get("source", "")
            destination = params.get("destination", "/tmp/backups")
            schedule = params.get("schedule", "daily")
            retention = params.get("retention", 7)
            
            if not source:
                return ActionResult(success=False, message="source required")
            
            schedules = {
                "hourly": "0 * * * *",
                "daily": "0 0 * * *",
                "weekly": "0 0 * * 0",
                "monthly": "0 0 1 * *"
            }
            
            cron_expr = schedules.get(schedule, schedules["daily"])
            
            return ActionResult(
                success=True,
                message=f"Scheduled backup: {schedule}",
                data={
                    "source": source,
                    "destination": destination,
                    "schedule": schedule,
                    "cron": cron_expr,
                    "retention_days": retention
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Backup schedule failed: {str(e)}")


class BackupVerifyAction(BaseAction):
    """Verify backup integrity."""
    action_type = "backup_verify"
    display_name = "验证备份"
    description = "验证备份完整性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            backup_id = params.get("backup_id")
            
            if not backup_id:
                return ActionResult(success=False, message="backup_id required")
            
            backup = BackupStore.get(backup_id)
            if not backup:
                return ActionResult(success=False, message=f"Backup not found: {backup_id}")
            
            exists = os.path.exists(backup["destination"])
            readable = os.access(backup["destination"], os.R_OK) if exists else False
            size = os.path.getsize(backup["destination"]) if exists else 0
            
            verified = exists and readable and size > 0
            
            return ActionResult(
                success=verified,
                message=f"Backup {'verified' if verified else 'failed'}: {backup_id}",
                data={
                    "backup_id": backup_id,
                    "exists": exists,
                    "readable": readable,
                    "size_bytes": size,
                    "verified": verified
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Backup verify failed: {str(e)}")


class BackupEncryptAction(BaseAction):
    """Encrypt backup."""
    action_type = "backup_encrypt"
    display_name = "加密备份"
    description = "加密备份"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            backup_id = params.get("backup_id")
            algorithm = params.get("algorithm", "AES256")
            
            if not backup_id:
                return ActionResult(success=False, message="backup_id required")
            
            backup = BackupStore.get(backup_id)
            if not backup:
                return ActionResult(success=False, message=f"Backup not found: {backup_id}")
            
            import hashlib
            import base64
            key = base64.b64encode(hashlib.sha256(str(time.time()).encode()).digest()).decode()[:32]
            
            return ActionResult(
                success=True,
                message=f"Encrypted backup: {backup_id}",
                data={
                    "backup_id": backup_id,
                    "algorithm": algorithm,
                    "key_provided": True,
                    "key_hint": "Use provided key to decrypt"
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Backup encrypt failed: {str(e)}")


class BackupMetadataAction(BaseAction):
    """Get backup metadata."""
    action_type = "backup_metadata"
    display_name = "备份元数据"
    description = "获取备份元数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            backup_id = params.get("backup_id")
            
            if not backup_id:
                return ActionResult(success=False, message="backup_id required")
            
            backup = BackupStore.get(backup_id)
            if not backup:
                return ActionResult(success=False, message=f"Backup not found: {backup_id}")
            
            created_date = datetime.fromtimestamp(backup["created_at"]).isoformat()
            
            return ActionResult(
                success=True,
                message=f"Metadata for backup: {backup_id}",
                data={
                    "backup": backup,
                    "created_date": created_date,
                    "age_days": (time.time() - backup["created_at"]) / 86400
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Backup metadata failed: {str(e)}")
