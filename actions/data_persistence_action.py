"""Data persistence action module for RabAI AutoClick.

Provides data persistence operations:
- DataSnapshotAction: Create snapshots of data
- DataRestoreAction: Restore data from snapshots
- DataVersioningAction: Version control for data
- DataBackupAction: Backup data to storage
- DataMigrationAction: Migrate data between formats
"""

from typing import Any, Dict, List, Optional
from datetime import datetime
import json
import copy

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataSnapshotAction(BaseAction):
    """Create snapshots of data."""
    action_type = "data_snapshot"
    display_name = "数据快照"
    description = "创建数据快照"
    
    def __init__(self):
        super().__init__()
        self._snapshots: Dict[str, Dict] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")
            
            if operation == "create":
                return self._create_snapshot(params)
            elif operation == "list":
                return self._list_snapshots(params)
            elif operation == "get":
                return self._get_snapshot(params)
            elif operation == "delete":
                return self._delete_snapshot(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _create_snapshot(self, params: Dict[str, Any]) -> ActionResult:
        snapshot_id = params.get("snapshot_id", f"snap_{int(datetime.now().timestamp())}")
        data = params.get("data")
        description = params.get("description", "")
        tags = params.get("tags", [])
        
        if data is None:
            return ActionResult(success=False, message="data is required")
        
        snapshot = {
            "snapshot_id": snapshot_id,
            "data": copy.deepcopy(data),
            "description": description,
            "tags": tags,
            "created_at": datetime.now().isoformat(),
            "size": self._estimate_size(data)
        }
        
        self._snapshots[snapshot_id] = snapshot
        
        return ActionResult(
            success=True,
            message=f"Snapshot created: {snapshot_id}",
            data={
                "snapshot_id": snapshot_id,
                "created_at": snapshot["created_at"],
                "size": snapshot["size"],
                "tags": tags
            }
        )
    
    def _list_snapshots(self, params: Dict[str, Any]) -> ActionResult:
        tag_filter = params.get("tag")
        
        snapshots = list(self._snapshots.values())
        
        if tag_filter:
            snapshots = [s for s in snapshots if tag_filter in s.get("tags", [])]
        
        snapshots.sort(key=lambda x: x["created_at"], reverse=True)
        
        return ActionResult(
            success=True,
            message=f"{len(snapshots)} snapshots found",
            data={
                "snapshots": [{
                    "snapshot_id": s["snapshot_id"],
                    "description": s["description"],
                    "tags": s["tags"],
                    "created_at": s["created_at"],
                    "size": s["size"]
                } for s in snapshots],
                "count": len(snapshots)
            }
        )
    
    def _get_snapshot(self, params: Dict[str, Any]) -> ActionResult:
        snapshot_id = params.get("snapshot_id")
        
        if snapshot_id not in self._snapshots:
            return ActionResult(success=False, message=f"Snapshot {snapshot_id} not found")
        
        snapshot = self._snapshots[snapshot_id]
        
        return ActionResult(
            success=True,
            message=f"Snapshot retrieved: {snapshot_id}",
            data={
                "snapshot_id": snapshot_id,
                "data": snapshot["data"],
                "description": snapshot["description"],
                "tags": snapshot["tags"],
                "created_at": snapshot["created_at"]
            }
        )
    
    def _delete_snapshot(self, params: Dict[str, Any]) -> ActionResult:
        snapshot_id = params.get("snapshot_id")
        
        if snapshot_id in self._snapshots:
            del self._snapshots[snapshot_id]
            return ActionResult(success=True, message=f"Snapshot {snapshot_id} deleted")
        
        return ActionResult(success=False, message=f"Snapshot {snapshot_id} not found")
    
    def _estimate_size(self, data: Any) -> int:
        try:
            return len(json.dumps(data))
        except Exception:
            return 0


class DataRestoreAction(BaseAction):
    """Restore data from snapshots."""
    action_type = "data_restore"
    display_name = "数据恢复"
    description = "从快照恢复数据"
    
    def __init__(self):
        super().__init__()
        self._snapshots: Dict[str, Dict] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            snapshot_id = params.get("snapshot_id")
            target = params.get("target")
            merge_strategy = params.get("merge_strategy", "replace")
            
            if not snapshot_id:
                return ActionResult(success=False, message="snapshot_id is required")
            
            if snapshot_id not in self._snapshots:
                return ActionResult(success=False, message=f"Snapshot {snapshot_id} not found")
            
            snapshot = self._snapshots[snapshot_id]
            data = snapshot["data"]
            
            if merge_strategy == "replace":
                restored_data = copy.deepcopy(data)
            elif merge_strategy == "merge" and target:
                restored_data = self._merge_data(target, data)
            else:
                restored_data = copy.deepcopy(data)
            
            return ActionResult(
                success=True,
                message=f"Data restored from snapshot: {snapshot_id}",
                data={
                    "snapshot_id": snapshot_id,
                    "restored_at": datetime.now().isoformat(),
                    "data": restored_data,
                    "merge_strategy": merge_strategy
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _merge_data(self, target: Dict, source: Dict) -> Dict:
        result = copy.deepcopy(target)
        
        for key, value in source.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_data(result[key], value)
            else:
                result[key] = copy.deepcopy(value)
        
        return result


class DataVersioningAction(BaseAction):
    """Version control for data."""
    action_type = "data_versioning"
    display_name = "数据版本控制"
    description = "数据版本控制"
    
    def __init__(self):
        super().__init__()
        self._versions: Dict[str, List[Dict]] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "commit")
            
            if operation == "commit":
                return self._commit_version(params)
            elif operation == "log":
                return self._get_version_log(params)
            elif operation == "checkout":
                return self._checkout_version(params)
            elif operation == "diff":
                return self._diff_versions(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _commit_version(self, params: Dict[str, Any]) -> ActionResult:
        entity_id = params.get("entity_id")
        data = params.get("data")
        message = params.get("message", "")
        
        if not entity_id or data is None:
            return ActionResult(success=False, message="entity_id and data are required")
        
        if entity_id not in self._versions:
            self._versions[entity_id] = []
        
        version_number = len(self._versions[entity_id]) + 1
        version_id = f"v{version_number}"
        
        version_entry = {
            "version_id": version_id,
            "version_number": version_number,
            "data": copy.deepcopy(data),
            "message": message,
            "created_at": datetime.now().isoformat()
        }
        
        self._versions[entity_id].append(version_entry)
        
        return ActionResult(
            success=True,
            message=f"Version committed: {version_id}",
            data={
                "entity_id": entity_id,
                "version_id": version_id,
                "version_number": version_number,
                "total_versions": len(self._versions[entity_id])
            }
        )
    
    def _get_version_log(self, params: Dict[str, Any]) -> ActionResult:
        entity_id = params.get("entity_id")
        limit = params.get("limit", 10)
        
        if not entity_id:
            return ActionResult(success=False, message="entity_id is required")
        
        if entity_id not in self._versions:
            return ActionResult(success=True, message="No versions found", data={"versions": []})
        
        versions = self._versions[entity_id][-limit:]
        
        return ActionResult(
            success=True,
            message=f"{len(versions)} versions found",
            data={
                "entity_id": entity_id,
                "versions": [{
                    "version_id": v["version_id"],
                    "version_number": v["version_number"],
                    "message": v["message"],
                    "created_at": v["created_at"]
                } for v in versions],
                "total_versions": len(self._versions[entity_id])
            }
        )
    
    def _checkout_version(self, params: Dict[str, Any]) -> ActionResult:
        entity_id = params.get("entity_id")
        version_id = params.get("version_id")
        
        if not entity_id or not version_id:
            return ActionResult(success=False, message="entity_id and version_id are required")
        
        if entity_id not in self._versions:
            return ActionResult(success=False, message=f"Entity {entity_id} not found")
        
        version_entry = None
        for v in self._versions[entity_id]:
            if v["version_id"] == version_id:
                version_entry = v
                break
        
        if not version_entry:
            return ActionResult(success=False, message=f"Version {version_id} not found")
        
        return ActionResult(
            success=True,
            message=f"Checked out version: {version_id}",
            data={
                "entity_id": entity_id,
                "version_id": version_id,
                "data": version_entry["data"],
                "created_at": version_entry["created_at"]
            }
        )
    
    def _diff_versions(self, params: Dict[str, Any]) -> ActionResult:
        entity_id = params.get("entity_id")
        version_id_1 = params.get("version_id_1")
        version_id_2 = params.get("version_id_2")
        
        if not entity_id or not version_id_1 or not version_id_2:
            return ActionResult(success=False, message="entity_id, version_id_1, and version_id_2 are required")
        
        if entity_id not in self._versions:
            return ActionResult(success=False, message=f"Entity {entity_id} not found")
        
        v1 = self._find_version(self._versions[entity_id], version_id_1)
        v2 = self._find_version(self._versions[entity_id], version_id_2)
        
        if not v1 or not v2:
            return ActionResult(success=False, message="Version not found")
        
        diff = self._compute_diff(v1["data"], v2["data"])
        
        return ActionResult(
            success=True,
            message="Diff computed",
            data={
                "version_1": version_id_1,
                "version_2": version_id_2,
                "diff": diff
            }
        )
    
    def _find_version(self, versions: List[Dict], version_id: str) -> Optional[Dict]:
        for v in versions:
            if v["version_id"] == version_id:
                return v
        return None
    
    def _compute_diff(self, data1: Any, data2: Any) -> Dict:
        differences = []
        
        if type(data1) != type(data2):
            differences.append({
                "type": "type_change",
                "before": str(type(data1).__name__),
                "after": str(type(data2).__name__)
            })
        elif isinstance(data1, dict):
            all_keys = set(data1.keys()) | set(data2.keys())
            for key in all_keys:
                if key not in data1:
                    differences.append({"type": "added", "key": key, "value": data2[key]})
                elif key not in data2:
                    differences.append({"type": "removed", "key": key, "value": data1[key]})
                elif data1[key] != data2[key]:
                    differences.append({"type": "changed", "key": key, "before": data1[key], "after": data2[key]})
        elif isinstance(data1, list):
            if len(data1) != len(data2):
                differences.append({
                    "type": "length_change",
                    "before_length": len(data1),
                    "after_length": len(data2)
                })
        else:
            if data1 != data2:
                differences.append({"type": "changed", "before": data1, "after": data2})
        
        return {"differences": differences, "change_count": len(differences)}


class DataBackupAction(BaseAction):
    """Backup data to storage."""
    action_type = "data_backup"
    display_name = "数据备份"
    description = "将数据备份到存储"
    
    def __init__(self):
        super().__init__()
        self._backups: Dict[str, Dict] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")
            
            if operation == "create":
                return self._create_backup(params)
            elif operation == "list":
                return self._list_backups(params)
            elif operation == "restore":
                return self._restore_backup(params)
            elif operation == "delete":
                return self._delete_backup(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _create_backup(self, params: Dict[str, Any]) -> ActionResult:
        backup_id = params.get("backup_id", f"backup_{int(datetime.now().timestamp())}")
        data = params.get("data")
        storage_type = params.get("storage_type", "local")
        compression = params.get("compression", False)
        
        if data is None:
            return ActionResult(success=False, message="data is required")
        
        backup = {
            "backup_id": backup_id,
            "data": copy.deepcopy(data),
            "storage_type": storage_type,
            "compression": compression,
            "created_at": datetime.now().isoformat(),
            "size": self._estimate_size(data)
        }
        
        self._backups[backup_id] = backup
        
        return ActionResult(
            success=True,
            message=f"Backup created: {backup_id}",
            data={
                "backup_id": backup_id,
                "storage_type": storage_type,
                "compression": compression,
                "size": backup["size"],
                "created_at": backup["created_at"]
            }
        )
    
    def _list_backups(self, params: Dict[str, Any]) -> ActionResult:
        storage_type = params.get("storage_type")
        
        backups = list(self._backups.values())
        
        if storage_type:
            backups = [b for b in backups if b["storage_type"] == storage_type]
        
        backups.sort(key=lambda x: x["created_at"], reverse=True)
        
        return ActionResult(
            success=True,
            message=f"{len(backups)} backups found",
            data={
                "backups": [{
                    "backup_id": b["backup_id"],
                    "storage_type": b["storage_type"],
                    "compression": b["compression"],
                    "size": b["size"],
                    "created_at": b["created_at"]
                } for b in backups],
                "count": len(backups)
            }
        )
    
    def _restore_backup(self, params: Dict[str, Any]) -> ActionResult:
        backup_id = params.get("backup_id")
        
        if backup_id not in self._backups:
            return ActionResult(success=False, message=f"Backup {backup_id} not found")
        
        backup = self._backups[backup_id]
        
        return ActionResult(
            success=True,
            message=f"Backup restored: {backup_id}",
            data={
                "backup_id": backup_id,
                "data": backup["data"],
                "restored_at": datetime.now().isoformat()
            }
        )
    
    def _delete_backup(self, params: Dict[str, Any]) -> ActionResult:
        backup_id = params.get("backup_id")
        
        if backup_id in self._backups:
            del self._backups[backup_id]
            return ActionResult(success=True, message=f"Backup {backup_id} deleted")
        
        return ActionResult(success=False, message=f"Backup {backup_id} not found")
    
    def _estimate_size(self, data: Any) -> int:
        try:
            return len(json.dumps(data))
        except Exception:
            return 0


class DataMigrationAction(BaseAction):
    """Migrate data between formats."""
    action_type = "data_migration"
    display_name = "数据迁移"
    description = "在不同格式之间迁移数据"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data")
            source_format = params.get("source_format")
            target_format = params.get("target_format")
            
            if data is None or not target_format:
                return ActionResult(success=False, message="data and target_format are required")
            
            migrated = self._migrate_data(data, source_format, target_format)
            
            return ActionResult(
                success=True,
                message=f"Migration complete: {source_format or 'auto'} -> {target_format}",
                data={
                    "source_format": source_format,
                    "target_format": target_format,
                    "data": migrated
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _migrate_data(self, data: Any, source_format: Optional[str], target_format: str) -> Any:
        if target_format == "json":
            return self._to_json(data)
        elif target_format == "dict":
            return self._to_dict(data)
        elif target_format == "list":
            return self._to_list(data)
        elif target_format == "flat":
            return self._flatten(data)
        else:
            return data
    
    def _to_json(self, data: Any) -> str:
        return json.dumps(data, indent=2)
    
    def _to_dict(self, data: Any) -> Dict:
        if isinstance(data, dict):
            return data
        elif isinstance(data, str):
            try:
                return json.loads(data)
            except Exception:
                return {"value": data}
        else:
            return {"value": data}
    
    def _to_list(self, data: Any) -> List:
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return list(data.values())
        else:
            return [data]
    
    def _flatten(self, data: Any, prefix: str = "") -> Dict:
        result = {}
        
        if isinstance(data, dict):
            for key, value in data.items():
                new_key = f"{prefix}.{key}" if prefix else key
                if isinstance(value, (dict, list)):
                    result.update(self._flatten(value, new_key))
                else:
                    result[new_key] = value
        elif isinstance(data, list):
            for i, item in enumerate(data):
                new_key = f"{prefix}[{i}]"
                if isinstance(item, (dict, list)):
                    result.update(self._flatten(item, new_key))
                else:
                    result[new_key] = item
        else:
            result[prefix or "value"] = data
        
        return result
