"""Data snapshot action module for RabAI AutoClick.

Provides data snapshot creation, versioning, rollback,
and point-in-time recovery capabilities.
"""

import time
import sys
import os
import json
import hashlib
from typing import Any, Dict, List, Optional, Union, Callable
from datetime import datetime
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataSnapshotAction(BaseAction):
    """Create and manage data snapshots for versioning and recovery.
    
    Supports snapshot creation, listing, comparison, rollback,
    and automatic snapshot policies.
    """
    action_type = "data_snapshot"
    display_name = "数据快照"
    description = "数据快照管理，支持版本化和回滚"
    
    def __init__(self):
        super().__init__()
        self._snapshots: Dict[str, Dict[str, Any]] = {}
        self._current_data: Dict[str, Any] = {}
        self._lock = threading.RLock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute snapshot operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: action (create, list, restore,
                   compare, delete, purge), config.
        
        Returns:
            ActionResult with operation result.
        """
        action = params.get('action', 'create')
        
        if action == 'create':
            return self._create_snapshot(params)
        elif action == 'list':
            return self._list_snapshots(params)
        elif action == 'restore':
            return self._restore_snapshot(params)
        elif action == 'compare':
            return self._compare_snapshots(params)
        elif action == 'delete':
            return self._delete_snapshot(params)
        elif action == 'purge':
            return self._purge_snapshots(params)
        elif action == 'set_data':
            return self._set_data(params)
        elif action == 'get_data':
            return self._get_data(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )
    
    def _create_snapshot(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Create a new snapshot."""
        name = params.get('name')
        if not name:
            return ActionResult(success=False, message="name is required")
        
        description = params.get('description', '')
        tags = params.get('tags', {})
        auto = params.get('auto', False)
        
        with self._lock:
            snapshot_id = f"snap_{name}_{int(time.time() * 1000)}"
            
            snapshot_data = self._deep_copy(self._current_data)
            
            snapshot = {
                'snapshot_id': snapshot_id,
                'name': name,
                'description': description,
                'tags': tags,
                'auto': auto,
                'created_at': time.time(),
                'data': snapshot_data,
                'size': len(json.dumps(snapshot_data, default=str))
            }
            
            self._snapshots[snapshot_id] = snapshot
        
        return ActionResult(
            success=True,
            message=f"Created snapshot '{name}' with ID {snapshot_id}",
            data={
                'snapshot_id': snapshot_id,
                'name': name,
                'created_at': snapshot['created_at']
            }
        )
    
    def _list_snapshots(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """List all snapshots."""
        name_filter = params.get('name')
        tag_filter = params.get('tag')
        auto_only = params.get('auto_only', False)
        limit = params.get('limit', 100)
        
        with self._lock:
            results = list(self._snapshots.values())
        
        if name_filter:
            results = [s for s in results if s['name'] == name_filter]
        if tag_filter:
            results = [s for s in results if tag_filter in s.get('tags', {})]
        if auto_only:
            results = [s for s in results if s.get('auto', False)]
        
        results.sort(key=lambda s: s['created_at'], reverse=True)
        results = results[:limit]
        
        snapshots_summary = [{
            'snapshot_id': s['snapshot_id'],
            'name': s['name'],
            'description': s['description'],
            'created_at': s['created_at'],
            'auto': s.get('auto', False),
            'size': s.get('size', 0)
        } for s in results]
        
        return ActionResult(
            success=True,
            message=f"Found {len(snapshots_summary)} snapshots",
            data={
                'snapshots': snapshots_summary,
                'count': len(snapshots_summary)
            }
        )
    
    def _restore_snapshot(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Restore data from a snapshot."""
        snapshot_id = params.get('snapshot_id')
        name = params.get('name')
        
        if not snapshot_id and not name:
            return ActionResult(
                success=False,
                message="snapshot_id or name is required"
            )
        
        with self._lock:
            if snapshot_id:
                if snapshot_id not in self._snapshots:
                    return ActionResult(
                        success=False,
                        message=f"Snapshot {snapshot_id} not found"
                    )
                snapshot = self._snapshots[snapshot_id]
            else:
                matching = [s for s in self._snapshots.values() if s['name'] == name]
                if not matching:
                    return ActionResult(
                        success=False,
                        message=f"No snapshot with name '{name}' found"
                    )
                snapshot = max(matching, key=lambda s: s['created_at'])
            
            self._current_data = self._deep_copy(snapshot['data'])
        
        return ActionResult(
            success=True,
            message=f"Restored from snapshot '{snapshot['name']}'",
            data={
                'snapshot_id': snapshot['snapshot_id'],
                'name': snapshot['name'],
                'restored_at': time.time()
            }
        )
    
    def _compare_snapshots(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compare two snapshots."""
        snapshot1_id = params.get('snapshot1_id')
        snapshot2_id = params.get('snapshot2_id')
        
        if not snapshot1_id or not snapshot2_id:
            return ActionResult(
                success=False,
                message="snapshot1_id and snapshot2_id are required"
            )
        
        with self._lock:
            if snapshot1_id not in self._snapshots:
                return ActionResult(
                    success=False,
                    message=f"Snapshot {snapshot1_id} not found"
                )
            if snapshot2_id not in self._snapshots:
                return ActionResult(
                    success=False,
                    message=f"Snapshot {snapshot2_id} not found"
                )
            
            snapshot1 = self._snapshots[snapshot1_id]
            snapshot2 = self._snapshots[snapshot2_id]
        
        data1 = snapshot1['data']
        data2 = snapshot2['data']
        
        differences = self._find_differences(data1, data2)
        
        return ActionResult(
            success=True,
            message=f"Found {len(differences)} differences",
            data={
                'snapshot1': {
                    'id': snapshot1_id,
                    'name': snapshot1['name'],
                    'created_at': snapshot1['created_at']
                },
                'snapshot2': {
                    'id': snapshot2_id,
                    'name': snapshot2['name'],
                    'created_at': snapshot2['created_at']
                },
                'differences': differences,
                'difference_count': len(differences)
            }
        )
    
    def _find_differences(
        self,
        data1: Any,
        data2: Any,
        path: str = "root"
    ) -> List[Dict[str, Any]]:
        """Find differences between two data structures."""
        differences = []
        
        if type(data1) != type(data2):
            differences.append({
                'path': path,
                'type': 'type_mismatch',
                'old_type': type(data1).__name__,
                'new_type': type(data2).__name__
            })
            return differences
        
        if isinstance(data1, dict):
            all_keys = set(data1.keys()) | set(data2.keys())
            for key in all_keys:
                new_path = f"{path}.{key}"
                if key not in data1:
                    differences.append({
                        'path': new_path,
                        'type': 'added',
                        'value': data2[key]
                    })
                elif key not in data2:
                    differences.append({
                        'path': new_path,
                        'type': 'removed',
                        'value': data1[key]
                    })
                else:
                    differences.extend(self._find_differences(data1[key], data2[key], new_path))
        
        elif isinstance(data1, list):
            if len(data1) != len(data2):
                differences.append({
                    'path': path,
                    'type': 'length_changed',
                    'old_length': len(data1),
                    'new_length': len(data2)
                })
            else:
                for idx, (item1, item2) in enumerate(zip(data1, data2)):
                    differences.extend(self._find_differences(item1, item2, f"{path}[{idx}]"))
        
        else:
            if data1 != data2:
                differences.append({
                    'path': path,
                    'type': 'changed',
                    'old_value': data1,
                    'new_value': data2
                })
        
        return differences
    
    def _delete_snapshot(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Delete a snapshot."""
        snapshot_id = params.get('snapshot_id')
        
        if not snapshot_id:
            return ActionResult(success=False, message="snapshot_id is required")
        
        with self._lock:
            if snapshot_id not in self._snapshots:
                return ActionResult(
                    success=False,
                    message=f"Snapshot {snapshot_id} not found"
                )
            
            deleted_snapshot = self._snapshots.pop(snapshot_id)
        
        return ActionResult(
            success=True,
            message=f"Deleted snapshot '{deleted_snapshot['name']}'",
            data={'deleted_snapshot_id': snapshot_id}
        )
    
    def _purge_snapshots(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Purge old snapshots based on policy."""
        keep_count = params.get('keep_count', 10)
        older_than = params.get('older_than')
        name = params.get('name')
        
        with self._lock:
            snapshots = list(self._snapshots.values())
            
            if name:
                snapshots = [s for s in snapshots if s['name'] == name]
            
            if older_than:
                snapshots = [s for s in snapshots if s['created_at'] >= older_than]
            
            snapshots.sort(key=lambda s: s['created_at'], reverse=True)
            
            to_keep = snapshots[:keep_count]
            to_delete = snapshots[keep_count:]
            
            deleted_ids = []
            for snapshot in to_delete:
                del self._snapshots[snapshot['snapshot_id']]
                deleted_ids.append(snapshot['snapshot_id'])
        
        return ActionResult(
            success=True,
            message=f"Purge complete: kept {len(to_keep)}, deleted {len(to_delete)}",
            data={
                'kept_count': len(to_keep),
                'deleted_count': len(to_delete),
                'deleted_ids': deleted_ids
            }
        )
    
    def _set_data(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Set current data for snapshotting."""
        data = params.get('data')
        
        if data is None:
            return ActionResult(success=False, message="data is required")
        
        with self._lock:
            self._current_data = self._deep_copy(data)
        
        return ActionResult(
            success=True,
            message="Data set for snapshots",
            data={'size': len(json.dumps(data, default=str))}
        )
    
    def _get_data(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get current data."""
        with self._lock:
            return ActionResult(
                success=True,
                message="Retrieved current data",
                data={'data': self._current_data}
            )
    
    def _deep_copy(self, obj: Any) -> Any:
        """Create a deep copy of an object."""
        if isinstance(obj, dict):
            return {k: self._deep_copy(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._deep_copy(item) for item in obj]
        else:
            return obj
