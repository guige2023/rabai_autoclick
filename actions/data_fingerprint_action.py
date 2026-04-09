"""Data Fingerprint Action Module.

Provides data fingerprinting and deduplication capabilities.
"""

import hashlib
import json
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Set, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataFingerprintAction(BaseAction):
    """Generate fingerprints for data records.
    
    Creates unique identifiers for data to enable deduplication and tracking.
    """
    action_type = "data_fingerprint"
    display_name = "数据指纹"
    description = "为数据记录生成唯一指纹"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute fingerprinting.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, algorithm, fields.
        
        Returns:
            ActionResult with fingerprint data.
        """
        data = params.get('data', [])
        algorithm = params.get('algorithm', 'sha256')
        fields = params.get('fields', [])
        
        if not data:
            return ActionResult(
                success=False,
                data=None,
                error="No data to fingerprint"
            )
        
        try:
            fingerprints = []
            seen: Set[str] = set()
            duplicates = 0
            
            for i, record in enumerate(data):
                fp = self._compute_fingerprint(record, algorithm, fields)
                is_duplicate = fp in seen
                if is_duplicate:
                    duplicates += 1
                else:
                    seen.add(fp)
                fingerprints.append({
                    'index': i,
                    'fingerprint': fp,
                    'is_duplicate': is_duplicate
                })
            
            return ActionResult(
                success=True,
                data={
                    'fingerprints': fingerprints,
                    'unique_count': len(seen),
                    'duplicate_count': duplicates,
                    'total_count': len(data)
                },
                error=None
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                data=None,
                error=f"Fingerprinting failed: {str(e)}"
            )
    
    def _compute_fingerprint(
        self,
        record: Any,
        algorithm: str,
        fields: List[str]
    ) -> str:
        """Compute fingerprint for a record."""
        if isinstance(record, dict):
            if fields:
                values = [str(record.get(f, '')) for f in fields]
            else:
                values = [str(v) for v in record.values()]
            content = '|'.join(values)
        else:
            content = str(record)
        
        if algorithm == 'md5':
            return hashlib.md5(content.encode()).hexdigest()
        elif algorithm == 'sha1':
            return hashlib.sha1(content.encode()).hexdigest()
        elif algorithm == 'sha256':
            return hashlib.sha256(content.encode()).hexdigest()
        elif algorithm == 'sha512':
            return hashlib.sha512(content.encode()).hexdigest()
        else:
            return hashlib.sha256(content.encode()).hexdigest()


class DataDeduplicatorAction(BaseAction):
    """Deduplicate data records.
    
    Removes duplicate records based on fingerprint or key fields.
    """
    action_type = "data_deduplicator"
    display_name = "数据去重"
    description = "基于指纹或键字段删除重复记录"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute deduplication.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, key_fields, keep_strategy.
        
        Returns:
            ActionResult with deduplication results.
        """
        data = params.get('data', [])
        key_fields = params.get('key_fields', [])
        keep_strategy = params.get('keep_strategy', 'first')
        
        if not data:
            return ActionResult(
                success=False,
                data=None,
                error="No data to deduplicate"
            )
        
        seen: Set[str] = set()
        unique_records = []
        duplicate_count = 0
        
        for record in data:
            if key_fields:
                key = self._compute_key(record, key_fields)
            else:
                key = self._compute_fingerprint(record)
            
            if key not in seen:
                seen.add(key)
                unique_records.append(record)
            else:
                duplicate_count += 1
        
        return ActionResult(
            success=True,
            data={
                'unique_records': unique_records,
                'unique_count': len(unique_records),
                'duplicate_count': duplicate_count,
                'original_count': len(data)
            },
            error=None
        )
    
    def _compute_key(self, record: Dict, fields: List[str]) -> str:
        """Compute key from fields."""
        values = [str(record.get(f, '')) for f in fields]
        return '|'.join(values)
    
    def _compute_fingerprint(self, record: Any) -> str:
        """Compute fingerprint for record."""
        content = json.dumps(record, sort_keys=True, default=str)
        return hashlib.sha256(content.encode()).hexdigest()


class DataVersionTrackerAction(BaseAction):
    """Track data versions and changes.
    
    Maintains version history for data records.
    """
    action_type = "data_version_tracker"
    display_name = "数据版本追踪"
    description = "追踪数据记录的版本变更"
    
    def __init__(self):
        super().__init__()
        self._versions: Dict[str, List[Dict]] = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute version tracking.
        
        Args:
            context: Execution context.
            params: Dict with keys: record_id, action, data.
        
        Returns:
            ActionResult with version tracking result.
        """
        record_id = params.get('record_id', '')
        action = params.get('action', 'track')
        data = params.get('data', {})
        
        if not record_id:
            return ActionResult(
                success=False,
                data=None,
                error="Record ID required"
            )
        
        if action == 'track':
            return self._track_version(record_id, data)
        elif action == 'get_versions':
            return self._get_versions(record_id)
        elif action == 'get_latest':
            return self._get_latest(record_id)
        elif action == 'compare':
            return self._compare_versions(record_id, params)
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown action: {action}"
            )
    
    def _track_version(self, record_id: str, data: Dict) -> ActionResult:
        """Track a new version."""
        if record_id not in self._versions:
            self._versions[record_id] = []
        
        version = {
            'version_id': len(self._versions[record_id]) + 1,
            'data': data.copy(),
            'timestamp': time.time()
        }
        self._versions[record_id].append(version)
        
        return ActionResult(
            success=True,
            data={
                'record_id': record_id,
                'version_id': version['version_id'],
                'total_versions': len(self._versions[record_id])
            },
            error=None
        )
    
    def _get_versions(self, record_id: str) -> ActionResult:
        """Get all versions for a record."""
        if record_id not in self._versions:
            return ActionResult(
                success=False,
                data=None,
                error="No versions found"
            )
        
        return ActionResult(
            success=True,
            data={
                'record_id': record_id,
                'versions': self._versions[record_id],
                'count': len(self._versions[record_id])
            },
            error=None
        )
    
    def _get_latest(self, record_id: str) -> ActionResult:
        """Get latest version."""
        if record_id not in self._versions or not self._versions[record_id]:
            return ActionResult(
                success=False,
                data=None,
                error="No versions found"
            )
        
        return ActionResult(
            success=True,
            data={
                'record_id': record_id,
                'version': self._versions[record_id][-1]
            },
            error=None
        )
    
    def _compare_versions(self, record_id: str, params: Dict) -> ActionResult:
        """Compare two versions."""
        v1 = params.get('version1', 1)
        v2 = params.get('version2', 2)
        
        if record_id not in self._versions:
            return ActionResult(
                success=False,
                data=None,
                error="No versions found"
            )
        
        versions = self._versions[record_id]
        if v1 > len(versions) or v2 > len(versions):
            return ActionResult(
                success=False,
                data=None,
                error="Version out of range"
            )
        
        data1 = versions[v1 - 1]['data']
        data2 = versions[v2 - 1]['data']
        
        changes = self._diff_dicts(data1, data2)
        
        return ActionResult(
            success=True,
            data={
                'record_id': record_id,
                'version1': v1,
                'version2': v2,
                'changes': changes
            },
            error=None
        )
    
    def _diff_dicts(self, d1: Dict, d2: Dict) -> Dict:
        """Diff two dictionaries."""
        changes = {
            'added': {},
            'removed': {},
            'modified': {}
        }
        
        all_keys = set(d1.keys()) | set(d2.keys())
        for key in all_keys:
            if key not in d1:
                changes['added'][key] = d2[key]
            elif key not in d2:
                changes['removed'][key] = d1[key]
            elif d1[key] != d2[key]:
                changes['modified'][key] = {'from': d1[key], 'to': d2[key]}
        
        return changes


def register_actions():
    """Register all Data Fingerprint actions."""
    return [
        DataFingerprintAction,
        DataDeduplicatorAction,
        DataVersionTrackerAction,
    ]
