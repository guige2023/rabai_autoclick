"""
Data Versioning Action.

Provides data versioning capabilities.
Supports:
- Snapshot versioning
- Change tracking
- Version comparison
- Rollback
"""

from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from datetime import datetime
import logging
import json
import hashlib
import copy

logger = logging.getLogger(__name__)


@dataclass
class Version:
    """Represents a data version."""
    version_id: str
    version_number: int
    timestamp: datetime
    data_hash: str
    data_size: int
    metadata: Dict[str, Any] = field(default_factory=dict)
    parent_version_id: Optional[str] = None
    change_summary: str = ""
    created_by: str = "system"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "version_id": self.version_id,
            "version_number": self.version_number,
            "timestamp": self.timestamp.isoformat(),
            "data_hash": self.data_hash,
            "data_size": self.data_size,
            "metadata": self.metadata,
            "parent_version_id": self.parent_version_id,
            "change_summary": self.change_summary,
            "created_by": self.created_by
        }


@dataclass
class ChangeRecord:
    """Record of a data change."""
    path: str
    operation: str  # add, update, delete
    old_value: Any = None
    new_value: Any = None
    timestamp: datetime = field(default_factory=datetime.utcnow)


class DataVersioningAction:
    """
    Data Versioning Action.
    
    Provides data versioning with support for:
    - Creating snapshots
    - Tracking changes
    - Comparing versions
    - Rolling back to previous versions
    """
    
    def __init__(self, entity_name: str):
        """
        Initialize the Data Versioning Action.
        
        Args:
            entity_name: Name of the entity being versioned
        """
        self.entity_name = entity_name
        self.versions: Dict[str, Version] = {}
        self.current_version_id: Optional[str] = None
        self._next_version_number = 1
        self._change_tracking: List[ChangeRecord] = []
        self._data_store: Dict[str, Any] = {}
    
    def create_version(
        self,
        data: Any,
        change_summary: str = "",
        metadata: Optional[Dict[str, Any]] = None,
        created_by: str = "system"
    ) -> Version:
        """
        Create a new version snapshot.
        
        Args:
            data: Data to version
            change_summary: Description of changes
            metadata: Additional metadata
            created_by: User/system that created this version
        
        Returns:
            Created Version object
        """
        # Serialize data for hashing
        data_str = json.dumps(data, sort_keys=True, default=str)
        data_hash = hashlib.sha256(data_str.encode()).hexdigest()
        
        # Calculate size
        data_size = len(data_str.encode())
        
        # Generate version ID
        version_id = self._generate_version_id(data_hash)
        
        version = Version(
            version_id=version_id,
            version_number=self._next_version_number,
            timestamp=datetime.utcnow(),
            data_hash=data_hash,
            data_size=data_size,
            metadata=metadata or {},
            parent_version_id=self.current_version_id,
            change_summary=change_summary,
            created_by=created_by
        )
        
        self.versions[version_id] = version
        self.current_version_id = version_id
        self._data_store[version_id] = copy.deepcopy(data)
        self._next_version_number += 1
        
        logger.info(
            f"Created version {version_id} (v{version.version_number}) "
            f"for {self.entity_name}"
        )
        
        return version
    
    def get_version(self, version_id: str) -> Optional[Version]:
        """Get a specific version."""
        return self.versions.get(version_id)
    
    def get_current_version(self) -> Optional[Version]:
        """Get the current version."""
        if self.current_version_id:
            return self.versions.get(self.current_version_id)
        return None
    
    def get_data(self, version_id: str) -> Optional[Any]:
        """Get data for a specific version."""
        return self._data_store.get(version_id)
    
    def list_versions(
        self,
        limit: int = 100,
        offset: int = 0
    ) -> List[Version]:
        """List versions in descending order (newest first)."""
        sorted_versions = sorted(
            self.versions.values(),
            key=lambda v: v.version_number,
            reverse=True
        )
        return sorted_versions[offset:offset + limit]
    
    def compare_versions(
        self,
        version_id_1: str,
        version_id_2: str
    ) -> Dict[str, Any]:
        """
        Compare two versions.
        
        Args:
            version_id_1: First version ID
            version_id_2: Second version ID
        
        Returns:
            Comparison result with changes
        """
        data1 = self._data_store.get(version_id_1)
        data2 = self._data_store.get(version_id_2)
        
        if data1 is None or data2 is None:
            raise ValueError("One or both versions not found")
        
        changes = self._diff_data(data1, data2)
        
        v1 = self.versions[version_id_1]
        v2 = self.versions[version_id_2]
        
        return {
            "version1": v1.to_dict(),
            "version2": v2.to_dict(),
            "changes": changes,
            "change_count": len(changes.get("all_changes", []))
        }
    
    def _diff_data(self, data1: Any, data2: Any, path: str = "") -> Dict[str, Any]:
        """Recursively diff two data structures."""
        changes = []
        
        if type(data1) != type(data2):
            changes.append({
                "path": path or "root",
                "operation": "type_change",
                "old_value": str(data1),
                "new_value": str(data2)
            })
            return {"all_changes": changes}
        
        if isinstance(data1, dict):
            all_keys = set(data1.keys()) | set(data2.keys())
            for key in all_keys:
                new_path = f"{path}.{key}" if path else key
                
                if key not in data1:
                    changes.append({
                        "path": new_path,
                        "operation": "add",
                        "new_value": data2[key]
                    })
                elif key not in data2:
                    changes.append({
                        "path": new_path,
                        "operation": "delete",
                        "old_value": data1[key]
                    })
                else:
                    sub_changes = self._diff_data(data1[key], data2[key], new_path)
                    changes.extend(sub_changes.get("all_changes", []))
        
        elif isinstance(data1, list):
            max_len = max(len(data1), len(data2))
            for i in range(max_len):
                new_path = f"{path}[{i}]"
                
                if i >= len(data1):
                    changes.append({
                        "path": new_path,
                        "operation": "add",
                        "new_value": data2[i]
                    })
                elif i >= len(data2):
                    changes.append({
                        "path": new_path,
                        "operation": "delete",
                        "old_value": data1[i]
                    })
                else:
                    sub_changes = self._diff_data(data1[i], data2[i], new_path)
                    changes.extend(sub_changes.get("all_changes", []))
        
        else:
            if data1 != data2:
                changes.append({
                    "path": path or "value",
                    "operation": "update",
                    "old_value": data1,
                    "new_value": data2
                })
        
        return {"all_changes": changes}
    
    def rollback(self, version_id: str) -> Optional[Any]:
        """
        Rollback to a specific version.
        
        Args:
            version_id: Version ID to rollback to
        
        Returns:
            Data at the rolled back version
        """
        if version_id not in self.versions:
            logger.error(f"Version {version_id} not found")
            return None
        
        data = self._data_store.get(version_id)
        if data is None:
            logger.error(f"Data for version {version_id} not found")
            return None
        
        # Create a new version for the rollback
        new_version = self.create_version(
            data=copy.deepcopy(data),
            change_summary=f"Rollback to version {version_id}",
            metadata={"rollback_from": self.current_version_id}
        )
        
        logger.info(f"Rolled back to version {version_id}, created new version {new_version.version_id}")
        return data
    
    def start_tracking_changes(self) -> None:
        """Start tracking changes for the current version."""
        self._change_tracking = []
    
    def stop_tracking_changes(self) -> List[ChangeRecord]:
        """Stop tracking and return changes."""
        changes = self._change_tracking.copy()
        self._change_tracking = []
        return changes
    
    def record_change(
        self,
        path: str,
        operation: str,
        old_value: Any = None,
        new_value: Any = None
    ) -> None:
        """Record a change."""
        record = ChangeRecord(
            path=path,
            operation=operation,
            old_value=old_value,
            new_value=new_value
        )
        self._change_tracking.append(record)
    
    def get_version_history(
        self,
        since_version: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get version history.
        
        Args:
            since_version: Start from this version
            limit: Maximum versions to return
        
        Returns:
            List of version summaries
        """
        history = []
        current = self.versions.get(since_version) if since_version else self.get_current_version()
        
        while current and len(history) < limit:
            history.append(current.to_dict())
            if current.parent_version_id:
                current = self.versions.get(current.parent_version_id)
            else:
                break
        
        return history
    
    def get_stats(self) -> Dict[str, Any]:
        """Get versioning statistics."""
        return {
            "entity_name": self.entity_name,
            "total_versions": len(self.versions),
            "current_version_id": self.current_version_id,
            "current_version_number": self.versions[self.current_version_id].version_number if self.current_version_id else None,
            "total_data_size": sum(v.data_size for v in self.versions.values()),
            "tracking_changes": len(self._change_tracking) > 0
        }
    
    def _generate_version_id(self, data_hash: str) -> str:
        """Generate a unique version ID."""
        timestamp = datetime.utcnow().isoformat()
        content = f"{self.entity_name}:{data_hash}:{timestamp}"
        return f"v{hashlib.md5(content.encode()).hexdigest()[:12]}"
    
    def prune_old_versions(self, keep_count: int = 10) -> int:
        """
        Prune old versions, keeping the most recent ones.
        
        Args:
            keep_count: Number of recent versions to keep
        
        Returns:
            Number of versions pruned
        """
        versions = sorted(
            self.versions.values(),
            key=lambda v: v.version_number,
            reverse=True
        )
        
        to_delete = versions[keep_count:]
        
        for version in to_delete:
            del self.versions[version.version_id]
            if version.version_id in self._data_store:
                del self._data_store[version.version_id]
            logger.info(f"Pruned version {version.version_id}")
        
        return len(to_delete)


# Standalone execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Create versioning for a user profile
    versioning = DataVersioningAction("user_profile")
    
    # Create initial version
    v1 = versioning.create_version(
        data={"name": "Alice", "email": "alice@example.com", "age": 30},
        change_summary="Initial version",
        created_by="system"
    )
    print(f"Created version: {v1.version_id} (v{v1.version_number})")
    
    # Update and create new version
    v2 = versioning.create_version(
        data={"name": "Alice", "email": "alice@example.com", "age": 31, "city": "NYC"},
        change_summary="Added city, updated age",
        created_by="user"
    )
    print(f"Created version: {v2.version_id} (v{v2.version_number})")
    
    # Another update
    v3 = versioning.create_version(
        data={"name": "Alice Smith", "email": "alice@example.com", "age": 31, "city": "NYC"},
        change_summary="Updated name"
    )
    print(f"Created version: {v3.version_id} (v{v3.version_number})")
    
    # Compare versions
    comparison = versioning.compare_versions(v1.version_id, v3.version_id)
    print(f"\nComparison v{v1.version_number} vs v{v3.version_number}:")
    print(f"  Changes: {comparison['change_count']}")
    for change in comparison["changes"][:5]:
        print(f"    - {change['path']}: {change['operation']}")
    
    # Rollback
    data = versioning.rollback(v2.version_id)
    print(f"\nRolled back to v{v2.version_number}: {data}")
    
    # Stats
    print(f"\nStats: {json.dumps(versioning.get_stats(), indent=2, default=str)}")
