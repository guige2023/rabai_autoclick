"""Automation Backup Action.

Backs up automation state, configurations, and execution history.
"""
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
import time
import json
import hashlib


@dataclass
class BackupManifest:
    backup_id: str
    created_at: float
    size_bytes: int
    checksum: str
    items: List[str]
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BackupItem:
    category: str
    name: str
    data: Any
    version: int = 1


class AutomationBackupAction:
    """Manages backups of automation state and configs."""

    def __init__(
        self,
        backup_dir: Optional[str] = None,
        max_backups: int = 10,
        compression: bool = False,
    ) -> None:
        self.backup_dir = backup_dir
        self.max_backups = max_backups
        self.compression = compression
        self.manifests: List[BackupManifest] = []
        self.items: Dict[str, BackupItem] = {}

    def add_item(
        self,
        category: str,
        name: str,
        data: Any,
        version: int = 1,
    ) -> None:
        key = f"{category}:{name}"
        self.items[key] = BackupItem(
            category=category,
            name=name,
            data=data,
            version=version,
        )

    def create_backup(
        self,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> BackupManifest:
        backup_id = hashlib.md5(f"{time.time()}".encode()).hexdigest()[:12]
        items_data = {k: {"category": v.category, "name": v.name, "data": v.data, "version": v.version} for k, v in self.items.items()}
        content = json.dumps(items_data, default=str)
        checksum = hashlib.sha256(content.encode()).hexdigest()
        manifest = BackupManifest(
            backup_id=backup_id,
            created_at=time.time(),
            size_bytes=len(content),
            checksum=checksum,
            items=list(self.items.keys()),
            metadata=metadata or {},
        )
        self.manifests.append(manifest)
        if len(self.manifests) > self.max_backups:
            self.manifests.pop(0)
        return manifest

    def restore_backup(self, backup_id: str) -> bool:
        manifest = next((m for m in self.manifests if m.backup_id == backup_id), None)
        return manifest is not None

    def list_backups(self) -> List[Dict[str, Any]]:
        return [
            {
                "backup_id": m.backup_id,
                "created_at": datetime.fromtimestamp(m.created_at).isoformat(),
                "size_bytes": m.size_bytes,
                "checksum": m.checksum,
                "items_count": len(m.items),
                "metadata": m.metadata,
            }
            for m in self.manifests
        ]

    def verify_backup(self, backup_id: str) -> bool:
        manifest = next((m for m in self.manifests if m.backup_id == backup_id), None)
        return manifest is not None
