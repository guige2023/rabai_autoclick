"""Data Versioning Action.

Tracks versions of data entities with full history.
"""
from typing import Any, Dict, List, Optional, Generic, TypeVar
from dataclasses import dataclass, field
import time
import uuid
import copy


T = TypeVar("T")


@dataclass
class Version:
    version_id: str
    version_number: int
    created_at: float
    created_by: Optional[str]
    data: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)


@dataclass
class VersionedEntity(Generic[T]):
    entity_id: str
    entity_type: str
    current_version: Optional[Version]
    history: List[Version] = field(default_factory=list)

    def add_version(self, data: Dict[str, Any], user: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None, tags: Optional[List[str]] = None) -> Version:
        version_number = (self.current_version.version_number + 1) if self.current_version else 1
        v = Version(
            version_id=uuid.uuid4().hex,
            version_number=version_number,
            created_at=time.time(),
            created_by=user,
            data=copy.deepcopy(data),
            metadata=metadata or {},
            tags=tags or [],
        )
        self.history.append(v)
        self.current_version = v
        return v


class DataVersioningAction(Generic[T]):
    """Version control for data entities."""

    def __init__(self) -> None:
        self.entities: Dict[str, VersionedEntity] = {}

    def create(
        self,
        entity_id: str,
        entity_type: str,
        initial_data: Dict[str, Any],
        user: Optional[str] = None,
    ) -> VersionedEntity:
        entity = VersionedEntity(
            entity_id=entity_id,
            entity_type=entity_type,
            current_version=None,
        )
        entity.add_version(initial_data, user=user)
        self.entities[entity_id] = entity
        return entity

    def update(
        self,
        entity_id: str,
        data: Dict[str, Any],
        user: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
    ) -> Optional[Version]:
        entity = self.entities.get(entity_id)
        if not entity:
            return None
        return entity.add_version(data, user=user, metadata=metadata, tags=tags)

    def get_version(
        self,
        entity_id: str,
        version_number: Optional[int] = None,
    ) -> Optional[Version]:
        entity = self.entities.get(entity_id)
        if not entity:
            return None
        if version_number is None:
            return entity.current_version
        return next((v for v in entity.history if v.version_number == version_number), None)

    def get_history(
        self,
        entity_id: str,
        limit: int = 50,
    ) -> List[Version]:
        entity = self.entities.get(entity_id)
        if not entity:
            return []
        return entity.history[-limit:]

    def rollback(
        self,
        entity_id: str,
        version_number: int,
    ) -> bool:
        entity = self.entities.get(entity_id)
        if not entity:
            return False
        target = self.get_version(entity_id, version_number)
        if not target:
            return False
        entity.add_version(
            copy.deepcopy(target.data),
            user="system",
            metadata={"rollback_from": entity.current_version.version_number if entity.current_version else None},
        )
        return True

    def diff(
        self,
        entity_id: str,
        v1: int,
        v2: int,
    ) -> Dict[str, Any]:
        ver1 = self.get_version(entity_id, v1)
        ver2 = self.get_version(entity_id, v2)
        if not ver1 or not ver2:
            return {}
        added = set(ver2.data.keys()) - set(ver1.data.keys())
        removed = set(ver1.data.keys()) - set(ver2.data.keys())
        changed = {k for k in set(ver1.data.keys()) & set(ver2.data.keys()) if ver1.data[k] != ver2.data[k]}
        return {"added": list(added), "removed": list(removed), "changed": list(changed)}
