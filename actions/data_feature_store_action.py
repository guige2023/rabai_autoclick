"""
Data Feature Store Action Module

Provides feature store capabilities for machine learning feature management.
Supports feature registration, versioning, serving, and point-in-time correctness
for ML training and inference.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class FeatureType(Enum):
    """Feature value types."""

    NUMERICAL = "numerical"
    CATEGORICAL = "categorical"
    BOOLEAN = "boolean"
    TEXT = "text"
    TIMESTAMP = "timestamp"


class FeatureStatus(Enum):
    """Feature registration status."""

    DRAFT = "draft"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"


@dataclass
class Feature:
    """A feature definition."""

    feature_id: str
    name: str
    feature_group: str
    value_type: FeatureType
    description: str = ""
    status: FeatureStatus = FeatureStatus.DRAFT
    default_value: Any = None
    constraints: Dict[str, Any] = field(default_factory=dict)
    tags: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FeatureVersion:
    """A version of a feature."""

    version_id: str
    feature_id: str
    version: int
    source: str
    transformation: Optional[str] = None
    created_at: float
    created_by: str
    statistics: Dict[str, float] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)


@dataclass
class FeatureValue:
    """A feature value for an entity."""

    feature_id: str
    entity_id: str
    value: Any
    timestamp: float
    version_id: Optional[str] = None


@dataclass
class FeatureGroup:
    """A group of related features."""

    group_id: str
    name: str
    entity_id_column: str
    feature_ids: List[str] = field(default_factory=list)
    description: str = ""
    version: int = 1
    created_at: float = field(default_factory=time.time)


@dataclass
class FeatureConfig:
    """Configuration for feature store."""

    default_ttl_seconds: float = 86400 * 7
    enable_versioning: bool = True
    enable_point_in_time: bool = True
    max_versions_per_feature: int = 10
    default_feature_group: str = "default"


class DataFeatureStoreAction:
    """
    Feature store action for ML feature management.

    Features:
    - Feature registration and versioning
    - Feature group management
    - Point-in-time correct feature retrieval
    - Feature serving with low latency
    - Feature metadata and documentation
    - Feature statistics tracking

    Usage:
        store = DataFeatureStoreAction(config)
        store.register_feature(feature)
        features = await store.get_features(entity_id="user123", features=["age", "income"])
    """

    def __init__(self, config: Optional[FeatureConfig] = None):
        self.config = config or FeatureConfig()
        self._features: Dict[str, Feature] = {}
        self._feature_versions: Dict[str, List[FeatureVersion]] = {}
        self._feature_values: Dict[str, Dict[str, FeatureValue]] = {}
        self._feature_groups: Dict[str, FeatureGroup] = {}
        self._stats = {
            "features_registered": 0,
            "features_served": 0,
            "versions_created": 0,
            "groups_created": 0,
        }

    def register_feature(
        self,
        name: str,
        feature_group: str,
        value_type: FeatureType,
        description: str = "",
        default_value: Any = None,
        tags: Optional[Set[str]] = None,
    ) -> Feature:
        """Register a new feature."""
        feature_id = f"feat_{uuid.uuid4().hex[:12]}"
        feature = Feature(
            feature_id=feature_id,
            name=name,
            feature_group=feature_group,
            value_type=value_type,
            description=description,
            default_value=default_value,
            tags=tags or set(),
        )
        self._features[feature_id] = feature
        self._stats["features_registered"] += 1
        return feature

    def get_feature(self, feature_id: str) -> Optional[Feature]:
        """Get a feature by ID."""
        return self._features.get(feature_id)

    def get_feature_by_name(self, name: str) -> Optional[Feature]:
        """Get a feature by name."""
        for feature in self._features.values():
            if feature.name == name:
                return feature
        return None

    def create_feature_group(
        self,
        name: str,
        entity_id_column: str,
        feature_ids: Optional[List[str]] = None,
        description: str = "",
    ) -> FeatureGroup:
        """Create a feature group."""
        group_id = f"fg_{uuid.uuid4().hex[:8]}"
        group = FeatureGroup(
            group_id=group_id,
            name=name,
            entity_id_column=entity_id_column,
            feature_ids=feature_ids or [],
            description=description,
        )
        self._feature_groups[group_id] = group
        self._stats["groups_created"] += 1

        for feature_id in group.feature_ids:
            feature = self._features.get(feature_id)
            if feature and feature.feature_group != name:
                feature.feature_group = name

        return group

    def add_feature_to_group(
        self,
        group_id: str,
        feature_id: str,
    ) -> bool:
        """Add a feature to a group."""
        group = self._feature_groups.get(group_id)
        if group is None:
            return False

        if feature_id not in group.feature_ids:
            group.feature_ids.append(feature_id)

        return True

    def write_features(
        self,
        entity_id: str,
        features: Dict[str, Any],
        timestamp: Optional[float] = None,
    ) -> List[FeatureValue]:
        """Write feature values for an entity."""
        ts = timestamp or time.time()
        values = []

        for feature_name, value in features.items():
            feature = self.get_feature_by_name(feature_name)
            if feature is None:
                continue

            if entity_id not in self._feature_values:
                self._feature_values[entity_id] = {}

            feature_value = FeatureValue(
                feature_id=feature.feature_id,
                entity_id=entity_id,
                value=value,
                timestamp=ts,
            )
            self._feature_values[entity_id][feature.feature_id] = feature_value
            values.append(feature_value)

        return values

    async def get_features(
        self,
        entity_id: str,
        feature_names: List[str],
        timestamp: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Get feature values for an entity.

        Args:
            entity_id: Entity identifier
            feature_names: List of feature names to retrieve
            timestamp: Optional timestamp for point-in-time retrieval

        Returns:
            Dictionary of feature_name -> value
        """
        ts = timestamp or time.time()
        result = {}

        entity_values = self._feature_values.get(entity_id, {})

        for feature_name in feature_names:
            feature = self.get_feature_by_name(feature_name)
            if feature is None:
                continue

            if self.config.enable_point_in_time and timestamp:
                # Find the most recent value before or at timestamp
                best_value = None
                for fv in entity_values.values():
                    if fv.feature_id == feature.feature_id and fv.timestamp <= ts:
                        if best_value is None or fv.timestamp > best_value.timestamp:
                            best_value = fv
                if best_value:
                    result[feature_name] = best_value.value
                else:
                    result[feature_name] = feature.default_value
            else:
                fv = entity_values.get(feature.feature_id)
                if fv:
                    result[feature_name] = fv.value
                else:
                    result[feature_name] = feature.default_value

        self._stats["features_served"] += len(result)
        return result

    async def get_feature_group(
        self,
        group_name: str,
        entity_id: str,
        timestamp: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Get all features in a group for an entity."""
        for group in self._feature_groups.values():
            if group.name == group_name:
                feature_names = []
                for feature_id in group.feature_ids:
                    feature = self._features.get(feature_id)
                    if feature:
                        feature_names.append(feature.name)

                return await self.get_features(entity_id, feature_names, timestamp)

        return {}

    def register_version(
        self,
        feature_id: str,
        source: str,
        transformation: Optional[str] = None,
        created_by: str = "system",
        statistics: Optional[Dict[str, float]] = None,
        dependencies: Optional[List[str]] = None,
    ) -> FeatureVersion:
        """Register a new version of a feature."""
        if feature_id not in self._feature_versions:
            self._feature_versions[feature_id] = []

        version_num = len(self._feature_versions[feature_id]) + 1
        version = FeatureVersion(
            version_id=f"v_{uuid.uuid4().hex[:12]}",
            feature_id=feature_id,
            version=version_num,
            source=source,
            transformation=transformation,
            created_at=time.time(),
            created_by=created_by,
            statistics=statistics or {},
            dependencies=dependencies or [],
        )
        self._feature_versions[feature_id].append(version)
        self._stats["versions_created"] += 1

        return version

    def get_feature_versions(self, feature_id: str) -> List[FeatureVersion]:
        """Get all versions of a feature."""
        return self._feature_versions.get(feature_id, [])

    def get_stats(self) -> Dict[str, Any]:
        """Get feature store statistics."""
        return {
            **self._stats.copy(),
            "total_features": len(self._features),
            "total_groups": len(self._feature_groups),
            "total_entities": len(self._feature_values),
        }


async def demo_feature_store():
    """Demonstrate feature store."""
    config = FeatureConfig()
    store = DataFeatureStoreAction(config)

    age_feat = store.register_feature(
        name="user_age",
        feature_group="user demographics",
        value_type=FeatureType.NUMERICAL,
        description="Age of the user",
    )

    income_feat = store.register_feature(
        name="user_income",
        feature_group="user demographics",
        value_type=FeatureType.NUMERICAL,
        description="Annual income",
    )

    store.write_features(
        entity_id="user123",
        features={"user_age": 30, "user_income": 75000},
    )

    features = await store.get_features(
        entity_id="user123",
        feature_names=["user_age", "user_income"],
    )

    print(f"Features: {features}")
    print(f"Stats: {store.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_feature_store())
