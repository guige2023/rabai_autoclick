"""
Configuration versioning module for tracking and managing configuration changes.

Supports version history, diff generation, rollback, and environment promotion.
"""
from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class ConfigStatus(Enum):
    """Configuration status."""
    DRAFT = "draft"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"


@dataclass
class ConfigVersion:
    """A configuration version."""
    id: str
    version: int
    config: dict
    environment: str
    status: ConfigStatus = ConfigStatus.DRAFT
    created_at: float = field(default_factory=time.time)
    created_by: str = ""
    comment: str = ""
    checksum: str = ""
    metadata: dict = field(default_factory=dict)


@dataclass
class ConfigDiff:
    """Configuration difference between versions."""
    from_version: int
    to_version: int
    added: dict = field(default_factory=dict)
    removed: dict = field(default_factory=dict)
    modified: dict = field(default_factory=dict)
    unchanged: dict = field(default_factory=dict)


class ConfigVersionManager:
    """
    Configuration versioning service.

    Supports version history, diff generation, rollback,
    and environment promotion.
    """

    def __init__(self):
        self._configs: dict[str, list[ConfigVersion]] = {}
        self._active_configs: dict[str, str] = {}

    def create_config(
        self,
        config_id: str,
        config: dict,
        environment: str,
        created_by: str = "",
        comment: str = "",
        metadata: Optional[dict] = None,
    ) -> ConfigVersion:
        """Create a new configuration version."""
        if config_id not in self._configs:
            self._configs[config_id] = []

        version_num = len(self._configs[config_id]) + 1

        config_str = json.dumps(config, sort_keys=True)
        checksum = hashlib.sha256(config_str.encode()).hexdigest()

        config_version = ConfigVersion(
            id=str(uuid.uuid4())[:12],
            version=version_num,
            config=config,
            environment=environment,
            created_by=created_by,
            comment=comment,
            checksum=checksum,
            metadata=metadata or {},
        )

        self._configs[config_id].append(config_version)
        return config_version

    def update_config(
        self,
        config_id: str,
        config: dict,
        environment: str,
        created_by: str = "",
        comment: str = "",
        status: ConfigStatus = ConfigStatus.DRAFT,
        metadata: Optional[dict] = None,
    ) -> ConfigVersion:
        """Update a configuration with a new version."""
        if config_id not in self._configs:
            return self.create_config(config_id, config, environment, created_by, comment, metadata)

        config_str = json.dumps(config, sort_keys=True)
        checksum = hashlib.sha256(config_str.encode()).hexdigest()

        existing_versions = [v for v in self._configs[config_id] if v.environment == environment]
        version_num = max((v.version for v in existing_versions), default=0) + 1

        config_version = ConfigVersion(
            id=str(uuid.uuid4())[:12],
            version=version_num,
            config=config,
            environment=environment,
            status=status,
            created_by=created_by,
            comment=comment,
            checksum=checksum,
            metadata=metadata or {},
        )

        self._configs[config_id].append(config_version)

        for v in self._configs[config_id]:
            if v.environment == environment:
                if v.version == version_num:
                    v.status = status

        return config_version

    def get_config(
        self,
        config_id: str,
        version: Optional[int] = None,
        environment: Optional[str] = None,
    ) -> Optional[ConfigVersion]:
        """Get a specific configuration version."""
        if config_id not in self._configs:
            return None

        versions = self._configs[config_id]

        if version:
            for v in versions:
                if v.version == version:
                    return v
        elif environment:
            env_versions = [v for v in versions if v.environment == environment]
            if env_versions:
                return max(env_versions, key=lambda v: v.version)
        else:
            if versions:
                return max(versions, key=lambda v: v.version)

        return None

    def get_version_history(
        self,
        config_id: str,
        environment: Optional[str] = None,
        limit: int = 100,
    ) -> list[ConfigVersion]:
        """Get version history for a configuration."""
        if config_id not in self._configs:
            return []

        versions = self._configs[config_id]

        if environment:
            versions = [v for v in versions if v.environment == environment]

        return sorted(versions, key=lambda v: v.version, reverse=True)[:limit]

    def rollback(
        self,
        config_id: str,
        target_version: int,
        environment: str,
        created_by: str = "",
        comment: str = "",
    ) -> Optional[ConfigVersion]:
        """Rollback to a previous version."""
        current = self.get_config(config_id, environment=environment)
        if not current:
            return None

        target = None
        for v in self._configs.get(config_id, []):
            if v.version == target_version and v.environment == environment:
                target = v
                break

        if not target:
            return None

        return self.update_config(
            config_id,
            target.config.copy(),
            environment,
            created_by=created_by,
            comment=f"Rollback to version {target_version}: {comment}" if comment else f"Rollback to version {target_version}",
        )

    def diff(
        self,
        config_id: str,
        from_version: int,
        to_version: int,
        environment: Optional[str] = None,
    ) -> Optional[ConfigDiff]:
        """Generate a diff between two versions."""
        from_v = self.get_config(config_id, version=from_version)
        to_v = self.get_config(config_id, version=to_version)

        if not from_v or not to_v:
            return None

        if environment and (from_v.environment != environment or to_v.environment != environment):
            return None

        from_keys = set(from_v.config.keys())
        to_keys = set(to_v.config.keys())

        added = {k: to_v.config[k] for k in to_keys - from_keys}
        removed = {k: from_v.config[k] for k in from_keys - to_keys}
        modified = {}

        for k in from_keys & to_keys:
            from_val = json.dumps(from_v.config[k], sort_keys=True)
            to_val = json.dumps(to_v.config[k], sort_keys=True)
            if from_val != to_val:
                modified[k] = {"from": from_v.config[k], "to": to_v.config[k]}

        unchanged = {k: to_v.config[k] for k in from_keys & to_keys - set(modified.keys())}

        return ConfigDiff(
            from_version=from_version,
            to_version=to_version,
            added=added,
            removed=removed,
            modified=modified,
            unchanged=unchanged,
        )

    def promote(
        self,
        config_id: str,
        from_environment: str,
        to_environment: str,
        created_by: str = "",
        comment: str = "",
    ) -> Optional[ConfigVersion]:
        """Promote a configuration from one environment to another."""
        source = self.get_config(config_id, environment=from_environment)
        if not source:
            return None

        return self.update_config(
            config_id,
            source.config.copy(),
            to_environment,
            created_by=created_by,
            comment=f"Promoted from {from_environment}: {comment}" if comment else f"Promoted from {from_environment}",
        )

    def compare_environments(
        self,
        config_id: str,
        env1: str,
        env2: str,
    ) -> Optional[ConfigDiff]:
        """Compare configurations between two environments."""
        v1 = self.get_config(config_id, environment=env1)
        v2 = self.get_config(config_id, environment=env2)

        if not v1 or not v2:
            return None

        from_keys = set(v1.config.keys())
        to_keys = set(v2.config.keys())

        added = {k: v2.config[k] for k in to_keys - from_keys}
        removed = {k: v1.config[k] for k in from_keys - to_keys}
        modified = {}

        for k in from_keys & to_keys:
            from_val = json.dumps(v1.config[k], sort_keys=True)
            to_val = json.dumps(v2.config[k], sort_keys=True)
            if from_val != to_val:
                modified[k] = {"from": v1.config[k], "to": v2.config[k]}

        unchanged = {k: v2.config[k] for k in from_keys & to_keys - set(modified.keys())}

        return ConfigDiff(
            from_version=v1.version,
            to_version=v2.version,
            added=added,
            removed=removed,
            modified=modified,
            unchanged=unchanged,
        )

    def archive_version(
        self,
        config_id: str,
        version: int,
        environment: Optional[str] = None,
    ) -> bool:
        """Archive a specific version."""
        config = self.get_config(config_id, version=version)
        if not config:
            return False

        if environment and config.environment != environment:
            return False

        config.status = ConfigStatus.ARCHIVED
        return True

    def list_configs(self) -> list[str]:
        """List all configuration IDs."""
        return list(self._configs.keys())

    def get_stats(self) -> dict:
        """Get configuration statistics."""
        total_versions = sum(len(versions) for versions in self._configs.values())

        by_environment = {}
        for config_id, versions in self._configs.items():
            for v in versions:
                env = v.environment
                by_environment[env] = by_environment.get(env, 0) + 1

        return {
            "total_configs": len(self._configs),
            "total_versions": total_versions,
            "by_environment": by_environment,
        }
