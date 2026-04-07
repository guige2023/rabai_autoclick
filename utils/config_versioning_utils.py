"""
Configuration Versioning Utilities.

Provides utilities for managing configuration file versioning,
tracking changes across environments, and supporting configuration
rollbacks and audits.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional


class ConfigFormat(Enum):
    """Supported configuration file formats."""
    JSON = "json"
    YAML = "yaml"
    INI = "ini"
    ENV = "env"
    TOML = "toml"
    XML = "xml"


class ChangeType(Enum):
    """Types of configuration changes."""
    ADDED = "added"
    MODIFIED = "modified"
    DELETED = "deleted"
    RENAMED = "renamed"


@dataclass
class ConfigChange:
    """Represents a single configuration change."""
    path: str
    change_type: ChangeType
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ConfigVersion:
    """Represents a configuration version snapshot."""
    version_id: str
    config_path: str
    environment: str
    version_number: int
    content_hash: str
    content: dict[str, Any]
    changes: list[ConfigChange]
    created_at: datetime
    created_by: str
    message: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version_id": self.version_id,
            "config_path": self.config_path,
            "environment": self.environment,
            "version_number": self.version_number,
            "content_hash": self.content_hash,
            "content": self.content,
            "changes": [
                {
                    "path": c.path,
                    "change_type": c.change_type.value,
                    "old_value": c.old_value,
                    "new_value": c.new_value,
                    "timestamp": c.timestamp.isoformat(),
                }
                for c in self.changes
            ],
            "created_at": self.created_at.isoformat(),
            "created_by": self.created_by,
            "message": self.message,
            "metadata": self.metadata,
        }


class ConfigVersioningManager:
    """Manages configuration file versioning and history."""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        config_dir: Optional[Path] = None,
    ) -> None:
        self.db_path = db_path or Path("config_versions.db")
        self.config_dir = config_dir or Path(".")
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the versioning database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS config_versions (
                version_id TEXT PRIMARY KEY,
                config_path TEXT NOT NULL,
                environment TEXT NOT NULL,
                version_number INTEGER NOT NULL,
                content_hash TEXT NOT NULL,
                content_json TEXT NOT NULL,
                changes_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                created_by TEXT NOT NULL,
                message TEXT,
                metadata_json TEXT,
                UNIQUE(config_path, environment, version_number)
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_config_env
            ON config_versions(config_path, environment)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_created_at
            ON config_versions(created_at)
        """)
        conn.commit()
        conn.close()

    def compute_hash(self, content: dict[str, Any]) -> str:
        """Compute hash of configuration content."""
        content_str = json.dumps(content, sort_keys=True, default=str)
        return hashlib.sha256(content_str.encode()).hexdigest()

    def parse_config_file(
        self,
        file_path: Path,
        format: Optional[ConfigFormat] = None,
    ) -> dict[str, Any]:
        """Parse a configuration file into a dictionary."""
        if format is None:
            format = self._detect_format(file_path)

        content = file_path.read_text()

        if format == ConfigFormat.JSON:
            return json.loads(content)
        elif format == ConfigFormat.YAML:
            import yaml
            return yaml.safe_load(content) or {}
        elif format == ConfigFormat.INI:
            return self._parse_ini(content)
        elif format == ConfigFormat.ENV:
            return self._parse_env(content)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def _detect_format(self, file_path: Path) -> ConfigFormat:
        """Detect configuration format from file extension."""
        ext = file_path.suffix.lower()
        format_map = {
            ".json": ConfigFormat.JSON,
            ".yaml": ConfigFormat.YAML,
            ".yml": ConfigFormat.YAML,
            ".ini": ConfigFormat.INI,
            ".env": ConfigFormat.ENV,
            ".toml": ConfigFormat.TOML,
            ".xml": ConfigFormat.XML,
        }
        return format_map.get(ext, ConfigFormat.JSON)

    def _parse_ini(self, content: str) -> dict[str, Any]:
        """Parse INI format content."""
        result: dict[str, Any] = {}
        current_section = "default"
        result[current_section] = {}

        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or line.startswith(";"):
                continue
            if line.startswith("[") and line.endswith("]"):
                current_section = line[1:-1]
                result[current_section] = {}
            elif "=" in line:
                key, _, value = line.partition("=")
                result[current_section][key.strip()] = value.strip()

        return result

    def _parse_env(self, content: str) -> dict[str, Any]:
        """Parse ENV format content."""
        result = {}
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                result[key.strip()] = value.strip().strip("\"'")
        return result

    def write_config_file(
        self,
        file_path: Path,
        content: dict[str, Any],
        format: Optional[ConfigFormat] = None,
    ) -> None:
        """Write configuration content to a file."""
        if format is None:
            format = self._detect_format(file_path)

        if format == ConfigFormat.JSON:
            file_path.write_text(json.dumps(content, indent=2, default=str))
        elif format == ConfigFormat.YAML:
            import yaml
            file_path.write_text(yaml.dump(content, default_flow_style=False))
        elif format == ConfigFormat.INI:
            file_path.write_text(self._format_ini(content))
        elif format == ConfigFormat.ENV:
            file_path.write_text(self._format_env(content))
        else:
            raise ValueError(f"Unsupported format: {format}")

    def _format_ini(self, content: dict[str, Any]) -> str:
        """Format dictionary as INI content."""
        lines = []
        for section, values in content.items():
            if section != "default":
                lines.append(f"[{section}]")
            for key, value in values.items():
                lines.append(f"{key} = {value}")
            lines.append("")
        return "\n".join(lines)

    def _format_env(self, content: dict[str, Any]) -> str:
        """Format dictionary as ENV content."""
        lines = []
        for key, value in content.items():
            if isinstance(value, dict):
                for subkey, subvalue in value.items():
                    lines.append(f"{key}_{subkey.upper()}={subvalue}")
            else:
                lines.append(f"{key.upper()}={value}")
        return "\n".join(lines)

    def create_version(
        self,
        config_path: str,
        environment: str,
        content: dict[str, Any],
        created_by: str,
        message: str = "",
        metadata: Optional[dict[str, Any]] = None,
    ) -> ConfigVersion:
        """Create a new configuration version."""
        version_id = f"v_{datetime.now().strftime('%Y%m%d%H%M%S')}_{hashlib.md5(str(datetime.now()).encode()).hexdigest()[:8]}"

        version_number = self._get_next_version(config_path, environment)
        content_hash = self.compute_hash(content)

        previous_version = self.get_latest_version(config_path, environment)
        changes: list[ConfigChange] = []

        if previous_version:
            changes = self._compute_diff(previous_version.content, content)

        version = ConfigVersion(
            version_id=version_id,
            config_path=config_path,
            environment=environment,
            version_number=version_number,
            content_hash=content_hash,
            content=content,
            changes=changes,
            created_at=datetime.now(),
            created_by=created_by,
            message=message,
            metadata=metadata or {},
        )

        self._save_version(version)
        return version

    def _get_next_version(self, config_path: str, environment: str) -> int:
        """Get the next version number for a config."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute(
            "SELECT MAX(version_number) FROM config_versions WHERE config_path = ? AND environment = ?",
            (config_path, environment),
        )
        row = cursor.fetchone()
        conn.close()
        return (row[0] or 0) + 1

    def _compute_diff(
        self,
        old_content: dict[str, Any],
        new_content: dict[str, Any],
    ) -> list[ConfigChange]:
        """Compute differences between two configurations."""
        changes: list[ConfigChange] = []

        all_keys = set(old_content.keys()) | set(new_content.keys())

        for key in all_keys:
            old_val = old_content.get(key)
            new_val = new_content.get(key)

            if key not in old_content:
                changes.append(ConfigChange(
                    path=key,
                    change_type=ChangeType.ADDED,
                    new_value=new_val,
                ))
            elif key not in new_content:
                changes.append(ConfigChange(
                    path=key,
                    change_type=ChangeType.DELETED,
                    old_value=old_val,
                ))
            elif old_val != new_val:
                changes.append(ConfigChange(
                    path=key,
                    change_type=ChangeType.MODIFIED,
                    old_value=old_val,
                    new_value=new_val,
                ))

        return changes

    def _save_version(self, version: ConfigVersion) -> None:
        """Save a version to the database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO config_versions (
                version_id, config_path, environment, version_number,
                content_hash, content_json, changes_json, created_at,
                created_by, message, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            version.version_id,
            version.config_path,
            version.environment,
            version.version_number,
            version.content_hash,
            json.dumps(version.content, default=str),
            json.dumps([c.__dict__ for c in version.changes], default=str),
            version.created_at.isoformat(),
            version.created_by,
            version.message,
            json.dumps(version.metadata, default=str),
        ))
        conn.commit()
        conn.close()

    def get_version(
        self,
        config_path: str,
        environment: str,
        version_number: int,
    ) -> Optional[ConfigVersion]:
        """Get a specific version of a configuration."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM config_versions
            WHERE config_path = ? AND environment = ? AND version_number = ?
        """, (config_path, environment, version_number))
        row = cursor.fetchone()
        conn.close()

        if row:
            return self._row_to_version(row)
        return None

    def get_latest_version(
        self,
        config_path: str,
        environment: str,
    ) -> Optional[ConfigVersion]:
        """Get the latest version of a configuration."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM config_versions
            WHERE config_path = ? AND environment = ?
            ORDER BY version_number DESC LIMIT 1
        """, (config_path, environment))
        row = cursor.fetchone()
        conn.close()

        if row:
            return self._row_to_version(row)
        return None

    def get_version_history(
        self,
        config_path: str,
        environment: str,
        limit: int = 50,
    ) -> list[ConfigVersion]:
        """Get version history for a configuration."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM config_versions
            WHERE config_path = ? AND environment = ?
            ORDER BY version_number DESC LIMIT ?
        """, (config_path, environment, limit))
        rows = cursor.fetchall()
        conn.close()

        return [self._row_to_version(row) for row in rows]

    def rollback(
        self,
        config_path: str,
        environment: str,
        target_version: int,
    ) -> ConfigVersion:
        """Rollback configuration to a specific version."""
        version = self.get_version(config_path, environment, target_version)
        if not version:
            raise ValueError(f"Version not found: {target_version}")

        return self.create_version(
            config_path=config_path,
            environment=environment,
            content=version.content,
            created_by="system",
            message=f"Rollback to version {target_version}",
        )

    def _row_to_version(self, row: sqlite3.Row) -> ConfigVersion:
        """Convert a database row to a ConfigVersion object."""
        changes_data = json.loads(row["changes_json"])
        changes = [
            ConfigChange(
                path=c["path"],
                change_type=ChangeType(c["change_type"]),
                old_value=c.get("old_value"),
                new_value=c.get("new_value"),
                timestamp=datetime.fromisoformat(c["timestamp"]) if c.get("timestamp") else datetime.now(),
            )
            for c in changes_data
        ]

        return ConfigVersion(
            version_id=row["version_id"],
            config_path=row["config_path"],
            environment=row["environment"],
            version_number=row["version_number"],
            content_hash=row["content_hash"],
            content=json.loads(row["content_json"]),
            changes=changes,
            created_at=datetime.fromisoformat(row["created_at"]),
            created_by=row["created_by"],
            message=row["message"] or "",
            metadata=json.loads(row["metadata_json"]) if row["metadata_json"] else {},
        )
