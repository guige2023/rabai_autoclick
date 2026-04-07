"""Database migration utilities: migration tracking, versioning, and rollback."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "Migration",
    "MigrationManager",
    "apply_up",
    "apply_down",
]


@dataclass
class Migration:
    """A database migration."""

    version: str
    name: str
    up: Callable[[], None]
    down: Callable[[], None] | None = None


class MigrationManager:
    """Manages database migrations with versioning."""

    def __init__(
        self,
        get_connection: Callable[[], Any],
        migrations_table: str = "_migrations",
    ) -> None:
        self.get_connection = get_connection
        self.migrations_table = migrations_table
        self._migrations: dict[str, Migration] = {}

    def register(self, migration: Migration) -> None:
        self._migrations[migration.version] = migration

    def get_current_version(self) -> str | None:
        """Get the currently applied migration version."""
        conn = self.get_connection()
        try:
            rows = conn.execute(
                f"SELECT version FROM {self.migrations_table} ORDER BY applied_at DESC LIMIT 1"
            ).fetchall()
            return rows[0][0] if rows else None
        except Exception:
            return None

    def get_applied_versions(self) -> list[str]:
        """Get all applied migration versions."""
        conn = self.get_connection()
        try:
            rows = conn.execute(
                f"SELECT version FROM {self.migrations_table} ORDER BY applied_at ASC"
            ).fetchall()
            return [row[0] for row in rows]
        except Exception:
            return []

    def migrate_up(self, target_version: str | None = None) -> list[str]:
        """Apply pending migrations up to target version."""
        applied = set(self.get_applied_versions())
        pending = [v for v in sorted(self._migrations.keys()) if v not in applied]

        if target_version:
            pending = [v for v in pending if v <= target_version]

        applied_versions: list[str] = []
        conn = self.get_connection()

        for version in pending:
            migration = self._migrations[version]
            print(f"Applying migration {version}: {migration.name}")
            migration.up()
            conn.execute(
                f"INSERT INTO {self.migrations_table} (version, name, applied_at) VALUES (?, ?, ?)",
                (version, migration.name, time.time()),
            )
            conn.commit()
            applied_versions.append(version)

        return applied_versions

    def migrate_down(self, steps: int = 1) -> list[str]:
        """Roll back the last N migrations."""
        applied = self.get_applied_versions()
        rolled_back: list[str] = []

        conn = self.get_connection()

        for version in reversed(applied[-steps:]):
            migration = self._migrations.get(version)
            if migration and migration.down:
                print(f"Rolling back {version}: {migration.name}")
                migration.down()
                conn.execute(
                    f"DELETE FROM {self.migrations_table} WHERE version = ?",
                    (version,),
                )
                conn.commit()
                rolled_back.append(version)

        return rolled_back


def apply_up(
    conn: Any,
    version: str,
    name: str,
    sql: str,
) -> None:
    """Apply a SQL migration up."""
    conn.execute(sql)
    conn.commit()


def apply_down(
    conn: Any,
    version: str,
    sql: str,
) -> None:
    """Apply a SQL migration down (rollback)."""
    conn.execute(sql)
    conn.commit()
