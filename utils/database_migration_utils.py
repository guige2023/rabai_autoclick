"""
Database migration utilities for schema versioning and rollbacks.

Provides migration runner, version tracking, rollback support,
and multi-database adapter implementations.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class MigrationDirection(Enum):
    UP = auto()
    DOWN = auto()


@dataclass
class Migration:
    """Represents a database migration."""
    version: str
    name: str
    up_sql: str
    down_sql: str
    checksum: str = ""
    applied_at: Optional[float] = None
    description: str = ""

    @property
    def is_applied(self) -> bool:
        return self.applied_at is not None


@dataclass
class MigrationResult:
    """Result of running migrations."""
    success: bool
    version: str
    direction: MigrationDirection
    duration_ms: float = 0.0
    error: Optional[str] = None


@dataclass
class SchemaVersion:
    """Current schema version information."""
    version: str
    applied_at: float
    checksum: str
    dirty: bool = False


class MigrationRunner:
    """Executes database migrations."""

    def __init__(
        self,
        migrations: Optional[list[Migration]] = None,
        version_table: str = "schema_migrations",
    ) -> None:
        self.migrations = migrations or []
        self.version_table = version_table
        self._sort_migrations()

    def _sort_migrations(self) -> None:
        """Sort migrations by version."""
        self.migrations.sort(key=lambda m: [int(x) for x in m.version.split(".")])

    def add_migration(self, migration: Migration) -> None:
        """Add a migration to the runner."""
        self.migrations.append(migration)
        self._sort_migrations()

    def get_migrations_to_apply(
        self,
        current_version: Optional[str],
        direction: MigrationDirection,
    ) -> list[Migration]:
        """Get migrations that need to be applied."""
        if direction == MigrationDirection.UP:
            if not current_version:
                return self.migrations
            applied_versions = set(m.version for m in self.migrations if m.is_applied)
            return [m for m in self.migrations if m.version > current_version and m.version not in applied_versions]
        else:
            if not current_version:
                return []
            return [m for m in reversed(self.migrations) if m.version <= current_version]

    def migrate(
        self,
        direction: MigrationDirection,
        target_version: Optional[str] = None,
        executor: Callable[[str], Any] = None,
    ) -> list[MigrationResult]:
        """Run migrations in the specified direction."""
        if executor is None:
            executor = self._default_executor

        current = self._get_current_version(executor)
        migrations = self.get_migrations_to_apply(current, direction)

        if direction == MigrationDirection.DOWN and target_version:
            migrations = [m for m in migrations if m.version > target_version]

        results = []
        for migration in migrations:
            start = time.perf_counter()
            sql = migration.up_sql if direction == MigrationDirection.UP else migration.down_sql
            try:
                executor(sql)
                migration.applied_at = time.time()
                self._record_migration(migration, direction, executor)
                duration_ms = (time.perf_counter() - start) * 1000
                results.append(MigrationResult(
                    success=True,
                    version=migration.version,
                    direction=direction,
                    duration_ms=duration_ms,
                ))
                logger.info("Migration %s (%s) completed", migration.version, direction.name)
            except Exception as e:
                duration_ms = (time.perf_counter() - start) * 1000
                results.append(MigrationResult(
                    success=False,
                    version=migration.version,
                    direction=direction,
                    duration_ms=duration_ms,
                    error=str(e),
                ))
                logger.error("Migration %s failed: %s", migration.version, e)
                break

        return results

    def _default_executor(self, sql: str) -> None:
        """Default SQL executor (override for real database)."""
        pass

    def _get_current_version(self, executor: Callable[[str], Any]) -> Optional[str]:
        """Get the current schema version from database."""
        return None

    def _record_migration(self, migration: Migration, direction: MigrationDirection, executor: Callable[[str], Any]) -> None:
        """Record migration in version table."""
        pass

    def get_pending_migrations(self, current_version: Optional[str]) -> list[Migration]:
        """Get migrations that haven't been applied."""
        return self.get_migrations_to_apply(current_version, MigrationDirection.UP)

    def rollback_last(self, executor: Callable[[str], Any]) -> Optional[MigrationResult]:
        """Rollback the last migration."""
        results = self.migrate(MigrationDirection.DOWN, executor=executor)
        return results[-1] if results else None


class AlembicStyleMigration:
    """Generates Alembic-style migration files."""

    def __init__(self, migrations_dir: str = "./migrations/versions") -> None:
        self.migrations_dir = migrations_dir
        os.makedirs(migrations_dir, exist_ok=True)

    def generate_migration(
        self,
        name: str,
        up_operations: list[str],
        down_operations: list[str],
    ) -> str:
        """Generate a migration file."""
        version = self._generate_version()
        filename = f"{version}_{name}.py"
        filepath = os.path.join(self.migrations_dir, filename)

        content = self._build_migration_file(version, name, up_operations, down_operations)

        with open(filepath, "w") as f:
            f.write(content)

        return filepath

    def _generate_version(self) -> str:
        """Generate a timestamp-based version string."""
        return f"{int(time.time() * 1000):016d}"

    def _build_migration_file(
        self,
        version: str,
        name: str,
        up_operations: list[str],
        down_operations: list[str],
    ) -> str:
        """Build migration file content."""
        up_sql = "\n    ".join(f'"{op}";' for op in up_operations)
        down_sql = "\n    ".join(f'"{op}";' for op in down_operations)

        return f'''
"""Auto-generated migration: {name}

Revision ID: {version}
Revises:
Create Date: {time.strftime("%Y-%m-%d %H:%M:%S")}

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers
revision = '{version}'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    {up_sql}


def downgrade() -> None:
    {down_sql}
'''
