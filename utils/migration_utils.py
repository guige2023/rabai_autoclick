"""Data migration utilities.

Supports schema migration, data transformation, rollback, and
dry-run validation for safe database migrations.

Example:
    migrator = Migrator(conn, migration_dir="./migrations")
    migrator.run_pending()
    migrator.status()
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)


class MigrationState(Enum):
    """State of a migration."""
    PENDING = "pending"
    APPLIED = "applied"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


@dataclass
class Migration:
    """Represents a single migration file."""
    version: str
    name: str
    up_sql: str
    down_sql: str
    checksum: str
    state: MigrationState = MigrationState.PENDING
    applied_at: datetime | None = None


@dataclass
class MigrationResult:
    """Result of a migration run."""
    success: bool
    version: str
    direction: str
    duration_ms: float
    rows_affected: int = 0
    error: str | None = None


class Migrator:
    """Database migrator with versioning, rollback, and dry-run support.

    Manages a collection of SQL migration files with up/down scripts.
    """

    VERSION_PATTERN = re.compile(r"^(\d{4})_(.+)\.(up|down)\.sql$")

    def __init__(
        self,
        connection: Any,
        migration_dir: str | Path = "./migrations",
        table_name: str = "schema_migrations",
    ) -> None:
        """Initialize migrator.

        Args:
            connection: Database connection (psycopg2/mysqldb/sqlite3).
            migration_dir: Directory containing migration files.
            table_name: Name of the migrations tracking table.
        """
        self.connection = connection
        self.migration_dir = Path(migration_dir)
        self.table_name = table_name
        self._ensure_migrations_table()

    def _ensure_migrations_table(self) -> None:
        """Create migrations tracking table if not exists."""
        cursor = self.connection.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                version VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                checksum VARCHAR(64) NOT NULL,
                applied_at TIMESTAMP NOT NULL,
                rolled_back_at TIMESTAMP NULL,
                direction VARCHAR(10) NOT NULL DEFAULT 'up'
            )
        """)
        self.connection.commit()

    def get_migrations(self, state: MigrationState | None = None) -> list[Migration]:
        """Get all migration files, optionally filtered by state.

        Returns:
            List of Migration objects sorted by version.
        """
        files = sorted(self.migration_dir.glob("[0-9][0-9][0-9][0-9]_*.up.sql"))
        migrations: dict[str, Migration] = {}

        for fpath in files:
            match = self.VERSION_PATTERN.match(fpath.name)
            if not match:
                continue

            version = match.group(1)
            name = match.group(2)
            up_path = fpath
            down_path = fpath.parent / f"{version}_{name}.down.sql"

            if not down_path.exists():
                logger.warning("Missing down migration for %s", name)
                continue

            checksum = self._file_checksum(up_path)

            if version not in migrations:
                migrations[version] = Migration(
                    version=version,
                    name=name,
                    up_sql=up_path.read_text(),
                    down_sql=down_path.read_text(),
                    checksum=checksum,
                )

        applied = self._get_applied_migrations()

        for m in migrations.values():
            if m.version in applied:
                m.state = MigrationState.APPLIED
                m.applied_at = applied[m.version]["applied_at"]

        result = sorted(migrations.values(), key=lambda x: x.version)

        if state:
            result = [m for m in result if m.state == state]

        return result

    def _get_applied_migrations(self) -> dict[str, dict[str, Any]]:
        """Get all applied migrations from tracking table."""
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT version, name, checksum, applied_at FROM {self.table_name}")
        return {
            row[0]: {"name": row[1], "checksum": row[2], "applied_at": row[3]}
            for row in cursor.fetchall()
        }

    def _file_checksum(self, path: Path) -> str:
        """Compute SHA-256 checksum of a file."""
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def run_pending(self, dry_run: bool = False) -> list[MigrationResult]:
        """Run all pending migrations.

        Args:
            dry_run: If True, simulate without making changes.

        Returns:
            List of MigrationResult for each migration run.
        """
        pending = self.get_migrations(MigrationState.PENDING)
        results: list[MigrationResult] = []

        for m in pending:
            result = self._run_migration(m, "up", dry_run=dry_run)
            results.append(result)

        return results

    def rollback_last(self, steps: int = 1, dry_run: bool = False) -> list[MigrationResult]:
        """Rollback the last N migrations.

        Args:
            steps: Number of migrations to rollback.
            dry_run: If True, simulate without making changes.

        Returns:
            List of MigrationResult for each rollback.
        """
        applied = self.get_migrations(MigrationState.APPLIED)
        applied = sorted(applied, key=lambda x: x.version, reverse=True)[:steps]

        results: list[MigrationResult] = []
        for m in applied:
            result = self._run_migration(m, "down", dry_run=dry_run)
            results.append(result)

        return results

    def _run_migration(
        self,
        migration: Migration,
        direction: str,
        dry_run: bool = False,
    ) -> MigrationResult:
        """Execute a single migration in given direction.

        Args:
            migration: Migration to execute.
            direction: "up" or "down".
            dry_run: If True, validate without executing.

        Returns:
            MigrationResult with outcome details.
        """
        import time
        start = time.perf_counter()
        sql = migration.up_sql if direction == "up" else migration.down_sql

        try:
            if dry_run:
                cursor = self.connection.cursor()
                cursor.execute(sql)
                self.connection.rollback()
                return MigrationResult(
                    success=True,
                    version=migration.version,
                    direction=direction,
                    duration_ms=(time.perf_counter() - start) * 1000,
                    rows_affected=0,
                )

            cursor = self.connection.cursor()
            cursor.execute(sql)
            rows = cursor.rowcount

            if direction == "up":
                cursor.execute(
                    f"INSERT INTO {self.table_name} "
                    f"(version, name, checksum, applied_at, direction) "
                    f"VALUES (%s, %s, %s, %s, %s)",
                    (
                        migration.version,
                        migration.name,
                        migration.checksum,
                        datetime.utcnow(),
                        direction,
                    ),
                )
            else:
                cursor.execute(
                    f"UPDATE {self.table_name} SET rolled_back_at = %s WHERE version = %s",
                    (datetime.utcnow(), migration.version),
                )

            self.connection.commit()

            duration_ms = (time.perf_counter() - start) * 1000
            logger.info(
                "Migration %s (%s) completed in %.2fms",
                migration.version,
                direction,
                duration_ms,
            )

            return MigrationResult(
                success=True,
                version=migration.version,
                direction=direction,
                duration_ms=duration_ms,
                rows_affected=rows,
            )

        except Exception as e:
            self.connection.rollback()
            duration_ms = (time.perf_counter() - start) * 1000
            logger.error("Migration %s (%s) failed: %s", migration.version, direction, e)

            return MigrationResult(
                success=False,
                version=migration.version,
                direction=direction,
                duration_ms=duration_ms,
                error=str(e),
            )

    def status(self) -> dict[str, Any]:
        """Get migration status summary.

        Returns:
            Dict with counts and list of pending/applied/failed migrations.
        """
        all_migrations = self.get_migrations()
        return {
            "total": len(all_migrations),
            "pending": [m for m in all_migrations if m.state == MigrationState.PENDING],
            "applied": [m for m in all_migrations if m.state == MigrationState.APPLIED],
            "failed": [m for m in all_migrations if m.state == MigrationState.FAILED],
            "rolled_back": [m for m in all_migrations if m.state == MigrationState.ROLLED_BACK],
        }

    def validate(self) -> list[str]:
        """Validate migration files (checksums, missing down migrations).

        Returns:
            List of validation error messages (empty if all valid).
        """
        errors: list[str] = []
        migrations = self.get_migrations()

        for m in migrations:
            current_checksum = hashlib.sha256(
                self.migration_dir / f"{m.version}_{m.name}.up.sql"
            ).hexdigest()

            if current_checksum != m.checksum:
                errors.append(
                    f"{m.version}_{m.name}: checksum mismatch (file modified after apply)"
                )

        return errors


def generate_migration(
    name: str,
    migration_dir: str | Path = "./migrations",
) -> tuple[Path, Path]:
    """Generate a new migration file pair.

    Args:
        name: Migration name (snake_case).
        migration_dir: Directory to create migration files in.

    Returns:
        Tuple of (up_path, down_path) for the generated files.
    """
    migration_dir = Path(migration_dir)
    migration_dir.mkdir(parents=True, exist_ok=True)

    timestamp = int(time.time())
    filename_base = f"{timestamp}_{name}"

    up_path = migration_dir / f"{filename_base}.up.sql"
    down_path = migration_dir / f"{filename_base}.down.sql"

    up_path.write_text(f"-- Migration: {name} (up)\n-- Created: {datetime.utcnow()}\n\n")
    down_path.write_text(f"-- Migration: {name} (down)\n-- Created: {datetime.utcnow()}\n\n")

    return up_path, down_path
