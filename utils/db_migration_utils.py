"""Database migration utilities for schema versioning and rollback."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

__all__ = ["Migration", "MigrationManager", "run_migrations"]


@dataclass
class Migration:
    """A single database migration."""
    version: int
    name: str
    up_sql: str
    down_sql: str = ""
    checksum: str = ""
    applied_at: float | None = None

    def compute_checksum(self) -> str:
        return hashlib.sha256(self.up_sql.encode()).hexdigest()


@dataclass
class AppliedMigration:
    """Record of an applied migration."""
    version: int
    name: str
    checksum: str
    applied_at: float


class MigrationManager:
    """Manages database schema migrations with rollback support."""

    def __init__(self, db_path: str | Path, table_name: str = "schema_migrations") -> None:
        self.db_path = Path(db_path)
        self.table_name = table_name
        self._conn: sqlite3.Connection | None = None
        self._ensure_schema()

    def _conn_getter(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path))
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def _ensure_schema(self) -> None:
        conn = self._conn_getter()
        conn.execute(
            f"""CREATE TABLE IF NOT EXISTS {self.table_name} (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                checksum TEXT NOT NULL,
                applied_at REAL NOT NULL
            )"""
        )
        conn.commit()

    def _get_applied(self) -> list[AppliedMigration]:
        conn = self._conn_getter()
        rows = conn.execute(
            f"SELECT version, name, checksum, applied_at FROM {self.table_name} ORDER BY version"
        ).fetchall()
        return [AppliedMigration(**dict(r)) for r in rows]

    def _get_current_version(self) -> int:
        conn = self._conn_getter()
        row = conn.execute(
            f"SELECT MAX(version) as v FROM {self.table_name}"
        ).fetchone()
        return row["v"] or 0 if row else 0

    def register(self, migration: Migration) -> None:
        """Register a migration. Must be called before apply."""
        if not migration.checksum:
            migration.checksum = migration.compute_checksum()

    def apply(self, migration: Migration, dry_run: bool = False) -> bool:
        """Apply a single migration."""
        applied = self._get_applied()
        if any(m.version == migration.version for m in applied):
            return False

        conn = self._conn_getter()
        if dry_run:
            print(f"[DRY RUN] Would apply migration {migration.version}: {migration.name}")
            print(f"  SQL: {migration.up_sql[:100]}...")
            return True

        try:
            conn.executescript(migration.up_sql)
            conn.execute(
                f"INSERT INTO {self.table_name} (version, name, checksum, applied_at) VALUES (?, ?, ?, ?)",
                (migration.version, migration.name, migration.checksum, time.time()),
            )
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            raise RuntimeError(f"Migration {migration.version} failed: {e}") from e

    def rollback(self, steps: int = 1) -> list[int]:
        """Rollback the last `steps` migrations."""
        applied = self._get_applied()
        to_rollback = list(reversed(applied))[:steps]
        rolled_back: list[int] = []

        conn = self._conn_getter()
        for m in to_rollback:
            if not m.name:
                continue
            migration = Migration(version=m.version, name=m.name, up_sql="", down_sql="")
            if migration.down_sql:
                try:
                    conn.executescript(migration.down_sql)
                    conn.execute(
                        f"DELETE FROM {self.table_name} WHERE version = ?",
                        (m.version,),
                    )
                    conn.commit()
                    rolled_back.append(m.version)
                except Exception as e:
                    conn.rollback()
                    raise RuntimeError(f"Rollback of {m.version} failed: {e}") from e
        return rolled_back

    def status(self) -> dict[str, Any]:
        """Return migration status."""
        return {
            "current_version": self._get_current_version(),
            "applied": self._get_applied(),
        }

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None


def run_migrations(
    db_path: str | Path,
    migrations: list[Migration],
    dry_run: bool = False,
) -> list[int]:
    """Run all pending migrations. Returns list of applied version numbers."""
    manager = MigrationManager(db_path)
    applied: list[int] = []
    for m in sorted(migrations, key=lambda x: x.version):
        manager.register(m)
        if manager.apply(m, dry_run=dry_run):
            applied.append(m.version)
    manager.close()
    return applied
