"""
Service Catalog Management Utilities.

Provides utilities for managing a service catalog with service definitions,
dependencies, SLAs, and ownership information.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional


class ServiceStatus(Enum):
    """Status of a service."""
    PLANNED = "planned"
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    DEPRECATED = "deprecated"
    RETIRED = "retired"


class Environment(Enum):
    """Deployment environment types."""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


@dataclass
class Owner:
    """Service owner information."""
    name: str
    email: str
    team: str
    role: str = "owner"


@dataclass
class SLADefinition:
    """SLA definition for a service."""
    availability_percent: float
    latency_p99_ms: int
    error_rate_percent: float
    recovery_time_minutes: int
    backup_frequency_hours: int = 24


@dataclass
class Dependency:
    """Service dependency information."""
    service_name: str
    service_version: str
    dependency_type: str
    optional: bool = False
    fallback_available: bool = False


@dataclass
class ServiceEndpoint:
    """Service endpoint definition."""
    name: str
    url: str
    method: str
    path: str
    description: str = ""
    auth_required: bool = True


@dataclass
class ServiceDefinition:
    """Complete service definition."""
    name: str
    version: str
    description: str
    status: ServiceStatus
    environment: Environment
    owners: list[Owner]
    tags: list[str] = field(default_factory=list)
    sla: Optional[SLADefinition] = None
    dependencies: list[Dependency] = field(default_factory=list)
    endpoints: list[ServiceEndpoint] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


class ServiceCatalog:
    """Service catalog for managing service metadata."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self.db_path = db_path or Path("service_catalog.db")
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the service catalog database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS services (
                name TEXT NOT NULL,
                version TEXT NOT NULL,
                description TEXT,
                status TEXT NOT NULL,
                environment TEXT NOT NULL,
                owners_json TEXT NOT NULL,
                tags_json TEXT NOT NULL DEFAULT '[]',
                sla_json TEXT,
                dependencies_json TEXT NOT NULL DEFAULT '[]',
                endpoints_json TEXT NOT NULL DEFAULT '[]',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (name, version)
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_service_name
            ON services(name)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_service_status
            ON services(status)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_service_tags
            ON services(tags_json)
        """)
        conn.commit()
        conn.close()

    def register_service(
        self,
        service: ServiceDefinition,
    ) -> ServiceDefinition:
        """Register a new service in the catalog."""
        self._save_service(service)
        return service

    def update_service(
        self,
        name: str,
        version: str,
        **updates: Any,
    ) -> Optional[ServiceDefinition]:
        """Update an existing service."""
        service = self.get_service(name, version)
        if not service:
            return None

        if "description" in updates:
            service.description = updates["description"]
        if "status" in updates:
            service.status = ServiceStatus(updates["status"]) if isinstance(updates["status"], str) else updates["status"]
        if "owners" in updates:
            service.owners = updates["owners"]
        if "tags" in updates:
            service.tags = updates["tags"]
        if "sla" in updates:
            service.sla = updates["sla"]
        if "dependencies" in updates:
            service.dependencies = updates["dependencies"]
        if "endpoints" in updates:
            service.endpoints = updates["endpoints"]
        if "metadata" in updates:
            service.metadata = updates["metadata"]

        service.updated_at = datetime.now()
        self._save_service(service)
        return service

    def get_service(
        self,
        name: str,
        version: str,
    ) -> Optional[ServiceDefinition]:
        """Get a service by name and version."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM services WHERE name = ? AND version = ?",
            (name, version),
        )
        row = cursor.fetchone()
        conn.close()

        if row:
            return self._row_to_service(row)
        return None

    def get_latest_version(
        self,
        name: str,
        environment: Optional[Environment] = None,
    ) -> Optional[ServiceDefinition]:
        """Get the latest version of a service."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        if environment:
            cursor.execute("""
                SELECT * FROM services
                WHERE name = ? AND environment = ?
                ORDER BY created_at DESC LIMIT 1
            """, (name, environment.value))
        else:
            cursor.execute("""
                SELECT * FROM services
                WHERE name = ?
                ORDER BY created_at DESC LIMIT 1
            """, (name,))

        row = cursor.fetchone()
        conn.close()

        if row:
            return self._row_to_service(row)
        return None

    def list_services(
        self,
        status: Optional[ServiceStatus] = None,
        environment: Optional[Environment] = None,
        tag: Optional[str] = None,
        owner_team: Optional[str] = None,
        limit: int = 100,
    ) -> list[ServiceDefinition]:
        """List services with optional filters."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        query = "SELECT * FROM services WHERE 1=1"
        params: list[Any] = []

        if status:
            query += " AND status = ?"
            params.append(status.value)
        if environment:
            query += " AND environment = ?"
            params.append(environment.value)
        if tag:
            query += " AND tags_json LIKE ?"
            params.append(f"%{tag}%")
        if owner_team:
            query += " AND owners_json LIKE ?"
            params.append(f"%{owner_team}%")

        query += " ORDER BY updated_at DESC LIMIT ?"
        params.append(limit)

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        services = [self._row_to_service(row) for row in rows]

        if tag:
            services = [s for s in services if tag in s.tags]

        if owner_team:
            services = [s for s in services if any(o.team == owner_team for o in s.owners)]

        return services

    def find_services_by_dependency(
        self,
        dependency_name: str,
    ) -> list[ServiceDefinition]:
        """Find services that depend on a specific service."""
        all_services = self.list_services(limit=1000)
        return [
            s for s in all_services
            if any(d.service_name == dependency_name for d in s.dependencies)
        ]

    def get_dependency_graph(
        self,
        service_name: str,
        version: str,
        max_depth: int = 3,
    ) -> dict[str, Any]:
        """Get the dependency graph for a service."""
        service = self.get_service(service_name, version)
        if not service:
            return {}

        graph: dict[str, Any] = {
            "service": service.name,
            "version": service.version,
            "dependencies": [],
        }

        visited = set()
        self._build_dependency_tree(
            service.dependencies,
            graph["dependencies"],
            visited,
            max_depth,
            1,
        )

        return graph

    def _build_dependency_tree(
        self,
        dependencies: list[Dependency],
        output: list[dict[str, Any]],
        visited: set[str],
        max_depth: int,
        current_depth: int,
    ) -> None:
        """Recursively build the dependency tree."""
        if current_depth > max_depth:
            return

        for dep in dependencies:
            node_id = f"{dep.service_name}:{dep.service_version}"
            if node_id in visited:
                continue
            visited.add(node_id)

            node: dict[str, Any] = {
                "name": dep.service_name,
                "version": dep.service_version,
                "type": dep.dependency_type,
                "optional": dep.optional,
                "dependencies": [],
            }

            dep_service = self.get_latest_version(dep.service_name)
            if dep_service:
                self._build_dependency_tree(
                    dep_service.dependencies,
                    node["dependencies"],
                    visited,
                    max_depth,
                    current_depth + 1,
                )

            output.append(node)

    def deprecate_service(
        self,
        name: str,
        version: str,
        replacement: Optional[str] = None,
    ) -> bool:
        """Mark a service as deprecated."""
        service = self.get_service(name, version)
        if not service:
            return False

        service.status = ServiceStatus.DEPRECATED
        service.updated_at = datetime.now()
        if replacement:
            service.metadata["replacement"] = replacement

        self._save_service(service)
        return True

    def retire_service(
        self,
        name: str,
        version: str,
    ) -> bool:
        """Retire a service completely."""
        service = self.get_service(name, version)
        if not service:
            return False

        service.status = ServiceStatus.RETIRED
        service.updated_at = datetime.now()
        self._save_service(service)
        return True

    def get_services_by_owner(
        self,
        owner_email: str,
    ) -> list[ServiceDefinition]:
        """Get all services owned by a specific owner."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM services WHERE owners_json LIKE ?",
            (f"%{owner_email}%",),
        )
        rows = cursor.fetchall()
        conn.close()

        return [self._row_to_service(row) for row in rows]

    def search_services(
        self,
        query: str,
        limit: int = 20,
    ) -> list[ServiceDefinition]:
        """Search services by name or description."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM services
            WHERE name LIKE ? OR description LIKE ?
            ORDER BY name LIMIT ?
        """, (f"%{query}%", f"%{query}%", limit))
        rows = cursor.fetchall()
        conn.close()

        return [self._row_to_service(row) for row in rows]

    def _save_service(self, service: ServiceDefinition) -> None:
        """Save a service to the database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO services (
                name, version, description, status, environment,
                owners_json, tags_json, sla_json, dependencies_json,
                endpoints_json, metadata_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            service.name,
            service.version,
            service.description,
            service.status.value,
            service.environment.value,
            json.dumps([o.__dict__ for o in service.owners]),
            json.dumps(service.tags),
            json.dumps(service.sla.__dict__ if service.sla else None),
            json.dumps([d.__dict__ for d in service.dependencies]),
            json.dumps([e.__dict__ for e in service.endpoints]),
            json.dumps(service.metadata),
            service.created_at.isoformat(),
            service.updated_at.isoformat(),
        ))
        conn.commit()
        conn.close()

    def _row_to_service(self, row: sqlite3.Row) -> ServiceDefinition:
        """Convert a database row to a ServiceDefinition."""
        owners = [
            Owner(
                name=o["name"],
                email=o["email"],
                team=o["team"],
                role=o.get("role", "owner"),
            )
            for o in json.loads(row["owners_json"])
        ]

        tags = json.loads(row["tags_json"])

        sla = None
        if row["sla_json"]:
            sla_data = json.loads(row["sla_json"])
            if sla_data:
                sla = SLADefinition(**sla_data)

        dependencies = [
            Dependency(**d)
            for d in json.loads(row["dependencies_json"])
        ]

        endpoints = [
            ServiceEndpoint(**e)
            for e in json.loads(row["endpoints_json"])
        ]

        return ServiceDefinition(
            name=row["name"],
            version=row["version"],
            description=row["description"] or "",
            status=ServiceStatus(row["status"]),
            environment=Environment(row["environment"]),
            owners=owners,
            tags=tags,
            sla=sla,
            dependencies=dependencies,
            endpoints=endpoints,
            metadata=json.loads(row["metadata_json"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )
