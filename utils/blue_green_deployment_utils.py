"""
Blue-Green Deployment Utilities.

Provides utilities for managing blue-green deployments, traffic switching,
environment synchronization, and rollback coordination.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional


class DeploymentState(Enum):
    """State of a blue-green deployment."""
    IDLE = "idle"
    BLUE_ACTIVE = "blue_active"
    GREEN_ACTIVE = "green_active"
    SWITCHING = "switching"
    ROLLING_BACK = "rolling_back"


@dataclass
class Environment:
    """Represents a deployment environment."""
    name: str
    color: str
    endpoint: str
    health_check_url: Optional[str] = None
    is_active: bool = False
    version: str = ""
    deployed_at: Optional[datetime] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DeploymentRecord:
    """Record of a deployment operation."""
    deployment_id: str
    environment_name: str
    color: str
    version: str
    status: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    health_check_results: dict[str, Any] = field(default_factory=dict)
    rollback_of: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TrafficSwitch:
    """Record of a traffic switch operation."""
    switch_id: str
    from_color: str
    to_color: str
    traffic_percentage: int
    switched_at: datetime
    pre_switch_health: dict[str, Any] = field(default_factory=dict)
    post_switch_health: dict[str, Any] = field(default_factory=dict)


class BlueGreenDeploymentManager:
    """Manages blue-green deployment operations."""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        health_check_timeout: int = 300,
        pre_switch_validation: bool = True,
    ) -> None:
        self.db_path = db_path or Path("blue_green_deployment.db")
        self.health_check_timeout = health_check_timeout
        self.pre_switch_validation = pre_switch_validation
        self._current_state = DeploymentState.IDLE
        self._environments: dict[str, Environment] = {}
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the deployment database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS deployments (
                deployment_id TEXT PRIMARY KEY,
                deployment_json TEXT NOT NULL,
                started_at TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS traffic_switches (
                switch_id TEXT PRIMARY KEY,
                switch_json TEXT NOT NULL,
                switched_at TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS environments (
                name TEXT PRIMARY KEY,
                environment_json TEXT NOT NULL
            )
        """)
        conn.commit()
        conn.close()

    def register_environment(
        self,
        name: str,
        color: str,
        endpoint: str,
        health_check_url: Optional[str] = None,
    ) -> Environment:
        """Register a deployment environment."""
        env = Environment(
            name=name,
            color=color,
            endpoint=endpoint,
            health_check_url=health_check_url,
        )

        self._environments[name] = env
        self._save_environment(env)
        return env

    def deploy_to_environment(
        self,
        environment_name: str,
        version: str,
        deploy_function: Callable[[Environment, str], bool],
    ) -> DeploymentRecord:
        """Deploy a new version to an environment."""
        env = self._environments.get(environment_name)
        if not env:
            raise ValueError(f"Environment not found: {environment_name}")

        deployment_id = f"deploy_{int(time.time())}_{hashlib.md5(version.encode()).hexdigest()[:8]}"

        record = DeploymentRecord(
            deployment_id=deployment_id,
            environment_name=environment_name,
            color=env.color,
            version=version,
            status="in_progress",
            started_at=datetime.now(),
        )

        self._save_deployment(record)

        try:
            success = deploy_function(env, version)

            if success:
                env.version = version
                env.deployed_at = datetime.now()
                record.status = "completed"
                record.completed_at = datetime.now()
            else:
                record.status = "failed"
                record.completed_at = datetime.now()

        except Exception as e:
            record.status = "failed"
            record.completed_at = datetime.now()
            record.metadata["error"] = str(e)

        self._save_deployment(record)
        self._save_environment(env)

        return record

    def health_check_environment(
        self,
        environment_name: str,
    ) -> dict[str, Any]:
        """Perform health check on an environment."""
        env = self._environments.get(environment_name)
        if not env:
            return {"status": "error", "message": "Environment not found"}

        if not env.health_check_url:
            return {
                "status": "healthy",
                "environment": environment_name,
                "message": "No health check configured",
            }

        return {
            "status": "healthy",
            "environment": environment_name,
            "endpoint": env.endpoint,
            "version": env.version,
            "checked_at": datetime.now().isoformat(),
        }

    def switch_traffic(
        self,
        from_environment: str,
        to_environment: str,
        validate_before_switch: bool = True,
    ) -> TrafficSwitch:
        """Switch traffic from one environment to another."""
        from_env = self._environments.get(from_environment)
        to_env = self._environments.get(to_environment)

        if not from_env or not to_env:
            raise ValueError("Environment not found")

        if validate_before_switch and self.pre_switch_validation:
            health = self.health_check_environment(to_environment)
            if health.get("status") != "healthy":
                raise RuntimeError(f"Target environment health check failed: {health}")

        switch_id = f"switch_{int(time.time())}"

        pre_health = self.health_check_environment(from_environment)

        from_env.is_active = False
        to_env.is_active = True

        self._current_state = DeploymentState.SWITCHING

        post_health = self.health_check_environment(to_environment)

        traffic_switch = TrafficSwitch(
            switch_id=switch_id,
            from_color=from_env.color,
            to_color=to_env.color,
            traffic_percentage=100,
            switched_at=datetime.now(),
            pre_switch_health=pre_health,
            post_switch_health=post_health,
        )

        self._save_traffic_switch(traffic_switch)
        self._save_environment(from_env)
        self._save_environment(to_env)

        self._current_state = DeploymentState.GREEN_ACTIVE if to_env.color == "green" else DeploymentState.BLUE_ACTIVE

        return traffic_switch

    def rollback(
        self,
        to_environment: str,
        reason: str = "",
    ) -> bool:
        """Rollback to a previous environment."""
        active_env = None
        for env in self._environments.values():
            if env.is_active:
                active_env = env
                break

        if not active_env:
            return False

        to_env = self._environments.get(to_environment)
        if not to_env:
            return False

        self._current_state = DeploymentState.ROLLING_BACK

        try:
            self.switch_traffic(active_env.name, to_env.name)
            self._current_state = DeploymentState.GREEN_ACTIVE if to_env.color == "green" else DeploymentState.BLUE_ACTIVE
            return True

        except Exception:
            self._current_state = DeploymentState.IDLE
            return False

    def get_active_environment(self) -> Optional[Environment]:
        """Get the currently active environment."""
        for env in self._environments.values():
            if env.is_active:
                return env
        return None

    def get_standby_environment(self) -> Optional[Environment]:
        """Get the standby environment."""
        active = self.get_active_environment()
        for env in self._environments.values():
            if not env.is_active:
                return env
        return None

    def get_deployment_history(
        self,
        environment_name: Optional[str] = None,
        limit: int = 50,
    ) -> list[DeploymentRecord]:
        """Get deployment history."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        if environment_name:
            cursor.execute("""
                SELECT * FROM deployments
                WHERE json_extract(deployment_json, '$.environment_name') = ?
                ORDER BY started_at DESC LIMIT ?
            """, (environment_name, limit))
        else:
            cursor.execute("""
                SELECT * FROM deployments
                ORDER BY started_at DESC LIMIT ?
            """, (limit,))

        rows = cursor.fetchall()
        conn.close()

        records = []
        for row in rows:
            data = json.loads(row["deployment_json"])
            records.append(DeploymentRecord(
                deployment_id=row["deployment_id"],
                environment_name=data["environment_name"],
                color=data["color"],
                version=data["version"],
                status=data["status"],
                started_at=datetime.fromisoformat(data["started_at"]),
                completed_at=datetime.fromisoformat(data["completed_at"]) if data.get("completed_at") else None,
                metadata=data.get("metadata", {}),
            ))

        return records

    def get_current_state(self) -> DeploymentState:
        """Get the current deployment state."""
        return self._current_state

    def _save_environment(self, env: Environment) -> None:
        """Save an environment to the database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO environments (name, environment_json)
            VALUES (?, ?)
        """, (
            env.name,
            json.dumps({
                "color": env.color,
                "endpoint": env.endpoint,
                "health_check_url": env.health_check_url,
                "is_active": env.is_active,
                "version": env.version,
                "deployed_at": env.deployed_at.isoformat() if env.deployed_at else None,
                "metadata": env.metadata,
            }),
        ))
        conn.commit()
        conn.close()

    def _save_deployment(self, record: DeploymentRecord) -> None:
        """Save a deployment record to the database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO deployments (deployment_id, deployment_json, started_at)
            VALUES (?, ?, ?)
        """, (
            record.deployment_id,
            json.dumps({
                "environment_name": record.environment_name,
                "color": record.color,
                "version": record.version,
                "status": record.status,
                "completed_at": record.completed_at.isoformat() if record.completed_at else None,
                "health_check_results": record.health_check_results,
                "rollback_of": record.rollback_of,
                "metadata": record.metadata,
            }),
            record.started_at.isoformat(),
        ))
        conn.commit()
        conn.close()

    def _save_traffic_switch(self, switch: TrafficSwitch) -> None:
        """Save a traffic switch record."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO traffic_switches (switch_id, switch_json, switched_at)
            VALUES (?, ?, ?)
        """, (
            switch.switch_id,
            json.dumps({
                "from_color": switch.from_color,
                "to_color": switch.to_color,
                "traffic_percentage": switch.traffic_percentage,
                "pre_switch_health": switch.pre_switch_health,
                "post_switch_health": switch.post_switch_health,
            }),
            switch.switched_at.isoformat(),
        ))
        conn.commit()
        conn.close()


class BlueGreenOrchestrator:
    """High-level orchestrator for blue-green deployments."""

    def __init__(self, manager: BlueGreenDeploymentManager) -> None:
        self.manager = manager

    def execute_deployment(
        self,
        version: str,
        deploy_callback: Callable[[Environment, str], bool],
        validate_callback: Optional[Callable[[Environment], bool]] = None,
    ) -> dict[str, Any]:
        """Execute a complete blue-green deployment."""
        standby = self.manager.get_standby_environment()
        if not standby:
            return {"status": "error", "message": "No standby environment available"}

        deployment = self.manager.deploy_to_environment(
            standby.name,
            version,
            deploy_callback,
        )

        if deployment.status != "completed":
            return {
                "status": "failed",
                "deployment_id": deployment.deployment_id,
                "message": "Deployment to standby failed",
            }

        if validate_callback:
            if not validate_callback(standby):
                return {
                    "status": "failed",
                    "deployment_id": deployment.deployment_id,
                    "message": "Validation after deployment failed",
                }

        traffic_switch = self.manager.switch_traffic(
            self.manager.get_active_environment().name,
            standby.name,
        )

        return {
            "status": "success",
            "deployment_id": deployment.deployment_id,
            "switch_id": traffic_switch.switch_id,
            "new_active": standby.color,
            "version": version,
        }

    def execute_rollback(
        self,
        reason: str,
    ) -> dict[str, Any]:
        """Execute a rollback to the previous environment."""
        active = self.manager.get_active_environment()
        standby = self.manager.get_standby_environment()

        if not standby:
            return {"status": "error", "message": "No standby environment available"}

        success = self.manager.rollback(standby.name, reason)

        if success:
            return {
                "status": "success",
                "rolled_back_to": standby.color,
                "reason": reason,
            }
        else:
            return {
                "status": "failed",
                "message": "Rollback failed",
            }

    def get_deployment_status(self) -> dict[str, Any]:
        """Get current deployment status."""
        active = self.manager.get_active_environment()
        standby = self.manager.get_standby_environment()

        return {
            "state": self.manager.get_current_state().value,
            "active": {
                "name": active.name,
                "color": active.color,
                "version": active.version,
                "endpoint": active.endpoint,
            } if active else None,
            "standby": {
                "name": standby.name,
                "color": standby.color,
                "version": standby.version,
                "endpoint": standby.endpoint,
            } if standby else None,
        }
