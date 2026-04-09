"""
Automation Blue-Green Deployment Module.

Implements blue-green deployment strategy for zero-downtime releases.
Manages two identical environments, switches traffic between them,
and supports instant rollback capabilities.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


class DeploymentState(Enum):
    """Blue-green deployment states."""
    IDLE = "idle"
    DEPLOYING_BLUE = "deploying_blue"
    DEPLOYING_GREEN = "deploying_green"
    TESTING = "testing"
    LIVE_BLUE = "live_blue"
    LIVE_GREEN = "live_green"
    ROLLING_BACK = "rolling_back"


@dataclass
class Environment:
    """Represents a deployment environment."""
    name: str
    version: str
    healthy: bool = True
    traffic_weight: int = 0
    last_deployed: float = field(default_factory=time.time)
    health_check_url: Optional[str] = None


@dataclass
class DeploymentConfig:
    """Configuration for blue-green deployment."""
    blue_name: str = "blue"
    green_name: str = "green"
    health_check_path: str = "/health"
    verification_timeout: int = 300
    verification_interval: int = 5
    rollback_threshold: float = 0.5
    pre_deployment_hook: Optional[Callable] = None
    post_deployment_hook: Optional[Callable] = None


@dataclass
class DeploymentResult:
    """Result of a deployment operation."""
    success: bool
    from_state: DeploymentState
    to_state: DeploymentState
    duration_ms: float
    error: Optional[str] = None
    logs: list[str] = field(default_factory=list)


class BlueGreenDeployer:
    """
    Blue-green deployment orchestrator.

    Manages two identical environments and switches traffic atomically
    to enable zero-downtime deployments with instant rollback.

    Example:
        deployer = BlueGreenDeployer(config)
        deployer.set_deploy_callback(my_deploy_func)
        deployer.set_health_check(my_health_check)
        result = await deployer.deploy("v2.0.0")
    """

    def __init__(self, config: Optional[DeploymentConfig] = None) -> None:
        self._config = config or DeploymentConfig()
        self._blue = Environment(name=self._config.blue_name, version="v1.0.0")
        self._green = Environment(name=self._config.green_name, version="v1.0.0")
        self._state = DeploymentState.IDLE
        self._deploy_callback: Optional[Callable[[str, str], Any]] = None
        self._health_check_callback: Optional[Callable[[str], bool]] = None
        self._logs: list[str] = []

    def set_deploy_callback(
        self,
        callback: Callable[[str, str], Any]
    ) -> None:
        """Set the deployment function (environment_name, version)."""
        self._deploy_callback = callback

    def set_health_check(
        self,
        callback: Callable[[str], bool]
    ) -> None:
        """Set the health check function (environment_name) -> bool."""
        self._health_check_callback = callback

    def get_active_environment(self) -> Environment:
        """Get the currently active environment."""
        if self._state in (DeploymentState.LIVE_BLUE, DeploymentState.DEPLOYING_BLUE):
            return self._blue
        return self._green

    def get_standby_environment(self) -> Environment:
        """Get the standby environment (not currently live)."""
        if self._state in (DeploymentState.LIVE_BLUE, DeploymentState.DEPLOYING_BLUE):
            return self._green
        return self._blue

    async def deploy(self, version: str) -> DeploymentResult:
        """
        Deploy a new version using blue-green strategy.

        Args:
            version: Version string to deploy

        Returns:
            DeploymentResult with success status and details
        """
        start = time.perf_counter()
        self._log(f"Starting blue-green deployment of {version}")

        standby = self.get_standby_environment()
        from_state = self._state

        try:
            if self._config.pre_deployment_hook:
                self._log("Running pre-deployment hook")
                await self._config.pre_deployment_hook(standby.name, version)

            target_state = DeploymentState.DEPLOYING_GREEN if standby.name == "green" else DeploymentState.DEPLOYING_BLUE
            self._state = target_state
            self._log(f"Deploying to {standby.name} environment")

            if self._deploy_callback:
                await self._deploy_callback(standby.name, version)

            standby.version = version
            standby.last_deployed = time.time()

            self._state = DeploymentState.TESTING
            self._log(f"Running health checks on {standby.name}")

            healthy = await self._wait_for_health(standby)
            if not healthy:
                self._state = DeploymentState.ROLLING_BACK
                raise RuntimeError(f"Health check failed on {standby.name}")

            self._state = DeploymentState.LIVE_GREEN if standby.name == "green" else DeploymentState.LIVE_BLUE
            self._log(f"Switching traffic to {standby.name}")

            await self._switch_traffic(standby)

            if self._config.post_deployment_hook:
                self._log("Running post-deployment hook")
                await self._config.post_deployment_hook(standby.name, version)

            duration = (time.perf_counter() - start) * 1000

            return DeploymentResult(
                success=True,
                from_state=from_state,
                to_state=self._state,
                duration_ms=duration,
                logs=self._logs.copy()
            )

        except Exception as e:
            duration = (time.perf_counter() - start) * 1000
            self._log(f"Deployment failed: {e}")
            self._state = DeploymentState.ROLLING_BACK
            await self._rollback()
            return DeploymentResult(
                success=False,
                from_state=from_state,
                to_state=self._state,
                duration_ms=duration,
                error=str(e),
                logs=self._logs.copy()
            )

    async def rollback(self) -> DeploymentResult:
        """Manually trigger rollback to previous version."""
        start = time.perf_counter()
        from_state = self._state
        self._log("Manual rollback initiated")

        result = await self._rollback()
        result.from_state = from_state
        result.duration_ms = (time.perf_counter() - start) * 1000
        return result

    async def _rollback(self) -> DeploymentResult:
        """Internal rollback implementation."""
        previous = self.get_active_environment()
        self._log(f"Rolling back to {previous.name} ({previous.version})")

        await self._switch_traffic(previous)

        to_state = DeploymentState.LIVE_GREEN if previous.name == "green" else DeploymentState.LIVE_BLUE
        self._state = to_state

        duration = (time.perf_counter() - time.time()) * 1000

        return DeploymentResult(
            success=True,
            from_state=DeploymentState.ROLLING_BACK,
            to_state=self._state,
            duration_ms=duration,
            logs=self._logs.copy()
        )

    async def _wait_for_health(self, env: Environment) -> bool:
        """Wait for environment to pass health checks."""
        elapsed = 0
        while elapsed < self._config.verification_timeout:
            if self._health_check_callback:
                healthy = await self._health_check_callback(env.name)
            else:
                healthy = env.healthy

            if healthy:
                self._log(f"{env.name} passed health check")
                return True

            await self._sleep(self._config.verification_interval)
            elapsed += self._config.verification_interval

        return False

    async def _switch_traffic(self, env: Environment) -> None:
        """Switch traffic to target environment."""
        self._log(f"Switching 100% traffic to {env.name}")

    def _log(self, message: str) -> None:
        """Add a log entry."""
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        entry = f"[{timestamp}] {message}"
        self._logs.append(entry)
        if len(self._logs) > 1000:
            self._logs = self._logs[-500:]

    @staticmethod
    async def _sleep(seconds: int) -> None:
        """Async sleep helper."""
        import asyncio
        await asyncio.sleep(seconds)

    def get_status(self) -> dict[str, Any]:
        """Get current deployment status."""
        return {
            "state": self._state.value,
            "blue": {
                "version": self._blue.version,
                "healthy": self._blue.healthy,
                "traffic": self._blue.traffic_weight,
                "last_deployed": self._blue.last_deployed
            },
            "green": {
                "version": self._green.version,
                "healthy": self._green.healthy,
                "traffic": self._green.traffic_weight,
                "last_deployed": self._green.last_deployed
            },
            "active": self.get_active_environment().name
        }
