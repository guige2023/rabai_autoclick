"""
Docker Compose utilities for multi-container application management.

Provides service definition building, volume management, network
configuration, health checks, and compose file generation.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Optional

logger = logging.getLogger(__name__)


class RestartPolicy(Enum):
    NO = "no"
    ALWAYS = "always"
    ON_FAILURE = "on-failure"
    UNLESS_STOPPED = "unless-stopped"


@dataclass
class PortMapping:
    """Port mapping definition."""
    host: int
    container: int
    protocol: str = "tcp"

    def to_string(self) -> str:
        return f"{self.host}:{self.container}/{self.protocol}"


@dataclass
class VolumeMount:
    """Volume mount definition."""
    source: str
    target: str
    read_only: bool = False
    type: str = "bind"  # bind, volume, tmpfs

    def to_string(self) -> str:
        opts = "ro" if self.read_only else "rw"
        if self.type == "bind":
            return f"{self.source}:{self.target}:{opts}"
        return f"{self.source}:{self.target}"


@dataclass
class HealthCheck:
    """Container health check configuration."""
    test: list[str]
    interval: str = "30s"
    timeout: str = "10s"
    retries: int = 3
    start_period: str = "0s"


@dataclass
class ServiceDefinition:
    """Docker service definition."""
    name: str
    image: str
    build: Optional[str] = None
    command: Optional[str] = None
    entrypoint: Optional[list[str]] = None
    environment: dict[str, str] = field(default_factory=dict)
    env_file: Optional[str] = None
    ports: list[PortMapping] = field(default_factory=list)
    volumes: list[VolumeMount] = field(default_factory=list)
    networks: list[str] = field(default_factory=list)
    depends_on: list[str] = field(default_factory=list)
    restart: RestartPolicy = RestartPolicy.UNLESS_STOPPED
    healthcheck: Optional[HealthCheck] = None
    labels: dict[str, str] = field(default_factory=dict)
    working_dir: Optional[str] = None
    user: Optional[str] = None
    cap_add: list[str] = field(default_factory=list)
    command_override: Optional[str] = None


@dataclass
class NetworkDefinition:
    """Docker network definition."""
    name: str
    driver: str = "bridge"
    driver_opts: dict[str, str] = field(default_factory=dict)
    ipam_config: Optional[dict[str, Any]] = None
    external: bool = False


@dataclass
class VolumeDefinition:
    """Docker volume definition."""
    name: str
    driver: str = "local"
    driver_opts: dict[str, str] = field(default_factory=dict)
    external: bool = False


class DockerComposeBuilder:
    """Builds Docker Compose YAML files."""

    def __init__(self, version: str = "3.9") -> None:
        self.version = version
        self.services: dict[str, ServiceDefinition] = {}
        self.networks: dict[str, NetworkDefinition] = {}
        self.volumes: dict[str, VolumeDefinition] = {}

    def add_service(self, service: ServiceDefinition) -> "DockerComposeBuilder":
        self.services[service.name] = service
        return self

    def add_network(self, network: NetworkDefinition) -> "DockerComposeBuilder":
        self.networks[network.name] = network
        return self

    def add_volume(self, volume: VolumeDefinition) -> "DockerComposeBuilder":
        self.volumes[volume.name] = volume
        return self

    def render(self) -> str:
        """Render the complete Docker Compose YAML."""
        import yaml

        doc: dict[str, Any] = {
            "version": f"'{self.version}'",
        }

        if self.services:
            doc["services"] = {}
            for name, svc in self.services.items():
                doc["services"][name] = self._render_service(svc)

        if self.networks:
            doc["networks"] = {}
            for name, net in self.networks.items():
                doc["networks"][name] = self._render_network(net)

        if self.volumes:
            doc["volumes"] = {}
            for name, vol in self.volumes.items():
                doc["volumes"][name] = self._render_volume(vol)

        return yaml.dump(doc, default_flow_style=False, sort_keys=False, allow_unicode=True)

    def _render_service(self, svc: ServiceDefinition) -> dict[str, Any]:
        result: dict[str, Any] = {"image": svc.image}

        if svc.build:
            result["build"] = svc.build
        if svc.command:
            result["command"] = svc.command
        if svc.entrypoint:
            result["entrypoint"] = svc.entrypoint
        if svc.working_dir:
            result["working_dir"] = svc.working_dir
        if svc.user:
            result["user"] = svc.user

        if svc.environment:
            result["environment"] = dict(svc.environment)
        if svc.env_file:
            result["env_file"] = svc.env_file

        if svc.ports:
            result["ports"] = [p.to_string() for p in svc.ports]

        if svc.volumes:
            result["volumes"] = [v.to_string() for v in svc.volumes]

        if svc.networks:
            result["networks"] = svc.networks

        if svc.depends_on:
            result["depends_on"] = svc.depends_on

        result["restart"] = svc.restart.value

        if svc.healthcheck:
            hc = svc.healthcheck
            result["healthcheck"] = {
                "test": hc.test,
                "interval": hc.interval,
                "timeout": hc.timeout,
                "retries": hc.retries,
                "start_period": hc.start_period,
            }

        if svc.labels:
            result["labels"] = dict(svc.labels)

        if svc.cap_add:
            result["cap_add"] = svc.cap_add

        return result

    def _render_network(self, net: NetworkDefinition) -> dict[str, Any]:
        result: dict[str, Any] = {"driver": net.driver}
        if net.driver_opts:
            result["driver_opts"] = net.driver_opts
        if net.ipam_config:
            result["ipam"] = {"config": [net.ipam_config]}
        if net.external:
            result["external"] = True
        return result

    def _render_volume(self, vol: VolumeDefinition) -> dict[str, Any]:
        result: dict[str, Any] = {"driver": vol.driver}
        if vol.driver_opts:
            result["driver_opts"] = vol.driver_opts
        if vol.external:
            result["external"] = True
        return result

    def write(self, filepath: str) -> None:
        """Write the compose file to disk."""
        os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
        with open(filepath, "w") as f:
            f.write(self.render())
        logger.info("Wrote Docker Compose file to %s", filepath)


class DockerComposeRunner:
    """Executes Docker Compose commands."""

    def __init__(self, compose_file: str = "docker-compose.yml", project_name: Optional[str] = None) -> None:
        self.compose_file = compose_file
        self.project_name = project_name
        import subprocess
        self._run = subprocess.run

    def up(
        self,
        detached: bool = True,
        remove_orphans: bool = True,
        build: bool = False,
    ) -> bool:
        """Run docker-compose up."""
        import subprocess
        cmd = ["docker-compose", "-f", self.compose_file]
        if self.project_name:
            cmd.extend(["-p", self.project_name])
        cmd.append("up")
        if detached:
            cmd.append("-d")
        if remove_orphans:
            cmd.append("--remove-orphans")
        if build:
            cmd.append("--build")
        try:
            result = self._run(cmd, check=True)
            return result.returncode == 0
        except subprocess.CalledProcessError as e:
            logger.error("docker-compose up failed: %s", e)
            return False

    def down(self, remove_volumes: bool = False, remove_images: bool = False) -> bool:
        """Run docker-compose down."""
        import subprocess
        cmd = ["docker-compose", "-f", self.compose_file]
        if self.project_name:
            cmd.extend(["-p", self.project_name])
        cmd.append("down")
        if remove_volumes:
            cmd.append("-v")
        if remove_images:
            cmd.append("--rmi all")
        try:
            result = self._run(cmd, check=True)
            return result.returncode == 0
        except subprocess.CalledProcessError as e:
            logger.error("docker-compose down failed: %s", e)
            return False

    def ps(self) -> str:
        """Run docker-compose ps."""
        import subprocess
        cmd = ["docker-compose", "-f", self.compose_file]
        if self.project_name:
            cmd.extend(["-p", self.project_name])
        cmd.append("ps")
        try:
            result = self._run(cmd, capture_output=True, text=True)
            return result.stdout
        except subprocess.CalledProcessError as e:
            return f"Error: {e}"

    def logs(self, service: Optional[str] = None, follow: bool = False, tail: int = 100) -> str:
        """Get docker-compose logs."""
        import subprocess
        cmd = ["docker-compose", "-f", self.compose_file]
        if self.project_name:
            cmd.extend(["-p", self.project_name])
        cmd.append("logs")
        if follow:
            cmd.append("-f")
        if tail:
            cmd.extend(["--tail", str(tail)])
        if service:
            cmd.append(service)
        try:
            result = self._run(cmd, capture_output=True, text=True)
            return result.stdout
        except subprocess.CalledProcessError as e:
            return f"Error: {e}"
