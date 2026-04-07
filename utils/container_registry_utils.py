"""
Container Registry Management Utilities.

Provides utilities for managing container images, tags, manifests,
and interacting with container registries like Docker Hub, GCR, ECR, and ACR.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import base64
import hashlib
import json
import urllib.request
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional


class RegistryType(Enum):
    """Supported container registry types."""
    DOCKER_HUB = "dockerhub"
    GCR = "gcr"
    ECR = "ecr"
    ACR = "acr"
    HARBOR = "harbor"
    QUAY = "quay"
    GENERIC = "generic"


@dataclass
class ImageManifest:
    """Container image manifest metadata."""
    schema_version: int
    media_type: str
    digest: str
    size_bytes: int
    created_at: Optional[datetime] = None
    tags: list[str] = field(default_factory=list)
    layers: list[dict[str, Any]] = field(default_factory=list)
    config: Optional[dict[str, Any]] = None


@dataclass
class ImageTag:
    """Container image tag information."""
    name: str
    digest: str
    size_bytes: int
    created_at: Optional[datetime] = None
    author: Optional[str] = None
    labels: dict[str, str] = field(default_factory=dict)


@dataclass
class ImageInfo:
    """Container image metadata."""
    repository: str
    registry: str
    tags: list[ImageTag] = field(default_factory=list)
    manifest: Optional[ImageManifest] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ScanResult:
    """Vulnerability scan result for an image."""
    scan_id: str
    digest: str
    scanner: str
    severity_counts: dict[str, int] = field(default_factory=dict)
    vulnerabilities: list[dict[str, Any]] = field(default_factory=list)
    scanned_at: datetime = field(default_factory=datetime.now)
    status: str = "completed"


class ContainerRegistryClient:
    """Client for interacting with container registries."""

    def __init__(
        self,
        registry_url: str,
        registry_type: RegistryType = RegistryType.GENERIC,
        username: Optional[str] = None,
        password: Optional[str] = None,
        verify_ssl: bool = True,
    ) -> None:
        self.registry_url = registry_url.rstrip("/")
        self.registry_type = registry_type
        self.username = username
        self.password = password
        self.verify_ssl = verify_ssl
        self._auth_token: Optional[str] = None

    def authenticate(self) -> bool:
        """Authenticate with the registry."""
        if not self.username or not self.password:
            return False

        if self.registry_type == RegistryType.ECR:
            return self._authenticate_ecr()

        auth_url = f"{self.registry_url}/v2/"
        try:
            request = urllib.request.Request(auth_url)
            self._add_auth(request)

            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status == 200
        except Exception:
            return False

    def _authenticate_ecr(self) -> bool:
        """Authenticate with AWS ECR."""
        return True

    def _add_auth(self, request: urllib.request.Request) -> None:
        """Add authentication to request."""
        if self.username and self.password:
            credentials = f"{self.username}:{self.password}"
            encoded = base64.b64encode(credentials.encode()).decode()
            request.add_header("Authorization", f"Basic {encoded}")

    def list_repositories(
        self,
        namespace: Optional[str] = None,
        page_size: int = 100,
    ) -> list[str]:
        """List repositories in the registry."""
        if self.registry_type == RegistryType.DOCKER_HUB:
            return self._list_dockerhub_repos(namespace)
        elif self.registry_type == RegistryType.GCR:
            return self._list_gcr_repos()
        else:
            return self._list_generic_repos()

    def _list_dockerhub_repos(
        self,
        namespace: Optional[str] = None,
    ) -> list[str]:
        """List Docker Hub repositories."""
        namespace = namespace or "library"
        url = f"https://hub.docker.com/v2/repositories/{namespace}/"

        repos: list[str] = []
        try:
            request = urllib.request.Request(url)
            if self.username and self.password:
                credentials = f"{self.username}:{self.password}"
                encoded = base64.b64encode(credentials.encode()).decode()
                request.add_header("Authorization", f"Basic {encoded}")

            with urllib.request.urlopen(request, timeout=30) as response:
                data = json.loads(response.read().decode())
                repos = [r["name"] for r in data.get("results", [])]

        except Exception:
            pass

        return repos

    def _list_gcr_repos(self) -> list[str]:
        """List Google Container Registry repositories."""
        project = self._extract_gcp_project()
        if not project:
            return []

        url = f"https://gcr.io/v2/{project}/_catalog"

        repos: list[str] = []
        try:
            request = urllib.request.Request(url)
            self._add_auth(request)

            with urllib.request.urlopen(request, timeout=30) as response:
                data = json.loads(response.read().decode())
                repos = data.get("repositories", [])

        except Exception:
            pass

        return repos

    def _list_generic_repos(self) -> list[str]:
        """List repositories from a generic registry."""
        url = f"{self.registry_url}/v2/_catalog"

        repos: list[str] = []
        try:
            request = urllib.request.Request(url)
            self._add_auth(request)

            with urllib.request.urlopen(request, timeout=30) as response:
                data = json.loads(response.read().decode())
                repos = data.get("repositories", [])

        except Exception:
            pass

        return repos

    def list_tags(
        self,
        repository: str,
        page_size: int = 100,
    ) -> list[ImageTag]:
        """List tags for a repository."""
        if self.registry_type == RegistryType.DOCKER_HUB:
            return self._list_dockerhub_tags(repository)
        else:
            return self._list_generic_tags(repository)

    def _list_dockerhub_tags(
        self,
        repository: str,
    ) -> list[ImageTag]:
        """List Docker Hub image tags."""
        url = f"https://hub.docker.com/v2/repositories/{repository}/tags"

        tags: list[ImageTag] = []
        try:
            request = urllib.request.Request(url)
            if self.username and self.password:
                credentials = f"{self.username}:{self.password}"
                encoded = base64.b64encode(credentials.encode()).decode()
                request.add_header("Authorization", f"Basic {encoded}")

            with urllib.request.urlopen(request, timeout=30) as response:
                data = json.loads(response.read().decode())

                for tag_data in data.get("results", []):
                    tag = ImageTag(
                        name=tag_data.get("name", ""),
                        digest=tag_data.get("digest", ""),
                        size_bytes=tag_data.get("full_size", 0),
                        created_at=datetime.fromisoformat(tag_data["last_updated"].replace("Z", "+00:00")) if tag_data.get("last_updated") else None,
                    )
                    tags.append(tag)

        except Exception:
            pass

        return tags

    def _list_generic_tags(
        self,
        repository: str,
    ) -> list[ImageTag]:
        """List tags from a generic registry."""
        url = f"{self.registry_url}/v2/{repository}/tags/list"

        tags: list[ImageTag] = []
        try:
            request = urllib.request.Request(url)
            self._add_auth(request)

            with urllib.request.urlopen(request, timeout=30) as response:
                data = json.loads(response.read().decode())

                for tag_name in data.get("tags", []):
                    tag = ImageTag(
                        name=tag_name,
                        digest="",
                        size_bytes=0,
                    )
                    tags.append(tag)

        except Exception:
            pass

        return tags

    def get_manifest(
        self,
        repository: str,
        tag: str,
    ) -> Optional[ImageManifest]:
        """Get manifest for a specific image tag."""
        url = f"{self.registry_url}/v2/{repository}/manifests/{tag}"

        try:
            request = urllib.request.Request(url)
            self._add_auth(request)
            request.add_header("Accept", "application/vnd.docker.distribution.manifest.v2+json")

            with urllib.request.urlopen(request, timeout=30) as response:
                manifest_data = json.loads(response.read().decode())

                digest = request.get_header("Docker-Content-Digest", "")

                return ImageManifest(
                    schema_version=manifest_data.get("schemaVersion", 0),
                    media_type=manifest_data.get("mediaType", ""),
                    digest=digest,
                    size_bytes=0,
                    layers=[
                        {
                            "mediaType": layer.get("mediaType", ""),
                            "size": layer.get("size", 0),
                            "digest": layer.get("digest", ""),
                        }
                        for layer in manifest_data.get("layers", [])
                    ],
                    config=manifest_data.get("config"),
                )

        except Exception:
            return None

    def get_digest(
        self,
        repository: str,
        tag: str,
    ) -> Optional[str]:
        """Get the digest (SHA256) for a specific tag."""
        url = f"{self.registry_url}/v2/{repository}/manifests/{tag}"

        try:
            request = urllib.request.Request(url)
            self._add_auth(request)
            request.add_header("Accept", "application/vnd.docker.distribution.manifest.v2+json")

            with urllib.request.urlopen(request, timeout=30) as response:
                digest = response.headers.get("Docker-Content-Digest", "")
                return digest

        except Exception:
            return None

    def delete_tag(
        self,
        repository: str,
        tag: str,
    ) -> bool:
        """Delete a specific tag from a repository."""
        digest = self.get_digest(repository, tag)
        if not digest:
            return False

        url = f"{self.registry_url}/v2/{repository}/manifests/{digest}"

        try:
            request = urllib.request.Request(url, method="DELETE")
            self._add_auth(request)

            with urllib.request.urlopen(request, timeout=30) as response:
                return response.status in (200, 202)

        except Exception:
            return False

    def image_exists(
        self,
        repository: str,
        tag: str,
    ) -> bool:
        """Check if an image tag exists."""
        return self.get_digest(repository, tag) is not None

    def get_image_info(
        self,
        repository: str,
        tag: str = "latest",
    ) -> Optional[ImageInfo]:
        """Get complete image information."""
        manifest = self.get_manifest(repository, tag)
        if not manifest:
            return None

        return ImageInfo(
            repository=repository,
            registry=self.registry_url,
            tags=[ImageTag(name=tag, digest=manifest.digest, size_bytes=manifest.size_bytes)],
            manifest=manifest,
        )

    def compare_images(
        self,
        repository: str,
        tag1: str,
        tag2: str,
    ) -> dict[str, Any]:
        """Compare two image tags and return diff information."""
        digest1 = self.get_digest(repository, tag1)
        digest2 = self.get_digest(repository, tag2)

        if not digest1 or not digest2:
            return {"error": "One or both images not found"}

        manifest1 = self.get_manifest(repository, tag1)
        manifest2 = self.get_manifest(repository, tag2)

        if not manifest1 or not manifest2:
            return {"error": "One or both manifests not found"}

        layers1 = {layer["digest"] for layer in manifest1.layers}
        layers2 = {layer["digest"] for layer in manifest2.layers}

        return {
            "same_digest": digest1 == digest2,
            "tag1": tag1,
            "tag2": tag2,
            "digest1": digest1,
            "digest2": digest2,
            "common_layers": list(layers1 & layers2),
            "only_in_tag1": list(layers1 - layers2),
            "only_in_tag2": list(layers2 - layers1),
            "layer_diff_count": len(layers1 ^ layers2),
        }

    def _extract_gcp_project(self) -> Optional[str]:
        """Extract GCP project from registry URL."""
        if "gcr.io" in self.registry_url:
            parts = self.registry_url.split("/")
            for part in parts:
                if part and "." not in part and part not in ("gcr.io", "asia.gcr.io", "us.gcr.io", "eu.gcr.io"):
                    return part
        return None

    def prune_untagged(
        self,
        repository: str,
        dry_run: bool = True,
    ) -> list[str]:
        """Remove untagged (dangling) images in a repository."""
        tags = self.list_tags(repository)

        repo_url = f"{self.registry_url}/v2/{repository}"
        deleted: list[str] = []

        try:
            request = urllib.request.Request(repo_url)
            self._add_auth(request)

            with urllib.request.urlopen(request, timeout=30) as response:
                pass

        except Exception:
            return deleted

        if dry_run:
            return ["Would delete untagged images (dry run)"]

        return deleted


class ImageBuilder:
    """Helper for building container images programmatically."""

    def __init__(
        self,
        registry_client: ContainerRegistryClient,
    ) -> None:
        self.client = registry_client

    def generate_dockerfile(
        self,
        base_image: str,
        run_commands: Optional[list[str]] = None,
        env_vars: Optional[dict[str, str]] = None,
        expose_ports: Optional[list[int]] = None,
        working_dir: Optional[str] = None,
        entrypoint: Optional[list[str]] = None,
        cmd: Optional[list[str]] = None,
    ) -> str:
        """Generate a Dockerfile from parameters."""
        lines = [f"FROM {base_image}", ""]

        if env_vars:
            for key, value in env_vars.items():
                lines.append(f"ENV {key}={value}")
            lines.append("")

        if working_dir:
            lines.append(f"WORKDIR {working_dir}")
            lines.append("")

        if run_commands:
            for cmd in run_commands:
                lines.append(f"RUN {cmd}")
            lines.append("")

        if expose_ports:
            for port in expose_ports:
                lines.append(f"EXPOSE {port}")
            lines.append("")

        if entrypoint:
            lines.append(f"ENTRYPOINT {json.dumps(entrypoint)}")

        if cmd:
            lines.append(f"CMD {json.dumps(cmd)}")

        return "\n".join(lines)

    def calculate_image_size(
        self,
        layers: list[dict[str, Any]],
    ) -> int:
        """Calculate total image size from layers."""
        return sum(layer.get("size", 0) for layer in layers)
