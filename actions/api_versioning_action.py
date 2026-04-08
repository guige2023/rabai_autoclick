"""
API Versioning Action Module.

Handles API versioning, deprecation cycles, and migration support
 with automatic version detection and routing.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class VersionPhase(Enum):
    """Lifecycle phase of an API version."""
    CURRENT = "current"
    DEPRECATED = "deprecated"
    SUNSET = "sunset"
    RETIRED = "retired"


@dataclass
class APIVersion:
    """A registered API version."""
    version: str
    phase: VersionPhase
    released_at: float
    sunset_at: Optional[float] = None
    docs_url: Optional[str] = None
    migration_guide: Optional[str] = None


@dataclass
class VersionConfig:
    """Configuration for API versioning."""
    default_version: str = "v1"
    supported_versions: list[str] = field(default_factory=lambda: ["v1", "v2"])
    deprecation_warning_threshold_days: int = 30
    sunset_warning_threshold_days: int = 14


@dataclass
class VersionedRoute:
    """A route handler for a specific version."""
    path: str
    method: str
    handler: Callable
    version: str


@dataclass
class VersionResult:
    """Result of version-aware request handling."""
    success: bool
    version_used: str
    data: Any
    deprecation_warning: Optional[str] = None
    sunset_warning: Optional[str] = None
    migration_suggestion: Optional[str] = None


class APIVersioningAction:
    """
    API versioning and lifecycle management system.

    Manages multiple API versions, handles deprecation, and
    provides migration guidance for API consumers.

    Example:
        versioning = APIVersioningAction(config=VersionConfig())
        versioning.register_version("v1", VersionPhase.CURRENT)
        versioning.register_version("v2", VersionPhase.CURRENT)
        versioning.deprecate_version("v1", sunset_days=90)
        result = await versioning.handle_request("v1", "/users", "GET", handler)
    """

    def __init__(
        self,
        config: Optional[VersionConfig] = None,
    ) -> None:
        self.config = config or VersionConfig()
        self._versions: Dict[str, APIVersion] = {}
        self._routes: Dict[Tuple[str, str, str], VersionedRoute] = {}
        self._version_handlers: Dict[str, Callable] = {}

    def register_version(
        self,
        version: str,
        phase: VersionPhase = VersionPhase.CURRENT,
        docs_url: Optional[str] = None,
    ) -> "APIVersioningAction":
        """Register a new API version."""
        api_version = APIVersion(
            version=version,
            phase=phase,
            released_at=time.time(),
            docs_url=docs_url,
        )
        self._versions[version] = api_version
        logger.info(f"Registered API version: {version} ({phase.value})")
        return self

    def deprecate_version(
        self,
        version: str,
        sunset_days: int = 90,
        migration_guide: Optional[str] = None,
    ) -> "APIVersioningAction":
        """Mark a version as deprecated with a sunset date."""
        if version not in self._versions:
            logger.warning(f"Version {version} not registered")
            return self

        v = self._versions[version]
        v.phase = VersionPhase.DEPRECATED
        v.sunset_at = time.time() + (sunset_days * 86400)
        v.migration_guide = migration_guide
        logger.info(f"Deprecated version {version}, sunset in {sunset_days} days")
        return self

    def retire_version(self, version: str) -> "APIVersioningAction":
        """Mark a version as retired."""
        if version in self._versions:
            self._versions[version].phase = VersionPhase.RETIRED
            logger.info(f"Retired version {version}")
        return self

    def register_route(
        self,
        path: str,
        method: str,
        handler: Callable,
        version: Optional[str] = None,
    ) -> "APIVersioningAction":
        """Register a versioned route handler."""
        version = version or self.config.default_version
        route = VersionedRoute(
            path=path,
            method=method.upper(),
            handler=handler,
            version=version,
        )
        self._routes[(version, method.upper(), path)] = route
        return self

    def get_version_info(self, version: str) -> Optional[APIVersion]:
        """Get information about a specific version."""
        return self._versions.get(version)

    def list_versions(self, phase: Optional[VersionPhase] = None) -> list[APIVersion]:
        """List all registered versions, optionally filtered by phase."""
        versions = list(self._versions.values())
        if phase:
            versions = [v for v in versions if v.phase == phase]
        return sorted(versions, key=lambda v: v.version)

    def get_current_version(self) -> Optional[APIVersion]:
        """Get the current (non-deprecated) version."""
        current = [
            v for v in self._versions.values()
            if v.phase == VersionPhase.CURRENT
        ]
        return current[0] if current else None

    async def handle_request(
        self,
        version: str,
        path: str,
        method: str,
        handler: Callable,
        **kwargs: Any,
    ) -> VersionResult:
        """Handle a versioned API request."""
        data = None
        deprecation_warning = None
        sunset_warning = None
        migration_suggestion = None

        api_version = self._versions.get(version)

        if not api_version:
            return VersionResult(
                success=False,
                version_used=version,
                data=None,
                migration_suggestion=f"Version {version} not found. Use one of: {', '.join(self.config.supported_versions)}",
            )

        if api_version.phase == VersionPhase.RETIRED:
            return VersionResult(
                success=False,
                version_used=version,
                data=None,
                migration_suggestion=f"Version {version} has been retired. Please migrate to a supported version.",
            )

        if api_version.phase == VersionPhase.DEPRECATED:
            deprecation_warning = f"Version {version} is deprecated"

            if api_version.sunset_at:
                days_until_sunset = (api_version.sunset_at - time.time()) / 86400
                if days_until_sunset <= self.config.sunset_warning_threshold_days:
                    sunset_warning = f"Version {version} will be retired in {int(days_until_sunset)} days"

                if days_until_sunset <= 0:
                    return VersionResult(
                        success=False,
                        version_used=version,
                        data=None,
                        sunset_warning="Version has reached sunset date",
                        migration_suggestion=api_version.migration_guide,
                    )

            if api_version.migration_guide:
                migration_suggestion = api_version.migration_guide

        try:
            if callable(handler):
                if asyncio.iscoroutinefunction(handler):
                    data = await handler(**kwargs)
                else:
                    data = handler(**kwargs)
            else:
                data = handler

            return VersionResult(
                success=True,
                version_used=version,
                data=data,
                deprecation_warning=deprecation_warning,
                sunset_warning=sunset_warning,
                migration_suggestion=migration_suggestion,
            )

        except Exception as e:
            return VersionResult(
                success=False,
                version_used=version,
                data=None,
                migration_suggestion=str(e),
            )

    def detect_version_from_header(
        self,
        accept_header: Optional[str] = None,
    ) -> Optional[str]:
        """Detect API version from Accept header."""
        if not accept_header:
            return self.config.default_version

        if "version=" in accept_header:
            for part in accept_header.split(","):
                if "version=" in part:
                    return part.split("version=")[1].strip()

        return self.config.default_version

    def get_deprecation_headers(
        self,
        version: str,
    ) -> dict[str, str]:
        """Get standard deprecation headers."""
        headers: dict[str, str] = {}
        api_version = self._versions.get(version)

        if api_version and api_version.phase == VersionPhase.DEPRECATED:
            headers["Deprecation"] = f"version {version}"
            headers["Sunset"] = f"{time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime(api_version.sunset_at))}"

            if api_version.migration_guide:
                headers["Link"] = f"<{api_version.migration_guide}>; rel=\"migration-guide\""

        return headers


import asyncio
