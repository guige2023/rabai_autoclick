"""API Version Action Module.

Provides API versioning with version negotiation,
deprecation handling, and migration support.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class VersionStrategy(Enum):
    """Version strategy."""
    PATH = "path"
    HEADER = "header"
    QUERY = "query"


@dataclass
class APIVersion:
    """API version definition."""
    version: str
    handler: Callable
    deprecated: bool = False
    sunset_date: Optional[str] = None
    migrations: Optional[List[Callable]] = None


class APIVersionAction:
    """API version manager.

    Example:
        versions = APIVersionAction(
            strategy=VersionStrategy.PATH,
            default_version="v1"
        )

        versions.register("v1", handler_v1)
        versions.register("v2", handler_v2)

        handler = versions.resolve("/api/v2/users", headers)
    """

    def __init__(
        self,
        strategy: VersionStrategy = VersionStrategy.PATH,
        default_version: Optional[str] = None,
    ) -> None:
        self.strategy = strategy
        self.default_version = default_version
        self._versions: Dict[str, APIVersion] = {}
        self._deprecated_handlers: List[str] = []

    def register(
        self,
        version: str,
        handler: Callable,
        deprecated: bool = False,
        sunset_date: Optional[str] = None,
    ) -> "APIVersionAction":
        """Register API version.

        Returns self for chaining.
        """
        self._versions[version] = APIVersion(
            version=version,
            handler=handler,
            deprecated=deprecated,
            sunset_date=sunset_date,
        )

        if deprecated:
            self._deprecated_handlers.append(version)

        return self

    def resolve(
        self,
        path: str,
        headers: Optional[Dict] = None,
        query_params: Optional[Dict] = None,
    ) -> Tuple[Callable, str]:
        """Resolve version and get handler.

        Returns:
            Tuple of (handler, version)
        """
        version = self._extract_version(path, headers, query_params)

        if version in self._versions:
            return self._versions[version].handler, version

        if self.default_version and self.default_version in self._versions:
            return self._versions[self.default_version].handler, self.default_version

        raise ValueError(f"No handler for version: {version}")

    def _extract_version(
        self,
        path: str,
        headers: Optional[Dict],
        query_params: Optional[Dict],
    ) -> Optional[str]:
        """Extract version from request."""
        if self.strategy == VersionStrategy.PATH:
            parts = path.split("/")
            for part in parts:
                if part.startswith("v") and part[1:].replace(".", "").isdigit():
                    return part
            return None

        elif self.strategy == VersionStrategy.HEADER:
            if headers:
                accept = headers.get("Accept", "")
                if "version=" in accept:
                    for part in accept.split(","):
                        if "version=" in part:
                            return part.split("version=")[1].strip()
            return None

        elif self.strategy == VersionStrategy.QUERY:
            if query_params:
                return query_params.get("version")

        return None

    def get_deprecated_versions(self) -> List[str]:
        """Get list of deprecated versions."""
        return self._deprecated_handlers.copy()

    def is_version_deprecated(self, version: str) -> bool:
        """Check if version is deprecated."""
        if version not in self._versions:
            return False
        return self._versions[version].deprecated

    def get_supported_versions(self) -> List[str]:
        """Get list of supported versions."""
        return list(self._versions.keys())

    def migrate(
        self,
        from_version: str,
        to_version: str,
        data: Any,
    ) -> Any:
        """Migrate data between versions.

        Args:
            from_version: Source version
            to_version: Target version
            data: Data to migrate

        Returns:
            Migrated data
        """
        api_version = self._versions.get(to_version)
        if not api_version or not api_version.migrations:
            return data

        result = data
        for migration in api_version.migrations:
            result = migration(result)

        return result
