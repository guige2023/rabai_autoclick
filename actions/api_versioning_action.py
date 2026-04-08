"""
API Versioning Action Module.

Handles API versioning strategies: path-based, header-based,
content negotiation. Supports version compatibility checking.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class VersionConfig:
    """API version configuration."""
    version: str
    deprecated: bool
    sunset_date: Optional[str]
    breaking_changes: list[str]


@dataclass
class VersionResult:
    """Result of version resolution."""
    resolved_version: Optional[str]
    handler: Any
    warnings: list[str]
    deprecated: bool


class APIVersioningAction(BaseAction):
    """Handle API versioning."""

    def __init__(self) -> None:
        super().__init__("api_versioning")
        self._versions: dict[str, VersionConfig] = {}
        self._handlers: dict[str, dict[str, Any]] = {}

    def execute(self, context: dict, params: dict) -> dict:
        """
        Resolve API version and get handler.

        Args:
            context: Execution context
            params: Parameters:
                - request: Request with version info
                - strategy: path, header, or content_negotiation
                - version_header: Header name for version
                - supported_versions: List of supported versions

        Returns:
            VersionResult with resolved version and handler
        """
        request = params.get("request", {})
        strategy = params.get("strategy", "path")
        version_header = params.get("version_header", "API-Version")
        supported_versions = params.get("supported_versions", ["v1"])

        headers = request.get("headers", {})
        path = request.get("path", "")

        resolved_version = None
        warnings = []

        if strategy == "path":
            import re
            match = re.search(r'/v(\d+)/', path)
            if match:
                resolved_version = f"v{match.group(1)}"

        elif strategy == "header":
            resolved_version = headers.get(version_header) or headers.get(version_header.lower())

        elif strategy == "content_negotiation":
            accept = headers.get("Accept", "")
            if "version=" in accept:
                import re
                match = re.search(r'version=([\w.]+)', accept)
                if match:
                    resolved_version = match.group(1)

        if not resolved_version:
            resolved_version = supported_versions[0] if supported_versions else "v1"

        deprecated = False
        if resolved_version in self._versions:
            deprecated = self._versions[resolved_version].deprecated

        if resolved_version not in supported_versions:
            warnings.append(f"Version {resolved_version} not in supported versions: {supported_versions}")
            resolved_version = supported_versions[0] if supported_versions else resolved_version

        handler = self._handlers.get(resolved_version, {})

        return VersionResult(
            resolved_version=resolved_version,
            handler=handler,
            warnings=warnings,
            deprecated=deprecated
        ).__dict__

    def register_version(self, version: str, handler: Any, deprecated: bool = False, sunset_date: Optional[str] = None, breaking_changes: Optional[list] = None) -> None:
        """Register an API version."""
        self._versions[version] = VersionConfig(
            version=version,
            deprecated=deprecated,
            sunset_date=sunset_date,
            breaking_changes=breaking_changes or []
        )
        self._handlers[version] = {"handler": handler, "version": version}
