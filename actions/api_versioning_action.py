"""API versioning action module for RabAI AutoClick.

Provides API versioning:
- APIVersioning: Manage API versions
- VersionRouter: Route based on version
- VersionNegotiator: Content negotiation
- DeprecationHandler: Handle deprecated APIs
"""

import time
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class VersionScheme(Enum):
    """Version scheme types."""
    HEADER = "header"
    URL_PATH = "url_path"
    QUERY_PARAM = "query_param"
    CONTENT_TYPE = "content_type"


@dataclass
class APIVersion:
    """API version definition."""
    version: str
    handler: Callable
    deprecated: bool = False
    sunset_date: Optional[str] = None
    deprecation_message: Optional[str] = None


@dataclass
class VersionMatch:
    """Version match result."""
    matched: bool
    version: Optional[str]
    handler: Optional[Callable]
    is_deprecated: bool
    warnings: List[str]


class APIVersioning:
    """API versioning manager."""

    def __init__(self, scheme: VersionScheme = VersionScheme.URL_PATH):
        self.scheme = scheme
        self._versions: Dict[str, APIVersion] = {}
        self._default_version: Optional[str] = None

    def register_version(
        self,
        version: str,
        handler: Callable,
        deprecated: bool = False,
        sunset_date: Optional[str] = None,
        deprecation_message: Optional[str] = None,
    ) -> bool:
        """Register API version."""
        api_version = APIVersion(
            version=version,
            handler=handler,
            deprecated=deprecated,
            sunset_date=sunset_date,
            deprecation_message=deprecation_message,
        )
        self._versions[version] = api_version

        if self._default_version is None:
            self._default_version = version

        return True

    def set_default_version(self, version: str) -> bool:
        """Set default version."""
        if version in self._versions:
            self._default_version = version
            return True
        return False

    def match_version(self, request_info: Dict[str, Any]) -> VersionMatch:
        """Match version from request."""
        version_str = self._extract_version(request_info)

        if version_str and version_str in self._versions:
            api_version = self._versions[version_str]
            warnings = []

            if api_version.deprecated:
                warnings.append(api_version.deprecation_message or f"Version {version_str} is deprecated")

            if api_version.sunset_date:
                warnings.append(f"Version {version_str} will be sunset on {api_version.sunset_date}")

            return VersionMatch(
                matched=True,
                version=version_str,
                handler=api_version.handler,
                is_deprecated=api_version.deprecated,
                warnings=warnings,
            )

        if self._default_version:
            default = self._versions[self._default_version]
            return VersionMatch(
                matched=True,
                version=self._default_version,
                handler=default.handler,
                is_deprecated=default.deprecated,
                warnings=["Using default version"],
            )

        return VersionMatch(
            matched=False,
            version=None,
            handler=None,
            is_deprecated=False,
            warnings=["No version matched and no default version set"],
        )

    def _extract_version(self, request_info: Dict[str, Any]) -> Optional[str]:
        """Extract version from request."""
        if self.scheme == VersionScheme.URL_PATH:
            path = request_info.get("path", "")
            parts = path.strip("/").split("/")
            if parts and parts[0].lower().startswith("v"):
                return parts[0]

        elif self.scheme == VersionScheme.HEADER:
            headers = request_info.get("headers", {})
            return headers.get("X-API-Version") or headers.get("Accept-Version")

        elif self.scheme == VersionScheme.QUERY_PARAM:
            params = request_info.get("query_params", {})
            return params.get("version") or params.get("v")

        elif self.scheme == VersionScheme.CONTENT_TYPE:
            content_type = request_info.get("headers", {}).get("Content-Type", "")
            if "version=" in content_type:
                for part in content_type.split(";"):
                    if part.strip().startswith("version="):
                        return part.split("=")[1].strip()

        return None

    def list_versions(self) -> List[Dict]:
        """List all registered versions."""
        return [
            {
                "version": v.version,
                "deprecated": v.deprecated,
                "sunset_date": v.sunset_date,
                "is_default": v.version == self._default_version,
            }
            for v in self._versions.values()
        ]

    def get_version_info(self, version: str) -> Optional[Dict]:
        """Get version information."""
        if version not in self._versions:
            return None

        v = self._versions[version]
        return {
            "version": v.version,
            "deprecated": v.deprecated,
            "sunset_date": v.sunset_date,
            "deprecation_message": v.deprecation_message,
            "is_default": v.version == self._default_version,
        }


class VersionRouter:
    """Route requests based on version."""

    def __init__(self, versioning: APIVersioning):
        self.versioning = versioning

    def route(self, request_info: Dict[str, Any]) -> Tuple[Optional[Callable], VersionMatch]:
        """Route request to versioned handler."""
        match = self.versioning.match_version(request_info)

        if match.matched and match.handler:
            return match.handler, match

        return None, match


class DeprecationHandler:
    """Handle deprecated API versions."""

    def __init__(self):
        self._handlers: Dict[str, Callable] = {}

    def register_handler(self, version: str, handler: Callable) -> bool:
        """Register deprecation handler."""
        self._handlers[version] = handler
        return True

    def get_handler(self, version: str) -> Optional[Callable]:
        """Get deprecation handler."""
        return self._handlers.get(version)

    def handle_deprecation(self, version: str, request: Dict, response: Dict) -> Dict:
        """Handle deprecated version."""
        handler = self._handlers.get(version)

        if handler:
            return handler(request, response)

        response["warnings"] = response.get("warnings", [])
        response["warnings"].append(f"Version {version} is deprecated. Please upgrade.")
        return response


class APIVersioningAction(BaseAction):
    """API versioning action."""
    action_type = "api_versioning"
    display_name = "API版本控制"
    description = "API版本管理和路由"

    def __init__(self):
        super().__init__()
        scheme_str = "URL_PATH"
        try:
            scheme = VersionScheme[scheme_str]
        except KeyError:
            scheme = VersionScheme.URL_PATH
        self._versioning = APIVersioning(scheme)
        self._router = VersionRouter(self._versioning)

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "register")

            if operation == "register":
                return self._register_version(params)
            elif operation == "route":
                return self._route_request(params)
            elif operation == "list":
                return self._list_versions(params)
            elif operation == "info":
                return self._get_version_info(params)
            elif operation == "set_default":
                return self._set_default(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Versioning error: {str(e)}")

    def _register_version(self, params: Dict) -> ActionResult:
        """Register API version."""
        version = params.get("version")
        handler = params.get("handler")

        if not version:
            return ActionResult(success=False, message="version is required")

        self._versioning.register_version(
            version=version,
            handler=handler or (lambda ctx: {}),
            deprecated=params.get("deprecated", False),
            sunset_date=params.get("sunset_date"),
            deprecation_message=params.get("deprecation_message"),
        )

        return ActionResult(success=True, message=f"Version '{version}' registered")

    def _route_request(self, params: Dict) -> ActionResult:
        """Route request to versioned handler."""
        request_info = {
            "path": params.get("path", "/"),
            "headers": params.get("headers", {}),
            "query_params": params.get("query_params", {}),
        }

        handler, match = self._router.route(request_info)

        if match.matched:
            return ActionResult(
                success=True,
                message=f"Routed to version {match.version}" + (" (deprecated)" if match.is_deprecated else ""),
                data={
                    "version": match.version,
                    "is_deprecated": match.is_deprecated,
                    "warnings": match.warnings,
                },
            )
        else:
            return ActionResult(
                success=False,
                message="No matching version found",
                data={"warnings": match.warnings},
            )

    def _list_versions(self, params: Dict) -> ActionResult:
        """List all versions."""
        versions = self._versioning.list_versions()
        return ActionResult(success=True, message=f"{len(versions)} versions", data={"versions": versions})

    def _get_version_info(self, params: Dict) -> ActionResult:
        """Get version info."""
        version = params.get("version")
        if not version:
            return ActionResult(success=False, message="version is required")

        info = self._versioning.get_version_info(version)
        if info:
            return ActionResult(success=True, message=f"Version {version}", data=info)
        else:
            return ActionResult(success=False, message=f"Version '{version}' not found")

    def _set_default(self, params: Dict) -> ActionResult:
        """Set default version."""
        version = params.get("version")
        if not version:
            return ActionResult(success=False, message="version is required")

        success = self._versioning.set_default_version(version)
        return ActionResult(success=success, message=f"Default version set to '{version}'" if success else "Version not found")
