"""API versioning action module for RabAI AutoClick.

Provides API versioning operations:
- VersionRouterAction: Route requests based on version
- VersionNegotiatorAction: Content negotiation for versions
- VersionMigrationAction: Migrate between API versions
- VersionManagerAction: Manage API version definitions
"""

import sys
import os
import logging
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult

logger = logging.getLogger(__name__)


@dataclass
class APIVersion:
    """API version definition."""
    version: str
    status: str
    deprecated: bool = False
    sunset_date: Optional[datetime] = None
    changelog: str = ""
    handlers: Dict[str, Callable] = field(default_factory=dict)


@dataclass
class VersionRoute:
    """Route configuration for version routing."""
    path_pattern: str
    version_constraint: str
    handler: Optional[Callable] = None


class VersionRegistry:
    """Registry for API versions."""

    def __init__(self) -> None:
        self._versions: Dict[str, APIVersion] = {}
        self._routes: List[VersionRoute] = []
        self._default_version = "v1"

    def register_version(self, version: APIVersion) -> None:
        self._versions[version.version] = version

    def get_version(self, version: str) -> Optional[APIVersion]:
        return self._versions.get(version)

    def list_versions(self, include_deprecated: bool = False) -> List[APIVersion]:
        versions = list(self._versions.values())
        if not include_deprecated:
            versions = [v for v in versions if not v.deprecated]
        return sorted(versions, key=lambda v: v.version, reverse=True)

    def add_route(self, route: VersionRoute) -> None:
        self._routes.append(route)

    def match_route(self, path: str, version: Optional[str] = None) -> Optional[VersionRoute]:
        for route in self._routes:
            if self._path_matches(path, route.path_pattern):
                if version and route.version_constraint:
                    if self._version_matches(version, route.version_constraint):
                        return route
                elif not route.version_constraint:
                    return route
        return None

    def _path_matches(self, path: str, pattern: str) -> bool:
        if pattern == "*":
            return True
        if "{" in pattern:
            import re
            regex = pattern.replace("{version}", r"[\w.]+")
            return bool(re.match(f"^{regex}$", path))
        return path.startswith(pattern.rstrip("*"))

    def _version_matches(self, version: str, constraint: str) -> bool:
        if constraint.startswith(">="):
            return version >= constraint[2:]
        if constraint.startswith(">"):
            return version > constraint[1:]
        if constraint.startswith("<="):
            return version <= constraint[2:]
        if constraint.startswith("<"):
            return version < constraint[1:]
        if constraint.startswith("^"):
            return version.startswith(constraint[1:])
        return version == constraint

    def set_default(self, version: str) -> bool:
        if version in self._versions:
            self._default_version = version
            return True
        return False

    def get_default(self) -> str:
        return self._default_version


_registry = VersionRegistry()


class VersionRouterAction(BaseAction):
    """Route requests based on version."""
    action_type = "api_version_router"
    display_name = "版本路由"
    description = "根据版本路由API请求"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        path = params.get("path", "/")
        version = params.get("version")
        operation = params.get("operation", "route")

        if operation == "route":
            route = _registry.match_route(path, version)
            if route:
                return ActionResult(
                    success=True,
                    message=f"路由匹配: {route.path_pattern} -> {route.version_constraint}",
                    data={"path": path, "version_constraint": route.version_constraint}
                )

            default = _registry.get_default()
            return ActionResult(
                success=True,
                message=f"使用默认版本 {default}",
                data={"path": path, "version": default, "is_default": True}
            )

        if operation == "list":
            versions = _registry.list_versions()
            return ActionResult(
                success=True,
                message=f"共 {len(versions)} 个版本",
                data={"versions": [{"version": v.version, "status": v.status, "deprecated": v.deprecated} for v in versions]}
            )

        return ActionResult(success=False, message=f"未知操作: {operation}")


class VersionNegotiatorAction(BaseAction):
    """Content negotiation for versions."""
    action_type = "api_version_negotiator"
    display_name = "版本协商"
    description = "协商确定使用的API版本"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        accept_header = params.get("accept", "")
        path = params.get("path", "/")
        operation = params.get("operation", "negotiate")

        if operation == "negotiate":
            version = None
            if accept_header:
                if "version=" in accept_header:
                    import re
                    match = re.search(r'version=([\w.]+)', accept_header)
                    if match:
                        version = match.group(1)

            if not version:
                version = _registry.get_default()

            version_info = _registry.get_version(version)
            if version_info and version_info.deprecated:
                return ActionResult(
                    success=True,
                    message=f"版本 {version} 已废弃",
                    data={"version": version, "deprecated": True, "sunset_date": version_info.sunset_date.isoformat() if version_info.sunset_date else None}
                )

            return ActionResult(
                success=True,
                message=f"协商版本: {version}",
                data={"version": version, "deprecated": False}
            )

        if operation == "parse":
            if not accept_header:
                return ActionResult(success=False, message="accept header是必需的")

            parsed = {}
            for part in accept_header.split(","):
                part = part.strip()
                if "=" in part:
                    key, value = part.split("=", 1)
                    parsed[key.strip()] = value.strip().strip('"')

            return ActionResult(
                success=True,
                message="Header解析完成",
                data={"parsed": parsed}
            )

        return ActionResult(success=False, message=f"未知操作: {operation}")


class VersionMigrationAction(BaseAction):
    """Migrate between API versions."""
    action_type = "api_version_migration"
    display_name = "版本迁移"
    description = "执行API版本间的数据迁移"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        from_version = params.get("from_version", "")
        to_version = params.get("to_version", "")
        data = params.get("data", {})
        migration_type = params.get("migration_type", "field_rename")

        if not from_version or not to_version:
            return ActionResult(success=False, message="from_version和to_version都是必需的")

        if from_version == to_version:
            return ActionResult(success=True, message="版本相同，无需迁移", data={"data": data})

        migrated = data.copy()

        if migration_type == "field_rename":
            renames = params.get("renames", {})
            for old_name, new_name in renames.items():
                if old_name in migrated:
                    migrated[new_name] = migrated.pop(old_name)

        elif migration_type == "field_remove":
            remove_fields = params.get("remove_fields", [])
            for field in remove_fields:
                migrated.pop(field, None)

        elif migration_type == "field_add":
            add_fields = params.get("add_fields", {})
            migrated.update(add_fields)

        elif migration_type == "transform":
            transforms = params.get("transforms", [])
            for t in transforms:
                field_name = t.get("field")
                transform_fn = t.get("fn", "identity")
                if field_name in migrated:
                    if transform_fn == "uppercase":
                        migrated[field_name] = str(migrated[field_name]).upper()
                    elif transform_fn == "lowercase":
                        migrated[field_name] = str(migrated[field_name]).lower()
                    elif transform_fn == "to_string":
                        migrated[field_name] = str(migrated[field_name])
                    elif transform_fn == "to_int":
                        try:
                            migrated[field_name] = int(migrated[field_name])
                        except (ValueError, TypeError):
                            pass

        return ActionResult(
            success=True,
            message=f"从 {from_version} 迁移到 {to_version} 完成",
            data={
                "from_version": from_version,
                "to_version": to_version,
                "migration_type": migration_type,
                "data": migrated
            }
        )


class VersionManagerAction(BaseAction):
    """Manage API version definitions."""
    action_type = "api_version_manager"
    display_name = "版本管理器"
    description = "管理API版本定义"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        operation = params.get("operation", "register")
        version = params.get("version", "")
        status = params.get("status", "stable")
        deprecated = params.get("deprecated", False)
        changelog = params.get("changelog", "")

        if operation == "register":
            if not version:
                return ActionResult(success=False, message="version是必需的")

            api_version = APIVersion(
                version=version,
                status=status,
                deprecated=deprecated,
                changelog=changelog
            )
            _registry.register_version(api_version)

            return ActionResult(
                success=True,
                message=f"版本 {version} 已注册",
                data={"version": version, "status": status, "deprecated": deprecated}
            )

        if operation == "list":
            include_deprecated = params.get("include_deprecated", False)
            versions = _registry.list_versions(include_deprecated=include_deprecated)
            return ActionResult(
                success=True,
                message=f"共 {len(versions)} 个版本",
                data={"versions": [{"version": v.version, "status": v.status, "deprecated": v.deprecated} for v in versions]}
            )

        if operation == "deprecate":
            v = _registry.get_version(version)
            if not v:
                return ActionResult(success=False, message=f"版本 {version} 不存在")
            v.deprecated = True
            return ActionResult(success=True, message=f"版本 {version} 已标记为废弃")

        if operation == "set_default":
            if _registry.set_default(version):
                return ActionResult(success=True, message=f"默认版本设为 {version}")
            return ActionResult(success=False, message=f"版本 {version} 不存在")

        if operation == "add_route":
            path_pattern = params.get("path_pattern", "/")
            version_constraint = params.get("version_constraint", "")
            route = VersionRoute(path_pattern=path_pattern, version_constraint=version_constraint)
            _registry.add_route(route)
            return ActionResult(
                success=True,
                message=f"路由已添加: {path_pattern} -> {version_constraint}",
                data={"path_pattern": path_pattern, "version_constraint": version_constraint}
            )

        return ActionResult(success=False, message=f"未知操作: {operation}")
