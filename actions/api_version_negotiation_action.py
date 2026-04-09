"""API version negotiation action module for RabAI AutoClick.

Provides API version negotiation:
- VersionNegotiatorAction: Negotiate best API version between client/server
- VersionRouterAction: Route requests to appropriate version handlers
- VersionDeprecationPlannerAction: Plan version deprecation timelines
- VersionCapabilityResolverAction: Resolve capability differences across versions
- VersionUpgradeGuideAction: Generate upgrade guides between versions
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta, timezone
from functools import cmp_to_key
from typing import Any, Dict, List, Optional, Set, Tuple

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class APIVersion:
    """Represents a semver-style API version."""

    def __init__(self, version_str: str) -> None:
        self.original = version_str
        self.major, self.minor, self.patch = self._parse(version_str)

    def _parse(self, v: str) -> Tuple[int, int, int]:
        v = v.lstrip("v")
        parts = re.split(r"[-.+]", v)
        try:
            return int(parts[0]), int(parts[1]) if len(parts) > 1 else 0, int(parts[2]) if len(parts) > 2 else 0
        except (ValueError, IndexError):
            return 0, 0, 0

    def __repr__(self) -> str:
        return f"v{self.major}.{self.minor}.{self.patch}"

    def __lt__(self, other: "APIVersion") -> bool:
        return (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, APIVersion):
            return False
        return self.major == other.major and self.minor == other.minor and self.patch == other.patch

    def __hash__(self) -> int:
        return hash((self.major, self.minor, self.patch))


class VersionNegotiatorAction(BaseAction):
    """Negotiate best API version between client and server."""
    action_type = "version_negotiator"
    display_name = "版本协商"
    description = "客户端与服务器之间协商最佳API版本"

    def __init__(self) -> None:
        super().__init__()
        self._supported_versions: Set[APIVersion] = set()
        self._negotiation_history: List[Dict[str, Any]] = []

    def register_versions(self, versions: List[str]) -> None:
        """Register supported API versions."""
        for v in versions:
            self._supported_versions.add(APIVersion(v))

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            client_versions = params.get("client_versions", [])
            server_versions = params.get("server_versions", [])
            strategy = params.get("strategy", "newest")
            if not server_versions:
                return ActionResult(success=False, message="server_versions are required")

            self.register_versions(server_versions)
            negotiated = self._negotiate(client_versions, server_versions, strategy)

            record = {
                "id": str(uuid.uuid4()),
                "client_versions": client_versions,
                "server_versions": server_versions,
                "strategy": strategy,
                "negotiated_version": str(negotiated) if negotiated else None,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            self._negotiation_history.append(record)
            return ActionResult(
                success=negotiated is not None,
                message=f"Negotiated version: {negotiated}",
                data=record,
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Version negotiation failed: {e}")

    def _negotiate(self, client_versions: List[str], server_versions: List[str], strategy: str) -> Optional[APIVersion]:
        server_ver_objs = [APIVersion(v) for v in server_versions]
        if strategy == "newest":
            server_ver_objs.sort(reverse=True)
            return server_ver_objs[0] if server_ver_objs else None
        elif strategy == "oldest":
            server_ver_objs.sort()
            return server_ver_objs[0] if server_ver_objs else None
        elif strategy == "major_compatible" and client_versions:
            client_major = APIVersion(client_versions[0]).major
            compatible = [v for v in server_ver_objs if v.major == client_major]
            if compatible:
                compatible.sort(reverse=True)
                return compatible[0]
            return server_ver_objs[-1] if server_ver_objs else None
        return server_versions[0] if server_versions else None


class VersionRouterAction(BaseAction):
    """Route requests to appropriate version handlers."""
    action_type = "version_router"
    display_name = "版本路由"
    description = "将请求路由到适当的版本处理器"

    def __init__(self) -> None:
        super().__init__()
        self._routes: Dict[str, Dict[str, str]] = {}
        self._default_version: Optional[str] = None

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "route")
            if action == "add_route":
                return self._add_route(params)
            elif action == "route":
                return self._route(params)
            elif action == "list":
                return self._list_routes()
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Version routing failed: {e}")

    def _add_route(self, params: Dict[str, Any]) -> ActionResult:
        path = params.get("path", "")
        method = params.get("method", "GET")
        version = params.get("version", "")
        handler = params.get("handler", "")
        if not path or not version:
            return ActionResult(success=False, message="path and version are required")
        key = f"{method.upper()}:{path}"
        self._routes.setdefault(key, {})[version] = handler
        return ActionResult(success=True, message=f"Route added: {method} {path} -> v{version}")

    def _route(self, params: Dict[str, Any]) -> ActionResult:
        path = params.get("path", "")
        method = params.get("method", "GET")
        version = params.get("version", "")
        key = f"{method.upper()}:{path}"
        if key not in self._routes:
            return ActionResult(success=False, message=f"No route found: {key}")
        routes = self._routes[key]
        handler = routes.get(version, routes.get(self._default_version or "", ""))
        if not handler:
            return ActionResult(success=False, message=f"No handler for version: {version}")
        return ActionResult(success=True, message=f"Routed to v{version}", data={"handler": handler, "version": version})

    def _list_routes(self) -> ActionResult:
        return ActionResult(success=True, message=f"{len(self._routes)} routes", data={"routes": self._routes})


class VersionDeprecationPlannerAction(BaseAction):
    """Plan version deprecation timelines."""
    action_type = "version_deprecation_planner"
    display_name = "版本废弃规划"
    description = "规划API版本废弃时间表"

    def __init__(self) -> None:
        super().__init__()
        self._deprecation_plans: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "plan")
            if action == "plan":
                return self._create_plan(params)
            elif action == "list":
                return self._list_plans()
            elif action == "check":
                return self._check_status(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Deprecation planning failed: {e}")

    def _create_plan(self, params: Dict[str, Any]) -> ActionResult:
        version = params.get("version", "")
        sunset_date = params.get("sunset_date", "")
        migration_guide = params.get("migration_guide", "")
        if not version or not sunset_date:
            return ActionResult(success=False, message="version and sunset_date are required")

        plan = {
            "version": version,
            "sunset_date": sunset_date,
            "migration_guide": migration_guide,
            "phases": params.get("phases", []),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "status": "active",
        }
        self._deprecation_plans[version] = plan
        return ActionResult(success=True, message=f"Deprecation plan created for v{version}", data=plan)

    def _list_plans(self) -> ActionResult:
        return ActionResult(success=True, message=f"{len(self._deprecation_plans)} plans", data={"plans": self._deprecation_plans})

    def _check_status(self, params: Dict[str, Any]) -> ActionResult:
        version = params.get("version", "")
        if version not in self._deprecation_plans:
            return ActionResult(success=False, message=f"No plan found for v{version}")
        plan = self._deprecation_plans[version]
        now = datetime.now(timezone.utc)
        sunset = datetime.fromisoformat(plan["sunset_date"].replace("Z", "+00:00"))
        days_remaining = (sunset - now).days
        return ActionResult(
            success=True,
            message=f"v{version}: {days_remaining} days until sunset",
            data={"version": version, "days_remaining": days_remaining, "status": plan["status"]},
        )


class VersionCapabilityResolverAction(BaseAction):
    """Resolve capability differences across API versions."""
    action_type = "version_capability_resolver"
    display_name = "版本能力解析"
    description = "解析不同API版本间的功能差异"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            from_version = params.get("from_version", "")
            to_version = params.get("to_version", "")
            capabilities_from = params.get("capabilities_from", {})
            capabilities_to = params.get("capabilities_to", {})
            if not from_version or not to_version:
                return ActionResult(success=False, message="from_version and to_version are required")

            added = set(capabilities_to.keys()) - set(capabilities_from.keys())
            removed = set(capabilities_from.keys()) - set(capabilities_to.keys())
            unchanged = set(capabilities_from.keys()) & set(capabilities_to.keys())

            return ActionResult(
                success=True,
                message=f"Capabilities: {len(added)} added, {len(removed)} removed, {len(unchanged)} unchanged",
                data={
                    "from_version": from_version,
                    "to_version": to_version,
                    "added": list(added),
                    "removed": list(removed),
                    "unchanged": list(unchanged),
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Capability resolution failed: {e}")


class VersionUpgradeGuideAction(BaseAction):
    """Generate upgrade guides between API versions."""
    action_type = "version_upgrade_guide"
    display_name = "版本升级指南"
    description = "生成API版本间升级指南"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            from_version = params.get("from_version", "")
            to_version = params.get("to_version", "")
            breaking_changes = params.get("breaking_changes", [])
            migration_steps = params.get("migration_steps", [])
            if not from_version or not to_version:
                return ActionResult(success=False, message="from_version and to_version are required")

            guide = {
                "id": str(uuid.uuid4()),
                "title": f"Upgrade Guide: v{from_version} → v{to_version}",
                "from_version": from_version,
                "to_version": to_version,
                "breaking_changes": breaking_changes,
                "migration_steps": [
                    {"step": i + 1, "description": step} for i, step in enumerate(migration_steps)
                ],
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
            return ActionResult(success=True, message=f"Upgrade guide generated v{from_version}→v{to_version}", data=guide)
        except Exception as e:
            return ActionResult(success=False, message=f"Upgrade guide generation failed: {e}")
