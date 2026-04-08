"""API versioning action module for RabAI AutoClick.

Provides API versioning operations:
- VersionCreateAction: Create API version
- VersionSwitchAction: Switch API version
- VersionDeprecateAction: Deprecate version
- VersionCompareAction: Compare versions
- VersionListAction: List versions
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class VersionCreateAction(BaseAction):
    """Create an API version."""
    action_type = "version_create"
    display_name = "创建版本"
    description = "创建API版本"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            api_name = params.get("api_name", "")
            version = params.get("version", "1.0.0")
            changelog = params.get("changelog", "")
            is_stable = params.get("is_stable", True)

            if not api_name or not version:
                return ActionResult(success=False, message="api_name and version are required")

            version_id = f"{api_name}:{version}"

            if not hasattr(context, "api_versions"):
                context.api_versions = {}
            context.api_versions[version_id] = {
                "version_id": version_id,
                "api_name": api_name,
                "version": version,
                "changelog": changelog,
                "is_stable": is_stable,
                "status": "active",
                "created_at": time.time(),
            }

            return ActionResult(
                success=True,
                data={"version_id": version_id, "api_name": api_name, "version": version},
                message=f"Version {version} created for {api_name}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Version create failed: {e}")


class VersionSwitchAction(BaseAction):
    """Switch to a different API version."""
    action_type = "version_switch"
    display_name = "切换版本"
    description = "切换API版本"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            api_name = params.get("api_name", "")
            target_version = params.get("target_version", "")
            graceful = params.get("graceful", True)

            if not api_name or not target_version:
                return ActionResult(success=False, message="api_name and target_version are required")

            version_id = f"{api_name}:{target_version}"
            versions = getattr(context, "api_versions", {})

            if version_id not in versions:
                return ActionResult(success=False, message=f"Version {version_id} not found")

            if not hasattr(context, "active_api_version"):
                context.active_api_version = {}
            context.active_api_version[api_name] = target_version

            return ActionResult(
                success=True,
                data={"api_name": api_name, "target_version": target_version, "graceful": graceful},
                message=f"Switched {api_name} to version {target_version}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Version switch failed: {e}")


class VersionDeprecateAction(BaseAction):
    """Deprecate an API version."""
    action_type = "version_deprecate"
    display_name = "废弃版本"
    description = "废弃API版本"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            version_id = params.get("version_id", "")
            sunset_date = params.get("sunset_date", "")
            migration_guide = params.get("migration_guide", "")

            if not version_id:
                return ActionResult(success=False, message="version_id is required")

            versions = getattr(context, "api_versions", {})
            if version_id not in versions:
                return ActionResult(success=False, message=f"Version {version_id} not found")

            versions[version_id]["status"] = "deprecated"
            versions[version_id]["sunset_date"] = sunset_date
            versions[version_id]["migration_guide"] = migration_guide
            versions[version_id]["deprecated_at"] = time.time()

            return ActionResult(
                success=True,
                data={"version_id": version_id, "sunset_date": sunset_date},
                message=f"Version {version_id} deprecated. Sunset: {sunset_date}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Version deprecate failed: {e}")


class VersionCompareAction(BaseAction):
    """Compare two API versions."""
    action_type = "version_compare"
    display_name = "版本对比"
    description = "对比两个API版本"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            version_a = params.get("version_a", "")
            version_b = params.get("version_b", "")

            if not version_a or not version_b:
                return ActionResult(success=False, message="version_a and version_b are required")

            def parse_version(v):
                return [int(x) for x in v.lstrip("v").split(".")]

            v_a = parse_version(version_a)
            v_b = parse_version(version_b)
            max_len = max(len(v_a), len(v_b))
            v_a.extend([0] * (max_len - len(v_a)))
            v_b.extend([0] * (max_len - len(v_b)))

            if v_a > v_b:
                relation = "greater"
            elif v_a < v_b:
                relation = "lesser"
            else:
                relation = "equal"

            return ActionResult(
                success=True,
                data={"version_a": version_a, "version_b": version_b, "relation": relation},
                message=f"{version_a} is {relation} than {version_b}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Version compare failed: {e}")


class VersionListAction(BaseAction):
    """List all versions for an API."""
    action_type = "version_list"
    display_name = "版本列表"
    description = "列出API版本"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            api_name = params.get("api_name", "")
            include_deprecated = params.get("include_deprecated", True)

            versions = getattr(context, "api_versions", {})
            if api_name:
                filtered = {k: v for k, v in versions.items() if v["api_name"] == api_name}
            else:
                filtered = versions

            if not include_deprecated:
                filtered = {k: v for k, v in filtered.items() if v.get("status") != "deprecated"}

            return ActionResult(
                success=True,
                data={"versions": list(filtered.values()), "count": len(filtered)},
                message=f"Found {len(filtered)} versions",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Version list failed: {e}")
