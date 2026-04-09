"""API deployment strategy action module for RabAI AutoClick.

Provides API deployment strategies:
- BlueGreenDeploymentAction: Blue-green deployment switching
- CanaryDeploymentAction: Canary release with gradual traffic shifting
- RollingDeploymentAction: Rolling update across instances
- ShadowDeploymentAction: Shadow mode traffic duplication
- FeatureFlagAction: Feature flag management for API rollouts
"""

from __future__ import annotations

import hashlib
import hmac
import json
import random
import time
import uuid
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Set

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DeploymentStrategy(Enum):
    """Deployment strategy types."""
    BLUE_GREEN = auto()
    CANARY = auto()
    ROLLING = auto()
    SHADOW = auto()
    RECreate = auto()


class DeploymentState(Enum):
    """Deployment state."""
    IDLE = auto()
    IN_PROGRESS = auto()
    PAUSED = auto()
    COMPLETED = auto()
    FAILED = auto()
    ROLLED_BACK = auto()


class BlueGreenDeploymentAction(BaseAction):
    """Blue-green deployment switching."""
    action_type = "blue_green_deployment"
    display_name = "蓝绿部署"
    description = "蓝绿部署环境切换"

    def __init__(self) -> None:
        super().__init__()
        self._environments: Dict[str, Dict[str, Any]] = {}
        self._active: str = ""
        self._history: List[Dict[str, Any]] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "setup":
                return self._setup(params)
            elif action == "switch":
                return self._switch(params)
            elif action == "rollback":
                return self._rollback(params)
            elif action == "status":
                return self._status()
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Blue-green deployment failed: {e}")

    def _setup(self, params: Dict[str, Any]) -> ActionResult:
        blue_url = params.get("blue_url", "")
        green_url = params.get("green_url", "")
        if not blue_url or not green_url:
            return ActionResult(success=False, message="blue_url and green_url are required")
        self._environments["blue"] = {"url": blue_url, "version": params.get("blue_version", "v1"), "active": False}
        self._environments["green"] = {"url": green_url, "version": params.get("green_version", "v2"), "active": False}
        self._active = params.get("initial", "blue")
        self._environments[self._active]["active"] = True
        return ActionResult(success=True, message=f"Blue-green environment setup complete, active: {self._active}")

    def _switch(self, params: Dict[str, Any]) -> ActionResult:
        target = params.get("target", "")
        health_check = params.get("health_check", True)
        if target not in self._environments:
            return ActionResult(success=False, message=f"Unknown environment: {target}")
        if not self._environments.get(target, {}).get("url"):
            return ActionResult(success=False, message=f"Environment not configured: {target}")
        old_active = self._active
        self._environments[self._active]["active"] = False
        self._environments[target]["active"] = True
        self._active = target
        self._history.append({
            "switch_at": datetime.now(timezone.utc).isoformat(),
            "from_env": old_active,
            "to_env": target,
            "health_check_passed": health_check,
        })
        return ActionResult(
            success=True,
            message=f"Switched from {old_active} to {target}",
            data={"active_env": self._active, "switch_record": self._history[-1]},
        )

    def _rollback(self, params: Dict[str, Any]) -> ActionResult:
        if len(self._history) < 2:
            return ActionResult(success=False, message="No previous environment to rollback to")
        prev = self._history[-2]
        target = prev["from_env"]
        self._environments[self._active]["active"] = False
        self._environments[target]["active"] = True
        self._active = target
        self._history.append({
            "rolled_back_at": datetime.now(timezone.utc).isoformat(),
            "to_env": target,
            "reason": params.get("reason", "manual rollback"),
        })
        return ActionResult(success=True, message=f"Rolled back to {target}", data={"active_env": self._active})

    def _status(self) -> ActionResult:
        return ActionResult(
            success=True,
            message="Blue-green status",
            data={
                "active_env": self._active,
                "environments": self._environments,
                "switch_history": self._history,
            },
        )


class CanaryDeploymentAction(BaseAction):
    """Canary release with gradual traffic shifting."""
    action_type = "canary_deployment"
    display_name = "金丝雀部署"
    description = "金丝雀发布逐步流量切换"

    def __init__(self) -> None:
        super().__init__()
        self._deployments: Dict[str, Dict[str, Any]] = {}
        self._metrics: Dict[str, List[Dict[str, Any]]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            deployment_id = params.get("deployment_id", str(uuid.uuid4()))
            action = params.get("action", "start")
            traffic_percent = params.get("traffic_percent", 10)

            if action == "start":
                return self._start_canary(params)
            elif action == "promote":
                return self._promote_canary(params)
            elif action == "rollback":
                return self._rollback_canary(params)
            elif action == "adjust_traffic":
                return self._adjust_traffic(params)
            elif action == "status":
                return self._status(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Canary deployment failed: {e}")

    def _start_canary(self, params: Dict[str, Any]) -> ActionResult:
        stable_url = params.get("stable_url", "")
        canary_url = params.get("canary_url", "")
        if not stable_url or not canary_url:
            return ActionResult(success=False, message="stable_url and canary_url are required")
        deployment_id = str(uuid.uuid4())
        self._deployments[deployment_id] = {
            "stable_url": stable_url,
            "canary_url": canary_url,
            "traffic_percent": params.get("initial_traffic", 10),
            "state": DeploymentState.IN_PROGRESS.name,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "metrics": [],
        }
        return ActionResult(
            success=True,
            message=f"Canary deployment {deployment_id[:8]} started",
            data={"deployment_id": deployment_id, "traffic_percent": 10},
        )

    def _promote_canary(self, params: Dict[str, Any]) -> ActionResult:
        deployment_id = params.get("deployment_id", "")
        if deployment_id not in self._deployments:
            return ActionResult(success=False, message="Deployment not found")
        self._deployments[deployment_id]["state"] = DeploymentState.COMPLETED.name
        self._deployments[deployment_id]["traffic_percent"] = 100
        return ActionResult(
            success=True,
            message="Canary promoted to 100%",
            data={"deployment_id": deployment_id, "traffic_percent": 100},
        )

    def _rollback_canary(self, params: Dict[str, Any]) -> ActionResult:
        deployment_id = params.get("deployment_id", "")
        if deployment_id not in self._deployments:
            return ActionResult(success=False, message="Deployment not found")
        self._deployments[deployment_id]["state"] = DeploymentState.ROLLED_BACK.name
        self._deployments[deployment_id]["traffic_percent"] = 0
        return ActionResult(success=True, message="Canary rolled back", data={"deployment_id": deployment_id})

    def _adjust_traffic(self, params: Dict[str, Any]) -> ActionResult:
        deployment_id = params.get("deployment_id", "")
        traffic_percent = params.get("traffic_percent", 10)
        if deployment_id not in self._deployments:
            return ActionResult(success=False, message="Deployment not found")
        if not (0 <= traffic_percent <= 100):
            return ActionResult(success=False, message="traffic_percent must be 0-100")
        self._deployments[deployment_id]["traffic_percent"] = traffic_percent
        return ActionResult(
            success=True,
            message=f"Canary traffic adjusted to {traffic_percent}%",
            data={"deployment_id": deployment_id, "traffic_percent": traffic_percent},
        )

    def _status(self, params: Dict[str, Any]) -> ActionResult:
        deployment_id = params.get("deployment_id", "")
        if deployment_id and deployment_id in self._deployments:
            return ActionResult(success=True, message="Deployment status", data=self._deployments[deployment_id])
        return ActionResult(success=True, message="All deployments", data={"deployments": self._deployments})


class RollingDeploymentAction(BaseAction):
    """Rolling update across instances."""
    action_type = "rolling_deployment"
    display_name = "滚动部署"
    description = "跨实例滚动更新"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            instances = params.get("instances", [])
            new_version = params.get("new_version", "")
            batch_size = params.get("batch_size", 1)
            pause_between_batches = params.get("pause_seconds", 10)

            if not instances:
                return ActionResult(success=False, message="instances are required")
            if not new_version:
                return ActionResult(success=False, message="new_version is required")

            updated: List[str] = []
            paused: List[str] = []
            total = len(instances)

            for i in range(0, total, batch_size):
                batch = instances[i : i + batch_size]
                for instance in batch:
                    updated.append(instance)
                paused.append(f"batch_{i // batch_size + 1}")

            return ActionResult(
                success=True,
                message=f"Rolling deployment of v{new_version} complete",
                data={
                    "new_version": new_version,
                    "total_instances": total,
                    "updated_count": len(updated),
                    "batch_count": len(paused),
                    "updated_instances": updated,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Rolling deployment failed: {e}")


class ShadowDeploymentAction(BaseAction):
    """Shadow mode traffic duplication for testing."""
    action_type = "shadow_deployment"
    display_name = "影子部署"
    description = "影子模式流量复制测试"

    def __init__(self) -> None:
        super().__init__()
        self._shadow_sessions: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "start")
            if action == "start":
                return self._start_shadow(params)
            elif action == "stop":
                return self._stop_shadow(params)
            elif action == "status":
                return self._status(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Shadow deployment failed: {e}")

    def _start_shadow(self, params: Dict[str, Any]) -> ActionResult:
        production_url = params.get("production_url", "")
        shadow_url = params.get("shadow_url", "")
        sampling_rate = params.get("sampling_rate", 1.0)
        if not production_url or not shadow_url:
            return ActionResult(success=False, message="production_url and shadow_url are required")
        session_id = str(uuid.uuid4())
        self._shadow_sessions[session_id] = {
            "production_url": production_url,
            "shadow_url": shadow_url,
            "sampling_rate": sampling_rate,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "requests_duplicated": 0,
            "errors_detected": 0,
        }
        return ActionResult(success=True, message=f"Shadow deployment started: {session_id[:8]}", data={"session_id": session_id})

    def _stop_shadow(self, params: Dict[str, Any]) -> ActionResult:
        session_id = params.get("session_id", "")
        if session_id in self._shadow_sessions:
            self._shadow_sessions[session_id]["stopped_at"] = datetime.now(timezone.utc).isoformat()
            return ActionResult(success=True, message=f"Shadow session {session_id[:8]} stopped")
        return ActionResult(success=False, message="Shadow session not found")

    def _status(self, params: Dict[str, Any]) -> ActionResult:
        return ActionResult(success=True, message="Shadow deployments", data={"sessions": self._shadow_sessions})


class FeatureFlagAction(BaseAction):
    """Feature flag management for API rollouts."""
    action_type = "feature_flag"
    display_name = "特性开关"
    description = "API特性开关管理"

    def __init__(self) -> None:
        super().__init__()
        self._flags: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            flag_name = params.get("flag_name", "")
            if action == "create":
                return self._create_flag(params)
            elif action == "toggle":
                return self._toggle_flag(params)
            elif action == "evaluate":
                return self._evaluate_flag(params)
            elif action == "list":
                return self._list_flags()
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Feature flag failed: {e}")

    def _create_flag(self, params: Dict[str, Any]) -> ActionResult:
        flag_name = params.get("flag_name", "")
        enabled = params.get("enabled", False)
        rollout_percent = params.get("rollout_percent", 0)
        conditions = params.get("conditions", [])
        if not flag_name:
            return ActionResult(success=False, message="flag_name is required")
        self._flags[flag_name] = {
            "enabled": enabled,
            "rollout_percent": rollout_percent,
            "conditions": conditions,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        return ActionResult(success=True, message=f"Flag '{flag_name}' created", data=self._flags[flag_name])

    def _toggle_flag(self, params: Dict[str, Any]) -> ActionResult:
        flag_name = params.get("flag_name", "")
        enabled = params.get("enabled", True)
        if flag_name not in self._flags:
            return ActionResult(success=False, message=f"Flag not found: {flag_name}")
        self._flags[flag_name]["enabled"] = enabled
        return ActionResult(success=True, message=f"Flag '{flag_name}' {'enabled' if enabled else 'disabled'}")

    def _evaluate_flag(self, params: Dict[str, Any]) -> ActionResult:
        flag_name = params.get("flag_name", "")
        user_id = params.get("user_id", "")
        context = params.get("context", {})
        if flag_name not in self._flags:
            return ActionResult(success=False, message=f"Flag not found: {flag_name}")
        flag = self._flags[flag_name]
        if not flag.get("enabled"):
            return ActionResult(success=True, message="Flag is disabled", data={"enabled": False})
        rollout = flag.get("rollout_percent", 0)
        if rollout >= 100:
            return ActionResult(success=True, message="Flag fully rolled out", data={"enabled": True})
        if rollout <= 0:
            return ActionResult(success=True, message="Flag not rolled out", data={"enabled": False})
        flag_hash = int(hashlib.md5(f"{flag_name}:{user_id}".encode()).hexdigest(), 16) % 100
        enabled = flag_hash < rollout
        return ActionResult(success=True, message=f"Flag evaluated: {enabled}", data={"enabled": enabled, "user_id": user_id})

    def _list_flags(self) -> ActionResult:
        return ActionResult(success=True, message=f"{len(self._flags)} flags", data={"flags": self._flags})
