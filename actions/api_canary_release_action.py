"""API Canary Release Action Module.

Provides canary release capabilities for gradually rolling out
API changes with traffic shifting and monitoring.
"""

import hashlib
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class ReleaseStage(Enum):
    """Canary release stages."""
    INITIAL = "initial"
    RAMPING = "ramping"
    STABLE = "stable"
    PROMOTED = "promoted"
    ROLLED_BACK = "rolled_back"
    PAUSED = "paused"


@dataclass
class VersionConfig:
    """Configuration for a service version."""
    version: str
    weight: int = 0  # Traffic weight (0-100)
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CanaryRelease:
    """Canary release state."""
    release_id: str
    service_name: str
    stable_version: str
    canary_version: str
    stage: ReleaseStage = ReleaseStage.INITIAL
    canary_weight: int = 0
    auto_promote: bool = False
    auto_promote_threshold: float = 99.0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    metrics: Dict[str, float] = field(default_factory=dict)


class APICanaryReleaseAction(BaseAction):
    """Canary release action for API traffic management.

    Manages gradual canary rollouts with traffic weight shifting,
    automatic promotion/rollback based on metrics.

    Args:
        context: Execution context.
        params: Dict with keys:
            - operation: Operation (create, update_weight, promote, rollback, route, status)
            - release: Release definition dict
            - release_id: ID of release to operate on
            - weight: New canary weight
            - request_context: Request context for routing decisions
    """
    action_type = "api_canary_release"
    display_name = "API金丝雀发布"
    description = "金丝雀发布与流量控制"

    def get_required_params(self) -> List[str]:
        return ["operation"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "release": None,
            "release_id": None,
            "weight": None,
            "request_context": None,
            "dataset_id": "default",
        }

    def __init__(self) -> None:
        super().__init__()
        self._releases: Dict[str, CanaryRelease] = {}
        self._routing_hooks: Dict[str, Callable] = {}

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute canary release operation."""
        start_time = time.time()

        operation = params.get("operation", "status")
        release = params.get("release")
        release_id = params.get("release_id")
        weight = params.get("weight")
        request_context = params.get("request_context", {})
        dataset_id = params.get("dataset_id", "default")

        if operation == "create":
            return self._create_release(release, start_time)
        elif operation == "update_weight":
            return self._update_weight(release_id, weight, start_time)
        elif operation == "promote":
            return self._promote_release(release_id, start_time)
        elif operation == "rollback":
            return self._rollback_release(release_id, start_time)
        elif operation == "route":
            return self._route_request(release_id, request_context, start_time)
        elif operation == "pause":
            return self._pause_release(release_id, start_time)
        elif operation == "resume":
            return self._resume_release(release_id, start_time)
        elif operation == "record_metric":
            return self._record_metric(release_id, params.get("metric_name"), params.get("metric_value"), start_time)
        elif operation == "status":
            return self._get_release_status(release_id, start_time)
        elif operation == "list":
            return self._list_releases(start_time)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}",
                duration=time.time() - start_time
            )

    def _create_release(self, release: Optional[Dict], start_time: float) -> ActionResult:
        """Create a new canary release."""
        if not release:
            return ActionResult(success=False, message="Release definition required", duration=time.time() - start_time)

        release_id = release.get("release_id") or release.get("id") or f"release_{int(time.time())}"
        service_name = release.get("service_name")
        stable_version = release.get("stable_version")
        canary_version = release.get("canary_version")

        if not all([service_name, stable_version, canary_version]):
            return ActionResult(success=False, message="service_name, stable_version, canary_version required", duration=time.time() - start_time)

        canary = CanaryRelease(
            release_id=release_id,
            service_name=service_name,
            stable_version=stable_version,
            canary_version=canary_version,
            stage=ReleaseStage.INITIAL,
            canary_weight=release.get("initial_weight", 0),
            auto_promote=release.get("auto_promote", False),
            auto_promote_threshold=release.get("auto_promote_threshold", 99.0),
        )

        self._releases[release_id] = canary

        return ActionResult(
            success=True,
            message=f"Canary release '{release_id}' created for '{service_name}'",
            data={
                "release_id": release_id,
                "service_name": service_name,
                "stable_version": stable_version,
                "canary_version": canary_version,
                "initial_weight": canary.canary_weight,
            },
            duration=time.time() - start_time
        )

    def _update_weight(self, release_id: Optional[str], weight: Optional[int], start_time: float) -> ActionResult:
        """Update canary traffic weight."""
        if not release_id or release_id not in self._releases:
            return ActionResult(success=False, message=f"Release '{release_id}' not found", duration=time.time() - start_time)
        if weight is None:
            return ActionResult(success=False, message="weight required", duration=time.time() - start_time)

        weight = max(0, min(100, weight))
        canary = self._releases[release_id]
        canary.canary_weight = weight
        canary.updated_at = time.time()

        if weight == 0:
            canary.stage = ReleaseStage.INITIAL
        elif weight < 100:
            canary.stage = ReleaseStage.RAMPING
        else:
            canary.stage = ReleaseStage.STABLE

        return ActionResult(
            success=True,
            message=f"Canary weight updated to {weight}% for '{release_id}'",
            data={
                "release_id": release_id,
                "weight": weight,
                "stage": canary.stage.value,
            },
            duration=time.time() - start_time
        )

    def _promote_release(self, release_id: Optional[str], start_time: float) -> ActionResult:
        """Promote canary to stable."""
        if not release_id or release_id not in self._releases:
            return ActionResult(success=False, message=f"Release '{release_id}' not found", duration=time.time() - start_time)

        canary = self._releases[release_id]
        canary.stage = ReleaseStage.PROMOTED
        canary.canary_weight = 100
        canary.stable_version = canary.canary_version
        canary.updated_at = time.time()

        return ActionResult(
            success=True,
            message=f"Canary '{release_id}' promoted to stable v{canary.stable_version}",
            data={
                "release_id": release_id,
                "new_stable_version": canary.stable_version,
                "stage": canary.stage.value,
            },
            duration=time.time() - start_time
        )

    def _rollback_release(self, release_id: Optional[str], start_time: float) -> ActionResult:
        """Rollback canary release."""
        if not release_id or release_id not in self._releases:
            return ActionResult(success=False, message=f"Release '{release_id}' not found", duration=time.time() - start_time)

        canary = self._releases[release_id]
        canary.stage = ReleaseStage.ROLLED_BACK
        canary.canary_weight = 0
        canary.updated_at = time.time()

        return ActionResult(
            success=True,
            message=f"Canary '{release_id}' rolled back",
            data={
                "release_id": release_id,
                "stage": canary.stage.value,
                "stable_version": canary.stable_version,
            },
            duration=time.time() - start_time
        )

    def _route_request(
        self,
        release_id: Optional[str],
        request_context: Dict,
        start_time: float
    ) -> ActionResult:
        """Route a request to stable or canary version."""
        if not release_id or release_id not in self._releases:
            return ActionResult(success=False, message=f"Release '{release_id}' not found", duration=time.time() - start_time)

        canary = self._releases[release_id]
        if canary.canary_weight == 0:
            version = canary.stable_version
            route_target = "stable"
        elif canary.canary_weight >= 100:
            version = canary.canary_version
            route_target = "canary"
        else:
            # Hash-based routing
            user_id = request_context.get("user_id") or request_context.get("request_id", "")
            hash_val = int(hashlib.md5(user_id.encode()).hexdigest(), 16) % 100
            if hash_val < canary.canary_weight:
                version = canary.canary_version
                route_target = "canary"
            else:
                version = canary.stable_version
                route_target = "stable"

        return ActionResult(
            success=True,
            message=f"Request routed to {route_target} ({version})",
            data={
                "release_id": release_id,
                "route_target": route_target,
                "version": version,
                "canary_weight": canary.canary_weight,
            },
            duration=time.time() - start_time
        )

    def _pause_release(self, release_id: Optional[str], start_time: float) -> ActionResult:
        """Pause a canary release."""
        if not release_id or release_id not in self._releases:
            return ActionResult(success=False, message=f"Release '{release_id}' not found", duration=time.time() - start_time)

        self._releases[release_id].stage = ReleaseStage.PAUSED
        return ActionResult(success=True, message=f"Canary '{release_id}' paused", duration=time.time() - start_time)

    def _resume_release(self, release_id: Optional[str], start_time: float) -> ActionResult:
        """Resume a paused canary release."""
        if not release_id or release_id not in self._releases:
            return ActionResult(success=False, message=f"Release '{release_id}' not found", duration=time.time() - start_time)

        canary = self._releases[release_id]
        if canary.canary_weight > 0:
            canary.stage = ReleaseStage.RAMPING if canary.canary_weight < 100 else ReleaseStage.STABLE
        else:
            canary.stage = ReleaseStage.INITIAL
        return ActionResult(success=True, message=f"Canary '{release_id}' resumed", data={"stage": canary.stage.value}, duration=time.time() - start_time)

    def _record_metric(self, release_id: Optional[str], metric_name: Optional[str], metric_value: Optional[float], start_time: float) -> ActionResult:
        """Record a metric for canary analysis."""
        if not release_id or release_id not in self._releases:
            return ActionResult(success=False, message=f"Release '{release_id}' not found", duration=time.time() - start_time)
        if not metric_name:
            return ActionResult(success=False, message="metric_name required", duration=time.time() - start_time)

        self._releases[release_id].metrics[metric_name] = metric_value or 0.0

        # Check auto-promotion
        canary = self._releases[release_id]
        if canary.auto_promote and canary.stage == ReleaseStage.RAMPING:
            if self._check_auto_promote(canary):
                canary.stage = ReleaseStage.PROMOTED
                canary.stable_version = canary.canary_version

        return ActionResult(
            success=True,
            message=f"Metric '{metric_name}' recorded for '{release_id}'",
            data={"release_id": release_id, "metric": metric_name, "value": metric_value, "all_metrics": canary.metrics},
            duration=time.time() - start_time
        )

    def _check_auto_promote(self, canary: CanaryRelease) -> bool:
        """Check if canary should auto-promote."""
        success_rate = canary.metrics.get("success_rate", 0)
        latency_p99 = canary.metrics.get("latency_p99_ms", float('inf'))
        return success_rate >= canary.auto_promote_threshold and latency_p99 < 1000

    def _get_release_status(self, release_id: Optional[str], start_time: float) -> ActionResult:
        """Get status of a release."""
        if not release_id or release_id not in self._releases:
            return ActionResult(success=False, message=f"Release '{release_id}' not found", duration=time.time() - start_time)

        canary = self._releases[release_id]
        return ActionResult(
            success=True,
            message=f"Canary release '{release_id}' status",
            data={
                "release_id": release_id,
                "service_name": canary.service_name,
                "stable_version": canary.stable_version,
                "canary_version": canary.canary_version,
                "stage": canary.stage.value,
                "canary_weight": canary.canary_weight,
                "metrics": canary.metrics,
                "created_at": canary.created_at,
                "updated_at": canary.updated_at,
            },
            duration=time.time() - start_time
        )

    def _list_releases(self, start_time: float) -> ActionResult:
        """List all releases."""
        releases = [
            {
                "release_id": r.release_id,
                "service_name": r.service_name,
                "stage": r.stage.value,
                "canary_weight": r.canary_weight,
            }
            for r in self._releases.values()
        ]
        return ActionResult(
            success=True,
            message=f"{len(releases)} releases tracked",
            data={"releases": releases, "total": len(releases)},
            duration=time.time() - start_time
        )
