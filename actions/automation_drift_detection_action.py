"""Automation Drift Detection Action.

Detects configuration drift in automation systems by comparing
actual state against desired state definitions.
"""
from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import hashlib
import json


class DriftStatus(Enum):
    IN_SYNC = "in_sync"
    DRIFTED = "drifted"
    UNKNOWN = "unknown"


@dataclass
class ResourceState:
    resource_id: str
    resource_type: str
    attributes: Dict[str, Any]
    last_checked: datetime
    checksum: str = ""

    def compute_checksum(self) -> str:
        state_str = json.dumps(self.attributes, sort_keys=True)
        return hashlib.sha256(state_str.encode()).hexdigest()[:16]


@dataclass
class DesiredState:
    resource_id: str
    resource_type: str
    expected_attributes: Dict[str, Any]
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class DriftReport:
    resource_id: str
    status: DriftStatus
    drifted_attributes: Dict[str, Any] = field(default_factory=dict)
    missing_attributes: Dict[str, Any] = field(default_factory=dict)
    unexpected_attributes: Dict[str, Any] = field(default_factory=dict)
    detected_at: datetime = field(default_factory=datetime.now)


class AutomationDriftDetectionAction:
    """Detects configuration drift in automation state."""

    def __init__(self) -> None:
        self._desired_states: Dict[str, DesiredState] = {}
        self._actual_states: Dict[str, ResourceState] = {}
        self._history: List[DriftReport] = []

    def register_desired_state(self, state: DesiredState) -> None:
        self._desired_states[state.resource_id] = state

    def register_actual_state(self, state: ResourceState) -> None:
        state.checksum = state.compute_checksum()
        self._actual_states[state.resource_id] = state

    def detect_drift(self, resource_id: str) -> DriftReport:
        desired = self._desired_states.get(resource_id)
        actual = self._actual_states.get(resource_id)
        if not desired:
            return DriftReport(
                resource_id=resource_id,
                status=DriftStatus.UNKNOWN,
            )
        if not actual:
            return DriftReport(
                resource_id=resource_id,
                status=DriftStatus.DRIFTED,
                missing_attributes=desired.expected_attributes,
            )
        drifted: Dict[str, Any] = {}
        missing: Dict[str, Any] = {}
        unexpected: Dict[str, Any] = {}
        for key, expected_val in desired.expected_attributes.items():
            if key not in actual.attributes:
                missing[key] = expected_val
            elif actual.attributes[key] != expected_val:
                drifted[key] = {
                    "expected": expected_val,
                    "actual": actual.attributes[key],
                }
        for key in actual.attributes:
            if key not in desired.expected_attributes:
                unexpected[key] = actual.attributes[key]
        is_drifted = bool(drifted or missing or unexpected)
        report = DriftReport(
            resource_id=resource_id,
            status=DriftStatus.DRIFTED if is_drifted else DriftStatus.IN_SYNC,
            drifted_attributes=drifted,
            missing_attributes=missing,
            unexpected_attributes=unexpected,
        )
        self._history.append(report)
        return report

    def detect_all(self) -> List[DriftReport]:
        reports = []
        all_resource_ids = set(self._desired_states.keys()) | set(
            self._actual_states.keys()
        )
        for rid in all_resource_ids:
            reports.append(self.detect_drift(rid))
        return reports

    def summary(self) -> Dict[str, Any]:
        reports = self.detect_all()
        return {
            "total_resources": len(reports),
            "in_sync": sum(1 for r in reports if r.status == DriftStatus.IN_SYNC),
            "drifted": sum(1 for r in reports if r.status == DriftStatus.DRIFTED),
            "unknown": sum(1 for r in reports if r.status == DriftStatus.UNKNOWN),
            "drift_rate": sum(1 for r in reports if r.status == DriftStatus.DRIFTED)
            / len(reports)
            if reports
            else 0.0,
        }
