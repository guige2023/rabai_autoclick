"""Data Freshness Action Module.

Provides data freshness monitoring and SLA tracking
for data pipelines and datasets.
"""

import time
import hashlib
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class FreshnessStatus(Enum):
    """Data freshness status."""
    FRESH = "fresh"
    STALE = "stale"
    EXPIRED = "expired"
    UNKNOWN = "unknown"


class SLAPriority(Enum):
    """SLA priority level."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class DataFreshnessRule:
    """Rule for data freshness."""
    rule_id: str
    dataset_name: str
    max_age_seconds: float
    check_interval_seconds: float = 300.0
    alert_threshold_seconds: Optional[float] = None
    priority: SLAPriority = SLAPriority.MEDIUM
    enabled: bool = True
    last_check: Optional[float] = None
    last_update: Optional[float] = None


@dataclass
class FreshnessCheck:
    """Result of a freshness check."""
    rule_id: str
    dataset_name: str
    status: FreshnessStatus
    age_seconds: float
    max_age_seconds: float
    timestamp: float = field(default_factory=time.time)
    passed: bool = True
    message: Optional[str] = None


@dataclass
class SLAMetric:
    """SLA compliance metric."""
    dataset_name: str
    total_checks: int = 0
    passed_checks: int = 0
    failed_checks: int = 0
    average_age_seconds: float = 0.0
    current_status: FreshnessStatus = FreshnessStatus.UNKNOWN


class DataFreshnessMonitor:
    """Monitors data freshness across datasets."""

    def __init__(self):
        self._rules: Dict[str, DataFreshnessRule] = {}
        self._check_history: List[FreshnessCheck] = []
        self._sla_metrics: Dict[str, SLAMetric] = {}
        self._alert_callbacks: List[Callable] = []

    def add_rule(
        self,
        dataset_name: str,
        max_age_seconds: float,
        check_interval_seconds: float = 300.0,
        alert_threshold_seconds: Optional[float] = None,
        priority: SLAPriority = SLAPriority.MEDIUM
    ) -> str:
        """Add a freshness rule."""
        rule_id = hashlib.md5(
            f"{dataset_name}{time.time()}".encode()
        ).hexdigest()[:8]

        rule = DataFreshnessRule(
            rule_id=rule_id,
            dataset_name=dataset_name,
            max_age_seconds=max_age_seconds,
            check_interval_seconds=check_interval_seconds,
            alert_threshold_seconds=alert_threshold_seconds,
            priority=priority
        )

        self._rules[rule_id] = rule
        self._init_sla_metric(dataset_name)

        return rule_id

    def update_rule(
        self,
        rule_id: str,
        max_age_seconds: Optional[float] = None,
        enabled: Optional[bool] = None
    ) -> bool:
        """Update a freshness rule."""
        if rule_id not in self._rules:
            return False

        rule = self._rules[rule_id]

        if max_age_seconds is not None:
            rule.max_age_seconds = max_age_seconds

        if enabled is not None:
            rule.enabled = enabled

        return True

    def remove_rule(self, rule_id: str) -> bool:
        """Remove a freshness rule."""
        if rule_id in self._rules:
            del self._rules[rule_id]
            return True
        return False

    def get_rule(self, rule_id: str) -> Optional[DataFreshnessRule]:
        """Get a rule by ID."""
        return self._rules.get(rule_id)

    def get_rules_for_dataset(
        self,
        dataset_name: str
    ) -> List[DataFreshnessRule]:
        """Get all rules for a dataset."""
        return [
            r for r in self._rules.values()
            if r.dataset_name == dataset_name
        ]

    def record_data_update(
        self,
        dataset_name: str,
        timestamp: Optional[float] = None
    ) -> None:
        """Record a data update for a dataset."""
        timestamp = timestamp or time.time()

        for rule in self._rules.values():
            if rule.dataset_name == dataset_name:
                rule.last_update = timestamp

        self._update_sla_metric(dataset_name, timestamp)

    def check_freshness(
        self,
        rule_id: str,
        current_time: Optional[float] = None
    ) -> Optional[FreshnessCheck]:
        """Check freshness for a rule."""
        rule = self._rules.get(rule_id)
        if not rule:
            return None

        current_time = current_time or time.time()
        rule.last_check = current_time

        if rule.last_update is None:
            status = FreshnessStatus.UNKNOWN
            age_seconds = 0.0
            message = "No update recorded"
            passed = False

        else:
            age_seconds = current_time - rule.last_update

            if age_seconds <= rule.max_age_seconds:
                status = FreshnessStatus.FRESH
                passed = True
                message = "Data is fresh"
            elif rule.alert_threshold_seconds and age_seconds >= rule.alert_threshold_seconds:
                status = FreshnessStatus.EXPIRED
                passed = False
                message = f"Data expired after {age_seconds:.0f}s"
            else:
                status = FreshnessStatus.STALE
                passed = False
                message = f"Data is stale ({age_seconds:.0f}s old)"

        check = FreshnessCheck(
            rule_id=rule_id,
            dataset_name=rule.dataset_name,
            status=status,
            age_seconds=age_seconds,
            max_age_seconds=rule.max_age_seconds,
            passed=passed,
            message=message
        )

        self._check_history.append(check)
        self._update_sla_metric(rule.dataset_name, current_time, passed)

        if not passed and rule.alert_threshold_seconds:
            self._trigger_alerts(check, rule)

        if len(self._check_history) > 10000:
            self._check_history = self._check_history[-5000:]

        return check

    def check_all_freshness(
        self,
        current_time: Optional[float] = None
    ) -> List[FreshnessCheck]:
        """Check freshness for all enabled rules."""
        current_time = current_time or time.time()
        results = []

        for rule in self._rules.values():
            if not rule.enabled:
                continue

            result = self.check_freshness(rule.rule_id, current_time)
            if result:
                results.append(result)

        return results

    def _update_sla_metric(
        self,
        dataset_name: str,
        timestamp: float,
        passed: Optional[bool] = None
    ) -> None:
        """Update SLA metrics for dataset."""
        if dataset_name not in self._sla_metrics:
            self._init_sla_metric(dataset_name)

        metric = self._sla_metrics[dataset_name]

        if passed is not None:
            metric.total_checks += 1
            if passed:
                metric.passed_checks += 1
            else:
                metric.failed_checks += 1

    def _init_sla_metric(self, dataset_name: str) -> None:
        """Initialize SLA metric for dataset."""
        if dataset_name not in self._sla_metrics:
            self._sla_metrics[dataset_name] = SLAMetric(
                dataset_name=dataset_name
            )

    def _trigger_alerts(
        self,
        check: FreshnessCheck,
        rule: DataFreshnessRule
    ) -> None:
        """Trigger alerts for stale data."""
        for callback in self._alert_callbacks:
            try:
                callback(check, rule)
            except Exception:
                pass

    def add_alert_callback(
        self,
        callback: Callable[[FreshnessCheck, DataFreshnessRule], None]
    ) -> None:
        """Add an alert callback."""
        self._alert_callbacks.append(callback)

    def get_sla_metrics(
        self,
        dataset_name: Optional[str] = None
    ) -> Dict[str, SLAMetric]:
        """Get SLA metrics."""
        if dataset_name:
            return {
                dataset_name: self._sla_metrics.get(
                    dataset_name,
                    SLAMetric(dataset_name=dataset_name)
                )
            }
        return self._sla_metrics.copy()

    def get_check_history(
        self,
        rule_id: Optional[str] = None,
        dataset_name: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get check history."""
        history = self._check_history

        if rule_id:
            history = [h for h in history if h.rule_id == rule_id]

        if dataset_name:
            history = [h for h in history if h.dataset_name == dataset_name]

        history = history[-limit:]

        return [
            {
                "rule_id": h.rule_id,
                "dataset_name": h.dataset_name,
                "status": h.status.value,
                "age_seconds": h.age_seconds,
                "max_age_seconds": h.max_age_seconds,
                "passed": h.passed,
                "message": h.message,
                "timestamp": h.timestamp
            }
            for h in history
        ]

    def get_compliance_report(
        self,
        dataset_name: Optional[str] = None,
        period_days: int = 7
    ) -> Dict[str, Any]:
        """Generate compliance report."""
        cutoff = time.time() - (period_days * 86400)

        history = [
            h for h in self._check_history
            if h.timestamp >= cutoff
        ]

        if dataset_name:
            history = [h for h in history if h.dataset_name == dataset_name]

        total = len(history)
        passed = sum(1 for h in history if h.passed)

        return {
            "period_days": period_days,
            "total_checks": total,
            "passed_checks": passed,
            "failed_checks": total - passed,
            "compliance_rate": passed / total if total > 0 else 0.0,
            "datasets_monitored": len(set(h.dataset_name for h in history))
        }


class DataFreshnessAction(BaseAction):
    """Action for data freshness operations."""

    def __init__(self):
        super().__init__("data_freshness")
        self._monitor = DataFreshnessMonitor()

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute data freshness action."""
        try:
            operation = params.get("operation", "add_rule")

            if operation == "add_rule":
                return self._add_rule(params)
            elif operation == "update_rule":
                return self._update_rule(params)
            elif operation == "remove_rule":
                return self._remove_rule(params)
            elif operation == "record_update":
                return self._record_update(params)
            elif operation == "check":
                return self._check_freshness(params)
            elif operation == "check_all":
                return self._check_all(params)
            elif operation == "get_rules":
                return self._get_rules(params)
            elif operation == "sla_metrics":
                return self._get_sla_metrics(params)
            elif operation == "history":
                return self._get_history(params)
            elif operation == "compliance_report":
                return self._get_compliance_report(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _add_rule(self, params: Dict[str, Any]) -> ActionResult:
        """Add a freshness rule."""
        dataset_name = params.get("dataset_name")
        max_age_seconds = params.get("max_age_seconds", 3600)

        if not dataset_name:
            return ActionResult(
                success=False,
                message="dataset_name required"
            )

        rule_id = self._monitor.add_rule(
            dataset_name=dataset_name,
            max_age_seconds=max_age_seconds,
            check_interval_seconds=params.get("check_interval_seconds", 300),
            alert_threshold_seconds=params.get("alert_threshold_seconds"),
            priority=SLAPriority(params.get("priority", "medium"))
        )

        return ActionResult(
            success=True,
            data={"rule_id": rule_id}
        )

    def _update_rule(self, params: Dict[str, Any]) -> ActionResult:
        """Update a freshness rule."""
        rule_id = params.get("rule_id")

        if not rule_id:
            return ActionResult(success=False, message="rule_id required")

        success = self._monitor.update_rule(
            rule_id=rule_id,
            max_age_seconds=params.get("max_age_seconds"),
            enabled=params.get("enabled")
        )

        return ActionResult(
            success=success,
            message="Rule updated" if success else "Rule not found"
        )

    def _remove_rule(self, params: Dict[str, Any]) -> ActionResult:
        """Remove a freshness rule."""
        rule_id = params.get("rule_id")

        if not rule_id:
            return ActionResult(success=False, message="rule_id required")

        success = self._monitor.remove_rule(rule_id)

        return ActionResult(
            success=success,
            message="Rule removed" if success else "Rule not found"
        )

    def _record_update(self, params: Dict[str, Any]) -> ActionResult:
        """Record a data update."""
        dataset_name = params.get("dataset_name")

        if not dataset_name:
            return ActionResult(
                success=False,
                message="dataset_name required"
            )

        self._monitor.record_data_update(
            dataset_name=dataset_name,
            timestamp=params.get("timestamp")
        )

        return ActionResult(
            success=True,
            message=f"Update recorded for: {dataset_name}"
        )

    def _check_freshness(self, params: Dict[str, Any]) -> ActionResult:
        """Check freshness for a rule."""
        rule_id = params.get("rule_id")

        if not rule_id:
            return ActionResult(success=False, message="rule_id required")

        check = self._monitor.check_freshness(rule_id)

        if not check:
            return ActionResult(success=False, message="Rule not found")

        return ActionResult(
            success=check.passed,
            data={
                "rule_id": check.rule_id,
                "dataset_name": check.dataset_name,
                "status": check.status.value,
                "age_seconds": check.age_seconds,
                "max_age_seconds": check.max_age_seconds,
                "passed": check.passed,
                "message": check.message
            }
        )

    def _check_all(self, params: Dict[str, Any]) -> ActionResult:
        """Check all rules."""
        results = self._monitor.check_all_freshness()

        return ActionResult(
            success=all(r.passed for r in results),
            data={
                "total": len(results),
                "passed": sum(1 for r in results if r.passed),
                "failed": sum(1 for r in results if not r.passed),
                "checks": [
                    {
                        "rule_id": r.rule_id,
                        "dataset_name": r.dataset_name,
                        "status": r.status.value,
                        "age_seconds": r.age_seconds,
                        "passed": r.passed
                    }
                    for r in results
                ]
            }
        )

    def _get_rules(self, params: Dict[str, Any]) -> ActionResult:
        """Get freshness rules."""
        dataset_name = params.get("dataset_name")

        if dataset_name:
            rules = self._monitor.get_rules_for_dataset(dataset_name)
        else:
            rules = list(self._monitor._rules.values())

        return ActionResult(
            success=True,
            data={
                "rules": [
                    {
                        "rule_id": r.rule_id,
                        "dataset_name": r.dataset_name,
                        "max_age_seconds": r.max_age_seconds,
                        "priority": r.priority.value,
                        "enabled": r.enabled,
                        "last_update": r.last_update
                    }
                    for r in rules
                ]
            }
        )

    def _get_sla_metrics(self, params: Dict[str, Any]) -> ActionResult:
        """Get SLA metrics."""
        dataset_name = params.get("dataset_name")

        metrics = self._monitor.get_sla_metrics(dataset_name)

        return ActionResult(
            success=True,
            data={
                "metrics": [
                    {
                        "dataset_name": m.dataset_name,
                        "total_checks": m.total_checks,
                        "passed_checks": m.passed_checks,
                        "failed_checks": m.failed_checks,
                        "current_status": m.current_status.value
                    }
                    for m in metrics.values()
                ]
            }
        )

    def _get_history(self, params: Dict[str, Any]) -> ActionResult:
        """Get check history."""
        history = self._monitor.get_check_history(
            rule_id=params.get("rule_id"),
            dataset_name=params.get("dataset_name"),
            limit=params.get("limit", 100)
        )

        return ActionResult(success=True, data={"history": history})

    def _get_compliance_report(self, params: Dict[str, Any]) -> ActionResult:
        """Get compliance report."""
        report = self._monitor.get_compliance_report(
            dataset_name=params.get("dataset_name"),
            period_days=params.get("period_days", 7)
        )

        return ActionResult(success=True, data=report)
