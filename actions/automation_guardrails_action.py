"""Automation guardrails action module for RabAI AutoClick.

Provides safety guard operations:
- GuardrailCheckAction: Pre-execution safety checks
- GuardrailMonitorAction: Runtime safety monitoring
- GuardrailBypassAction: Temporarily bypass guardrails
- GuardrailAuditAction: Audit guardrail violations
"""

import sys
import os
import time
import logging
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult

logger = logging.getLogger(__name__)


class ViolationSeverity(Enum):
    """Severity level for guardrail violations."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class GuardrailRule:
    """A safety guardrail rule."""
    rule_id: str
    name: str
    check_fn: Callable[[Dict[str, Any]], tuple[bool, str]]
    severity: ViolationSeverity = ViolationSeverity.WARNING
    enabled: bool = True
    bypass_tokens: List[str] = field(default_factory=list)


@dataclass
class Violation:
    """A guardrail violation record."""
    rule_id: str
    rule_name: str
    message: str
    severity: ViolationSeverity
    timestamp: datetime = field(default_factory=datetime.now)
    context: Dict[str, Any] = field(default_factory=dict)
    bypassed: bool = False


class GuardrailRegistry:
    """Registry for guardrail rules and violations."""

    def __init__(self) -> None:
        self._rules: Dict[str, GuardrailRule] = {}
        self._violations: List[Violation] = []
        self._bypass_until: Optional[datetime] = None
        self._bypass_token: Optional[str] = None

    def add_rule(self, rule: GuardrailRule) -> None:
        self._rules[rule.rule_id] = rule

    def remove_rule(self, rule_id: str) -> bool:
        if rule_id in self._rules:
            del self._rules[rule_id]
            return True
        return False

    def get_rule(self, rule_id: str) -> Optional[GuardrailRule]:
        return self._rules.get(rule_id)

    def list_rules(self) -> List[GuardrailRule]:
        return list(self._rules.values())

    def check(self, context: Dict[str, Any]) -> List[Violation]:
        violations = []
        for rule in self._rules.values():
            if not rule.enabled:
                continue
            try:
                passed, message = rule.check_fn(context)
                if not passed:
                    violation = Violation(
                        rule_id=rule.rule_id,
                        rule_name=rule.name,
                        message=message,
                        severity=rule.severity,
                        context=context
                    )
                    if self._is_bypassed(rule):
                        violation.bypassed = True
                    violations.append(violation)
                    self._violations.append(violation)
            except Exception as e:
                logger.error(f"Rule {rule.rule_id} check failed: {e}")
        return violations

    def _is_bypassed(self, rule: GuardrailRule) -> bool:
        if self._bypass_until and datetime.now() < self._bypass_until:
            if self._bypass_token in rule.bypass_tokens:
                return True
        return False

    def set_bypass(self, token: str, duration_seconds: int = 300) -> bool:
        self._bypass_until = datetime.now() + timedelta(seconds=duration_seconds)
        self._bypass_token = token
        return True

    def clear_bypass(self) -> None:
        self._bypass_until = None
        self._bypass_token = None

    def get_violations(
        self,
        since: Optional[datetime] = None,
        severity: Optional[ViolationSeverity] = None
    ) -> List[Violation]:
        violations = self._violations
        if since:
            violations = [v for v in violations if v.timestamp >= since]
        if severity:
            violations = [v for v in violations if v.severity == severity]
        return violations


_registry = GuardrailRegistry()


def _always_pass(context: Dict[str, Any]) -> tuple[bool, str]:
    return True, ""


def _check_not_empty(key: str, context: Dict[str, Any]) -> tuple[bool, str]:
    value = context.get(key)
    if value is None or value == "":
        return False, f"{key} cannot be empty"
    return True, ""


def _check_positive(key: str, context: Dict[str, Any]) -> tuple[bool, str]:
    value = context.get(key)
    if value is not None and isinstance(value, (int, float)) and value <= 0:
        return False, f"{key} must be positive"
    return True, ""


def _check_in_range(key: str, min_val: float, max_val: float, context: Dict[str, Any]) -> tuple[bool, str]:
    value = context.get(key)
    if value is not None and isinstance(value, (int, float)):
        if not (min_val <= value <= max_val):
            return False, f"{key} must be between {min_val} and {max_val}"
    return True, ""


class GuardrailCheckAction(BaseAction):
    """Pre-execution safety checks using guardrails."""
    action_type = "automation_guardrail_check"
    display_name = "安全规则检查"
    description = "执行预定义的安全规则检查"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        rule_id = params.get("rule_id", "")
        rule_name = params.get("rule_name", "")
        check_type = params.get("check_type", "always_pass")
        check_context = params.get("context", {})
        severity = params.get("severity", "warning")

        if rule_id and not rule_name:
            existing = _registry.get_rule(rule_id)
            if existing:
                violations = _registry.check(check_context)
                critical = [v for v in violations if v.severity in (ViolationSeverity.ERROR, ViolationSeverity.CRITICAL) and not v.bypassed]
                return ActionResult(
                    success=len(critical) == 0,
                    message=f"检查完成，{len(critical)} 个严重违规",
                    data={"violations": [{"rule": v.rule_name, "message": v.message, "severity": v.severity.value} for v in violations]}
                )

        if not rule_id or not rule_name:
            return ActionResult(success=False, message="rule_id和rule_name是必需的")

        check_fn_map = {
            "always_pass": _always_pass,
            "not_empty": lambda ctx: _check_not_empty(check_context.get("check_key", ""), ctx),
            "positive": lambda ctx: _check_positive(check_context.get("check_key", ""), ctx),
            "in_range": lambda ctx: _check_in_range(check_context.get("check_key", ""), check_context.get("min_val", 0), check_context.get("max_val", 100), ctx),
        }

        check_fn = check_fn_map.get(check_type, _always_pass)
        severity_enum = ViolationSeverity(severity)

        rule = GuardrailRule(
            rule_id=rule_id,
            name=rule_name,
            check_fn=check_fn,
            severity=severity_enum
        )
        _registry.add_rule(rule)

        return ActionResult(
            success=True,
            message=f"规则 {rule_name} 已注册",
            data={"rule_id": rule_id, "severity": severity_enum.value}
        )


class GuardrailMonitorAction(BaseAction):
    """Runtime safety monitoring."""
    action_type = "automation_guardrail_monitor"
    display_name = "安全规则监控"
    description = "监控运行时安全规则状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        operation = params.get("operation", "status")
        check_context = params.get("context", {})

        if operation == "status":
            rules = _registry.list_rules()
            enabled = [r for r in rules if r.enabled]
            disabled = [r for r in rules if not r.enabled]

            return ActionResult(
                success=True,
                message=f"共 {len(rules)} 条规则，{len(enabled)} 启用，{len(disabled)} 禁用",
                data={
                    "total": len(rules),
                    "enabled": len(enabled),
                    "disabled": len(disabled),
                    "rules": [{"id": r.rule_id, "name": r.name, "enabled": r.enabled, "severity": r.severity.value} for r in rules]
                }
            )

        if operation == "check":
            violations = _registry.check(check_context)
            bypassed = [v for v in violations if v.bypassed]
            active = [v for v in violations if not v.bypassed]
            critical = [v for v in active if v.severity in (ViolationSeverity.ERROR, ViolationSeverity.CRITICAL)]

            return ActionResult(
                success=len(critical) == 0,
                message=f"检查完成: {len(violations)} 违规, {len(bypassed)} 绕过, {len(critical)} 严重",
                data={
                    "total_violations": len(violations),
                    "bypassed": len(bypassed),
                    "critical": len(critical),
                    "can_proceed": len(critical) == 0
                }
            )

        if operation == "enable":
            rule_id = params.get("rule_id", "")
            rule = _registry.get_rule(rule_id)
            if not rule:
                return ActionResult(success=False, message=f"规则 {rule_id} 不存在")
            rule.enabled = True
            return ActionResult(success=True, message=f"规则 {rule_id} 已启用")

        if operation == "disable":
            rule_id = params.get("rule_id", "")
            rule = _registry.get_rule(rule_id)
            if not rule:
                return ActionResult(success=False, message=f"规则 {rule_id} 不存在")
            rule.enabled = False
            return ActionResult(success=True, message=f"规则 {rule_id} 已禁用")

        return ActionResult(success=False, message=f"未知操作: {operation}")


class GuardrailBypassAction(BaseAction):
    """Temporarily bypass guardrails."""
    action_type = "automation_guardrail_bypass"
    display_name = "安全规则临时绕过"
    description = "临时绕过安全规则检查"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        operation = params.get("operation", "enable")
        token = params.get("token", "")
        duration = params.get("duration_seconds", 300)

        if operation == "enable":
            if not token:
                return ActionResult(success=False, message="token是必需的")
            if duration <= 0 or duration > 3600:
                return ActionResult(success=False, message="duration必须在1-3600秒之间")

            _registry.set_bypass(token, duration)
            bypass_until = datetime.now() + timedelta(seconds=duration)
            return ActionResult(
                success=True,
                message=f"绕过已启用，有效期至 {bypass_until.strftime('%H:%M:%S')}",
                data={"until": bypass_until.isoformat()}
            )

        if operation == "disable":
            _registry.clear_bypass()
            return ActionResult(success=True, message="绕过已禁用")

        return ActionResult(success=False, message=f"未知操作: {operation}")


class GuardrailAuditAction(BaseAction):
    """Audit guardrail violations."""
    action_type = "automation_guardrail_audit"
    display_name = "安全规则审计"
    description = "审计安全规则违规记录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        since_minutes = params.get("since_minutes", 60)
        severity_filter = params.get("severity")
        include_bypassed = params.get("include_bypassed", True)

        since = datetime.now() - timedelta(minutes=since_minutes)
        severity = ViolationSeverity(severity_filter) if severity_filter else None
        violations = _registry.get_violations(since=since, severity=severity)

        if not include_bypassed:
            violations = [v for v in violations if not v.bypassed]

        total = len(violations)
        by_severity = {
            "critical": len([v for v in violations if v.severity == ViolationSeverity.CRITICAL]),
            "error": len([v for v in violations if v.severity == ViolationSeverity.ERROR]),
            "warning": len([v for v in violations if v.severity == ViolationSeverity.WARNING]),
            "info": len([v for v in violations if v.severity == ViolationSeverity.INFO]),
        }

        return ActionResult(
            success=True,
            message=f"审计完成: {total} 条违规记录",
            data={
                "total": total,
                "by_severity": by_severity,
                "violations": [
                    {
                        "rule_id": v.rule_id,
                        "rule_name": v.rule_name,
                        "message": v.message,
                        "severity": v.severity.value,
                        "timestamp": v.timestamp.isoformat(),
                        "bypassed": v.bypassed
                    }
                    for v in violations[-50:]
                ]
            }
        )
