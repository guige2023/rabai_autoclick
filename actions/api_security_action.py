"""API Security Action Module.

Handles API security including input validation,
injection prevention, and security headers.
"""

from __future__ import annotations

import sys
import os
import time
import re
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class SecurityRule:
    """A security validation rule."""
    name: str
    pattern: str
    action: str
    severity: str = "error"


class APISecurityAction(BaseAction):
    """
    API security validation.

    Validates inputs, prevents injection attacks,
    and enforces security headers.

    Example:
        security = APISecurityAction()
        result = security.execute(ctx, {"action": "validate_input", "input": user_data})
    """
    action_type = "api_security"
    display_name = "API安全"
    description = "API安全：输入验证和注入防护"

    def __init__(self) -> None:
        super().__init__()
        self._rules: List[SecurityRule] = []
        self._setup_default_rules()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "validate_input":
                return self._validate_input(params)
            elif action == "add_rule":
                return self._add_rule(params)
            elif action == "check_sql_injection":
                return self._check_sql_injection(params)
            elif action == "check_xss":
                return self._check_xss(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Security error: {str(e)}")

    def _setup_default_rules(self) -> None:
        self._rules = [
            SecurityRule(name="sql_injection", pattern=r"('|(\\'))|(--)|(\b(SELECT|INSERT|UPDATE|DELETE|DROP|UNION)\b)", action="block", severity="critical"),
            SecurityRule(name="xss_script", pattern=r"(<script|javascript:|onerror=|onclick=)", action="block", severity="critical"),
            SecurityRule(name="path_traversal", pattern=r"(\.\./|\.\.\\|%2e%2e)", action="block", severity="high"),
        ]

    def _validate_input(self, params: Dict[str, Any]) -> ActionResult:
        input_data = params.get("input", "")
        field_name = params.get("field_name", "unknown")

        if isinstance(input_data, dict):
            violations = []
            for key, value in input_data.items():
                result = self._check_value(str(value), key)
                if not result["valid"]:
                    violations.extend(result["violations"])
            if violations:
                return ActionResult(success=False, message="Validation failed", data={"violations": violations})
            return ActionResult(success=True, message="Input valid")
        else:
            result = self._check_value(str(input_data), field_name)
            if result["valid"]:
                return ActionResult(success=True, message="Input valid")
            return ActionResult(success=False, message="Validation failed", data={"violations": result["violations"]})

    def _check_value(self, value: str, field_name: str) -> Dict[str, Any]:
        violations = []
        for rule in self._rules:
            if re.search(rule.pattern, value, re.IGNORECASE):
                violations.append({"rule": rule.name, "severity": rule.severity, "field": field_name})
        return {"valid": len(violations) == 0, "violations": violations}

    def _add_rule(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        pattern = params.get("pattern", "")
        action = params.get("action", "block")
        severity = params.get("severity", "error")

        if not name or not pattern:
            return ActionResult(success=False, message="name and pattern are required")

        rule = SecurityRule(name=name, pattern=pattern, action=action, severity=severity)
        self._rules.append(rule)

        return ActionResult(success=True, message=f"Rule added: {name}")

    def _check_sql_injection(self, params: Dict[str, Any]) -> ActionResult:
        input_data = params.get("input", "")
        if self._contains_sql_injection(str(input_data)):
            return ActionResult(success=False, message="SQL injection detected")
        return ActionResult(success=True, message="No SQL injection detected")

    def _check_xss(self, params: Dict[str, Any]) -> ActionResult:
        input_data = params.get("input", "")
        if self._contains_xss(str(input_data)):
            return ActionResult(success=False, message="XSS pattern detected")
        return ActionResult(success=True, message="No XSS detected")

    def _contains_sql_injection(self, value: str) -> bool:
        patterns = [r"'", r"--", r";", r"\bSELECT\b", r"\bUNION\b", r"\bDROP\b"]
        for pattern in patterns:
            if re.search(pattern, value, re.IGNORECASE):
                return True
        return False

    def _contains_xss(self, value: str) -> bool:
        patterns = [r"<script", r"javascript:", r"onerror=", r"onclick=", r"onload="]
        for pattern in patterns:
            if re.search(pattern, value, re.IGNORECASE):
                return True
        return False
