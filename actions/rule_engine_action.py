"""Rule engine action module for RabAI AutoClick.

Provides rule-based processing operations:
- RuleEngineAction: Define and evaluate rules
- RuleConditionAction: Define rule conditions
- RuleActionAction: Define rule actions
- RuleSetAction: Manage sets of rules
"""

import re
import operator
from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from enum import Enum


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RuleOperator(Enum):
    """Operators for rule conditions."""
    EQ = "=="
    NE = "!="
    GT = ">"
    LT = "<"
    GTE = ">="
    LTE = "<="
    CONTAINS = "contains"
    STARTSWITH = "startswith"
    ENDSWITH = "endswith"
    MATCHES = "matches"
    IN = "in"
    NOT_IN = "not_in"


@dataclass
class RuleCondition:
    """Represents a rule condition."""
    field: str
    operator: str
    value: Any
    logical: str = "and"


@dataclass
class Rule:
    """Represents a rule."""
    rule_id: str
    name: str
    conditions: List[RuleCondition]
    action: Optional[Callable] = None
    priority: int = 0
    enabled: bool = True
    description: str = ""


_ops = {
    "==": operator.eq,
    "!=": operator.ne,
    ">": operator.gt,
    "<": operator.lt,
    ">=": operator.gte,
    "<=": operator.lte,
    "contains": lambda a, b: b in str(a),
    "startswith": lambda a, b: str(a).startswith(str(b)),
    "endswith": lambda a, b: str(a).endswith(str(b)),
    "matches": lambda a, b: bool(re.search(b, str(a))),
    "in": lambda a, b: a in b if isinstance(b, (list, tuple, set)) else a == b,
    "not_in": lambda a, b: a not in b if isinstance(b, (list, tuple, set)) else a != b,
}


class RuleEngine:
    """Rule engine for evaluating rules."""
    def __init__(self):
        self._rules: Dict[str, Rule] = {}
        self._rules_by_priority: List[Rule] = []

    def add_rule(self, rule: Rule) -> None:
        self._rules[rule.rule_id] = rule
        self._rebuild_priority_list()

    def remove_rule(self, rule_id: str) -> bool:
        if rule_id in self._rules:
            del self._rules[rule_id]
            self._rebuild_priority_list()
            return True
        return False

    def _rebuild_priority_list(self):
        self._rules_by_priority = sorted(
            self._rules.values(),
            key=lambda r: r.priority,
            reverse=True
        )

    def evaluate_condition(self, condition: RuleCondition, data: Dict[str, Any]) -> bool:
        field_path = condition.field.split(".")
        current = data
        for part in field_path:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return False

        op_fn = _ops.get(condition.operator, operator.eq)
        try:
            return op_fn(current, condition.value)
        except Exception:
            return False

    def evaluate_rule(self, rule: Rule, data: Dict[str, Any]) -> bool:
        if not rule.enabled:
            return False
        if not rule.conditions:
            return True

        results = []
        for cond in rule.conditions:
            result = self.evaluate_condition(cond, data)
            results.append((cond, result))

        first_cond = results[0][1]
        for i in range(1, len(results)):
            cond, result = results[i]
            if cond.logical == "and":
                first_cond = first_cond and result
            else:
                first_cond = first_cond or result

        return first_cond

    def evaluate(self, data: Dict[str, Any]) -> List[Rule]:
        matched = []
        for rule in self._rules_by_priority:
            if self.evaluate_rule(rule, data):
                matched.append(rule)
                if rule.action:
                    try:
                        rule.action(data)
                    except Exception:
                        pass
        return matched

    def list_rules(self) -> List[Dict[str, Any]]:
        return [
            {
                "rule_id": r.rule_id,
                "name": r.name,
                "enabled": r.enabled,
                "priority": r.priority,
                "condition_count": len(r.conditions),
                "description": r.description
            }
            for r in self._rules_by_priority
        ]


_engine = RuleEngine()


class RuleEngineAction(BaseAction):
    """Evaluate data against rules."""
    action_type = "rule_engine"
    display_name = "规则引擎"
    description = "根据规则评估数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            rule_ids = params.get("rule_ids", None)

            if not data:
                return ActionResult(success=False, message="data is required")

            if rule_ids:
                rules_to_eval = [_engine._rules.get(rid) for rid in rule_ids if rid in _engine._rules]
                matched = [r for r in rules_to_eval if r and _engine.evaluate_rule(r, data)]
            else:
                matched = _engine.evaluate(data)

            return ActionResult(
                success=True,
                message=f"Matched {len(matched)} rules",
                data={
                    "matched_rules": [{"rule_id": r.rule_id, "name": r.name} for r in matched],
                    "data": data
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Rule engine failed: {str(e)}")


class RuleConditionAction(BaseAction):
    """Define rule conditions."""
    action_type = "rule_condition"
    display_name = "规则条件"
    description = "定义规则条件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            field = params.get("field", "")
            operator_type = params.get("operator", "==")
            value = params.get("value", None)
            logical = params.get("logical", "and")

            if not field:
                return ActionResult(success=False, message="field is required")

            condition = RuleCondition(field=field, operator=operator_type, value=value, logical=logical)

            return ActionResult(
                success=True,
                message=f"Condition created: {field} {operator_type} {value}",
                data={
                    "condition": {
                        "field": field,
                        "operator": operator_type,
                        "value": value,
                        "logical": logical
                    }
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Rule condition failed: {str(e)}")


class RuleActionAction(BaseAction):
    """Define and manage rule actions."""
    action_type = "rule_action"
    display_name = "规则动作"
    description = "定义和管理规则动作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action_type = params.get("action_type", "log")
            rule_id = params.get("rule_id", None)
            action_params = params.get("params", {})

            if action_type == "create":
                import uuid
                name = params.get("name", "")
                conditions = params.get("conditions", [])
                priority = params.get("priority", 0)
                enabled = params.get("enabled", True)

                if not name:
                    return ActionResult(success=False, message="name is required")

                rule_conditions = []
                for c in conditions:
                    rule_conditions.append(RuleCondition(
                        field=c.get("field", ""),
                        operator=c.get("operator", "=="),
                        value=c.get("value", None),
                        logical=c.get("logical", "and")
                    ))

                rule = Rule(
                    rule_id=str(uuid.uuid4()),
                    name=name,
                    conditions=rule_conditions,
                    priority=priority,
                    enabled=enabled
                )
                _engine.add_rule(rule)

                return ActionResult(
                    success=True,
                    message=f"Rule '{name}' created",
                    data={"rule_id": rule.rule_id, "name": name}
                )

            elif action_type == "enable":
                if rule_id and rule_id in _engine._rules:
                    _engine._rules[rule_id].enabled = True
                    return ActionResult(success=True, message=f"Rule {rule_id} enabled")
                return ActionResult(success=False, message=f"Rule {rule_id} not found")

            elif action_type == "disable":
                if rule_id and rule_id in _engine._rules:
                    _engine._rules[rule_id].enabled = False
                    return ActionResult(success=True, message=f"Rule {rule_id} disabled")
                return ActionResult(success=False, message=f"Rule {rule_id} not found")

            elif action_type == "delete":
                if _engine.remove_rule(rule_id):
                    return ActionResult(success=True, message=f"Rule {rule_id} deleted")
                return ActionResult(success=False, message=f"Rule {rule_id} not found")

            elif action_type == "list":
                rules = _engine.list_rules()
                return ActionResult(success=True, message=f"{len(rules)} rules", data={"rules": rules})

            else:
                return ActionResult(success=False, message=f"Unknown action_type: {action_type}")

        except Exception as e:
            return ActionResult(success=False, message=f"Rule action failed: {str(e)}")


class RuleSetAction(BaseAction):
    """Manage sets of rules."""
    action_type = "rule_set"
    display_name = "规则集"
    description = "管理规则集"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "list")
            set_name = params.get("set_name", "default")
            rule_ids = params.get("rule_ids", [])

            if operation == "list":
                rules = _engine.list_rules()
                return ActionResult(success=True, message=f"{len(rules)} rules", data={"rules": rules})

            elif operation == "export":
                rules = _engine.list_rules()
                return ActionResult(success=True, message=f"Exported {len(rules)} rules", data={"rules": rules})

            elif operation == "clear":
                count = len(_engine._rules)
                _engine._rules.clear()
                _engine._rules_by_priority.clear()
                return ActionResult(success=True, message=f"Cleared {count} rules")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Rule set failed: {str(e)}")
