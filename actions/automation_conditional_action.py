"""
Automation Conditional Action Module.

Provides conditional logic capabilities for automation workflows including
branching, if-then-else, switch statements, and complex predicate evaluation.

Author: RabAI Team
"""

from typing import Any, Callable, Dict, List, Optional, TypeVar
from dataclasses import dataclass, field
from enum import Enum
import threading
from datetime import datetime


T = TypeVar('T')


class ConditionOperator(Enum):
    """Comparison operators for conditions."""
    EQUALS = "=="
    NOT_EQUALS = "!="
    GREATER_THAN = ">"
    LESS_THAN = "<"
    GREATER_EQUAL = ">="
    LESS_EQUAL = "<="
    IN = "in"
    NOT_IN = "not in"
    CONTAINS = "contains"
    NOT_CONTAINS = "not contains"
    MATCHES = "matches"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    AND = "and"
    OR = "or"
    NOT = "not"


@dataclass
class Predicate:
    """Represents a single predicate condition."""
    field: str
    operator: ConditionOperator
    value: Any
    value_field: Optional[str] = None  # For comparing with another field


@dataclass
class Condition:
    """Represents a complex condition with predicates."""
    predicates: List[Predicate]
    logic: str = "AND"  # AND or OR
    
    def evaluate(self, data: Dict[str, Any]) -> bool:
        """Evaluate condition against data."""
        results = []
        
        for predicate in self.predicates:
            results.append(self._evaluate_predicate(predicate, data))
        
        if self.logic.upper() == "AND":
            return all(results)
        else:
            return any(results)
    
    def _evaluate_predicate(self, predicate: Predicate, data: Dict[str, Any]) -> bool:
        """Evaluate a single predicate."""
        # Get field value
        field_value = self._get_field_value(data, predicate.field)
        
        # Get comparison value
        if predicate.value_field:
            compare_value = self._get_field_value(data, predicate.value_field)
        else:
            compare_value = predicate.value
        
        # Evaluate operator
        op = predicate.operator
        
        if op == ConditionOperator.EQUALS:
            return field_value == compare_value
        elif op == ConditionOperator.NOT_EQUALS:
            return field_value != compare_value
        elif op == ConditionOperator.GREATER_THAN:
            return field_value > compare_value
        elif op == ConditionOperator.LESS_THAN:
            return field_value < compare_value
        elif op == ConditionOperator.GREATER_EQUAL:
            return field_value >= compare_value
        elif op == ConditionOperator.LESS_EQUAL:
            return field_value <= compare_value
        elif op == ConditionOperator.IN:
            return field_value in compare_value
        elif op == ConditionOperator.NOT_IN:
            return field_value not in compare_value
        elif op == ConditionOperator.CONTAINS:
            return compare_value in str(field_value)
        elif op == ConditionOperator.NOT_CONTAINS:
            return compare_value not in str(field_value)
        elif op == ConditionOperator.MATCHES:
            import re
            return bool(re.match(compare_value, str(field_value)))
        elif op == ConditionOperator.IS_NULL:
            return field_value is None
        elif op == ConditionOperator.IS_NOT_NULL:
            return field_value is not None
        
        return False
    
    def _get_field_value(self, data: Dict, field: str) -> Any:
        """Get field value using dot notation."""
        keys = field.split(".")
        value = data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value


@dataclass
class Branch:
    """Represents a branch in conditional logic."""
    name: str
    condition: Optional[Condition] = None
    condition_fn: Optional[Callable[[Dict], bool]] = None
    action: Optional[Callable] = None
    is_default: bool = False


class ConditionalExecutor:
    """
    Executes conditional logic with branches.
    
    Example:
        executor = ConditionalExecutor()
        executor.add_branch("success", condition_fn=lambda d: d["status"] == "ok", action=handle_success)
        executor.add_branch("failure", condition_fn=lambda d: d["status"] == "error", action=handle_failure)
        executor.add_branch("default", is_default=True, action=handle_default)
        
        result = executor.execute(data)
    """
    
    def __init__(self):
        self.branches: List[Branch] = []
        self._lock = threading.RLock()
    
    def add_branch(
        self,
        name: str,
        condition: Optional[Condition] = None,
        condition_fn: Optional[Callable] = None,
        action: Optional[Callable] = None,
        is_default: bool = False
    ) -> "ConditionalExecutor":
        """Add a branch to the executor."""
        with self._lock:
            self.branches.append(Branch(
                name=name,
                condition=condition,
                condition_fn=condition_fn,
                action=action,
                is_default=is_default
            ))
        return self
    
    def execute(
        self,
        data: Dict[str, Any],
        default_action: Optional[Callable] = None
    ) -> Dict[str, Any]:
        """Execute conditional logic."""
        # Find matching branch
        for branch in self.branches:
            if branch.is_default:
                continue
            
            matches = False
            
            if branch.condition:
                matches = branch.condition.evaluate(data)
            elif branch.condition_fn:
                try:
                    matches = branch.condition_fn(data)
                except Exception:
                    matches = False
            
            if matches:
                if branch.action:
                    try:
                        result = branch.action(data)
                        return {
                            "branch": branch.name,
                            "matched": True,
                            "result": result,
                            "executed_at": datetime.now().isoformat()
                        }
                    except Exception as e:
                        return {
                            "branch": branch.name,
                            "matched": True,
                            "error": str(e),
                            "executed_at": datetime.now().isoformat()
                        }
        
        # Execute default branch
        for branch in self.branches:
            if branch.is_default:
                if branch.action:
                    try:
                        result = branch.action(data)
                        return {
                            "branch": branch.name,
                            "matched": True,
                            "is_default": True,
                            "result": result,
                            "executed_at": datetime.now().isoformat()
                        }
                    except Exception as e:
                        return {
                            "branch": branch.name,
                            "matched": True,
                            "is_default": True,
                            "error": str(e),
                            "executed_at": datetime.now().isoformat()
                        }
        
        # Execute passed default action
        if default_action:
            try:
                result = default_action(data)
                return {
                    "matched": False,
                    "result": result,
                    "executed_at": datetime.now().isoformat()
                }
            except Exception as e:
                return {
                    "matched": False,
                    "error": str(e),
                    "executed_at": datetime.now().isoformat()
                }
        
        return {
            "matched": False,
            "message": "No matching branch found",
            "executed_at": datetime.now().isoformat()
        }


class SwitchCase:
    """
    Switch-case style conditional execution.
    
    Example:
        switch = SwitchCase(lambda d: d["status"])
        switch.case("active", handle_active)
        switch.case("inactive", handle_inactive)
        switch.default(handle_default)
        
        result = switch.execute(data)
    """
    
    def __init__(self, key_fn: Callable[[Dict], Any]):
        self.key_fn = key_fn
        self.cases: Dict[Any, Callable] = {}
        self.default_fn: Optional[Callable] = None
    
    def case(self, key: Any, action: Callable) -> "SwitchCase":
        """Add a case."""
        self.cases[key] = action
        return self
    
    def default(self, action: Callable) -> "SwitchCase":
        """Set default action."""
        self.default_fn = action
        return self
    
    def execute(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute switch-case."""
        key = self.key_fn(data)
        
        action = self.cases.get(key, self.default_fn)
        
        if action:
            try:
                result = action(data)
                return {
                    "matched_key": key,
                    "matched": True,
                    "result": result,
                    "executed_at": datetime.now().isoformat()
                }
            except Exception as e:
                return {
                    "matched_key": key,
                    "matched": True,
                    "error": str(e),
                    "executed_at": datetime.now().isoformat()
                }
        
        return {
            "matched_key": key,
            "matched": False,
            "executed_at": datetime.now().isoformat()
        }


class IfThenElse:
    """
    Simple if-then-else conditional.
    
    Example:
        conditional = IfThenElse()
        conditional.when(lambda d: d["value"] > 10, lambda d: d["value"] * 2)
        conditional.otherwise(lambda d: d["value"] * 3)
        
        result = conditional.evaluate({"value": 5})
    """
    
    def __init__(self):
        self.when_clauses: List[tuple] = []
        self.otherwise_fn: Optional[Callable] = None
    
    def when(self, condition: Callable[[Dict], bool], then_fn: Callable) -> "IfThenElse":
        """Add a when clause."""
        self.when_clauses.append((condition, then_fn))
        return self
    
    def otherwise(self, then_fn: Callable) -> "IfThenElse":
        """Set otherwise action."""
        self.otherwise_fn = then_fn
        return self
    
    def evaluate(self, data: Dict[str, Any]) -> Any:
        """Evaluate the conditional."""
        for condition, then_fn in self.when_clauses:
            if condition(data):
                return then_fn(data)
        
        if self.otherwise_fn:
            return self.otherwise_fn(data)
        
        return None


def create_predicate(
    field: str,
    operator: str,
    value: Any,
    value_field: Optional[str] = None
) -> Predicate:
    """Create a predicate from string parameters."""
    return Predicate(
        field=field,
        operator=ConditionOperator(operator),
        value=value,
        value_field=value_field
    )


def create_condition(
    predicates: List[Predicate],
    logic: str = "AND"
) -> Condition:
    """Create a condition from predicates."""
    return Condition(predicates=predicates, logic=logic)


class BaseAction:
    """Base class for all actions."""
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Any:
        raise NotImplementedError


class AutomationConditionalAction(BaseAction):
    """
    Conditional action for workflow branching.
    
    Parameters:
        operation: Operation type (evaluate/execute/switch)
        condition: Condition definition
        branches: List of branch definitions
        data: Data to evaluate
    
    Example:
        action = AutomationConditionalAction()
        result = action.execute({}, {
            "operation": "evaluate",
            "condition": {
                "predicates": [{"field": "status", "operator": "==", "value": "active"}],
                "logic": "AND"
            },
            "data": {"status": "active"}
        })
    """
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute conditional operation."""
        operation = params.get("operation", "evaluate")
        data = params.get("data", {})
        condition_def = params.get("condition")
        branches = params.get("branches", [])
        
        if operation == "evaluate" and condition_def:
            # Parse condition
            predicates = []
            for pred_def in condition_def.get("predicates", []):
                predicates.append(create_predicate(
                    field=pred_def["field"],
                    operator=pred_def["operator"],
                    value=pred_def.get("value"),
                    value_field=pred_def.get("value_field")
                ))
            
            logic = condition_def.get("logic", "AND")
            condition = create_condition(predicates, logic)
            
            result = condition.evaluate(data)
            
            return {
                "success": True,
                "operation": "evaluate",
                "matched": result,
                "data": data,
                "evaluated_at": datetime.now().isoformat()
            }
        
        elif operation == "execute":
            executor = ConditionalExecutor()
            
            for branch_def in branches:
                def placeholder_action(d):
                    return {"executed": branch_def.get("name", "branch")}
                
                executor.add_branch(
                    name=branch_def.get("name", "branch"),
                    condition_fn=lambda d, b=branch_def: d.get(b.get("field")) == b.get("value") if "field" in b and "value" in b else True,
                    action=placeholder_action
                )
            
            result = executor.execute(data)
            
            return {
                "success": True,
                "operation": "execute",
                **result
            }
        
        elif operation == "switch":
            def key_fn(d):
                return d.get(params.get("key_field", "value"))
            
            switch = SwitchCase(key_fn)
            
            for branch_def in branches:
                key = branch_def.get("value")
                
                def action(d):
                    return {"handled": True, "branch": branch_def.get("name")}
                
                switch.case(key, action)
            
            result = switch.execute(data)
            
            return {
                "success": True,
                "operation": "switch",
                **result
            }
        
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
