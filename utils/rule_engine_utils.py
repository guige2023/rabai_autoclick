"""
Rule Engine Utilities

Provides utilities for defining and evaluating
business rules in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable
from dataclasses import dataclass


@dataclass
class Rule:
    """Represents a business rule."""
    name: str
    condition: Callable[[dict], bool]
    action: Callable[[dict], Any]
    priority: int = 0


class RuleEngine:
    """
    Evaluates business rules against facts.
    
    Processes rules in priority order and
    executes matching rule actions.
    """

    def __init__(self) -> None:
        self._rules: list[Rule] = []

    def add_rule(
        self,
        name: str,
        condition: Callable[[dict], bool],
        action: Callable[[dict], Any],
        priority: int = 0,
    ) -> None:
        """Add a rule to the engine."""
        rule = Rule(name=name, condition=condition, action=action, priority=priority)
        self._rules.append(rule)
        self._rules.sort(key=lambda r: -r.priority)

    def evaluate(self, facts: dict[str, Any]) -> list[Any]:
        """
        Evaluate all rules against facts.
        
        Returns:
            List of action results from matching rules.
        """
        results = []
        for rule in self._rules:
            try:
                if rule.condition(facts):
                    result = rule.action(facts)
                    results.append(result)
            except Exception:
                pass
        return results

    def find_matching(self, facts: dict[str, Any]) -> list[Rule]:
        """Find all rules that match facts."""
        matching = []
        for rule in self._rules:
            try:
                if rule.condition(facts):
                    matching.append(rule)
            except Exception:
                pass
        return matching

    def remove_rule(self, name: str) -> bool:
        """Remove a rule by name."""
        for i, rule in enumerate(self._rules):
            if rule.name == name:
                self._rules.pop(i)
                return True
        return False
