"""Rule engine utilities.

Declarative rule evaluation engine for business logic, validation,
and decision-making with a fluent API for rule definition.

Example:
    engine = RuleEngine()
    engine.rule("adult_check").when(lambda ctx: ctx["age"] >= 18).then(
        lambda ctx: ctx.update(eligible=True)
    ).execute({"age": 25})
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Generator

logger = logging.getLogger(__name__)


@dataclass
class RuleResult:
    """Result of rule evaluation."""
    rule_name: str
    matched: bool
    actions_run: int
    context: dict[str, Any]
    error: str | None = None


class Rule:
    """A single rule with condition and action functions."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._conditions: list[Callable[[dict], bool]] = []
        self._actions: list[Callable[[dict], Any]] = []
        self._priority: int = 100
        self._enabled: bool = True

    def when(self, condition: Callable[[dict], bool]) -> "Rule":
        """Add a condition - rule only fires if condition returns True.

        Args:
            condition: Function that takes context and returns bool.

        Returns:
            Self for chaining.
        """
        self._conditions.append(condition)
        return self

    def when_any(self, *conditions: Callable[[dict], bool]) -> "Rule":
        """Add multiple conditions - rule fires if ANY returns True.

        Args:
            *conditions: Functions that take context and return bool.

        Returns:
            Self for chaining.
        """
        def combined(ctx: dict) -> bool:
            return any(c(ctx) for c in conditions)
        self._conditions.append(combined)
        return self

    def when_all(self, *conditions: Callable[[dict], bool]) -> "Rule":
        """Add multiple conditions - rule fires if ALL return True.

        Args:
            *conditions: Functions that take context and return bool.

        Returns:
            Self for chaining.
        """
        def combined(ctx: dict) -> bool:
            return all(c(ctx) for c in conditions)
        self._conditions.append(combined)
        return self

    def then(self, action: Callable[[dict], Any]) -> "Rule":
        """Add an action to run when rule matches.

        Args:
            action: Function that takes context and performs side effects.

        Returns:
            Self for chaining.
        """
        self._actions.append(action)
        return self

    def priority(self, level: int) -> "Rule":
        """Set rule priority (lower = runs first).

        Args:
            level: Priority level.

        Returns:
            Self for chaining.
        """
        self._priority = level
        return self

    def disabled(self) -> "Rule":
        """Disable this rule."""
        self._enabled = False
        return self

    def evaluate(self, context: dict[str, Any]) -> RuleResult:
        """Evaluate the rule against a context.

        Args:
            context: Rule evaluation context.

        Returns:
            RuleResult with match status and any action results.
        """
        if not self._enabled:
            return RuleResult(
                rule_name=self.name,
                matched=False,
                actions_run=0,
                context=context,
            )

        try:
            for condition in self._conditions:
                if not condition(context):
                    return RuleResult(
                        rule_name=self.name,
                        matched=False,
                        actions_run=0,
                        context=context,
                    )

            actions_run = 0
            for action in self._actions:
                action(context)
                actions_run += 1

            return RuleResult(
                rule_name=self.name,
                matched=True,
                actions_run=actions_run,
                context=context,
            )

        except Exception as e:
            logger.error("Rule %s failed: %s", self.name, e)
            return RuleResult(
                rule_name=self.name,
                matched=False,
                actions_run=0,
                context=context,
                error=str(e),
            )


class RuleEngine:
    """Engine for managing and evaluating collections of rules.

    Supports rule registration, priority ordering, and batch evaluation.
    """

    def __init__(self) -> None:
        self._rules: list[Rule] = []
        self._rule_map: dict[str, Rule] = {}

    def rule(self, name: str) -> Rule:
        """Create and register a new rule.

        Args:
            name: Unique rule name.

        Returns:
            New Rule instance for chaining.
        """
        r = Rule(name)
        self._rules.append(r)
        self._rule_map[name] = r
        return r

    def get_rule(self, name: str) -> Rule | None:
        """Get a registered rule by name."""
        return self._rule_map.get(name)

    def remove_rule(self, name: str) -> bool:
        """Remove a rule from the engine.

        Returns:
            True if rule was removed.
        """
        rule = self._rule_map.pop(name, None)
        if rule:
            self._rules.remove(rule)
            return True
        return False

    def evaluate(
        self,
        context: dict[str, Any],
        rule_names: list[str] | None = None,
    ) -> dict[str, RuleResult]:
        """Evaluate rules against a context.

        Args:
            context: Evaluation context passed to all rules.
            rule_names: Optional list of specific rules to evaluate.

        Returns:
            Dict mapping rule names to their results.
        """
        results: dict[str, RuleResult] = {}

        rules = self._rules
        if rule_names:
            rules = [r for r in self._rules if r.name in rule_names]

        rules.sort(key=lambda r: r._priority)

        for rule in rules:
            results[rule.name] = rule.evaluate(dict(context))

        return results

    def evaluate_first(
        self,
        context: dict[str, Any],
    ) -> RuleResult | None:
        """Evaluate rules and return the first matching result.

        Args:
            context: Evaluation context.

        Returns:
            First matching RuleResult or None.
        """
        sorted_rules = sorted(self._rules, key=lambda r: r._priority)

        for rule in sorted_rules:
            if not rule._enabled:
                continue
            result = rule.evaluate(dict(context))
            if result.matched:
                return result

        return None

    def list_rules(self) -> list[str]:
        """List all registered rule names."""
        return [r.name for r in sorted(self._rules, key=lambda x: x._priority)]


class ValidationRule(Rule):
    """Rule subclass with validation helpers."""

    def require(self, field_name: str, condition: Callable[[Any], bool] | None = None) -> "ValidationRule":
        """Add a field requirement.

        Args:
            field_name: Field that must be present.
            condition: Optional additional condition on field value.

        Returns:
            Self for chaining.
        """
        def check(ctx: dict) -> bool:
            if field_name not in ctx:
                return False
            if condition is not None:
                return condition(ctx[field_name])
            return True

        return self.when(check)

    def validate_range(
        self,
        field_name: str,
        min_val: float | None = None,
        max_val: float | None = None,
    ) -> "ValidationRule":
        """Validate a numeric field is within range.

        Returns:
            Self for chaining.
        """
        def check(ctx: dict) -> bool:
            val = ctx.get(field_name)
            if val is None:
                return False
            if min_val is not None and val < min_val:
                return False
            if max_val is not None and val > max_val:
                return False
            return True

        return self.when(check)


def field_equals(field: str, value: Any) -> Callable[[dict], bool]:
    """Create a condition that checks a field equals a value."""
    return lambda ctx: ctx.get(field) == value


def field_in(field: str, values: list[Any]) -> Callable[[dict], bool]:
    """Create a condition that checks a field is in a list."""
    return lambda ctx: ctx.get(field) in values


def field_gt(field: str, threshold: float) -> Callable[[dict], bool]:
    """Create a condition that checks a field is greater than threshold."""
    return lambda ctx: (ctx.get(field) or 0) > threshold


def field_matches(field: str, pattern: str) -> Callable[[dict], bool]:
    """Create a condition that checks a field matches regex pattern."""
    import re
    compiled = re.compile(pattern)
    return lambda ctx: compiled.match(str(ctx.get(field) or ""))
