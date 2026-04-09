"""Workflow trigger and condition utilities.

Provides declarative triggers and conditional execution
for automation workflows and action sequences.
"""

import re
import time
from typing import Any, Callable, Dict, List, Optional, Pattern, Union


class Condition:
    """Base class for workflow conditions."""

    def evaluate(self, context: Dict[str, Any]) -> bool:
        """Evaluate condition against context.

        Args:
            context: Workflow execution context.

        Returns:
            True if condition passes.
        """
        raise NotImplementedError


class ThresholdCondition(Condition):
    """Condition based on numeric threshold.

    Example:
        cond = ThresholdCondition("cpu_percent", threshold=80.0, operator="gt")
        if cond.evaluate({"cpu_percent": 85.0}):
            trigger_alert()
    """

    OPERATORS = {
        "gt": lambda a, b: a > b,
        "gte": lambda a, b: a >= b,
        "lt": lambda a, b: a < b,
        "lte": lambda a, b: a <= b,
        "eq": lambda a, b: a == b,
        "neq": lambda a, b: a != b,
    }

    def __init__(
        self,
        field: str,
        threshold: Union[int, float],
        operator: str = "gt",
    ) -> None:
        self.field = field
        self.threshold = threshold
        self.operator = self.OPERATORS.get(operator)
        if self.operator is None:
            raise ValueError(f"Unknown operator: {operator}")

    def evaluate(self, context: Dict[str, Any]) -> bool:
        value = self._get_field_value(context)
        if value is None:
            return False
        return self.operator(value, self.threshold)

    def _get_field_value(self, context: Dict[str, Any]) -> Optional[Union[int, float]]:
        """Get field value from context using dot notation."""
        parts = self.field.split(".")
        value = context
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
        return value


class RegexCondition(Condition):
    """Condition based on regex pattern match.

    Example:
        cond = RegexCondition("log_message", pattern=r"ERROR:.*")
        if cond.evaluate({"log_message": "ERROR: connection failed"}):
            handle_error()
    """

    def __init__(
        self,
        field: str,
        pattern: Union[str, Pattern[str]],
        invert: bool = False,
    ) -> None:
        self.field = field
        self.pattern = re.compile(pattern) if isinstance(pattern, str) else pattern
        self.invert = invert

    def evaluate(self, context: Dict[str, Any]) -> bool:
        value = self._get_field_value(context)
        if value is None:
            return False
        matches = bool(self.pattern.search(str(value)))
        return not matches if self.invert else matches

    def _get_field_value(self, context: Dict[str, Any]) -> Optional[str]:
        """Get string field value from context."""
        parts = self.field.split(".")
        value = context
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
        return str(value) if value is not None else None


class BooleanCondition(Condition):
    """Boolean combination of conditions.

    Example:
        cond = BooleanCondition("and",
            ThresholdCondition("cpu", 80),
            RegexCondition("msg", "warning")
        )
    """

    OPERATORS = {"and": all, "or": any, "nand": lambda x: not all(x), "nor": lambda x: not any(x)}

    def __init__(
        self,
        operator: str,
        *conditions: Condition,
    ) -> None:
        self.operator_name = operator
        self.operator_func = self.OPERATORS.get(operator)
        if self.operator_func is None:
            raise ValueError(f"Unknown operator: {operator}")
        self.conditions = conditions

    def evaluate(self, context: Dict[str, Any]) -> bool:
        results = [c.evaluate(context) for c in self.conditions]
        return self.operator_func(results)


class TimeCondition(Condition):
    """Condition based on time constraints.

    Example:
        cond = TimeCondition(hour_start=9, hour_end=17, days=[0,1,2,3,4])
        if cond.evaluate({}):
            run_business_hours_task()
    """

    def __init__(
        self,
        hour_start: Optional[int] = None,
        hour_end: Optional[int] = None,
        days: Optional[List[int]] = None,
        interval_seconds: Optional[float] = None,
    ) -> None:
        self.hour_start = hour_start
        self.hour_end = hour_end
        self.days = set(days) if days else None
        self.interval_seconds = interval_seconds
        self._last_trigger_time: float = 0.0

    def evaluate(self, context: Dict[str, Any]) -> bool:
        now = time.localtime()

        if self.days is not None and now.tm_wday not in self.days:
            return False

        if self.hour_start is not None and now.tm_hour < self.hour_start:
            return False

        if self.hour_end is not None and now.tm_hour >= self.hour_end:
            return False

        if self.interval_seconds is not None:
            elapsed = time.time() - self._last_trigger_time
            if elapsed < self.interval_seconds:
                return False

        return True

    def mark_triggered(self) -> None:
        """Mark that trigger fired."""
        self._last_trigger_time = time.time()


class CounterCondition(Condition):
    """Condition based on event counter.

    Example:
        cond = CounterCondition("click_count", min_count=5)
        if cond.evaluate(context):
            print("Button clicked 5 times!")
    """

    def __init__(
        self,
        counter_name: str,
        min_count: int = 1,
        max_count: Optional[int] = None,
        reset_on_true: bool = True,
    ) -> None:
        self.counter_name = counter_name
        self.min_count = min_count
        self.max_count = max_count
        self.reset_on_true = reset_on_true
        self._counters: Dict[str, int] = {}

    def evaluate(self, context: Dict[str, Any]) -> bool:
        count = self._counters.get(self.counter_name, 0)
        count += 1
        self._counters[self.counter_name] = count

        if count < self.min_count:
            return False

        if self.max_count is not None and count > self.max_count:
            return False

        if self.reset_on_true and count >= self.min_count:
            self._counters[self.counter_name] = 0

        return True

    def reset(self, counter_name: Optional[str] = None) -> None:
        """Reset counter(s)."""
        if counter_name:
            self._counters[counter_name] = 0
        else:
            self._counters.clear()

    def get_count(self, counter_name: Optional[str] = None) -> int:
        """Get current count."""
        name = counter_name or self.counter_name
        return self._counters.get(name, 0)


class WorkflowTrigger:
    """Declarative workflow trigger.

    Example:
        trigger = WorkflowTrigger(
            condition=ThresholdCondition("value", 100),
            action=my_action,
            cooldown=60.0
        )
        if trigger.should_fire(context):
            trigger.execute(context)
    """

    def __init__(
        self,
        condition: Condition,
        action: Callable[[Dict[str, Any]], Any],
        cooldown: float = 0.0,
        name: Optional[str] = None,
    ) -> None:
        self.condition = condition
        self.action = action
        self.cooldown = cooldown
        self.name = name or "trigger"
        self._last_fire_time: float = 0.0

    def should_fire(self, context: Dict[str, Any]) -> bool:
        """Check if trigger should fire."""
        if self.cooldown > 0:
            elapsed = time.time() - self._last_fire_time
            if elapsed < self.cooldown:
                return False

        return self.condition.evaluate(context)

    def execute(self, context: Dict[str, Any]) -> Any:
        """Execute trigger action."""
        self._last_fire_time = time.time()
        return self.action(context)

    def fire(self, context: Dict[str, Any]) -> Optional[Any]:
        """Conditionally fire the trigger.

        Returns:
            Action result if fired, None otherwise.
        """
        if self.should_fire(context):
            return self.execute(context)
        return None
