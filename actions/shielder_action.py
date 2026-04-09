"""
Shielder Action Module.

Provides protection and safety mechanisms for actions
including validation, checks, and guards.
"""

import time
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum


class ShieldLevel(Enum):
    """Protection levels for shields."""
    NONE = "none"
    SOFT = "soft"
    MEDIUM = "medium"
    HARD = "hard"


@dataclass
class ShieldRule:
    """A rule that must be satisfied."""
    name: str
    check: Callable[[Any], bool]
    error_message: str = "Rule check failed"
    severity: str = "error"


@dataclass
class ShieldViolation:
    """A violated rule."""
    rule_name: str
    severity: str
    message: str
    timestamp: float = field(default_factory=time.time)


@dataclass
class ShieldConfig:
    """Configuration for shield behavior."""
    level: ShieldLevel = ShieldLevel.MEDIUM
    strict: bool = True
    timeout: Optional[float] = None
    max_violations: int = 10


class Shield:
    """Protection shield for actions."""

    def __init__(self, name: str, config: Optional[ShieldConfig] = None):
        self.name = name
        self.config = config or ShieldConfig()
        self._rules: List[ShieldRule] = []
        self._violations: List[ShieldViolation] = []
        self._lock = threading.RLock()
        self._enabled = True

    def add_rule(
        self,
        name: str,
        check: Callable[[Any], bool],
        error_message: str = "Rule check failed",
        severity: str = "error",
    ) -> "Shield":
        """Add a protection rule."""
        self._rules.append(
            ShieldRule(
                name=name,
                check=check,
                error_message=error_message,
                severity=severity,
            )
        )
        return self

    def remove_rule(self, name: str) -> bool:
        """Remove a rule by name."""
        with self._lock:
            for i, rule in enumerate(self._rules):
                if rule.name == name:
                    self._rules.pop(i)
                    return True
            return False

    def _validate_input(self, data: Any) -> List[ShieldViolation]:
        """Validate input against all rules."""
        violations = []

        for rule in self._rules:
            try:
                if not rule.check(data):
                    violations.append(
                        ShieldViolation(
                            rule_name=rule.name,
                            severity=rule.severity,
                            message=rule.error_message,
                        )
                    )
            except Exception as e:
                violations.append(
                    ShieldViolation(
                        rule_name=rule.name,
                        severity="error",
                        message=f"Rule check raised exception: {e}",
                    )
                )

        return violations

    def _handle_violations(
        self,
        violations: List[ShieldViolation],
    ) -> None:
        """Handle rule violations based on config."""
        self._violations.extend(violations)

        if not violations:
            return

        error_count = sum(1 for v in violations if v.severity == "error")

        if self.config.strict and error_count > 0:
            messages = [v.message for v in violations]
            raise ValueError(f"Shield violations: {'; '.join(messages)}")

    def wrap(self, func: Callable) -> Callable:
        """Wrap a function with this shield."""
        def wrapper(*args, **kwargs):
            if not self._enabled:
                return func(*args, **kwargs)

            start_time = time.time()

            if self.config.timeout:
                violations = self._validate_input((args, kwargs))
                self._handle_violations(violations)

            try:
                result = func(*args, **kwargs)

                if self.config.timeout and time.time() - start_time > self.config.timeout:
                    raise TimeoutError(f"Execution exceeded timeout of {self.config.timeout}s")

                return result

            except Exception as e:
                if self.config.strict:
                    raise
                return None

        return wrapper

    def validate(self, data: Any) -> Tuple[bool, List[ShieldViolation]]:
        """Validate data against rules without executing."""
        violations = self._validate_input(data)
        return len(violations) == 0, violations

    def get_violations(
        self,
        since: Optional[float] = None,
    ) -> List[ShieldViolation]:
        """Get violations, optionally filtered by timestamp."""
        with self._lock:
            if since:
                return [v for v in self._violations if v.timestamp >= since]
            return list(self._violations)

    def clear_violations(self) -> None:
        """Clear all recorded violations."""
        with self._lock:
            self._violations.clear()

    def enable(self) -> None:
        """Enable the shield."""
        self._enabled = True

    def disable(self) -> None:
        """Disable the shield."""
        self._enabled = False

    @property
    def is_enabled(self) -> bool:
        """Check if shield is enabled."""
        return self._enabled


class ShielderAction:
    """
    Action that provides safety shields.

    Example:
        shielder = ShielderAction("safe_action")
        shielder.add_rule("not_none", lambda x: x is not None, "Input cannot be None")
        shielder.add_rule("positive", lambda x: x > 0, "Input must be positive")
        safe_func = shielder.wrap(my_function)
    """

    def __init__(self, name: str, config: Optional[ShieldConfig] = None):
        self.name = name
        self._shields: Dict[str, Shield] = {}
        self._default_shield: Shield
        self._lock = threading.RLock()

        default_config = config or ShieldConfig()
        self._default_shield = Shield(f"{name}_default", default_config)

    def create_shield(
        self,
        shield_name: str,
        config: Optional[ShieldConfig] = None,
    ) -> Shield:
        """Create a named shield."""
        with self._lock:
            shield = Shield(shield_name, config)
            self._shields[shield_name] = shield
            return shield

    def get_shield(self, name: str) -> Optional[Shield]:
        """Get a shield by name."""
        return self._shields.get(name)

    def add_rule(
        self,
        name: str,
        check: Callable[[Any], bool],
        error_message: str = "Rule check failed",
        severity: str = "error",
        shield_name: Optional[str] = None,
    ) -> "ShielderAction":
        """Add a rule to the default or named shield."""
        shield = (
            self._shields.get(shield_name)
            if shield_name
            else self._default_shield
        )
        if shield:
            shield.add_rule(name, check, error_message, severity)
        return self

    def wrap(
        self,
        func: Callable,
        shield_name: Optional[str] = None,
    ) -> Callable:
        """Wrap a function with a shield."""
        shield = (
            self._shields.get(shield_name)
            if shield_name
            else self._default_shield
        )
        if not shield:
            return func
        return shield.wrap(func)

    def execute(
        self,
        func: Callable,
        data: Any,
        shield_name: Optional[str] = None,
    ) -> Any:
        """Execute function with shield protection."""
        shield = (
            self._shields.get(shield_name)
            if shield_name
            else self._default_shield
        )
        if not shield:
            return func(data)

        wrapped = shield.wrap(func)
        return wrapped(data)

    def validate(
        self,
        data: Any,
        shield_name: Optional[str] = None,
    ) -> Tuple[bool, List[ShieldViolation]]:
        """Validate data against shield rules."""
        shield = (
            self._shields.get(shield_name)
            if shield_name
            else self._default_shield
        )
        if not shield:
            return True, []
        return shield.validate(data)

    def get_all_violations(
        self,
        since: Optional[float] = None,
    ) -> Dict[str, List[ShieldViolation]]:
        """Get violations from all shields."""
        result = {}
        for name, shield in self._shields.items():
            result[name] = shield.get_violations(since)
        result["default"] = self._default_shield.get_violations(since)
        return result

    def reset(self) -> None:
        """Reset all shields."""
        for shield in self._shields.values():
            shield.clear_violations()
        self._default_shield.clear_violations()
