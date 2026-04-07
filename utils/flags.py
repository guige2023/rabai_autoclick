"""Feature flag utilities for RabAI AutoClick.

Provides:
- Feature flag management
- Flag evaluation
- Flag persistence
"""

import json
import os
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class FlagType(Enum):
    """Flag value types."""
    BOOLEAN = "boolean"
    STRING = "string"
    NUMBER = "number"
    JSON = "json"


@dataclass
class Flag:
    """A feature flag."""
    name: str
    flag_type: FlagType
    default_value: Any
    description: str = ""
    enabled: bool = True


class FlagValue:
    """Flag value with evaluation context."""

    def __init__(
        self,
        flag: Flag,
        value: Any,
        is_default: bool = False,
    ) -> None:
        """Initialize flag value.

        Args:
            flag: Flag definition.
            value: Current value.
            is_default: Whether using default value.
        """
        self._flag = flag
        self._value = value
        self._is_default = is_default

    @property
    def flag(self) -> Flag:
        """Get flag definition."""
        return self._flag

    @property
    def value(self) -> Any:
        """Get flag value."""
        return self._value

    @property
    def is_default(self) -> bool:
        """Check if using default value."""
        return self._is_default


class FlagEvaluator:
    """Evaluate feature flags."""

    def __init__(self) -> None:
        """Initialize evaluator."""
        self._rules: Dict[str, List[Callable[[Any], bool]]] = {}

    def add_rule(
        self,
        flag_name: str,
        rule: Callable[[Any], bool],
    ) -> None:
        """Add evaluation rule for flag.

        Args:
            flag_name: Flag to add rule for.
            rule: Function that returns True if flag should be enabled.
        """
        if flag_name not in self._rules:
            self._rules[flag_name] = []
        self._rules[flag_name].append(rule)

    def evaluate(self, flag: Flag, context: Optional[Dict[str, Any]] = None) -> FlagValue:
        """Evaluate a flag.

        Args:
            flag: Flag to evaluate.
            context: Evaluation context.

        Returns:
            Flag value with evaluation result.
        """
        if not flag.enabled:
            return FlagValue(flag, flag.default_value, is_default=True)

        # Check rules
        rules = self._rules.get(flag.name, [])
        for rule in rules:
            try:
                if context and not rule(context):
                    return FlagValue(flag, flag.default_value, is_default=True)
            except Exception:
                pass

        return FlagValue(flag, flag.default_value, is_default=False)


class FeatureFlags:
    """Manage feature flags."""

    def __init__(self) -> None:
        """Initialize flags."""
        self._flags: Dict[str, Flag] = {}
        self._values: Dict[str, Any] = {}
        self._evaluator = FlagEvaluator()

    def add_flag(
        self,
        name: str,
        flag_type: FlagType,
        default_value: Any,
        description: str = "",
        enabled: bool = True,
    ) -> Flag:
        """Add a feature flag.

        Args:
            name: Flag name.
            flag_type: Type of flag.
            default_value: Default value.
            description: Flag description.
            enabled: Whether flag is enabled.

        Returns:
            Created flag.
        """
        flag = Flag(
            name=name,
            flag_type=flag_type,
            default_value=default_value,
            description=description,
            enabled=enabled,
        )
        self._flags[name] = flag
        return flag

    def get_flag(self, name: str) -> Optional[Flag]:
        """Get flag by name.

        Args:
            name: Flag name.

        Returns:
            Flag or None.
        """
        return self._flags.get(name)

    def set_value(self, name: str, value: Any) -> bool:
        """Set flag value.

        Args:
            name: Flag name.
            value: Value to set.

        Returns:
            True if set successfully.
        """
        if name not in self._flags:
            return False
        self._values[name] = value
        return True

    def get_value(
        self,
        name: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[FlagValue]:
        """Get flag value.

        Args:
            name: Flag name.
            context: Optional evaluation context.

        Returns:
            Flag value or None.
        """
        flag = self._flags.get(name)
        if not flag:
            return None

        # Check overrides
        if name in self._values:
            return FlagValue(flag, self._values[name], is_default=False)

        # Use evaluator
        return self._evaluator.evaluate(flag, context)

    def is_enabled(self, name: str, context: Optional[Dict[str, Any]] = None) -> bool:
        """Check if flag is enabled.

        Args:
            name: Flag name.
            context: Optional context.

        Returns:
            True if enabled.
        """
        flag_value = self.get_value(name, context)
        if not flag_value:
            return False

        if flag_value.flag.flag_type == FlagType.BOOLEAN:
            return bool(flag_value.value)
        return flag_value.value is not None

    def enable(self, name: str) -> bool:
        """Enable a flag.

        Args:
            name: Flag name.

        Returns:
            True if enabled.
        """
        flag = self._flags.get(name)
        if flag:
            flag.enabled = True
            return True
        return False

    def disable(self, name: str) -> bool:
        """Disable a flag.

        Args:
            name: Flag name.

        Returns:
            True if disabled.
        """
        flag = self._flags.get(name)
        if flag:
            flag.enabled = False
            return True
        return False

    def list_flags(self) -> List[Flag]:
        """List all flags.

        Returns:
            List of flags.
        """
        return list(self._flags.values())

    def add_rule(self, flag_name: str, rule: Callable[[Any], bool]) -> None:
        """Add evaluation rule.

        Args:
            flag_name: Flag to add rule for.
            rule: Rule function.
        """
        self._evaluator.add_rule(flag_name, rule)


class FlagPersistence:
    """Persist flags to disk."""

    def __init__(self, path: str) -> None:
        """Initialize persistence.

        Args:
            path: Path to persistence file.
        """
        self._path = path

    def save(self, flags: FeatureFlags) -> bool:
        """Save flags to disk.

        Args:
            flags: Flags to save.

        Returns:
            True if saved.
        """
        data = {
            "flags": [
                {
                    "name": f.name,
                    "type": f.flag_type.value,
                    "default": f.default_value,
                    "description": f.description,
                    "enabled": f.enabled,
                }
                for f in flags.list_flags()
            ],
            "values": flags._values,
        }

        try:
            with open(self._path, "w") as f:
                json.dump(data, f, indent=2)
            return True
        except Exception:
            return False

    def load(self, flags: FeatureFlags) -> bool:
        """Load flags from disk.

        Args:
            flags: Flags instance to load into.

        Returns:
            True if loaded.
        """
        if not os.path.exists(self._path):
            return False

        try:
            with open(self._path, "r") as f:
                data = json.load(f)

            # Load flag definitions
            for flag_data in data.get("flags", []):
                flags.add_flag(
                    name=flag_data["name"],
                    flag_type=FlagType(flag_data["type"]),
                    default_value=flag_data["default"],
                    description=flag_data.get("description", ""),
                    enabled=flag_data.get("enabled", True),
                )

            # Load values
            for name, value in data.get("values", {}).items():
                flags.set_value(name, value)

            return True
        except Exception:
            return False


# Global flags instance
_global_flags = FeatureFlags()


def get_flags() -> FeatureFlags:
    """Get global flags instance.

    Returns:
        Global FeatureFlags instance.
    """
    return _global_flags


def is_flag_enabled(name: str, context: Optional[Dict[str, Any]] = None) -> bool:
    """Check if flag is enabled.

    Args:
        name: Flag name.
        context: Optional context.

    Returns:
        True if enabled.
    """
    return _global_flags.is_enabled(name, context)


def set_flag(name: str, value: Any) -> bool:
    """Set flag value.

    Args:
        name: Flag name.
        value: Value to set.

    Returns:
        True if set.
    """
    return _global_flags.set_value(name, value)
