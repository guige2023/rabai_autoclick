"""Workflow variable management for UI automation.

Provides scoped variable storage, templating, and interpolation
for automation workflow parameters.
"""

from __future__ import annotations

import uuid
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class VariableScope(Enum):
    """Variable scope levels."""
    GLOBAL = auto()
    WORKFLOW = auto()
    STEP = auto()


@dataclass
class Variable:
    """A workflow variable.

    Attributes:
        name: Variable name.
        value: Current value.
        scope: Variable scope level.
        vtype: Optional type hint string.
        default: Default value if not set.
        is_secret: Whether value should be masked in logs.
        created_at: Creation timestamp.
        modified_at: Last modification timestamp.
        description: Human-readable description.
    """
    name: str
    value: Any = None
    scope: VariableScope = VariableScope.STEP
    vtype: str = ""
    default: Any = None
    is_secret: bool = False
    created_at: float = field(default_factory=time.time)
    modified_at: float = field(default_factory=time.time)
    description: str = ""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def get(self) -> Any:
        """Return value or default if None."""
        return self.value if self.value is not None else self.default

    def set(self, value: Any) -> None:
        """Set value and update timestamp."""
        self.value = value
        self.modified_at = time.time()

    def reset(self) -> None:
        """Reset to default."""
        self.value = self.default
        self.modified_at = time.time()


class WorkflowVariableStore:
    """Scoped variable storage for workflow automation.

    Supports hierarchical scopes (global > workflow > step)
    with automatic shadowing and variable interpolation.
    """

    def __init__(self) -> None:
        """Initialize empty variable store."""
        self._variables: dict[VariableScope, dict[str, Variable]] = {
            scope: {} for scope in VariableScope
        }

    def define(
        self,
        name: str,
        value: Any = None,
        scope: VariableScope = VariableScope.STEP,
        vtype: str = "",
        default: Any = None,
        is_secret: bool = False,
        description: str = "",
    ) -> Variable:
        """Define a new variable."""
        var = Variable(
            name=name,
            value=value,
            scope=scope,
            vtype=vtype,
            default=default,
            is_secret=is_secret,
            description=description,
        )
        self._variables[scope][name] = var
        return var

    def get(self, name: str, default: Any = None) -> Any:
        """Get a variable's value, searching scopes hierarchically."""
        for scope in [VariableScope.STEP, VariableScope.WORKFLOW, VariableScope.GLOBAL]:
            var = self._variables[scope].get(name)
            if var:
                return var.get()
        return default

    def set(self, name: str, value: Any, scope: VariableScope) -> None:
        """Set a variable at a specific scope."""
        if name in self._variables[scope]:
            self._variables[scope][name].set(value)
        else:
            self.define(name, value, scope)

    def unset(self, name: str, scope: VariableScope) -> bool:
        """Remove a variable at a scope. Returns True if found."""
        if name in self._variables[scope]:
            del self._variables[scope][name]
            return True
        return False

    def has(self, name: str) -> bool:
        """Check if variable exists at any scope."""
        return any(
            name in self._variables[scope]
            for scope in VariableScope
        )

    def get_variable(self, name: str) -> Optional[Variable]:
        """Get the Variable object (highest priority scope)."""
        for scope in [VariableScope.STEP, VariableScope.WORKFLOW, VariableScope.GLOBAL]:
            var = self._variables[scope].get(name)
            if var:
                return var
        return None

    def list_variables(self, scope: Optional[VariableScope] = None) -> list[Variable]:
        """List all variables, optionally filtered by scope."""
        if scope:
            return list(self._variables[scope].values())
        all_vars: list[Variable] = []
        for scope_vars in self._variables.values():
            all_vars.extend(scope_vars.values())
        return all_vars

    def clear_scope(self, scope: VariableScope) -> None:
        """Clear all variables at a scope."""
        self._variables[scope].clear()

    def interpolate(self, template: str) -> str:
        """Interpolate {{variable}} placeholders in a template string.

        Supports:
            {{name}} - simple variable
            {{name|default}} - with default if missing
        """
        import re
        pattern = r'\{\{([^}|]+)(?:\|([^{}]*))?\}\}'

        def replacer(match: re.Match[str]) -> str:
            var_name = match.group(1).strip()
            default_val = match.group(2).strip() if match.group(2) else None
            val = self.get(var_name)
            if val is None:
                return default_val if default_val else ""
            return str(val)

        return re.sub(pattern, replacer, template)

    @property
    def global_vars(self) -> dict[str, Any]:
        """Return dict of global variables."""
        return {
            name: var.get()
            for name, var in self._variables[VariableScope.GLOBAL].items()
        }

    @property
    def workflow_vars(self) -> dict[str, Any]:
        """Return dict of workflow variables."""
        return {
            name: var.get()
            for name, var in self._variables[VariableScope.WORKFLOW].items()
        }

    @property
    def step_vars(self) -> dict[str, Any]:
        """Return dict of step variables."""
        return {
            name: var.get()
            for name, var in self._variables[VariableScope.STEP].items()
        }


# Global store instance
_workflow_store = WorkflowVariableStore()


def get_workflow_store() -> WorkflowVariableStore:
    """Return the global workflow variable store."""
    return _workflow_store
