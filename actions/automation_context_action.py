"""Automation Context Action Module.

Manages shared execution context across automation workflow steps,
including state persistence, variable scoping, and context inheritance.
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime
import threading
import time
import copy
import logging

logger = logging.getLogger(__name__)


@dataclass
class ContextVariable:
    """A variable stored in the execution context."""
    name: str
    value: Any
    scope: str = "global"  # global, step, workflow
    created_at: float = field(default_factory=time.time)
    modified_at: float = field(default_factory=time.time)
    readonly: bool = False


class AutomationContextAction:
    """Manages shared execution context for automation workflows.
    
    Provides scoped variable storage, change tracking, and
    context isolation between concurrent workflow branches.
    """

    def __init__(self, parent: Optional["AutomationContextAction"] = None) -> None:
        self._variables: Dict[str, ContextVariable] = {}
        self._lock = threading.RLock()
        self._parent = parent
        self._children: List["AutomationContextAction"] = []
        self._step_id: Optional[str] = None
        self._change_log: List[Dict[str, Any]] = []
        if parent:
            parent._children.append(self)

    def set(
        self,
        name: str,
        value: Any,
        scope: str = "global",
        readonly: bool = False,
    ) -> None:
        """Set a context variable.
        
        Args:
            name: Variable name.
            value: Variable value.
            scope: Scope: global, step, or workflow.
            readonly: If True, prevents modification.
        """
        with self._lock:
            if name in self._variables and self._variables[name].readonly:
                raise ValueError(f"Cannot modify readonly variable: {name}")
            self._variables[name] = ContextVariable(
                name=name, value=value, scope=scope, readonly=readonly,
                created_at=time.time(), modified_at=time.time(),
            )
            self._change_log.append({
                "action": "set", "name": name, "scope": scope,
                "timestamp": time.time(),
            })

    def get(self, name: str, default: Any = None) -> Any:
        """Get a context variable.
        
        Args:
            name: Variable name.
            default: Default value if not found.
        
        Returns:
            Variable value or default.
        """
        with self._lock:
            if name in self._variables:
                return self._variables[name].value
            if self._parent:
                return self._parent.get(name, default)
        return default

    def get_all(self, scope: Optional[str] = None) -> Dict[str, Any]:
        """Get all variables, optionally filtered by scope.
        
        Args:
            scope: If provided, only return variables of this scope.
        
        Returns:
            Dict of variable names to values.
        """
        with self._lock:
            result = {}
            if self._parent:
                result.update(self._parent.get_all(scope))
            for name, var in self._variables.items():
                if scope is None or var.scope == scope:
                    result[name] = var.value
            return result

    def delete(self, name: str) -> bool:
        """Delete a variable.
        
        Returns:
            True if variable was found and deleted.
        """
        with self._lock:
            if name in self._variables:
                del self._variables[name]
                return True
        return False

    def fork(self) -> "AutomationContextAction":
        """Create a child context that inherits from this one.
        
        Returns:
            New child AutomationContextAction.
        """
        return AutomationContextAction(parent=self)

    def merge_back(self, child: "AutomationContextAction") -> None:
        """Merge variables from a child context back into this one.
        
        Args:
            child: The child context to merge.
        """
        with self._lock:
            for name, var in child._variables.items():
                if var.scope in ("global", "workflow"):
                    self.set(name, var.value, scope=var.scope)

    def push_step(self, step_id: str) -> None:
        """Enter a new step, creating step-scoped context."""
        self._step_id = step_id

    def pop_step(self) -> None:
        """Exit the current step."""
        self._step_id = None
        with self._lock:
            to_delete = [n for n, v in self._variables.items() if v.scope == "step"]
            for n in to_delete:
                del self._variables[n]

    def get_change_log(self, since: Optional[float] = None) -> List[Dict[str, Any]]:
        """Get the change log, optionally filtered by time."""
        with self._lock:
            if since is None:
                return list(self._change_log)
            return [e for e in self._change_log if e["timestamp"] >= since]
