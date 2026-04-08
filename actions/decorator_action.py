"""Decorator Pattern Action Module.

Provides decorator pattern for dynamic
behavior extension.
"""

import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Wrapper:
    """Wrapper/decorator implementation."""
    wrapper_id: str
    name: str
    wrapped_func: Optional[Callable]
    before_func: Optional[Callable] = None
    after_func: Optional[Callable] = None


class DecoratorManager:
    """Manages decorator pattern."""

    def __init__(self):
        self._wrappers: Dict[str, Wrapper] = {}

    def register(
        self,
        name: str,
        wrapped_func: Optional[Callable] = None,
        before_func: Optional[Callable] = None,
        after_func: Optional[Callable] = None
    ) -> str:
        """Register a wrapper."""
        wrapper_id = f"wrap_{name.lower().replace(' ', '_')}"

        wrapper = Wrapper(
            wrapper_id=wrapper_id,
            name=name,
            wrapped_func=wrapped_func,
            before_func=before_func,
            after_func=after_func
        )

        self._wrappers[wrapper_id] = wrapper
        return wrapper_id

    def execute(self, wrapper_id: str, *args, **kwargs) -> Any:
        """Execute wrapped function."""
        wrapper = self._wrappers.get(wrapper_id)
        if not wrapper:
            raise ValueError(f"Wrapper not found: {wrapper_id}")

        result = None

        if wrapper.before_func:
            wrapper.before_func(*args, **kwargs)

        if wrapper.wrapped_func:
            result = wrapper.wrapped_func(*args, **kwargs)

        if wrapper.after_func:
            wrapper.after_func(result)

        return result


class DecoratorPatternAction(BaseAction):
    """Action for decorator pattern operations."""

    def __init__(self):
        super().__init__("decorator")
        self._manager = DecoratorManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute decorator action."""
        try:
            operation = params.get("operation", "register")

            if operation == "register":
                return self._register(params)
            elif operation == "execute":
                return self._execute(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _register(self, params: Dict) -> ActionResult:
        """Register wrapper."""
        def identity(x):
            return x

        wrapper_id = self._manager.register(
            name=params.get("name", ""),
            wrapped_func=params.get("wrapped_func") or identity,
            before_func=params.get("before_func"),
            after_func=params.get("after_func")
        )
        return ActionResult(success=True, data={"wrapper_id": wrapper_id})

    def _execute(self, params: Dict) -> ActionResult:
        """Execute wrapped function."""
        try:
            result = self._manager.execute(params.get("wrapper_id", ""))
            return ActionResult(success=True, data={"result": result})
        except Exception as e:
            return ActionResult(success=False, message=str(e))
