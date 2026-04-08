"""
Sandbox Utilities

Provides utilities for sandboxed execution
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable, TypeVar
import sys

T = TypeVar("T")


class Sandbox:
    """
    Provides sandboxed execution environment.
    
    Runs code with restricted access to
    system resources.
    """

    def __init__(self, allowed_modules: list[str] | None = None) -> None:
        self._allowed_modules = allowed_modules or []
        self._blocked_attrs = {"__import__", "eval", "exec", "open"}

    def execute(
        self,
        code: str,
        context: dict[str, Any] | None = None,
    ) -> Any:
        """
        Execute code in sandbox.
        
        Args:
            code: Code string to execute.
            context: Context dict for local variables.
            
        Returns:
            Execution result.
        """
        context = context or {}
        try:
            compiled = compile(code, "<sandbox>", "eval")
            result = eval(compiled, {"__builtins__": {}}, context)
            return result
        except Exception as e:
            return e

    def is_module_allowed(self, module_name: str) -> bool:
        """Check if module is allowed."""
        if not self._allowed_modules:
            return True
        return module_name in self._allowed_modules
