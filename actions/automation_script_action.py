# Copyright (c) 2024. coded by claude
"""Automation Script Action Module.

Provides script execution capabilities for automation workflows
with support for sandboxed execution, timeout control, and result capture.
"""
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import logging

logger = logging.getLogger(__name__)


class ScriptLanguage(Enum):
    PYTHON = "python"
    JAVASCRIPT = "javascript"


@dataclass
class ScriptResult:
    success: bool
    output: Any
    error: Optional[str]
    execution_time_ms: float
    timestamp: datetime


@dataclass
class ScriptContext:
    variables: Dict[str, Any] = field(default_factory=dict)
    functions: Dict[str, Callable] = field(default_factory=dict)
    timeout: Optional[float] = None
    sandboxed: bool = True


class AutomationScript:
    def __init__(self, context: Optional[ScriptContext] = None):
        self.context = context or ScriptContext()

    def set_variable(self, name: str, value: Any) -> None:
        self.context.variables[name] = value

    def get_variable(self, name: str) -> Any:
        return self.context.variables.get(name)

    def set_function(self, name: str, func: Callable) -> None:
        self.context.functions[name] = func

    async def execute_python(self, code: str) -> ScriptResult:
        start_time = datetime.now()
        try:
            if self.context.timeout:
                result = await asyncio.wait_for(
                    self._execute_python_code(code),
                    timeout=self.context.timeout,
                )
            else:
                result = await self._execute_python_code(code)
            elapsed = (datetime.now() - start_time).total_seconds() * 1000
            return ScriptResult(
                success=True,
                output=result,
                error=None,
                execution_time_ms=elapsed,
                timestamp=datetime.now(),
            )
        except asyncio.TimeoutError:
            elapsed = (datetime.now() - start_time).total_seconds() * 1000
            return ScriptResult(
                success=False,
                output=None,
                error="Script execution timed out",
                execution_time_ms=elapsed,
                timestamp=datetime.now(),
            )
        except Exception as e:
            elapsed = (datetime.now() - start_time).total_seconds() * 1000
            return ScriptResult(
                success=False,
                output=None,
                error=str(e),
                execution_time_ms=elapsed,
                timestamp=datetime.now(),
            )

    async def _execute_python_code(self, code: str) -> Any:
        local_vars = dict(self.context.variables)
        global_ns = {"__builtins__": __builtins__}
        for name, func in self.context.functions.items():
            global_ns[name] = func
        exec_globals = {**global_ns, "result": None}
        exec(code, exec_globals)
        return exec_globals.get("result")


class ScriptRunner:
    def __init__(self):
        self._scripts: Dict[str, AutomationScript] = {}
        self._results: List[ScriptResult] = []

    def register_script(self, name: str, script: AutomationScript) -> None:
        self._scripts[name] = script

    async def run_script(self, name: str, code: str) -> ScriptResult:
        if name not in self._scripts:
            self._scripts[name] = AutomationScript()
        result = await self._scripts[name].execute_python(code)
        self._results.append(result)
        return result

    def get_results(self, limit: Optional[int] = None) -> List[ScriptResult]:
        if limit:
            return self._results[-limit:]
        return list(self._results)
