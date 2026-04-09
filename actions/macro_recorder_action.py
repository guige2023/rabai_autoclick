"""
Macro Recording and Playback Module.

Records sequences of automation actions as reusable macros.
Supports conditional branches, loops, variables, and parameterized execution.

Author: AutoGen
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Set, Tuple, Union

logger = logging.getLogger(__name__)


class MacroCommand(Enum):
    CLICK = auto()
    TYPE = auto()
    WAIT = auto()
    PRESS = auto()
    SCROLL = auto()
    CONDITION = auto()
    LOOP = auto()
    ASSIGN = auto()
    CALL = auto()
    GOTO = auto()
    LABEL = auto()
    SCREENSHOT = auto()
    EXECUTE = auto()
    COMMENT = auto()


@dataclass
class MacroVariable:
    name: str
    vtype: str = "string"
    default_value: Any = None
    description: str = ""


@dataclass
class MacroStep:
    step_id: str
    command: MacroCommand
    params: Dict[str, Any] = field(default_factory=dict)
    label: Optional[str] = None
    condition: Optional[str] = None
    loop_count: Optional[int] = None
    timeout_ms: float = 0.0
    retry_on_fail: bool = False
    comment: str = ""


@dataclass
class MacroDefinition:
    macro_id: str
    name: str
    description: str = ""
    version: str = "1.0"
    variables: List[MacroVariable] = field(default_factory=list)
    steps: List[MacroStep] = field(default_factory=list)
    tags: FrozenSet[str] = field(default_factory=frozenset)
    created_at: datetime = field(default_factory=datetime.utcnow)
    modified_at: datetime = field(default_factory=datetime.utcnow)
    author: str = "system"
    timeout_seconds: float = 3600.0
    max_iterations: int = 10000


@dataclass
class MacroExecutionContext:
    variables: Dict[str, Any] = field(default_factory=dict)
    counters: Dict[str, int] = field(default_factory=dict)
    current_label: Optional[str] = field(default=None)
    execution_log: List[Dict[str, Any]] = field(default_factory=list)
    start_time: datetime = field(default_factory=datetime.utcnow)


@dataclass
class MacroResult:
    success: bool
    output: Any = None
    error: Optional[str] = None
    steps_executed: int = 0
    duration_ms: float = 0.0
    log: List[Dict[str, Any]] = field(default_factory=list)


class MacroParser:
    """Parses macro definitions from JSON/YAML."""

    @classmethod
    def from_json(cls, data: str) -> MacroDefinition:
        obj = json.loads(data)
        return cls._parse_dict(obj)

    @classmethod
    def _parse_dict(cls, obj: Dict[str, Any]) -> MacroDefinition:
        variables = [
            MacroVariable(
                name=v.get("name", ""),
                vtype=v.get("type", "string"),
                default_value=v.get("default"),
                description=v.get("description", ""),
            )
            for v in obj.get("variables", [])
        ]

        steps = []
        for i, s in enumerate(obj.get("steps", [])):
            cmd_str = s.get("command", "").upper().replace(" ", "_")
            try:
                command = MacroCommand[cmd_str]
            except KeyError:
                command = MacroCommand.EXECUTE

            step = MacroStep(
                step_id=s.get("id", f"step_{i}"),
                command=command,
                params=s.get("params", {}),
                label=s.get("label"),
                condition=s.get("condition"),
                loop_count=s.get("loop_count"),
                timeout_ms=s.get("timeout_ms", 0),
                retry_on_fail=s.get("retry_on_fail", False),
                comment=s.get("comment", ""),
            )
            steps.append(step)

        return MacroDefinition(
            macro_id=obj.get("id", "unknown"),
            name=obj.get("name", "Unnamed Macro"),
            description=obj.get("description", ""),
            version=obj.get("version", "1.0"),
            variables=variables,
            steps=steps,
            tags=frozenset(obj.get("tags", [])),
            author=obj.get("author", "system"),
            timeout_seconds=obj.get("timeout_seconds", 3600.0),
            max_iterations=obj.get("max_iterations", 10000),
        )

    @classmethod
    def to_json(cls, macro: MacroDefinition) -> str:
        return json.dumps(
            {
                "id": macro.macro_id,
                "name": macro.name,
                "description": macro.description,
                "version": macro.version,
                "variables": [
                    {
                        "name": v.name,
                        "type": v.vtype,
                        "default": v.default_value,
                        "description": v.description,
                    }
                    for v in macro.variables
                ],
                "steps": [
                    {
                        "id": s.step_id,
                        "command": s.command.name,
                        "params": s.params,
                        "label": s.label,
                        "condition": s.condition,
                        "loop_count": s.loop_count,
                        "timeout_ms": s.timeout_ms,
                        "retry_on_fail": s.retry_on_fail,
                        "comment": s.comment,
                    }
                    for s in macro.steps
                ],
                "tags": list(macro.tags),
                "author": macro.author,
                "timeout_seconds": macro.timeout_seconds,
                "max_iterations": macro.max_iterations,
            },
            indent=2,
        )


class MacroEngine:
    """
    Executes macro definitions with support for variables,
    conditions, loops, and sub-macro calls.
    """

    def __init__(self):
        self._macros: Dict[str, MacroDefinition] = {}
        self._handlers: Dict[MacroCommand, Callable] = {}
        self._subroutine_handlers: Dict[str, Callable] = {}

    def register_macro(self, macro: MacroDefinition) -> None:
        self._macros[macro.macro_id] = macro
        logger.info("Registered macro: %s", macro.macro_id)

    def register_handler(
        self, command: MacroCommand, handler: Callable[[Any], Any]
    ) -> None:
        self._handlers[command] = handler

    def register_subroutine(self, name: str, handler: Callable) -> None:
        self._subroutine_handlers[name] = handler

    async def execute(
        self,
        macro_id: str,
        input_params: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> MacroResult:
        macro = self._macros.get(macro_id)
        if not macro:
            return MacroResult(
                success=False, error=f"Macro not found: {macro_id}"
            )

        context = MacroExecutionContext(
            start_time=datetime.utcnow(),
            variables={v.name: v.default_value for v in macro.variables},
        )

        if input_params:
            context.variables.update(input_params)

        start_time = time.time()
        step_index = 0
        iterations = 0

        try:
            while step_index < len(macro.steps):
                if iterations > macro.max_iterations:
                    return MacroResult(
                        success=False,
                        error=f"Max iterations exceeded: {macro.max_iterations}",
                        steps_executed=step_index,
                        duration_ms=(time.time() - start_time) * 1000,
                        log=context.execution_log,
                    )

                step = macro.steps[step_index]

                if step.label:
                    context.current_label = step.label

                if step.condition and not self._evaluate_condition(
                    step.condition, context
                ):
                    step_index += 1
                    continue

                if step.loop_count:
                    loop_vars = context.variables.copy()
                    for i in range(step.loop_count):
                        context.counters["loop_index"] = i
                        for j, loop_step in enumerate(macro.steps[step_index + 1 :]):
                            if loop_step.command == MacroCommand.LABEL:
                                break
                            await self._execute_step(loop_step, context)

                else:
                    success = await self._execute_step(step, context)
                    if not success and step.retry_on_fail:
                        for retry in range(3):
                            await asyncio.sleep(0.5 * (retry + 1))
                            if await self._execute_step(step, context):
                                break

                step_index += 1
                iterations += 1

            return MacroResult(
                success=True,
                output=context.variables.get("_result"),
                steps_executed=step_index,
                duration_ms=(time.time() - start_time) * 1000,
                log=context.execution_log,
            )

        except Exception as exc:
            logger.error("Macro execution error: %s", exc)
            return MacroResult(
                success=False,
                error=str(exc),
                steps_executed=step_index,
                duration_ms=(time.time() - start_time) * 1000,
                log=context.execution_log,
            )

    async def _execute_step(self, step: MacroStep, context: MacroExecutionContext) -> bool:
        context.execution_log.append(
            {
                "step_id": step.step_id,
                "command": step.command.name,
                "timestamp": datetime.utcnow().isoformat(),
            }
        )

        if step.command == MacroCommand.LABEL:
            return True

        if step.command == MacroCommand.ASSIGN:
            var_name = step.params.get("variable", "")
            value = step.params.get("value", "")
            if var_name:
                context.variables[var_name] = self._resolve_value(value, context)
            return True

        if step.command == MacroCommand.CALL:
            subroutine_name = step.params.get("name", "")
            handler = self._subroutine_handlers.get(subroutine_name)
            if handler:
                try:
                    args = self._resolve_value(step.params.get("args", {}), context)
                    if asyncio.iscoroutinefunction(handler):
                        await handler(args)
                    else:
                        handler(args)
                except Exception as exc:
                    logger.error("Subroutine error: %s", exc)
                    return False
            return True

        handler = self._handlers.get(step.command)
        if handler:
            try:
                resolved_params = {
                    k: self._resolve_value(v, context) for k, v in step.params.items()
                }
                if asyncio.iscoroutinefunction(handler):
                    await handler(resolved_params)
                else:
                    handler(resolved_params)
                return True
            except Exception as exc:
                logger.error("Step execution error: %s", exc)
                return False

        return True

    def _resolve_value(self, value: Any, context: MacroExecutionContext) -> Any:
        if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
            var_name = value[2:-1]
            return context.variables.get(var_name, value)
        if isinstance(value, dict):
            return {k: self._resolve_value(v, context) for k, v in value.items()}
        if isinstance(value, list):
            return [self._resolve_value(v, context) for v in value]
        return value

    def _evaluate_condition(
        self, condition: str, context: MacroExecutionContext
    ) -> bool:
        try:
            resolved = self._resolve_value(condition, context)
            return bool(resolved)
        except Exception:
            return False


class MacroRecorder:
    """
    Records user actions as macro definitions.
    """

    def __init__(self, engine: MacroEngine):
        self.engine = engine
        self._recording: Optional[MacroDefinition] = None
        self._step_counter: int = 0

    def start_recording(
        self, macro_id: str, name: str, description: str = ""
    ) -> None:
        self._recording = MacroDefinition(
            macro_id=macro_id,
            name=name,
            description=description,
            steps=[],
        )
        self._step_counter = 0
        logger.info("Started recording macro: %s", name)

    def record_action(
        self,
        command: MacroCommand,
        params: Dict[str, Any],
        comment: str = "",
    ) -> None:
        if not self._recording:
            return

        self._step_counter += 1
        step = MacroStep(
            step_id=f"step_{self._step_counter}",
            command=command,
            params=params,
            comment=comment,
        )
        self._recording.steps.append(step)

    def stop_recording(self) -> MacroDefinition:
        macro = self._recording
        self._recording = None
        if macro:
            self.engine.register_macro(macro)
            logger.info(
                "Stopped recording: %s (%d steps)", macro.name, len(macro.steps)
            )
        return macro or MacroDefinition(macro_id="", name="")

    def is_recording(self) -> bool:
        return self._recording is not None

    def add_variable(
        self, name: str, vtype: str = "string", default: Any = None
    ) -> None:
        if self._recording:
            self._recording.variables.append(
                MacroVariable(name=name, vtype=vtype, default_value=default)
            )
