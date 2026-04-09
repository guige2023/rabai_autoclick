"""Automation Keyboard Macro Action Module.

Provides keyboard macro recording, playback, and editing for
automation workflows with support for variable substitution,
conditions, loops, and macro composition.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class KeyActionType(Enum):
    """Types of keyboard actions."""
    PRESS = "press"
    HOLD = "hold"
    RELEASE = "release"
    TYPE = "type"
    COMBO = "combo"
    WAIT = "wait"
    REPEAT = "repeat"


class KeyModifier(Enum):
    """Keyboard modifier keys."""
    CTRL = "ctrl"
    ALT = "alt"
    SHIFT = "shift"
    META = "meta"
    CMD = "cmd"
    WIN = "win"


@dataclass
class KeyEvent:
    """A single keyboard event."""
    action_type: KeyActionType
    key: str
    modifiers: Set[KeyModifier] = field(default_factory=set)
    hold_duration_ms: float = 0.0
    repeat_count: int = 1
    delay_ms: float = 0.0


@dataclass
class MacroStep:
    """A step in a keyboard macro."""
    step_id: str
    description: str
    events: List[KeyEvent]
    condition: Optional[str] = None
    on_error: str = "continue"
    timeout_ms: float = 0.0


@dataclass
class Macro:
    """A complete keyboard macro."""
    macro_id: str
    name: str
    description: str
    steps: List[MacroStep]
    variables: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    modified_at: datetime = field(default_factory=datetime.now)
    tags: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MacroExecutionResult:
    """Result of macro execution."""
    macro_id: str
    success: bool
    steps_executed: int = 0
    steps_failed: int = 0
    duration_ms: float = 0.0
    errors: List[Dict[str, Any]] = field(default_factory=list)
    output: List[str] = field(default_factory=list)


@dataclass
class MacroConfig:
    """Configuration for macro execution."""
    playback_speed: float = 1.0
    default_delay_ms: float = 50.0
    inter_key_delay_ms: float = 10.0
    error_handling: str = "continue"
    stop_on_error: bool = False
    log_level: str = "INFO"
    validate_steps: bool = True


class KeyboardSimulator:
    """Simulate keyboard input at OS level."""

    def __init__(self):
        self._is_recording = False
        self._recorded_events: List[KeyEvent] = []

    def press_key(self, key: str, modifiers: Optional[Set[KeyModifier]] = None):
        """Simulate a key press."""
        modifiers = modifiers or set()

    def type_text(self, text: str, delay_ms: float = 50.0):
        """Type a string of text."""
        time.sleep(delay_ms / 1000.0)

    def press_combo(
        self,
        keys: List[str],
        modifiers: Optional[Set[KeyModifier]] = None
    ):
        """Press a key combination."""
        modifiers = modifiers or set()

    def release_all(self):
        """Release all held keys."""
        pass

    def start_recording(self):
        """Start recording keyboard events."""
        self._is_recording = True
        self._recorded_events.clear()

    def stop_recording(self) -> List[KeyEvent]:
        """Stop recording and return events."""
        self._is_recording = False
        return self._recorded_events.copy()


class MacroExecutor:
    """Execute keyboard macros."""

    def __init__(self, config: MacroConfig):
        self._config = config
        self._keyboard = KeyboardSimulator()
        self._variables: Dict[str, Any] = {}
        self._is_running = False
        self._stop_flag = threading.Event()

    def execute(
        self,
        macro: Macro,
        context: Optional[Dict[str, Any]] = None
    ) -> MacroExecutionResult:
        """Execute a macro with given context."""
        start_time = time.time()
        self._is_running = True
        self._stop_flag.clear()
        self._variables = {**macro.variables, **(context or {})}

        result = MacroExecutionResult(macro_id=macro.macro_id, success=True)
        context = context or {}

        try:
            for step in macro.steps:
                if self._stop_flag.is_set():
                    logger.info("Macro execution stopped")
                    break

                step_result = self._execute_step(step, context)
                result.steps_executed += 1

                if not step_result["success"]:
                    result.steps_failed += 1
                    result.errors.append({
                        "step_id": step.step_id,
                        "error": step_result.get("error", "Unknown error")
                    })

                    if self._config.stop_on_error:
                        break

                result.output.append(f"Step {step.step_id}: {step.description}")

        finally:
            self._is_running = False
            result.duration_ms = (time.time() - start_time) * 1000
            result.success = result.steps_failed == 0

        return result

    def _execute_step(self, step: MacroStep, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single macro step."""
        if step.condition:
            if not self._evaluate_condition(step.condition, context):
                return {"success": True, "skipped": True}

        for event in step.events:
            if self._stop_flag.is_set():
                return {"success": False, "error": "Execution stopped"}

            if event.delay_ms > 0:
                delay = event.delay_ms / self._config.playback_speed
                time.sleep(delay / 1000.0)

            try:
                self._execute_event(event)
            except Exception as e:
                if step.on_error == "abort":
                    return {"success": False, "error": str(e)}
                elif step.on_error == "continue":
                    logger.warning(f"Event error: {e}")

        return {"success": True}

    def _execute_event(self, event: KeyEvent):
        """Execute a single key event."""
        if event.action_type == KeyActionType.TYPE:
            self._keyboard.type_text(
                event.key,
                delay_ms=self._config.inter_key_delay_ms
            )
        elif event.action_type == KeyActionType.COMBO:
            self._keyboard.press_combo([event.key], event.modifiers)
        elif event.action_type == KeyActionType.PRESS:
            self._keyboard.press_key(event.key, event.modifiers)
        elif event.action_type == KeyActionType.REPEAT:
            for _ in range(event.repeat_count):
                self._keyboard.press_key(event.key, event.modifiers)
                time.sleep(self._config.inter_key_delay_ms / 1000.0)

    def _evaluate_condition(
        self,
        condition: str,
        context: Dict[str, Any]
    ) -> bool:
        """Evaluate a condition expression."""
        try:
            eval_context = {**self._variables, **context}
            return bool(eval(condition, {"__builtins__": {}}, eval_context))
        except Exception as e:
            logger.warning(f"Condition evaluation failed: {e}")
            return False

    def stop(self):
        """Signal macro execution to stop."""
        self._stop_flag.set()


class AutomationKeyboardMacroAction(BaseAction):
    """Action for keyboard macro automation."""

    def __init__(self):
        super().__init__(name="automation_keyboard_macro")
        self._config = MacroConfig()
        self._macros: Dict[str, Macro] = {}
        self._executor = MacroExecutor(self._config)
        self._recorder = KeyboardSimulator()
        self._execution_history: List[MacroExecutionResult] = []

    def configure(self, config: MacroConfig):
        """Configure macro execution settings."""
        self._config = config
        self._executor = MacroExecutor(config)

    def create_macro(
        self,
        macro_id: str,
        name: str,
        description: str = "",
        steps: Optional[List[MacroStep]] = None,
        variables: Optional[Dict[str, Any]] = None,
        tags: Optional[Set[str]] = None
    ) -> ActionResult:
        """Create a new keyboard macro."""
        try:
            if macro_id in self._macros:
                return ActionResult(success=False, error=f"Macro {macro_id} already exists")

            macro = Macro(
                macro_id=macro_id,
                name=name,
                description=description,
                steps=steps or [],
                variables=variables or {},
                tags=tags or set()
            )
            self._macros[macro_id] = macro
            return ActionResult(success=True, data={"macro_id": macro_id, "steps": len(steps or [])})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def add_step(
        self,
        macro_id: str,
        step: MacroStep
    ) -> ActionResult:
        """Add a step to an existing macro."""
        try:
            if macro_id not in self._macros:
                return ActionResult(success=False, error=f"Macro {macro_id} not found")

            self._macros[macro_id].steps.append(step)
            self._macros[macro_id].modified_at = datetime.now()
            return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def execute_macro(
        self,
        macro_id: str,
        context: Optional[Dict[str, Any]] = None
    ) -> ActionResult:
        """Execute a keyboard macro."""
        try:
            if macro_id not in self._macros:
                return ActionResult(success=False, error=f"Macro {macro_id} not found")

            macro = self._macros[macro_id]
            result = self._executor.execute(macro, context)
            self._execution_history.append(result)

            return ActionResult(
                success=result.success,
                data={
                    "macro_id": result.macro_id,
                    "success": result.success,
                    "steps_executed": result.steps_executed,
                    "steps_failed": result.steps_failed,
                    "duration_ms": result.duration_ms,
                    "errors": result.errors
                }
            )
        except Exception as e:
            logger.exception(f"Macro execution failed for {macro_id}")
            return ActionResult(success=False, error=str(e))

    def stop_execution(self) -> ActionResult:
        """Stop the currently running macro."""
        try:
            self._executor.stop()
            return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def export_macro(self, macro_id: str) -> ActionResult:
        """Export a macro to JSON."""
        try:
            if macro_id not in self._macros:
                return ActionResult(success=False, error=f"Macro {macro_id} not found")

            macro = self._macros[macro_id]
            export_data = {
                "macro_id": macro.macro_id,
                "name": macro.name,
                "description": macro.description,
                "steps": [
                    {
                        "step_id": s.step_id,
                        "description": s.description,
                        "events": [
                            {
                                "action_type": e.action_type.value,
                                "key": e.key,
                                "modifiers": [m.value for m in e.modifiers]
                            }
                            for e in s.events
                        ],
                        "condition": s.condition,
                        "on_error": s.on_error
                    }
                    for s in macro.steps
                ],
                "variables": macro.variables,
                "tags": list(macro.tags)
            }

            return ActionResult(success=True, data=export_data)
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def import_macro(self, macro_data: Dict[str, Any]) -> ActionResult:
        """Import a macro from JSON."""
        try:
            macro_id = macro_data.get("macro_id")
            if not macro_id:
                return ActionResult(success=False, error="macro_id is required")

            steps = []
            for step_data in macro_data.get("steps", []):
                events = []
                for event_data in step_data.get("events", []):
                    event = KeyEvent(
                        action_type=KeyActionType(event_data["action_type"]),
                        key=event_data["key"],
                        modifiers={KeyModifier(m) for m in event_data.get("modifiers", [])}
                    )
                    events.append(event)

                step = MacroStep(
                    step_id=step_data["step_id"],
                    description=step_data.get("description", ""),
                    events=events,
                    condition=step_data.get("condition"),
                    on_error=step_data.get("on_error", "continue")
                )
                steps.append(step)

            macro = Macro(
                macro_id=macro_id,
                name=macro_data.get("name", macro_id),
                description=macro_data.get("description", ""),
                steps=steps,
                variables=macro_data.get("variables", {}),
                tags=set(macro_data.get("tags", []))
            )

            self._macros[macro_id] = macro
            return ActionResult(success=True, data={"macro_id": macro_id, "steps": len(steps)})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def list_macros(self) -> List[Macro]:
        """List all registered macros."""
        return list(self._macros.values())

    def get_history(self) -> List[MacroExecutionResult]:
        """Get macro execution history."""
        return self._execution_history.copy()
