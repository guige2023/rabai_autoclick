"""Automation Playback Action Module.

Provides enhanced playback capabilities with conditional branching,
loop control, variable substitution, and error recovery for recorded
automation sequences.
"""

import time
import re
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class PlaybackCommand(Enum := type("Enum", (), {"PLAY": "play", "PAUSE": "pause", "STOP": "stop", "STEP": "step", "SKIP": "skip"}))):
    PLAY = "play"
    PAUSE = "pause"
    STOP = "stop"
    STEP = "step"
    SKIP = "skip"


@dataclass
class Variable:
    name: str
    value: Any
    readonly: bool = False


@dataclass
class PlaybackStep:
    step_id: str
    action_type: str
    params: Dict[str, Any]
    condition: Optional[str] = None
    loop_count: int = 1
    on_error: str = "abort"
    timeout_ms: int = 30000


@dataclass
class PlaybackResult:
    step_id: str
    success: bool
    duration_ms: float
    output: Any
    error: Optional[str] = None


class AutomationPlaybackAction:
    """Enhanced playback engine with control flow and variable substitution."""

    def __init__(self) -> None:
        self._variables: Dict[str, Variable] = {}
        self._steps: List[PlaybackStep] = []
        self._current_step: int = 0
        self._state = PlaybackCommand.PLAY
        self._results: List[PlaybackResult] = []
        self._breakpoints: set = set()
        self._listeners: Dict[str, List[Callable]] = {
            "step_start": [],
            "step_complete": [],
            "step_error": [],
            "variable_change": [],
        }

    def add_step(
        self,
        step_id: str,
        action_type: str,
        params: Dict[str, Any],
        condition: Optional[str] = None,
        loop_count: int = 1,
        on_error: str = "abort",
        timeout_ms: int = 30000,
    ) -> None:
        step = PlaybackStep(
            step_id=step_id,
            action_type=action_type,
            params=params,
            condition=condition,
            loop_count=loop_count,
            on_error=on_error,
            timeout_ms=timeout_ms,
        )
        self._steps.append(step)

    def set_variable(self, name: str, value: Any, readonly: bool = False) -> None:
        self._variables[name] = Variable(name=name, value=value, readonly=readonly)
        self._notify("variable_change", {"name": name, "value": value})

    def get_variable(self, name: str, default: Any = None) -> Any:
        var = self._variables.get(name)
        return var.value if var else default

    def interpolate(self, text: str) -> str:
        pattern = re.compile(r"\$\{(\w+)(?::([^}]*))?\}")
        def replacer(match):
            name = match.group(1)
            default = match.group(2)
            return str(self.get_variable(name, default or ""))
        return pattern.sub(replacer, text)

    def execute(
        self,
        action_executor: Callable[[str, Dict[str, Any]], Any],
    ) -> Tuple[bool, List[PlaybackResult]]:
        self._results.clear()
        self._current_step = 0
        success = True
        for i, step in enumerate(self._steps):
            self._current_step = i
            if self._state == PlaybackCommand.STOP:
                break
            while self._state == PlaybackCommand.PAUSE:
                time.sleep(0.1)
            if step.condition and not self._evaluate_condition(step.condition):
                logger.debug(f"Skipping step {step.step_id} (condition not met)")
                continue
            for loop_i in range(step.loop_count):
                self._notify("step_start", {"step": step, "loop": loop_i})
                result = self._execute_step(step, action_executor)
                self._results.append(result)
                self._notify("step_complete", {"step": step, "result": result})
                if not result.success:
                    if step.on_error == "abort":
                        success = False
                        self._notify("step_error", {"step": step, "error": result.error})
                        return False, self._results
                    elif step.on_error == "continue":
                        pass
                if self._state == PlaybackCommand.STOP:
                    break
        return success, self._results

    def _execute_step(
        self,
        step: PlaybackStep,
        executor: Callable,
    ) -> PlaybackResult:
        start = time.time()
        try:
            params = {k: self.interpolate(str(v)) for k, v in step.params.items()}
            output = executor(step.action_type, params)
            return PlaybackResult(
                step_id=step.step_id,
                success=True,
                duration_ms=(time.time() - start) * 1000,
                output=output,
            )
        except Exception as e:
            return PlaybackResult(
                step_id=step.step_id,
                success=False,
                duration_ms=(time.time() - start) * 1000,
                output=None,
                error=str(e),
            )

    def _evaluate_condition(self, condition: str) -> bool:
        condition = self.interpolate(condition)
        condition = condition.strip()
        if condition.startswith("var:"):
            var_name = condition[4:].strip()
            return bool(self.get_variable(var_name))
        if "==" in condition:
            left, right = condition.split("==", 1)
            return self.interpolate(left.strip()) == self.interpolate(right.strip())
        if "!=" in condition:
            left, right = condition.split("!=", 1)
            return self.interpolate(left.strip()) != self.interpolate(right.strip())
        return bool(condition)

    def pause(self) -> None:
        self._state = PlaybackCommand.PAUSE

    def resume(self) -> None:
        self._state = PlaybackCommand.PLAY

    def stop(self) -> None:
        self._state = PlaybackCommand.STOP

    def step_forward(self) -> Optional[str]:
        if self._current_step < len(self._steps) - 1:
            self._current_step += 1
            return self._steps[self._current_step].step_id
        return None

    def set_breakpoint(self, step_id: str) -> None:
        self._breakpoints.add(step_id)

    def remove_breakpoint(self, step_id: str) -> None:
        self._breakpoints.discard(step_id)

    def add_listener(self, event: str, callback: Callable) -> None:
        if event in self._listeners:
            self._listeners[event].append(callback)

    def _notify(self, event: str, data: Dict[str, Any]) -> None:
        for cb in self._listeners.get(event, []):
            try:
                cb(data)
            except Exception as e:
                logger.error(f"Playback listener error for {event}: {e}")

    def get_progress(self) -> Dict[str, Any]:
        return {
            "current_step": self._current_step,
            "total_steps": len(self._steps),
            "state": self._state.value,
            "completed": len(self._results),
            "breakpoints": list(self._breakpoints),
        }
