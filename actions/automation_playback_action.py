# Copyright (c) 2024. coded by claude
"""Automation Playback Action Module.

Provides playback capabilities for recorded automation sequences
with support for variable substitution, conditional execution, and error handling.
"""
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging

logger = logging.getLogger(__name__)


class ActionType(Enum):
    CLICK = "click"
    TYPE = "type"
    WAIT = "wait"
    SCREENSHOT = "screenshot"
    CUSTOM = "custom"


@dataclass
class RecordedAction:
    action_type: ActionType
    timestamp: datetime
    target: Optional[str] = None
    value: Optional[str] = None
    duration_ms: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PlaybackConfig:
    speed: float = 1.0
    stop_on_error: bool = True
    screenshot_on_error: bool = True
    variable_substitution: bool = True


@dataclass
class PlaybackResult:
    success: bool
    actions_executed: int
    actions_failed: int
    total_time_ms: float
    error: Optional[str] = None


class AutomationPlayback:
    def __init__(self, config: Optional[PlaybackConfig] = None):
        self.config = config or PlaybackConfig()
        self._action_handlers: Dict[ActionType, Callable] = {}
        self._variables: Dict[str, Any] = {}

    def register_handler(self, action_type: ActionType, handler: Callable) -> None:
        self._action_handlers[action_type] = handler

    def set_variable(self, name: str, value: Any) -> None:
        self._variables[name] = value

    def get_variable(self, name: str, default: Any = None) -> Any:
        return self._variables.get(name, default)

    async def playback(self, actions: List[RecordedAction]) -> PlaybackResult:
        start_time = datetime.now()
        actions_executed = 0
        actions_failed = 0
        for action in actions:
            try:
                if self.config.variable_substitution:
                    action = self._substitute_variables(action)
                adjusted_duration = self._adjust_duration(action)
                if adjusted_duration:
                    await asyncio.sleep(adjusted_duration)
                await self._execute_action(action)
                actions_executed += 1
            except Exception as e:
                actions_failed += 1
                logger.error(f"Action execution failed: {e}")
                if self.config.stop_on_error:
                    elapsed = (datetime.now() - start_time).total_seconds() * 1000
                    return PlaybackResult(
                        success=False,
                        actions_executed=actions_executed,
                        actions_failed=actions_failed,
                        total_time_ms=elapsed,
                        error=str(e),
                    )
        elapsed = (datetime.now() - start_time).total_seconds() * 1000
        return PlaybackResult(
            success=actions_failed == 0,
            actions_executed=actions_executed,
            actions_failed=actions_failed,
            total_time_ms=elapsed,
        )

    def _substitute_variables(self, action: RecordedAction) -> RecordedAction:
        if action.value and isinstance(action.value, str):
            for var_name, var_value in self._variables.items():
                placeholder = f"${{{var_name}}}"
                if placeholder in action.value:
                    action.value = action.value.replace(placeholder, str(var_value))
        return action

    def _adjust_duration(self, action: RecordedAction) -> Optional[float]:
        if action.duration_ms and self.config.speed > 0:
            return action.duration_ms / self.config.speed
        return None

    async def _execute_action(self, action: RecordedAction) -> None:
        if action.action_type in self._action_handlers:
            handler = self._action_handlers[action.action_type]
            result = handler(action)
            if asyncio.iscoroutine(result):
                await result
        else:
            logger.warning(f"No handler registered for action type: {action.action_type}")
