# Copyright (c) 2024. coded by claude
"""Automation Trigger Action Module.

Implements event-based automation triggers for workflow execution
with support for time-based, event-based, and condition-based triggers.
"""
from typing import Optional, Dict, Any, List, Callable, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging

logger = logging.getLogger(__name__)


class TriggerType(Enum):
    TIME = "time"
    EVENT = "event"
    CONDITION = "condition"
    SCHEDULE = "schedule"


@dataclass
class TriggerConfig:
    trigger_type: TriggerType
    name: str
    enabled: bool = True
    description: Optional[str] = None


@dataclass
class TimeTriggerConfig(TriggerConfig):
    interval_seconds: Optional[float] = None
    schedule: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None


@dataclass
class EventTriggerConfig(TriggerConfig):
    event_types: Set[str] = field(default_factory=set)
    filter_function: Optional[Callable[[Dict[str, Any]], bool]] = None


@dataclass
class ConditionTriggerConfig(TriggerConfig):
    condition_function: Callable[[], bool]
    check_interval: float = 1.0


@dataclass
class TriggerEvent:
    trigger_name: str
    trigger_type: TriggerType
    timestamp: datetime
    data: Dict[str, Any] = field(default_factory=dict)


class AutomationTrigger:
    def __init__(self, config: TriggerConfig):
        self.config = config
        self._handlers: List[Callable] = []
        self._running = False

    def add_handler(self, handler: Callable) -> None:
        self._handlers.append(handler)

    async def execute_handlers(self, event: TriggerEvent) -> None:
        for handler in self._handlers:
            try:
                result = handler(event)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.error(f"Handler failed for trigger '{self.config.name}': {e}")

    def is_enabled(self) -> bool:
        return self.config.enabled


class TimeTrigger(AutomationTrigger):
    def __init__(self, config: TimeTriggerConfig):
        super().__init__(config)
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        while self._running:
            event = TriggerEvent(
                trigger_name=self.config.name,
                trigger_type=TriggerType.TIME,
                timestamp=datetime.now(),
            )
            await self.execute_handlers(event)
            if isinstance(self.config, TimeTriggerConfig) and self.config.interval_seconds:
                await asyncio.sleep(self.config.interval_seconds)
            else:
                break


class EventTrigger(AutomationTrigger):
    def __init__(self, config: EventTriggerConfig):
        super().__init__(config)

    async def on_event(self, event_type: str, event_data: Dict[str, Any]) -> None:
        if not self.is_enabled():
            return
        if isinstance(self.config, EventTriggerConfig):
            if event_type not in self.config.event_types:
                return
            if self.config.filter_function and not self.config.filter_function(event_data):
                return
        event = TriggerEvent(
            trigger_name=self.config.name,
            trigger_type=TriggerType.EVENT,
            timestamp=datetime.now(),
            data=event_data,
        )
        await self.execute_handlers(event)


class ConditionTrigger(AutomationTrigger):
    def __init__(self, config: ConditionTriggerConfig):
        super().__init__(config)
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        while self._running:
            if isinstance(self.config, ConditionTriggerConfig):
                if self.config.condition_function():
                    event = TriggerEvent(
                        trigger_name=self.config.name,
                        trigger_type=TriggerType.CONDITION,
                        timestamp=datetime.now(),
                    )
                    await self.execute_handlers(event)
                await asyncio.sleep(self.config.check_interval)
            else:
                break
