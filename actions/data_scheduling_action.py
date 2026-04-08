"""Data Scheduling Action.

Schedules data processing tasks with cron expressions and intervals.
"""
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
import time
import re


@dataclass
class Schedule:
    name: str
    interval_sec: Optional[float] = None
    cron: Optional[str] = None
    enabled: bool = True
    last_run: Optional[float] = None
    next_run: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataSchedulingAction:
    """Schedules and triggers data processing tasks."""

    CRON_RE = re.compile(r'^(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)$')

    def __init__(self) -> None:
        self.schedules: Dict[str, Schedule] = {}
        self._handlers: Dict[str, Callable] = {}

    def add_interval(
        self,
        name: str,
        interval_sec: float,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Schedule:
        schedule = Schedule(
            name=name,
            interval_sec=interval_sec,
            next_run=time.time() + interval_sec,
            metadata=metadata or {},
        )
        self.schedules[name] = schedule
        return schedule

    def add_cron(
        self,
        name: str,
        cron_expr: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Schedule:
        if not self.CRON_RE.match(cron_expr):
            raise ValueError(f"Invalid cron expression: {cron_expr}")
        schedule = Schedule(name=name, cron=cron_expr, metadata=metadata or {})
        self.schedules[name] = schedule
        return schedule

    def register_handler(self, name: str, handler: Callable) -> None:
        self._handlers[name] = handler

    def is_due(self, name: str) -> bool:
        schedule = self.schedules.get(name)
        if not schedule or not schedule.enabled:
            return False
        now = time.time()
        if schedule.next_run is None:
            return True
        return now >= schedule.next_run

    def trigger(self, name: str) -> Optional[Any]:
        if not self.is_due(name):
            return None
        schedule = self.schedules[name]
        handler = self._handlers.get(name)
        result = None
        if handler:
            result = handler()
        schedule.last_run = time.time()
        if schedule.interval_sec:
            schedule.next_run = time.time() + schedule.interval_sec
        return result

    def run_due(self) -> List[Dict[str, Any]]:
        results = []
        for name in self.schedules:
            if self.is_due(name):
                result = self.trigger(name)
                results.append({"name": name, "result": result})
        return results

    def get_status(self) -> Dict[str, Any]:
        now = time.time()
        return {
            name: {
                "enabled": s.enabled,
                "last_run": s.last_run,
                "next_run": s.next_run,
                "is_due": self.is_due(name),
            }
            for name, s in self.schedules.items()
        }

    def enable(self, name: str) -> None:
        if name in self.schedules:
            self.schedules[name].enabled = True

    def disable(self, name: str) -> None:
        if name in self.schedules:
            self.schedules[name].enabled = False
