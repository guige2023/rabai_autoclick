"""Scheduler utilities: cron-like scheduling, task queues, and periodic jobs."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable

__all__ = [
    "Schedule",
    "Scheduler",
    "PeriodicJob",
    "CronSchedule",
]


class ScheduleType(Enum):
    INTERVAL = "interval"
    CRON = "cron"
    ONCE = "once"
    DATETIME = "datetime"


@dataclass
class Schedule:
    """Base schedule configuration."""
    schedule_type: ScheduleType
    interval_seconds: float | None = None
    cron_expr: str | None = None
    run_at: datetime | None = None


@dataclass
class PeriodicJob:
    """A scheduled periodic job."""
    id: str
    name: str
    fn: Callable[..., Any]
    schedule: Schedule
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    max_instances: int = 1
    last_run: datetime | None = None
    next_run: datetime | None = None
    run_count: int = 0


class Scheduler:
    """Thread-safe task scheduler with interval, datetime, and cron support."""

    def __init__(self) -> None:
        self._jobs: dict[str, PeriodicJob] = {}
        self._running = False
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def add_interval(
        self,
        job_id: str,
        fn: Callable[..., Any],
        interval_seconds: float,
        name: str = "",
        **kwargs: Any,
    ) -> PeriodicJob:
        schedule = Schedule(schedule_type=ScheduleType.INTERVAL, interval_seconds=interval_seconds)
        job = PeriodicJob(
            id=job_id,
            name=name or job_id,
            fn=fn,
            schedule=schedule,
            kwargs=kwargs,
        )
        with self._lock:
            self._jobs[job_id] = job
        return job

    def add_once(
        self,
        job_id: str,
        fn: Callable[..., Any],
        run_at: datetime,
        name: str = "",
        **kwargs: Any,
    ) -> PeriodicJob:
        schedule = Schedule(schedule_type=ScheduleType.DATETIME, run_at=run_at)
        job = PeriodicJob(
            id=job_id,
            name=name or job_id,
            fn=fn,
            schedule=schedule,
        )
        with self._lock:
            self._jobs[job_id] = job
        return job

    def remove(self, job_id: str) -> bool:
        with self._lock:
            if job_id in self._jobs:
                del self._jobs[job_id]
                return True
            return False

    def get_next_run(self, job: PeriodicJob) -> datetime | None:
        if job.schedule.schedule_type == ScheduleType.INTERVAL:
            interval = job.schedule.interval_seconds or 60.0
            return datetime.now() + timedelta(seconds=interval)
        elif job.schedule.schedule_type == ScheduleType.DATETIME:
            return job.schedule.run_at
        return None

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)
            self._thread = None

    def _run_loop(self) -> None:
        while self._running:
            now = datetime.now()
            with self._lock:
                for job in list(self._jobs.values()):
                    if not job.enabled:
                        continue
                    next_run = self.get_next_run(job)
                    if next_run and now >= next_run:
                        self._execute_job(job)

            time.sleep(0.5)

    def _execute_job(self, job: PeriodicJob) -> None:
        job.last_run = datetime.now()
        job.run_count += 1
        next_run = self.get_next_run(job)
        if next_run:
            job.next_run = next_run

        try:
            job.fn(*job.args, **job.kwargs)
        except Exception:
            pass

    def list_jobs(self) -> list[PeriodicJob]:
        with self._lock:
            return list(self._jobs.values())

    def enable(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job.enabled = True
                return True
            return False

    def disable(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job.enabled = False
                return True
            return False


class CronSchedule:
    """Simple cron expression parser (5-field: min hour day month weekday)."""

    @staticmethod
    def parse(expr: str) -> dict[str, list[int]]:
        parts = expr.split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expr: {expr}")
        fields = ["minute", "hour", "day", "month", "weekday"]
        result = {}
        for field_name, part in zip(fields, parts):
            result[field_name] = CronSchedule._parse_field(part)
        return result

    @staticmethod
    def _parse_field(field_str: str) -> list[int]:
        if field_str == "*":
            return list(range(0 if field_str == "*" else 0, 60))
        values = []
        for segment in field_str.split(","):
            if "-" in segment:
                start, end = segment.split("-", 1)
                values.extend(range(int(start), int(end) + 1))
            elif "/" in segment:
                base, step = segment.split("/", 1)
                start = 0 if base == "*" else int(base)
                for i in range(start, 60, int(step)):
                    values.append(i)
            else:
                values.append(int(segment))
        return sorted(set(values))

    @staticmethod
    def next_run(expr: str, after: datetime | None = None) -> datetime:
        parsed = CronSchedule.parse(expr)
        after = after or datetime.now()
        minute_vals = parsed["minute"]
        hour_vals = parsed["hour"]
        candidates = []
        for h in hour_vals:
            for m in minute_vals:
                if h > after.hour or (h == after.hour and m > after.minute):
                    candidates.append(datetime(after.year, after.month, after.day, h, m))
        if not candidates:
            return after + timedelta(days=1)
        return min(candidates)
