"""Scheduler utilities for RabAI AutoClick.

Provides:
- Task scheduling
- Cron-like scheduling
- Periodic tasks
"""

import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, List, Optional


@dataclass
class ScheduledTask:
    """Represents a scheduled task."""
    name: str
    callback: Callable
    interval: float  # seconds
    last_run: float = 0
    next_run: float = 0
    enabled: bool = True

    def should_run(self) -> bool:
        """Check if task should run now."""
        return self.enabled and time.time() >= self.next_run


class Scheduler:
    """Task scheduler.

    Usage:
        scheduler = Scheduler()
        scheduler.add_task("task1", my_func, interval=60)  # Every 60 seconds
        scheduler.start()
    """

    def __init__(self) -> None:
        self._tasks: List[ScheduledTask] = []
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def add_task(
        self,
        name: str,
        callback: Callable,
        interval: float,
        initial_delay: float = 0,
    ) -> None:
        """Add a scheduled task.

        Args:
            name: Task name.
            callback: Function to call.
            interval: Interval in seconds.
            initial_delay: Initial delay before first run.
        """
        now = time.time()
        task = ScheduledTask(
            name=name,
            callback=callback,
            interval=interval,
            next_run=now + initial_delay,
        )
        self._tasks.append(task)

    def remove_task(self, name: str) -> bool:
        """Remove a task.

        Args:
            name: Task name.

        Returns:
            True if removed.
        """
        for i, task in enumerate(self._tasks):
            if task.name == name:
                self._tasks.pop(i)
                return True
        return False

    def get_task(self, name: str) -> Optional[ScheduledTask]:
        """Get task by name.

        Args:
            name: Task name.

        Returns:
            Task or None.
        """
        for task in self._tasks:
            if task.name == name:
                return task
        return None

    def enable_task(self, name: str) -> bool:
        """Enable a task.

        Args:
            name: Task name.

        Returns:
            True if found and enabled.
        """
        task = self.get_task(name)
        if task:
            task.enabled = True
            return True
        return False

    def disable_task(self, name: str) -> bool:
        """Disable a task.

        Args:
            name: Task name.

        Returns:
            True if found and disabled.
        """
        task = self.get_task(name)
        if task:
            task.enabled = False
            return True
        return False

    def start(self) -> None:
        """Start the scheduler."""
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the scheduler."""
        if not self._running:
            return

        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _run_loop(self) -> None:
        """Main scheduler loop."""
        while self._running:
            now = time.time()

            for task in self._tasks:
                if task.should_run():
                    try:
                        task.callback()
                    except Exception:
                        pass

                    task.last_run = now
                    task.next_run = now + task.interval

            # Sleep a bit to avoid busy waiting
            time.sleep(0.1)

    @property
    def tasks(self) -> List[ScheduledTask]:
        """Get all tasks."""
        return self._tasks.copy()


class CronScheduler:
    """Cron-like scheduler.

    Supports cron expressions for scheduling.
    """

    def __init__(self) -> None:
        self._tasks: List[dict] = []
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def add_cron(
        self,
        name: str,
        callback: Callable,
        cron_expr: str,
    ) -> None:
        """Add a cron-style task.

        Args:
            name: Task name.
            callback: Function to call.
            cron_expr: Cron expression (simplified).
        """
        # Simplified cron: "*/5 * * * *" means every 5 minutes
        parts = cron_expr.split()
        if len(parts) != 5:
            raise ValueError("Invalid cron expression")

        interval = self._parse_cron(cron_expr)
        scheduler = Scheduler()
        scheduler.add_task(name, callback, interval=interval)

        self._tasks.append({
            "name": name,
            "callback": callback,
            "scheduler": scheduler,
        })

    def _parse_cron(self, expr: str) -> float:
        """Parse cron expression to interval in seconds."""
        parts = expr.split()
        if parts[0].startswith("*/"):
            minutes = int(parts[0][2:])
            return minutes * 60
        return 60  # Default 1 minute

    def start(self) -> None:
        """Start all scheduled tasks."""
        self._running = True
        for task in self._tasks:
            task["scheduler"].start()

    def stop(self) -> None:
        """Stop all scheduled tasks."""
        self._running = False
        for task in self._tasks:
            task["scheduler"].stop()


class DelayedTask:
    """Execute a task after a delay.

    Usage:
        task = DelayedTask(lambda: print("Done!"), delay=5)
        task.start()  # Executes after 5 seconds
    """

    def __init__(
        self,
        callback: Callable,
        delay: float,
        name: Optional[str] = None,
    ) -> None:
        """Initialize delayed task.

        Args:
            callback: Function to execute.
            delay: Delay in seconds.
            name: Optional task name.
        """
        self.callback = callback
        self.delay = delay
        self.name = name or f"DelayedTask-{id(self)}"
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the delayed task."""
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        """Run the task after delay."""
        time.sleep(self.delay)
        try:
            self.callback()
        except Exception:
            pass

    def cancel(self) -> None:
        """Cancel the task (if not yet executed)."""
        # Note: Can't truly cancel a sleeping thread
        pass


class RepeatingTask:
    """Execute a task repeatedly.

    Usage:
        task = RepeatingTask(lambda: print("Tick"), interval=1)
        task.start()
        task.stop()
    """

    def __init__(
        self,
        callback: Callable,
        interval: float,
        name: Optional[str] = None,
    ) -> None:
        """Initialize repeating task.

        Args:
            callback: Function to execute.
            interval: Interval in seconds.
            name: Optional task name.
        """
        self.callback = callback
        self.interval = interval
        self.name = name or f"RepeatingTask-{id(self)}"
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the repeating task."""
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the repeating task."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=self.interval * 2)

    def _run(self) -> None:
        """Run the task repeatedly."""
        while self._running:
            try:
                self.callback()
            except Exception:
                pass
            time.sleep(self.interval)