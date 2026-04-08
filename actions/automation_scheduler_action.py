"""
Automation Scheduler Action Module.

Provides scheduling capabilities for automated tasks including
cron-based scheduling, interval-based execution, and task queuing.
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging
import uuid
from collections import defaultdict

logger = logging.getLogger(__name__)


class ScheduleType(Enum):
    """Schedule types."""
    CRON = "cron"
    INTERVAL = "interval"
    ONE_TIME = "one_time"
    MANUAL = "manual"


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


@dataclass
class CronExpression:
    """Cron expression parser and validator."""
    expression: str

    def __post_init__(self):
        self.parts = self.expression.split()
        if len(self.parts) != 5:
            raise ValueError(f"Invalid cron expression: {self.expression}")

    def get_next_run(self, from_time: Optional[datetime] = None) -> datetime:
        """Get next run time from given time."""
        if from_time is None:
            from_time = datetime.now()

        minute, hour, day, month, dow = self.parts

        next_run = from_time.replace(second=0, microsecond=0) + timedelta(minutes=1)

        for _ in range(366 * 24 * 60):
            if self._matches(next_run):
                return next_run
            next_run += timedelta(minutes=1)

        raise ValueError("Could not find next run time")

    def _matches(self, dt: datetime) -> bool:
        """Check if datetime matches cron expression."""
        minute, hour, day, month, dow = self.parts

        if not self._match_field(minute, dt.minute):
            return False
        if not self._match_field(hour, dt.hour):
            return False
        if not self._match_field(day, dt.day) and day != "*":
            return False
        if not self._match_field(month, dt.month):
            return False
        if not self._match_field(dow, dt.weekday()) and dow != "*":
            return False

        return True

    def _match_field(self, field: str, value: int) -> bool:
        """Match a single cron field."""
        if field == "*":
            return True

        if "," in field:
            return any(self._match_field(f, value) for f in field.split(","))

        if "/" in field:
            base, step = field.split("/")
            base_val = int(base) if base != "*" else 0
            step_val = int(step)
            return (value - base_val) % step_val == 0

        if "-" in field:
            start, end = field.split("-")
            return int(start) <= value <= int(end)

        return int(field) == value


@dataclass
class ScheduledTask:
    """A scheduled task definition."""
    task_id: str
    name: str
    handler: Callable
    schedule_type: ScheduleType
    cron_expression: Optional[str] = None
    interval_seconds: Optional[float] = None
    run_at: Optional[datetime] = None
    enabled: bool = True
    max_instances: int = 1
    timeout: Optional[float] = None
    retry_count: int = 0
    retry_delay: float = 1.0
    tags: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskExecution:
    """Record of task execution."""
    execution_id: str
    task_id: str
    status: TaskStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    result: Any = None
    error: Optional[str] = None
    retry_count: int = 0


@dataclass
class TaskMetrics:
    """Metrics for task execution."""
    total_executions: int = 0
    successful_executions: int = 0
    failed_executions: int = 0
    skipped_executions: int = 0
    total_runtime: float = 0.0

    @property
    def success_rate(self) -> float:
        """Get success rate."""
        if self.total_executions == 0:
            return 0.0
        return self.successful_executions / self.total_executions

    @property
    def average_runtime(self) -> float:
        """Get average runtime."""
        if self.total_executions == 0:
            return 0.0
        return self.total_runtime / self.total_executions


class TaskQueue:
    """Queue for managing scheduled tasks."""

    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self._queue: List[Tuple[datetime, ScheduledTask]] = []
        self._by_id: Dict[str, ScheduledTask] = {}

    def add(self, task: ScheduledTask, run_at: datetime):
        """Add task to queue."""
        self._queue.append((run_at, task))
        self._by_id[task.task_id] = task
        self._queue.sort(key=lambda x: x[0])

    def add_interval_task(
        self,
        task: ScheduledTask,
        interval_seconds: float
    ):
        """Add interval-based task."""
        next_run = datetime.now() + timedelta(seconds=interval_seconds)
        self.add(task, next_run)

    def get_next_task(self) -> Optional[Tuple[datetime, ScheduledTask]]:
        """Get next task to execute."""
        if not self._queue:
            return None

        return self._queue[0]

    def pop_next(self) -> Optional[Tuple[datetime, ScheduledTask]]:
        """Pop and return next task."""
        if not self._queue:
            return None

        task_data = self._queue.pop(0)
        del self._by_id[task_data[1].task_id]
        return task_data

    def remove(self, task_id: str) -> bool:
        """Remove task from queue."""
        if task_id not in self._by_id:
            return False

        self._queue = [
            (run_at, task) for run_at, task in self._queue
            if task.task_id != task_id
        ]
        del self._by_id[task_id]
        return True

    def get_task(self, task_id: str) -> Optional[ScheduledTask]:
        """Get task by ID."""
        return self._by_id.get(task_id)

    def update_next_run(
        self,
        task_id: str,
        next_run: datetime
    ):
        """Update next run time for task."""
        self.remove(task_id)
        task = self._by_id.get(task_id)
        if task:
            self.add(task, next_run)


class Scheduler:
    """Main task scheduler."""

    def __init__(self):
        self.tasks: Dict[str, ScheduledTask] = {}
        self.task_queue = TaskQueue()
        self.executions: Dict[str, List[TaskExecution]] = defaultdict(list)
        self.metrics: Dict[str, TaskMetrics] = defaultdict(TaskMetrics)
        self._running = False
        self._active_tasks: Set[str] = set()
        self._lock = asyncio.Lock()

    def add_task(self, task: ScheduledTask):
        """Add a scheduled task."""
        self.tasks[task.task_id] = task

        if task.schedule_type == ScheduleType.CRON and task.cron_expression:
            cron = CronExpression(task.cron_expression)
            next_run = cron.get_next_run()
            self.task_queue.add(task, next_run)

        elif task.schedule_type == ScheduleType.INTERVAL and task.interval_seconds:
            self.task_queue.add_interval_task(task, task.interval_seconds)

        elif task.schedule_type == ScheduleType.ONE_TIME and task.run_at:
            self.task_queue.add(task, task.run_at)

        logger.info(f"Added task: {task.name} ({task.task_id})")

    def remove_task(self, task_id: str) -> bool:
        """Remove a scheduled task."""
        if task_id in self.tasks:
            del self.tasks[task_id]
            return self.task_queue.remove(task_id)
        return False

    def enable_task(self, task_id: str):
        """Enable a task."""
        if task_id in self.tasks:
            self.tasks[task_id].enabled = True

    def disable_task(self, task_id: str):
        """Disable a task."""
        if task_id in self.tasks:
            self.tasks[task_id].enabled = False

    async def run_task(self, task: ScheduledTask) -> TaskExecution:
        """Execute a single task."""
        execution_id = str(uuid.uuid4())
        execution = TaskExecution(
            execution_id=execution_id,
            task_id=task.task_id,
            status=TaskStatus.RUNNING,
            started_at=datetime.now()
        )

        async with self._lock:
            if task.task_id in self._active_tasks:
                execution.status = TaskStatus.SKIPPED
                return execution
            self._active_tasks.add(task.task_id)

        try:
            if asyncio.iscoroutinefunction(task.handler):
                if task.timeout:
                    execution.result = await asyncio.wait_for(
                        task.handler(),
                        timeout=task.timeout
                    )
                else:
                    execution.result = await task.handler()
            else:
                if task.timeout:
                    execution.result = await asyncio.wait_for(
                        asyncio.to_thread(task.handler),
                        timeout=task.timeout
                    )
                else:
                    execution.result = await asyncio.to_thread(task.handler)

            execution.status = TaskStatus.COMPLETED

            if task.task_id in self.metrics:
                self.metrics[task.task_id].successful_executions += 1

        except asyncio.TimeoutError:
            execution.status = TaskStatus.FAILED
            execution.error = "Task timeout"

            if task.task_id in self.metrics:
                self.metrics[task.task_id].failed_executions += 1

        except Exception as e:
            execution.status = TaskStatus.FAILED
            execution.error = str(e)

            if task.task_id in self.metrics:
                self.metrics[task.task_id].failed_executions += 1

        finally:
            execution.completed_at = datetime.now()
            async with self._lock:
                self._active_tasks.discard(task.task_id)

            if execution.completed_at and execution.started_at:
                runtime = (execution.completed_at - execution.started_at).total_seconds()
                if task.task_id in self.metrics:
                    self.metrics[task.task_id].total_runtime += runtime

        self.executions[task.task_id].append(execution)

        if len(self.executions[task.task_id]) > 100:
            self.executions[task.task_id] = self.executions[task.task_id][-100:]

        return execution

    async def _scheduler_loop(self):
        """Main scheduler loop."""
        while self._running:
            try:
                task_data = self.task_queue.get_next_task()

                if task_data:
                    run_at, task = task_data

                    if datetime.now() >= run_at:
                        self.task_queue.pop_next()

                        if task.enabled:
                            asyncio.create_task(self.run_task(task))

                        if task.schedule_type == ScheduleType.CRON and task.cron_expression:
                            cron = CronExpression(task.cron_expression)
                            next_run = cron.get_next_run()
                            self.task_queue.add(task, next_run)

                        elif task.schedule_type == ScheduleType.INTERVAL:
                            self.task_queue.add_interval_task(
                                task,
                                task.interval_seconds
                            )

                        elif task.schedule_type == ScheduleType.ONE_TIME:
                            pass

                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Scheduler loop error: {e}")
                await asyncio.sleep(5)

    async def start(self):
        """Start the scheduler."""
        if self._running:
            return

        self._running = True
        asyncio.create_task(self._scheduler_loop())
        logger.info("Scheduler started")

    async def stop(self):
        """Stop the scheduler."""
        self._running = False
        logger.info("Scheduler stopped")

    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get task status and metrics."""
        if task_id not in self.tasks:
            return None

        task = self.tasks[task_id]
        metrics = self.metrics.get(task_id, TaskMetrics())

        return {
            "task_id": task_id,
            "name": task.name,
            "enabled": task.enabled,
            "schedule_type": task.schedule_type.value,
            "status": "active" if task.task_id in self._active_tasks else "idle",
            "metrics": {
                "total_executions": metrics.total_executions,
                "success_rate": metrics.success_rate,
                "average_runtime": metrics.average_runtime
            }
        }

    def list_tasks(self) -> List[Dict[str, Any]]:
        """List all tasks."""
        return [
            self.get_task_status(task_id)
            for task_id in self.tasks.keys()
        ]


async def demo_task():
    """Demo task function."""
    await asyncio.sleep(0.1)
    return {"status": "completed"}


async def main():
    """Demonstrate scheduler."""
    scheduler = Scheduler()

    scheduler.add_task(ScheduledTask(
        task_id=str(uuid.uuid4()),
        name="Demo Task",
        handler=demo_task,
        schedule_type=ScheduleType.INTERVAL,
        interval_seconds=5.0
    ))

    await scheduler.start()
    await asyncio.sleep(10)
    await scheduler.stop()

    for task_id in scheduler.tasks:
        status = scheduler.get_task_status(task_id)
        print(f"Task: {status}")


if __name__ == "__main__":
    asyncio.run(main())
