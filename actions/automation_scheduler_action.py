"""
Automation Task Scheduler Module.

Provides advanced scheduling capabilities for automation tasks
including cron expressions, interval-based scheduling,
priority queues, and distributed task coordination.
"""

from typing import (
    Dict, List, Optional, Any, Callable, Set,
    Tuple, Union, TypeVar
)
from dataclasses import dataclass, field
from enum import Enum, auto
from datetime import datetime, timedelta
import time
import threading
import logging
from collections import deque
import re

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ScheduleType(Enum):
    """Type of scheduling strategy."""
    INTERVAL = auto()
    CRON = auto()
    ONE_TIME = auto()
    MANUAL = auto()


class TaskPriority(Enum):
    """Task priority levels."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()
    TIMEOUT = auto()


@dataclass
class ScheduledTask:
    """Represents a scheduled automation task."""
    task_id: str
    name: str
    func: Callable[..., Any]
    args: Tuple[Any, ...] = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    
    schedule_type: ScheduleType = ScheduleType.MANUAL
    priority: TaskPriority = TaskPriority.NORMAL
    
    # For interval scheduling
    interval_seconds: Optional[float] = None
    
    # For cron scheduling
    cron_expression: Optional[str] = None
    
    # For one-time scheduling
    run_at: Optional[datetime] = None
    
    # Execution settings
    timeout: Optional[float] = None
    retry_count: int = 0
    retry_delay: float = 1.0
    
    # Status tracking
    status: TaskStatus = TaskStatus.PENDING
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    run_count: int = 0
    error: Optional[str] = None
    
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __lt__(self, other: "ScheduledTask") -> bool:
        """Compare tasks by priority for queue ordering."""
        return self.priority.value > other.priority.value


@dataclass
class ExecutionResult:
    """Result of task execution."""
    task_id: str
    status: TaskStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    duration_ms: Optional[float] = None
    result: Any = None
    error: Optional[str] = None
    retry_count: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "status": self.status.name,
            "duration_ms": self.duration_ms,
            "error": self.error,
            "retry_count": self.retry_count
        }


class CronParser:
    """Parses and handles cron expressions."""
    
    CRON_PATTERN = re.compile(
        r"^([0-9,\-\*\/]+)\s+([0-9,\-\*\/]+)\s+([0-9,\-\*\/]+)\s+([0-9,\-\*\/]+)\s+([0-9,\-\*\/]+)$"
    )
    
    FIELD_RANGES = {
        0: (0, 59),      # minute
        1: (0, 23),      # hour
        2: (1, 31),      # day of month
        3: (1, 12),      # month
        4: (0, 6),       # day of week
    }
    
    @classmethod
    def parse(cls, expression: str) -> Optional[List[Set[int]]]:
        """
        Parse cron expression to field value sets.
        
        Args:
            expression: Cron expression (e.g., "*/5 * * * *")
            
        Returns:
            List of 5 sets for minute, hour, day, month, weekday
        """
        match = cls.CRON_PATTERN.match(expression.strip())
        if not match:
            return None
        
        fields = []
        for i, field_str in enumerate(match.groups()):
            values = cls._parse_field(field_str, cls.FIELD_RANGES[i])
            fields.append(values)
        
        return fields
    
    @classmethod
    def _parse_field(cls, field_str: str, valid_range: Tuple[int, int]) -> Set[int]:
        """Parse single cron field."""
        values = set()
        min_val, max_val = valid_range
        
        for part in field_str.split(","):
            if "/" in part:
                # Step value
                base, step = part.split("/")
                step = int(step)
                if base == "*":
                    start, end = min_val, max_val
                elif "-" in base:
                    start, end = map(int, base.split("-"))
                else:
                    start = int(base)
                    end = max_val
                
                for v in range(start, end + 1, step):
                    if min_val <= v <= max_val:
                        values.add(v)
            
            elif "-" in part:
                # Range
                start, end = map(int, part.split("-"))
                for v in range(start, end + 1):
                    values.add(v)
            
            elif part == "*":
                values.update(range(min_val, max_val + 1))
            
            else:
                v = int(part)
                if min_val <= v <= max_val:
                    values.add(v)
        
        return values
    
    @classmethod
    def matches(cls, expression: str, when: Optional[datetime] = None) -> bool:
        """Check if cron expression matches datetime."""
        when = when or datetime.now()
        fields = cls.parse(expression)
        
        if not fields:
            return False
        
        return (
            when.minute in fields[0] and
            when.hour in fields[1] and
            when.day in fields[2] and
            when.month in fields[3] and
            when.weekday() in fields[4]
        )


class TaskScheduler:
    """
    Advanced task scheduler for automation workflows.
    
    Supports multiple scheduling strategies, priority queues,
    task dependencies, and execution monitoring.
    """
    
    def __init__(self) -> None:
        self.tasks: Dict[str, ScheduledTask] = {}
        self.execution_history: deque = deque(maxlen=1000)
        self._running = False
        self._lock = threading.RLock()
        self._scheduler_thread: Optional[threading.Thread] = None
        self._worker_threads: List[threading.Thread] = []
        self._max_workers = 4
    
    def add_task(self, task: ScheduledTask) -> None:
        """
        Add a task to the scheduler.
        
        Args:
            task: ScheduledTask to add
        """
        with self._lock:
            self.tasks[task.task_id] = task
            self._calculate_next_run(task)
            logger.info(f"Added task: {task.name} ({task.task_id})")
    
    def remove_task(self, task_id: str) -> bool:
        """Remove task from scheduler."""
        with self._lock:
            if task_id in self.tasks:
                del self.tasks[task_id]
                return True
            return False
    
    def get_task(self, task_id: str) -> Optional[ScheduledTask]:
        """Get task by ID."""
        return self.tasks.get(task_id)
    
    def schedule_interval(
        self,
        task_id: str,
        name: str,
        func: Callable,
        interval_seconds: float,
        args: Tuple[Any, ...] = (),
        kwargs: Optional[Dict[str, Any]] = None,
        priority: TaskPriority = TaskPriority.NORMAL,
        timeout: Optional[float] = None,
        start_now: bool = True
    ) -> ScheduledTask:
        """
        Schedule task to run at fixed interval.
        
        Args:
            task_id: Unique task identifier
            name: Human-readable name
            func: Function to execute
            interval_seconds: Interval between executions
            args: Positional arguments
            kwargs: Keyword arguments
            priority: Task priority
            timeout: Optional execution timeout
            start_now: Whether to run immediately first
            
        Returns:
            Created ScheduledTask
        """
        kwargs = kwargs or {}
        task = ScheduledTask(
            task_id=task_id,
            name=name,
            func=func,
            args=args,
            kwargs=kwargs,
            schedule_type=ScheduleType.INTERVAL,
            interval_seconds=interval_seconds,
            priority=priority,
            timeout=timeout
        )
        
        if start_now:
            task.next_run = datetime.now()
        else:
            task.next_run = datetime.now() + timedelta(seconds=interval_seconds)
        
        self.add_task(task)
        return task
    
    def schedule_cron(
        self,
        task_id: str,
        name: str,
        func: Callable,
        cron_expression: str,
        args: Tuple[Any, ...] = (),
        kwargs: Optional[Dict[str, Any]] = None
    ) -> ScheduledTask:
        """Schedule task using cron expression."""
        kwargs = kwargs or {}
        task = ScheduledTask(
            task_id=task_id,
            name=name,
            func=func,
            args=args,
            kwargs=kwargs,
            schedule_type=ScheduleType.CRON,
            cron_expression=cron_expression
        )
        
        self._calculate_next_run(task)
        self.add_task(task)
        return task
    
    def schedule_once(
        self,
        task_id: str,
        name: str,
        func: Callable,
        run_at: datetime,
        args: Tuple[Any, ...] = (),
        kwargs: Optional[Dict[str, Any]] = None
    ) -> ScheduledTask:
        """Schedule task to run once at specific time."""
        kwargs = kwargs or {}
        task = ScheduledTask(
            task_id=task_id,
            name=name,
            func=func,
            args=args,
            kwargs=kwargs,
            schedule_type=ScheduleType.ONE_TIME,
            run_at=run_at
        )
        
        task.next_run = run_at
        self.add_task(task)
        return task
    
    def _calculate_next_run(self, task: ScheduledTask) -> None:
        """Calculate next run time for task."""
        now = datetime.now()
        
        if task.schedule_type == ScheduleType.INTERVAL:
            if task.interval_seconds:
                task.next_run = now + timedelta(seconds=task.interval_seconds)
        
        elif task.schedule_type == ScheduleType.CRON:
            if task.cron_expression:
                # Find next matching time
                for minutes in range(1440):  # Check next 24 hours
                    candidate = now + timedelta(minutes=minutes)
                    if CronParser.matches(task.cron_expression, candidate):
                        task.next_run = candidate
                        break
        
        elif task.schedule_type == ScheduleType.ONE_TIME:
            if task.run_at and task.run_at > now:
                task.next_run = task.run_at
    
    def execute_task(self, task: ScheduledTask) -> ExecutionResult:
        """Execute a single task."""
        result = ExecutionResult(
            task_id=task.task_id,
            status=TaskStatus.RUNNING,
            started_at=datetime.now()
        )
        
        task.status = TaskStatus.RUNNING
        task.last_run = datetime.now()
        
        for attempt in range(task.retry_count + 1):
            try:
                if task.timeout:
                    # Execute with timeout
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(task.func, *task.args, **task.kwargs)
                        result.result = future.result(timeout=task.timeout)
                else:
                    result.result = task.func(*task.args, **task.kwargs)
                
                result.status = TaskStatus.COMPLETED
                task.status = TaskStatus.COMPLETED
                break
            
            except Exception as e:
                logger.warning(f"Task {task.task_id} attempt {attempt + 1} failed: {e}")
                result.error = str(e)
                
                if attempt < task.retry_count:
                    time.sleep(task.retry_delay)
                    result.retry_count = attempt + 1
                else:
                    result.status = TaskStatus.FAILED
                    task.status = TaskStatus.FAILED
                    task.error = str(e)
        
        result.completed_at = datetime.now()
        if result.started_at and result.completed_at:
            result.duration_ms = (
                result.completed_at - result.started_at
            ).total_seconds() * 1000
        
        task.run_count += 1
        self.execution_history.append(result)
        
        # Schedule next run for recurring tasks
        if task.schedule_type in (ScheduleType.INTERVAL, ScheduleType.CRON):
            self._calculate_next_run(task)
        elif task.schedule_type == ScheduleType.ONE_TIME:
            task.status = TaskStatus.COMPLETED
        
        return result
    
    def start(self) -> None:
        """Start the scheduler."""
        if self._running:
            return
        
        self._running = True
        self._scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self._scheduler_thread.start()
        
        logger.info("Scheduler started")
    
    def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5)
        
        logger.info("Scheduler stopped")
    
    def _run_scheduler(self) -> None:
        """Main scheduler loop."""
        while self._running:
            now = datetime.now()
            
            # Find tasks ready to run
            ready_tasks = []
            with self._lock:
                for task in self.tasks.values():
                    if task.next_run and task.next_run <= now:
                        if task.status != TaskStatus.RUNNING:
                            ready_tasks.append(task)
            
            # Sort by priority
            ready_tasks.sort()
            
            # Execute ready tasks
            for task in ready_tasks[:self._max_workers]:
                thread = threading.Thread(
                    target=lambda t: self.execute_task(t),
                    args=(task,),
                    daemon=True
                )
                thread.start()
            
            # Check every second
            time.sleep(1)
    
    def get_status(self) -> Dict[str, Any]:
        """Get scheduler status."""
        status_counts = {}
        for task in self.tasks.values():
            name = task.status.name
            status_counts[name] = status_counts.get(name, 0) + 1
        
        return {
            "running": self._running,
            "total_tasks": len(self.tasks),
            "task_status": status_counts,
            "history_size": len(self.execution_history)
        }


# Entry point for direct execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    def sample_task(name: str) -> str:
        logger.info(f"Running task: {name}")
        return f"{name} completed"
    
    scheduler = TaskScheduler()
    
    # Schedule interval task
    scheduler.schedule_interval(
        task_id="task1",
        name="Sample Interval Task",
        func=sample_task,
        interval_seconds=5,
        args=("Interval Task",),
        start_now=True
    )
    
    # Schedule cron task
    scheduler.schedule_cron(
        task_id="task2",
        name="Sample Cron Task",
        func=sample_task,
        cron_expression="*/10 * * * *",
        args=("Cron Task",)
    )
    
    scheduler.start()
    
    # Run for 20 seconds
    time.sleep(20)
    
    scheduler.stop()
    
    print(f"\nStatus: {scheduler.get_status()}")
