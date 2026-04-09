"""
Automation Scheduler Action Module.

Provides scheduling capabilities for automation workflows including cron-like
scheduling, interval-based execution, and complex scheduling patterns.

Author: RabAI Team
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import threading
import time
from datetime import datetime, timedelta
import re


class ScheduleType(Enum):
    """Types of schedules."""
    ONCE = "once"
    INTERVAL = "interval"
    CRON = "cron"
    CRON_RANGE = "cron_range"
    CALENDAR = "calendar"


@dataclass
class Schedule:
    """Represents a schedule definition."""
    id: str
    name: str
    schedule_type: ScheduleType
    next_run: Optional[datetime] = None
    last_run: Optional[datetime] = None
    interval_seconds: Optional[float] = None
    cron_expression: Optional[str] = None
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ScheduledTask:
    """Represents a scheduled task."""
    schedule: Schedule
    func: Callable
    params: Dict[str, Any] = field(default_factory=dict)


class CronParser:
    """
    Parses and evaluates cron expressions.
    
    Example:
        parser = CronParser("*/5 * * * *")  # Every 5 minutes
        if parser.should_run(datetime.now()):
            execute_task()
    """
    
    def __init__(self, expression: str):
        self.expression = expression
        self.fields = expression.split()
        
        if len(self.fields) != 5:
            raise ValueError(f"Invalid cron expression: {expression}")
        
        self.minute, self.hour, self.day, self.month, self.dow = self.fields
    
    def should_run(self, dt: datetime) -> bool:
        """Check if cron expression matches given datetime."""
        return (
            self._match_field(self.minute, dt.minute, 0, 59) and
            self._match_field(self.hour, dt.hour, 0, 23) and
            self._match_field(self.day, dt.day, 1, 31) and
            self._match_field(self.month, dt.month, 1, 12) and
            self._match_field(self.dow, dt.weekday(), 0, 6)
        )
    
    def _match_field(self, field: str, value: int, min_val: int, max_val: int) -> bool:
        """Match a single cron field."""
        if field == "*":
            return True
        
        # Handle step values */n
        if field.startswith("*/"):
            step = int(field[2:])
            return value % step == 0
        
        # Handle ranges 1-5
        if "-" in field:
            start, end = field.split("-")
            return int(start) <= value <= int(end)
        
        # Handle lists 1,3,5
        if "," in field:
            values = [int(v) for v in field.split(",")]
            return value in values
        
        # Single value
        return int(field) == value
    
    def get_next_run(self, after: datetime) -> datetime:
        """Get next run time after given datetime."""
        next_time = after + timedelta(minutes=1)
        
        for _ in range(366 * 24 * 60):  # Max ~1 year of minutes
            if self.should_run(next_time):
                return next_time.replace(second=0, microsecond=0)
            next_time += timedelta(minutes=1)
        
        raise ValueError("No next run time found within a year")


class IntervalScheduler:
    """
    Interval-based scheduler.
    
    Example:
        scheduler = IntervalScheduler(interval_seconds=60)
        scheduler.schedule(task_func, {"key": "value"})
    """
    
    def __init__(self, interval_seconds: float):
        self.interval_seconds = interval_seconds
    
    def get_next_run(self, after: datetime) -> datetime:
        """Get next run time."""
        return after + timedelta(seconds=self.interval_seconds)


class CalendarScheduler:
    """
    Calendar-based scheduler with specific dates/times.
    
    Example:
        scheduler = CalendarScheduler()
        scheduler.add_date("2024-12-25")  # Christmas
        scheduler.add_time("09:00")  # 9 AM daily
    """
    
    def __init__(self):
        self.dates: Set[str] = set()
        self.times: Set[str] = set()
        self.exclusions: Set[str] = set()
    
    def add_date(self, date_str: str) -> "CalendarScheduler":
        """Add a specific date (YYYY-MM-DD)."""
        self.dates.add(date_str)
        return self
    
    def add_time(self, time_str: str) -> "CalendarScheduler":
        """Add a specific time (HH:MM)."""
        self.times.add(time_str)
        return self
    
    def add_exclusion(self, date_str: str) -> "CalendarScheduler":
        """Add an exclusion date."""
        self.exclusions.add(date_str)
        return self
    
    def should_run(self, dt: datetime) -> bool:
        """Check if should run at given datetime."""
        date_str = dt.strftime("%Y-%m-%d")
        time_str = dt.strftime("%H:%M")
        
        if date_str in self.exclusions:
            return False
        
        if self.dates and date_str not in self.dates:
            return False
        
        if self.times and time_str not in self.times:
            return False
        
        return True


class Scheduler:
    """
    Main scheduler for automation workflows.
    
    Example:
        scheduler = Scheduler()
        scheduler.add_job("my_task", "*/5 * * * *", task_func)
        scheduler.start()
        
        scheduler.remove_job("my_task")
    """
    
    def __init__(self):
        self.schedules: Dict[str, Schedule] = {}
        self.tasks: Dict[str, ScheduledTask] = {}
        self._running = False
        self._lock = threading.RLock()
        self._scheduler_thread: Optional[threading.Thread] = None
        self._execution_history: List[Dict] = []
    
    def add_schedule(
        self,
        schedule_id: str,
        name: str,
        schedule_type: ScheduleType,
        **kwargs
    ) -> Schedule:
        """Add a schedule."""
        with self._lock:
            schedule = Schedule(
                id=schedule_id,
                name=name,
                schedule_type=schedule_type,
                cron_expression=kwargs.get("cron_expression"),
                interval_seconds=kwargs.get("interval_seconds"),
                next_run=kwargs.get("next_run", datetime.now())
            )
            self.schedules[schedule_id] = schedule
            return schedule
    
    def add_job(
        self,
        job_id: str,
        schedule_expr: str,
        func: Callable,
        params: Optional[Dict] = None,
        schedule_type: ScheduleType = ScheduleType.CRON
    ) -> str:
        """Add a scheduled job."""
        with self._lock:
            schedule = self.add_schedule(
                schedule_id=job_id,
                name=job_id,
                schedule_type=schedule_type,
                cron_expression=schedule_expr if schedule_type == ScheduleType.CRON else None,
                interval_seconds=float(schedule_expr) if schedule_type == ScheduleType.INTERVAL else None
            )
            
            self.tasks[job_id] = ScheduledTask(
                schedule=schedule,
                func=func,
                params=params or {}
            )
            
            return job_id
    
    def remove_job(self, job_id: str) -> bool:
        """Remove a scheduled job."""
        with self._lock:
            self.schedules.pop(job_id, None)
            self.tasks.pop(job_id, None)
            return True
    
    def enable_job(self, job_id: str) -> bool:
        """Enable a scheduled job."""
        with self._lock:
            if job_id in self.schedules:
                self.schedules[job_id].enabled = True
                return True
            return False
    
    def disable_job(self, job_id: str) -> bool:
        """Disable a scheduled job."""
        with self._lock:
            if job_id in self.schedules:
                self.schedules[job_id].enabled = False
                return True
            return False
    
    def start(self):
        """Start the scheduler."""
        with self._lock:
            if self._running:
                return
            
            self._running = True
            self._scheduler_thread = threading.Thread(
                target=self._run_loop,
                daemon=True
            )
            self._scheduler_thread.start()
    
    def stop(self):
        """Stop the scheduler."""
        with self._lock:
            self._running = False
    
    def _run_loop(self):
        """Main scheduler loop."""
        while self._running:
            try:
                now = datetime.now()
                
                with self._lock:
                    for job_id, task in self.tasks.items():
                        schedule = task.schedule
                        
                        if not schedule.enabled:
                            continue
                        
                        if schedule.next_run and now >= schedule.next_run:
                            # Execute task
                            self._execute_task(task)
                            
                            # Calculate next run
                            schedule.last_run = now
                            
                            if schedule.schedule_type == ScheduleType.INTERVAL:
                                schedule.next_run = now + timedelta(
                                    seconds=schedule.interval_seconds or 60
                                )
                            elif schedule.schedule_type == ScheduleType.CRON:
                                parser = CronParser(schedule.cron_expression or "")
                                schedule.next_run = parser.get_next_run(now)
                            else:
                                schedule.next_run = None
                
                time.sleep(1)
                
            except Exception:
                time.sleep(1)
    
    def _execute_task(self, task: ScheduledTask):
        """Execute a scheduled task."""
        try:
            result = task.func(task.params)
            
            self._execution_history.append({
                "job_id": task.schedule.id,
                "executed_at": datetime.now().isoformat(),
                "success": True,
                "result": str(result)[:100]
            })
        except Exception as e:
            self._execution_history.append({
                "job_id": task.schedule.id,
                "executed_at": datetime.now().isoformat(),
                "success": False,
                "error": str(e)
            })
    
    def get_next_runs(self, limit: int = 10) -> List[Dict]:
        """Get upcoming scheduled runs."""
        with self._lock:
            runs = []
            for schedule in self.schedules.values():
                if schedule.enabled and schedule.next_run:
                    runs.append({
                        "job_id": schedule.id,
                        "name": schedule.name,
                        "next_run": schedule.next_run.isoformat(),
                        "schedule_type": schedule.schedule_type.value
                    })
            
            runs.sort(key=lambda x: x["next_run"])
            return runs[:limit]
    
    def get_history(self, limit: int = 100) -> List[Dict]:
        """Get execution history."""
        with self._lock:
            return list(self._execution_history[-limit:])
    
    def get_stats(self) -> Dict[str, Any]:
        """Get scheduler statistics."""
        with self._lock:
            enabled = sum(1 for s in self.schedules.values() if s.enabled)
            return {
                "total_jobs": len(self.schedules),
                "enabled_jobs": enabled,
                "disabled_jobs": len(self.schedules) - enabled,
                "running": self._running,
                "history_size": len(self._execution_history)
            }


class BaseAction:
    """Base class for all actions."""
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Any:
        raise NotImplementedError


class AutomationSchedulerAction(BaseAction):
    """
    Scheduler action for automation workflows.
    
    Parameters:
        operation: Operation type (add_job/list_jobs/remove_job/stats)
        job_id: Job identifier
        schedule: Schedule expression (cron or interval)
        schedule_type: Type of schedule
    
    Example:
        action = AutomationSchedulerAction()
        result = action.execute({}, {
            "operation": "add_job",
            "job_id": "daily_report",
            "schedule": "0 9 * * *",
            "schedule_type": "cron"
        })
    """
    
    _scheduler: Optional[Scheduler] = None
    _lock = threading.Lock()
    
    def _get_scheduler(self) -> Scheduler:
        """Get or create scheduler."""
        with self._lock:
            if self._scheduler is None:
                self._scheduler = Scheduler()
                self._scheduler.start()
            return self._scheduler
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute scheduler operation."""
        operation = params.get("operation", "add_job")
        job_id = params.get("job_id")
        schedule = params.get("schedule", "60")
        schedule_type_str = params.get("schedule_type", "interval")
        
        scheduler = self._get_scheduler()
        
        if operation == "add_job":
            schedule_type = ScheduleType(schedule_type_str)
            
            def placeholder_func(params):
                return {"executed": True}
            
            scheduler.add_job(
                job_id=job_id,
                schedule_expr=schedule,
                func=placeholder_func,
                params={},
                schedule_type=schedule_type
            )
            
            return {
                "success": True,
                "operation": "add_job",
                "job_id": job_id,
                "schedule": schedule,
                "schedule_type": schedule_type_str,
                "added_at": datetime.now().isoformat()
            }
        
        elif operation == "remove_job":
            success = scheduler.remove_job(job_id)
            return {
                "success": success,
                "operation": "remove_job",
                "job_id": job_id
            }
        
        elif operation == "enable_job":
            success = scheduler.enable_job(job_id)
            return {
                "success": success,
                "operation": "enable_job",
                "job_id": job_id
            }
        
        elif operation == "disable_job":
            success = scheduler.disable_job(job_id)
            return {
                "success": success,
                "operation": "disable_job",
                "job_id": job_id
            }
        
        elif operation == "list_jobs":
            runs = scheduler.get_next_runs()
            return {
                "success": True,
                "operation": "list_jobs",
                "jobs": runs
            }
        
        elif operation == "history":
            history = scheduler.get_history()
            return {
                "success": True,
                "operation": "history",
                "history": history
            }
        
        elif operation == "stats":
            stats = scheduler.get_stats()
            return {
                "success": True,
                "operation": "stats",
                "stats": stats
            }
        
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
