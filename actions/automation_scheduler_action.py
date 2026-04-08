"""
Automation Scheduler Action Module

Provides task scheduling, cron jobs, and periodic execution.
"""
from typing import Any, Optional, Callable, Awaitable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import croniter


class ScheduleType(Enum):
    """Schedule types."""
    CRON = "cron"
    INTERVAL = "interval"
    ONCE = "once"
    DELAY = "delay"


@dataclass
class Schedule:
    """Schedule definition."""
    schedule_id: str
    name: str
    schedule_type: ScheduleType
    expression: str  # cron expression, interval seconds, or datetime
    job: Callable[[], Awaitable]
    enabled: bool = True
    timezone: str = "UTC"
    max_instances: int = 1
    misfire_grace_seconds: float = 60.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ScheduledTask:
    """A scheduled task execution."""
    task_id: str
    schedule_id: str
    scheduled_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    status: str = "pending"  # pending, running, completed, failed, skipped
    result: Any = None
    error: Optional[str] = None


@dataclass
class SchedulerStats:
    """Scheduler statistics."""
    total_scheduled: int = 0
    total_executed: int = 0
    total_completed: int = 0
    total_failed: int = 0
    total_skipped: int = 0


class AutomationSchedulerAction:
    """Main scheduler action handler."""
    
    def __init__(self):
        self._schedules: dict[str, Schedule] = {}
        self._tasks: dict[str, ScheduledTask] = {}
        self._running_tasks: dict[str, asyncio.Task] = {}
        self._task_semaphores: dict[str, asyncio.Semaphore] = {}
        self._stats = SchedulerStats()
        self._running = False
    
    def add_schedule(self, schedule: Schedule) -> "AutomationSchedulerAction":
        """Add a schedule."""
        self._schedules[schedule.schedule_id] = schedule
        self._task_semaphores[schedule.schedule_id] = asyncio.Semaphore(
            schedule.max_instances
        )
        return self
    
    def remove_schedule(self, schedule_id: str) -> bool:
        """Remove a schedule."""
        if schedule_id in self._schedules:
            del self._schedules[schedule_id]
            return True
        return False
    
    async def start(self):
        """Start the scheduler."""
        self._running = True
        self._stats.total_scheduled = len(self._schedules)
        
        while self._running:
            now = datetime.now()
            
            for schedule_id, schedule in self._schedules.items():
                if not schedule.enabled:
                    continue
                
                if self._should_run(schedule, now):
                    await self._trigger_job(schedule)
            
            await asyncio.sleep(1)  # Check every second
    
    async def stop(self):
        """Stop the scheduler."""
        self._running = False
        
        # Cancel running tasks
        for task in self._running_tasks.values():
            task.cancel()
    
    def _should_run(self, schedule: Schedule, now: datetime) -> bool:
        """Check if schedule should run now."""
        if schedule.schedule_type == ScheduleType.CRON:
            return self._should_run_cron(schedule, now)
        elif schedule.schedule_type == ScheduleType.INTERVAL:
            return self._should_run_interval(schedule, now)
        elif schedule.schedule_type == ScheduleType.ONCE:
            return self._should_run_once(schedule, now)
        return False
    
    def _should_run_cron(self, schedule: Schedule, now: datetime) -> bool:
        """Check if cron schedule should run."""
        try:
            cron = croniter.croniter(schedule.expression, now, timezone=schedule.timezone)
            prev = cron.get_prev(datetime)
            
            # Run if within misfire grace period
            elapsed = (now - prev).total_seconds()
            return elapsed < schedule.misfire_grace_seconds and elapsed >= 0
            
        except:
            return False
    
    def _should_run_interval(self, schedule: Schedule, now: datetime) -> bool:
        """Check if interval schedule should run."""
        last_run_key = f"_last_run_{schedule.schedule_id}"
        last_run = getattr(self, last_run_key, None)
        
        if not last_run:
            return True
        
        interval_seconds = float(schedule.expression)
        elapsed = (now - last_run).total_seconds()
        
        return elapsed >= interval_seconds
    
    def _should_run_once(self, schedule: Schedule, now: datetime) -> bool:
        """Check if one-time schedule should run."""
        try:
            run_time = datetime.fromisoformat(schedule.expression)
            elapsed = (now - run_time).total_seconds()
            return 0 <= elapsed < schedule.misfire_grace_seconds
        except:
            return False
    
    async def _trigger_job(self, schedule: Schedule):
        """Trigger a scheduled job."""
        # Check max instances
        semaphore = self._task_semaphores.get(schedule.schedule_id)
        if not semaphore:
            return
        
        if not semaphore.locked():
            await semaphore.acquire()
        else:
            # Already at max instances
            return
        
        task_id = f"{schedule.schedule_id}:{datetime.now().isoformat()}"
        
        task = ScheduledTask(
            task_id=task_id,
            schedule_id=schedule.schedule_id,
            scheduled_at=datetime.now(),
            status="running"
        )
        
        self._tasks[task_id] = task
        
        async def run_job():
            try:
                task.started_at = datetime.now()
                result = await schedule.job()
                task.completed_at = datetime.now()
                task.status = "completed"
                task.result = result
                self._stats.total_completed += 1
                
            except Exception as e:
                task.completed_at = datetime.now()
                task.status = "failed"
                task.error = str(e)
                self._stats.total_failed += 1
                
            finally:
                semaphore.release()
                self._stats.total_executed += 1
        
        asyncio.create_task(run_job())
        self._running_tasks[task_id] = asyncio.current_task()
    
    async def trigger_now(self, schedule_id: str) -> Optional[str]:
        """Manually trigger a schedule immediately."""
        if schedule_id not in self._schedules:
            return None
        
        schedule = self._schedules[schedule_id]
        await self._trigger_job(schedule)
        return f"{schedule_id}:manual:{datetime.now().isoformat()}"
    
    def pause_schedule(self, schedule_id: str) -> bool:
        """Pause a schedule."""
        if schedule_id in self._schedules:
            self._schedules[schedule_id].enabled = False
            return True
        return False
    
    def resume_schedule(self, schedule_id: str) -> bool:
        """Resume a paused schedule."""
        if schedule_id in self._schedules:
            self._schedules[schedule_id].enabled = True
            return True
        return False
    
    def get_schedule_next_run(self, schedule_id: str) -> Optional[datetime]:
        """Get next scheduled run time."""
        if schedule_id not in self._schedules:
            return None
        
        schedule = self._schedules[schedule_id]
        
        if schedule.schedule_type == ScheduleType.CRON:
            try:
                cron = croniter.croniter(
                    schedule.expression,
                    datetime.now(),
                    timezone=schedule.timezone
                )
                return cron.get_next(datetime)
            except:
                return None
        
        elif schedule.schedule_type == ScheduleType.INTERVAL:
            interval_seconds = float(schedule.expression)
            return datetime.now() + timedelta(seconds=interval_seconds)
        
        return None
    
    def list_schedules(self, enabled_only: bool = False) -> list[dict[str, Any]]:
        """List all schedules."""
        schedules = list(self._schedules.values())
        
        if enabled_only:
            schedules = [s for s in schedules if s.enabled]
        
        return [
            {
                "schedule_id": s.schedule_id,
                "name": s.name,
                "type": s.schedule_type.value,
                "expression": s.expression,
                "enabled": s.enabled,
                "next_run": self.get_schedule_next_run(s.schedule_id).isoformat()
                if self.get_schedule_next_run(s.schedule_id) else None
            }
            for s in schedules
        ]
    
    def get_task_history(
        self,
        schedule_id: Optional[str] = None,
        limit: int = 100
    ) -> list[dict[str, Any]]:
        """Get task execution history."""
        tasks = list(self._tasks.values())
        
        if schedule_id:
            tasks = [t for t in tasks if t.schedule_id == schedule_id]
        
        tasks.sort(key=lambda t: t.scheduled_at, reverse=True)
        
        return [
            {
                "task_id": t.task_id,
                "schedule_id": t.schedule_id,
                "scheduled_at": t.scheduled_at.isoformat(),
                "started_at": t.started_at.isoformat() if t.started_at else None,
                "completed_at": t.completed_at.isoformat() if t.completed_at else None,
                "status": t.status,
                "error": t.error
            }
            for t in tasks[:limit]
        ]
    
    def get_stats(self) -> dict[str, Any]:
        """Get scheduler statistics."""
        return {
            "running": self._running,
            "total_schedules": len(self._schedules),
            "enabled_schedules": len([s for s in self._schedules.values() if s.enabled]),
            "active_tasks": len(self._running_tasks),
            "total_tasks": len(self._tasks),
            **vars(self._stats)
        }
