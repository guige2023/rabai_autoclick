"""Automation backoff action module for RabAI AutoClick.

Provides backoff strategies for automation workflows:
- AutomationBackoffScheduler: Schedule retries with backoff
- AutomationRecoveryBackoff: Backoff for automation failure recovery
- WorkflowBackoffManager: Manage backoff across workflow steps
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import time
import random
import threading
import logging
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BackoffPhase(Enum):
    """Backoff phases in automation workflow."""
    IMMEDIATE = "immediate"
    DELAYED = "delayed"
    SUSPENDED = "suspended"
    RECOVERING = "recovering"
    NORMAL = "normal"


@dataclass
class AutomationBackoffConfig:
    """Configuration for automation backoff."""
    immediate_retries: int = 2
    delayed_retries: int = 3
    immediate_delay: float = 0.5
    initial_backoff: float = 2.0
    max_backoff: float = 120.0
    backoff_multiplier: float = 2.0
    jitter: float = 0.15
    max_suspension_time: float = 3600.0
    adaptive_enabled: bool = True
    recovery_timeout: float = 300.0
    exponential_base: float = 2.0


class AutomationBackoffEntry:
    """Individual backoff entry for a workflow step."""
    
    def __init__(self, step_id: str, config: AutomationBackoffConfig):
        self.step_id = step_id
        self.config = config
        self.phase = BackoffPhase.NORMAL
        self.attempt = 0
        self.immediate_attempts = 0
        self.delayed_attempts = 0
        self.suspension_start: Optional[float] = None
        self.backoff_history: deque = deque(maxlen=20)
        self._lock = threading.RLock()
    
    def record_failure(self) -> Tuple[bool, float]:
        """Record failure and compute next backoff. Returns (should_retry, delay)."""
        with self._lock:
            self.attempt += 1
            self.backoff_history.append({"time": time.time(), "success": False})
            
            if self.phase == BackoffPhase.IMMEDIATE:
                self.immediate_attempts += 1
                if self.immediate_attempts >= self.config.immediate_retries:
                    self.phase = BackoffPhase.DELAYED
                    self.delayed_attempts = 0
                return True, self.config.immediate_delay
            
            if self.phase == BackoffPhase.DELAYED:
                self.delayed_attempts += 1
                if self.delayed_attempts >= self.config.delayed_retries:
                    self.phase = BackoffPhase.SUSPENDED
                    self.suspension_start = time.time()
                    return False, 0.0
                
                delay = self._compute_delay()
                return True, delay
            
            if self.phase == BackoffPhase.SUSPENDED:
                elapsed = time.time() - self.suspension_start
                if elapsed >= self.config.max_suspension_time:
                    self.phase = BackoffPhase.RECOVERING
                    return False, 0.0
                return False, 0.0
            
            if self.phase == BackoffPhase.RECOVERING:
                self.phase = BackoffPhase.NORMAL
                self.attempt = 0
                return True, 0.0
            
            delay = self._compute_delay()
            return True, delay
    
    def _compute_delay(self) -> float:
        """Compute backoff delay."""
        delay = self.config.initial_backoff * (self.config.backoff_multiplier ** (self.delayed_attempts - 1))
        delay = min(delay, self.config.max_backoff)
        
        if self.config.jitter > 0:
            delta = delay * self.config.jitter
            delay += random.uniform(-delta, delta)
        
        return max(0, delay)
    
    def record_success(self):
        """Record successful execution."""
        with self._lock:
            self.backoff_history.append({"time": time.time(), "success": True})
            if self.phase == BackoffPhase.RECOVERING:
                self.phase = BackoffPhase.NORMAL
            self.attempt = 0
            self.immediate_attempts = 0
            self.delayed_attempts = 0
    
    def get_state(self) -> Dict[str, Any]:
        """Get current backoff state."""
        with self._lock:
            return {
                "step_id": self.step_id,
                "phase": self.phase.value,
                "attempt": self.attempt,
                "immediate_attempts": self.immediate_attempts,
                "delayed_attempts": self.delayed_attempts,
                "history_size": len(self.backoff_history),
            }


class AutomationBackoffScheduler:
    """Manage backoff across automation workflow steps."""
    
    def __init__(self, config: Optional[AutomationBackoffConfig] = None):
        self.config = config or AutomationBackoffConfig()
        self._entries: Dict[str, AutomationBackoffEntry] = {}
        self._lock = threading.RLock()
        self._stats = {"total_failures": 0, "total_successes": 0, "total_retries": 0, "total_suspensions": 0}
    
    def get_entry(self, step_id: str) -> AutomationBackoffEntry:
        """Get or create backoff entry for step."""
        with self._lock:
            if step_id not in self._entries:
                self._entries[step_id] = AutomationBackoffEntry(step_id, self.config)
            return self._entries[step_id]
    
    def record_failure(self, step_id: str) -> Tuple[bool, float]:
        """Record failure for step. Returns (should_retry, delay)."""
        entry = self.get_entry(step_id)
        should_retry, delay = entry.record_failure()
        
        with self._lock:
            self._stats["total_failures"] += 1
            if should_retry:
                self._stats["total_retries"] += 1
            elif entry.phase == BackoffPhase.SUSPENDED:
                self._stats["total_suspensions"] += 1
        
        return should_retry, delay
    
    def record_success(self, step_id: str):
        """Record success for step."""
        entry = self.get_entry(step_id)
        entry.record_success()
        
        with self._lock:
            self._stats["total_successes"] += 1
    
    def get_workflow_state(self) -> Dict[str, Dict[str, Any]]:
        """Get state of all workflow steps."""
        with self._lock:
            return {step_id: entry.get_state() for step_id, entry in self._entries.items()}
    
    def get_stats(self) -> Dict[str, Any]:
        """Get scheduler statistics."""
        with self._lock:
            return dict(self._stats)


class AutomationBackoffAction(BaseAction):
    """Automation backoff action."""
    action_type = "automation_backoff"
    display_name = "自动化退避"
    description = "自动化工作流退避策略"
    
    def __init__(self):
        super().__init__()
        self._scheduler: Optional[AutomationBackoffScheduler] = None
        self._lock = threading.Lock()
    
    def _get_scheduler(self, params: Dict[str, Any]) -> AutomationBackoffScheduler:
        """Get or create scheduler."""
        with self._lock:
            if self._scheduler is None:
                config = AutomationBackoffConfig(
                    immediate_retries=params.get("immediate_retries", 2),
                    delayed_retries=params.get("delayed_retries", 3),
                    immediate_delay=params.get("immediate_delay", 0.5),
                    initial_backoff=params.get("initial_backoff", 2.0),
                    max_backoff=params.get("max_backoff", 120.0),
                    backoff_multiplier=params.get("backoff_multiplier", 2.0),
                    max_suspension_time=params.get("max_suspension_time", 3600.0),
                )
                self._scheduler = AutomationBackoffScheduler(config)
            return self._scheduler
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute automation step with backoff."""
        try:
            scheduler = self._get_scheduler(params)
            step_id = params.get("step_id", "default")
            operation = params.get("operation")
            
            if operation is None:
                state = scheduler.get_workflow_state()
                stats = scheduler.get_stats()
                return ActionResult(success=True, data={"state": state, "stats": stats})
            
            should_retry, delay = scheduler.record_failure(step_id)
            
            if not should_retry:
                entry = scheduler.get_entry(step_id)
                state = entry.get_state()
                return ActionResult(
                    success=False,
                    message=f"Step {step_id} suspended",
                    data={"phase": state.get("phase"), "should_retry": False}
                )
            
            if delay > 0:
                time.sleep(delay)
            
            try:
                result = operation()
                scheduler.record_success(step_id)
                return ActionResult(success=True, data={"result": result})
            except Exception as e:
                should_retry, delay = scheduler.record_failure(step_id)
                if should_retry:
                    return ActionResult(
                        success=False,
                        message=f"Step {step_id} failed, will retry after {delay}s: {str(e)}",
                        data={"should_retry": True, "delay": delay}
                    )
                return ActionResult(success=False, message=f"Step {step_id} failed: {str(e)}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"AutomationBackoffAction error: {str(e)}")
    
    def reset(self, step_id: Optional[str] = None) -> ActionResult:
        """Reset backoff state for step or all steps."""
        try:
            if self._scheduler:
                if step_id and step_id in self._scheduler._entries:
                    self._scheduler._entries[step_id].attempt = 0
                    self._scheduler._entries[step_id].phase = BackoffPhase.NORMAL
                else:
                    for entry in self._scheduler._entries.values():
                        entry.attempt = 0
                        entry.phase = BackoffPhase.NORMAL
            return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, message=str(e))
