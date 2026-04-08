"""Automation circuit breaker action module for RabAI AutoClick.

Provides circuit breaker pattern for automation workflows:
- WorkflowCircuitBreaker: Circuit breaker for workflow steps
- AutomationCircuitManager: Manage circuit state across workflows
- StepFailureTracker: Track and analyze step failures
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import time
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


class AutomationCircuitState(Enum):
    """Circuit states for automation."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    TRIPPED = "tripped"
    RECOVERING = "recovering"
    FAILSAFE = "failsafe"


@dataclass
class AutomationCircuitConfig:
    """Configuration for automation circuit breaker."""
    failure_threshold: int = 5
    success_threshold: int = 3
    trip_timeout: float = 60.0
    recovery_timeout: float = 300.0
    degraded_threshold: int = 3
    failure_window: float = 120.0
    max_failures_per_window: int = 10
    enable_failsafe: bool = True
    failsafe_mode: str = "allow_minimal"
    step_timeout: float = 30.0
    slow_operation_threshold: float = 10.0


class CircuitTrippedException(Exception):
    """Exception raised when circuit is tripped."""
    pass


class SlowOperationException(Exception):
    """Exception raised when operation is too slow."""
    pass


class StepMetrics:
    """Metrics for a workflow step."""
    
    def __init__(self, step_id: str):
        self.step_id = step_id
        self.successes = 0
        self.failures = 0
        self.timeouts = 0
        self.slow_operations = 0
        self.total_duration = 0.0
        self._duration_history = deque(maxlen=100)
        self._failure_times: deque = deque(maxlen=200)
        self._lock = threading.Lock()
    
    def record_success(self, duration: float):
        """Record successful execution."""
        with self._lock:
            self.successes += 1
            self.total_duration += duration
            self._duration_history.append(duration)
    
    def record_failure(self, error_type: str = "generic"):
        """Record failed execution."""
        with self._lock:
            self.failures += 1
            self._failure_times.append((time.time(), error_type))
    
    def record_timeout(self):
        """Record timeout."""
        with self._lock:
            self.timeouts += 1
            self.failures += 1
    
    def record_slow(self):
        """Record slow operation."""
        with self._lock:
            self.slow_operations += 1
    
    def get_recent_failure_rate(self, window: float = 120.0) -> float:
        """Get recent failure rate."""
        with self._lock:
            cutoff = time.time() - window
            recent = [f for f, _ in self._failure_times if f >= cutoff]
            if not recent:
                return 0.0
            return len(recent) / max(1, len(recent) + self.successes)
    
    def get_avg_duration(self) -> float:
        """Get average execution duration."""
        with self._lock:
            if not self._duration_history:
                return 0.0
            return sum(self._duration_history) / len(self._duration_history)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get step statistics."""
        with self._lock:
            return {
                "step_id": self.step_id,
                "successes": self.successes,
                "failures": self.failures,
                "timeouts": self.timeouts,
                "slow_operations": self.slow_operations,
                "avg_duration": self.get_avg_duration(),
                "failure_rate": self.get_recent_failure_rate(),
            }


class WorkflowCircuitBreaker:
    """Circuit breaker for workflow execution."""
    
    def __init__(self, workflow_id: str, config: Optional[AutomationCircuitConfig] = None):
        self.workflow_id = workflow_id
        self.config = config or AutomationCircuitConfig()
        self._state = AutomationCircuitState.HEALTHY
        self._metrics: Dict[str, StepMetrics] = {}
        self._tripped_at: Optional[float] = None
        self._recovery_started: Optional[float] = None
        self._lock = threading.RLock()
        self._state_history: deque = deque(maxlen=50)
        self._stats = {"total_executions": 0, "blocked_executions": 0, "state_changes": 0}
    
    @property
    def state(self) -> AutomationCircuitState:
        """Get current circuit state with auto-recovery logic."""
        with self._lock:
            if self._state == AutomationCircuitState.TRIPPED:
                if self._tripped_at and (time.time() - self._tripped_at) >= self.config.trip_timeout:
                    self._transition_to(AutomationCircuitState.RECOVERING)
                else:
                    return AutomationCircuitState.TRIPPED
            
            if self._state == AutomationCircuitState.RECOVERING:
                if self._recovery_started and (time.time() - self._recovery_started) >= self.config.recovery_timeout:
                    self._transition_to(AutomationCircuitState.HEALTHY)
                else:
                    return AutomationCircuitState.RECOVERING
            
            if self._state == AutomationCircuitState.FAILSAFE:
                return AutomationCircuitState.FAILSAFE
            
            recent_rate = self._get_global_failure_rate()
            if recent_rate > 0.5:
                return AutomationCircuitState.TRIPPED
            elif recent_rate > 0.2:
                return AutomationCircuitState.DEGRADED
            
            return AutomationCircuitState.HEALTHY
    
    def _transition_to(self, new_state: AutomationCircuitState):
        """Transition to new state."""
        with self._lock:
            if self._state != new_state:
                logging.info(f"Circuit {self.workflow_id}: {self._state.value} -> {new_state.value}")
                self._state = new_state
                self._state_history.append((time.time(), new_state))
                self._stats["state_changes"] += 1
                
                if new_state == AutomationCircuitState.TRIPPED:
                    self._tripped_at = time.time()
                elif new_state == AutomationCircuitState.RECOVERING:
                    self._recovery_started = time.time()
                elif new_state == AutomationCircuitState.HEALTHY:
                    self._tripped_at = None
                    self._recovery_started = None
    
    def _get_global_failure_rate(self) -> float:
        """Get global failure rate across all steps."""
        with self._lock:
            if not self._metrics:
                return 0.0
            total = sum(m.failures + m.successes for m in self._metrics.values())
            failures = sum(m.failures for m in self._metrics.values())
            return failures / max(1, total)
    
    def _get_step_metrics(self, step_id: str) -> StepMetrics:
        """Get or create step metrics."""
        with self._lock:
            if step_id not in self._metrics:
                self._metrics[step_id] = StepMetrics(step_id)
            return self._metrics[step_id]
    
    def execute_step(self, step_id: str, operation: Callable, *args, **kwargs) -> Any:
        """Execute workflow step through circuit breaker."""
        current_state = self.state
        
        if current_state == AutomationCircuitState.TRIPPED:
            if self.config.enable_failsafe and self.config.failsafe_mode != "block_all":
                pass
            else:
                self._stats["blocked_executions"] += 1
                raise CircuitTrippedException(f"Circuit for {self.workflow_id} is TRIPPED")
        
        self._stats["total_executions"] += 1
        metrics = self._get_step_metrics(step_id)
        
        start_time = time.time()
        try:
            result = operation(*args, **kwargs)
            duration = time.time() - start_time
            
            metrics.record_success(duration)
            
            if current_state == AutomationCircuitState.RECOVERING:
                successes = sum(m.successes for m in self._metrics.values())
                if successes >= self.config.success_threshold:
                    self._transition_to(AutomationCircuitState.HEALTHY)
            
            if duration > self.config.slow_operation_threshold:
                metrics.record_slow()
            
            return result
            
        except TimeoutError as e:
            duration = time.time() - start_time
            metrics.record_timeout()
            self._check_trip_conditions()
            raise
        
        except Exception as e:
            duration = time.time() - start_time
            metrics.record_failure(type(e).__name__)
            self._check_trip_conditions()
            raise
    
    def _check_trip_conditions(self):
        """Check if circuit should trip."""
        with self._lock:
            recent_rate = self._get_global_failure_rate()
            total_failures = sum(m.failures for m in self._metrics.values())
            
            if total_failures >= self.config.failure_threshold:
                if self.config.enable_failsafe:
                    self._transition_to(AutomationCircuitState.FAILSAFE)
                else:
                    self._transition_to(AutomationCircuitState.TRIPPED)
            elif recent_rate > 0.3:
                self._transition_to(AutomationCircuitState.DEGRADED)
    
    def get_all_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get all step metrics."""
        with self._lock:
            return {step_id: m.get_stats() for step_id, m in self._metrics.items()}
    
    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        with self._lock:
            return {
                "workflow_id": self.workflow_id,
                "state": self.state.value,
                "tripped_at": self._tripped_at,
                "recovery_started": self._recovery_started,
                **{k: v for k, v in self._stats.items()}
            }


class AutomationCircuitBreakerAction(BaseAction):
    """Automation circuit breaker action."""
    action_type = "automation_circuit_breaker"
    display_name = "自动化熔断器"
    description = "自动化工作流熔断保护"
    
    def __init__(self):
        super().__init__()
        self._circuits: Dict[str, WorkflowCircuitBreaker] = {}
        self._lock = threading.Lock()
    
    def _get_circuit(self, workflow_id: str, config: Optional[AutomationCircuitConfig] = None) -> WorkflowCircuitBreaker:
        """Get or create circuit breaker."""
        with self._lock:
            if workflow_id not in self._circuits:
                self._circuits[workflow_id] = WorkflowCircuitBreaker(workflow_id, config)
            return self._circuits[workflow_id]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute step with circuit breaker."""
        try:
            workflow_id = params.get("workflow_id", "default")
            step_id = params.get("step_id", "default")
            operation = params.get("operation")
            
            config = AutomationCircuitConfig(
                failure_threshold=params.get("failure_threshold", 5),
                success_threshold=params.get("success_threshold", 3),
                trip_timeout=params.get("trip_timeout", 60.0),
                recovery_timeout=params.get("recovery_timeout", 300.0),
                enable_failsafe=params.get("enable_failsafe", True),
            )
            
            circuit = self._get_circuit(workflow_id, config)
            
            if operation is None:
                stats = circuit.get_stats()
                metrics = circuit.get_all_metrics()
                return ActionResult(success=True, data={"stats": stats, "metrics": metrics})
            
            try:
                result = circuit.execute_step(step_id, operation)
                return ActionResult(success=True, data={"result": result})
            except CircuitTrippedException as e:
                return ActionResult(success=False, message=str(e), data={"circuit_state": "tripped"})
            except Exception as e:
                return ActionResult(success=False, message=f"Step execution failed: {str(e)}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"AutomationCircuitBreakerAction error: {str(e)}")
    
    def trip(self, workflow_id: str) -> ActionResult:
        """Manually trip circuit."""
        try:
            with self._lock:
                if workflow_id in self._circuits:
                    circuit = self._circuits[workflow_id]
                    circuit._transition_to(AutomationCircuitState.TRIPPED)
            return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, message=str(e))
    
    def reset(self, workflow_id: Optional[str] = None) -> ActionResult:
        """Reset circuit."""
        try:
            with self._lock:
                if workflow_id and workflow_id in self._circuits:
                    circuit = self._circuits[workflow_id]
                    circuit._transition_to(AutomationCircuitState.HEALTHY)
                    circuit._metrics.clear()
                else:
                    for circuit in self._circuits.values():
                        circuit._transition_to(AutomationCircuitState.HEALTHY)
                        circuit._metrics.clear()
            return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, message=str(e))
