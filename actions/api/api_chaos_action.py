"""API Chaos Engineering Action Module.

Provides chaos engineering capabilities for API resilience testing,
including fault injection, chaos experiments, and resilience scoring.

Example:
    >>> from actions.api.api_chaos_action import APIChaosEngineer
    >>> engineer = APIChaosEngineer()
    >>> result = await engineer.run_experiment(experiment_config)
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import threading


class ChaosAction(Enum):
    """Types of chaos actions to inject."""
    LATENCY = "latency"
    TIMEOUT = "timeout"
    ERROR = "error"
    ABORT = "abort"
    THROTTLE = "throttle"
    BLACKLIST = "blacklist"
    PARTITION = "partition"
    CPU_LOAD = "cpu_load"
    MEMORY_LOAD = "memory_load"
    NETWORK_LOSS = "network_loss"
    NETWORK_CORRUPT = "network_corrupt"


class ExperimentStatus(Enum):
    """Status of a chaos experiment."""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    ABORTED = "aborted"
    FAILED = "failed"


class Severity(Enum):
    """Severity level of chaos actions."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class ChaosTarget:
    """Target of chaos injection.
    
    Attributes:
        target_id: Unique target identifier
        target_type: Type (endpoint, service, host)
        identifier: How to identify this target
        labels: Labels for targeting
    """
    target_id: str
    target_type: str
    identifier: str
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class ChaosAction_:
    """A chaos action to inject.
    
    Attributes:
        action_type: Type of chaos
        target: Target for this action
        severity: Severity level
        duration: How long to inject (seconds)
        probability: Probability of triggering (0-1)
        parameters: Action-specific parameters
    """
    action_type: ChaosAction
    target: ChaosTarget
    severity: Severity = Severity.MEDIUM
    duration: float = 10.0
    probability: float = 1.0
    parameters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ChaosExperiment:
    """Chaos experiment definition.
    
    Attributes:
        experiment_id: Unique identifier
        name: Experiment name
        description: What this experiment tests
        actions: List of chaos actions
        steady_state_hypothesis: Criteria for steady state
        rollbacks: Actions to rollback changes
        created_at: Creation timestamp
        last_run: Last execution timestamp
    """
    experiment_id: str
    name: str
    description: str = ""
    actions: List[ChaosAction_] = field(default_factory=list)
    steady_state_hypothesis: Dict[str, Any] = field(default_factory=dict)
    rollbacks: List[Dict[str, Any]] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    last_run: Optional[datetime] = None
    tags: Set[str] = field(default_factory=set)


@dataclass
class ExperimentResult:
    """Result of a chaos experiment.
    
    Attributes:
        experiment_id: Experiment identifier
        status: Final status
        start_time: When experiment started
        end_time: When experiment ended
        steady_state_before: Steady state before experiment
        steady_state_during: Steady state during experiment
        steady_state_after: Steady state after experiment
        actions_triggered: Number of actions triggered
        errors: Any errors encountered
        recommendations: Improvement recommendations
    """
    experiment_id: str
    status: ExperimentStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    steady_state_before: Dict[str, Any] = field(default_factory=dict)
    steady_state_during: Dict[str, Any] = field(default_factory=dict)
    steady_state_after: Dict[str, Any] = field(default_factory=dict)
    actions_triggered: int = 0
    errors: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)


@dataclass
class ChaosConfig:
    """Configuration for chaos engineering.
    
    Attributes:
        auto_rollback: Whether to auto-rollback on failure
        rollback_timeout: Rollback timeout in seconds
        steady_state_check_interval: Interval between steady state checks
        enable_notifications: Whether to send notifications
        max_concurrent_actions: Maximum concurrent chaos actions
        dry_run: Whether to run in dry-run mode
    """
    auto_rollback: bool = True
    rollback_timeout: float = 30.0
    steady_state_check_interval: float = 5.0
    enable_notifications: bool = True
    max_concurrent_actions: int = 5
    dry_run: bool = False


class APIChaosEngineer:
    """Handles API chaos engineering experiments.
    
    Provides fault injection and chaos engineering capabilities
    to test API resilience and identify weaknesses.
    
    Attributes:
        config: Chaos engineering configuration
    
    Example:
        >>> engineer = APIChaosEngineer()
        >>> result = await engineer.run_experiment(experiment)
    """
    
    def __init__(self, config: Optional[ChaosConfig] = None):
        """Initialize the chaos engineer.
        
        Args:
            config: Chaos configuration. Uses defaults if not provided.
        """
        self.config = config or ChaosConfig()
        self._experiments: Dict[str, ChaosExperiment] = {}
        self._active_experiments: Dict[str, ExperimentResult] = {}
        self._action_handlers: Dict[ChaosAction, Callable] = {}
        self._steady_state_checkers: Dict[str, Callable] = {}
        self._lock = threading.RLock()
        self._experiment_counter = 0
        self._register_default_handlers()
    
    def _register_default_handlers(self) -> None:
        """Register default chaos action handlers."""
        self._action_handlers = {
            ChaosAction.LATENCY: self._inject_latency,
            ChaosAction.TIMEOUT: self._inject_timeout,
            ChaosAction.ERROR: self._inject_error,
            ChaosAction.ABORT: self._inject_abort,
            ChaosAction.THROTTLE: self._inject_throttle,
            ChaosAction.BLACKLIST: self._inject_blacklist,
            ChaosAction.NETWORK_LOSS: self._inject_network_loss,
            ChaosAction.NETWORK_CORRUPT: self._inject_network_corrupt,
        }
    
    def register_experiment(self, experiment: ChaosExperiment) -> str:
        """Register a chaos experiment.
        
        Args:
            experiment: Experiment to register
        
        Returns:
            Experiment ID
        """
        with self._lock:
            self._experiments[experiment.experiment_id] = experiment
        return experiment.experiment_id
    
    def create_experiment(
        self,
        name: str,
        description: str = "",
        tags: Optional[Set[str]] = None
    ) -> ChaosExperiment:
        """Create a new chaos experiment.
        
        Args:
            name: Experiment name
            description: Experiment description
            tags: Optional tags
        
        Returns:
            Created experiment
        """
        with self._lock:
            self._experiment_counter += 1
            experiment_id = f"exp_{self._experiment_counter}_{int(time.time())}"
        
        experiment = ChaosExperiment(
            experiment_id=experiment_id,
            name=name,
            description=description,
            tags=tags or set()
        )
        
        self.register_experiment(experiment)
        return experiment
    
    def add_action(
        self,
        experiment_id: str,
        action_type: ChaosAction,
        target: ChaosTarget,
        severity: Severity = Severity.MEDIUM,
        duration: float = 10.0,
        probability: float = 1.0,
        parameters: Optional[Dict[str, Any]] = None
    ) -> ChaosAction_:
        """Add an action to an experiment.
        
        Args:
            experiment_id: Target experiment
            action_type: Type of chaos action
            target: Target for the action
            severity: Severity level
            duration: Duration in seconds
            probability: Trigger probability
            parameters: Action parameters
        
        Returns:
            Created action
        
        Raises:
            ValueError: If experiment not found
        """
        with self._lock:
            experiment = self._experiments.get(experiment_id)
            if not experiment:
                raise ValueError(f"Experiment not found: {experiment_id}")
        
        action = ChaosAction_(
            action_type=action_type,
            target=target,
            severity=severity,
            duration=duration,
            probability=probability,
            parameters=parameters or {}
        )
        
        with self._lock:
            experiment.actions.append(action)
        
        return action
    
    def set_steady_state_checker(
        self,
        name: str,
        checker_fn: Callable[[], Dict[str, Any]]
    ) -> None:
        """Set a steady state checker function.
        
        Args:
            name: Checker name
            checker_fn: Async function that returns steady state metrics
        """
        self._steady_state_checkers[name] = checker_fn
    
    async def run_experiment(
        self,
        experiment_id: str,
        execute_fn: Optional[Callable] = None
    ) -> ExperimentResult:
        """Run a chaos experiment.
        
        Args:
            experiment_id: Experiment to run
            execute_fn: Optional function to execute during chaos
        
        Returns:
            ExperimentResult
        
        Raises:
            ValueError: If experiment not found
        """
        with self._lock:
            experiment = self._experiments.get(experiment_id)
            if not experiment:
                raise ValueError(f"Experiment not found: {experiment_id}")
        
        result = ExperimentResult(
            experiment_id=experiment_id,
            status=ExperimentStatus.RUNNING,
            start_time=datetime.now()
        )
        
        self._active_experiments[experiment_id] = result
        
        try:
            # Measure steady state before
            result.steady_state_before = await self._measure_steady_state()
            
            # Inject chaos actions
            await self._inject_chaos(experiment.actions, result)
            
            # Measure steady state during
            result.steady_state_during = await self._measure_steady_state()
            
            # Execute workload if provided
            if execute_fn:
                await execute_fn()
            
            # Measure steady state after
            result.steady_state_after = await self._measure_steady_state()
            
            # Analyze results
            result.recommendations = self._analyze_results(result)
            
            result.status = ExperimentStatus.COMPLETED
            
        except Exception as e:
            result.errors.append(str(e))
            result.status = ExperimentStatus.FAILED
            
            if self.config.auto_rollback:
                await self._rollback(experiment)
        
        result.end_time = datetime.now()
        experiment.last_run = result.end_time
        
        with self._lock:
            self._active_experiments.pop(experiment_id, None)
        
        return result
    
    async def _measure_steady_state(self) -> Dict[str, Any]:
        """Measure current steady state metrics.
        
        Returns:
            Dictionary of metrics
        """
        metrics: Dict[str, Any] = {}
        
        for name, checker in self._steady_state_checkers.items():
            try:
                if asyncio.iscoroutinefunction(checker):
                    metrics[name] = await checker()
                else:
                    metrics[name] = checker()
            except Exception as e:
                metrics[name] = {"error": str(e)}
        
        return metrics
    
    async def _inject_chaos(
        self,
        actions: List[ChaosAction_],
        result: ExperimentResult
    ) -> None:
        """Inject chaos actions.
        
        Args:
            actions: Actions to inject
            result: Result to update
        """
        for action in actions:
            # Check probability
            if random.random() > action.probability:
                continue
            
            handler = self._action_handlers.get(action.action_type)
            if handler:
                try:
                    if asyncio.iscoroutinefunction(handler):
                        await handler(action)
                    else:
                        handler(action)
                    result.actions_triggered += 1
                except Exception as e:
                    result.errors.append(f"Action {action.action_type.value} failed: {str(e)}")
    
    async def _inject_latency(self, action: ChaosAction_) -> None:
        """Inject latency chaos.
        
        Args:
            action: Chaos action configuration
        """
        delay = action.parameters.get("delay_ms", 1000)
        jitter = action.parameters.get("jitter_ms", 100)
        
        actual_delay = delay + random.randint(-jitter, jitter)
        
        if not self.config.dry_run:
            await asyncio.sleep(actual_delay / 1000.0)
    
    async def _inject_timeout(self, action: ChaosAction_) -> None:
        """Inject timeout chaos.
        
        Args:
            action: Chaos action configuration
        """
        timeout = action.parameters.get("timeout_ms", 1)
        
        if not self.config.dry_run:
            await asyncio.sleep(timeout / 1000.0)
            raise TimeoutError(f"Simulated timeout of {timeout}ms on {action.target.identifier}")
    
    async def _inject_error(self, action: ChaosAction_) -> None:
        """Inject error chaos.
        
        Args:
            action: Chaos action configuration
        """
        error_code = action.parameters.get("error_code", 500)
        error_message = action.parameters.get("error_message", "Chaos injection error")
        
        if not self.config.dry_run:
            raise Exception(f"HTTP {error_code}: {error_message}")
    
    async def _inject_abort(self, action: ChaosAction_) -> None:
        """Inject connection abort chaos.
        
        Args:
            action: Chaos action configuration
        """
        if not self.config.dry_run:
            raise ConnectionAbortedError(f"Connection aborted on {action.target.identifier}")
    
    async def _inject_throttle(self, action: ChaosAction_) -> None:
        """Inject throttling chaos.
        
        Args:
            action: Chaos action configuration
        """
        rate = action.parameters.get("rate", 1)  # Requests per second
        
        if not self.config.dry_run:
            await asyncio.sleep(1.0 / rate)
    
    async def _inject_blacklist(self, action: ChaosAction_) -> None:
        """Inject IP blacklisting chaos.
        
        Args:
            action: Chaos action configuration
        """
        # In real implementation, would update firewall rules
        if not self.config.dry_run:
            await asyncio.sleep(action.duration)
    
    async def _inject_network_loss(self, action: ChaosAction_) -> None:
        """Inject network packet loss.
        
        Args:
            action: Chaos action configuration
        """
        loss_rate = action.parameters.get("loss_rate", 0.5)
        
        if random.random() < loss_rate:
            raise ConnectionError("Simulated packet loss")
    
    async def _inject_network_corrupt(self, action: ChaosAction_) -> None:
        """Inject network packet corruption.
        
        Args:
            action: Chaos action configuration
        """
        if not self.config.dry_run:
            await asyncio.sleep(0.001)  # Small delay for corruption
    
    async def _rollback(self, experiment: ChaosExperiment) -> None:
        """Execute rollback procedures.
        
        Args:
            experiment: Experiment with rollbacks
        """
        for rollback in experiment.rollbacks:
            try:
                # In real implementation, execute rollback actions
                await asyncio.sleep(0.1)
            except Exception as e:
                pass  # Best effort rollback
    
    def _analyze_results(self, result: ExperimentResult) -> List[str]:
        """Analyze experiment results and generate recommendations.
        
        Args:
            result: Experiment results
        
        Returns:
            List of recommendations
        """
        recommendations = []
        
        # Compare steady states
        before = result.steady_state_before
        during = result.steady_state_during
        after = result.steady_state_after
        
        for key in before:
            if key in during and key in after:
                before_val = before.get(key, 0)
                during_val = during.get(key, 0)
                after_val = after.get(key, 0)
                
                # Check for significant degradation
                if isinstance(before_val, (int, float)) and isinstance(during_val, (int, float)):
                    if before_val > 0:
                        change = (during_val - before_val) / before_val
                        if abs(change) > 0.2:  # 20% change threshold
                            recommendations.append(
                                f"Steady state '{key}' changed by {change*100:.1f}% during experiment"
                            )
        
        if not recommendations:
            recommendations.append("No significant impact detected. Consider increasing chaos severity.")
        
        return recommendations
    
    def abort_experiment(self, experiment_id: str) -> bool:
        """Abort a running experiment.
        
        Args:
            experiment_id: Experiment to abort
        
        Returns:
            True if aborted successfully
        """
        with self._lock:
            if experiment_id in self._active_experiments:
                self._active_experiments[experiment_id].status = ExperimentStatus.ABORTED
                return True
            return False
    
    def get_experiment_status(self, experiment_id: str) -> Optional[ExperimentResult]:
        """Get status of an experiment.
        
        Args:
            experiment_id: Experiment identifier
        
        Returns:
            ExperimentResult or None
        """
        with self._lock:
            return self._active_experiments.get(experiment_id)
    
    def get_experiment(self, experiment_id: str) -> Optional[ChaosExperiment]:
        """Get an experiment definition.
        
        Args:
            experiment_id: Experiment identifier
        
        Returns:
            ChaosExperiment or None
        """
        with self._lock:
            return self._experiments.get(experiment_id)
    
    def list_experiments(self, tags: Optional[Set[str]] = None) -> List[ChaosExperiment]:
        """List registered experiments.
        
        Args:
            tags: Optional filter by tags
        
        Returns:
            List of experiments
        """
        with self._lock:
            experiments = list(self._experiments.values())
        
        if tags:
            experiments = [e for e in experiments if e.tags & tags]
        
        return experiments
