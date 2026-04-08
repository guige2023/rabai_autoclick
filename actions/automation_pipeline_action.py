"""Automation pipeline action module for RabAI AutoClick.

Provides pipeline orchestration for automation workflows:
- AutomationPipeline: Orchestrate multi-step automation
- PipelineExecutor: Execute automation steps with dependencies
- StepDependencyGraph: Manage step dependencies
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


class StepState(Enum):
    """Step execution states."""
    PENDING = "pending"
    BLOCKED = "blocked"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class DependencyType(Enum):
    """Step dependency types."""
    BLOCKS = "blocks"
    REQUIRES = "requires"
    ENABLES = "enables"
    CONCURRENT = "concurrent"


@dataclass
class AutomationStep:
    """Automation step definition."""
    step_id: str
    name: str
    action: Optional[Callable] = None
    timeout: float = 60.0
    retry_count: int = 0
    retry_delay: float = 1.0
    continue_on_failure: bool = False
    condition: Optional[Callable] = None
    cleanup: Optional[Callable] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StepDependency:
    """Step dependency definition."""
    from_step: str
    to_step: str
    dependency_type: DependencyType = DependencyType.REQUIRES


@dataclass
class PipelineExecutionResult:
    """Result of pipeline execution."""
    success: bool
    completed_steps: List[str]
    failed_steps: List[str]
    skipped_steps: List[str]
    step_results: Dict[str, Any]
    total_duration: float
    error: Optional[str] = None


class AutomationPipeline:
    """Automation pipeline with dependency management."""
    
    def __init__(self, name: str):
        self.name = name
        self._steps: Dict[str, AutomationStep] = {}
        self._dependencies: List[StepDependency] = []
        self._dependents: Dict[str, Set[str]] = defaultdict(set)
        self._required_by: Dict[str, Set[str]] = defaultdict(set)
        self._step_states: Dict[str, StepState] = {}
        self._step_results: Dict[str, Any] = {}
        self._execution_order: List[str] = []
        self._lock = threading.RLock()
        self._running = False
        self._cancelled = False
    
    def add_step(self, step: AutomationStep) -> "AutomationPipeline":
        """Add step to pipeline."""
        with self._lock:
            self._steps[step.step_id] = step
            self._step_states[step.step_id] = StepState.PENDING
        return self
    
    def add_dependency(self, from_step: str, to_step: str, dep_type: DependencyType = DependencyType.REQUIRES):
        """Add dependency between steps."""
        with self._lock:
            self._dependencies.append(StepDependency(from_step, to_step, dep_type))
            self._dependents[from_step].add(to_step)
            self._required_by[to_step].add(from_step)
    
    def _get_execution_order(self) -> List[List[str]]:
        """Get execution order with parallelizable steps grouped."""
        with self._lock:
            in_degree = {step_id: len(self._required_by[step_id]) for step_id in self._steps}
            
            levels = []
            remaining = set(self._steps.keys())
            
            while remaining:
                current_level = [s for s in remaining if in_degree[s] == 0]
                
                if not current_level:
                    raise ValueError("Circular dependency detected")
                
                levels.append(current_level)
                
                for step_id in current_level:
                    remaining.remove(step_id)
                    for dependent in self._dependents[step_id]:
                        in_degree[dependent] -= 1
            
            return levels
    
    def _can_execute(self, step_id: str) -> bool:
        """Check if step can execute."""
        with self._lock:
            for required_step_id in self._required_by[step_id]:
                if self._step_states[required_step_id] != StepState.COMPLETED:
                    return False
            return True
    
    def execute_step(self, step_id: str) -> Tuple[bool, Any]:
        """Execute single step."""
        with self._lock:
            step = self._steps.get(step_id)
            if not step:
                return False, ValueError(f"Step {step_id} not found")
        
        if step.condition and not step.condition():
            with self._lock:
                self._step_states[step_id] = StepState.SKIPPED
            return True, None
        
        self._step_states[step_id] = StepState.RUNNING
        
        last_error = None
        for attempt in range(step.retry_count + 1):
            try:
                if step.timeout:
                    result = [None]
                    error = [None]
                    
                    def worker():
                        try:
                            result[0] = step.action()
                        except Exception as e:
                            error[0] = e
                    
                    t = threading.Thread(target=worker)
                    t.daemon = True
                    t.start()
                    t.join(timeout=step.timeout)
                    
                    if t.is_alive():
                        raise TimeoutError(f"Step {step_id} timed out after {step.timeout}s")
                    if error[0]:
                        raise error[0]
                    result_data = result[0]
                else:
                    result_data = step.action()
                
                with self._lock:
                    self._step_states[step_id] = StepState.COMPLETED
                    self._step_results[step_id] = result_data
                
                return True, result_data
                
            except Exception as e:
                last_error = e
                if attempt < step.retry_count:
                    time.sleep(step.retry_delay)
        
        with self._lock:
            self._step_states[step_id] = StepState.FAILED
            self._step_results[step_id] = last_error
        
        if not step.continue_on_failure:
            raise last_error
        
        return False, last_error
    
    def execute(self) -> PipelineExecutionResult:
        """Execute entire pipeline."""
        start_time = time.time()
        completed = []
        failed = []
        skipped = []
        
        try:
            levels = self._get_execution_order()
            
            for level in levels:
                if self._cancelled:
                    break
                
                threads = []
                results = {}
                
                def run_step(step_id: str):
                    success, result = self.execute_step(step_id)
                    results[step_id] = (success, result)
                
                for step_id in level:
                    if self._can_execute(step_id):
                        t = threading.Thread(target=run_step, args=(step_id,))
                        threads.append(t)
                        t.start()
                    else:
                        self._step_states[step_id] = StepState.BLOCKED
                
                for t in threads:
                    t.join()
                
                for step_id, (success, result) in results.items():
                    if self._step_states[step_id] == StepState.COMPLETED:
                        completed.append(step_id)
                    elif self._step_states[step_id] == StepState.FAILED:
                        failed.append(step_id)
                    elif self._step_states[step_id] == StepState.SKIPPED:
                        skipped.append(step_id)
                
                failed_steps_exist = any(self._step_states[s] == StepState.FAILED for s in level)
                if failed_steps_exist:
                    break
            
        except Exception as e:
            return PipelineExecutionResult(
                success=False,
                completed_steps=completed,
                failed_steps=failed,
                skipped_steps=skipped,
                step_results=dict(self._step_results),
                total_duration=time.time() - start_time,
                error=str(e)
            )
        
        return PipelineExecutionResult(
            success=len(failed) == 0,
            completed_steps=completed,
            failed_steps=failed,
            skipped_steps=skipped,
            step_results=dict(self._step_results),
            total_duration=time.time() - start_time
        )
    
    def cancel(self):
        """Cancel pipeline execution."""
        self._cancelled = True
    
    def get_state(self) -> Dict[str, StepState]:
        """Get current pipeline state."""
        with self._lock:
            return dict(self._step_states)


class AutomationPipelineAction(BaseAction):
    """Automation pipeline action."""
    action_type = "automation_pipeline"
    display_name = "自动化流水线"
    description = "自动化多步骤流水线编排"
    
    def __init__(self):
        super().__init__()
        self._pipelines: Dict[str, AutomationPipeline] = {}
        self._lock = threading.Lock()
    
    def _get_pipeline(self, name: str) -> AutomationPipeline:
        """Get or create pipeline."""
        with self._lock:
            if name not in self._pipelines:
                self._pipelines[name] = AutomationPipeline(name)
            return self._pipelines[name]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute pipeline operation."""
        try:
            pipeline_name = params.get("pipeline", "default")
            command = params.get("command", "execute")
            
            pipeline = self._get_pipeline(pipeline_name)
            
            if command == "add_step":
                step_id = params.get("step_id")
                name = params.get("name", step_id)
                action = params.get("action")
                
                if step_id:
                    step = AutomationStep(
                        step_id=step_id,
                        name=name,
                        action=action,
                        timeout=params.get("timeout", 60.0),
                        retry_count=params.get("retry_count", 0),
                        retry_delay=params.get("retry_delay", 1.0),
                        continue_on_failure=params.get("continue_on_failure", False),
                    )
                    pipeline.add_step(step)
                    return ActionResult(success=True, message=f"Step {step_id} added")
                return ActionResult(success=False, message="step_id required")
            
            elif command == "add_dep":
                from_step = params.get("from_step")
                to_step = params.get("to_step")
                dep_type_str = params.get("dependency_type", "requires").upper()
                
                dep_type = DependencyType[dep_type_str] if dep_type_str in [d.name for d in DependencyType] else DependencyType.REQUIRES
                pipeline.add_dependency(from_step, to_step, dep_type)
                return ActionResult(success=True, message=f"Dependency added: {from_step} -> {to_step}")
            
            elif command == "execute":
                result = pipeline.execute()
                return ActionResult(
                    success=result.success,
                    message=result.error or f"Completed {len(result.completed_steps)} steps",
                    data={
                        "completed": result.completed_steps,
                        "failed": result.failed_steps,
                        "skipped": result.skipped_steps,
                        "duration": result.total_duration,
                        "results": result.step_results,
                    }
                )
            
            elif command == "cancel":
                pipeline.cancel()
                return ActionResult(success=True)
            
            elif command == "state":
                state = pipeline.get_state()
                return ActionResult(success=True, data={"state": state})
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"AutomationPipelineAction error: {str(e)}")
