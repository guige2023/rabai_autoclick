"""
Automation Rollback Module.

Provides workflow rollback, compensation, and state recovery
for failed automation operations.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
from collections import deque
import logging

logger = logging.getLogger(__name__)


class RollbackStrategy(Enum):
    """Rollback strategy types."""
    COMPENSATION = "compensation"
    STATE_RESTORE = "state_restore"
    COMPENSATION_CHAIN = "compensation_chain"
    SAGAS = "sagas"


@dataclass
class WorkflowStep:
    """Container for a workflow step."""
    step_id: str
    name: str
    execute: Callable[[Dict[str, Any]], Any]
    rollback: Optional[Callable[[Dict[str, Any], Any], Any]] = None
    compensate: Optional[Callable[[Dict[str, Any]], Any]] = None
    timeout: float = 30.0
    retryable: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    
@dataclass
class StepResult:
    """Result of a step execution."""
    step_id: str
    success: bool
    result: Any = None
    error: Optional[str] = None
    started_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    rollback_performed: bool = False


@dataclass
class WorkflowState:
    """Workflow execution state."""
    workflow_id: str
    current_step: int
    completed_steps: List[StepResult]
    context: Dict[str, Any]
    started_at: float
    status: str = "running"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RollbackConfig:
    """Configuration for rollback handling."""
    strategy: RollbackStrategy = RollbackStrategy.COMPENSATION
    max_retries: int = 3
    rollback_timeout: float = 60.0
    continue_on_rollback_error: bool = True
    enable_checkpoints: bool = True
    checkpoint_interval: int = 5


class RollbackManager:
    """
    Manages workflow rollback and compensation.
    
    Example:
        manager = RollbackManager(RollbackConfig(
            strategy=RollbackStrategy.COMPENSATION
        ))
        
        # Define workflow with rollback
        workflow = [
            WorkflowStep(
                step_id="1",
                name="create_order",
                execute=create_order,
                rollback=cancel_order
            ),
            WorkflowStep(
                step_id="2", 
                name="process_payment",
                execute=process_payment,
                rollback=refund_payment
            ),
        ]
        
        result = await manager.execute_with_rollback(workflow, context)
    """
    
    def __init__(self, config: Optional[RollbackConfig] = None) -> None:
        """
        Initialize rollback manager.
        
        Args:
            config: Rollback configuration.
        """
        self.config = config or RollbackConfig()
        self._workflows: Dict[str, List[WorkflowStep]] = {}
        self._checkpoints: Dict[str, deque] = {}
        self._lock = asyncio.Lock()
        
    def register_workflow(
        self,
        workflow_id: str,
        steps: List[WorkflowStep],
    ) -> None:
        """
        Register a workflow for rollback support.
        
        Args:
            workflow_id: Unique workflow identifier.
            steps: List of workflow steps with rollback handlers.
        """
        self._workflows[workflow_id] = steps
        self._checkpoints[workflow_id] = deque(maxlen=100)
        
    async def execute_with_rollback(
        self,
        workflow_id: str,
        context: Dict[str, Any],
        workflow: Optional[List[WorkflowStep]] = None,
    ) -> Tuple[bool, WorkflowState]:
        """
        Execute workflow with rollback support.
        
        Args:
            workflow_id: Workflow identifier.
            context: Initial workflow context.
            workflow: Optional inline workflow definition.
            
        Returns:
            Tuple of (success, final_state).
        """
        workflow = workflow or self._workflows.get(workflow_id, [])
        
        if not workflow:
            raise ValueError(f"Workflow {workflow_id} not found")
            
        state = WorkflowState(
            workflow_id=workflow_id,
            current_step=0,
            completed_steps=[],
            context=context,
            started_at=time.time(),
        )
        
        # Save initial checkpoint
        if self.config.enable_checkpoints:
            await self._save_checkpoint(workflow_id, state)
            
        try:
            for i, step in enumerate(workflow):
                state.current_step = i
                
                # Execute step
                result = await self._execute_step(step, state.context)
                state.completed_steps.append(result)
                
                if not result.success:
                    logger.error(f"Step {step.name} failed: {result.error}")
                    
                    # Attempt rollback
                    rollback_success = await self._rollback_steps(
                        workflow_id,
                        state.completed_steps[:-1],
                        state.context,
                    )
                    
                    state.status = "rolled_back" if rollback_success else "rollback_failed"
                    return False, state
                    
                # Update context with result
                if result.result:
                    state.context[f"step_{i}_result"] = result.result
                    
                # Periodic checkpoint
                if self.config.enable_checkpoints and i % self.config.checkpoint_interval == 0:
                    await self._save_checkpoint(workflow_id, state)
                    
            state.status = "completed"
            return True, state
            
        except Exception as e:
            logger.error(f"Workflow {workflow_id} failed: {e}")
            
            # Rollback completed steps
            await self._rollback_steps(
                workflow_id,
                state.completed_steps,
                state.context,
            )
            
            state.status = "failed"
            return False, state
            
    async def _execute_step(
        self,
        step: WorkflowStep,
        context: Dict[str, Any],
    ) -> StepResult:
        """Execute a single workflow step."""
        result = StepResult(step_id=step.step_id, success=False)
        
        for attempt in range(self.config.max_retries if step.retryable else 1):
            try:
                if asyncio.iscoroutinefunction(step.execute):
                    exec_result = await asyncio.wait_for(
                        step.execute(context),
                        timeout=step.timeout,
                    )
                else:
                    exec_result = await asyncio.wait_for(
                        asyncio.to_thread(step.execute, context),
                        timeout=step.timeout,
                    )
                    
                result.success = True
                result.result = exec_result
                result.completed_at = time.time()
                return result
                
            except asyncio.TimeoutError:
                result.error = f"Step timed out after {step.timeout}s"
            except Exception as e:
                result.error = str(e)
                
            if attempt < (self.config.max_retries - 1) and step.retryable:
                await asyncio.sleep(1 * (attempt + 1))
                
        result.completed_at = time.time()
        return result
        
    async def _rollback_steps(
        self,
        workflow_id: str,
        completed_steps: List[StepResult],
        context: Dict[str, Any],
    ) -> bool:
        """
        Rollback completed steps in reverse order.
        
        Args:
            workflow_id: Workflow identifier.
            completed_steps: Steps that were completed.
            context: Workflow context.
            
        Returns:
            True if all rollbacks succeeded.
        """
        logger.info(f"Starting rollback for {len(completed_steps)} steps")
        
        rollback_errors = []
        
        # Rollback in reverse order
        for step_result in reversed(completed_steps):
            if not step_result.success:
                continue
                
            # Get step definition
            workflow = self._workflows.get(workflow_id, [])
            step_def = next((s for s in workflow if s.step_id == step_result.step_id), None)
            
            if not step_def:
                continue
                
            if step_def.rollback:
                try:
                    logger.info(f"Rolling back step {step_def.name}")
                    
                    if asyncio.iscoroutinefunction(step_def.rollback):
                        await asyncio.wait_for(
                            step_def.rollback(context, step_result.result),
                            timeout=self.config.rollback_timeout,
                        )
                    else:
                        await asyncio.wait_for(
                            asyncio.to_thread(step_def.rollback, context, step_result.result),
                            timeout=self.config.rollback_timeout,
                        )
                            
                    step_result.rollback_performed = True
                    
                except Exception as e:
                    logger.error(f"Rollback failed for step {step_def.name}: {e}")
                    rollback_errors.append((step_def.name, str(e)))
                    
                    if not self.config.continue_on_rollback_error:
                        return False
                        
            elif step_def.compensate:
                # Compensation pattern
                try:
                    if asyncio.iscoroutinefunction(step_def.compensate):
                        await asyncio.wait_for(
                            step_def.compensate(context),
                            timeout=self.config.rollback_timeout,
                        )
                    else:
                        await asyncio.wait_for(
                            asyncio.to_thread(step_def.compensate, context),
                            timeout=self.config.rollback_timeout,
                        )
                            
                    step_result.rollback_performed = True
                    
                except Exception as e:
                    logger.error(f"Compensation failed for step {step_def.name}: {e}")
                    rollback_errors.append((step_def.name, str(e)))
                    
                    if not self.config.continue_on_rollback_error:
                        return False
                        
        return len(rollback_errors) == 0
        
    async def _save_checkpoint(
        self,
        workflow_id: str,
        state: WorkflowState,
    ) -> None:
        """Save workflow checkpoint."""
        checkpoint = WorkflowState(
            workflow_id=state.workflow_id,
            current_step=state.current_step,
            completed_steps=list(state.completed_steps),
            context=dict(state.context),
            started_at=state.started_at,
            status=state.status,
        )
        
        self._checkpoints[workflow_id].append(checkpoint)
        
    async def restore_checkpoint(
        self,
        workflow_id: str,
        checkpoint_index: int = -1,
    ) -> Optional[WorkflowState]:
        """
        Restore workflow from checkpoint.
        
        Args:
            workflow_id: Workflow identifier.
            checkpoint_index: Checkpoint to restore (-1 for latest).
            
        Returns:
            Restored state or None.
        """
        checkpoints = self._checkpoints.get(workflow_id, [])
        
        if not checkpoints:
            return None
            
        if checkpoint_index < 0:
            checkpoint_index = len(checkpoints) + checkpoint_index
            
        if 0 <= checkpoint_index < len(checkpoints):
            return checkpoints[checkpoint_index]
            
        return None


class SagasCoordinator:
    """
    Sagas pattern coordinator for distributed transactions.
    
    Example:
        coordinator = SagasCoordinator()
        
        coordinator.add_transaction(
            name="order",
            execute=create_order,
            compensate=cancel_order
        )
        coordinator.add_transaction(
            name="payment",
            execute=charge_payment,
            compensate=refund_payment
        )
        
        result = await coordinator.execute(saga_id="order_123", context={})
    """
    
    def __init__(self, max_retries: int = 3) -> None:
        """
        Initialize saga coordinator.
        
        Args:
            max_retries: Maximum compensation retries.
        """
        self.max_retries = max_retries
        self._transactions: Dict[str, Dict[str, Any]] = {}
        self._saga_states: Dict[str, Dict[str, Any]] = {}
        
    def add_transaction(
        self,
        name: str,
        execute: Callable[[Dict[str, Any]], Any],
        compensate: Callable[[Dict[str, Any]], Any],
        retryable: bool = False,
    ) -> None:
        """
        Add a transaction to the saga.
        
        Args:
            name: Transaction name.
            execute: Execute function.
            compensate: Compensation function.
            retryable: Whether transaction is retryable.
        """
        self._transactions[name] = {
            "execute": execute,
            "compensate": compensate,
            "retryable": retryable,
        }
        
    async def execute(
        self,
        saga_id: str,
        context: Dict[str, Any],
        transaction_order: Optional[List[str]] = None,
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Execute saga with all transactions.
        
        Args:
            saga_id: Unique saga identifier.
            context: Initial context.
            transaction_order: Optional specific order of transactions.
            
        Returns:
            Tuple of (success, final_context).
        """
        order = transaction_order or list(self._transactions.keys())
        completed: List[Tuple[str, Any]] = []
        
        state = {
            "saga_id": saga_id,
            "completed": [],
            "failed": None,
            "compensated": [],
        }
        
        self._saga_states[saga_id] = state
        
        for tx_name in order:
            tx = self._transactions.get(tx_name)
            if not tx:
                logger.error(f"Transaction {tx_name} not found")
                continue
                
            try:
                logger.info(f"Executing saga transaction: {tx_name}")
                
                if asyncio.iscoroutinefunction(tx["execute"]):
                    result = await tx["execute"](context)
                else:
                    result = await asyncio.to_thread(tx["execute"], context)
                    
                completed.append((tx_name, result))
                state["completed"].append({"name": tx_name, "result": result})
                
            except Exception as e:
                logger.error(f"Saga transaction {tx_name} failed: {e}")
                state["failed"] = {"name": tx_name, "error": str(e)}
                
                # Compensate in reverse order
                for comp_name, comp_result in reversed(completed):
                    comp_tx = self._transactions[comp_name]
                    
                    for attempt in range(self.max_retries):
                        try:
                            logger.info(f"Compensating: {comp_name}")
                            
                            if asyncio.iscoroutinefunction(comp_tx["compensate"]):
                                await comp_tx["compensate"](context, comp_result)
                            else:
                                await asyncio.to_thread(
                                    comp_tx["compensate"], context, comp_result
                                )
                                
                            state["compensated"].append(comp_name)
                            break
                            
                        except Exception as comp_error:
                            logger.warning(
                                f"Compensation {comp_name} attempt {attempt + 1} failed: {comp_error}"
                            )
                            
                            if attempt == self.max_retries - 1:
                                logger.error(f"Compensation {comp_name} failed permanently")
                                
                return False, context
                
        return True, context
        
    def get_saga_state(self, saga_id: str) -> Optional[Dict[str, Any]]:
        """Get current saga state."""
        return self._saga_states.get(saga_id)
