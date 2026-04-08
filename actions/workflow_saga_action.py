"""
Workflow Saga Action.

Provides saga pattern implementation for distributed transactions.
Supports:
- Choreography-based saga
- Orchestration-based saga
- Compensation actions
- Error handling and rollback
"""

from typing import Dict, List, Optional, Any, Callable, Awaitable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import logging
import json
import uuid

logger = logging.getLogger(__name__)


class SagaStatus(Enum):
    """Saga execution status."""
    STARTED = "started"
    RUNNING = "running"
    COMPLETED = "completed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"
    FAILED = "failed"


class StepStatus(Enum):
    """Individual saga step status."""
    PENDING = "pending"
    EXECUTING = "executing"
    COMPLETED = "completed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class SagaStep:
    """Represents a single step in a saga."""
    step_id: str
    name: str
    execute: Callable[["SagaContext"], Awaitable[Any]]
    compensate: Optional[Callable[["SagaContext"], Awaitable[None]]] = None
    retry_count: int = 0
    max_retries: int = 3
    timeout: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if self.step_id is None:
            self.step_id = str(uuid.uuid4())[:8]


@dataclass
class StepResult:
    """Result of a saga step execution."""
    step_id: str
    step_name: str
    status: StepStatus
    input_data: Any = None
    output_data: Any = None
    error: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    compensation_done: bool = False
    
    @property
    def duration_ms(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds() * 1000
        return None


@dataclass
class SagaContext:
    """Context passed through saga execution."""
    saga_id: str
    step_results: Dict[str, StepResult] = field(default_factory=dict)
    shared_data: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def get_result(self, step_id: str) -> Optional[StepResult]:
        """Get result of a specific step."""
        return self.step_results.get(step_id)
    
    def get_output(self, step_id: str) -> Any:
        """Get output of a specific step."""
        result = self.step_results.get(step_id)
        return result.output_data if result else None


class WorkflowSagaAction:
    """
    Workflow Saga Action.
    
    Provides saga pattern for distributed transactions with support for:
    - Sequential step execution
    - Compensation (rollback) actions
    - Error handling with retry
    - Partial completion handling
    """
    
    def __init__(self, name: str):
        """
        Initialize the Workflow Saga Action.
        
        Args:
            name: Saga name
        """
        self.name = name
        self.steps: List[SagaStep] = []
        self.status = SagaStatus.STARTED
        self.saga_id: Optional[str] = None
        self.context: Optional[SagaContext] = None
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self._compensating = False
    
    def add_step(
        self,
        name: str,
        execute: Callable[[SagaContext], Awaitable[Any]],
        compensate: Optional[Callable[[SagaContext], Awaitable[None]]] = None,
        max_retries: int = 3,
        timeout: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> "WorkflowSagaAction":
        """
        Add a step to the saga.
        
        Args:
            name: Step name
            execute: Async function to execute
            compensate: Async function to compensate (rollback)
            max_retries: Maximum retry attempts
            timeout: Step timeout in seconds
            metadata: Additional metadata
        
        Returns:
            Self for chaining
        """
        step = SagaStep(
            step_id=str(uuid.uuid4())[:8],
            name=name,
            execute=execute,
            compensate=compensate,
            max_retries=max_retries,
            timeout=timeout,
            metadata=metadata or {}
        )
        self.steps.append(step)
        return self
    
    async def execute(
        self,
        initial_data: Optional[Dict[str, Any]] = None
    ) -> SagaContext:
        """
        Execute the saga.
        
        Args:
            initial_data: Initial data to pass through saga
        
        Returns:
            SagaContext with all step results
        """
        if self.status in (SagaStatus.RUNNING, SagaStatus.COMPENSATING):
            raise RuntimeError("Saga is already running")
        
        self.status = SagaStatus.RUNNING
        self.start_time = datetime.utcnow()
        self.saga_id = str(uuid.uuid4())
        self.context = SagaContext(
            saga_id=self.saga_id,
            shared_data=initial_data or {}
        )
        
        logger.info(f"Starting saga: {self.name} ({self.saga_id})")
        
        try:
            # Execute steps sequentially
            for step in self.steps:
                await self._execute_step(step)
                
                # Check if step failed and we need to compensate
                step_result = self.context.step_results.get(step.step_id)
                if step_result and step_result.status == StepStatus.FAILED:
                    logger.warning(f"Step '{step.name}' failed, starting compensation")
                    await self._compensate()
                    self.status = SagaStatus.COMPENSATED
                    break
            
            if self.status == SagaStatus.RUNNING:
                self.status = SagaStatus.COMPLETED
        
        except Exception as e:
            logger.error(f"Saga failed: {e}")
            self.status = SagaStatus.FAILED
            if self.context:
                self.context.metadata["error"] = str(e)
            await self._compensate()
            self.status = SagaStatus.COMPENSATED
        
        finally:
            self.end_time = datetime.utcnow()
            logger.info(f"Saga finished: {self.name} ({self.status.value})")
        
        return self.context
    
    async def _execute_step(self, step: SagaStep) -> StepResult:
        """Execute a single step."""
        result = StepResult(
            step_id=step.step_id,
            step_name=step.name,
            status=StepStatus.EXECUTING,
            start_time=datetime.utcnow()
        )
        
        for attempt in range(step.max_retries + 1):
            try:
                if step.timeout:
                    output = await asyncio.wait_for(
                        step.execute(self.context),
                        timeout=step.timeout
                    )
                else:
                    output = await step.execute(self.context)
                
                result.status = StepStatus.COMPLETED
                result.output_data = output
                break
            
            except asyncio.TimeoutError:
                result.error = f"Step timed out after {step.timeout}s"
                result.status = StepStatus.FAILED
            
            except Exception as e:
                result.error = str(e)
                
                if attempt < step.max_retries:
                    result.status = StepStatus.EXECUTING
                    logger.warning(
                        f"Step '{step.name}' failed, retrying "
                        f"({attempt + 1}/{step.max_retries}): {e}"
                    )
                    await asyncio.sleep(2 ** attempt)
                else:
                    result.status = StepStatus.FAILED
        
        result.end_time = datetime.utcnow()
        self.context.step_results[step.step_id] = result
        
        logger.debug(
            f"Step '{step.name}' {result.status.value} "
            f"({result.duration_ms:.0f}ms)"
        )
        
        return result
    
    async def _compensate(self) -> None:
        """Execute compensation actions in reverse order."""
        self._compensating = True
        self.status = SagaStatus.COMPENSATING
        
        logger.info(f"Starting compensation for saga: {self.name}")
        
        # Execute compensations in reverse order
        for step in reversed(self.steps):
            step_result = self.context.step_results.get(step.step_id)
            
            # Skip steps that didn't complete or have no compensation
            if not step_result or step_result.status != StepStatus.COMPLETED:
                continue
            
            if not step.compensate:
                logger.debug(f"Step '{step.name}' has no compensation, skipping")
                continue
            
            try:
                logger.info(f"Compensating step: {step.name}")
                await step.compensate(self.context)
                step_result.compensation_done = True
                
                # Update status to compensated
                self.context.step_results[step.step_id] = StepResult(
                    step_id=step.step_id,
                    step_name=step.name,
                    status=StepStatus.COMPENSATED,
                    output_data=step_result.output_data,
                    start_time=step_result.start_time,
                    end_time=datetime.utcnow(),
                    compensation_done=True
                )
            
            except Exception as e:
                logger.error(f"Compensation failed for step '{step.name}': {e}")
                # Continue with other compensations even if one fails
        
        logger.info(f"Compensation completed for saga: {self.name}")
    
    def get_progress(self) -> Dict[str, Any]:
        """Get saga progress."""
        if not self.context:
            return {"status": "not_started"}
        
        completed = sum(
            1 for r in self.context.step_results.values()
            if r.status == StepStatus.COMPLETED
        )
        failed = sum(
            1 for r in self.context.step_results.values()
            if r.status == StepStatus.FAILED
        )
        compensated = sum(
            1 for r in self.context.step_results.values()
            if r.compensation_done
        )
        
        return {
            "saga_id": self.saga_id,
            "status": self.status.value,
            "total_steps": len(self.steps),
            "completed_steps": completed,
            "failed_steps": failed,
            "compensated_steps": compensated,
            "step_statuses": {
                r.step_name: r.status.value
                for r in self.context.step_results.values()
            }
        }
    
    def get_summary(self) -> Dict[str, Any]:
        """Get saga execution summary."""
        progress = self.get_progress()
        
        duration_ms = None
        if self.start_time:
            end = self.end_time or datetime.utcnow()
            duration_ms = (end - self.start_time).total_seconds() * 1000
        
        return {
            "name": self.name,
            "saga_id": self.saga_id,
            "status": self.status.value,
            "duration_ms": duration_ms,
            "step_count": len(self.steps),
            "completed_count": progress.get("completed_steps", 0),
            "failed_count": progress.get("failed_steps", 0),
            "compensated_count": progress.get("compensated_steps", 0)
        }


# Example saga steps
async def reserve_inventory(ctx: SagaContext) -> Dict[str, Any]:
    """Reserve inventory step."""
    order_id = ctx.shared_data.get("order_id", "ORD-001")
    items = ctx.shared_data.get("items", [{"sku": "ITEM-1", "qty": 2}])
    await asyncio.sleep(0.1)
    return {"reservation_id": f"RES-{order_id}", "items": items}


async def charge_payment(ctx: SagaContext) -> Dict[str, Any]:
    """Charge payment step."""
    reservation = ctx.get_output("reserve_inventory")
    amount = ctx.shared_data.get("amount", 100.00)
    await asyncio.sleep(0.1)
    return {"payment_id": "PAY-001", "amount": amount, "reservation_id": reservation["reservation_id"]}


async def ship_order(ctx: SagaContext) -> Dict[str, Any]:
    """Ship order step."""
    payment = ctx.get_output("charge_payment")
    await asyncio.sleep(0.1)
    return {"shipment_id": "SHIP-001", "payment_id": payment["payment_id"]}


async def compensate_payment(ctx: SagaContext) -> None:
    """Compensate payment step."""
    payment = ctx.get_output("charge_payment")
    logger.info(f"Refunding payment: {payment['payment_id']}")
    await asyncio.sleep(0.1)


async def compensate_inventory(ctx: SagaContext) -> None:
    """Compensate inventory step."""
    reservation = ctx.get_output("reserve_inventory")
    logger.info(f"Releasing inventory: {reservation['reservation_id']}")
    await asyncio.sleep(0.1)


# Standalone execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    async def main():
        # Create saga
        saga = WorkflowSagaAction("order-processing")
        
        # Add steps with compensation
        saga.add_step(
            "reserve_inventory",
            reserve_inventory,
            compensate=compensate_inventory
        )
        saga.add_step(
            "charge_payment",
            charge_payment,
            compensate=compensate_payment
        )
        saga.add_step(
            "ship_order",
            ship_order
        )
        
        # Execute
        result = await saga.execute({
            "order_id": "ORD-12345",
            "items": [{"sku": "ITEM-1", "qty": 2}],
            "amount": 99.99
        })
        
        print(f"Saga status: {saga.status.value}")
        print(f"Summary: {json.dumps(saga.get_summary(), indent=2, default=str)}")
        print(f"Progress: {json.dumps(saga.get_progress(), indent=2)}")
        
        for step_id, step_result in result.step_results.items():
            print(f"  {step_result.step_name}: {step_result.status.value}")
    
    asyncio.run(main())
