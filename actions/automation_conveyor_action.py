"""Automation Conveyor Action Module.

Provides a conveyor belt pattern for sequential task processing
with batching, backpressure, and flow control.
"""

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Deque, Dict, List, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class ConveyorState(Enum):
    """Conveyor belt states."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    DRAINING = "draining"
    STOPPED = "stopped"


class BackpressurePolicy(Enum):
    """Backpressure handling policies."""
    DROP_OLDEST = "drop_oldest"
    DROP_NEWEST = "drop_newest"
    BLOCK = "block"
    ERROR = "error"


@dataclass
class ConveyorItem:
    """An item on the conveyor belt."""
    item_id: str
    data: Any
    priority: int = 0
    enqueued_at: float = field(default_factory=time.time)
    processed_at: Optional[float] = None
    result: Any = None
    error: Optional[str] = None


@dataclass
class ConveyorStats:
    """Conveyor processing statistics."""
    total_enqueued: int = 0
    total_processed: int = 0
    total_failed: int = 0
    total_dropped: int = 0
    current_queue_size: int = 0
    avg_process_time: float = 0.0


class AutomationConveyorAction(BaseAction):
    """Conveyor belt pattern for sequential task processing.

    Processes items through a conveyor with batching,
    backpressure control, and flow monitoring.

    Args:
        context: Execution context.
        params: Dict with keys:
            - operation: Operation type (enqueue, process, drain, status, pause, resume)
            - items: List[Dict] with items to enqueue
            - item: Single item dict to enqueue
            - batch_size: Number of items per processing batch
            - max_queue_size: Maximum queue before backpressure
            - backpressure_policy: How to handle overflow
            - processor: Name of processor function
    """
    action_type = "automation_conveyor"
    display_name = "自动化流水线"
    description = "顺序任务处理的流水线模式"

    def get_required_params(self) -> List[str]:
        return ["operation"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "items": [],
            "item": None,
            "batch_size": 10,
            "max_queue_size": 1000,
            "backpressure_policy": "drop_oldest",
            "processor": "default",
            "conveyor_id": "default",
        }

    def __init__(self) -> None:
        super().__init__()
        self._conveyors: Dict[str, Deque[ConveyorItem]] = {}
        self._conveyor_stats: Dict[str, ConveyorStats] = {}
        self._conveyor_state: Dict[str, ConveyorState] = {}
        self._processors: Dict[str, Callable] = {}

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute conveyor operation."""
        start_time = time.time()

        operation = params.get("operation", "status")
        conveyor_id = params.get("conveyor_id", "default")
        items = params.get("items", [])
        item = params.get("item")
        batch_size = params.get("batch_size", 10)
        max_queue_size = params.get("max_queue_size", 1000)
        backpressure = params.get("backpressure_policy", "drop_oldest")

        # Initialize conveyor
        if conveyor_id not in self._conveyors:
            self._conveyors[conveyor_id] = deque()
            self._conveyor_stats[conveyor_id] = ConveyorStats()
            self._conveyor_state[conveyor_id] = ConveyorState.IDLE

        conveyor = self._conveyors[conveyor_id]
        stats = self._conveyor_stats[conveyor_id]
        state = self._conveyor_state[conveyor_id]

        if operation == "enqueue":
            return self._enqueue_items(
                conveyor, stats, conveyor_id, item, items,
                max_queue_size, backpressure, start_time
            )
        elif operation == "process":
            processor = params.get("processor", "default")
            return self._process_batch(
                conveyor, stats, conveyor_id, batch_size, processor, start_time
            )
        elif operation == "drain":
            return self._drain_conveyor(conveyor, stats, conveyor_id, start_time)
        elif operation == "status":
            return self._get_conveyor_status(
                conveyor, stats, state, conveyor_id, start_time
            )
        elif operation == "pause":
            self._conveyor_state[conveyor_id] = ConveyorState.PAUSED
            return ActionResult(success=True, message=f"Conveyor '{conveyor_id}' paused", duration=time.time() - start_time)
        elif operation == "resume":
            self._conveyor_state[conveyor_id] = ConveyorState.RUNNING
            return ActionResult(success=True, message=f"Conveyor '{conveyor_id}' resumed", duration=time.time() - start_time)
        elif operation == "stop":
            self._conveyor_state[conveyor_id] = ConveyorState.STOPPED
            return ActionResult(success=True, message=f"Conveyor '{conveyor_id}' stopped", duration=time.time() - start_time)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}",
                duration=time.time() - start_time
            )

    def _enqueue_items(
        self,
        conveyor: Deque[ConveyorItem],
        stats: ConveyorStats,
        conveyor_id: str,
        item: Optional[Dict],
        items: List[Dict],
        max_queue_size: int,
        backpressure: str,
        start_time: float
    ) -> ActionResult:
        """Enqueue items onto the conveyor."""
        to_enqueue = []
        if item:
            to_enqueue.append(item)
        to_enqueue.extend(items)

        enqueued_count = 0
        dropped_count = 0

        for item_data in to_enqueue:
            if len(conveyor) >= max_queue_size:
                if backpressure == "drop_oldest":
                    conveyor.popleft()
                    dropped_count += 1
                elif backpressure == "drop_newest":
                    dropped_count += 1
                    continue
                elif backpressure == "error":
                    return ActionResult(
                        success=False,
                        message=f"Queue full, cannot enqueue more items",
                        data={"queue_size": len(conveyor), "max_size": max_queue_size, "dropped": dropped_count},
                        duration=time.time() - start_time
                    )
                elif backpressure == "block":
                    # In real impl, would wait. Here simulate by dropping
                    dropped_count += 1
                    continue

            ci = ConveyorItem(
                item_id=item_data.get("id", f"item_{stats.total_enqueued}"),
                data=item_data,
                priority=item_data.get("priority", 0),
            )
            conveyor.append(ci)
            enqueued_count += 1
            stats.total_enqueued += 1

        stats.current_queue_size = len(conveyor)

        return ActionResult(
            success=True,
            message=f"Enqueued {enqueued_count} items onto '{conveyor_id}'",
            data={
                "conveyor_id": conveyor_id,
                "enqueued": enqueued_count,
                "dropped": dropped_count,
                "queue_size": len(conveyor),
                "max_queue_size": max_queue_size,
            },
            duration=time.time() - start_time
        )

    def _process_batch(
        self,
        conveyor: Deque[ConveyorItem],
        stats: ConveyorStats,
        conveyor_id: str,
        batch_size: int,
        processor: str,
        start_time: float
    ) -> ActionResult:
        """Process a batch of items from the conveyor."""
        if not conveyor:
            return ActionResult(
                success=True,
                message=f"Conveyor '{conveyor_id}' is empty",
                data={"processed": 0, "queue_size": 0},
                duration=time.time() - start_time
            )

        batch = []
        for _ in range(min(batch_size, len(conveyor))):
            if conveyor:
                batch.append(conveyor.popleft())

        results = []
        total_time = 0.0
        for ci in batch:
            item_start = time.time()
            try:
                # Simulate processing - in real impl, call registered processor
                result = self._simulate_process(ci.data)
                ci.result = result
                ci.processed_at = time.time()
                total_time += (ci.processed_at - item_start)
                stats.total_processed += 1
                results.append({"item_id": ci.item_id, "success": True, "result": result})
            except Exception as e:
                ci.error = str(e)
                ci.processed_at = time.time()
                total_time += (ci.processed_at - item_start)
                stats.total_failed += 1
                results.append({"item_id": ci.item_id, "success": False, "error": str(e)})

        if results:
            stats.avg_process_time = total_time / len(results)
        stats.current_queue_size = len(conveyor)

        return ActionResult(
            success=True,
            message=f"Processed {len(results)} items from '{conveyor_id}'",
            data={
                "conveyor_id": conveyor_id,
                "processed": len(results),
                "queue_size": len(conveyor),
                "avg_process_time_ms": stats.avg_process_time * 1000,
                "results": results,
            },
            duration=time.time() - start_time
        )

    def _drain_conveyor(
        self,
        conveyor: Deque[ConveyorItem],
        stats: ConveyorStats,
        conveyor_id: str,
        start_time: float
    ) -> ActionResult:
        """Drain all items from the conveyor."""
        drained = []
        while conveyor:
            ci = conveyor.popleft()
            drained.append({
                "item_id": ci.item_id,
                "data": ci.data,
                "enqueued_at": ci.enqueued_at,
                "priority": ci.priority,
            })

        stats.current_queue_size = 0

        return ActionResult(
            success=True,
            message=f"Drained {len(drained)} items from '{conveyor_id}'",
            data={
                "conveyor_id": conveyor_id,
                "drained_count": len(drained),
                "items": drained,
            },
            duration=time.time() - start_time
        )

    def _get_conveyor_status(
        self,
        conveyor: Deque[ConveyorItem],
        stats: ConveyorStats,
        state: ConveyorState,
        conveyor_id: str,
        start_time: float
    ) -> ActionResult:
        """Get conveyor status."""
        pending = [{"item_id": ci.item_id, "priority": ci.priority, "enqueued_at": ci.enqueued_at} for ci in conveyor]
        return ActionResult(
            success=True,
            message=f"Conveyor '{conveyor_id}' status",
            data={
                "conveyor_id": conveyor_id,
                "state": state.value,
                "queue_size": len(conveyor),
                "stats": {
                    "total_enqueued": stats.total_enqueued,
                    "total_processed": stats.total_processed,
                    "total_failed": stats.total_failed,
                    "total_dropped": stats.total_dropped,
                    "avg_process_time_ms": stats.avg_process_time * 1000,
                },
                "pending_items": pending[:20],  # Limit to first 20
            },
            duration=time.time() - start_time
        )

    def _simulate_process(self, data: Any) -> Any:
        """Simulate item processing. Replace with actual processor."""
        import random
        time.sleep(0.001)  # Simulate tiny processing time
        return {"processed": True, "value": data}
