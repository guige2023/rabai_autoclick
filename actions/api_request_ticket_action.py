"""API Request Ticket Action Module.

Provides a distributed ticket/queue system for API requests
with priority handling, dead letter queue, and retry management.
"""

import logging
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class TicketStatus(Enum):
    """Ticket processing status."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


class RetryPolicy(Enum):
    """Retry policies."""
    NONE = "none"
    FIXED = "fixed"
    EXPONENTIAL = "exponential"
    FIBONACCI = "fibonacci"


@dataclass
class Ticket:
    """An API request ticket."""
    ticket_id: str
    payload: Any
    priority: int = 0  # Lower = higher priority
    status: TicketStatus = TicketStatus.PENDING
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    retry_count: int = 0
    max_retries: int = 3
    retry_delay_sec: float = 1.0
    retry_policy: RetryPolicy = RetryPolicy.EXPONENTIAL
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class APIRequestTicketAction(BaseAction):
    """Distributed ticket queue action.

    Manages API requests as tickets with priority queuing,
    retry handling, and dead letter queue.

    Args:
        context: Execution context.
        params: Dict with keys:
            - operation: Operation (create, poll, complete, fail, cancel, retry, get_status)
            - ticket_id: Ticket identifier
            - payload: Request payload
            - priority: Ticket priority (0=highest)
            - dataset_id: Queue identifier
    """
    action_type = "api_request_ticket"
    display_name = "API请求票据"
    description = "分布式请求票据队列管理"

    def get_required_params(self) -> List[str]:
        return ["operation"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "ticket_id": None,
            "payload": None,
            "priority": 5,
            "max_retries": 3,
            "retry_delay_sec": 1.0,
            "retry_policy": "exponential",
            "metadata": {},
            "dataset_id": "default",
        }

    def __init__(self) -> None:
        super().__init__()
        self._queues: Dict[str, Dict] = {}

    def _get_or_create_queue(self, dataset_id: str) -> Dict:
        """Get or create a ticket queue."""
        if dataset_id not in self._queues:
            self._queues[dataset_id] = {
                "tickets": {},  # ticket_id -> Ticket
                "pending": [],  # List of pending ticket_ids (sorted by priority)
                "processing": set(),  # Currently processing ticket_ids
                "completed": {},  # ticket_id -> Ticket
                "dead_letter": {},  # Failed tickets
                "stats": {
                    "created": 0,
                    "completed": 0,
                    "failed": 0,
                    "cancelled": 0,
                }
            }
        return self._queues[dataset_id]

    def _parse_retry_policy(self, policy_str: str) -> RetryPolicy:
        """Parse retry policy string."""
        try:
            return RetryPolicy(policy_str.lower())
        except ValueError:
            return RetryPolicy.EXPONENTIAL

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute ticket operation."""
        start_time = time.time()

        operation = params.get("operation", "status")
        ticket_id = params.get("ticket_id")
        payload = params.get("payload")
        priority = params.get("priority", 5)
        max_retries = params.get("max_retries", 3)
        retry_delay_sec = params.get("retry_delay_sec", 1.0)
        retry_policy_str = params.get("retry_policy", "exponential")
        metadata = params.get("metadata", {})
        dataset_id = params.get("dataset_id", "default")

        queue = self._get_or_create_queue(dataset_id)
        retry_policy = self._parse_retry_policy(retry_policy_str)

        if operation == "create":
            return self._create_ticket(
                queue, payload, priority, max_retries, retry_delay_sec,
                retry_policy, metadata, dataset_id, start_time
            )
        elif operation == "poll":
            return self._poll_ticket(queue, dataset_id, start_time)
        elif operation == "complete":
            return self._complete_ticket(queue, ticket_id, dataset_id, start_time)
        elif operation == "fail":
            return self._fail_ticket(queue, ticket_id, params.get("error"), params.get("should_retry"), dataset_id, start_time)
        elif operation == "cancel":
            return self._cancel_ticket(queue, ticket_id, dataset_id, start_time)
        elif operation == "retry":
            return self._retry_ticket(queue, ticket_id, dataset_id, start_time)
        elif operation == "get_status":
            return self._get_ticket_status(queue, ticket_id, dataset_id, start_time)
        elif operation == "get_queue_status":
            return self._get_queue_status(queue, dataset_id, start_time)
        elif operation == "purge_completed":
            return self._purge_completed(queue, dataset_id, start_time)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}",
                duration=time.time() - start_time
            )

    def _create_ticket(
        self,
        queue: Dict,
        payload: Any,
        priority: int,
        max_retries: int,
        retry_delay_sec: float,
        retry_policy: RetryPolicy,
        metadata: Dict,
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Create a new ticket."""
        ticket_id = ticket_id or f"ticket_{uuid.uuid4().hex[:12]}"
        ticket = Ticket(
            ticket_id=ticket_id,
            payload=payload,
            priority=priority,
            max_retries=max_retries,
            retry_delay_sec=retry_delay_sec,
            retry_policy=retry_policy,
            metadata=metadata,
        )

        queue["tickets"][ticket_id] = ticket
        # Insert sorted by priority
        inserted = False
        for i, tid in enumerate(queue["pending"]):
            if priority < queue["tickets"][tid].priority:
                queue["pending"].insert(i, ticket_id)
                inserted = True
                break
        if not inserted:
            queue["pending"].append(ticket_id)

        queue["stats"]["created"] += 1

        return ActionResult(
            success=True,
            message=f"Ticket '{ticket_id}' created with priority {priority}",
            data={
                "ticket_id": ticket_id,
                "priority": priority,
                "queue_position": queue["pending"].index(ticket_id) + 1,
                "total_pending": len(queue["pending"]),
            },
            duration=time.time() - start_time
        )

    def _poll_ticket(self, queue: Dict, dataset_id: str, start_time: float) -> ActionResult:
        """Poll the next ticket for processing."""
        if not queue["pending"]:
            return ActionResult(
                success=True,
                message=f"Queue '{dataset_id}' is empty",
                data={"dataset_id": dataset_id, "ticket": None, "pending_count": 0},
                duration=time.time() - start_time
            )

        ticket_id = queue["pending"].pop(0)
        ticket = queue["tickets"][ticket_id]
        ticket.status = TicketStatus.PROCESSING
        ticket.started_at = time.time()
        queue["processing"].add(ticket_id)

        return ActionResult(
            success=True,
            message=f"Polled ticket '{ticket_id}'",
            data={
                "ticket_id": ticket_id,
                "priority": ticket.priority,
                "payload": ticket.payload,
                "metadata": ticket.metadata,
                "processing_count": len(queue["processing"]),
                "pending_count": len(queue["pending"]),
            },
            duration=time.time() - start_time
        )

    def _complete_ticket(self, queue: Dict, ticket_id: Optional[str], dataset_id: str, start_time: float) -> ActionResult:
        """Mark a ticket as completed."""
        if not ticket_id or ticket_id not in queue["tickets"]:
            return ActionResult(success=False, message=f"Ticket '{ticket_id}' not found", duration=time.time() - start_time)

        ticket = queue["tickets"][ticket_id]
        ticket.status = TicketStatus.COMPLETED
        ticket.completed_at = time.time()
        queue["processing"].discard(ticket_id)
        queue["completed"][ticket_id] = ticket
        queue["stats"]["completed"] += 1

        processing_time = (ticket.completed_at - ticket.started_at) if ticket.started_at else 0

        return ActionResult(
            success=True,
            message=f"Ticket '{ticket_id}' completed",
            data={
                "ticket_id": ticket_id,
                "processing_time_sec": processing_time,
                "retry_count": ticket.retry_count,
                "completed_count": queue["stats"]["completed"],
            },
            duration=time.time() - start_time
        )

    def _fail_ticket(
        self,
        queue: Dict,
        ticket_id: Optional[str],
        error: Optional[str],
        should_retry: bool,
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Mark a ticket as failed."""
        if not ticket_id or ticket_id not in queue["tickets"]:
            return ActionResult(success=False, message=f"Ticket '{ticket_id}' not found", duration=time.time() - start_time)

        ticket = queue["tickets"][ticket_id]
        ticket.error_message = error or "Unknown error"
        queue["processing"].discard(ticket_id)

        if should_retry and ticket.retry_count < ticket.max_retries:
            # Requeue for retry
            ticket.retry_count += 1
            # Calculate delay based on retry policy
            delay = self._calculate_retry_delay(ticket)
            time.sleep(min(delay, 0.01))  # In real impl, would reschedule
            ticket.status = TicketStatus.PENDING
            # Reinsert by priority
            for i, tid in enumerate(queue["pending"]):
                if ticket.priority < queue["tickets"][tid].priority:
                    queue["pending"].insert(i, ticket_id)
                    break
            else:
                queue["pending"].append(ticket_id)

            return ActionResult(
                success=True,
                message=f"Ticket '{ticket_id}' scheduled for retry ({ticket.retry_count}/{ticket.max_retries})",
                data={
                    "ticket_id": ticket_id,
                    "retry_count": ticket.retry_count,
                    "will_retry": True,
                },
                duration=time.time() - start_time
            )
        else:
            # Move to dead letter queue
            ticket.status = TicketStatus.FAILED
            queue["dead_letter"][ticket_id] = ticket
            queue["stats"]["failed"] += 1

            return ActionResult(
                success=True,
                message=f"Ticket '{ticket_id}' moved to dead letter queue",
                data={
                    "ticket_id": ticket_id,
                    "will_retry": False,
                    "total_failures": queue["stats"]["failed"],
                    "error": ticket.error_message,
                },
                duration=time.time() - start_time
            )

    def _calculate_retry_delay(self, ticket: Ticket) -> float:
        """Calculate retry delay based on policy."""
        if ticket.retry_policy == RetryPolicy.FIXED:
            return ticket.retry_delay_sec
        elif ticket.retry_policy == RetryPolicy.EXPONENTIAL:
            return ticket.retry_delay_sec * (2 ** (ticket.retry_count - 1))
        elif ticket.retry_policy == RetryPolicy.FIBONACCI:
            # Fibonacci: 1, 1, 2, 3, 5, 8...
            a, b = 1, 1
            for _ in range(ticket.retry_count - 1):
                a, b = b, a + b
            return ticket.retry_delay_sec * a
        return ticket.retry_delay_sec

    def _cancel_ticket(self, queue: Dict, ticket_id: Optional[str], dataset_id: str, start_time: float) -> ActionResult:
        """Cancel a ticket."""
        if not ticket_id or ticket_id not in queue["tickets"]:
            return ActionResult(success=False, message=f"Ticket '{ticket_id}' not found", duration=time.time() - start_time)

        ticket = queue["tickets"][ticket_id]
        ticket.status = TicketStatus.CANCELLED
        ticket.completed_at = time.time()
        queue["processing"].discard(ticket_id)
        if ticket_id in queue["pending"]:
            queue["pending"].remove(ticket_id)
        queue["stats"]["cancelled"] += 1

        return ActionResult(
            success=True,
            message=f"Ticket '{ticket_id}' cancelled",
            data={"ticket_id": ticket_id, "cancelled_count": queue["stats"]["cancelled"]},
            duration=time.time() - start_time
        )

    def _retry_ticket(self, queue: Dict, ticket_id: Optional[str], dataset_id: str, start_time: float) -> ActionResult:
        """Manually retry a failed or dead letter ticket."""
        if not ticket_id or ticket_id not in queue["tickets"]:
            return ActionResult(success=False, message=f"Ticket '{ticket_id}' not found", duration=time.time() - start_time)

        ticket = queue["tickets"][ticket_id]
        if ticket_id in queue["dead_letter"]:
            del queue["dead_letter"][ticket_id]
        ticket.status = TicketStatus.PENDING
        ticket.retry_count = 0
        # Reinsert by priority
        for i, tid in enumerate(queue["pending"]):
            if ticket.priority < queue["tickets"][tid].priority:
                queue["pending"].insert(i, ticket_id)
                break
        else:
            queue["pending"].append(ticket_id)

        return ActionResult(
            success=True,
            message=f"Ticket '{ticket_id}' requeued for retry",
            data={"ticket_id": ticket_id},
            duration=time.time() - start_time
        )

    def _get_ticket_status(self, queue: Dict, ticket_id: Optional[str], dataset_id: str, start_time: float) -> ActionResult:
        """Get status of a specific ticket."""
        if not ticket_id or ticket_id not in queue["tickets"]:
            return ActionResult(success=False, message=f"Ticket '{ticket_id}' not found", duration=time.time() - start_time)

        ticket = queue["tickets"][ticket_id]
        return ActionResult(
            success=True,
            message=f"Ticket '{ticket_id}' status: {ticket.status.value}",
            data={
                "ticket_id": ticket_id,
                "status": ticket.status.value,
                "priority": ticket.priority,
                "retry_count": ticket.retry_count,
                "created_at": ticket.created_at,
                "started_at": ticket.started_at,
                "completed_at": ticket.completed_at,
                "error_message": ticket.error_message,
            },
            duration=time.time() - start_time
        )

    def _get_queue_status(self, queue: Dict, dataset_id: str, start_time: float) -> ActionResult:
        """Get overall queue status."""
        return ActionResult(
            success=True,
            message=f"Queue '{dataset_id}' status",
            data={
                "dataset_id": dataset_id,
                "pending": len(queue["pending"]),
                "processing": len(queue["processing"]),
                "completed": len(queue["completed"]),
                "dead_letter": len(queue["dead_letter"]),
                "total_tickets": len(queue["tickets"]),
                "stats": queue["stats"],
            },
            duration=time.time() - start_time
        )

    def _purge_completed(self, queue: Dict, dataset_id: str, start_time: float) -> ActionResult:
        """Purge completed tickets."""
        count = len(queue["completed"])
        queue["completed"].clear()
        return ActionResult(
            success=True,
            message=f"Purged {count} completed tickets from '{dataset_id}'",
            data={"purged": count},
            duration=time.time() - start_time
        )
