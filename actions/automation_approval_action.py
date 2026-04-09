"""
Automation Approval Action Module.

Provides human-in-the-loop approval workflows for automation
with configurable thresholds and notification support.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class ApprovalStatus(Enum):
    """Approval request status."""

    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


class ApprovalPriority(Enum):
    """Approval priority levels."""

    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class ApprovalRequest:
    """Represents an approval request."""

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    task_name: str = ""
    description: str = ""
    priority: ApprovalPriority = ApprovalPriority.NORMAL
    status: ApprovalStatus = ApprovalStatus.PENDING
    requested_at: float = field(default_factory=time.time)
    decided_at: float = 0.0
    requested_by: str = ""
    decided_by: str = ""
    approver_comment: str = ""
    context: dict[str, Any] = field(default_factory=dict)
    expires_in_seconds: float = 3600.0


class AutomationApprovalAction:
    """
    Manages approval workflows for automation tasks.

    Features:
    - Configurable approval thresholds
    - Priority-based queueing
    - Auto-expiration of stale requests
    - Notification callbacks

    Example:
        approval = AutomationApprovalAction()
        request = await approval.request_approval(
            task_name="Delete user",
            description="Confirm user deletion",
            priority=ApprovalPriority.HIGH,
        )
        result = await approval.wait_for_decision(request.id)
    """

    def __init__(
        self,
        default_timeout_seconds: float = 3600.0,
        enable_notifications: bool = True,
    ) -> None:
        """
        Initialize approval action.

        Args:
            default_timeout_seconds: Default approval timeout.
            enable_notifications: Enable notification callbacks.
        """
        self.default_timeout_seconds = default_timeout_seconds
        self.enable_notifications = enable_notifications
        self._pending: dict[str, ApprovalRequest] = {}
        self._history: list[ApprovalRequest] = []
        self._callbacks: dict[str, Callable] = {}
        self._lock = asyncio.Lock()

    async def request_approval(
        self,
        task_name: str,
        description: str = "",
        priority: ApprovalPriority = ApprovalPriority.NORMAL,
        requested_by: str = "",
        context: Optional[dict[str, Any]] = None,
        approvers: Optional[list[str]] = None,
        expires_in: Optional[float] = None,
    ) -> ApprovalRequest:
        """
        Create a new approval request.

        Args:
            task_name: Name of the task requiring approval.
            description: Detailed description.
            priority: Request priority.
            requested_by: User requesting approval.
            context: Additional context data.
            approvers: List of approver identifiers.
            expires_in: Timeout in seconds.

        Returns:
            Created ApprovalRequest.
        """
        request = ApprovalRequest(
            task_name=task_name,
            description=description,
            priority=priority,
            requested_by=requested_by,
            context=context or {},
            expires_in_seconds=expires_in or self.default_timeout_seconds,
        )

        async with self._lock:
            self._pending[request.id] = request

        logger.info(
            f"Approval requested: {request.id} - {task_name} "
            f"(priority={priority.value})"
        )

        if self.enable_notifications:
            await self._notify_approvers(request, approvers or [])

        return request

    async def approve(
        self,
        request_id: str,
        decided_by: str = "",
        comment: str = "",
    ) -> bool:
        """
        Approve a pending request.

        Args:
            request_id: Request ID to approve.
            decided_by: Approver identifier.
            comment: Optional approver comment.

        Returns:
            True if approval was successful.
        """
        async with self._lock:
            if request_id not in self._pending:
                return False

            request = self._pending[request_id]
            request.status = ApprovalStatus.APPROVED
            request.decided_at = time.time()
            request.decided_by = decided_by
            request.approver_comment = comment

            del self._pending[request_id]
            self._history.append(request)

        logger.info(f"Approval granted: {request_id} by {decided_by}")
        await self._trigger_callback(request_id, "approved")

        return True

    async def reject(
        self,
        request_id: str,
        decided_by: str = "",
        comment: str = "",
    ) -> bool:
        """
        Reject a pending request.

        Args:
            request_id: Request ID to reject.
            decided_by: Rejector identifier.
            comment: Rejection reason.

        Returns:
            True if rejection was successful.
        """
        async with self._lock:
            if request_id not in self._pending:
                return False

            request = self._pending[request_id]
            request.status = ApprovalStatus.REJECTED
            request.decided_at = time.time()
            request.decided_by = decided_by
            request.approver_comment = comment

            del self._pending[request_id]
            self._history.append(request)

        logger.info(f"Approval rejected: {request_id} by {decided_by}")
        await self._trigger_callback(request_id, "rejected")

        return True

    async def cancel(self, request_id: str) -> bool:
        """
        Cancel a pending request.

        Args:
            request_id: Request ID to cancel.

        Returns:
            True if cancellation was successful.
        """
        async with self._lock:
            if request_id not in self._pending:
                return False

            request = self._pending[request_id]
            request.status = ApprovalStatus.CANCELLED
            request.decided_at = time.time()

            del self._pending[request_id]
            self._history.append(request)

        logger.info(f"Approval cancelled: {request_id}")
        return True

    async def wait_for_decision(
        self,
        request_id: str,
        timeout_seconds: Optional[float] = None,
    ) -> ApprovalStatus:
        """
        Wait for an approval decision.

        Args:
            request_id: Request ID to wait for.
            timeout_seconds: Maximum wait time.

        Returns:
            Final approval status.
        """
        timeout = timeout_seconds or self.default_timeout_seconds
        start = time.time()

        while time.time() - start < timeout:
            async with self._lock:
                if request_id not in self._pending:
                    for req in self._history:
                        if req.id == request_id:
                            return req.status

            if self._check_expired(request_id):
                return ApprovalStatus.EXPIRED

            await asyncio.sleep(0.5)

        return ApprovalStatus.EXPIRED

    async def get_pending(self) -> list[ApprovalRequest]:
        """
        Get all pending approval requests.

        Returns:
            List of pending requests sorted by priority.
        """
        async with self._lock:
            requests = list(self._pending.values())

        priority_order = {
            ApprovalPriority.CRITICAL: 0,
            ApprovalPriority.HIGH: 1,
            ApprovalPriority.NORMAL: 2,
            ApprovalPriority.LOW: 3,
        }

        return sorted(requests, key=lambda r: priority_order.get(r.priority, 99))

    async def process_expired(self) -> int:
        """
        Process and expire stale requests.

        Returns:
            Number of requests expired.
        """
        expired_count = 0

        async with self._lock:
            to_expire = [
                rid for rid, req in self._pending.items()
                if self._check_expired(rid)
            ]

            for request_id in to_expire:
                request = self._pending[request_id]
                request.status = ApprovalStatus.EXPIRED
                request.decided_at = time.time()
                del self._pending[request_id]
                self._history.append(request)
                expired_count += 1

        if expired_count > 0:
            logger.info(f"Expired {expired_count} approval requests")

        return expired_count

    def register_callback(
        self,
        event: str,
        callback: Callable[[ApprovalRequest], None],
    ) -> None:
        """
        Register a callback for approval events.

        Args:
            event: Event name ('approved', 'rejected', 'expired').
            callback: Callback function.
        """
        if event not in self._callbacks:
            self._callbacks[event] = []
        self._callbacks[event].append(callback)

    def _check_expired(self, request_id: str) -> bool:
        """Check if a request has expired."""
        if request_id not in self._pending:
            return False
        request = self._pending[request_id]
        return time.time() - request.requested_at > request.expires_in_seconds

    async def _notify_approvers(
        self,
        request: ApprovalRequest,
        approvers: list[str],
    ) -> None:
        """Notify approvers of new request."""
        logger.debug(f"Notifying approvers for {request.id}: {approvers}")

    async def _trigger_callback(self, request_id: str, event: str) -> None:
        """Trigger registered callbacks."""
        if event not in self._callbacks:
            return

        async with self._lock:
            request = next((r for r in self._history if r.id == request_id), None)

        if request:
            for callback in self._callbacks[event]:
                try:
                    callback(request)
                except Exception as e:
                    logger.error(f"Callback error: {e}")

    def get_stats(self) -> dict[str, Any]:
        """
        Get approval statistics.

        Returns:
            Statistics dictionary.
        """
        approved = sum(1 for r in self._history if r.status == ApprovalStatus.APPROVED)
        rejected = sum(1 for r in self._history if r.status == ApprovalStatus.REJECTED)
        expired = sum(1 for r in self._history if r.status == ApprovalStatus.EXPIRED)

        return {
            "pending": len(self._pending),
            "history_total": len(self._history),
            "approved": approved,
            "rejected": rejected,
            "expired": expired,
        }
