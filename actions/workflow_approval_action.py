"""Workflow Approval Action Module.

Handle approval workflows with multi-level approval chains.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable

from .command_handler_action import CommandStatus


class ApprovalStatus(Enum):
    """Approval status."""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    SKIPPED = "skipped"
    EXPIRED = "expired"


class ApprovalLevel(Enum):
    """Approval level type."""
    REQUIRED = "required"
    OPTIONAL = "optional"
    CONDITIONAL = "conditional"


@dataclass
class ApprovalStep:
    """Single approval step in a workflow."""
    step_id: str
    name: str
    approver_role: str
    level: ApprovalLevel = ApprovalLevel.REQUIRED
    timeout_hours: float = 72.0
    auto_approve: bool = False
    auto_skip: bool = False
    condition: Callable[[dict], bool] | None = None


@dataclass
class ApprovalRequest:
    """Approval request."""
    request_id: str
    workflow_id: str
    steps: list[ApprovalStep]
    current_step_index: int = 0
    status: ApprovalStatus = ApprovalStatus.PENDING
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    decided_at: datetime | None = None
    approver_comments: dict[str, str] = field(default_factory=dict)
    context: dict[str, Any] = field(default_factory=dict)


@dataclass
class ApprovalResult:
    """Result of approval decision."""
    request_id: str
    decision: ApprovalStatus
    approver_id: str
    comments: str | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class ApprovalWorkflow:
    """Approval workflow manager."""

    def __init__(self, workflow_id: str, name: str) -> None:
        self.workflow_id = workflow_id
        self.name = name
        self._requests: dict[str, ApprovalRequest] = {}
        self._handlers: dict[str, Callable] = {}
        self._lock = asyncio.Lock()

    def add_approval_step(
        self,
        name: str,
        approver_role: str,
        level: ApprovalLevel = ApprovalLevel.REQUIRED,
        timeout_hours: float = 72.0,
        auto_approve: bool = False,
        condition: Callable[[dict], bool] | None = None
    ) -> str:
        """Add an approval step to the workflow."""
        step_id = str(uuid.uuid4())
        step = ApprovalStep(
            step_id=step_id,
            name=name,
            approver_role=approver_role,
            level=level,
            timeout_hours=timeout_hours,
            auto_approve=auto_approve,
            condition=condition
        )
        return step_id

    async def submit(
        self,
        context: dict[str, Any],
        steps: list[ApprovalStep]
    ) -> str:
        """Submit a new approval request."""
        request_id = str(uuid.uuid4())
        request = ApprovalRequest(
            request_id=request_id,
            workflow_id=self.workflow_id,
            steps=steps,
            context=context
        )
        async with self._lock:
            self._requests[request_id] = request
        return request_id

    async def approve(
        self,
        request_id: str,
        approver_id: str,
        comments: str | None = None
    ) -> ApprovalResult:
        """Approve the current step."""
        async with self._lock:
            request = self._requests.get(request_id)
            if not request:
                raise ValueError(f"Unknown request: {request_id}")
            if request.status != ApprovalStatus.PENDING:
                raise ValueError(f"Request already decided: {request.status}")
        result = ApprovalResult(
            request_id=request_id,
            decision=ApprovalStatus.APPROVED,
            approver_id=approver_id,
            comments=comments
        )
        request.approver_comments[approver_id] = comments or ""
        request.current_step_index += 1
        if request.current_step_index >= len(request.steps):
            request.status = ApprovalStatus.APPROVED
            request.decided_at = datetime.now(timezone.utc)
        return result

    async def reject(
        self,
        request_id: str,
        approver_id: str,
        comments: str | None = None
    ) -> ApprovalResult:
        """Reject the current step."""
        async with self._lock:
            request = self._requests.get(request_id)
            if not request:
                raise ValueError(f"Unknown request: {request_id}")
        result = ApprovalResult(
            request_id=request_id,
            decision=ApprovalStatus.REJECTED,
            approver_id=approver_id,
            comments=comments
        )
        request.approver_comments[approver_id] = comments or ""
        request.status = ApprovalStatus.REJECTED
        request.decided_at = datetime.now(timezone.utc)
        return result

    async def get_request(self, request_id: str) -> ApprovalRequest | None:
        """Get approval request by ID."""
        return self._requests.get(request_id)

    async def get_pending_requests(self, approver_role: str | None = None) -> list[ApprovalRequest]:
        """Get all pending requests."""
        return [
            r for r in self._requests.values()
            if r.status == ApprovalStatus.PENDING
            and (not approver_role or r.steps[r.current_step_index].approver_role == approver_role)
        ]

    async def expire_old_requests(self) -> int:
        """Expire requests that have timed out. Returns count expired."""
        expired = 0
        now = datetime.now(timezone.utc)
        async with self._lock:
            for request in self._requests.values():
                if request.status != ApprovalStatus.PENDING:
                    continue
                current_step = request.steps[request.current_step_index]
                age_hours = (now - request.created_at).total_seconds() / 3600
                if age_hours > current_step.timeout_hours:
                    if current_step.level == ApprovalLevel.REQUIRED:
                        request.status = ApprovalStatus.EXPIRED
                        request.decided_at = now
                        expired += 1
                    else:
                        request.current_step_index += 1
                        if request.current_step_index >= len(request.steps):
                            request.status = ApprovalStatus.EXPIRED
                            request.decided_at = now
                            expired += 1
        return expired
