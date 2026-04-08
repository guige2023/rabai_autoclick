"""
Approval Workflow Action Module.

Provides human-in-the-loop approval workflows with multi-level approval chains,
timeout handling, delegation, and full audit trail.

Author: RabAi Team
"""

from __future__ import annotations

import json
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set


class ApprovalStatus(Enum):
    """Approval request status."""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    DELEGATED = "delegated"


class ApprovalLevel(Enum):
    """Multi-level approval levels."""
    L1 = "l1"
    L2 = "l2"
    L3 = "l3"
    FINAL = "final"


@dataclass
class ApprovalStep:
    """Single step in an approval chain."""
    level: ApprovalLevel
    approvers: List[str]
    status: ApprovalStatus = ApprovalStatus.PENDING
    decided_at: Optional[datetime] = None
    decided_by: Optional[str] = None
    comment: Optional[str] = None
    delegated_to: Optional[str] = None


@dataclass
class ApprovalRequest:
    """An approval request with full context."""
    id: str
    title: str
    description: str
    requester: str
    steps: List[ApprovalStep] = field(default_factory=list)
    current_step: int = 0
    status: ApprovalStatus = ApprovalStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    expires_at: Optional[datetime] = None
    priority: int = 3
    metadata: Dict[str, Any] = field(default_factory=dict)
    audit_log: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "requester": self.requester,
            "steps": [
                {
                    "level": s.level.value,
                    "approvers": s.approvers,
                    "status": s.status.value,
                    "decided_at": s.decided_at.isoformat() if s.decided_at else None,
                    "decided_by": s.decided_by,
                    "comment": s.comment,
                }
                for s in self.steps
            ],
            "current_step": self.current_step,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "priority": self.priority,
            "metadata": self.metadata,
            "audit_log": self.audit_log,
        }

    def add_audit_entry(self, action: str, actor: str, details: str) -> None:
        """Add entry to audit log."""
        self.audit_log.append({
            "action": action,
            "actor": actor,
            "details": details,
            "timestamp": datetime.now().isoformat(),
        })
        self.updated_at = datetime.now()

    @property
    def is_complete(self) -> bool:
        return self.status in (
            ApprovalStatus.APPROVED,
            ApprovalStatus.REJECTED,
            ApprovalStatus.EXPIRED,
            ApprovalStatus.CANCELLED,
        )


class ApprovalNotifier:
    """Interface for sending approval notifications."""

    def send_pending_notification(self, request: ApprovalRequest, approver: str) -> None:
        """Send notification to approver about pending request."""
        pass

    def send_decision_notification(
        self,
        request: ApprovalRequest,
        recipient: str,
        decision: ApprovalStatus,
    ) -> None:
        """Send notification about approval decision."""
        pass


class ApprovalWorkflow:
    """
    Human-in-the-loop approval workflow engine.

    Supports multi-level approval chains, delegation, timeout handling,
    and full audit trails for compliance-critical workflows.

    Example:
        >>> workflow = ApprovalWorkflow()
        >>> workflow.add_approver_role("manager", ["alice@example.com"])
        >>> workflow.add_approver_role("director", ["bob@example.com"])
        >>> req_id = workflow.create_request("Deploy to production", "alice", ["manager", "director"])
        >>> workflow.approve(req_id, "alice@example.com", "LGTM")
        >>> workflow.get_status(req_id)
    """

    def __init__(
        self,
        notifier: Optional[ApprovalNotifier] = None,
        storage: Optional[Callable] = None,
    ):
        self.notifier = notifier or ApprovalNotifier()
        self.storage = storage
        self._approver_roles: Dict[str, List[str]] = defaultdict(list)
        self._requests: Dict[str, ApprovalRequest] = {}
        self._user_tasks: Dict[str, List[str]] = defaultdict(list)
        self._pending_notifications: List[Dict] = []

    def add_approver_role(self, role: str, approvers: List[str]) -> None:
        """Define an approver role with one or more people."""
        self._approver_roles[role] = approvers

    def create_request(
        self,
        title: str,
        requester: str,
        approval_chain: List[str],
        description: str = "",
        priority: int = 3,
        timeout_hours: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Create a new approval request."""
        request_id = str(uuid.uuid4())

        steps = []
        for idx, role in enumerate(approval_chain):
            level_name = f"L{idx + 1}"
            if idx == len(approval_chain) - 1:
                level_name = "FINAL"
            level = ApprovalLevel(level_name)
            approvers = self._approver_roles.get(role, [])
            steps.append(ApprovalStep(level=level, approvers=approvers))

        expires_at = None
        if timeout_hours:
            expires_at = datetime.now() + timedelta(hours=timeout_hours)

        request = ApprovalRequest(
            id=request_id,
            title=title,
            description=description,
            requester=requester,
            steps=steps,
            status=ApprovalStatus.PENDING,
            expires_at=expires_at,
            priority=priority,
            metadata=metadata or {},
        )

        self._requests[request_id] = request
        request.add_audit_entry("created", requester, f"Request created for chain: {approval_chain}")

        # Notify first step approvers
        if steps:
            self._notify_step_approvers(request, steps[0])

        return request_id

    def approve(
        self,
        request_id: str,
        approver: str,
        comment: Optional[str] = None,
    ) -> bool:
        """Approve the current step of a request."""
        request = self._requests.get(request_id)
        if not request:
            return False
        if request.is_complete:
            return False

        current_step = request.steps[request.current_step]
        if approver not in current_step.approvers:
            return False

        current_step.status = ApprovalStatus.APPROVED
        current_step.decided_at = datetime.now()
        current_step.decided_by = approver
        current_step.comment = comment
        request.status = ApprovalStatus.PENDING
        request.add_audit_entry("approved", approver, comment or "Approved")

        # Advance to next step or complete
        if request.current_step < len(request.steps) - 1:
            request.current_step += 1
            self._notify_step_approvers(request, request.steps[request.current_step])
        else:
            request.status = ApprovalStatus.APPROVED
            request.add_audit_entry("completed", approver, "All steps approved - request completed")
            self._notify_requester(request, ApprovalStatus.APPROVED)

        self._save(request)
        return True

    def reject(
        self,
        request_id: str,
        approver: str,
        reason: str,
    ) -> bool:
        """Reject the current step of a request."""
        request = self._requests.get(request_id)
        if not request:
            return False
        if request.is_complete:
            return False

        current_step = request.steps[request.current_step]
        if approver not in current_step.approvers:
            return False

        current_step.status = ApprovalStatus.REJECTED
        current_step.decided_at = datetime.now()
        current_step.decided_by = approver
        current_step.comment = reason
        request.status = ApprovalStatus.REJECTED
        request.add_audit_entry("rejected", approver, reason)

        self._notify_requester(request, ApprovalStatus.REJECTED)
        self._save(request)
        return True

    def delegate(
        self,
        request_id: str,
        approver: str,
        delegate_to: str,
        reason: Optional[str] = None,
    ) -> bool:
        """Delegate approval authority to another user."""
        request = self._requests.get(request_id)
        if not request:
            return False
        if request.is_complete:
            return False

        current_step = request.steps[request.current_step]
        if approver not in current_step.approvers:
            return False

        if delegate_to not in current_step.approvers:
            current_step.approvers.append(delegate_to)

        current_step.delegated_to = delegate_to
        current_step.status = ApprovalStatus.DELEGATED
        request.status = ApprovalStatus.PENDING
        request.add_audit_entry(
            "delegated",
            approver,
            f"Delegated to {delegate_to}: {reason or 'No reason provided'}",
        )

        self._save(request)
        return True

    def cancel(self, request_id: str, canceller: str, reason: str) -> bool:
        """Cancel an approval request."""
        request = self._requests.get(request_id)
        if not request:
            return False
        if request.is_complete:
            return False

        request.status = ApprovalStatus.CANCELLED
        request.add_audit_entry("cancelled", canceller, reason)
        self._save(request)
        return True

    def get_pending_for_approver(self, approver: str) -> List[ApprovalRequest]:
        """Get all pending requests for an approver."""
        pending = []
        for request in self._requests.values():
            if request.is_complete:
                continue
            if request.status == ApprovalStatus.PENDING:
                current_step = request.steps[request.current_step]
                if approver in current_step.approvers:
                    pending.append(request)
        return pending

    def get_status(self, request_id: str) -> Optional[Dict[str, Any]]:
        """Get current status of an approval request."""
        request = self._requests.get(request_id)
        if not request:
            return None
        return request.to_dict()

    def check_timeouts(self) -> List[str]:
        """Check for expired requests and mark them as expired."""
        expired_ids = []
        now = datetime.now()
        for request in self._requests.values():
            if request.is_complete:
                continue
            if request.expires_at and now >= request.expires_at:
                request.status = ApprovalStatus.EXPIRED
                request.add_audit_entry("expired", "system", f"Request expired at {request.expires_at}")
                expired_ids.append(request.id)
                self._notify_requester(request, ApprovalStatus.EXPIRED)
                self._save(request)
        return expired_ids

    def _notify_step_approvers(self, request: ApprovalRequest, step: ApprovalStep) -> None:
        """Send notifications to approvers of current step."""
        for approver in step.approvers:
            self._user_tasks[approver].append(request.id)
            try:
                self.notifier.send_pending_notification(request, approver)
            except Exception:
                pass

    def _notify_requester(
        self,
        request: ApprovalRequest,
        decision: ApprovalStatus,
    ) -> None:
        """Notify requester of final decision."""
        try:
            self.notifier.send_decision_notification(request, request.requester, decision)
        except Exception:
            pass

    def _save(self, request: ApprovalRequest) -> None:
        """Persist request state."""
        if self.storage:
            self.storage(request.id, request.to_dict())

    def get_statistics(self) -> Dict[str, Any]:
        """Get approval workflow statistics."""
        total = len(self._requests)
        pending = sum(1 for r in self._requests.values() if r.status == ApprovalStatus.PENDING)
        approved = sum(1 for r in self._requests.values() if r.status == ApprovalStatus.APPROVED)
        rejected = sum(1 for r in self._requests.values() if r.status == ApprovalStatus.REJECTED)
        expired = sum(1 for r in self._requests.values() if r.status == ApprovalStatus.EXPIRED)

        return {
            "total_requests": total,
            "pending": pending,
            "approved": approved,
            "rejected": rejected,
            "expired": expired,
            "approval_rate": approved / total if total > 0 else 0.0,
        }


def create_approval_workflow(
    approvers: Optional[Dict[str, List[str]]] = None,
) -> ApprovalWorkflow:
    """Factory to create a configured approval workflow."""
    workflow = ApprovalWorkflow()
    if approvers:
        for role, users in approvers.items():
            workflow.add_approver_role(role, users)
    return workflow
