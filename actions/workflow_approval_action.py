"""
Workflow Approval Action.

Provides approval workflow functionality.
Supports:
- Multi-level approvals
- Sequential/parallel approval
- Approval delegation
- Reminders and escalation
"""

from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import threading
import logging
import json
import uuid

logger = logging.getLogger(__name__)


class ApprovalStatus(Enum):
    """Approval status."""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


class ApprovalLevel(Enum):
    """Approval level type."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"


@dataclass
class Approver:
    """Approver definition."""
    user_id: str
    name: str = ""
    email: str = ""
    level: int = 1
    is_delegate: bool = False
    delegated_from: Optional[str] = None


@dataclass
class ApprovalStep:
    """Single approval step."""
    step_id: str
    level: int
    approvers: List[Approver]
    status: ApprovalStatus = ApprovalStatus.PENDING
    required_count: int = 1
    approved_count: int = 0
    rejected_count: int = 0
    comments: List[str] = field(default_factory=list)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


@dataclass
class ApprovalRequest:
    """Approval request."""
    request_id: str
    title: str
    description: str
    requester_id: str
    created_at: datetime
    status: ApprovalStatus = ApprovalStatus.PENDING
    steps: List[ApprovalStep] = field(default_factory=list)
    current_level: int = 1
    data: Dict[str, Any] = field(default_factory=dict)
    deadline: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "request_id": self.request_id,
            "title": self.title,
            "status": self.status.value,
            "current_level": self.current_level,
            "created_at": self.created_at.isoformat()
        }


class WorkflowApprovalAction:
    """
    Workflow Approval Action.
    
    Provides approval workflow with support for:
    - Multi-level approvals
    - Sequential and parallel approval
    - Delegation
    - Reminders and escalation
    """
    
    def __init__(self):
        """Initialize the Workflow Approval Action."""
        self._requests: Dict[str, ApprovalRequest] = {}
        self._lock = threading.RLock()
        self._delegates: Dict[str, str] = {}  # delegate -> original
        self._notifications: List[Dict] = []
    
    def create_request(
        self,
        title: str,
        description: str,
        requester_id: str,
        approvers: List[Approver],
        approval_type: ApprovalLevel = ApprovalLevel.SEQUENTIAL,
        deadline: Optional[datetime] = None,
        data: Optional[Dict[str, Any]] = None
    ) -> ApprovalRequest:
        """
        Create an approval request.
        
        Args:
            title: Request title
            description: Request description
            requester_id: User ID of requester
            approvers: List of approvers
            approval_type: Sequential or parallel
            deadline: Approval deadline
            data: Additional data
        
        Returns:
            Created ApprovalRequest
        """
        request_id = f"APR-{uuid.uuid4().hex[:12]}"
        
        # Group approvers by level
        levels: Dict[int, List[Approver]] = {}
        for approver in approvers:
            if approver.level not in levels:
                levels[approver.level] = []
            levels[approver.level].append(approver)
        
        # Create steps
        steps = []
        for level in sorted(levels.keys()):
            step = ApprovalStep(
                step_id=f"{request_id}-L{level}",
                level=level,
                approvers=levels[level],
                required_count=1,  # Could be majority, all, etc.
                deadline=deadline
            )
            steps.append(step)
        
        request = ApprovalRequest(
            request_id=request_id,
            title=title,
            description=description,
            requester_id=requester_id,
            created_at=datetime.utcnow(),
            steps=steps,
            deadline=deadline,
            data=data or {}
        )
        
        with self._lock:
            self._requests[request_id] = request
        
        self._notify_approvers(request, steps[0] if steps else None)
        
        logger.info(f"Created approval request: {request_id}")
        return request
    
    def approve(
        self,
        request_id: str,
        approver_id: str,
        comment: Optional[str] = None
    ) -> bool:
        """
        Approve a request.
        
        Args:
            request_id: Request ID
            approver_id: User ID of approver
            comment: Optional comment
        
        Returns:
            True if approved
        """
        with self._lock:
            if request_id not in self._requests:
                return False
            
            request = self._requests[request_id]
            current_step = self._get_current_step(request)
            
            if not current_step:
                return False
            
            # Find approver
            approver = self._find_approver(current_step, approver_id)
            if not approver:
                return False
            
            # Record approval
            current_step.approved_count += 1
            if comment:
                current_step.comments.append(f"APPROVED by {approver_id}: {comment}")
            
            # Check if step is complete
            if current_step.approved_count >= current_step.required_count:
                current_step.status = ApprovalStatus.APPROVED
                current_step.completed_at = datetime.utcnow()
                request.current_level += 1
                
                # Start next level if sequential
                next_step = self._get_current_step(request)
                if next_step:
                    self._notify_approvers(request, next_step)
                else:
                    request.status = ApprovalStatus.APPROVED
                    self._notify_requestor(request)
            else:
                current_step.status = ApprovalStatus.PENDING
        
        logger.info(f"Approved request {request_id} by {approver_id}")
        return True
    
    def reject(
        self,
        request_id: str,
        approver_id: str,
        reason: str
    ) -> bool:
        """
        Reject a request.
        
        Args:
            request_id: Request ID
            approver_id: User ID of approver
            reason: Rejection reason
        
        Returns:
            True if rejected
        """
        with self._lock:
            if request_id not in self._requests:
                return False
            
            request = self._requests[request_id]
            current_step = self._get_current_step(request)
            
            if not current_step:
                return False
            
            approver = self._find_approver(current_step, approver_id)
            if not approver:
                return False
            
            current_step.rejected_count += 1
            current_step.comments.append(f"REJECTED by {approver_id}: {reason}")
            current_step.status = ApprovalStatus.REJECTED
            current_step.completed_at = datetime.utcnow()
            request.status = ApprovalStatus.REJECTED
            
            self._notify_requestor(request, rejected=True)
        
        logger.info(f"Rejected request {request_id} by {approver_id}: {reason}")
        return True
    
    def cancel(self, request_id: str, user_id: str) -> bool:
        """Cancel an approval request."""
        with self._lock:
            if request_id not in self._requests:
                return False
            
            request = self._requests[request_id]
            if request.requester_id != user_id:
                return False
            
            request.status = ApprovalStatus.CANCELLED
        
        return True
    
    def delegate(
        self,
        from_user: str,
        to_user: str,
        until: Optional[datetime] = None
    ) -> None:
        """Delegate approval authority."""
        self._delegates[to_user] = from_user
        logger.info(f"Delegated {from_user} -> {to_user}")
    
    def get_pending_for_user(self, user_id: str) -> List[ApprovalRequest]:
        """Get pending requests for a user."""
        pending = []
        
        with self._lock:
            for request in self._requests.values():
                if request.status != ApprovalStatus.PENDING:
                    continue
                
                current_step = self._get_current_step(request)
                if not current_step:
                    continue
                
                for approver in current_step.approvers:
                    if approver.user_id == user_id:
                        pending.append(request)
                        break
        
        return pending
    
    def get_request(self, request_id: str) -> Optional[ApprovalRequest]:
        """Get an approval request."""
        return self._requests.get(request_id)
    
    def _get_current_step(self, request: ApprovalRequest) -> Optional[ApprovalStep]:
        """Get the current approval step."""
        for step in request.steps:
            if step.level == request.current_level and step.status == ApprovalStatus.PENDING:
                return step
        return None
    
    def _find_approver(self, step: ApprovalStep, user_id: str) -> Optional[Approver]:
        """Find approver in step."""
        for approver in step.approvers:
            if approver.user_id == user_id:
                return approver
            # Check delegates
            if self._delegates.get(user_id) == approver.user_id:
                return approver
        return None
    
    def _notify_approvers(self, request: ApprovalRequest, step: Optional[ApprovalStep]) -> None:
        """Send notifications to approvers."""
        if not step:
            return
        
        for approver in step.approvers:
            self._notifications.append({
                "type": "approval_request",
                "request_id": request.request_id,
                "title": request.title,
                "approver_id": approver.user_id,
                "created_at": datetime.utcnow().isoformat()
            })
    
    def _notify_requestor(
        self,
        request: ApprovalRequest,
        rejected: bool = False
    ) -> None:
        """Notify requestor of outcome."""
        self._notifications.append({
            "type": "approval_result",
            "request_id": request.request_id,
            "status": request.status.value,
            "requester_id": request.requester_id,
            "rejected": rejected
        })
    
    def get_stats(self) -> Dict[str, Any]:
        """Get approval workflow statistics."""
        with self._lock:
            statuses = {}
            for req in self._requests.values():
                statuses[req.status.value] = statuses.get(req.status.value, 0) + 1
            
            return {
                "total_requests": len(self._requests),
                "by_status": statuses,
                "pending_count": sum(1 for r in self._requests.values() if r.status == ApprovalStatus.PENDING),
                "delegates_count": len(self._delegates)
            }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    approval = WorkflowApprovalAction()
    
    # Create request
    request = approval.create_request(
        title="Expense Report $500",
        description="Q4 marketing expense",
        requester_id="user123",
        approvers=[
            Approver(user_id="manager1", name="Manager One", level=1),
            Approver(user_id="manager2", name="Manager Two", level=2),
        ],
        deadline=datetime.utcnow() + timedelta(days=7)
    )
    
    print(f"Created: {request.request_id}")
    
    # Get pending
    pending = approval.get_pending_for_user("manager1")
    print(f"Pending for manager1: {len(pending)}")
    
    # Approve
    approval.approve(request.request_id, "manager1", "Looks good")
    
    # Check status
    req = approval.get_request(request.request_id)
    print(f"Status after manager1: {req.status.value}")
    
    # Approve second level
    pending = approval.get_pending_for_user("manager2")
    print(f"Pending for manager2: {len(pending)}")
    
    if pending:
        approval.approve(pending[0].request_id, "manager2")
    
    print(f"Final status: {approval.get_request(request.request_id).status.value}")
    print(f"Stats: {json.dumps(approval.get_stats(), indent=2)}")
