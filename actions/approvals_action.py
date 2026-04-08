"""Approvals action module for RabAI AutoClick.

Provides approval workflow management with multi-step
approval chains, delegation, and rejection handling.
"""

import sys
import os
import json
import time
import uuid
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApprovalStatus(Enum):
    """Status of an approval request."""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


class ApprovalStepStatus(Enum):
    """Status of an approval step."""
    WAITING = "waiting"
    APPROVED = "approved"
    REJECTED = "rejected"
    SKIPPED = "skipped"


@dataclass
class ApprovalStep:
    """Represents a step in an approval chain."""
    step_id: str
    name: str
    approver: str  # User ID or role
    status: ApprovalStepStatus = ApprovalStepStatus.WAITING
    decided_at: Optional[float] = None
    comment: Optional[str] = None
    delegated_to: Optional[str] = None


@dataclass
class ApprovalRequest:
    """Represents an approval request."""
    request_id: str
    title: str
    requester: str
    resource_type: str
    resource_id: str
    status: ApprovalStatus
    steps: List[ApprovalStep]
    current_step: int = 0
    created_at: float
    expires_at: float
    completed_at: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class ApprovalWorkflow:
    """Approval workflow management."""
    
    def __init__(self):
        self._requests: Dict[str, ApprovalRequest] = {}
        self._user_approvals: Dict[str, List[str]] = {}  # user -> [request_ids]
    
    def create_request(
        self,
        title: str,
        requester: str,
        resource_type: str,
        resource_id: str,
        approvers: List[Dict],  # [{"approver": "user1", "name": "Manager"}]
        ttl_seconds: float = 604800.0  # 7 days
    ) -> str:
        """Create an approval request."""
        request_id = str(uuid.uuid4())
        
        steps = []
        for i, approver_data in enumerate(approvers):
            step = ApprovalStep(
                step_id=str(uuid.uuid4()),
                name=approver_data.get("name", f"Step {i+1}"),
                approver=approver_data["approver"]
            )
            if i == 0:
                step.status = ApprovalStepStatus.WAITING
            else:
                step.status = ApprovalStepStatus.SKIPPED
            steps.append(step)
        
        request = ApprovalRequest(
            request_id=request_id,
            title=title,
            requester=requester,
            resource_type=resource_type,
            resource_id=resource_id,
            status=ApprovalStatus.PENDING,
            steps=steps,
            created_at=time.time(),
            expires_at=time.time() + ttl_seconds
        )
        
        self._requests[request_id] = request
        # Track for approvers
        for step in steps:
            self._user_approvals.setdefault(step.approver, []).append(request_id)
        
        return request_id
    
    def get_pending_for_user(self, user: str) -> List[ApprovalRequest]:
        """Get pending approvals for a user."""
        request_ids = self._user_approvals.get(user, [])
        pending = []
        
        for rid in request_ids:
            req = self._requests.get(rid)
            if not req or req.status != ApprovalStatus.PENDING:
                continue
            
            # Check if this is the current step for this user
            if req.current_step < len(req.steps):
                current_step = req.steps[req.current_step]
                if current_step.approver == user:
                    pending.append(req)
        
        return pending
    
    def approve(
        self,
        request_id: str,
        approver: str,
        comment: Optional[str] = None
    ) -> tuple[bool, str]:
        """Approve the current step."""
        request = self._requests.get(request_id)
        if not request:
            return False, "Request not found"
        
        if request.status != ApprovalStatus.PENDING:
            return False, f"Request is {request.status.value}"
        
        if request.current_step >= len(request.steps):
            return False, "No more steps"
        
        current_step = request.steps[request.current_step]
        if current_step.approver != approver:
            return False, "Not authorized to approve this step"
        
        current_step.status = ApprovalStepStatus.APPROVED
        current_step.decided_at = time.time()
        current_step.comment = comment
        
        # Move to next step
        request.current_step += 1
        
        if request.current_step >= len(request.steps):
            request.status = ApprovalStatus.APPROVED
            request.completed_at = time.time()
        else:
            # Activate next step
            request.steps[request.current_step].status = ApprovalStepStatus.WAITING
        
        return True, "Approved"
    
    def reject(
        self,
        request_id: str,
        approver: str,
        comment: Optional[str] = None
    ) -> tuple[bool, str]:
        """Reject the current step."""
        request = self._requests.get(request_id)
        if not request:
            return False, "Request not found"
        
        if request.status != ApprovalStatus.PENDING:
            return False, f"Request is {request.status.value}"
        
        current_step = request.steps[request.current_step]
        if current_step.approver != approver:
            return False, "Not authorized to reject this step"
        
        current_step.status = ApprovalStepStatus.REJECTED
        current_step.decided_at = time.time()
        current_step.comment = comment
        
        request.status = ApprovalStatus.REJECTED
        request.completed_at = time.time()
        
        return True, "Rejected"
    
    def delegate(
        self,
        request_id: str,
        from_approver: str,
        to_approver: str
    ) -> tuple[bool, str]:
        """Delegate current step to another approver."""
        request = self._requests.get(request_id)
        if not request:
            return False, "Request not found"
        
        if request.status != ApprovalStatus.PENDING:
            return False, "Request not pending"
        
        current_step = request.steps[request.current_step]
        if current_step.approver != from_approver:
            return False, "Not authorized"
        
        # Update delegations
        if from_approver in self._user_approvals:
            self._user_approvals[from_approver] = [
                rid for rid in self._user_approvals[from_approver]
                if rid != request_id
            ]
        
        current_step.approver = to_approver
        current_step.delegated_to = to_approver
        self._user_approvals.setdefault(to_approver, []).append(request_id)
        
        return True, "Delegated"
    
    def cancel(self, request_id: str, requester: str) -> bool:
        """Cancel an approval request."""
        request = self._requests.get(request_id)
        if not request or request.requester != requester:
            return False
        if request.status != ApprovalStatus.PENDING:
            return False
        
        request.status = ApprovalStatus.CANCELLED
        request.completed_at = time.time()
        return True
    
    def get_request(self, request_id: str) -> Optional[ApprovalRequest]:
        """Get an approval request."""
        return self._requests.get(request_id)
    
    def list_requests(
        self,
        requester: Optional[str] = None,
        status: Optional[ApprovalStatus] = None
    ) -> List[ApprovalRequest]:
        """List approval requests."""
        requests = list(self._requests.values())
        
        if requester:
            requests = [r for r in requests if r.requester == requester]
        if status:
            requests = [r for r in requests if r.status == status]
        
        return sorted(requests, key=lambda r: r.created_at, reverse=True)


class ApprovalWorkflowAction(BaseAction):
    """Approval workflow with multi-step approval chains.
    
    Supports sequential approvals, delegation, rejection,
    and expiration handling.
    """
    action_type = "approval_workflow"
    display_name = "审批工作流"
    description = "多级审批工作流，支持委托和拒绝"
    
    def __init__(self):
        super().__init__()
        self._workflow = ApprovalWorkflow()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute approval operation."""
        operation = params.get("operation", "")
        
        try:
            if operation == "create":
                return self._create(params)
            elif operation == "get_pending":
                return self._get_pending(params)
            elif operation == "approve":
                return self._approve(params)
            elif operation == "reject":
                return self._reject(params)
            elif operation == "delegate":
                return self._delegate(params)
            elif operation == "cancel":
                return self._cancel(params)
            elif operation == "get":
                return self._get(params)
            elif operation == "list":
                return self._list(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _create(self, params: Dict[str, Any]) -> ActionResult:
        """Create approval request."""
        request_id = self._workflow.create_request(
            title=params.get("title", ""),
            requester=params.get("requester", ""),
            resource_type=params.get("resource_type", ""),
            resource_id=params.get("resource_id", ""),
            approvers=params.get("approvers", []),
            ttl_seconds=params.get("ttl_seconds", 604800.0)
        )
        return ActionResult(success=True, message=f"Created: {request_id}",
                         data={"request_id": request_id})
    
    def _get_pending(self, params: Dict[str, Any]) -> ActionResult:
        """Get pending for user."""
        user = params.get("approver", "")
        pending = self._workflow.get_pending_for_user(user)
        return ActionResult(success=True, message=f"{len(pending)} pending",
                         data={"requests": [{"id": r.request_id, "title": r.title} for r in pending]})
    
    def _approve(self, params: Dict[str, Any]) -> ActionResult:
        """Approve a request."""
        request_id = params.get("request_id", "")
        approver = params.get("approver", "")
        success, msg = self._workflow.approve(request_id, approver, params.get("comment"))
        return ActionResult(success=success, message=msg)
    
    def _reject(self, params: Dict[str, Any]) -> ActionResult:
        """Reject a request."""
        request_id = params.get("request_id", "")
        approver = params.get("approver", "")
        success, msg = self._workflow.reject(request_id, approver, params.get("comment"))
        return ActionResult(success=success, message=msg)
    
    def _delegate(self, params: Dict[str, Any]) -> ActionResult:
        """Delegate to another approver."""
        request_id = params.get("request_id", "")
        from_approver = params.get("from_approver", "")
        to_approver = params.get("to_approver", "")
        success, msg = self._workflow.delegate(request_id, from_approver, to_approver)
        return ActionResult(success=success, message=msg)
    
    def _cancel(self, params: Dict[str, Any]) -> ActionResult:
        """Cancel a request."""
        request_id = params.get("request_id", "")
        requester = params.get("requester", "")
        cancelled = self._workflow.cancel(request_id, requester)
        return ActionResult(success=cancelled, message="Cancelled" if cancelled else "Cannot cancel")
    
    def _get(self, params: Dict[str, Any]) -> ActionResult:
        """Get a request."""
        request_id = params.get("request_id", "")
        request = self._workflow.get_request(request_id)
        if not request:
            return ActionResult(success=False, message="Not found")
        return ActionResult(success=True, message="Retrieved",
                         data={"request_id": request.request_id, "status": request.status.value})
    
    def _list(self, params: Dict[str, Any]) -> ActionResult:
        """List requests."""
        requests = self._workflow.list_requests(
            params.get("requester"),
            ApprovalStatus(params.get("status")) if params.get("status") else None
        )
        return ActionResult(success=True, message=f"{len(requests)} requests",
                         data={"requests": [{"id": r.request_id, "title": r.title,
                                           "status": r.status.value} for r in requests]})
