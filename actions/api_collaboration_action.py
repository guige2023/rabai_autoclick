"""API Collaboration Action Module.

Manages API design collaboration, reviews, comments,
approvals, and team workflows for API development.
"""

from __future__ import annotations

import sys
import os
import time
import json
import hashlib
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ReviewStatus(Enum):
    """Status of an API review."""
    DRAFT = "draft"
    PENDING_REVIEW = "pending_review"
    IN_REVIEW = "in_review"
    CHANGES_REQUESTED = "changes_requested"
    APPROVED = "approved"
    REJECTED = "rejected"


class CommentType(Enum):
    """Type of review comment."""
    GENERAL = "general"
    SUGGESTION = "suggestion"
    ISSUE = "issue"
    QUESTION = "question"
    PRAISE = "praise"


class ApprovalAction(Enum):
    """Approval action types."""
    APPROVE = "approve"
    REQUEST_CHANGES = "request_changes"
    REJECT = "reject"


@dataclass
class TeamMember:
    """Represents a team member."""
    member_id: str
    name: str
    email: str
    role: str = "developer"
    avatar_url: Optional[str] = None


@dataclass
class Comment:
    """A review comment on an API specification."""
    comment_id: str
    author: str
    content: str
    comment_type: CommentType
    created_at: float
    updated_at: Optional[float] = None
    parent_id: Optional[str] = None
    resolved: bool = False
    resolved_by: Optional[str] = None
    resolved_at: Optional[float] = None
    line_range: Optional[Dict[str, int]] = None
    reactions: Dict[str, List[str]] = field(default_factory=dict)


@dataclass
class Review:
    """An API design review."""
    review_id: str
    api_id: str
    title: str
    status: ReviewStatus
    author: str
    created_at: float
    updated_at: float
    reviewers: List[str] = field(default_factory=list)
    approvers: List[str] = field(default_factory=list)
    comments: List[Comment] = field(default_factory=list)
    versions: List[str] = field(default_factory=list)
    current_version: str = "1.0.0"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ApprovalDecision:
    """Records an approval decision."""
    review_id: str
    approver: str
    action: ApprovalAction
    timestamp: float
    comment: str = ""
    version: str = ""


class APICollaborationAction(BaseAction):
    """
    Manages API design collaboration and review workflows.

    Handles API reviews, comments, approvals, and team
    coordination for API development processes.

    Example:
        collab = APICollaborationAction()
        result = collab.execute(ctx, {
            "action": "create_review",
            "api_id": "my-api",
            "title": "API Design Review"
        })
    """
    action_type = "api_collaboration"
    display_name = "API协作管理"
    description = "管理API设计协作、评审、评论、审批和团队工作流"

    def __init__(self) -> None:
        super().__init__()
        self._reviews: Dict[str, Review] = {}
        self._members: Dict[str, TeamMember] = {}
        self._approval_history: List[ApprovalDecision] = []
        self._notifications: List[Dict[str, Any]] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute a collaboration action.

        Args:
            context: Execution context.
            params: Dict with keys: action, review_id, api_id, etc.

        Returns:
            ActionResult with operation result.
        """
        action = params.get("action", "")

        try:
            if action == "create_review":
                return self._create_review(params)
            elif action == "submit_for_review":
                return self._submit_for_review(params)
            elif action == "add_comment":
                return self._add_comment(params)
            elif action == "resolve_comment":
                return self._resolve_comment(params)
            elif action == "approve":
                return self._approve_review(params)
            elif action == "request_changes":
                return self._request_changes(params)
            elif action == "get_review":
                return self._get_review(params)
            elif action == "list_reviews":
                return self._list_reviews(params)
            elif action == "get_comments":
                return self._get_comments(params)
            elif action == "add_reviewer":
                return self._add_reviewer(params)
            elif action == "add_member":
                return self._add_member(params)
            elif action == "get_notifications":
                return self._get_notifications(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Collaboration error: {str(e)}")

    def _create_review(self, params: Dict[str, Any]) -> ActionResult:
        """Create a new API review."""
        api_id = params.get("api_id", "")
        title = params.get("title", "")
        author = params.get("author", "system")
        description = params.get("description", "")

        if not api_id or not title:
            return ActionResult(success=False, message="api_id and title are required")

        review_id = self._generate_review_id(api_id, title)
        now = time.time()

        review = Review(
            review_id=review_id,
            api_id=api_id,
            title=title,
            status=ReviewStatus.DRAFT,
            author=author,
            created_at=now,
            updated_at=now,
            metadata={"description": description},
        )

        self._reviews[review_id] = review
        self._send_notification(
            reviewer=author,
            notification_type="review_created",
            review_id=review_id,
            message=f"Review '{title}' created for API {api_id}"
        )

        return ActionResult(
            success=True,
            message=f"Review created: {review_id}",
            data={"review_id": review_id, "status": ReviewStatus.DRAFT.value}
        )

    def _submit_for_review(self, params: Dict[str, Any]) -> ActionResult:
        """Submit a review for review."""
        review_id = params.get("review_id", "")

        if review_id not in self._reviews:
            return ActionResult(success=False, message=f"Review not found: {review_id}")

        review = self._reviews[review_id]

        if review.status != ReviewStatus.DRAFT and review.status != ReviewStatus.CHANGES_REQUESTED:
            return ActionResult(
                success=False,
                message=f"Cannot submit review in status: {review.status.value}"
            )

        review.status = ReviewStatus.PENDING_REVIEW
        review.updated_at = time.time()

        for reviewer in review.reviewers:
            self._send_notification(
                reviewer=reviewer,
                notification_type="review_submitted",
                review_id=review_id,
                message=f"Review '{review.title}' submitted for your review"
            )

        return ActionResult(
            success=True,
            message=f"Review submitted: {review_id}",
            data={"review_id": review_id, "status": ReviewStatus.PENDING_REVIEW.value}
        )

    def _add_comment(self, params: Dict[str, Any]) -> ActionResult:
        """Add a comment to a review."""
        review_id = params.get("review_id", "")
        author = params.get("author", "anonymous")
        content = params.get("content", "")
        comment_type_str = params.get("comment_type", "general")
        parent_id = params.get("parent_id")
        line_range = params.get("line_range")

        if not review_id or not content:
            return ActionResult(success=False, message="review_id and content are required")

        if review_id not in self._reviews:
            return ActionResult(success=False, message=f"Review not found: {review_id}")

        try:
            comment_type = CommentType(comment_type_str)
        except ValueError:
            comment_type = CommentType.GENERAL

        comment_id = self._generate_comment_id()
        now = time.time()

        comment = Comment(
            comment_id=comment_id,
            author=author,
            content=content,
            comment_type=comment_type,
            created_at=now,
            parent_id=parent_id,
            line_range=line_range,
        )

        review = self._reviews[review_id]
        review.comments.append(comment)
        review.updated_at = now

        if review.status == ReviewStatus.PENDING_REVIEW:
            review.status = ReviewStatus.IN_REVIEW

        self._send_notification(
            reviewer=review.author,
            notification_type="new_comment",
            review_id=review_id,
            message=f"New {comment_type.value} comment on '{review.title}'"
        )

        return ActionResult(
            success=True,
            message=f"Comment added: {comment_id}",
            data={"comment_id": comment_id, "review_status": review.status.value}
        )

    def _resolve_comment(self, params: Dict[str, Any]) -> ActionResult:
        """Resolve a comment."""
        review_id = params.get("review_id", "")
        comment_id = params.get("comment_id", "")
        resolved_by = params.get("resolved_by", "system")

        if review_id not in self._reviews:
            return ActionResult(success=False, message=f"Review not found: {review_id}")

        review = self._reviews[review_id]

        comment = next((c for c in review.comments if c.comment_id == comment_id), None)

        if not comment:
            return ActionResult(success=False, message=f"Comment not found: {comment_id}")

        comment.resolved = True
        comment.resolved_by = resolved_by
        comment.resolved_at = time.time()

        return ActionResult(
            success=True,
            message=f"Comment resolved: {comment_id}",
            data={"comment_id": comment_id, "resolved": True}
        )

    def _approve_review(self, params: Dict[str, Any]) -> ActionResult:
        """Approve a review."""
        review_id = params.get("review_id", "")
        approver = params.get("approver", "system")
        comment = params.get("comment", "")
        version = params.get("version", "")

        if review_id not in self._reviews:
            return ActionResult(success=False, message=f"Review not found: {review_id}")

        review = self._reviews[review_id]

        if approver not in review.reviewers and approver not in review.approvers:
            return ActionResult(success=False, message=f"User {approver} is not a reviewer")

        if approver in review.approvers:
            return ActionResult(success=False, message=f"User {approver} has already approved")

        review.approvers.append(approver)
        review.updated_at = time.time()

        decision = ApprovalDecision(
            review_id=review_id,
            approver=approver,
            action=ApprovalAction.APPROVE,
            timestamp=time.time(),
            comment=comment,
            version=version,
        )
        self._approval_history.append(decision)

        if len(review.approvers) >= len(review.reviewers):
            review.status = ReviewStatus.APPROVED

        self._send_notification(
            reviewer=review.author,
            notification_type="review_approved",
            review_id=review_id,
            message=f"Review '{review.title}' approved by {approver}"
        )

        return ActionResult(
            success=True,
            message=f"Review approved by {approver}",
            data={
                "review_id": review_id,
                "approvers": review.approvers,
                "status": review.status.value,
                "required_approvals": len(review.reviewers),
                "current_approvals": len(review.approvers),
            }
        )

    def _request_changes(self, params: Dict[str, Any]) -> ActionResult:
        """Request changes on a review."""
        review_id = params.get("review_id", "")
        reviewer = params.get("reviewer", "system")
        comment = params.get("comment", "")

        if review_id not in self._reviews:
            return ActionResult(success=False, message=f"Review not found: {review_id}")

        review = self._reviews[review_id]

        review.status = ReviewStatus.CHANGES_REQUESTED
        review.updated_at = time.time()

        decision = ApprovalDecision(
            review_id=review_id,
            approver=reviewer,
            action=ApprovalAction.REQUEST_CHANGES,
            timestamp=time.time(),
            comment=comment,
        )
        self._approval_history.append(decision)

        self._send_notification(
            reviewer=review.author,
            notification_type="changes_requested",
            review_id=review_id,
            message=f"Changes requested on '{review.title}' by {reviewer}"
        )

        return ActionResult(
            success=True,
            message=f"Changes requested on review: {review_id}",
            data={"review_id": review_id, "status": ReviewStatus.CHANGES_REQUESTED.value}
        )

    def _get_review(self, params: Dict[str, Any]) -> ActionResult:
        """Get a review by ID."""
        review_id = params.get("review_id", "")

        if review_id not in self._reviews:
            return ActionResult(success=False, message=f"Review not found: {review_id}")

        review = self._reviews[review_id]
        return ActionResult(
            success=True,
            data=self._serialize_review(review)
        )

    def _list_reviews(self, params: Dict[str, Any]) -> ActionResult:
        """List reviews with optional filters."""
        api_id = params.get("api_id")
        status_filter = params.get("status")
        author = params.get("author")

        reviews = list(self._reviews.values())

        if api_id:
            reviews = [r for r in reviews if r.api_id == api_id]

        if status_filter:
            try:
                status = ReviewStatus(status_filter)
                reviews = [r for r in reviews if r.status == status]
            except ValueError:
                pass

        if author:
            reviews = [r for r in reviews if r.author == author]

        serialized = [self._serialize_review(r) for r in reviews]

        return ActionResult(
            success=True,
            data={"reviews": serialized, "count": len(serialized)}
        )

    def _get_comments(self, params: Dict[str, Any]) -> ActionResult:
        """Get comments for a review."""
        review_id = params.get("review_id", "")
        include_resolved = params.get("include_resolved", False)

        if review_id not in self._reviews:
            return ActionResult(success=False, message=f"Review not found: {review_id}")

        review = self._reviews[review_id]
        comments = review.comments

        if not include_resolved:
            comments = [c for c in comments if not c.resolved]

        serialized = [
            {
                "comment_id": c.comment_id,
                "author": c.author,
                "content": c.content,
                "comment_type": c.comment_type.value,
                "created_at": c.created_at,
                "resolved": c.resolved,
                "resolved_by": c.resolved_by,
                "reactions": c.reactions,
            }
            for c in comments
        ]

        return ActionResult(
            success=True,
            data={"comments": serialized, "count": len(serialized)}
        )

    def _add_reviewer(self, params: Dict[str, Any]) -> ActionResult:
        """Add a reviewer to a review."""
        review_id = params.get("review_id", "")
        reviewer_id = params.get("reviewer_id", "")

        if review_id not in self._reviews:
            return ActionResult(success=False, message=f"Review not found: {review_id}")

        if not reviewer_id:
            return ActionResult(success=False, message="reviewer_id is required")

        review = self._reviews[review_id]

        if reviewer_id in review.reviewers:
            return ActionResult(success=False, message=f"User {reviewer_id} is already a reviewer")

        review.reviewers.append(reviewer_id)
        review.updated_at = time.time()

        self._send_notification(
            reviewer=reviewer_id,
            notification_type="reviewer_added",
            review_id=review_id,
            message=f"You've been added as a reviewer on '{review.title}'"
        )

        return ActionResult(
            success=True,
            message=f"Reviewer added: {reviewer_id}",
            data={"review_id": review_id, "reviewers": review.reviewers}
        )

    def _add_member(self, params: Dict[str, Any]) -> ActionResult:
        """Add a team member."""
        member_id = params.get("member_id", "")
        name = params.get("name", "")
        email = params.get("email", "")
        role = params.get("role", "developer")

        if not member_id or not name:
            return ActionResult(success=False, message="member_id and name are required")

        member = TeamMember(
            member_id=member_id,
            name=name,
            email=email,
            role=role,
        )

        self._members[member_id] = member

        return ActionResult(
            success=True,
            message=f"Member added: {member_id}",
            data={"member_id": member_id, "name": name, "role": role}
        )

    def _get_notifications(self, params: Dict[str, Any]) -> ActionResult:
        """Get notifications for a user."""
        reviewer = params.get("reviewer", "")
        limit = params.get("limit", 50)

        if not reviewer:
            return ActionResult(success=False, message="reviewer is required")

        notifications = [
            n for n in self._notifications
            if n.get("reviewer") == reviewer
        ][-limit:]

        return ActionResult(
            success=True,
            data={"notifications": notifications, "count": len(notifications)}
        )

    def _send_notification(
        self,
        reviewer: str,
        notification_type: str,
        review_id: str,
        message: str,
    ) -> None:
        """Send a notification to a user."""
        self._notifications.append({
            "reviewer": reviewer,
            "type": notification_type,
            "review_id": review_id,
            "message": message,
            "timestamp": time.time(),
            "read": False,
        })

    def _serialize_review(self, review: Review) -> Dict[str, Any]:
        """Serialize a review to a dictionary."""
        return {
            "review_id": review.review_id,
            "api_id": review.api_id,
            "title": review.title,
            "status": review.status.value,
            "author": review.author,
            "created_at": review.created_at,
            "updated_at": review.updated_at,
            "reviewers": review.reviewers,
            "approvers": review.approvers,
            "comment_count": len(review.comments),
            "resolved_comments": sum(1 for c in review.comments if c.resolved),
            "current_version": review.current_version,
            "metadata": review.metadata,
        }

    def _generate_review_id(self, api_id: str, title: str) -> str:
        """Generate a unique review ID."""
        raw = f"{api_id}:{title}:{time.time()}"
        return f"rev_{hashlib.sha1(raw.encode()).hexdigest()[:12]}"

    def _generate_comment_id(self) -> str:
        """Generate a unique comment ID."""
        return f"cmt_{hashlib.sha1(str(time.time_ns()).encode()).hexdigest()[:12]}"

    def get_pending_reviews(self, user_id: str) -> List[Review]:
        """Get reviews pending for a user."""
        return [
            r for r in self._reviews.values()
            if user_id in r.reviewers and r.status == ReviewStatus.PENDING_REVIEW
        ]

    def get_review_statistics(self) -> Dict[str, Any]:
        """Get collaboration statistics."""
        total_reviews = len(self._reviews)
        by_status = {}

        for review in self._reviews.values():
            status = review.status.value
            by_status[status] = by_status.get(status, 0) + 1

        total_comments = sum(len(r.comments) for r in self._reviews.values())
        resolved_comments = sum(
            sum(1 for c in r.comments if c.resolved)
            for r in self._reviews.values()
        )

        return {
            "total_reviews": total_reviews,
            "reviews_by_status": by_status,
            "total_comments": total_comments,
            "resolved_comments": resolved_comments,
            "resolution_rate": resolved_comments / total_comments if total_comments > 0 else 0,
        }
