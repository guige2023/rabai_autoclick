"""
Workflow Collaboration System
Team collaboration with user management, permissions, real-time editing,
change tracking, comments, approval workflow, version history, and notifications.
"""

import json
import uuid
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Optional


class Role(str, Enum):
    """User roles in the system."""
    ADMIN = "admin"
    EDITOR = "editor"
    VIEWER = "viewer"


class Permission(str, Enum):
    """Granular workflow permissions."""
    READ = "read"
    WRITE = "write"
    EXECUTE = "execute"
    ADMIN = "admin"


class WorkflowStatus(str, Enum):
    """Workflow lifecycle status."""
    DRAFT = "draft"
    PENDING_REVIEW = "pending_review"
    APPROVED = "approved"
    REJECTED = "rejected"
    DEPLOYED = "deployed"


class User:
    """Represents a team member."""
    def __init__(self, user_id: str, username: str, email: str, role: Role = Role.VIEWER):
        self.user_id = user_id
        self.username = username
        self.email = email
        self.role = role
        self.created_at = datetime.now()
        self.is_active = True

    def to_dict(self) -> dict:
        return {
            "user_id": self.user_id,
            "username": self.username,
            "email": self.email,
            "role": self.role.value,
            "created_at": self.created_at.isoformat(),
            "is_active": self.is_active
        }


class WorkflowPermission:
    """Per-user per-workflow permissions."""
    def __init__(self, user_id: str, workflow_id: str, permissions: list[Permission]):
        self.user_id = user_id
        self.workflow_id = workflow_id
        self.permissions = permissions
        self.granted_at = datetime.now()
        self.granted_by = None

    def has_permission(self, permission: Permission) -> bool:
        return permission.value in self.permissions or Permission.ADMIN.value in self.permissions

    def to_dict(self) -> dict:
        return {
            "user_id": self.user_id,
            "workflow_id": self.workflow_id,
            "permissions": [p.value for p in self.permissions],
            "granted_at": self.granted_at.isoformat(),
            "granted_by": self.granted_by
        }


class WorkflowVersion:
    """Version history entry for a workflow."""
    def __init__(self, version_id: str, workflow_id: str, content: dict, author: str):
        self.version_id = version_id
        self.workflow_id = workflow_id
        self.version_number = 1
        self.content = content
        self.author = author
        self.created_at = datetime.now()
        self.message = ""
        self.diff_from_previous = {}

    def to_dict(self) -> dict:
        return {
            "version_id": self.version_id,
            "workflow_id": self.workflow_id,
            "version_number": self.version_number,
            "content": self.content,
            "author": self.author,
            "created_at": self.created_at.isoformat(),
            "message": self.message,
            "diff_from_previous": self.diff_from_previous
        }


class ChangeRecord:
    """Tracks changes made to workflows."""
    def __init__(self, record_id: str, workflow_id: str, user_id: str, change_type: str, details: dict):
        self.record_id = record_id
        self.workflow_id = workflow_id
        self.user_id = user_id
        self.change_type = change_type
        self.details = details
        self.timestamp = datetime.now()

    def to_dict(self) -> dict:
        return {
            "record_id": self.record_id,
            "workflow_id": self.workflow_id,
            "user_id": self.user_id,
            "change_type": self.change_type,
            "details": self.details,
            "timestamp": self.timestamp.isoformat()
        }


class Comment:
    """Comments on workflows or specific versions."""
    def __init__(self, comment_id: str, workflow_id: str, user_id: str, content: str, version_id: str = None):
        self.comment_id = comment_id
        self.workflow_id = workflow_id
        self.user_id = user_id
        self.content = content
        self.version_id = version_id
        self.created_at = datetime.now()
        self.updated_at = datetime.now()
        self.resolved = False
        self.replies = []

    def to_dict(self) -> dict:
        return {
            "comment_id": self.comment_id,
            "workflow_id": self.workflow_id,
            "user_id": self.user_id,
            "content": self.content,
            "version_id": self.version_id,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "resolved": self.resolved,
            "replies": self.replies
        }


class Review:
    """Review request for workflow approval."""
    def __init__(self, review_id: str, workflow_id: str, requester_id: str, reviewers: list[str]):
        self.review_id = review_id
        self.workflow_id = workflow_id
        self.requester_id = requester_id
        self.reviewers = reviewers
        self.status = "pending"
        self.created_at = datetime.now()
        self.decided_at = None
        self.decision_by = None
        self.decision_comment = None

    def to_dict(self) -> dict:
        return {
            "review_id": self.review_id,
            "workflow_id": self.workflow_id,
            "requester_id": self.requester_id,
            "reviewers": self.reviewers,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "decided_at": self.decided_at.isoformat() if self.decided_at else None,
            "decision_by": self.decision_by,
            "decision_comment": self.decision_comment
        }


class Notification:
    """Notification for team members."""
    def __init__(self, notification_id: str, user_id: str, notification_type: str, title: str, message: str, workflow_id: str = None):
        self.notification_id = notification_id
        self.user_id = user_id
        self.notification_type = notification_type
        self.title = title
        self.message = message
        self.workflow_id = workflow_id
        self.read = False
        self.created_at = datetime.now()

    def to_dict(self) -> dict:
        return {
            "notification_id": self.notification_id,
            "user_id": self.user_id,
            "notification_type": self.notification_type,
            "title": self.title,
            "message": self.message,
            "workflow_id": self.workflow_id,
            "read": self.read,
            "created_at": self.created_at.isoformat()
        }


class ActivityFeedEntry:
    """Activity feed entry."""
    def __init__(self, entry_id: str, user_id: str, workflow_id: str, action: str, details: dict):
        self.entry_id = entry_id
        self.user_id = user_id
        self.workflow_id = workflow_id
        self.action = action
        self.details = details
        self.timestamp = datetime.now()

    def to_dict(self) -> dict:
        return {
            "entry_id": self.entry_id,
            "user_id": self.user_id,
            "workflow_id": self.workflow_id,
            "action": self.action,
            "details": self.details,
            "timestamp": self.timestamp.isoformat()
        }


class LockInfo:
    """Real-time collaboration lock."""
    def __init__(self, workflow_id: str, user_id: str, lock_type: str = "edit"):
        self.workflow_id = workflow_id
        self.user_id = user_id
        self.lock_type = lock_type
        self.locked_at = datetime.now()
        self.expires_at = datetime.now() + timedelta(minutes=15)

    def is_expired(self) -> bool:
        return datetime.now() > self.expires_at

    def to_dict(self) -> dict:
        return {
            "workflow_id": self.workflow_id,
            "user_id": self.user_id,
            "lock_type": self.lock_type,
            "locked_at": self.locked_at.isoformat(),
            "expires_at": self.expires_at.isoformat()
        }


class TeamWorkspace:
    """Shared team workspace settings."""
    def __init__(self, workspace_id: str, name: str, owner_id: str):
        self.workspace_id = workspace_id
        self.name = name
        self.owner_id = owner_id
        self.members = [owner_id]
        self.created_at = datetime.now()
        self.settings = {
            "default_permissions": [Permission.READ.value],
            "require_approval": True,
            "allow_comments": True,
            "max_workflows_per_user": 50
        }
        self.default_workflow_permissions = {
            Role.ADMIN: [p.value for p in Permission],
            Role.EDITOR: [Permission.READ.value, Permission.WRITE.value, Permission.EXECUTE.value],
            Role.VIEWER: [Permission.READ.value]
        }

    def to_dict(self) -> dict:
        return {
            "workspace_id": self.workspace_id,
            "name": self.name,
            "owner_id": self.owner_id,
            "members": self.members,
            "created_at": self.created_at.isoformat(),
            "settings": self.settings,
            "default_permissions": self.default_workflow_permissions
        }


class WorkflowCollaboration:
    """Main collaboration system class."""

    def __init__(self, storage_path: str = None):
        self.storage_path = storage_path
        self.users: dict[str, User] = {}
        self.workflow_permissions: dict[str, dict[str, WorkflowPermission]] = {}
        self.workflow_versions: dict[str, list[WorkflowVersion]] = {}
        self.change_records: list[ChangeRecord] = []
        self.comments: dict[str, list[Comment]] = {}
        self.reviews: dict[str, Review] = {}
        self.notifications: dict[str, list[Notification]] = {}
        self.activity_feed: list[ActivityFeedEntry] = []
        self.locks: dict[str, LockInfo] = {}
        self.workspaces: dict[str, TeamWorkspace] = {}
        self.workflows: dict[str, dict] = {}
        self._next_version_numbers: dict[str, int] = {}

    def _generate_id(self) -> str:
        return str(uuid.uuid4())

    def _log_activity(self, user_id: str, workflow_id: str, action: str, details: dict = None):
        entry = ActivityFeedEntry(self._generate_id(), user_id, workflow_id, action, details or {})
        self.activity_feed.insert(0, entry)
        if len(self.activity_feed) > 1000:
            self.activity_feed = self.activity_feed[:1000]

    def _notify(self, user_ids: list[str], notification_type: str, title: str, message: str, workflow_id: str = None):
        for user_id in user_ids:
            if user_id not in self.notifications:
                self.notifications[user_id] = []
            notification = Notification(self._generate_id(), user_id, notification_type, title, message, workflow_id)
            self.notifications[user_id].append(notification)

    def _track_change(self, workflow_id: str, user_id: str, change_type: str, details: dict):
        record = ChangeRecord(self._generate_id(), workflow_id, user_id, change_type, details)
        self.change_records.append(record)

    def _check_permission(self, user_id: str, workflow_id: str, permission: Permission) -> bool:
        if user_id not in self.users:
            return False
        user = self.users[user_id]
        if user.role == Role.ADMIN:
            return True
        if workflow_id in self.workflow_permissions and user_id in self.workflow_permissions[workflow_id]:
            return self.workflow_permissions[workflow_id][user_id].has_permission(permission)
        return False

    # User Management
    def create_user(self, username: str, email: str, role: Role = Role.VIEWER) -> User:
        user_id = self._generate_id()
        user = User(user_id, username, email, role)
        self.users[user_id] = user
        self.notifications[user_id] = []
        self._log_activity(user_id, "", "user_created", {"username": username, "role": role.value})
        return user

    def get_user(self, user_id: str) -> Optional[User]:
        return self.users.get(user_id)

    def update_user_role(self, admin_id: str, user_id: str, new_role: Role) -> bool:
        if admin_id not in self.users or self.users[admin_id].role != Role.ADMIN:
            return False
        if user_id in self.users:
            old_role = self.users[user_id].role
            self.users[user_id].role = new_role
            self._track_change("", admin_id, "role_change", {"user_id": user_id, "old_role": old_role.value, "new_role": new_role.value})
            self._notify([user_id], "role_change", "Role Updated", f"Your role has been changed to {new_role.value}")
            return True
        return False

    def deactivate_user(self, admin_id: str, user_id: str) -> bool:
        if admin_id not in self.users or self.users[admin_id].role != Role.ADMIN:
            return False
        if user_id in self.users and user_id != admin_id:
            self.users[user_id].is_active = False
            self._track_change("", admin_id, "user_deactivated", {"user_id": user_id})
            self._notify([user_id], "account_deactivated", "Account Deactivated", "Your account has been deactivated")
            return True
        return False

    def list_users(self) -> list[User]:
        return [u for u in self.users.values() if u.is_active]

    # Team Workspace
    def create_workspace(self, name: str, owner_id: str) -> TeamWorkspace:
        workspace_id = self._generate_id()
        workspace = TeamWorkspace(workspace_id, name, owner_id)
        self.workspaces[workspace_id] = workspace
        self._log_activity(owner_id, "", "workspace_created", {"workspace_id": workspace_id, "name": name})
        return workspace

    def get_workspace(self, workspace_id: str) -> Optional[TeamWorkspace]:
        return self.workspaces.get(workspace_id)

    def add_workspace_member(self, workspace_id: str, admin_id: str, user_id: str) -> bool:
        workspace = self.workspaces.get(workspace_id)
        if not workspace or admin_id != workspace.owner_id:
            return False
        if user_id not in workspace.members:
            workspace.members.append(user_id)
            self._notify([user_id], "workspace_added", "Added to Workspace", f"You have been added to {workspace.name}")
            self._log_activity(admin_id, "", "member_added", {"workspace_id": workspace_id, "user_id": user_id})
        return True

    def update_workspace_settings(self, workspace_id: str, user_id: str, settings: dict) -> bool:
        workspace = self.workspaces.get(workspace_id)
        if not workspace:
            return False
        if user_id != workspace.owner_id and self.users.get(user_id).role != Role.ADMIN:
            return False
        workspace.settings.update(settings)
        self._log_activity(user_id, "", "workspace_settings_updated", {"workspace_id": workspace_id})
        return True

    # Workflow Permissions
    def create_workflow(self, workflow_id: str, name: str, content: dict, user_id: str) -> dict:
        self.workflows[workflow_id] = {
            "workflow_id": workflow_id,
            "name": name,
            "content": content,
            "status": WorkflowStatus.DRAFT.value,
            "created_by": user_id,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        self.workflow_permissions[workflow_id] = {}
        self.workflow_versions[workflow_id] = []
        self.comments[workflow_id] = []
        self._next_version_numbers[workflow_id] = 1
        initial_version = WorkflowVersion(self._generate_id(), workflow_id, content, user_id)
        initial_version.version_number = 1
        initial_version.message = "Initial version"
        self.workflow_versions[workflow_id].append(initial_version)
        self._track_change(workflow_id, user_id, "workflow_created", {"name": name})
        self._log_activity(user_id, workflow_id, "workflow_created", {"name": name})
        return self.workflows[workflow_id]

    def get_workflow(self, workflow_id: str) -> Optional[dict]:
        return self.workflows.get(workflow_id)

    def set_workflow_permission(self, workflow_id: str, admin_id: str, user_id: str, permissions: list[Permission]) -> bool:
        if not self._check_permission(admin_id, workflow_id, Permission.ADMIN):
            return False
        if workflow_id not in self.workflow_permissions:
            return False
        perm = WorkflowPermission(user_id, workflow_id, permissions)
        perm.granted_by = admin_id
        self.workflow_permissions[workflow_id][user_id] = perm
        self._track_change(workflow_id, admin_id, "permission_set", {"user_id": user_id, "permissions": [p.value for p in permissions]})
        self._log_activity(admin_id, workflow_id, "permission_changed", {"target_user": user_id, "permissions": [p.value for p in permissions]})
        return True

    def get_user_workflow_permissions(self, workflow_id: str, user_id: str) -> list[str]:
        if workflow_id in self.workflow_permissions and user_id in self.workflow_permissions[workflow_id]:
            return [p.value for p in self.workflow_permissions[workflow_id][user_id].permissions]
        user = self.users.get(user_id)
        if user and workflow_id in self.workspaces:
            workspace = self.workspaces[workflow_id]
            if user_id in workspace.members:
                return workspace.default_workflow_permissions.get(user.role, [])
        return []

    # Real-time Collaboration
    def acquire_lock(self, workflow_id: str, user_id: str, lock_type: str = "edit") -> Optional[LockInfo]:
        if workflow_id in self.locks:
            lock = self.locks[workflow_id]
            if not lock.is_expired() and lock.user_id != user_id:
                return None
        lock = LockInfo(workflow_id, user_id, lock_type)
        self.locks[workflow_id] = lock
        self._log_activity(user_id, workflow_id, "lock_acquired", {"lock_type": lock_type})
        return lock

    def release_lock(self, workflow_id: str, user_id: str) -> bool:
        if workflow_id in self.locks and self.locks[workflow_id].user_id == user_id:
            del self.locks[workflow_id]
            self._log_activity(user_id, workflow_id, "lock_released", {})
            return True
        return False

    def get_lock_info(self, workflow_id: str) -> Optional[LockInfo]:
        lock = self.locks.get(workflow_id)
        if lock and lock.is_expired():
            del self.locks[workflow_id]
            return None
        return lock

    def get_active_collaborators(self, workflow_id: str) -> list[str]:
        collaborators = []
        if workflow_id in self.locks:
            lock = self.locks[workflow_id]
            if not lock.is_expired():
                collaborators.append(lock.user_id)
        return collaborators

    # Change Tracking
    def track_workflow_change(self, workflow_id: str, user_id: str, change_type: str, old_content: dict, new_content: dict, message: str = "") -> WorkflowVersion:
        version_id = self._generate_id()
        version = WorkflowVersion(version_id, workflow_id, new_content, user_id)
        version.version_number = self._next_version_numbers.get(workflow_id, 1)
        self._next_version_numbers[workflow_id] = version.version_number + 1
        version.message = message
        version.diff_from_previous = self._compute_diff(old_content, new_content)
        self.workflow_versions[workflow_id].append(version)
        self._track_change(workflow_id, user_id, change_type, {"message": message})
        self._log_activity(user_id, workflow_id, "workflow_modified", {"version": version.version_number, "message": message})
        workflow = self.workflows.get(workflow_id)
        if workflow:
            workflow["updated_at"] = datetime.now().isoformat()
            workflow["content"] = new_content
        return version

    def _compute_diff(self, old: dict, new: dict) -> dict:
        diff = {"added": {}, "removed": {}, "modified": {}}
        all_keys = set(old.keys()) | set(new.keys())
        for key in all_keys:
            if key not in old:
                diff["added"][key] = new[key]
            elif key not in new:
                diff["removed"][key] = old[key]
            elif old[key] != new[key]:
                diff["modified"][key] = {"old": old[key], "new": new[key]}
        return diff

    def get_change_history(self, workflow_id: str, limit: int = 50) -> list[ChangeRecord]:
        changes = [c for c in self.change_records if c.workflow_id == workflow_id]
        return sorted(changes, key=lambda x: x.timestamp, reverse=True)[:limit]

    # Comments and Reviews
    def add_comment(self, workflow_id: str, user_id: str, content: str, version_id: str = None) -> Comment:
        comment = Comment(self._generate_id(), workflow_id, user_id, content, version_id)
        if workflow_id not in self.comments:
            self.comments[workflow_id] = []
        self.comments[workflow_id].append(comment)
        self._track_change(workflow_id, user_id, "comment_added", {"comment_id": comment.comment_id})
        self._log_activity(user_id, workflow_id, "comment_added", {"comment_id": comment.comment_id})
        reviewers = self._get_workflow_reviewers(workflow_id)
        if reviewers:
            self._notify(reviewers, "new_comment", "New Comment", f"New comment on workflow", workflow_id)
        return comment

    def resolve_comment(self, workflow_id: str, comment_id: str, user_id: str) -> bool:
        if workflow_id in self.comments:
            for comment in self.comments[workflow_id]:
                if comment.comment_id == comment_id:
                    comment.resolved = True
                    comment.updated_at = datetime.now()
                    self._log_activity(user_id, workflow_id, "comment_resolved", {"comment_id": comment_id})
                    return True
        return False

    def get_comments(self, workflow_id: str, include_resolved: bool = False) -> list[Comment]:
        if workflow_id not in self.comments:
            return []
        if include_resolved:
            return self.comments[workflow_id]
        return [c for c in self.comments[workflow_id] if not c.resolved]

    def request_review(self, workflow_id: str, requester_id: str, reviewers: list[str], message: str = "") -> Review:
        review = Review(self._generate_id(), workflow_id, requester_id, reviewers)
        self.reviews[review.review_id] = review
        workflow = self.workflows.get(workflow_id)
        if workflow:
            workflow["status"] = WorkflowStatus.PENDING_REVIEW.value
        self._track_change(workflow_id, requester_id, "review_requested", {"review_id": review.review_id, "reviewers": reviewers})
        self._log_activity(requester_id, workflow_id, "review_requested", {"review_id": review.review_id})
        self._notify(reviewers, "review_request", "Review Requested", f"Review requested for workflow", workflow_id)
        return review

    def _get_workflow_reviewers(self, workflow_id: str) -> list[str]:
        if workflow_id in self.workflow_permissions:
            return list(self.workflow_permissions[workflow_id].keys())
        return []

    def submit_review_decision(self, review_id: str, reviewer_id: str, approved: bool, comment: str = None) -> bool:
        review = self.reviews.get(review_id)
        if not review or review.status != "pending" or reviewer_id not in review.reviewers:
            return False
        review.status = "approved" if approved else "rejected"
        review.decided_at = datetime.now()
        review.decision_by = reviewer_id
        review.decision_comment = comment
        workflow = self.workflows.get(review.workflow_id)
        if workflow:
            workflow["status"] = WorkflowStatus.APPROVED.value if approved else WorkflowStatus.REJECTED.value
        self._track_change(review.workflow_id, reviewer_id, "review_decision", {"review_id": review_id, "approved": approved})
        self._log_activity(reviewer_id, review.workflow_id, "review_decided", {"review_id": review_id, "approved": approved})
        self._notify([review.requester_id], "review_completed", "Review Completed", f"Your review request has been {review.status}", review.workflow_id)
        return True

    def get_pending_reviews(self, user_id: str) -> list[Review]:
        return [r for r in self.reviews.values() if r.status == "pending" and user_id in r.reviewers]

    # Approval Workflow
    def submit_for_approval(self, workflow_id: str, user_id: str, approvers: list[str]) -> bool:
        if not self._check_permission(user_id, workflow_id, Permission.WRITE):
            return False
        self.request_review(workflow_id, user_id, approvers)
        return True

    def approve_workflow(self, workflow_id: str, approver_id: str, comment: str = None) -> bool:
        pending = [r for r in self.reviews.values() if r.workflow_id == workflow_id and r.status == "pending"]
        if not pending:
            return False
        return self.submit_review_decision(pending[0].review_id, approver_id, True, comment)

    def reject_workflow(self, workflow_id: str, approver_id: str, comment: str = None) -> bool:
        pending = [r for r in self.reviews.values() if r.workflow_id == workflow_id and r.status == "pending"]
        if not pending:
            return False
        return self.submit_review_decision(pending[0].review_id, approver_id, False, comment)

    def deploy_workflow(self, workflow_id: str, user_id: str) -> bool:
        workflow = self.workflows.get(workflow_id)
        if not workflow:
            return False
        if workflow["status"] != WorkflowStatus.APPROVED.value:
            return False
        if not self._check_permission(user_id, workflow_id, Permission.EXECUTE):
            return False
        workflow["status"] = WorkflowStatus.DEPLOYED.value
        self._track_change(workflow_id, user_id, "workflow_deployed", {})
        self._log_activity(user_id, workflow_id, "workflow_deployed", {})
        return True

    # Version History
    def get_version_history(self, workflow_id: str) -> list[WorkflowVersion]:
        return sorted(self.workflow_versions.get(workflow_id, []), key=lambda x: x.version_number, reverse=True)

    def get_version(self, workflow_id: str, version_number: int) -> Optional[WorkflowVersion]:
        versions = self.workflow_versions.get(workflow_id, [])
        for v in versions:
            if v.version_number == version_number:
                return v
        return None

    def get_version_diff(self, workflow_id: str, from_version: int, to_version: int) -> dict:
        v1 = self.get_version(workflow_id, from_version)
        v2 = self.get_version(workflow_id, to_version)
        if not v1 or not v2:
            return {}
        return self._compute_diff(v1.content, v2.content)

    def rollback_to_version(self, workflow_id: str, version_number: int, user_id: str) -> Optional[WorkflowVersion]:
        version = self.get_version(workflow_id, version_number)
        if not version:
            return None
        current = self.workflows.get(workflow_id)
        if not current:
            return None
        old_content = current["content"]
        new_version = self.track_workflow_change(workflow_id, user_id, "rollback", old_content, version.content, f"Rolled back to version {version_number}")
        new_version.version_number = self._next_version_numbers.get(workflow_id, 1) - 1
        return new_version

    # Notifications
    def get_notifications(self, user_id: str, unread_only: bool = False) -> list[Notification]:
        if user_id not in self.notifications:
            return []
        notifications = self.notifications[user_id]
        if unread_only:
            notifications = [n for n in notifications if not n.read]
        return sorted(notifications, key=lambda x: x.created_at, reverse=True)

    def mark_notification_read(self, user_id: str, notification_id: str) -> bool:
        if user_id in self.notifications:
            for n in self.notifications[user_id]:
                if n.notification_id == notification_id:
                    n.read = True
                    return True
        return False

    def mark_all_notifications_read(self, user_id: str) -> int:
        count = 0
        if user_id in self.notifications:
            for n in self.notifications[user_id]:
                if not n.read:
                    n.read = True
                    count += 1
        return count

    # Activity Feed
    def get_activity_feed(self, workflow_id: str = None, limit: int = 100) -> list[ActivityFeedEntry]:
        if workflow_id:
            entries = [e for e in self.activity_feed if e.workflow_id == workflow_id]
        else:
            entries = self.activity_feed
        return sorted(entries, key=lambda x: x.timestamp, reverse=True)[:limit]

    # Team Workspace Integration
    def get_user_workspaces(self, user_id: str) -> list[TeamWorkspace]:
        return [w for w in self.workspaces.values() if user_id in w.members]

    def get_workspace_workflows(self, workspace_id: str) -> list[dict]:
        workspace = self.workspaces.get(workspace_id)
        if not workspace:
            return []
        return [w for wid, w in self.workflows.items() if wid in workspace.members or w.get("workspace_id") == workspace_id]

    # Export/Import for persistence
    def to_dict(self) -> dict:
        return {
            "users": {uid: u.to_dict() for uid, u in self.users.items()},
            "workflows": self.workflows,
            "workflow_permissions": {wid: {uid: p.to_dict() for uid, p in perms.items()} for wid, perms in self.workflow_permissions.items()},
            "workflow_versions": {wid: [v.to_dict() for v in vers] for wid, vers in self.workflow_versions.items()},
            "change_records": [c.to_dict() for c in self.change_records],
            "comments": {wid: [c.to_dict() for c in cmts] for wid, cmts in self.comments.items()},
            "reviews": {rid: r.to_dict() for rid, r in self.reviews.items()},
            "notifications": {uid: [n.to_dict() for n in notifs] for uid, notifs in self.notifications.items()},
            "activity_feed": [a.to_dict() for a in self.activity_feed],
            "locks": {wid: l.to_dict() for wid, l in self.locks.items()},
            "workspaces": {wid: w.to_dict() for wid, w in self.workspaces.items()},
            "next_version_numbers": self._next_version_numbers
        }

    def save(self, path: str = None) -> bool:
        path = path or self.storage_path
        if not path:
            return False
        try:
            with open(path, 'w') as f:
                json.dump(self.to_dict(), f, indent=2, default=str)
            return True
        except Exception:
            return False

    @classmethod
    def load(cls, path: str) -> Optional["WorkflowCollaboration"]:
        try:
            with open(path, 'r') as f:
                data = json.load(f)
            collab = cls(storage_path=path)
            collab.users = {uid: User(**u) for uid, u in data.get("users", {}).items()}
            collab.workflows = data.get("workflows", {})
            collab.workflow_permissions = {}
            for wid, perms in data.get("workflow_permissions", {}).items():
                collab.workflow_permissions[wid] = {}
                for uid, pdata in perms.items():
                    perm = WorkflowPermission(pdata["user_id"], wid, [Permission(p) for p in pdata["permissions"]])
                    perm.granted_at = datetime.fromisoformat(pdata["granted_at"])
                    perm.granted_by = pdata.get("granted_by")
                    collab.workflow_permissions[wid][uid] = perm
            collab.workflow_versions = {}
            for wid, vers in data.get("workflow_versions", {}).items():
                collab.workflow_versions[wid] = []
                for vdata in vers:
                    v = WorkflowVersion(vdata["version_id"], vdata["workflow_id"], vdata["content"], vdata["author"])
                    v.version_number = vdata["version_number"]
                    v.created_at = datetime.fromisoformat(vdata["created_at"])
                    v.message = vdata.get("message", "")
                    v.diff_from_previous = vdata.get("diff_from_previous", {})
                    collab.workflow_versions[wid].append(v)
            collab.change_records = [ChangeRecord(c["record_id"], c["workflow_id"], c["user_id"], c["change_type"], c["details"]) for c in data.get("change_records", [])]
            for c in collab.change_records:
                c.timestamp = datetime.fromisoformat(c.timestamp)
            collab.comments = {}
            for wid, cmts in data.get("comments", {}).items():
                collab.comments[wid] = []
                for cdata in cmts:
                    c = Comment(cdata["comment_id"], cdata["workflow_id"], cdata["user_id"], cdata["content"], cdata.get("version_id"))
                    c.created_at = datetime.fromisoformat(cdata["created_at"])
                    c.updated_at = datetime.fromisoformat(cdata["updated_at"])
                    c.resolved = cdata.get("resolved", False)
                    collab.comments[wid].append(c)
            collab.reviews = {}
            for rid, rdata in data.get("reviews", {}).items():
                r = Review(rdata["review_id"], rdata["workflow_id"], rdata["requester_id"], rdata["reviewers"])
                r.status = rdata["status"]
                r.created_at = datetime.fromisoformat(rdata["created_at"])
                if rdata.get("decided_at"):
                    r.decided_at = datetime.fromisoformat(rdata["decided_at"])
                r.decision_by = rdata.get("decision_by")
                r.decision_comment = rdata.get("decision_comment")
                collab.reviews[rid] = r
            collab.notifications = {}
            for uid, notifs in data.get("notifications", {}).items():
                collab.notifications[uid] = []
                for ndata in notifs:
                    n = Notification(ndata["notification_id"], ndata["user_id"], ndata["notification_type"], ndata["title"], ndata["message"], ndata.get("workflow_id"))
                    n.read = ndata.get("read", False)
                    n.created_at = datetime.fromisoformat(ndata["created_at"])
                    collab.notifications[uid].append(n)
            collab.activity_feed = []
            for adata in data.get("activity_feed", []):
                a = ActivityFeedEntry(adata["entry_id"], adata["user_id"], adata["workflow_id"], adata["action"], adata["details"])
                a.timestamp = datetime.fromisoformat(adata["timestamp"])
                collab.activity_feed.append(a)
            collab.locks = {}
            for wid, ldata in data.get("locks", {}).items():
                l = LockInfo(ldata["workflow_id"], ldata["user_id"], ldata["lock_type"])
                l.locked_at = datetime.fromisoformat(ldata["locked_at"])
                l.expires_at = datetime.fromisoformat(ldata["expires_at"])
                collab.locks[wid] = l
            collab.workspaces = {}
            for wid, wdata in data.get("workspaces", {}).items():
                w = TeamWorkspace(wdata["workspace_id"], wdata["name"], wdata["owner_id"])
                w.members = wdata.get("members", [])
                w.created_at = datetime.fromisoformat(wdata["created_at"])
                w.settings = wdata.get("settings", {})
                w.default_workflow_permissions = wdata.get("default_permissions", {})
                collab.workspaces[wid] = w
            collab._next_version_numbers = data.get("next_version_numbers", {})
            return collab
        except Exception:
            return None
