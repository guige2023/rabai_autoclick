"""Permission manager action for access control.

Provides role-based access control, permission checking,
and user/session management.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class PermissionScope(Enum):
    READ = "read"
    WRITE = "write"
    EXECUTE = "execute"
    ADMIN = "admin"


@dataclass
class Role:
    name: str
    permissions: set[PermissionScope] = field(default_factory=set)
    inherits_from: Optional[str] = None


@dataclass
class User:
    user_id: str
    username: str
    roles: set[str] = field(default_factory=set)
    is_active: bool = True
    created_at: float = field(default_factory=time.time)
    last_login: Optional[float] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class AccessLog:
    user_id: str
    action: str
    resource: str
    granted: bool
    timestamp: float
    ip_address: Optional[str] = None


class PermissionManagerAction:
    """Manage roles, permissions, and access control.

    Args:
        enable_audit_log: Whether to maintain access audit log.
        max_audit_entries: Maximum audit log entries.
    """

    def __init__(
        self,
        enable_audit_log: bool = True,
        max_audit_entries: int = 5000,
    ) -> None:
        self._roles: dict[str, Role] = {}
        self._users: dict[str, User] = {}
        self._access_log: list[AccessLog] = []
        self._max_audit_entries = max_audit_entries
        self._enable_audit_log = enable_audit_log
        self._permission_hooks: dict[str, list[Callable]] = {
            "on_grant": [],
            "on_revoke": [],
            "on_deny": [],
        }

    def create_role(
        self,
        name: str,
        permissions: Optional[set[PermissionScope]] = None,
        inherits_from: Optional[str] = None,
    ) -> bool:
        """Create a new role.

        Args:
            name: Role name.
            permissions: Set of permissions for the role.
            inherits_from: Optional parent role name.

        Returns:
            True if role was created successfully.
        """
        if name in self._roles:
            logger.warning(f"Role already exists: {name}")
            return False

        if inherits_from and inherits_from not in self._roles:
            logger.error(f"Parent role not found: {inherits_from}")
            return False

        self._roles[name] = Role(
            name=name,
            permissions=permissions or set(),
            inherits_from=inherits_from,
        )
        logger.debug(f"Created role: {name}")
        return True

    def delete_role(self, name: str) -> bool:
        """Delete a role.

        Args:
            name: Role name to delete.

        Returns:
            True if role was deleted.
        """
        if name not in self._roles:
            return False

        for user in self._users.values():
            user.roles.discard(name)

        del self._roles[name]
        return True

    def grant_permission(
        self,
        role_name: str,
        permission: PermissionScope,
    ) -> bool:
        """Grant a permission to a role.

        Args:
            role_name: Role name.
            permission: Permission to grant.

        Returns:
            True if permission was granted.
        """
        role = self._roles.get(role_name)
        if not role:
            logger.error(f"Role not found: {role_name}")
            return False

        role.permissions.add(permission)
        for hook in self._permission_hooks["on_grant"]:
            try:
                hook(role_name, permission)
            except Exception as e:
                logger.error(f"Grant hook error: {e}")
        return True

    def revoke_permission(
        self,
        role_name: str,
        permission: PermissionScope,
    ) -> bool:
        """Revoke a permission from a role.

        Args:
            role_name: Role name.
            permission: Permission to revoke.

        Returns:
            True if permission was revoked.
        """
        role = self._roles.get(role_name)
        if not role:
            return False

        role.permissions.discard(permission)
        for hook in self._permission_hooks["on_revoke"]:
            try:
                hook(role_name, permission)
            except Exception as e:
                logger.error(f"Revoke hook error: {e}")
        return True

    def register_user(
        self,
        user_id: str,
        username: str,
        role_names: Optional[set[str]] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> bool:
        """Register a new user.

        Args:
            user_id: Unique user ID.
            username: Username.
            role_names: Initial role assignments.
            metadata: Additional user metadata.

        Returns:
            True if user was registered.
        """
        if user_id in self._users:
            logger.warning(f"User already exists: {user_id}")
            return False

        invalid_roles = (role_names or set()) - set(self._roles.keys())
        if invalid_roles:
            logger.error(f"Invalid roles: {invalid_roles}")
            return False

        self._users[user_id] = User(
            user_id=user_id,
            username=username,
            roles=role_names or set(),
            metadata=metadata or {},
        )
        logger.debug(f"Registered user: {username} ({user_id})")
        return True

    def assign_role(self, user_id: str, role_name: str) -> bool:
        """Assign a role to a user.

        Args:
            user_id: User ID.
            role_name: Role name.

        Returns:
            True if role was assigned.
        """
        user = self._users.get(user_id)
        if not user:
            logger.error(f"User not found: {user_id}")
            return False

        if role_name not in self._roles:
            logger.error(f"Role not found: {role_name}")
            return False

        user.roles.add(role_name)
        return True

    def remove_role(self, user_id: str, role_name: str) -> bool:
        """Remove a role from a user.

        Args:
            user_id: User ID.
            role_name: Role name.

        Returns:
            True if role was removed.
        """
        user = self._users.get(user_id)
        if not user:
            return False

        user.roles.discard(role_name)
        return True

    def check_permission(
        self,
        user_id: str,
        permission: PermissionScope,
        resource: Optional[str] = None,
        ip_address: Optional[str] = None,
    ) -> bool:
        """Check if a user has a specific permission.

        Args:
            user_id: User ID to check.
            permission: Permission to check.
            resource: Optional resource being accessed.
            ip_address: Optional client IP address.

        Returns:
            True if permission is granted.
        """
        user = self._users.get(user_id)
        if not user or not user.is_active:
            self._log_access(user_id, permission.value, resource or "", False, ip_address)
            return False

        for role_name in user.roles:
            if self._role_has_permission(role_name, permission):
                self._log_access(user_id, permission.value, resource or "", True, ip_address)
                return True

        for hook in self._permission_hooks["on_deny"]:
            try:
                hook(user_id, permission)
            except Exception as e:
                logger.error(f"Deny hook error: {e}")

        self._log_access(user_id, permission.value, resource or "", False, ip_address)
        return False

    def _role_has_permission(self, role_name: str, permission: PermissionScope) -> bool:
        """Check if a role has a permission (including inherited).

        Args:
            role_name: Role name.
            permission: Permission to check.

        Returns:
            True if role has permission.
        """
        role = self._roles.get(role_name)
        if not role:
            return False

        if permission in role.permissions:
            return True

        if role.inherits_from:
            return self._role_has_permission(role.inherits_from, permission)

        return False

    def _log_access(
        self,
        user_id: str,
        action: str,
        resource: str,
        granted: bool,
        ip_address: Optional[str] = None,
    ) -> None:
        """Log an access attempt.

        Args:
            user_id: User ID.
            action: Action being attempted.
            resource: Resource being accessed.
            granted: Whether access was granted.
            ip_address: Client IP address.
        """
        if not self._enable_audit_log:
            return

        self._access_log.append(AccessLog(
            user_id=user_id,
            action=action,
            resource=resource,
            granted=granted,
            timestamp=time.time(),
            ip_address=ip_address,
        ))

        if len(self._access_log) > self._max_audit_entries:
            self._access_log.pop(0)

    def register_permission_hook(
        self,
        hook_type: str,
        callback: Callable,
    ) -> None:
        """Register a permission hook callback.

        Args:
            hook_type: Hook type ('on_grant', 'on_revoke', 'on_deny').
            callback: Callback function.
        """
        if hook_type in self._permission_hooks:
            self._permission_hooks[hook_type].append(callback)

    def get_user(self, user_id: str) -> Optional[User]:
        """Get user by ID.

        Args:
            user_id: User ID.

        Returns:
            User object or None.
        """
        return self._users.get(user_id)

    def get_role(self, name: str) -> Optional[Role]:
        """Get role by name.

        Args:
            name: Role name.

        Returns:
            Role object or None.
        """
        return self._roles.get(name)

    def get_access_log(
        self,
        user_id_filter: Optional[str] = None,
        granted_filter: Optional[bool] = None,
        limit: int = 100,
    ) -> list[AccessLog]:
        """Get access audit log.

        Args:
            user_id_filter: Filter by user ID.
            granted_filter: Filter by granted status.
            limit: Maximum results.

        Returns:
            List of access log entries (newest first).
        """
        log = self._access_log
        if user_id_filter:
            log = [e for e in log if e.user_id == user_id_filter]
        if granted_filter is not None:
            log = [e for e in log if e.granted == granted_filter]
        return log[-limit:][::-1]

    def get_stats(self) -> dict[str, Any]:
        """Get permission manager statistics.

        Returns:
            Dictionary with stats.
        """
        total_users = len(self._users)
        active_users = sum(1 for u in self._users.values() if u.is_active)
        total_roles = len(self._roles)
        total_permissions = sum(
            len(r.permissions) for r in self._roles.values()
        )
        return {
            "total_users": total_users,
            "active_users": active_users,
            "total_roles": total_roles,
            "total_permissions": total_permissions,
            "audit_log_entries": len(self._access_log),
        }
