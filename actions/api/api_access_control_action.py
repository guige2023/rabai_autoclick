"""API Access Control Action Module.

Provides role-based access control (RBAC) and permission
management for API operations including permission checking,
role assignment, and access policy enforcement.

Example:
    >>> from actions.api.api_access_control_action import APIAccessControl, AccessPolicy
    >>> acl = APIAccessControl()
    >>> result = acl.check_permission(user_id, "read", resource="api:/data")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
import threading
import time
import uuid


class Permission(Enum):
    """Available permissions."""
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    ADMIN = "admin"
    EXECUTE = "execute"
    CREATE = "create"


class ResourceType(Enum):
    """Resource types."""
    API_ENDPOINT = "api_endpoint"
    DATA = "data"
    USER = "user"
    SYSTEM = "system"


@dataclass
class Role:
    """Role definition.
    
    Attributes:
        role_id: Unique role identifier
        name: Role name
        permissions: Set of granted permissions
        description: Role description
    """
    role_id: str
    name: str
    permissions: Set[Permission] = field(default_factory=set)
    description: str = ""


@dataclass
class User:
    """User with roles and attributes.
    
    Attributes:
        user_id: Unique user identifier
        username: Username
        roles: Assigned role IDs
        attributes: Additional user attributes
        is_active: Whether user is active
    """
    user_id: str
    username: str
    roles: Set[str] = field(default_factory=set)
    attributes: Dict[str, Any] = field(default_factory=dict)
    is_active: bool = True


@dataclass
class AccessDecision:
    """Access control decision result.
    
    Attributes:
        allowed: Whether access is granted
        reason: Reason for decision
        matched_permissions: Permissions that matched
        evaluated_at: Evaluation timestamp
    """
    allowed: bool
    reason: str
    matched_permissions: Set[Permission] = field(default_factory=set)
    evaluated_at: datetime = field(default_factory=datetime.now)


@dataclass
class AccessPolicy:
    """Access control policy.
    
    Attributes:
        policy_id: Unique policy identifier
        name: Policy name
        resource_pattern: Resource pattern (e.g., "api:/users/*")
        allowed_permissions: Permissions that match this policy
        role_conditions: Role-based conditions
        time_restrictions: Time-based restrictions
        ip_whitelist: Allowed IP addresses
    """
    policy_id: str
    name: str
    resource_pattern: str
    allowed_permissions: Set[Permission] = field(default_factory=set)
    role_conditions: Dict[str, Any] = field(default_factory=dict)
    time_restrictions: Optional[Dict[str, Any]] = None
    ip_whitelist: Set[str] = field(default_factory=set)
    is_active: bool = True


class APIAccessControl:
    """Role-based access control for API operations.
    
    Provides permission checking, role management, and
    policy enforcement for API access control.
    
    Attributes:
        _roles: Registered roles
        _users: Registered users
        _policies: Access control policies
        _audit_log: Access audit trail
        _cache: Permission cache
        _lock: Thread safety lock
    """
    
    def __init__(self) -> None:
        """Initialize access control."""
        self._roles: Dict[str, Role] = {}
        self._users: Dict[str, User] = {}
        self._policies: Dict[str, AccessPolicy] = {}
        self._audit_log: List[Dict[str, Any]] = []
        self._cache: Dict[str, AccessDecision] = {}
        self._cache_ttl: int = 300  # 5 minutes
        self._lock = threading.RLock()
        self._default_roles()
    
    def _default_roles(self) -> None:
        """Create default roles."""
        admin_role = Role(
            role_id="admin",
            name="Administrator",
            permissions={p for p in Permission},
            description="Full system access",
        )
        self._roles["admin"] = admin_role
        
        user_role = Role(
            role_id="user",
            name="Standard User",
            permissions={Permission.READ, Permission.EXECUTE},
            description="Standard user access",
        )
        self._roles["user"] = user_role
        
        readonly_role = Role(
            role_id="readonly",
            name="Read Only",
            permissions={Permission.READ},
            description="Read-only access",
        )
        self._roles["readonly"] = readonly_role
    
    def register_role(self, role: Role) -> str:
        """Register a new role.
        
        Args:
            role: Role to register
            
        Returns:
            Registered role ID
        """
        with self._lock:
            if role.role_id in self._roles:
                raise ValueError(f"Role {role.role_id} already exists")
            self._roles[role.role_id] = role
            self._invalidate_cache()
            return role.role_id
    
    def register_user(self, user: User) -> str:
        """Register a new user.
        
        Args:
            user: User to register
            
        Returns:
            Registered user ID
        """
        with self._lock:
            if user.user_id in self._users:
                raise ValueError(f"User {user.user_id} already exists")
            self._users[user.user_id] = user
            self._invalidate_cache()
            return user.user_id
    
    def assign_role(self, user_id: str, role_id: str) -> None:
        """Assign role to user.
        
        Args:
            user_id: User ID
            role_id: Role ID to assign
        """
        with self._lock:
            if user_id not in self._users:
                raise KeyError(f"User {user_id} not found")
            if role_id not in self._roles:
                raise KeyError(f"Role {role_id} not found")
            self._users[user_id].roles.add(role_id)
            self._invalidate_cache()
    
    def revoke_role(self, user_id: str, role_id: str) -> None:
        """Revoke role from user.
        
        Args:
            user_id: User ID
            role_id: Role ID to revoke
        """
        with self._lock:
            if user_id not in self._users:
                raise KeyError(f"User {user_id} not found")
            self._users[user_id].roles.discard(role_id)
            self._invalidate_cache()
    
    def add_policy(self, policy: AccessPolicy) -> str:
        """Add access control policy.
        
        Args:
            policy: Policy to add
            
        Returns:
            Policy ID
        """
        with self._lock:
            self._policies[policy.policy_id] = policy
            self._invalidate_cache()
            return policy.policy_id
    
    def check_permission(
        self,
        user_id: str,
        permission: Permission,
        resource: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> AccessDecision:
        """Check if user has permission for resource.
        
        Args:
            user_id: User ID
            permission: Required permission
            resource: Resource identifier
            context: Additional context
            
        Returns:
            Access decision
        """
        context = context or {}
        cache_key = f"{user_id}:{permission.value}:{resource}"
        
        # Check cache
        if cache_key in self._cache:
            cached = self._cache[cache_key]
            age = (datetime.now() - cached.evaluated_at).total_seconds()
            if age < self._cache_ttl:
                return cached
        
        # Get user
        if user_id not in self._users:
            decision = AccessDecision(False, f"User {user_id} not found")
            self._audit(decision, user_id, permission, resource)
            return decision
        
        user = self._users[user_id]
        if not user.is_active:
            decision = AccessDecision(False, "User is inactive")
            self._audit(decision, user_id, permission, resource)
            return decision
        
        # Check direct role permissions
        matched = self._check_role_permissions(user, permission, resource)
        
        # Check policies
        policy_matched = self._check_policies(user, permission, resource, context)
        matched.update(policy_matched)
        
        allowed = len(matched) > 0
        reason = "Access granted" if allowed else "No matching permission found"
        
        decision = AccessDecision(allowed, reason, matched)
        self._cache[cache_key] = decision
        self._audit(decision, user_id, permission, resource)
        
        return decision
    
    def _check_role_permissions(
        self,
        user: User,
        permission: Permission,
        resource: str,
    ) -> Set[Permission]:
        """Check permissions from user roles.
        
        Args:
            user: User to check
            permission: Required permission
            resource: Resource identifier
            
        Returns:
            Set of matched permissions
        """
        matched: Set[Permission] = set()
        
        for role_id in user.roles:
            if role_id not in self._roles:
                continue
            role = self._roles[role_id]
            if permission in role.permissions:
                matched.add(permission)
            # Admin has all permissions
            if Permission.ADMIN in role.permissions:
                matched.add(permission)
                for p in Permission:
                    matched.add(p)
        
        return matched
    
    def _check_policies(
        self,
        user: User,
        permission: Permission,
        resource: str,
        context: Dict[str, Any],
    ) -> Set[Permission]:
        """Check policy-based permissions.
        
        Args:
            user: User to check
            permission: Required permission
            resource: Resource identifier
            context: Additional context
            
        Returns:
            Set of matched permissions
        """
        matched: Set[Permission] = set()
        
        for policy in self._policies.values():
            if not policy.is_active:
                continue
            if not self._match_resource_pattern(resource, policy.resource_pattern):
                continue
            if permission in policy.allowed_permissions:
                if self._check_policy_conditions(policy, user, context):
                    matched.add(permission)
        
        return matched
    
    def _match_resource_pattern(self, resource: str, pattern: str) -> bool:
        """Match resource against pattern.
        
        Args:
            resource: Resource identifier
            pattern: Resource pattern (supports * wildcards)
            
        Returns:
            True if matches
        """
        import fnmatch
        return fnmatch.fnmatch(resource, pattern)
    
    def _check_policy_conditions(
        self,
        policy: AccessPolicy,
        user: User,
        context: Dict[str, Any],
    ) -> bool:
        """Check policy conditions.
        
        Args:
            policy: Policy to check
            user: User to check
            context: Additional context
            
        Returns:
            True if conditions pass
        """
        # Check time restrictions
        if policy.time_restrictions:
            hour_restriction = policy.time_restrictions.get("allowed_hours")
            if hour_restriction:
                current_hour = datetime.now().hour
                if current_hour not in hour_restriction:
                    return False
        
        # Check IP whitelist
        if policy.ip_whitelist:
            client_ip = context.get("ip_address", "")
            if client_ip and client_ip not in policy.ip_whitelist:
                return False
        
        return True
    
    def _audit(
        self,
        decision: AccessDecision,
        user_id: str,
        permission: Permission,
        resource: str,
    ) -> None:
        """Log access decision to audit trail.
        
        Args:
            decision: Access decision
            user_id: User ID
            permission: Permission checked
            resource: Resource identifier
        """
        entry = {
            "timestamp": datetime.now().isoformat(),
            "user_id": user_id,
            "permission": permission.value,
            "resource": resource,
            "allowed": decision.allowed,
            "reason": decision.reason,
        }
        with self._lock:
            self._audit_log.append(entry)
            # Keep last 10000 entries
            if len(self._audit_log) > 10000:
                self._audit_log = self._audit_log[-5000:]
    
    def _invalidate_cache(self) -> None:
        """Invalidate permission cache."""
        self._cache.clear()
    
    def get_audit_log(
        self,
        user_id: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get audit log entries.
        
        Args:
            user_id: Filter by user ID
            limit: Maximum entries to return
            
        Returns:
            List of audit entries
        """
        with self._lock:
            entries = self._audit_log
            if user_id:
                entries = [e for e in entries if e["user_id"] == user_id]
            return entries[-limit:]
