"""
Workflow Compliance Module - Compliance and Governance for RabAI AutoClick
Provides policy enforcement, audit trail, RBAC, data residency, consent management,
data retention, privacy impact assessment, regulatory reporting, and compliance dashboard.
"""
import hashlib
import json
import os
import time
import uuid
import re
import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from functools import wraps
import logging

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False


# ============================================================================
# Enums and Data Classes
# ============================================================================

class ComplianceLevel(Enum):
    """Compliance levels for regulatory frameworks."""
    NONE = auto()
    GDPR = auto()       # General Data Protection Regulation
    CCPA = auto()       # California Consumer Privacy Act
    HIPAA = auto()      # Health Insurance Portability and Accountability Act
    SOC2 = auto()       # Service Organization Control 2
    ISO27001 = auto()   # ISO/IEC 27001 Information Security


class DataCategory(Enum):
    """Categories of data for classification."""
    PUBLIC = auto()
    INTERNAL = auto()
    CONFIDENTIAL = auto()
    RESTRICTED = auto()
    PII = auto()        # Personally Identifiable Information
    PHI = auto()        # Protected Health Information
    FINANCIAL = auto()
    USER_CONTENT = auto()


class DataResidencyRegion(Enum):
    """Allowed data residency regions."""
    US = auto()
    EU = auto()
    UK = auto()
    APAC = auto()
    LATAM = auto()
    GLOBAL = auto()


class RetentionPolicy(Enum):
    """Data retention policy types."""
    DEFAULT = auto()
    SHORT_TERM = auto()     # 30 days
    MEDIUM_TERM = auto()    # 90 days
    LONG_TERM = auto()      # 1 year
    ARCHIVAL = auto()       # 7 years
    INDEFINITE = auto()
    IMMEDIATE = auto()      # No retention


class UserRole(Enum):
    """User roles for RBAC."""
    ADMIN = auto()
    COMPLIANCE_OFFICER = auto()
    AUDITOR = auto()
    WORKFLOW_CREATOR = auto()
    WORKFLOW_EXECUTOR = auto()
    VIEWER = auto()
    GUEST = auto()


class PolicyViolationSeverity(Enum):
    """Severity levels for policy violations."""
    INFO = auto()
    LOW = auto()
    MEDIUM = auto()
    HIGH = auto()
    CRITICAL = auto()
    BLOCKING = auto()


class ConsentStatus(Enum):
    """User consent status."""
    GRANTED = auto()
    DENIED = auto()
    WITHDRAWN = auto()
    EXPIRED = auto()
    PENDING = auto()


class AuditAction(Enum):
    """Auditable compliance actions."""
    POLICY_CREATED = auto()
    POLICY_UPDATED = auto()
    POLICY_DELETED = auto()
    POLICY_VIOLATION = auto()
    CONSENT_GRANTED = auto()
    CONSENT_WITHDRAWN = auto()
    DATA_ACCESSED = auto()
    DATA_EXPORTED = auto()
    DATA_DELETED = auto()
    DATA_RETENTION_APPLIED = auto()
    PRIVACY_ASSESSMENT = auto()
    REGULATORY_REPORT_GENERATED = auto()
    ROLE_ASSIGNED = auto()
    DATA_RESIDENCY_VIOLATION = auto()


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class Policy:
    """Represents a compliance policy."""
    id: str
    name: str
    description: str
    compliance_levels: List[ComplianceLevel]
    enabled: bool = True
    priority: int = 0
    conditions: Dict[str, Any] = field(default_factory=dict)
    actions: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class PolicyViolation:
    """Represents a detected policy violation."""
    id: str
    policy_id: str
    policy_name: str
    severity: PolicyViolationSeverity
    description: str
    workflow_id: Optional[str] = None
    user_id: Optional[str] = None
    resource_id: Optional[str] = None
    detected_at: datetime = field(default_factory=datetime.utcnow)
    resolved: bool = False
    resolved_at: Optional[datetime] = None
    resolved_by: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AuditEntry:
    """Represents an audit log entry."""
    id: str
    timestamp: datetime
    action: AuditAction
    user_id: Optional[str]
    workflow_id: Optional[str]
    resource_type: Optional[str]
    resource_id: Optional[str]
    details: Dict[str, Any]
    ip_address: Optional[str] = None
    compliance_levels: List[ComplianceLevel] = field(default_factory=list)
    regulatory_framework: Optional[str] = None


@dataclass
class UserConsent:
    """Represents user consent for data processing."""
    id: str
    user_id: str
    consent_type: str
    purpose: str
    status: ConsentStatus
    granted_at: Optional[datetime] = None
    withdrawn_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    version: str = "1.0"
    proof: Optional[str] = None  # Hash proof of consent


@dataclass
class DataRetentionRule:
    """Data retention rule for a specific data type."""
    id: str
    name: str
    data_type: DataCategory
    retention_period: RetentionPolicy
    max_age_days: int
    auto_delete: bool = True
    requires_approval: bool = False
    approved_by: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class PrivacyImpactAssessment:
    """Privacy impact assessment for a workflow."""
    id: str
    workflow_id: str
    risk_score: float  # 0.0 - 1.0
    risk_factors: List[str] = field(default_factory=list)
    data_types_collected: List[DataCategory] = field(default_factory=list)
    data_types_shared: List[DataCategory] = field(default_factory=list)
    pii_involved: bool = False
    phi_involved: bool = False
    cross_border_transfer: bool = False
    automated_decisions: bool = False
    recommendations: List[str] = field(default_factory=list)
    approved: bool = False
    approved_by: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class ComplianceReport:
    """Compliance report for regulatory frameworks."""
    id: str
    report_type: str  # GDPR, CCPA, HIPAA, etc.
    generated_at: datetime
    period_start: datetime
    period_end: datetime
    summary: Dict[str, Any]
    violations_count: int
    data_access_count: int
    consent_changes: int
    retention_actions: int
    findings: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)


@dataclass
class RolePermission:
    """Role permission mapping."""
    role: UserRole
    allowed_actions: Set[str]
    denied_actions: Set[str] = field(default_factory=set)
    resource_restrictions: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# Compliance Manager
# ============================================================================

class ComplianceManager:
    """
    Central compliance and governance manager for workflow operations.
    
    Features:
    1. Policy enforcement: Enforce workflow policies
    2. Audit trail: Complete audit logging for compliance
    3. Role-based access: RBAC for workflow operations
    4. Data residency: Control where data can be stored
    5. Consent management: Manage user consents for data processing
    6. Data retention: Enforce data retention policies
    7. Privacy impact: Assess privacy impact of workflows
    8. Regulatory reporting: Generate compliance reports (GDPR, CCPA, HIPAA)
    9. Policy violations: Detect and report policy violations
    10. Compliance dashboard: Dashboard for compliance status
    """
    
    # Default role permission mappings
    DEFAULT_ROLE_PERMISSIONS: Dict[UserRole, RolePermission] = {}
    
    def __init__(self, storage_path: str = "./data/compliance"):
        """Initialize the compliance manager."""
        self.storage_path = storage_path
        self.logger = logging.getLogger(__name__)
        
        # State
        self._policies: Dict[str, Policy] = {}
        self._violations: Dict[str, PolicyViolation] = {}
        self._audit_log: List[AuditEntry] = []
        self._consents: Dict[str, List[UserConsent]] = {}
        self._retention_rules: Dict[str, DataRetentionRule] = {}
        self._privacy_assessments: Dict[str, PrivacyImpactAssessment] = {}
        self._user_roles: Dict[str, UserRole] = {}
        self._data_residency_rules: Dict[str, DataResidencyRegion] = {}
        self._workflow_data_locations: Dict[str, DataResidencyRegion] = {}
        
        # Initialize default role permissions
        self._init_default_permissions()
        
        # Create storage directory
        os.makedirs(storage_path, exist_ok=True)
        
        # Load persisted data
        self._load_data()
    
    def _init_default_permissions(self):
        """Initialize default role permissions."""
        self.DEFAULT_ROLE_PERMISSIONS = {
            UserRole.ADMIN: RolePermission(
                role=UserRole.ADMIN,
                allowed_actions={"*"},
                denied_actions=set()
            ),
            UserRole.COMPLIANCE_OFFICER: RolePermission(
                role=UserRole.COMPLIANCE_OFFICER,
                allowed_actions={
                    "view_policies", "create_policy", "update_policy", "delete_policy",
                    "view_audit_logs", "view_violations", "resolve_violations",
                    "view_reports", "generate_reports", "manage_consents",
                    "view_privacy_assessments", "approve_assessments"
                },
                denied_actions=set()
            ),
            UserRole.AUDITOR: RolePermission(
                role=UserRole.AUDITOR,
                allowed_actions={
                    "view_policies", "view_audit_logs", "view_violations",
                    "view_reports", "view_privacy_assessments"
                },
                denied_actions=set()
            ),
            UserRole.WORKFLOW_CREATOR: RolePermission(
                role=UserRole.WORKFLOW_CREATOR,
                allowed_actions={
                    "create_workflow", "edit_own_workflow", "view_own_workflow",
                    "submit_privacy_assessment"
                },
                denied_actions=set()
            ),
            UserRole.WORKFLOW_EXECUTOR: RolePermission(
                role=UserRole.WORKFLOW_EXECUTOR,
                allowed_actions={
                    "execute_workflow", "view_own_workflow"
                },
                denied_actions=set()
            ),
            UserRole.VIEWER: RolePermission(
                role=UserRole.VIEWER,
                allowed_actions={"view_own_workflow"},
                denied_actions=set()
            ),
            UserRole.GUEST: RolePermission(
                role=UserRole.GUEST,
                allowed_actions=set(),
                denied_actions={"*"}
            )
        }
    
    # =========================================================================
    # Policy Enforcement
    # =========================================================================
    
    def create_policy(
        self,
        name: str,
        description: str,
        compliance_levels: List[ComplianceLevel],
        conditions: Optional[Dict[str, Any]] = None,
        actions: Optional[List[str]] = None,
        priority: int = 0
    ) -> Policy:
        """Create a new compliance policy."""
        policy = Policy(
            id=str(uuid.uuid4()),
            name=name,
            description=description,
            compliance_levels=compliance_levels,
            conditions=conditions or {},
            actions=actions or [],
            priority=priority
        )
        self._policies[policy.id] = policy
        self._log_audit(
            AuditAction.POLICY_CREATED,
            user_id=None,
            workflow_id=None,
            resource_type="policy",
            resource_id=policy.id,
            details={"name": name, "compliance_levels": [c.name for c in compliance_levels]}
        )
        self._save_data()
        return policy
    
    def update_policy(self, policy_id: str, **updates) -> Optional[Policy]:
        """Update an existing policy."""
        if policy_id not in self._policies:
            return None
        
        policy = self._policies[policy_id]
        for key, value in updates.items():
            if hasattr(policy, key):
                setattr(policy, key, value)
        policy.updated_at = datetime.utcnow()
        
        self._log_audit(
            AuditAction.POLICY_UPDATED,
            user_id=None,
            workflow_id=None,
            resource_type="policy",
            resource_id=policy_id,
            details=updates
        )
        self._save_data()
        return policy
    
    def delete_policy(self, policy_id: str) -> bool:
        """Delete a policy."""
        if policy_id not in self._policies:
            return False
        
        policy = self._policies.pop(policy_id)
        self._log_audit(
            AuditAction.POLICY_DELETED,
            user_id=None,
            workflow_id=None,
            resource_type="policy",
            resource_id=policy_id,
            details={"name": policy.name}
        )
        self._save_data()
        return True
    
    def get_policy(self, policy_id: str) -> Optional[Policy]:
        """Get a policy by ID."""
        return self._policies.get(policy_id)
    
    def list_policies(
        self,
        compliance_level: Optional[ComplianceLevel] = None,
        enabled_only: bool = False
    ) -> List[Policy]:
        """List all policies, optionally filtered."""
        policies = list(self._policies.values())
        
        if compliance_level:
            policies = [p for p in policies if compliance_level in p.compliance_levels]
        
        if enabled_only:
            policies = [p for p in policies if p.enabled]
        
        return sorted(policies, key=lambda x: x.priority, reverse=True)
    
    def enforce_policy(
        self,
        policy_id: str,
        context: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """
        Enforce a policy against a given context.
        Returns (passed, reason).
        """
        policy = self._policies.get(policy_id)
        if not policy or not policy.enabled:
            return True, None
        
        # Check conditions
        for key, expected in policy.conditions.items():
            actual = context.get(key)
            if actual is None:
                return False, f"Missing required context key: {key}"
            if isinstance(expected, list) and actual not in expected:
                return False, f"Context value '{actual}' not in allowed values: {expected}"
            elif actual != expected:
                return False, f"Context value mismatch: expected {expected}, got {actual}"
        
        return True, None
    
    def check_all_policies(
        self,
        context: Dict[str, Any],
        workflow_id: Optional[str] = None
    ) -> List[PolicyViolation]:
        """Check all enabled policies and return violations."""
        violations = []
        
        for policy in self._policies.values():
            if not policy.enabled:
                continue
            
            passed, reason = self.enforce_policy(policy.id, context)
            if not passed:
                violation = self._create_violation(
                    policy=policy,
                    description=reason,
                    workflow_id=workflow_id,
                    context=context
                )
                violations.append(violation)
        
        return violations
    
    def _create_violation(
        self,
        policy: Policy,
        description: str,
        workflow_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> PolicyViolation:
        """Create and record a policy violation."""
        # Determine severity based on policy priority
        if policy.priority >= 10:
            severity = PolicyViolationSeverity.CRITICAL
        elif policy.priority >= 7:
            severity = PolicyViolationSeverity.HIGH
        elif policy.priority >= 4:
            severity = PolicyViolationSeverity.MEDIUM
        else:
            severity = PolicyViolationSeverity.LOW
        
        violation = PolicyViolation(
            id=str(uuid.uuid4()),
            policy_id=policy.id,
            policy_name=policy.name,
            severity=severity,
            description=description,
            workflow_id=workflow_id,
            metadata=context or {}
        )
        
        self._violations[violation.id] = violation
        
        self._log_audit(
            AuditAction.POLICY_VIOLATION,
            user_id=context.get("user_id") if context else None,
            workflow_id=workflow_id,
            resource_type="violation",
            resource_id=violation.id,
            details={
                "policy_name": policy.name,
                "severity": severity.name,
                "description": description
            }
        )
        
        return violation
    
    # =========================================================================
    # Audit Trail
    # =========================================================================
    
    def _log_audit(
        self,
        action: AuditAction,
        user_id: Optional[str],
        workflow_id: Optional[str],
        resource_type: Optional[str],
        resource_id: Optional[str],
        details: Dict[str, Any],
        ip_address: Optional[str] = None,
        compliance_levels: Optional[List[ComplianceLevel]] = None
    ):
        """Log an audit entry."""
        entry = AuditEntry(
            id=str(uuid.uuid4()),
            timestamp=datetime.utcnow(),
            action=action,
            user_id=user_id,
            workflow_id=workflow_id,
            resource_type=resource_type,
            resource_id=resource_id,
            details=details,
            ip_address=ip_address,
            compliance_levels=compliance_levels or []
        )
        self._audit_log.append(entry)
        
        # Keep audit log bounded
        if len(self._audit_log) > 100000:
            self._audit_log = self._audit_log[-50000:]
    
    def query_audit_log(
        self,
        action: Optional[AuditAction] = None,
        user_id: Optional[str] = None,
        workflow_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 1000
    ) -> List[AuditEntry]:
        """Query the audit log with filters."""
        results = self._audit_log
        
        if action:
            results = [e for e in results if e.action == action]
        if user_id:
            results = [e for e in results if e.user_id == user_id]
        if workflow_id:
            results = [e for e in results if e.workflow_id == workflow_id]
        if start_time:
            results = [e for e in results if e.timestamp >= start_time]
        if end_time:
            results = [e for e in results if e.timestamp <= end_time]
        
        # Sort by timestamp descending and limit
        results = sorted(results, key=lambda x: x.timestamp, reverse=True)[:limit]
        return results
    
    def get_audit_summary(
        self,
        days: int = 30
    ) -> Dict[str, Any]:
        """Get audit log summary for the past N days."""
        cutoff = datetime.utcnow() - timedelta(days=days)
        recent_entries = [e for e in self._audit_log if e.timestamp >= cutoff]
        
        # Count by action type
        action_counts: Dict[str, int] = {}
        for entry in recent_entries:
            action_name = entry.action.name
            action_counts[action_name] = action_counts.get(action_name, 0) + 1
        
        return {
            "total_entries": len(recent_entries),
            "action_counts": action_counts,
            "period_days": days,
            "oldest_entry": min(e.timestamp for e in recent_entries) if recent_entries else None,
            "newest_entry": max(e.timestamp for e in recent_entries) if recent_entries else None
        }
    
    # =========================================================================
    # Role-Based Access Control (RBAC)
    # =========================================================================
    
    def assign_role(self, user_id: str, role: UserRole) -> bool:
        """Assign a role to a user."""
        self._user_roles[user_id] = role
        
        self._log_audit(
            AuditAction.ROLE_ASSIGNED,
            user_id=user_id,
            workflow_id=None,
            resource_type="user_role",
            resource_id=user_id,
            details={"role": role.name}
        )
        self._save_data()
        return True
    
    def get_user_role(self, user_id: str) -> Optional[UserRole]:
        """Get a user's role."""
        return self._user_roles.get(user_id)
    
    def check_permission(
        self,
        user_id: str,
        action: str,
        resource_id: Optional[str] = None
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if a user has permission to perform an action.
        Returns (allowed, reason).
        """
        role = self._user_roles.get(user_id)
        if not role:
            return False, "User has no assigned role"
        
        role_perm = self.DEFAULT_ROLE_PERMISSIONS.get(role)
        if not role_perm:
            return False, f"No permissions defined for role: {role.name}"
        
        # Check denied actions
        if "*" in role_perm.denied_actions or action in role_perm.denied_actions:
            return False, f"Action '{action}' is denied for role {role.name}"
        
        # Check allowed actions
        if "*" not in role_perm.allowed_actions and action not in role_perm.allowed_actions:
            return False, f"Action '{action}' is not allowed for role {role.name}"
        
        return True, None
    
    def require_permission(self, action: str):
        """Decorator to enforce permission on a method."""
        def decorator(func):
            @wraps(func)
            def wrapper(self, user_id: str, *args, **kwargs):
                allowed, reason = self.check_permission(user_id, action)
                if not allowed:
                    raise PermissionError(reason)
                return func(self, user_id, *args, **kwargs)
            return wrapper
        return decorator
    
    def get_user_actions(self, user_id: str) -> Set[str]:
        """Get all allowed actions for a user."""
        role = self._user_roles.get(user_id)
        if not role:
            return set()
        
        role_perm = self.DEFAULT_ROLE_PERMISSIONS.get(role)
        if not role_perm:
            return set()
        
        if "*" in role_perm.allowed_actions:
            return {"*"}
        
        return role_perm.allowed_actions.copy()
    
    # =========================================================================
    # Data Residency
    # =========================================================================
    
    def set_data_residency(
        self,
        workflow_id: str,
        region: DataResidencyRegion
    ) -> bool:
        """Set data residency for a workflow."""
        self._workflow_data_locations[workflow_id] = region
        
        self._log_audit(
            AuditAction.DATA_ACCESSED,
            user_id=None,
            workflow_id=workflow_id,
            resource_type="data_residency",
            resource_id=workflow_id,
            details={"region": region.name}
        )
        self._save_data()
        return True
    
    def get_data_residency(self, workflow_id: str) -> Optional[DataResidencyRegion]:
        """Get data residency for a workflow."""
        return self._workflow_data_locations.get(workflow_id)
    
    def check_data_residency_compliance(
        self,
        workflow_id: str,
        required_regions: List[DataResidencyRegion]
    ) -> Tuple[bool, Optional[str]]:
        """Check if workflow data residency complies with requirements."""
        actual_region = self._workflow_data_locations.get(workflow_id)
        
        if not actual_region:
            return False, "No data residency set for workflow"
        
        if actual_region not in required_regions:
            return False, f"Data residency '{actual_region.name}' not in required regions"
        
        return True, None
    
    def add_residency_rule(
        self,
        data_type: str,
        allowed_regions: List[DataResidencyRegion]
    ) -> None:
        """Add a data residency rule for a data type."""
        # Store first region as primary (simplified)
        self._data_residency_rules[data_type] = allowed_regions[0] if allowed_regions else DataResidencyRegion.GLOBAL
    
    # =========================================================================
    # Consent Management
    # =========================================================================
    
    def grant_consent(
        self,
        user_id: str,
        consent_type: str,
        purpose: str,
        version: str = "1.0",
        expires_in_days: Optional[int] = None
    ) -> UserConsent:
        """Grant consent for data processing."""
        consent = UserConsent(
            id=str(uuid.uuid4()),
            user_id=user_id,
            consent_type=consent_type,
            purpose=purpose,
            status=ConsentStatus.GRANTED,
            granted_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(days=expires_in_days) if expires_in_days else None,
            version=version,
            proof=self._generate_consent_proof(user_id, consent_type, purpose)
        )
        
        if user_id not in self._consents:
            self._consents[user_id] = []
        self._consents[user_id].append(consent)
        
        self._log_audit(
            AuditAction.CONSENT_GRANTED,
            user_id=user_id,
            workflow_id=None,
            resource_type="consent",
            resource_id=consent.id,
            details={"consent_type": consent_type, "purpose": purpose}
        )
        self._save_data()
        return consent
    
    def withdraw_consent(self, user_id: str, consent_id: str) -> bool:
        """Withdraw a user's consent."""
        if user_id not in self._consents:
            return False
        
        for consent in self._consents[user_id]:
            if consent.id == consent_id and consent.status == ConsentStatus.GRANTED:
                consent.status = ConsentStatus.WITHDRAWN
                consent.withdrawn_at = datetime.utcnow()
                
                self._log_audit(
                    AuditAction.CONSENT_WITHDRAWN,
                    user_id=user_id,
                    workflow_id=None,
                    resource_type="consent",
                    resource_id=consent_id,
                    details={"consent_type": consent.consent_type}
                )
                self._save_data()
                return True
        
        return False
    
    def get_user_consents(
        self,
        user_id: str,
        active_only: bool = False
    ) -> List[UserConsent]:
        """Get all consents for a user."""
        consents = self._consents.get(user_id, [])
        
        if active_only:
            now = datetime.utcnow()
            consents = [
                c for c in consents
                if c.status == ConsentStatus.GRANTED
                and (c.expires_at is None or c.expires_at > now)
            ]
        
        return consents
    
    def check_consent(
        self,
        user_id: str,
        consent_type: str,
        purpose: Optional[str] = None
    ) -> Tuple[bool, Optional[str]]:
        """Check if user has valid consent for a given type and purpose."""
        consents = self.get_user_consents(user_id, active_only=True)
        
        for consent in consents:
            if consent.consent_type == consent_type:
                if purpose and consent.purpose != purpose:
                    continue
                return True, None
        
        return False, f"No valid consent found for type '{consent_type}'"
    
    def _generate_consent_proof(
        self,
        user_id: str,
        consent_type: str,
        purpose: str
    ) -> str:
        """Generate a hash proof of consent."""
        data = f"{user_id}:{consent_type}:{purpose}:{datetime.utcnow().isoformat()}"
        return hashlib.sha256(data.encode()).hexdigest()[:32]
    
    # =========================================================================
    # Data Retention
    # =========================================================================
    
    def create_retention_rule(
        self,
        name: str,
        data_type: DataCategory,
        retention_policy: RetentionPolicy,
        max_age_days: Optional[int] = None,
        auto_delete: bool = True
    ) -> DataRetentionRule:
        """Create a data retention rule."""
        # Calculate max_age_days from retention policy if not provided
        if max_age_days is None:
            policy_days = {
                RetentionPolicy.IMMEDIATE: 0,
                RetentionPolicy.SHORT_TERM: 30,
                RetentionPolicy.MEDIUM_TERM: 90,
                RetentionPolicy.LONG_TERM: 365,
                RetentionPolicy.ARCHIVAL: 2555,
                RetentionPolicy.INDEFINITE: 36500,
                RetentionPolicy.DEFAULT: 90
            }
            max_age_days = policy_days.get(retention_policy, 90)
        
        rule = DataRetentionRule(
            id=str(uuid.uuid4()),
            name=name,
            data_type=data_type,
            retention_period=retention_policy,
            max_age_days=max_age_days,
            auto_delete=auto_delete
        )
        
        self._retention_rules[rule.id] = rule
        self._save_data()
        return rule
    
    def get_retention_rule(self, rule_id: str) -> Optional[DataRetentionRule]:
        """Get a retention rule by ID."""
        return self._retention_rules.get(rule_id)
    
    def list_retention_rules(
        self,
        data_category: Optional[DataCategory] = None
    ) -> List[DataRetentionRule]:
        """List all retention rules."""
        rules = list(self._retention_rules.values())
        
        if data_category:
            rules = [r for r in rules if r.data_type == data_category]
        
        return rules
    
    def check_data_age_compliance(
        self,
        data_type: DataCategory,
        data_age_days: int
    ) -> Tuple[bool, Optional[str], Optional[DataRetentionRule]]:
        """
        Check if data is within retention period.
        Returns (compliant, reason, applicable_rule).
        """
        applicable_rule = None
        
        for rule in self._retention_rules.values():
            if rule.data_type == data_type:
                applicable_rule = rule
                if data_age_days > rule.max_age_days:
                    return False, f"Data exceeds retention period of {rule.max_age_days} days", rule
                break
        
        return True, None, applicable_rule
    
    def get_expired_data(
        self,
        data_type: Optional[DataCategory] = None
    ) -> List[Tuple[str, int, DataRetentionRule]]:
        """
        Get list of (data_id, age_days, rule) for expired data.
        Note: data_id and actual age should be provided by caller.
        """
        expired = []
        
        for rule in self._retention_rules.values():
            if data_type and rule.data_type != data_type:
                continue
            if rule.auto_delete:
                expired.append((None, rule.max_age_days + 1, rule))
        
        return expired
    
    # =========================================================================
    # Privacy Impact Assessment
    # =========================================================================
    
    def assess_privacy_impact(
        self,
        workflow_id: str,
        data_types_collected: List[DataCategory],
        data_types_shared: List[DataCategory],
        cross_border_transfer: bool = False,
        automated_decisions: bool = False,
        user_provided_data: bool = False
    ) -> PrivacyImpactAssessment:
        """Perform a privacy impact assessment for a workflow."""
        pii_involved = DataCategory.PII in data_types_collected or DataCategory.PII in data_types_shared
        phi_involved = DataCategory.PHI in data_types_collected or DataCategory.PHI in data_types_shared
        
        # Calculate risk score
        risk_factors = []
        risk_score = 0.0
        
        if pii_involved:
            risk_score += 0.2
            risk_factors.append("PII data involved")
        
        if phi_involved:
            risk_score += 0.3
            risk_factors.append("PHI data involved")
        
        if cross_border_transfer:
            risk_score += 0.15
            risk_factors.append("Cross-border data transfer")
        
        if automated_decisions:
            risk_score += 0.15
            risk_factors.append("Automated decision-making")
        
        if DataCategory.USER_CONTENT in data_types_collected:
            risk_score += 0.1
            risk_factors.append("User content collection")
        
        if len(data_types_collected) > 5:
            risk_score += 0.1
            risk_factors.append("Large amount of data types collected")
        
        risk_score = min(risk_score, 1.0)
        
        # Generate recommendations
        recommendations = []
        if risk_score > 0.5:
            recommendations.append("Consider reducing data collection scope")
        if pii_involved:
            recommendations.append("Implement enhanced access controls for PII")
        if phi_involved:
            recommendations.append("Ensure HIPAA compliance requirements are met")
        if cross_border_transfer:
            recommendations.append("Verify data transfer mechanisms comply with GDPR")
        if automated_decisions:
            recommendations.append("Implement explainability for automated decisions")
        
        assessment = PrivacyImpactAssessment(
            id=str(uuid.uuid4()),
            workflow_id=workflow_id,
            risk_score=risk_score,
            risk_factors=risk_factors,
            data_types_collected=data_types_collected,
            data_types_shared=data_types_shared,
            pii_involved=pii_involved,
            phi_involved=phi_involved,
            cross_border_transfer=cross_border_transfer,
            automated_decisions=automated_decisions,
            recommendations=recommendations
        )
        
        self._privacy_assessments[workflow_id] = assessment
        
        self._log_audit(
            AuditAction.PRIVACY_ASSESSMENT,
            user_id=None,
            workflow_id=workflow_id,
            resource_type="privacy_assessment",
            resource_id=assessment.id,
            details={"risk_score": risk_score, "risk_factors": risk_factors}
        )
        self._save_data()
        return assessment
    
    def get_privacy_assessment(self, workflow_id: str) -> Optional[PrivacyImpactAssessment]:
        """Get privacy assessment for a workflow."""
        return self._privacy_assessments.get(workflow_id)
    
    def approve_privacy_assessment(
        self,
        workflow_id: str,
        approved_by: str
    ) -> bool:
        """Approve a privacy assessment."""
        assessment = self._privacy_assessments.get(workflow_id)
        if not assessment:
            return False
        
        assessment.approved = True
        assessment.approved_by = approved_by
        self._save_data()
        return True
    
    # =========================================================================
    # Regulatory Reporting
    # =========================================================================
    
    def generate_compliance_report(
        self,
        report_type: str,
        period_start: datetime,
        period_end: datetime
    ) -> ComplianceReport:
        """Generate a compliance report for a regulatory framework."""
        # Filter data for period
        period_violations = [
            v for v in self._violations.values()
            if period_start <= v.detected_at <= period_end
        ]
        
        period_audit = [
            e for e in self._audit_log
            if period_start <= e.timestamp <= period_end
        ]
        
        # Count consent changes
        consent_actions = [
            e for e in period_audit
            if e.action in (AuditAction.CONSENT_GRANTED, AuditAction.CONSENT_WITHDRAWN)
        ]
        
        # Count data access/export
        data_access = [
            e for e in period_audit
            if e.action in (AuditAction.DATA_ACCESSED, AuditAction.DATA_EXPORTED, AuditAction.DATA_DELETED)
        ]
        
        # Generate findings and recommendations based on report type
        findings = []
        recommendations = []
        
        if report_type.upper() == "GDPR":
            # GDPR-specific analysis
            gdpr_violations = [
                v for v in period_violations
                if any(c.name in ["GDPR", "CCPA", "HIPAA", "SOC2", "ISO27001"] for c in ComplianceLevel)
            ]
            if len(gdpr_violations) > 0:
                findings.append(f"Found {len(gdpr_violations)} policy violations during reporting period")
            
            # Check for consent issues
            consent_issues = [c for c in self._consents.values() if c.status == ConsentStatus.WITHDRAWN]
            if consent_issues:
                findings.append(f"Found {len(consent_issues)} withdrawn consents requiring review")
            
            recommendations.extend([
                "Ensure privacy notices are updated for all data processing activities",
                "Review and update consent mechanisms",
                "Implement data protection impact assessments for high-risk processing"
            ])
        
        elif report_type.upper() == "CCPA":
            # CCPA-specific analysis
            if period_violations:
                findings.append(f"Identified {len(period_violations)} potential CCPA violations")
            
            recommendations.extend([
                "Verify do-not-share requests are honored within required timeframe",
                "Update privacy policy to reflect CCPA requirements",
                "Ensure service provider contracts are in place"
            ])
        
        elif report_type.upper() == "HIPAA":
            # HIPAA-specific analysis
            phi_violations = [
                v for v in period_violations
                if v.metadata.get("phi_involved")
            ]
            if phi_violations:
                findings.append(f"Found {len(phi_violations)} violations involving PHI")
            
            recommendations.extend([
                "Review access logs for PHI data",
                "Ensure Business Associate Agreements are current",
                "Verify encryption of PHI at rest and in transit"
            ])
        
        # Count retention actions
        retention_actions = len([
            e for e in period_audit
            if e.action == AuditAction.DATA_RETENTION_APPLIED
        ])
        
        report = ComplianceReport(
            id=str(uuid.uuid4()),
            report_type=report_type,
            generated_at=datetime.utcnow(),
            period_start=period_start,
            period_end=period_end,
            summary={
                "total_violations": len(period_violations),
                "critical_violations": len([v for v in period_violations if v.severity == PolicyViolationSeverity.CRITICAL]),
                "high_violations": len([v for v in period_violations if v.severity == PolicyViolationSeverity.HIGH]),
                "resolved_violations": len([v for v in period_violations if v.resolved])
            },
            violations_count=len(period_violations),
            data_access_count=len(data_access),
            consent_changes=len(consent_actions),
            retention_actions=retention_actions,
            findings=findings,
            recommendations=recommendations
        )
        
        self._log_audit(
            AuditAction.REGULATORY_REPORT_GENERATED,
            user_id=None,
            workflow_id=None,
            resource_type="compliance_report",
            resource_id=report.id,
            details={
                "report_type": report_type,
                "period_start": period_start.isoformat(),
                "period_end": period_end.isoformat()
            },
            compliance_levels=[ComplianceLevel[report_type.upper()]] if report_type.upper() in [c.name for c in ComplianceLevel] else []
        )
        
        return report
    
    # =========================================================================
    # Policy Violations
    # =========================================================================
    
    def get_violation(self, violation_id: str) -> Optional[PolicyViolation]:
        """Get a policy violation by ID."""
        return self._violations.get(violation_id)
    
    def list_violations(
        self,
        severity: Optional[PolicyViolationSeverity] = None,
        resolved: Optional[bool] = None,
        workflow_id: Optional[str] = None,
        limit: int = 1000
    ) -> List[PolicyViolation]:
        """List violations with optional filters."""
        violations = list(self._violations.values())
        
        if severity:
            violations = [v for v in violations if v.severity == severity]
        if resolved is not None:
            violations = [v for v in violations if v.resolved == resolved]
        if workflow_id:
            violations = [v for v in violations if v.workflow_id == workflow_id]
        
        return sorted(violations, key=lambda x: x.detected_at, reverse=True)[:limit]
    
    def resolve_violation(
        self,
        violation_id: str,
        resolved_by: str,
        notes: Optional[str] = None
    ) -> bool:
        """Mark a violation as resolved."""
        violation = self._violations.get(violation_id)
        if not violation:
            return False
        
        violation.resolved = True
        violation.resolved_at = datetime.utcnow()
        violation.resolved_by = resolved_by
        if notes:
            violation.metadata["resolution_notes"] = notes
        
        self._save_data()
        return True
    
    def get_violation_summary(self, days: int = 30) -> Dict[str, Any]:
        """Get summary of violations for the past N days."""
        cutoff = datetime.utcnow() - timedelta(days=days)
        recent = [v for v in self._violations.values() if v.detected_at >= cutoff]
        
        return {
            "total_violations": len(recent),
            "by_severity": {
                s.name: len([v for v in recent if v.severity == s])
                for s in PolicyViolationSeverity
            },
            "resolved_count": len([v for v in recent if v.resolved]),
            "unresolved_count": len([v for v in recent if not v.resolved]),
            "period_days": days
        }
    
    # =========================================================================
    # Compliance Dashboard
    # =========================================================================
    
    def get_compliance_dashboard(self) -> Dict[str, Any]:
        """
        Get compliance dashboard data with overall status.
        """
        now = datetime.utcnow()
        thirty_days_ago = now - timedelta(days=30)
        seven_days_ago = now - timedelta(days=7)
        
        # Policy stats
        total_policies = len(self._policies)
        enabled_policies = len([p for p in self._policies.values() if p.enabled])
        
        # Violation stats
        recent_violations = [v for v in self._violations.values() if v.detected_at >= thirty_days_ago]
        unresolved_violations = [v for v in recent_violations if not v.resolved]
        critical_violations = [v for v in recent_violations if v.severity == PolicyViolationSeverity.CRITICAL]
        
        # Audit stats
        recent_audit = [e for e in self._audit_log if e.timestamp >= thirty_days_ago]
        
        # Consent stats
        total_consents = sum(len(c) for c in self._consents.values())
        active_consents = sum(
            len([c for c in consents if c.status == ConsentStatus.GRANTED])
            for consents in self._consents.values()
        )
        
        # Privacy assessments
        pending_assessments = [
            a for a in self._privacy_assessments.values() if not a.approved
        ]
        high_risk_assessments = [
            a for a in self._privacy_assessments.values() if a.risk_score > 0.6
        ]
        
        # Determine overall status
        if critical_violations:
            overall_status = "CRITICAL"
        elif len(unresolved_violations) > 10:
            overall_status = "AT_RISK"
        elif len(pending_assessments) > 5:
            overall_status = "ATTENTION_NEEDED"
        else:
            overall_status = "COMPLIANT"
        
        return {
            "overall_status": overall_status,
            "policies": {
                "total": total_policies,
                "enabled": enabled_policies,
                "disabled": total_policies - enabled_policies
            },
            "violations": {
                "total_last_30_days": len(recent_violations),
                "unresolved": len(unresolved_violations),
                "critical": len(critical_violations),
                "resolved": len(recent_violations) - len(unresolved_violations)
            },
            "audit": {
                "entries_last_30_days": len(recent_audit),
                "entries_last_7_days": len([e for e in self._audit_log if e.timestamp >= seven_days_ago])
            },
            "consents": {
                "total": total_consents,
                "active": active_consents,
                "users_with_consents": len(self._consents)
            },
            "privacy": {
                "total_assessments": len(self._privacy_assessments),
                "pending_approval": len(pending_assessments),
                "high_risk": len(high_risk_assessments)
            },
            "data_residency": {
                "workflows_with_residency": len(self._workflow_data_locations),
                "regions": {
                    r.name: len([w for w, reg in self._workflow_data_locations.items() if reg == r])
                    for r in DataResidencyRegion
                }
            },
            "generated_at": now.isoformat()
        }
    
    # =========================================================================
    # Persistence
    # =========================================================================
    
    def _get_storage_path(self, name: str) -> str:
        """Get path for storage file."""
        return os.path.join(self.storage_path, f"{name}.json")
    
    def _save_data(self):
        """Save all state to disk."""
        try:
            # Save policies
            with open(self._get_storage_path("policies"), "w") as f:
                policies_data = [
                    {**vars(p), "created_at": p.created_at.isoformat(), "updated_at": p.updated_at.isoformat()}
                    for p in self._policies.values()
                ]
                json.dump(policies_data, f, indent=2)
            
            # Save violations
            with open(self._get_storage_path("violations"), "w") as f:
                violations_data = []
                for v in self._violations.values():
                    vd = {**vars(v), "detected_at": v.detected_at.isoformat()}
                    if v.resolved_at:
                        vd["resolved_at"] = v.resolved_at.isoformat()
                    violations_data.append(vd)
                json.dump(violations_data, f, indent=2)
            
            # Save consents
            with open(self._get_storage_path("consents"), "w") as f:
                consents_data = {}
                for uid, consents in self._consents.items():
                    consents_data[uid] = []
                    for c in consents:
                        cd = {**vars(c)}
                        if cd["granted_at"]:
                            cd["granted_at"] = cd["granted_at"].isoformat()
                        if cd["withdrawn_at"]:
                            cd["withdrawn_at"] = cd["withdrawn_at"].isoformat()
                        if cd["expires_at"]:
                            cd["expires_at"] = cd["expires_at"].isoformat()
                        consents_data[uid].append(cd)
                json.dump(consents_data, f, indent=2)
            
            # Save retention rules
            with open(self._get_storage_path("retention_rules"), "w") as f:
                rules_data = [
                    {**vars(r), "created_at": r.created_at.isoformat()}
                    for r in self._retention_rules.values()
                ]
                json.dump(rules_data, f, indent=2)
            
            # Save user roles
            with open(self._get_storage_path("user_roles"), "w") as f:
                roles_data = {uid: role.name for uid, role in self._user_roles.items()}
                json.dump(roles_data, f, indent=2)
            
            # Save workflow data residency
            with open(self._get_storage_path("data_residency"), "w") as f:
                residency_data = {wid: reg.name for wid, reg in self._workflow_data_locations.items()}
                json.dump(residency_data, f, indent=2)
            
            # Save audit log (last 10000 entries)
            with open(self._get_storage_path("audit_log"), "w") as f:
                audit_data = []
                for e in self._audit_log[-10000:]:
                    ed = {
                        "id": e.id,
                        "timestamp": e.timestamp.isoformat(),
                        "action": e.action.name,
                        "user_id": e.user_id,
                        "workflow_id": e.workflow_id,
                        "resource_type": e.resource_type,
                        "resource_id": e.resource_id,
                        "details": e.details,
                        "ip_address": e.ip_address,
                        "compliance_levels": [c.name for c in e.compliance_levels]
                    }
                    audit_data.append(ed)
                json.dump(audit_data, f, indent=2)
            
        except Exception as e:
            self.logger.error(f"Failed to save compliance data: {e}")
    
    def _load_data(self):
        """Load state from disk."""
        try:
            # Load policies
            path = self._get_storage_path("policies")
            if os.path.exists(path):
                with open(path) as f:
                    policies_data = json.load(f)
                    for pd in policies_data:
                        pd["created_at"] = datetime.fromisoformat(pd["created_at"])
                        pd["updated_at"] = datetime.fromisoformat(pd["updated_at"])
                        pd["compliance_levels"] = [ComplianceLevel[c] for c in pd["compliance_levels"]]
                        self._policies[pd["id"]] = Policy(**pd)
            
            # Load violations
            path = self._get_storage_path("violations")
            if os.path.exists(path):
                with open(path) as f:
                    violations_data = json.load(f)
                    for vd in violations_data:
                        vd["detected_at"] = datetime.fromisoformat(vd["detected_at"])
                        if vd.get("resolved_at"):
                            vd["resolved_at"] = datetime.fromisoformat(vd["resolved_at"])
                        self._violations[vd["id"]] = PolicyViolation(**vd)
            
            # Load consents
            path = self._get_storage_path("consents")
            if os.path.exists(path):
                with open(path) as f:
                    consents_data = json.load(f)
                    for uid, consents in consents_data.items():
                        self._consents[uid] = []
                        for cd in consents:
                            if cd.get("granted_at"):
                                cd["granted_at"] = datetime.fromisoformat(cd["granted_at"])
                            if cd.get("withdrawn_at"):
                                cd["withdrawn_at"] = datetime.fromisoformat(cd["withdrawn_at"])
                            if cd.get("expires_at"):
                                cd["expires_at"] = datetime.fromisoformat(cd["expires_at"])
                            cd["status"] = ConsentStatus[cd["status"]]
                            self._consents[uid].append(UserConsent(**cd))
            
            # Load retention rules
            path = self._get_storage_path("retention_rules")
            if os.path.exists(path):
                with open(path) as f:
                    rules_data = json.load(f)
                    for rd in rules_data:
                        rd["created_at"] = datetime.fromisoformat(rd["created_at"])
                        rd["data_type"] = DataCategory[rd["data_type"]]
                        rd["retention_policy"] = RetentionPolicy[rd["retention_period"]]
                        self._retention_rules[rd["id"]] = DataRetentionRule(**rd)
            
            # Load user roles
            path = self._get_storage_path("user_roles")
            if os.path.exists(path):
                with open(path) as f:
                    roles_data = json.load(f)
                    for uid, role_name in roles_data.items():
                        self._user_roles[uid] = UserRole[role_name]
            
            # Load data residency
            path = self._get_storage_path("data_residency")
            if os.path.exists(path):
                with open(path) as f:
                    residency_data = json.load(f)
                    for wid, reg_name in residency_data.items():
                        self._workflow_data_locations[wid] = DataResidencyRegion[reg_name]
            
            # Load audit log
            path = self._get_storage_path("audit_log")
            if os.path.exists(path):
                with open(path) as f:
                    audit_data = json.load(f)
                    for ed in audit_data:
                        ed["timestamp"] = datetime.fromisoformat(ed["timestamp"])
                        ed["action"] = AuditAction[ed["action"]]
                        ed["compliance_levels"] = [ComplianceLevel[c] for c in ed.get("compliance_levels", [])]
                        self._audit_log.append(AuditEntry(**ed))
            
        except Exception as e:
            self.logger.error(f"Failed to load compliance data: {e}")
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def reset(self):
        """Reset all compliance data."""
        self._policies.clear()
        self._violations.clear()
        self._audit_log.clear()
        self._consents.clear()
        self._retention_rules.clear()
        self._privacy_assessments.clear()
        self._user_roles.clear()
        self._data_residency_rules.clear()
        self._workflow_data_locations.clear()
        self._save_data()
    
    def export_compliance_data(self, filepath: str) -> bool:
        """Export all compliance data to a file."""
        try:
            data = {
                "policies": [vars(p) for p in self._policies.values()],
                "violations": [vars(v) for v in self._violations.values()],
                "consents": {uid: [vars(c) for c in consents] for uid, consents in self._consents.items()},
                "retention_rules": [vars(r) for r in self._retention_rules.values()],
                "user_roles": {uid: r.name for uid, r in self._user_roles.items()},
                "data_residency": {wid: r.name for wid, r in self._workflow_data_locations.items()},
                "exported_at": datetime.utcnow().isoformat()
            }
            
            with open(filepath, "w") as f:
                json.dump(data, f, indent=2, default=str)
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to export compliance data: {e}")
            return False


# ============================================================================
# Module Exports
# ============================================================================

__all__ = [
    "ComplianceManager",
    "ComplianceLevel",
    "DataCategory",
    "DataResidencyRegion",
    "RetentionPolicy",
    "UserRole",
    "PolicyViolationSeverity",
    "ConsentStatus",
    "AuditAction",
    "Policy",
    "PolicyViolation",
    "AuditEntry",
    "UserConsent",
    "DataRetentionRule",
    "PrivacyImpactAssessment",
    "ComplianceReport",
    "RolePermission"
]
