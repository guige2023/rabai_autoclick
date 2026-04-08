"""Feature flag action module for RabAI AutoClick.

Provides feature flag management with targeting rules,
percentage rollouts, and real-time updates.
"""

import sys
import os
import time
import hashlib
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from threading import Lock
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class FlagStatus(Enum):
    """Feature flag status."""
    ENABLED = "enabled"
    DISABLED = "disabled"
    ROLLING_OUT = "rolling_out"


@dataclass
class TargetingRule:
    """A targeting rule for feature flag."""
    attribute: str
    operator: str  # eq, ne, gt, lt, in, not_in, contains
    value: Any
    percentage: int = 100  # 0-100


@dataclass
class FeatureFlag:
    """A feature flag definition."""
    key: str
    name: str
    description: str = ""
    status: FlagStatus = FlagStatus.DISABLED
    default_value: Any = False
    rules: List[TargetingRule] = field(default_factory=list)
    percentage: int = 0  # Global rollout percentage
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


class FeatureFlagAction(BaseAction):
    """Manage feature flags with targeting and rollouts.
    
    Supports boolean flags, percentage rollouts, user targeting,
    and real-time flag updates.
    """
    action_type = "feature_flag"
    display_name = "特性开关"
    description = "特性开关管理和灰度发布"
    
    def __init__(self):
        super().__init__()
        self._flags: Dict[str, FeatureFlag] = {}
        self._lock = Lock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute feature flag operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'get', 'create', 'update', 'delete', 'list', 'toggle'
                - key: Flag key
                - flag: Flag config (for create/update)
                - user: User context for targeting
        
        Returns:
            ActionResult with flag evaluation result.
        """
        operation = params.get('operation', 'get').lower()
        
        if operation == 'get':
            return self._get_flag(params)
        elif operation == 'create':
            return self._create_flag(params)
        elif operation == 'update':
            return self._update_flag(params)
        elif operation == 'delete':
            return self._delete_flag(params)
        elif operation == 'list':
            return self._list_flags(params)
        elif operation == 'toggle':
            return self._toggle_flag(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _create_flag(self, params: Dict[str, Any]) -> ActionResult:
        """Create a new feature flag."""
        key = params.get('key')
        name = params.get('name', key)
        description = params.get('description', '')
        default_value = params.get('default_value', False)
        percentage = params.get('percentage', 0)
        rules = params.get('rules', [])
        
        if not key:
            return ActionResult(success=False, message="key is required")
        
        with self._lock:
            if key in self._flags:
                return ActionResult(
                    success=False,
                    message=f"Flag '{key}' already exists"
                )
            
            flag = FeatureFlag(
                key=key,
                name=name,
                description=description,
                default_value=default_value,
                percentage=percentage,
                rules=[
                    TargetingRule(
                        attribute=r.get('attribute'),
                        operator=r.get('operator', 'eq'),
                        value=r.get('value'),
                        percentage=r.get('percentage', 100)
                    )
                    for r in rules
                ]
            )
            
            self._flags[key] = flag
        
        return ActionResult(
            success=True,
            message=f"Created flag '{key}'",
            data={'key': key, 'name': name}
        )
    
    def _get_flag(self, params: Dict[str, Any]) -> ActionResult:
        """Evaluate a feature flag."""
        key = params.get('key')
        user = params.get('user', {})
        
        if not key:
            return ActionResult(success=False, message="key is required")
        
        with self._lock:
            if key not in self._flags:
                return ActionResult(
                    success=True,
                    message=f"Flag '{key}' not found, returning default",
                    data={'key': key, 'enabled': False, 'value': False, 'source': 'default'}
                )
            
            flag = self._flags[key]
        
        # Evaluate flag
        enabled = self._evaluate_flag(flag, user)
        value = flag.default_value if enabled else False
        
        return ActionResult(
            success=True,
            message=f"Flag '{key}' is {'enabled' if enabled else 'disabled'}",
            data={
                'key': key,
                'enabled': enabled,
                'value': value,
                'source': 'rule' if enabled else 'disabled'
            }
        )
    
    def _evaluate_flag(self, flag: FeatureFlag, user: Dict[str, Any]) -> bool:
        """Evaluate flag for a user."""
        # Check if flag is enabled at all
        if flag.status == FlagStatus.DISABLED:
            return False
        
        if flag.status == FlagStatus.ENABLED:
            # No rules, check percentage only
            if not flag.rules and flag.percentage > 0:
                return self._check_percentage(flag.key, user, flag.percentage)
            elif not flag.rules:
                return True
        
        # Evaluate targeting rules
        for rule in flag.rules:
            if self._check_rule(rule, user):
                # Check percentage for this rule
                return self._check_percentage(flag.key, user, rule.percentage)
        
        # Check global percentage
        if flag.percentage > 0:
            return self._check_percentage(flag.key, user, flag.percentage)
        
        return False
    
    def _check_rule(self, rule: TargetingRule, user: Dict[str, Any]) -> bool:
        """Check if user matches targeting rule."""
        user_value = user.get(rule.attribute)
        
        if user_value is None:
            return False
        
        operator = rule.operator
        value = rule.value
        
        if operator == 'eq':
            return user_value == value
        elif operator == 'ne':
            return user_value != value
        elif operator == 'gt':
            return user_value > value
        elif operator == 'lt':
            return user_value < value
        elif operator == 'gte':
            return user_value >= value
        elif operator == 'lte':
            return user_value <= value
        elif operator == 'in':
            return user_value in (value if isinstance(value, list) else [value])
        elif operator == 'not_in':
            return user_value not in (value if isinstance(value, list) else [value])
        elif operator == 'contains':
            return value in user_value
        
        return False
    
    def _check_percentage(self, key: str, user: Dict[str, Any], percentage: int) -> bool:
        """Check if user falls within percentage rollout."""
        if percentage >= 100:
            return True
        if percentage <= 0:
            return False
        
        # Use consistent hashing for percentage
        user_id = str(user.get('id', user.get('user_id', 'anonymous')))
        hash_input = f"{key}:{user_id}"
        hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
        bucket = hash_value % 100
        
        return bucket < percentage
    
    def _update_flag(self, params: Dict[str, Any]) -> ActionResult:
        """Update a feature flag."""
        key = params.get('key')
        updates = params.get('flag', {})
        
        if not key:
            return ActionResult(success=False, message="key is required")
        
        with self._lock:
            if key not in self._flags:
                return ActionResult(
                    success=False,
                    message=f"Flag '{key}' not found"
                )
            
            flag = self._flags[key]
            
            # Apply updates
            if 'name' in updates:
                flag.name = updates['name']
            if 'description' in updates:
                flag.description = updates['description']
            if 'default_value' in updates:
                flag.default_value = updates['default_value']
            if 'percentage' in updates:
                flag.percentage = updates['percentage']
            if 'rules' in updates:
                flag.rules = [
                    TargetingRule(
                        attribute=r.get('attribute'),
                        operator=r.get('operator', 'eq'),
                        value=r.get('value'),
                        percentage=r.get('percentage', 100)
                    )
                    for r in updates['rules']
                ]
            
            flag.updated_at = time.time()
        
        return ActionResult(
            success=True,
            message=f"Updated flag '{key}'"
        )
    
    def _delete_flag(self, params: Dict[str, Any]) -> ActionResult:
        """Delete a feature flag."""
        key = params.get('key')
        
        with self._lock:
            if key in self._flags:
                del self._flags[key]
                return ActionResult(
                    success=True,
                    message=f"Deleted flag '{key}'"
                )
        
        return ActionResult(
            success=False,
            message=f"Flag '{key}' not found"
        )
    
    def _list_flags(self, params: Dict[str, Any]) -> ActionResult:
        """List all feature flags."""
        with self._lock:
            flags = [
                {
                    'key': f.key,
                    'name': f.name,
                    'status': f.status.value,
                    'percentage': f.percentage,
                    'rules_count': len(f.rules),
                    'updated_at': f.updated_at
                }
                for f in self._flags.values()
            ]
        
        return ActionResult(
            success=True,
            message=f"{len(flags)} flags",
            data={'flags': flags, 'count': len(flags)}
        )
    
    def _toggle_flag(self, params: Dict[str, Any]) -> ActionResult:
        """Toggle a feature flag on/off."""
        key = params.get('key')
        
        with self._lock:
            if key not in self._flags:
                return ActionResult(
                    success=False,
                    message=f"Flag '{key}' not found"
                )
            
            flag = self._flags[key]
            
            if flag.status == FlagStatus.ENABLED:
                flag.status = FlagStatus.DISABLED
            else:
                flag.status = FlagStatus.ENABLED
            
            flag.updated_at = time.time()
        
        return ActionResult(
            success=True,
            message=f"Flag '{key}' is now {flag.status.value}",
            data={'key': key, 'status': flag.status.value}
        )
