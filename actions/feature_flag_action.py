"""Feature Flag action module for RabAI AutoClick.

Provides feature flag evaluation with support for boolean, percentage,
user targeting, and multi-variant flags. Integrates with common
feature flag providers.
"""

import sys
import os
import hashlib
import json
import time
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class FeatureFlag:
    """Represents a feature flag configuration."""
    name: str
    flag_type: str = "boolean"  # boolean, percentage, variant, user_targeting
    enabled: bool = False
    default_value: Any = False
    percentage: float = 0.0  # 0.0 to 100.0
    variants: Dict[str, Any] = field(default_factory=dict)
    target_users: List[str] = field(default_factory=list)
    target_attributes: Dict[str, Any] = field(default_factory=dict)
    rollout_rules: List[Dict[str, Any]] = field(default_factory=list)
    description: str = ""


class FeatureFlagStore:
    """In-memory feature flag store with optional persistence."""
    
    def __init__(self, persistence_path: Optional[str] = None):
        self._flags: Dict[str, FeatureFlag] = {}
        self._evaluation_cache: Dict[str, tuple[Any, float]] = {}
        self._cache_ttl: float = 60.0  # seconds
        self._persistence_path = persistence_path
        self._load()
    
    def _load(self) -> None:
        """Load flags from persistence file if available."""
        if self._persistence_path and os.path.exists(self._persistence_path):
            try:
                with open(self._persistence_path, 'r') as f:
                    data = json.load(f)
                    for name, flag_data in data.items():
                        self._flags[name] = FeatureFlag(**flag_data)
            except (json.JSONDecodeError, TypeError):
                pass
    
    def _persist(self) -> None:
        """Save flags to persistence file."""
        if self._persistence_path:
            try:
                data = {name: vars(flag) for name, flag in self._flags.items()}
                with open(self._persistence_path, 'w') as f:
                    json.dump(data, f, indent=2)
            except OSError:
                pass
    
    def add_flag(self, flag: FeatureFlag) -> None:
        """Add or update a feature flag."""
        self._flags[flag.name] = flag
        self._evaluation_cache.clear()
        self._persist()
    
    def remove_flag(self, name: str) -> bool:
        """Remove a feature flag by name."""
        if name in self._flags:
            del self._flags[name]
            self._evaluation_cache.clear()
            self._persist()
            return True
        return False
    
    def get_flag(self, name: str) -> Optional[FeatureFlag]:
        """Get a feature flag by name."""
        return self._flags.get(name)
    
    def list_flags(self) -> List[str]:
        """List all feature flag names."""
        return list(self._flags.keys())
    
    def _get_cache_key(self, flag_name: str, user_id: Optional[str], 
                       context: Dict[str, Any]) -> str:
        """Generate cache key for evaluation result."""
        key_parts = [flag_name]
        if user_id:
            key_parts.append(user_id)
        context_str = json.dumps(context, sort_keys=True)
        key_parts.append(hashlib.md5(context_str.encode()).hexdigest()[:8])
        return "|".join(key_parts)
    
    def _hash_variation(self, flag_name: str, user_id: str, 
                        variation_index: int) -> float:
        """Generate a deterministic hash for percentage rollout."""
        hash_input = f"{flag_name}:{user_id}:{variation_index}"
        hash_value = hashlib.md5(hash_input.encode()).hexdigest()
        return int(hash_value[:8], 16) / 0xFFFFFFFFFFFFF
    
    def evaluate(self, flag_name: str, user_id: Optional[str] = None,
                 context: Optional[Dict[str, Any]] = None) -> Any:
        """Evaluate a feature flag for a given user and context.
        
        Args:
            flag_name: Name of the feature flag.
            user_id: Optional user identifier for targeting.
            context: Additional context attributes for evaluation.
        
        Returns:
            The evaluated flag value (bool, str, or any configured value).
        """
        context = context or {}
        cache_key = self._get_cache_key(flag_name, user_id, context)
        
        # Check cache
        if cache_key in self._evaluation_cache:
            cached_value, cached_time = self._evaluation_cache[cache_key]
            if time.time() - cached_time < self._cache_ttl:
                return cached_value
        
        # Get flag
        flag = self._flags.get(flag_name)
        if not flag:
            return flag.default_value if flag else False
        
        # Evaluate based on flag type
        result = self._evaluate_flag(flag, user_id, context)
        
        # Cache result
        self._evaluation_cache[cache_key] = (result, time.time())
        return result
    
    def _evaluate_flag(self, flag: FeatureFlag, user_id: Optional[str],
                      context: Dict[str, Any]) -> Any:
        """Internal flag evaluation logic."""
        # Check if flag is globally disabled
        if not flag.enabled:
            return flag.default_value
        
        # User targeting check
        if flag.target_users and user_id in flag.target_users:
            return True
        
        # Attribute-based targeting
        if flag.target_attributes:
            match = all(
                context.get(k) == v 
                for k, v in flag.target_attributes.items()
            )
            if match:
                return True
        
        # Rollout rules evaluation
        for rule in flag.rollout_rules:
            if self._matches_rule(rule, user_id, context):
                return rule.get("value", flag.default_value)
        
        # Percentage-based rollout
        if flag.flag_type == "percentage" and user_id:
            hash_val = self._hash_variation(flag.name, user_id, 0)
            threshold = flag.percentage / 100.0
            return hash_val < threshold
        
        # Variant flag
        if flag.flag_type == "variant" and flag.variants:
            if user_id:
                hash_val = self._hash_variation(flag.name, user_id, 0)
                variant_names = list(flag.variants.keys())
                index = int(hash_val * len(variant_names)) % len(variant_names)
                return variant_names[index]
            return list(flag.variants.keys())[0] if flag.variants else None
        
        # Default boolean evaluation
        return True
    
    def _matches_rule(self, rule: Dict[str, Any], user_id: Optional[str],
                     context: Dict[str, Any]) -> bool:
        """Check if a rollout rule matches the given context."""
        rule_context = rule.get("context", {})
        operator = rule.get("operator", "equals")
        
        for key, expected in rule_context.items():
            actual = context.get(key)
            if operator == "equals" and actual != expected:
                return False
            elif operator == "not_equals" and actual == expected:
                return False
            elif operator == "contains" and expected not in str(actual):
                return False
            elif operator == "in" and actual not in expected:
                return False
        return True
    
    def clear_cache(self) -> None:
        """Clear the evaluation cache."""
        self._evaluation_cache.clear()


class FeatureFlagAction(BaseAction):
    """Evaluate and manage feature flags.
    
    Supports boolean, percentage, variant, and user-targeted flags.
    Provides flag management (add, remove, list) and evaluation.
    """
    action_type = "feature_flag"
    display_name = "功能开关"
    description = "评估和管理功能开关，支持多种开关类型"
    
    def __init__(self):
        super().__init__()
        self._store: Optional[FeatureFlagStore] = None
    
    def _get_store(self, params: Dict[str, Any]) -> FeatureFlagStore:
        """Get or create the feature flag store."""
        if self._store is None:
            persistence_path = params.get("persistence_path")
            self._store = FeatureFlagStore(persistence_path)
        return self._store
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute feature flag operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: "evaluate", "add", "remove", "list", "clear_cache"
                - For evaluate: flag_name, user_id, context
                - For add: flag (FeatureFlag dict)
                - For remove: flag_name
                - For list: none
        
        Returns:
            ActionResult with evaluation result or operation status.
        """
        operation = params.get("operation", "evaluate")
        
        try:
            if operation == "evaluate":
                return self._evaluate(params)
            elif operation == "add":
                return self._add_flag(params)
            elif operation == "remove":
                return self._remove_flag(params)
            elif operation == "list":
                return self._list_flags(params)
            elif operation == "clear_cache":
                return self._clear_cache(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Feature flag error: {str(e)}")
    
    def _evaluate(self, params: Dict[str, Any]) -> ActionResult:
        """Evaluate a feature flag."""
        store = self._get_store(params)
        flag_name = params.get("flag_name", "")
        user_id = params.get("user_id")
        eval_context = params.get("context", {})
        
        if not flag_name:
            return ActionResult(success=False, message="flag_name is required")
        
        result = store.evaluate(flag_name, user_id, eval_context)
        return ActionResult(
            success=True,
            message=f"Flag '{flag_name}' evaluated",
            data={"flag_name": flag_name, "value": result}
        )
    
    def _add_flag(self, params: Dict[str, Any]) -> ActionResult:
        """Add a new feature flag."""
        store = self._get_store(params)
        flag_data = params.get("flag", {})
        
        if not flag_data or "name" not in flag_data:
            return ActionResult(success=False, message="flag with name is required")
        
        flag = FeatureFlag(**flag_data)
        store.add_flag(flag)
        return ActionResult(
            success=True,
            message=f"Flag '{flag.name}' added",
            data={"name": flag.name, "type": flag.flag_type}
        )
    
    def _remove_flag(self, params: Dict[str, Any]) -> ActionResult:
        """Remove a feature flag."""
        store = self._get_store(params)
        flag_name = params.get("flag_name", "")
        
        if not flag_name:
            return ActionResult(success=False, message="flag_name is required")
        
        removed = store.remove_flag(flag_name)
        if removed:
            return ActionResult(success=True, message=f"Flag '{flag_name}' removed")
        return ActionResult(success=False, message=f"Flag '{flag_name}' not found")
    
    def _list_flags(self, params: Dict[str, Any]) -> ActionResult:
        """List all feature flags."""
        store = self._get_store(params)
        flags = store.list_flags()
        return ActionResult(
            success=True,
            message=f"Found {len(flags)} flags",
            data={"flags": flags, "count": len(flags)}
        )
    
    def _clear_cache(self, params: Dict[str, Any]) -> ActionResult:
        """Clear the evaluation cache."""
        store = self._get_store(params)
        store.clear_cache()
        return ActionResult(success=True, message="Cache cleared")
