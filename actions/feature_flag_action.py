"""Feature flag action module for RabAI AutoClick.

Provides feature flag operations:
- FeatureFlagGetAction: Get feature flag status
- FeatureFlagSetAction: Set feature flag
- FeatureFlagToggleAction: Toggle feature flag
- FeatureFlagListAction: List all feature flags
- FeatureFlagDeleteAction: Delete feature flag
- FeatureFlagEvaluateAction: Evaluate flag for user
- FeatureFlagABTestAction: A/B test with flags
- FeatureFlagGradualRolloutAction: Gradual rollout
"""

import os
import random
import sys
import time
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FeatureFlagStore:
    """In-memory feature flag storage."""
    
    _flags: Dict[str, Dict[str, Any]] = {}
    
    @classmethod
    def set(cls, name: str, enabled: bool, metadata: Dict[str, Any] = None) -> None:
        if name not in cls._flags:
            cls._flags[name] = {
                "name": name,
                "created_at": time.time(),
                "history": []
            }
        cls._flags[name]["enabled"] = enabled
        cls._flags[name]["updated_at"] = time.time()
        if metadata:
            cls._flags[name]["metadata"] = metadata
    
    @classmethod
    def get(cls, name: str) -> Optional[Dict[str, Any]]:
        return cls._flags.get(name)
    
    @classmethod
    def list_all(cls) -> List[Dict[str, Any]]:
        return list(cls._flags.values())
    
    @classmethod
    def delete(cls, name: str) -> bool:
        if name in cls._flags:
            del cls._flags[name]
            return True
        return False
    
    @classmethod
    def is_enabled(cls, name: str) -> bool:
        flag = cls._flags.get(name)
        return flag.get("enabled", False) if flag else False


class FeatureFlagGetAction(BaseAction):
    """Get feature flag status."""
    action_type = "feature_flag_get"
    display_name = "获取特性开关"
    description = "获取特性开关状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            
            if not name:
                return ActionResult(success=False, message="name required")
            
            flag = FeatureFlagStore.get(name)
            
            if not flag:
                return ActionResult(success=False, message=f"Flag not found: {name}")
            
            return ActionResult(
                success=True,
                message=f"Flag '{name}': {'enabled' if flag['enabled'] else 'disabled'}",
                data={"flag": flag}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Feature flag get failed: {str(e)}")


class FeatureFlagSetAction(BaseAction):
    """Set feature flag."""
    action_type = "feature_flag_set"
    display_name = "设置特性开关"
    description = "设置特性开关"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            enabled = params.get("enabled", False)
            metadata = params.get("metadata", {})
            
            if not name:
                return ActionResult(success=False, message="name required")
            
            FeatureFlagStore.set(name, enabled, metadata)
            flag = FeatureFlagStore.get(name)
            
            return ActionResult(
                success=True,
                message=f"Set flag '{name}': {enabled}",
                data={"flag": flag}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Feature flag set failed: {str(e)}")


class FeatureFlagToggleAction(BaseAction):
    """Toggle feature flag."""
    action_type = "feature_flag_toggle"
    display_name = "切换特性开关"
    description = "切换特性开关"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            
            if not name:
                return ActionResult(success=False, message="name required")
            
            flag = FeatureFlagStore.get(name)
            if not flag:
                return ActionResult(success=False, message=f"Flag not found: {name}")
            
            new_state = not flag["enabled"]
            FeatureFlagStore.set(name, new_state)
            
            return ActionResult(
                success=True,
                message=f"Toggled flag '{name}': {new_state}",
                data={"name": name, "enabled": new_state}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Feature flag toggle failed: {str(e)}")


class FeatureFlagListAction(BaseAction):
    """List all feature flags."""
    action_type = "feature_flag_list"
    display_name = "特性开关列表"
    description = "列出所有特性开关"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            flags = FeatureFlagStore.list_all()
            
            enabled = [f for f in flags if f.get("enabled")]
            disabled = [f for f in flags if not f.get("enabled")]
            
            return ActionResult(
                success=True,
                message=f"Feature flags: {len(enabled)} enabled, {len(disabled)} disabled",
                data={
                    "flags": flags,
                    "enabled_count": len(enabled),
                    "disabled_count": len(disabled)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Feature flag list failed: {str(e)}")


class FeatureFlagDeleteAction(BaseAction):
    """Delete feature flag."""
    action_type = "feature_flag_delete"
    display_name = "删除特性开关"
    description = "删除特性开关"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            
            if not name:
                return ActionResult(success=False, message="name required")
            
            deleted = FeatureFlagStore.delete(name)
            
            return ActionResult(
                success=deleted,
                message=f"Deleted flag: {name}" if deleted else f"Flag not found: {name}",
                data={"name": name, "deleted": deleted}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Feature flag delete failed: {str(e)}")


class FeatureFlagEvaluateAction(BaseAction):
    """Evaluate flag for user."""
    action_type = "feature_flag_evaluate"
    display_name = "评估特性开关"
    description = "为用户评估特性开关"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            user_id = params.get("user_id", "")
            
            if not name:
                return ActionResult(success=False, message="name required")
            
            flag = FeatureFlagStore.get(name)
            if not flag:
                return ActionResult(success=False, message=f"Flag not found: {name}")
            
            enabled = flag.get("enabled", False)
            
            if user_id and "rollout_percentage" in flag.get("metadata", {}):
                percentage = flag["metadata"]["rollout_percentage"]
                hash_val = hash(f"{name}:{user_id}") % 100
                enabled = hash_val < percentage
            
            return ActionResult(
                success=True,
                message=f"Flag '{name}' for user '{user_id}': {'enabled' if enabled else 'disabled'}",
                data={"name": name, "user_id": user_id, "enabled": enabled}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Feature flag evaluate failed: {str(e)}")


class FeatureFlagABTestAction(BaseAction):
    """A/B test with flags."""
    action_type = "feature_flag_abtest"
    display_name = "AB测试"
    description = "特性开关AB测试"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            flag_a = params.get("flag_a", "")
            flag_b = params.get("flag_b", "")
            user_id = params.get("user_id", "")
            
            if not user_id:
                user_id = str(random.randint(1000, 9999))
            
            hash_val = hash(user_id) % 100
            variant = "A" if hash_val < 50 else "B"
            
            return ActionResult(
                success=True,
                message=f"A/B test variant: {variant} for user {user_id}",
                data={
                    "user_id": user_id,
                    "variant": variant,
                    "flag_a": flag_a,
                    "flag_b": flag_b
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Feature flag AB test failed: {str(e)}")


class FeatureFlagGradualRolloutAction(BaseAction):
    """Gradual rollout."""
    action_type = "feature_flag_rollout"
    display_name = "渐进式发布"
    description = "特性开关渐进式发布"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            percentage = params.get("percentage", 10)
            
            if not name:
                return ActionResult(success=False, message="name required")
            
            if percentage < 0 or percentage > 100:
                return ActionResult(success=False, message="Percentage must be 0-100")
            
            metadata = {"rollout_percentage": percentage}
            FeatureFlagStore.set(name, percentage > 0, metadata)
            
            return ActionResult(
                success=True,
                message=f"Gradual rollout for '{name}': {percentage}%",
                data={"name": name, "percentage": percentage}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Feature flag rollout failed: {str(e)}")
