"""API cache invalidation action module for RabAI AutoClick.

Provides cache invalidation operations:
- CacheInvalidateAction: Invalidate cache entries
- CacheInvalidatePatternAction: Invalidate by pattern
- CacheInvalidateTagAction: Invalidate by tag
- CacheInvalidateAllAction: Invalidate all entries
"""

import time
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CacheInvalidateAction(BaseAction):
    """Invalidate specific cache entries."""
    action_type = "cache_invalidate"
    display_name = "缓存失效"
    description = "使指定缓存条目失效"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            keys = params.get("keys", [])
            reason = params.get("reason", "")

            if not keys:
                return ActionResult(success=False, message="keys is required")

            if not hasattr(context, "api_cache"):
                context.api_cache = {}

            invalidated = 0
            for key in keys:
                if key in context.api_cache:
                    del context.api_cache[key]
                    invalidated += 1

            return ActionResult(
                success=True,
                data={"invalidated_count": invalidated, "reason": reason},
                message=f"Invalidated {invalidated}/{len(keys)} cache entries",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cache invalidate failed: {e}")


class CacheInvalidatePatternAction(BaseAction):
    """Invalidate cache entries matching pattern."""
    action_type = "cache_invalidate_pattern"
    display_name = "模式缓存失效"
    description = "按模式匹配失效缓存"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pattern = params.get("pattern", "")
            if not pattern:
                return ActionResult(success=False, message="pattern is required")

            if not hasattr(context, "api_cache"):
                context.api_cache = {}

            keys_to_delete = [k for k in context.api_cache if pattern in k]
            for key in keys_to_delete:
                del context.api_cache[key]

            return ActionResult(
                success=True,
                data={"pattern": pattern, "invalidated_count": len(keys_to_delete)},
                message=f"Invalidated {len(keys_to_delete)} entries matching '{pattern}'",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cache invalidate pattern failed: {e}")


class CacheInvalidateTagAction(BaseAction):
    """Invalidate cache entries by tag."""
    action_type = "cache_invalidate_tag"
    display_name = "标签缓存失效"
    description = "按标签失效缓存"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            tag = params.get("tag", "")
            if not tag:
                return ActionResult(success=False, message="tag is required")

            if not hasattr(context, "cache_tags"):
                context.cache_tags = {}

            keys = context.cache_tags.get(tag, [])
            for key in keys:
                if hasattr(context, "api_cache") and key in context.api_cache:
                    del context.api_cache[key]

            count = len(keys)
            context.cache_tags[tag] = []

            return ActionResult(
                success=True,
                data={"tag": tag, "invalidated_count": count},
                message=f"Invalidated {count} entries with tag '{tag}'",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cache invalidate tag failed: {e}")


class CacheInvalidateAllAction(BaseAction):
    """Invalidate all cache entries."""
    action_type = "cache_invalidate_all"
    display_name = "全部缓存失效"
    description = "使所有缓存条目失效"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            reason = params.get("reason", "manual_invalidation")

            if not hasattr(context, "api_cache"):
                context.api_cache = {}

            count = len(context.api_cache)
            context.api_cache.clear()

            return ActionResult(
                success=True,
                data={"invalidated_count": count, "reason": reason},
                message=f"Invalidated all {count} cache entries",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cache invalidate all failed: {e}")
