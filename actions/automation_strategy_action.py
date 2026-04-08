"""Automation strategy action module for RabAI AutoClick.

Provides strategy pattern for automation:
- StrategyCreateAction: Create strategy
- StrategySetAction: Set active strategy
- StrategyExecuteAction: Execute with strategy
- StrategyCompareAction: Compare strategies
- StrategyListAction: List available strategies
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StrategyCreateAction(BaseAction):
    """Create a strategy."""
    action_type = "strategy_create"
    display_name = "创建策略"
    description = "创建自动化策略"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            strategy_type = params.get("type", "selection")
            config = params.get("config", {})

            if not name:
                return ActionResult(success=False, message="name is required")

            strategy_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "automation_strategies"):
                context.automation_strategies = {}
            context.automation_strategies[strategy_id] = {
                "strategy_id": strategy_id,
                "name": name,
                "type": strategy_type,
                "config": config,
                "created_at": time.time(),
                "use_count": 0,
            }

            return ActionResult(
                success=True,
                data={"strategy_id": strategy_id, "name": name, "type": strategy_type},
                message=f"Strategy {strategy_id} created: {name}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Strategy create failed: {e}")


class StrategySetAction(BaseAction):
    """Set active strategy."""
    action_type = "strategy_set"
    display_name = "设置策略"
    description = "设置活跃策略"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            strategy_id = params.get("strategy_id", "")
            context_name = params.get("context", "default")

            if not strategy_id:
                return ActionResult(success=False, message="strategy_id is required")

            strategies = getattr(context, "automation_strategies", {})
            if strategy_id not in strategies:
                return ActionResult(success=False, message=f"Strategy {strategy_id} not found")

            if not hasattr(context, "active_strategy"):
                context.active_strategy = {}
            context.active_strategy[context_name] = strategy_id

            return ActionResult(
                success=True,
                data={"strategy_id": strategy_id, "name": strategies[strategy_id]["name"], "context": context_name},
                message=f"Strategy '{strategies[strategy_id]['name']}' set as active for {context_name}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Strategy set failed: {e}")


class StrategyExecuteAction(BaseAction):
    """Execute with active strategy."""
    action_type = "strategy_execute"
    display_name = "执行策略"
    description = "使用策略执行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            context_name = params.get("context", "default")
            task = params.get("task", {})

            active = getattr(context, "active_strategy", {}).get(context_name)
            if not active:
                return ActionResult(success=False, message=f"No active strategy for context '{context_name}'")

            strategies = getattr(context, "automation_strategies", {})
            if active not in strategies:
                return ActionResult(success=False, message=f"Strategy {active} not found")

            strategy = strategies[active]
            strategy["use_count"] += 1

            return ActionResult(
                success=True,
                data={"strategy_id": active, "strategy_name": strategy["name"], "task": task},
                message=f"Executed with strategy '{strategy['name']}'",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Strategy execute failed: {e}")


class StrategyCompareAction(BaseAction):
    """Compare strategies."""
    action_type = "strategy_compare"
    display_name = "策略对比"
    description = "对比策略"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            strategy_ids = params.get("strategy_ids", [])

            if len(strategy_ids) < 2:
                return ActionResult(success=False, message="at least 2 strategy_ids required")

            strategies = getattr(context, "automation_strategies", {})
            comparison = []
            for sid in strategy_ids:
                if sid in strategies:
                    s = strategies[sid]
                    comparison.append({"strategy_id": sid, "name": s["name"], "use_count": s.get("use_count", 0)})

            return ActionResult(
                success=True,
                data={"comparison": comparison, "count": len(comparison)},
                message=f"Compared {len(comparison)} strategies",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Strategy compare failed: {e}")


class StrategyListAction(BaseAction):
    """List available strategies."""
    action_type = "strategy_list"
    display_name = "策略列表"
    description = "列出可用策略"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            strategy_type = params.get("type", "")

            strategies = getattr(context, "automation_strategies", {})
            results = list(strategies.values())
            if strategy_type:
                results = [s for s in results if s.get("type") == strategy_type]

            return ActionResult(
                success=True,
                data={"strategies": results, "count": len(results)},
                message=f"Found {len(results)} strategies",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Strategy list failed: {e}")
