"""Automation engine action module for RabAI AutoClick.

Provides automation engine operations:
- EngineInitAction: Initialize automation engine
- EngineRegisterAction: Register action handler
- EngineUnregisterAction: Unregister action handler
- EngineRunAction: Run automation plan
- EnginePauseAction: Pause engine execution
- EngineResumeAction: Resume engine execution
- EngineStopAction: Stop engine execution
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EngineInitAction(BaseAction):
    """Initialize automation engine."""
    action_type = "engine_init"
    display_name = "引擎初始化"
    description = "初始化自动化引擎"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            config = params.get("config", {})
            max_workers = params.get("max_workers", 4)
            log_level = params.get("log_level", "INFO")

            engine_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "automation_engines"):
                context.automation_engines = {}

            context.automation_engines[engine_id] = {
                "engine_id": engine_id,
                "config": config,
                "max_workers": max_workers,
                "log_level": log_level,
                "status": "idle",
                "registered_actions": [],
                "created_at": time.time(),
            }

            return ActionResult(
                success=True,
                data={"engine_id": engine_id, "max_workers": max_workers, "log_level": log_level},
                message=f"Engine {engine_id} initialized",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Engine init failed: {e}")


class EngineRegisterAction(BaseAction):
    """Register an action handler."""
    action_type = "engine_register"
    display_name = "引擎注册"
    description = "注册动作处理器到引擎"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            engine_id = params.get("engine_id", "")
            action_name = params.get("action_name", "")
            handler = params.get("handler", "")

            if not engine_id:
                return ActionResult(success=False, message="engine_id is required")
            if not action_name:
                return ActionResult(success=False, message="action_name is required")

            if not hasattr(context, "automation_engines") or engine_id not in context.automation_engines:
                return ActionResult(success=False, message=f"Engine {engine_id} not found")

            engine = context.automation_engines[engine_id]
            if "registered_actions" not in engine:
                engine["registered_actions"] = []
            engine["registered_actions"].append(action_name)

            return ActionResult(
                success=True,
                data={"engine_id": engine_id, "action_name": action_name, "registered_count": len(engine["registered_actions"])},
                message=f"Registered {action_name} to engine {engine_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Engine register failed: {e}")


class EngineUnregisterAction(BaseAction):
    """Unregister an action handler."""
    action_type = "engine_unregister"
    display_name = "引擎注销"
    description = "注销动作处理器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            engine_id = params.get("engine_id", "")
            action_name = params.get("action_name", "")

            if not engine_id or not action_name:
                return ActionResult(success=False, message="engine_id and action_name are required")

            if not hasattr(context, "automation_engines") or engine_id not in context.automation_engines:
                return ActionResult(success=False, message=f"Engine {engine_id} not found")

            engine = context.automation_engines[engine_id]
            if "registered_actions" in engine and action_name in engine["registered_actions"]:
                engine["registered_actions"].remove(action_name)

            return ActionResult(
                success=True,
                data={"engine_id": engine_id, "action_name": action_name},
                message=f"Unregistered {action_name} from engine {engine_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Engine unregister failed: {e}")


class EngineRunAction(BaseAction):
    """Run an automation plan."""
    action_type = "engine_run"
    display_name = "引擎运行"
    description = "运行自动化计划"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            engine_id = params.get("engine_id", "")
            plan_id = params.get("plan_id", "")
            async_run = params.get("async", False)

            if not engine_id:
                return ActionResult(success=False, message="engine_id is required")

            if not hasattr(context, "automation_engines") or engine_id not in context.automation_engines:
                return ActionResult(success=False, message=f"Engine {engine_id} not found")

            engine = context.automation_engines[engine_id]
            engine["status"] = "running"
            engine["current_plan"] = plan_id
            engine["started_at"] = time.time()

            run_id = str(uuid.uuid4())[:8]

            return ActionResult(
                success=True,
                data={"run_id": run_id, "engine_id": engine_id, "plan_id": plan_id, "status": "running"},
                message=f"Engine {engine_id} running plan {plan_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Engine run failed: {e}")


class EnginePauseAction(BaseAction):
    """Pause engine execution."""
    action_type = "engine_pause"
    display_name = "引擎暂停"
    description = "暂停引擎执行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            engine_id = params.get("engine_id", "")
            if not engine_id:
                return ActionResult(success=False, message="engine_id is required")

            if not hasattr(context, "automation_engines") or engine_id not in context.automation_engines:
                return ActionResult(success=False, message=f"Engine {engine_id} not found")

            engine = context.automation_engines[engine_id]
            engine["status"] = "paused"
            engine["paused_at"] = time.time()

            return ActionResult(
                success=True,
                data={"engine_id": engine_id, "status": "paused"},
                message=f"Engine {engine_id} paused",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Engine pause failed: {e}")


class EngineResumeAction(BaseAction):
    """Resume engine execution."""
    action_type = "engine_resume"
    display_name = "引擎恢复"
    description = "恢复引擎执行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            engine_id = params.get("engine_id", "")
            if not engine_id:
                return ActionResult(success=False, message="engine_id is required")

            if not hasattr(context, "automation_engines") or engine_id not in context.automation_engines:
                return ActionResult(success=False, message=f"Engine {engine_id} not found")

            engine = context.automation_engines[engine_id]
            engine["status"] = "running"
            paused_duration = time.time() - engine.get("paused_at", time.time())

            return ActionResult(
                success=True,
                data={"engine_id": engine_id, "status": "running", "paused_duration_s": paused_duration},
                message=f"Engine {engine_id} resumed",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Engine resume failed: {e}")


class EngineStopAction(BaseAction):
    """Stop engine execution."""
    action_type = "engine_stop"
    display_name = "引擎停止"
    description = "停止引擎执行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            engine_id = params.get("engine_id", "")
            force = params.get("force", False)

            if not engine_id:
                return ActionResult(success=False, message="engine_id is required")

            if not hasattr(context, "automation_engines") or engine_id not in context.automation_engines:
                return ActionResult(success=False, message=f"Engine {engine_id} not found")

            engine = context.automation_engines[engine_id]
            engine["status"] = "stopped"
            engine["stopped_at"] = time.time()
            if "started_at" in engine:
                engine["total_runtime_s"] = engine["stopped_at"] - engine["started_at"]

            return ActionResult(
                success=True,
                data={"engine_id": engine_id, "status": "stopped", "force": force},
                message=f"Engine {engine_id} stopped",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Engine stop failed: {e}")
