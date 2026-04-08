"""Automation command action module for RabAI AutoClick.

Provides command pattern operations:
- CommandCreateAction: Create a command
- CommandExecuteAction: Execute a command
- CommandUndoAction: Undo a command
- CommandHistoryAction: Get command history
- CommandMacroAction: Create command macro
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CommandCreateAction(BaseAction):
    """Create a command."""
    action_type = "command_create"
    display_name = "创建命令"
    description = "创建可执行命令"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            command_type = params.get("type", "action")
            payload = params.get("payload", {})
            undo_payload = params.get("undo_payload", None)

            if not name:
                return ActionResult(success=False, message="name is required")

            cmd_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "commands"):
                context.commands = {}
            context.commands[cmd_id] = {
                "cmd_id": cmd_id,
                "name": name,
                "type": command_type,
                "payload": payload,
                "undo_payload": undo_payload,
                "status": "created",
                "created_at": time.time(),
                "executed_at": None,
                "undone_at": None,
            }

            return ActionResult(
                success=True,
                data={"cmd_id": cmd_id, "name": name, "type": command_type},
                message=f"Command {cmd_id} created: {name}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Command create failed: {e}")


class CommandExecuteAction(BaseAction):
    """Execute a command."""
    action_type = "command_execute"
    display_name = "执行命令"
    description = "执行命令"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            cmd_id = params.get("cmd_id", "")
            if not cmd_id:
                return ActionResult(success=False, message="cmd_id is required")

            commands = getattr(context, "commands", {})
            if cmd_id not in commands:
                return ActionResult(success=False, message=f"Command {cmd_id} not found")

            cmd = commands[cmd_id]
            cmd["status"] = "executed"
            cmd["executed_at"] = time.time()

            if not hasattr(context, "command_history"):
                context.command_history = []
            context.command_history.append(cmd_id)

            return ActionResult(
                success=True,
                data={"cmd_id": cmd_id, "name": cmd["name"], "executed_at": cmd["executed_at"]},
                message=f"Command {cmd_id} executed: {cmd['name']}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Command execute failed: {e}")


class CommandUndoAction(BaseAction):
    """Undo a command."""
    action_type = "command_undo"
    display_name = "撤销命令"
    description = "撤销命令"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            cmd_id = params.get("cmd_id", "")
            if not cmd_id:
                return ActionResult(success=False, message="cmd_id is required")

            commands = getattr(context, "commands", {})
            if cmd_id not in commands:
                return ActionResult(success=False, message=f"Command {cmd_id} not found")

            cmd = commands[cmd_id]
            if cmd["status"] != "executed":
                return ActionResult(success=False, message=f"Command {cmd_id} not yet executed")

            if cmd.get("undo_payload") is None:
                return ActionResult(success=False, message=f"Command {cmd_id} has no undo action")

            cmd["status"] = "undone"
            cmd["undone_at"] = time.time()

            return ActionResult(
                success=True,
                data={"cmd_id": cmd_id, "name": cmd["name"], "undone_at": cmd["undone_at"]},
                message=f"Command {cmd_id} undone: {cmd['name']}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Command undo failed: {e}")


class CommandHistoryAction(BaseAction):
    """Get command execution history."""
    action_type = "command_history"
    display_name = "命令历史"
    description = "获取命令执行历史"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            limit = params.get("limit", 50)
            history = getattr(context, "command_history", [])[-limit:]
            commands = getattr(context, "commands", {})

            history_data = []
            for cmd_id in history:
                if cmd_id in commands:
                    cmd = commands[cmd_id]
                    history_data.append({
                        "cmd_id": cmd_id,
                        "name": cmd["name"],
                        "status": cmd["status"],
                        "executed_at": cmd.get("executed_at"),
                    })

            return ActionResult(
                success=True,
                data={"history": history_data, "count": len(history_data)},
                message=f"Command history: {len(history_data)} commands",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Command history failed: {e}")


class CommandMacroAction(BaseAction):
    """Create command macro (sequence of commands)."""
    action_type = "command_macro"
    display_name = "创建命令宏"
    description = "创建命令序列宏"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            command_ids = params.get("command_ids", [])

            if not name or not command_ids:
                return ActionResult(success=False, message="name and command_ids are required")

            macro_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "command_macros"):
                context.command_macros = {}
            context.command_macros[macro_id] = {
                "macro_id": macro_id,
                "name": name,
                "command_ids": command_ids,
                "created_at": time.time(),
            }

            return ActionResult(
                success=True,
                data={"macro_id": macro_id, "name": name, "command_count": len(command_ids)},
                message=f"Macro {macro_id} created: {name} with {len(command_ids)} commands",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Command macro failed: {e}")
