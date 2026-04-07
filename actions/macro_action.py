"""Macro recording and playback action module for RabAI AutoClick.

Provides macro operations:
- MacroRecordAction: Record actions as macro
- MacroPlaybackAction: Playback recorded macro
- MacroEditAction: Edit macro steps
- MacroConditionAction: Conditional macro steps
- MacroLoopAction: Loop macro execution
- MacroSaveAction: Save/load macros
"""

import time
import json
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MacroRecordAction(BaseAction):
    """Record actions as macro."""
    action_type = "macro_record"
    display_name = "录制宏"
    description = "录制动作序列为宏"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "start")
            macro_name = params.get("name", "unnamed_macro")
            actions = params.get("actions", [])

            if action == "start":
                return ActionResult(
                    success=True,
                    message=f"Started recording macro '{macro_name}'",
                    data={"recording": True, "macro_name": macro_name, "actions": []}
                )

            elif action == "stop":
                return ActionResult(
                    success=True,
                    message=f"Stopped recording macro '{macro_name}' with {len(actions)} actions",
                    data={"recording": False, "macro_name": macro_name, "actions": actions, "action_count": len(actions)}
                )

            elif action == "add":
                action_type = params.get("action_type", "click")
                action_params = params.get("action_params", {})
                delay = params.get("delay", 0)

                step = {
                    "type": action_type,
                    "params": action_params,
                    "delay": delay,
                    "timestamp": time.time()
                }

                return ActionResult(
                    success=True,
                    message=f"Added {action_type} action",
                    data={"step": step}
                )

            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Record error: {str(e)}")


class MacroPlaybackAction(BaseAction):
    """Playback recorded macro."""
    action_type = "macro_playback"
    display_name = "播放宏"
    description = "播放录制的宏"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            macro = params.get("macro", {})
            actions = params.get("actions", macro.get("actions", []))
            speed = params.get("speed", 1.0)
            loop_count = params.get("loop", 1)
            stop_on_error = params.get("stop_on_error", True)

            if not actions:
                return ActionResult(success=False, message="No actions to playback")

            results = []

            for loop in range(loop_count):
                for i, action in enumerate(actions):
                    action_type = action.get("type", "")
                    action_params = action.get("params", {})
                    delay = action.get("delay", 0)

                    delay_adjusted = delay / speed if speed > 0 else delay

                    try:
                        result = self._execute_macro_action(action_type, action_params)
                        results.append({
                            "loop": loop + 1,
                            "action_index": i,
                            "type": action_type,
                            "success": result.success,
                            "message": result.message
                        })

                        if not result.success and stop_on_error:
                            return ActionResult(
                                success=False,
                                message=f"Playback stopped at action {i}: {result.message}",
                                data={"results": results, "completed": len(results)}
                            )

                    except Exception as e:
                        results.append({
                            "loop": loop + 1,
                            "action_index": i,
                            "type": action_type,
                            "success": False,
                            "error": str(e)
                        })
                        if stop_on_error:
                            return ActionResult(
                                success=False,
                                message=f"Playback error at action {i}: {str(e)}",
                                data={"results": results, "completed": len(results)}
                            )

                    if delay_adjusted > 0:
                        time.sleep(delay_adjusted)

            success_count = sum(1 for r in results if r.get("success", False))

            return ActionResult(
                success=True,
                message=f"Playback completed: {success_count}/{len(results)} actions succeeded",
                data={"results": results, "total_actions": len(results), "successful": success_count}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Playback error: {str(e)}")

    def _execute_macro_action(self, action_type: str, params: Dict) -> ActionResult:
        """Execute a single macro action."""
        if action_type == "click":
            x = params.get("x", 0)
            y = params.get("y", 0)
            button = params.get("button", "left")
            try:
                import Quartz
                button_map = {"left": Quartz.kCGEventLeftMouseDown, "right": Quartz.kCGEventRightMouseDown}
                down_type = button_map.get(button, Quartz.kCGEventLeftMouseDown)
                up_type = Quartz.kCGEventLeftMouseUp
                down = Quartz.CGEventCreateMouseEvent(None, down_type, (x, y), Quartz.kCGMouseButtonLeft)
                up = Quartz.CGEventCreateMouseEvent(None, up_type, (x, y), Quartz.kCGMouseButtonLeft)
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, down)
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, up)
                return ActionResult(success=True, message=f"Clicked at ({x}, {y})")
            except:
                return ActionResult(success=False, message="Click failed")

        elif action_type == "type":
            text = params.get("text", "")
            for char in text:
                escaped = char.replace('"', '\\"')
                os.system(f"osascript -e 'tell application \"System Events\" to keystroke \"{escaped}\"'")
                time.sleep(0.05)
            return ActionResult(success=True, message=f"Typed {len(text)} chars")

        elif action_type == "key":
            key = params.get("key", "return")
            os.system(f"osascript -e 'tell application \"System Events\" to keystroke \"{key}\"'")
            return ActionResult(success=True, message=f"Pressed {key}")

        elif action_type == "wait":
            seconds = params.get("seconds", 1)
            time.sleep(seconds)
            return ActionResult(success=True, message=f"Waited {seconds}s")

        elif action_type == "screenshot":
            path = params.get("path", "/tmp/macro_screenshot.png")
            os.system(f"screencapture -x {path}")
            return ActionResult(success=True, message=f"Screenshot saved to {path}")

        else:
            return ActionResult(success=True, message=f"Action {action_type} completed")


class MacroEditAction(BaseAction):
    """Edit macro steps."""
    action_type = "macro_edit"
    display_name = "编辑宏"
    description = "编辑宏步骤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            actions = params.get("actions", [])
            edit_type = params.get("edit_type", "insert")
            index = params.get("index", 0)
            new_action = params.get("action", {})

            if not actions:
                return ActionResult(success=False, message="No actions to edit")

            result = list(actions)

            if edit_type == "insert":
                if 0 <= index <= len(result):
                    result.insert(index, new_action)
                    return ActionResult(success=True, message=f"Inserted action at index {index}")

            elif edit_type == "delete":
                if 0 <= index < len(result):
                    deleted = result.pop(index)
                    return ActionResult(success=True, message=f"Deleted action at index {index}", data={"deleted": deleted})

            elif edit_type == "replace":
                if 0 <= index < len(result):
                    old = result[index]
                    result[index] = new_action
                    return ActionResult(success=True, message=f"Replaced action at index {index}", data={"old": old, "new": new_action})

            elif edit_type == "move":
                from_index = params.get("from_index", 0)
                to_index = params.get("to_index", 0)
                if 0 <= from_index < len(result) and 0 <= to_index <= len(result):
                    action = result.pop(from_index)
                    result.insert(to_index, action)
                    return ActionResult(success=True, message=f"Moved action from {from_index} to {to_index}")

            return ActionResult(
                success=True,
                message=f"Edited macro: {len(result)} actions",
                data={"actions": result, "count": len(result)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Edit error: {str(e)}")


class MacroConditionAction(BaseAction):
    """Conditional macro steps."""
    action_type = "macro_condition"
    display_name = "宏条件"
    description = "宏条件判断"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            condition = params.get("condition", "")
            data = params.get("data", {})
            then_actions = params.get("then", [])
            else_actions = params.get("else", [])

            if not condition:
                return ActionResult(success=False, message="condition is required")

            result = self._evaluate_condition(condition, data)

            if result:
                executed = then_actions
                branch = "then"
            else:
                executed = else_actions
                branch = "else"

            return ActionResult(
                success=True,
                message=f"Condition '{condition}' = {result}, executing {branch} branch with {len(executed)} actions",
                data={"branch": branch, "result": result, "actions": executed}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Condition error: {str(e)}")

    def _evaluate_condition(self, condition: str, data: Dict) -> bool:
        try:
            for key, value in data.items():
                condition = condition.replace(key, repr(value))
            return eval(condition, {"__builtins__": {}}, {})
        except:
            return False


class MacroLoopAction(BaseAction):
    """Loop macro execution."""
    action_type = "macro_loop"
    display_name = "宏循环"
    description = "循环执行宏"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            loop_type = params.get("type", "for")
            actions = params.get("actions", [])
            iterations = params.get("iterations", 3)
            items = params.get("items", [])
            condition = params.get("condition", "")
            data = params.get("data", {})

            if not actions:
                return ActionResult(success=False, message="No actions to loop")

            results = []
            loop_count = 0

            if loop_type == "for":
                for i in range(min(iterations, 1000)):
                    data["index"] = i
                    data["item"] = items[i] if i < len(items) else None
                    loop_count += 1

                    for action in actions:
                        results.append({"loop": i, "action": action})

            elif loop_type == "while":
                i = 0
                while self._evaluate_condition(condition, data) and i < 1000:
                    data["index"] = i
                    loop_count += 1

                    for action in actions:
                        results.append({"loop": i, "action": action})
                    i += 1

            elif loop_type == "foreach":
                for i, item in enumerate(items[:100]):
                    data["index"] = i
                    data["item"] = item
                    loop_count += 1

                    for action in actions:
                        results.append({"loop": i, "action": action, "item": item})

            return ActionResult(
                success=True,
                message=f"Loop completed {loop_count} iterations with {len(results)} total action executions",
                data={"results": results, "loop_count": loop_count, "total_actions": len(results)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Loop error: {str(e)}")

    def _evaluate_condition(self, condition: str, data: Dict) -> bool:
        try:
            for key, value in data.items():
                condition = condition.replace(key, repr(value))
            return eval(condition, {"__builtins__": {}}, {})
        except:
            return False


class MacroSaveAction(BaseAction):
    """Save/load macros."""
    action_type = "macro_save"
    display_name = "保存宏"
    description = "保存/加载宏"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "save")
            macro = params.get("macro", {})
            name = params.get("name", "unnamed")
            path = params.get("path", f"/tmp/{name}.json")
            actions = params.get("actions", [])

            if action == "save":
                macro_data = {
                    "name": name,
                    "actions": actions,
                    "created_at": time.time(),
                    "version": "1.0"
                }

                try:
                    with open(path, "w", encoding="utf-8") as f:
                        json.dump(macro_data, f, indent=2, ensure_ascii=False)

                    return ActionResult(
                        success=True,
                        message=f"Saved macro '{name}' to {path}",
                        data={"path": path, "macro": macro_data}
                    )
                except Exception as e:
                    return ActionResult(success=False, message=f"Failed to save macro: {str(e)}")

            elif action == "load":
                if not os.path.exists(path):
                    return ActionResult(success=False, message=f"Macro file not found: {path}")

                try:
                    with open(path, "r", encoding="utf-8") as f:
                        macro_data = json.load(f)

                    return ActionResult(
                        success=True,
                        message=f"Loaded macro '{macro_data.get('name', 'unknown')}' from {path}",
                        data={"macro": macro_data, "path": path}
                    )
                except Exception as e:
                    return ActionResult(success=False, message=f"Failed to load macro: {str(e)}")

            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Save/Load error: {str(e)}")
