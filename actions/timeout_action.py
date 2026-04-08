"""Timeout action module for RabAI AutoClick.

Provides timeout handling for long-running operations with
auto-cancellation, fallback actions, and timeout chaining.
"""

import sys
import os
import time
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TimeoutStrategy(Enum):
    """Timeout handling strategies."""
    CANCEL = "cancel"
    FALLBACK = "fallback"
    CONTINUE = "continue"
    EXTEND = "extend"


@dataclass
class TimeoutConfig:
    """Configuration for a timeout handler."""
    timeout_seconds: float = 30.0
    strategy: str = "cancel"
    fallback_action: Optional[str] = None
    fallback_params: Dict[str, Any] = field(default_factory=dict)
    on_timeout: Optional[str] = None
    on_timeout_params: Dict[str, Any] = field(default_factory=dict)
    extendable: bool = False
    max_extension: float = 0.0


class TimeoutAction(BaseAction):
    """Execute an action with timeout protection.
    
    Supports auto-cancellation, fallback actions,
    timeout callbacks, and extendable timeouts.
    """
    action_type = "timeout"
    display_name = "超时控制"
    description = "为操作设置超时保护，支持自动取消和降级"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute an action with timeout protection.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - action_name: str, action to wrap
                - action_params: dict, parameters for the action
                - timeout_seconds: float, timeout duration
                - strategy: str (cancel/fallback/continue/extend)
                - fallback_action: str, optional fallback action
                - fallback_params: dict
                - on_timeout: str, optional callback action
                - save_to_var: str, optional output variable
        
        Returns:
            ActionResult with execution result or timeout handling result.
        """
        action_name = params.get('action_name', '')
        action_params = params.get('action_params', {})
        timeout_seconds = params.get('timeout_seconds', 30.0)
        strategy = params.get('strategy', 'cancel')
        fallback_action = params.get('fallback_action', None)
        fallback_params = params.get('fallback_params', {})
        on_timeout = params.get('on_timeout', None)
        on_timeout_params = params.get('on_timeout_params', {})
        save_to_var = params.get('save_to_var', None)
        extendable = params.get('extendable', False)
        max_extension = params.get('max_extension', 0.0)

        if not action_name:
            return ActionResult(success=False, message="action_name is required")

        if timeout_seconds <= 0:
            return ActionResult(
                success=False,
                message=f"timeout_seconds must be positive, got {timeout_seconds}"
            )

        start_time = time.time()
        result_holder = {'result': None, 'error': None, 'done': False}
        current_timeout = timeout_seconds
        total_extension = 0.0

        def run_action():
            try:
                action = self._find_action(action_name)
                if action is None:
                    result_holder['error'] = f"Action not found: {action_name}"
                    result_holder['done'] = True
                    return
                result = action.execute(context, action_params)
                result_holder['result'] = result
                result_holder['done'] = True
            except Exception as e:
                result_holder['error'] = str(e)
                result_holder['done'] = True

        thread = threading.Thread(target=run_action, daemon=True)
        thread.start()

        # Wait for completion or timeout
        while not result_holder['done']:
            elapsed = time.time() - start_time
            if elapsed >= current_timeout:
                break
            time.sleep(min(0.1, current_timeout - elapsed))

        elapsed = time.time() - start_time

        if result_holder['done']:
            if result_holder['error']:
                return ActionResult(
                    success=False,
                    message=f"Action failed: {result_holder['error']}",
                    duration=elapsed
                )
            result = result_holder['result']
            if save_to_var and hasattr(context, 'vars'):
                context.vars[save_to_var] = result.data if result else None
            return ActionResult(
                success=result.success if result else False,
                message=result.message if result else "Completed",
                data=result.data if result else None,
                duration=elapsed
            )

        # Timeout occurred
        timeout_msg = f"Action '{action_name}' timed out after {elapsed:.2f}s"

        if strategy == 'cancel':
            if on_timeout:
                self._run_callback(context, on_timeout, on_timeout_params)
            return ActionResult(
                success=False,
                message=timeout_msg,
                duration=elapsed
            )

        elif strategy == 'fallback':
            if fallback_action:
                try:
                    fallback_act = self._find_action(fallback_action)
                    if fallback_act:
                        fb_result = fallback_act.execute(context, fallback_params)
                        return ActionResult(
                            success=fb_result.success,
                            message=f"{timeout_msg}, fallback executed: {fb_result.message}",
                            data=fb_result.data,
                            duration=elapsed
                        )
                except Exception as e:
                    return ActionResult(
                        success=False,
                        message=f"{timeout_msg}, fallback failed: {e}",
                        duration=elapsed
                    )
            return ActionResult(
                success=False,
                message=timeout_msg,
                duration=elapsed
            )

        elif strategy == 'extend':
            if extendable and (max_extension <= 0 or total_extension < max_extension):
                extension = min(timeout_seconds, max_extension) if max_extension > 0 else timeout_seconds
                total_extension += extension
                current_timeout += extension
                while not result_holder['done']:
                    extended_elapsed = time.time() - start_time
                    if extended_elapsed >= current_timeout:
                        break
                    time.sleep(min(0.1, current_timeout - extended_elapsed))
                if result_holder['done']:
                    result = result_holder['result']
                    return ActionResult(
                        success=result.success if result else False,
                        message=f"Action completed after {total_extension:.2f}s extension",
                        data=result.data if result else None,
                        duration=time.time() - start_time
                    )
            if on_timeout:
                self._run_callback(context, on_timeout, on_timeout_params)
            return ActionResult(
                success=False,
                message=f"{timeout_msg} (extension: {total_extension:.2f}s)",
                duration=time.time() - start_time
            )

        else:  # continue
            return ActionResult(
                success=True,
                message=f"{timeout_msg}, continuing anyway",
                duration=elapsed
            )

    def _find_action(self, action_name: str) -> Optional[BaseAction]:
        """Find an action by name."""
        try:
            from actions import (
                ClickAction, TypeAction, KeyPressAction, ImageMatchAction,
                FindImageAction, OCRAction, ScrollAction, MouseMoveAction,
                DragAction, ScriptAction, DelayAction, ConditionAction,
                LoopAction, SetVariableAction, ScreenshotAction,
                GetMousePosAction, AlertAction
            )
            action_map = {
                'click': ClickAction,
                'type': TypeAction,
                'key_press': KeyPressAction,
                'image_match': ImageMatchAction,
                'find_image': FindImageAction,
                'ocr': OCRAction,
                'scroll': ScrollAction,
                'mouse_move': MouseMoveAction,
                'drag': DragAction,
                'script': ScriptAction,
                'delay': DelayAction,
                'condition': ConditionAction,
                'loop': LoopAction,
                'set_variable': SetVariableAction,
                'screenshot': ScreenshotAction,
                'get_mouse_pos': GetMousePosAction,
                'alert': AlertAction,
            }
            action_cls = action_map.get(action_name.lower())
            if action_cls:
                return action_cls()
        except Exception:
            pass
        return None

    def _run_callback(self, context: Any, callback_action: str, callback_params: Dict[str, Any]) -> None:
        """Run a timeout callback action."""
        try:
            callback = self._find_action(callback_action)
            if callback:
                callback.execute(context, callback_params)
        except Exception:
            pass

    def get_required_params(self) -> List[str]:
        return ['action_name', 'timeout_seconds']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'action_params': {},
            'strategy': 'cancel',
            'fallback_action': None,
            'fallback_params': {},
            'on_timeout': None,
            'on_timeout_params': {},
            'save_to_var': None,
            'extendable': False,
            'max_extension': 0.0,
        }
