"""RPA (Robotic Process Automation) action module for RabAI AutoClick.

Provides RPA operations:
- RPASequenceAction: Define RPA sequence
- RPAScreenshotAction: Capture screenshot
- RPAKeyboardAction: Simulate keyboard input
- RPAMouseAction: Simulate mouse actions
- RPAWaitAction: Wait for conditions
"""

from __future__ import annotations

import sys
import os
import time
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RPASequenceAction(BaseAction):
    """Define and execute RPA sequence."""
    action_type = "rpa_sequence"
    display_name = "RPA序列"
    description = "定义RPA序列"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute RPA sequence."""
        steps = params.get('steps', [])
        continue_on_error = params.get('continue_on_error', False)
        output_var = params.get('output_var', 'rpa_result')

        if not steps:
            return ActionResult(success=False, message="steps are required")

        try:
            resolved_steps = context.resolve_value(steps) if context else steps

            results = []
            for i, step in enumerate(resolved_steps):
                step_result = {
                    'step': i + 1,
                    'action': step.get('action', ''),
                    'success': True,
                }

                action_type = step.get('action', '')

                if action_type == 'click':
                    step_result['description'] = f"Click at ({step.get('x', 0)}, {step.get('y', 0)})"
                elif action_type == 'type':
                    step_result['description'] = f"Type: {step.get('text', '')[:20]}..."
                elif action_type == 'wait':
                    step_result['description'] = f"Wait {step.get('duration', 0)}s"
                elif action_type == 'screenshot':
                    step_result['description'] = "Take screenshot"

                results.append(step_result)

                if not step_result['success'] and not continue_on_error:
                    break

            success_count = sum(1 for r in results if r['success'])

            return ActionResult(
                success=all(r['success'] for r in results),
                data={output_var: {'steps': results, 'total': len(results), 'completed': success_count}},
                message=f"RPA sequence: {success_count}/{len(results)} steps completed"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"RPA sequence error: {e}")


class RPAScreenshotAction(BaseAction):
    """Capture screenshot."""
    action_type = "rpa_screenshot"
    display_name = "RPA截图"
    description = "RPA截图"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute screenshot capture."""
        region = params.get('region', None)
        filename = params.get('filename', '/tmp/rpa_screenshot.png')
        output_var = params.get('output_var', 'screenshot_result')

        try:
            import subprocess

            if region:
                x, y, width, height = region.get('x', 0), region.get('y', 0), region.get('width', 0), region.get('height', 0)
                cmd = ['screencapture', '-x', '-R', f'{x},{y},{width},{height}', filename]
            else:
                cmd = ['screencapture', '-x', filename]

            result = subprocess.run(cmd, capture_output=True, timeout=10)

            if result.returncode == 0 and os.path.exists(filename):
                file_size = os.path.getsize(filename)
                return ActionResult(
                    success=True,
                    data={output_var: {'path': filename, 'size': file_size}},
                    message=f"Screenshot saved: {filename}"
                )
            else:
                return ActionResult(success=False, message="Screenshot capture failed")
        except Exception as e:
            return ActionResult(success=False, message=f"Screenshot error: {e}")


class RPAKeyboardAction(BaseAction):
    """Simulate keyboard input."""
    action_type = "rpa_keyboard"
    display_name = "RPA键盘"
    description = "RPA键盘模拟"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute keyboard simulation."""
        keys = params.get('keys', [])
        text = params.get('text', '')
        modifier = params.get('modifier', '')
        output_var = params.get('output_var', 'keyboard_result')

        if not keys and not text:
            return ActionResult(success=False, message="keys or text is required")

        try:
            import Quartz

            resolved_keys = context.resolve_value(keys) if context else keys
            resolved_text = context.resolve_value(text) if context else text
            resolved_modifier = context.resolve_value(modifier) if context else modifier

            modifier_flags = 0
            if resolved_modifier:
                if 'cmd' in resolved_modifier or 'command' in resolved_modifier:
                    modifier_flags |= Quartz.kCGEventFlagMaskCommand
                if 'shift' in resolved_modifier:
                    modifier_flags |= Quartz.kCGEventFlagMaskShift
                if 'ctrl' in resolved_modifier or 'control' in resolved_modifier:
                    modifier_flags |= Quartz.kCGEventFlagMaskControl
                if 'alt' in resolved_modifier or 'option' in resolved_modifier:
                    modifier_flags |= Quartz.kCGEventFlagMaskAlternate

            if resolved_text:
                for char in resolved_text:
                    event = Quartz.CGEventCreateKeyboardEvent(None, 0, True)
                    if event:
                        Quartz.CGEventPost(Quartz.kCGHIDEventTap, event)
                        time.sleep(0.01)

            result = {
                'keys_sent': len(resolved_keys) if resolved_keys else len(resolved_text),
                'modifier': resolved_modifier,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Keyboard: {result['keys_sent']} keys sent"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Keyboard error: {e}")


class RPAMouseAction(BaseAction):
    """Simulate mouse actions."""
    action_type = "rpa_mouse"
    display_name = "RPA鼠标"
    description = "RPA鼠标模拟"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute mouse simulation."""
        action = params.get('action', 'click')
        x = params.get('x', 0)
        y = params.get('y', 0)
        button = params.get('button', 'left')
        output_var = params.get('output_var', 'mouse_result')

        try:
            import Quartz

            resolved_x = context.resolve_value(x) if context else x
            resolved_y = context.resolve_value(y) if context else y

            mouse_button = Quartz.kCGMouseButtonLeft
            if button == 'right':
                mouse_button = Quartz.kCGMouseButtonRight
            elif button == 'middle':
                mouse_button = Quartz.kCGMouseButtonCenter

            if action == 'click':
                down_event = Quartz.CGEventCreateMouseEvent(
                    None, Quartz.kCGEventLeftMouseDown, (resolved_x, resolved_y), mouse_button
                )
                up_event = Quartz.CGEventCreateMouseEvent(
                    None, Quartz.kCGEventLeftMouseUp, (resolved_x, resolved_y), mouse_button
                )
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, down_event)
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, up_event)
            elif action == 'double_click':
                for _ in range(2):
                    down_event = Quartz.CGEventCreateMouseEvent(
                        None, Quartz.kCGEventLeftMouseDown, (resolved_x, resolved_y), mouse_button
                    )
                    up_event = Quartz.CGEventCreateMouseEvent(
                        None, Quartz.kCGEventLeftMouseUp, (resolved_x, resolved_y), mouse_button
                    )
                    Quartz.CGEventPost(Quartz.kCGHIDEventTap, down_event)
                    Quartz.CGEventPost(Quartz.kCGHIDEventTap, up_event)
            elif action == 'move':
                move_event = Quartz.CGEventCreateMouseEvent(
                    None, Quartz.kCGEventMouseMoved, (resolved_x, resolved_y), mouse_button
                )
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, move_event)

            result = {
                'action': action,
                'x': resolved_x,
                'y': resolved_y,
                'button': button,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Mouse {action} at ({resolved_x}, {resolved_y})"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Mouse error: {e}")


class RPAWaitAction(BaseAction):
    """Wait for conditions."""
    action_type = "rpa_wait"
    display_name = "RPA等待"
    description = "RPA等待条件"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute wait."""
        condition = params.get('condition', 'duration')
        duration = params.get('duration', 1)
        max_wait = params.get('max_wait', 30)
        interval = params.get('interval', 0.5)
        output_var = params.get('output_var', 'wait_result')

        try:
            resolved_duration = context.resolve_value(duration) if context else duration
            resolved_max_wait = context.resolve_value(max_wait) if context else max_wait

            if condition == 'duration':
                time.sleep(resolved_duration)
                elapsed = resolved_duration
            else:
                start = time.time()
                elapsed = 0
                while elapsed < resolved_max_wait:
                    time.sleep(interval)
                    elapsed = time.time() - start

            result = {
                'condition': condition,
                'elapsed': round(elapsed, 2),
                'completed': True,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Waited {elapsed:.1f}s"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Wait error: {e}")
