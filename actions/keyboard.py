"""Keyboard action module for RabAI AutoClick.

Provides keyboard automation actions:
- TypeAction: Text input simulation
- KeyPressAction: Single key or key combination press
"""

import pyautogui
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from ..core.base_action import BaseAction, ActionResult


class TypeAction(BaseAction):
    """Simulate keyboard text input."""
    action_type = "type_text"
    display_name = "键盘输入"
    description = "模拟键盘输入文本内容"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute text input.
        
        Args:
            context: Execution context.
            params: Dict with text, interval, enter_after.
        
        Returns:
            ActionResult indicating success or failure.
        """
        text = params.get('text', '')
        interval = params.get('interval', 0.05)
        enter_after = params.get('enter_after', False)
        
        # Validate text
        if not text:
            return ActionResult(
                success=False,
                message="输入文本为空"
            )
        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        # Validate interval
        valid, msg = self.validate_type(interval, (int, float), 'interval')
        if not valid:
            return ActionResult(success=False, message=msg)
        if interval < 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'interval' must be >= 0, got {interval}"
            )
        
        # Validate enter_after
        valid, msg = self.validate_type(enter_after, bool, 'enter_after')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            pyautogui.write(text, interval=interval)
            
            if enter_after:
                time.sleep(0.1)
                pyautogui.press('enter')
            
            truncated = text[:50] + '...' if len(text) > 50 else text
            return ActionResult(
                success=True,
                message=f"输入成功: {truncated}",
                data={'text': text}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"输入失败: {str(e)}"
            )
    
    def get_required_params(self) -> List[str]:
        return ['text']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'interval': 0.05,
            'enter_after': False
        }


class KeyPressAction(BaseAction):
    """Simulate pressing a single key or key combination."""
    action_type = "key_press"
    display_name = "按键操作"
    description = "模拟按下特定按键或组合键"
    
    # Common valid keys for validation (partial list)
    VALID_KEYS: List[str] = None  # Will be set in __init__
    
    def __init__(self) -> None:
        super().__init__()
        if KeyPressAction.VALID_KEYS is None:
            # Initialize with pyautogui supported keys
            KeyPressAction.VALID_KEYS = [
                'alt', 'altleft', 'altright', 'backspace', 'capslock',
                'cmd', 'ctrl', 'delete', 'down', 'end', 'enter',
                'esc', 'f1', 'f2', 'f3', 'f4', 'f5', 'f6', 'f7', 'f8',
                'f9', 'f10', 'f11', 'f12', 'home', 'insert', 'left',
                'meta', 'pagedown', 'pageup', 'printscreen', 'right',
                'shift', 'space', 'tab', 'up', 'volumeup', 'volumedown',
                'volumemute', 'win'
            ]
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a key press or key combination.
        
        Args:
            context: Execution context.
            params: Dict with key, keys (list), hold_time.
        
        Returns:
            ActionResult indicating success or failure.
        """
        key = params.get('key', '')
        keys = params.get('keys', [])
        hold_time = params.get('hold_time', 0.1)
        
        # Validate that at least one of key or keys is provided
        if not key and not keys:
            return ActionResult(
                success=False,
                message="未指定按键: 需要 'key' 或 'keys' 参数"
            )
        
        # Validate hold_time
        valid, msg = self.validate_type(hold_time, (int, float), 'hold_time')
        if not valid:
            return ActionResult(success=False, message=msg)
        if hold_time < 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'hold_time' must be >= 0, got {hold_time}"
            )
        
        # Validate keys list
        if keys:
            valid, msg = self.validate_type(keys, list, 'keys')
            if not valid:
                return ActionResult(success=False, message=msg)
        
        try:
            if keys:
                # Key combination: press all keys down, hold, then release in reverse
                for k in keys:
                    pyautogui.keyDown(k)
                time.sleep(hold_time)
                for k in reversed(keys):
                    pyautogui.keyUp(k)
                key_desc = '+'.join(keys)
            elif key:
                pyautogui.press(key)
                key_desc = key
            else:
                return ActionResult(
                    success=False,
                    message="未指定按键"
                )
            
            return ActionResult(
                success=True,
                message=f"按键成功: {key_desc}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"按键失败: {str(e)}"
            )
    
    def get_required_params(self) -> List[str]:
        return []
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'key': '',
            'keys': [],
            'hold_time': 0.1
        }
