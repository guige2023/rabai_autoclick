import pyautogui
import time
from typing import Dict, Any
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TypeAction(BaseAction):
    action_type = "type_text"
    display_name = "键盘输入"
    description = "模拟键盘输入文本内容"
    
    def execute(self, context, params: Dict[str, Any]) -> ActionResult:
        text = params.get('text', '')
        interval = params.get('interval', 0.05)
        enter_after = params.get('enter_after', False)
        
        if not text:
            return ActionResult(
                success=False,
                message="输入文本为空"
            )
        
        try:
            pyautogui.write(text, interval=interval)
            
            if enter_after:
                time.sleep(0.1)
                pyautogui.press('enter')
            
            return ActionResult(
                success=True,
                message=f"输入成功: {text[:50]}{'...' if len(text) > 50 else ''}",
                data={'text': text}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"输入失败: {str(e)}"
            )
    
    def get_required_params(self) -> list:
        return ['text']
    
    def get_optional_params(self) -> dict:
        return {
            'interval': 0.05,
            'enter_after': False
        }


class KeyPressAction(BaseAction):
    action_type = "key_press"
    display_name = "按键操作"
    description = "模拟按下特定按键或组合键"
    
    def execute(self, context, params: Dict[str, Any]) -> ActionResult:
        key = params.get('key', '')
        keys = params.get('keys', [])
        hold_time = params.get('hold_time', 0.1)
        
        try:
            if keys:
                for k in keys:
                    pyautogui.keyDown(k)
                time.sleep(hold_time)
                for k in reversed(keys):
                    pyautogui.keyUp(k)
            elif key:
                pyautogui.press(key)
            else:
                return ActionResult(
                    success=False,
                    message="未指定按键"
                )
            
            return ActionResult(
                success=True,
                message=f"按键成功: {keys if keys else key}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"按键失败: {str(e)}"
            )
    
    def get_required_params(self) -> list:
        return []
    
    def get_optional_params(self) -> dict:
        return {
            'key': '',
            'keys': [],
            'hold_time': 0.1
        }
