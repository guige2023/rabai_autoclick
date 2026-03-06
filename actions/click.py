import pyautogui
import time
from typing import Dict, Any
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ClickAction(BaseAction):
    action_type = "click"
    display_name = "鼠标点击"
    description = "在指定坐标位置执行鼠标点击操作"
    
    def execute(self, context, params: Dict[str, Any]) -> ActionResult:
        x = params.get('x', 0)
        y = params.get('y', 0)
        button = params.get('button', 'left')
        clicks = params.get('clicks', 1)
        interval = params.get('interval', 0.1)
        move_duration = params.get('move_duration', 0.2)
        relative = params.get('relative', False)
        
        try:
            if relative:
                current_x, current_y = pyautogui.position()
                x = current_x + x
                y = current_y + y
            
            pyautogui.moveTo(x, y, duration=move_duration)
            time.sleep(0.05)
            pyautogui.click(x=x, y=y, button=button, clicks=clicks, interval=interval)
            
            return ActionResult(
                success=True,
                message=f"点击成功: ({x}, {y})",
                data={'x': x, 'y': y}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"点击失败: {str(e)}"
            )
    
    def get_required_params(self) -> list:
        return ['x', 'y']
    
    def get_optional_params(self) -> dict:
        return {
            'button': 'left',
            'clicks': 1,
            'interval': 0.1,
            'move_duration': 0.2,
            'relative': False
        }
