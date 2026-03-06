import pyautogui
import time
import platform
from typing import Dict, Any
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


def check_pyautogui_permission() -> bool:
    if platform.system() != 'Darwin':
        return True
    
    try:
        current_pos = pyautogui.position()
        test_x = current_pos.x
        test_y = current_pos.y
        
        pyautogui.moveTo(test_x + 1, test_y + 1, duration=0.1)
        new_pos = pyautogui.position()
        
        if new_pos.x == test_x + 1 and new_pos.y == test_y + 1:
            pyautogui.moveTo(test_x, test_y, duration=0.1)
            return True
        return False
    except:
        return False


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
            if not check_pyautogui_permission():
                return ActionResult(
                    success=False,
                    message="⚠️ macOS辅助功能权限未授权！\n\n请前往：\n系统偏好设置 → 安全性与隐私 → 隐私 → 辅助功能\n添加终端或Python并重启程序"
                )
            
            if relative:
                current_x, current_y = pyautogui.position()
                x = current_x + x
                y = current_y + y
            
            pyautogui.moveTo(x, y, duration=move_duration)
            time.sleep(0.05)
            pyautogui.click(x=x, y=y, button=button, clicks=clicks, interval=interval)
            
            final_pos = pyautogui.position()
            
            return ActionResult(
                success=True,
                message=f"点击成功: ({x}, {y}) | 实际位置: {final_pos}",
                data={'x': x, 'y': y, 'actual': (final_pos.x, final_pos.y)}
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
