"""Click action module for RabAI AutoClick.

Provides mouse click actions including single click and relative coordinate clicks.
"""

import pyautogui
import time
from typing import Any, Dict, List, Tuple, Union

from ..core.base_action import BaseAction, ActionResult


class ClickAction(BaseAction):
    """Simulate a mouse click at specified coordinates.
    
    Supports absolute and relative coordinates, multiple buttons,
    and configurable click intervals.
    """
    action_type = "click"
    display_name = "鼠标点击"
    description = "在指定坐标位置执行鼠标点击操作"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a mouse click.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: x, y, button, clicks, interval, 
                   move_duration, relative.
        
        Returns:
            ActionResult with success status and coordinates.
        """
        # Validate and extract parameters
        x = params.get('x', 0)
        y = params.get('y', 0)
        button = params.get('button', 'left')
        clicks = params.get('clicks', 1)
        interval = params.get('interval', 0.1)
        move_duration = params.get('move_duration', 0.2)
        relative = params.get('relative', False)
        
        # Validate button parameter
        valid, msg = self.validate_in(button, self.VALID_BUTTONS, 'button')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        # Validate clicks parameter
        valid, msg = self.validate_type(clicks, int, 'clicks')
        if not valid:
            return ActionResult(success=False, message=msg)
        if clicks < 1:
            return ActionResult(
                success=False, 
                message=f"Parameter 'clicks' must be >= 1, got {clicks}"
            )
        
        # Validate interval parameter
        valid, msg = self.validate_type(interval, (int, float), 'interval')
        if not valid:
            return ActionResult(success=False, message=msg)
        if interval < 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'interval' must be >= 0, got {interval}"
            )
        
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
                message=f"点击成功: ({x}, {y}) {button} x{clicks}",
                data={'x': x, 'y': y, 'button': button, 'clicks': clicks}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"点击失败: {str(e)}"
            )
    
    def get_required_params(self) -> List[str]:
        return ['x', 'y']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'button': 'left',
            'clicks': 1,
            'interval': 0.1,
            'move_duration': 0.2,
            'relative': False
        }
