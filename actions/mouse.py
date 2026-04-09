"""Mouse action module for RabAI AutoClick.

Provides mouse-related automation actions including:
- MouseClickAction: Standard mouse click
- DoubleClickAction: Double click simulation
- ScrollAction: Mouse wheel scrolling
- MouseMoveAction: Mouse movement without clicking
- DragAction: Mouse drag from one position to another
"""

import pyautogui
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from rabai_autoclick.core.base_action import BaseAction, ActionResult


class MouseClickAction(BaseAction):
    """Simulate a mouse click at specified coordinates.
    
    This is the mouse module's click implementation, distinct from
    ClickAction in the click.py module.
    """
    action_type = "mouse_click"
    display_name = "鼠标单击"
    description = "模拟鼠标单击操作"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a mouse click.
        
        Args:
            context: Execution context.
            params: Dict with x, y, button, clicks, move_duration.
        
        Returns:
            ActionResult indicating success or failure.
        """
        x = params.get('x', None)
        y = params.get('y', None)
        button = params.get('button', 'left')
        clicks = params.get('clicks', 1)
        move_duration = params.get('move_duration', 0.2)
        
        # Validate button
        valid, msg = self.validate_in(button, self.VALID_BUTTONS, 'button')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        # Validate clicks
        valid, msg = self.validate_type(clicks, int, 'clicks')
        if not valid:
            return ActionResult(success=False, message=msg)
        if clicks < 1:
            return ActionResult(
                success=False,
                message=f"Parameter 'clicks' must be >= 1, got {clicks}"
            )
        
        try:
            if x is not None and y is not None:
                pyautogui.moveTo(x, y, duration=move_duration)
                time.sleep(0.05)
            
            pyautogui.click(x=x, y=y, clicks=clicks, button=button)
            
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
        return []
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'x': None,
            'y': None,
            'button': 'left',
            'clicks': 1,
            'move_duration': 0.2
        }


class DoubleClickAction(BaseAction):
    """Simulate a mouse double-click at specified coordinates."""
    action_type = "double_click"
    display_name = "鼠标双击"
    description = "模拟鼠标双击操作"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a mouse double-click.
        
        Args:
            context: Execution context.
            params: Dict with x, y, button, move_duration.
        
        Returns:
            ActionResult indicating success or failure.
        """
        x = params.get('x', None)
        y = params.get('y', None)
        button = params.get('button', 'left')
        move_duration = params.get('move_duration', 0.2)
        
        # Validate button
        valid, msg = self.validate_in(button, self.VALID_BUTTONS, 'button')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            if x is not None and y is not None:
                pyautogui.moveTo(x, y, duration=move_duration)
                time.sleep(0.05)
            
            pyautogui.doubleClick(x=x, y=y, button=button)
            
            return ActionResult(
                success=True,
                message=f"双击成功: ({x}, {y}) {button}",
                data={'x': x, 'y': y, 'button': button}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"双击失败: {str(e)}"
            )
    
    def get_required_params(self) -> List[str]:
        return []
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'x': None,
            'y': None,
            'button': 'left',
            'move_duration': 0.2
        }


class ScrollAction(BaseAction):
    """Simulate mouse wheel scrolling."""
    action_type = "scroll"
    display_name = "鼠标滚轮"
    description = "模拟鼠标滚轮滚动操作"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a mouse scroll.
        
        Args:
            context: Execution context.
            params: Dict with clicks, direction, x, y, move_duration.
        
        Returns:
            ActionResult indicating success or failure.
        """
        clicks = params.get('clicks', 3)
        direction = params.get('direction', 'down')
        x = params.get('x', None)
        y = params.get('y', None)
        move_duration = params.get('move_duration', 0.2)
        
        # Validate direction
        valid, msg = self.validate_in(direction, self.VALID_DIRECTIONS, 'direction')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        # Validate clicks
        valid, msg = self.validate_type(clicks, (int, float), 'clicks')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        # Convert direction to scroll value
        scroll_value = abs(clicks)
        if direction == 'down':
            scroll_value = -scroll_value
        
        try:
            if x is not None and y is not None:
                pyautogui.moveTo(x, y, duration=move_duration)
                time.sleep(0.05)
            
            pyautogui.scroll(scroll_value, x=x, y=y)
            
            return ActionResult(
                success=True,
                message=f"滚动成功: {abs(clicks)} 格 ({direction})",
                data={'clicks': scroll_value, 'direction': direction}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"滚动失败: {str(e)}"
            )
    
    def get_required_params(self) -> List[str]:
        return []
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'clicks': 3,
            'direction': 'down',
            'x': None,
            'y': None,
            'move_duration': 0.2
        }


class MouseMoveAction(BaseAction):
    """Move mouse cursor to specified position without clicking."""
    action_type = "mouse_move"
    display_name = "鼠标移动"
    description = "移动鼠标到指定位置，不执行点击"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a mouse move.
        
        Args:
            context: Execution context.
            params: Dict with x, y, duration, relative.
        
        Returns:
            ActionResult indicating success or failure.
        """
        x = params.get('x', 0)
        y = params.get('y', 0)
        duration = params.get('duration', 0.2)
        relative = params.get('relative', False)
        
        # Validate duration
        valid, msg = self.validate_type(duration, (int, float), 'duration')
        if not valid:
            return ActionResult(success=False, message=msg)
        if duration < 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'duration' must be >= 0, got {duration}"
            )
        
        try:
            if relative:
                current_x, current_y = pyautogui.position()
                x = current_x + x
                y = current_y + y
            
            pyautogui.moveTo(x, y, duration=duration)
            
            return ActionResult(
                success=True,
                message=f"移动成功: ({x}, {y})",
                data={'x': x, 'y': y}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"移动失败: {str(e)}"
            )
    
    def get_required_params(self) -> List[str]:
        return ['x', 'y']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'duration': 0.2,
            'relative': False
        }


class DragAction(BaseAction):
    """Simulate a mouse drag operation from start to end position."""
    action_type = "drag"
    display_name = "鼠标拖拽"
    description = "从起点拖拽到终点"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a mouse drag.
        
        Args:
            context: Execution context.
            params: Dict with start_x, start_y, end_x, end_y, duration, 
                   button, relative.
        
        Returns:
            ActionResult indicating success or failure.
        """
        start_x = params.get('start_x', 0)
        start_y = params.get('start_y', 0)
        end_x = params.get('end_x', 0)
        end_y = params.get('end_y', 0)
        duration = params.get('duration', 0.5)
        button = params.get('button', 'left')
        relative = params.get('relative', False)
        
        # Validate button
        valid, msg = self.validate_in(button, self.VALID_BUTTONS, 'button')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        # Validate duration
        valid, msg = self.validate_type(duration, (int, float), 'duration')
        if not valid:
            return ActionResult(success=False, message=msg)
        if duration < 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'duration' must be >= 0, got {duration}"
            )
        
        try:
            if relative:
                current_x, current_y = pyautogui.position()
                start_x = current_x
                start_y = current_y
            
            pyautogui.moveTo(start_x, start_y, duration=0.1)
            time.sleep(0.05)
            pyautogui.drag(end_x - start_x, end_y - start_y, duration=duration, button=button)
            
            return ActionResult(
                success=True,
                message=f"拖拽成功: ({start_x}, {start_y}) -> ({end_x}, {end_y})",
                data={'start': (start_x, start_y), 'end': (end_x, end_y)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"拖拽失败: {str(e)}"
            )
    
    def get_required_params(self) -> List[str]:
        return ['start_x', 'start_y', 'end_x', 'end_y']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'duration': 0.5,
            'button': 'left',
            'relative': False
        }
