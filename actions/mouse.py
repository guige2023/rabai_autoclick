import pyautogui
import time
from typing import Dict, Any
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ClickAction(BaseAction):
    action_type = "click"
    display_name = "鼠标单击"
    description = "模拟鼠标单击操作"
    
    def execute(self, context, params: Dict[str, Any]) -> ActionResult:
        x = params.get('x', None)
        y = params.get('y', None)
        button = params.get('button', 'left')
        clicks = params.get('clicks', 1)
        move_duration = params.get('move_duration', 0.2)
        
        try:
            if x is not None and y is not None:
                pyautogui.moveTo(x, y, duration=move_duration)
                time.sleep(0.05)
            
            pyautogui.click(x=x, y=y, clicks=clicks, button=button)
            
            return ActionResult(
                success=True,
                message=f"点击成功: ({x}, {y}) {button}",
                data={'x': x, 'y': y, 'button': button, 'clicks': clicks}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"点击失败: {str(e)}"
            )
    
    def get_required_params(self) -> list:
        return []
    
    def get_optional_params(self) -> dict:
        return {
            'x': None,
            'y': None,
            'button': 'left',
            'clicks': 1,
            'move_duration': 0.2
        }


class DoubleClickAction(BaseAction):
    action_type = "double_click"
    display_name = "鼠标双击"
    description = "模拟鼠标双击操作"
    
    def execute(self, context, params: Dict[str, Any]) -> ActionResult:
        x = params.get('x', None)
        y = params.get('y', None)
        button = params.get('button', 'left')
        move_duration = params.get('move_duration', 0.2)
        
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
    
    def get_required_params(self) -> list:
        return []
    
    def get_optional_params(self) -> dict:
        return {
            'x': None,
            'y': None,
            'button': 'left',
            'move_duration': 0.2
        }


class ScrollAction(BaseAction):
    action_type = "scroll"
    display_name = "鼠标滚轮"
    description = "模拟鼠标滚轮滚动操作"
    
    def execute(self, context, params: Dict[str, Any]) -> ActionResult:
        clicks = params.get('clicks', 1)
        direction = params.get('direction', 'down')
        x = params.get('x', None)
        y = params.get('y', None)
        move_duration = params.get('move_duration', 0.2)
        
        if direction == 'up':
            clicks = abs(clicks)
        elif direction == 'down':
            clicks = -abs(clicks)
        
        try:
            if x is not None and y is not None:
                pyautogui.moveTo(x, y, duration=move_duration)
                time.sleep(0.05)
            
            pyautogui.scroll(clicks, x=x, y=y)
            
            return ActionResult(
                success=True,
                message=f"滚动成功: {clicks} 格 ({direction})",
                data={'clicks': clicks, 'direction': direction}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"滚动失败: {str(e)}"
            )
    
    def get_required_params(self) -> list:
        return []
    
    def get_optional_params(self) -> dict:
        return {
            'clicks': 3,
            'direction': 'down',
            'x': None,
            'y': None,
            'move_duration': 0.2
        }


class MouseMoveAction(BaseAction):
    action_type = "mouse_move"
    display_name = "鼠标移动"
    description = "移动鼠标到指定位置，不执行点击"
    
    def execute(self, context, params: Dict[str, Any]) -> ActionResult:
        x = params.get('x', 0)
        y = params.get('y', 0)
        duration = params.get('duration', 0.2)
        relative = params.get('relative', False)
        
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
    
    def get_required_params(self) -> list:
        return ['x', 'y']
    
    def get_optional_params(self) -> dict:
        return {
            'duration': 0.2,
            'relative': False
        }


class DragAction(BaseAction):
    action_type = "drag"
    display_name = "鼠标拖拽"
    description = "从起点拖拽到终点"
    
    def execute(self, context, params: Dict[str, Any]) -> ActionResult:
        start_x = params.get('start_x', 0)
        start_y = params.get('start_y', 0)
        end_x = params.get('end_x', 0)
        end_y = params.get('end_y', 0)
        duration = params.get('duration', 0.5)
        button = params.get('button', 'left')
        relative = params.get('relative', False)
        
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
    
    def get_required_params(self) -> list:
        return ['start_x', 'start_y', 'end_x', 'end_y']
    
    def get_optional_params(self) -> dict:
        return {
            'duration': 0.5,
            'button': 'left',
            'relative': False
        }
