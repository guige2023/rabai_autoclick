import pyautogui
import time
from typing import Dict, Any
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ScreenshotAction(BaseAction):
    action_type = "screenshot"
    display_name = "截图"
    description = "截取屏幕指定区域并保存"
    
    def execute(self, context, params: Dict[str, Any]) -> ActionResult:
        region = params.get('region', None)
        save_path = params.get('save_path', None)
        
        try:
            if region and isinstance(region, str):
                parts = region.split(',')
                if len(parts) == 4:
                    region = tuple(int(p) for p in parts)
                else:
                    region = None
            
            screenshot = pyautogui.screenshot(region=region)
            
            if not save_path:
                default_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'screenshots')
                os.makedirs(default_dir, exist_ok=True)
                timestamp = time.strftime('%Y%m%d_%H%M%S')
                save_path = os.path.join(default_dir, f'screenshot_{timestamp}.png')
            
            save_dir = os.path.dirname(save_path)
            if save_dir:
                os.makedirs(save_dir, exist_ok=True)
            
            screenshot.save(save_path)
            return ActionResult(
                success=True,
                message=f"截图已保存: {save_path}",
                data={'path': save_path}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"截图失败: {str(e)}"
            )
    
    def get_required_params(self) -> list:
        return []
    
    def get_optional_params(self) -> dict:
        return {
            'region': None,
            'save_path': None
        }


class GetMousePosAction(BaseAction):
    action_type = "get_mouse_pos"
    display_name = "获取鼠标位置"
    description = "获取当前鼠标位置坐标"
    
    def execute(self, context, params: Dict[str, Any]) -> ActionResult:
        output_var = params.get('output_var', 'mouse_pos')
        
        try:
            x, y = pyautogui.position()
            
            return ActionResult(
                success=True,
                message=f"鼠标位置: ({x}, {y})",
                data={'x': x, 'y': y}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取鼠标位置失败: {str(e)}"
            )
    
    def get_required_params(self) -> list:
        return []
    
    def get_optional_params(self) -> dict:
        return {
            'output_var': 'mouse_pos'
        }


class AlertAction(BaseAction):
    action_type = "alert"
    display_name = "弹出提示"
    description = "显示提示对话框"
    
    def execute(self, context, params: Dict[str, Any]) -> ActionResult:
        title = params.get('title', 'RabAI AutoClick')
        message = params.get('message', '')
        alert_type = params.get('alert_type', 'info')
        
        try:
            if alert_type == 'confirm':
                result = pyautogui.confirm(text=message, title=title)
                return ActionResult(
                    success=True,
                    message=f"用户选择: {result}",
                    data={'result': result}
                )
            elif alert_type == 'prompt':
                result = pyautogui.prompt(text=message, title=title)
                return ActionResult(
                    success=True,
                    message=f"用户输入: {result}",
                    data={'result': result}
                )
            elif alert_type == 'password':
                result = pyautogui.password(text=message, title=title)
                return ActionResult(
                    success=True,
                    message="密码已输入",
                    data={'result': result}
                )
            else:
                pyautogui.alert(text=message, title=title)
                return ActionResult(
                    success=True,
                    message="提示已显示"
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"显示提示失败: {str(e)}"
            )
    
    def get_required_params(self) -> list:
        return ['message']
    
    def get_optional_params(self) -> dict:
        return {
            'title': 'RabAI AutoClick',
            'alert_type': 'info'
        }
