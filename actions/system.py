"""System action module for RabAI AutoClick.

Provides system-level automation actions:
- ScreenshotAction: Capture screen or region
- GetMousePosAction: Get current mouse position
- AlertAction: Display alert/confirm/prompt dialogs
"""

import pyautogui
import io
import base64
from typing import Any, Dict, List, Optional, Tuple

from rabai_autoclick.core.base_action import BaseAction, ActionResult


# Valid alert types
VALID_ALERT_TYPES: List[str] = ['info', 'confirm', 'prompt', 'password']

# Valid region format: (left, top, width, height)
REGION_TUPLE_LEN: int = 4


class ScreenshotAction(BaseAction):
    """Capture a screenshot of the screen or a specified region."""
    action_type = "screenshot"
    display_name = "截图"
    description = "截取屏幕指定区域并保存"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a screenshot capture.
        
        Args:
            context: Execution context.
            params: Dict with region (tuple), save_path (str).
            
        Returns:
            ActionResult with image data (base64) or save path.
        """
        region = params.get('region', None)
        save_path = params.get('save_path', None)
        
        # Validate region if provided
        if region is not None:
            valid, msg = self.validate_type(region, tuple, 'region')
            if not valid:
                return ActionResult(success=False, message=msg)
            if len(region) != REGION_TUPLE_LEN:
                return ActionResult(
                    success=False,
                    message=f"Parameter 'region' must be a tuple of 4 integers "
                            f"(left, top, width, height), got length {len(region)}"
                )
            for i, val in enumerate(region):
                if not isinstance(val, (int, float)):
                    return ActionResult(
                        success=False,
                        message=f"Parameter 'region' element {i} must be a number, "
                                f"got {type(val).__name__}"
                    )
        
        # Validate save_path if provided
        if save_path is not None:
            valid, msg = self.validate_type(save_path, str, 'save_path')
            if not valid:
                return ActionResult(success=False, message=msg)
        
        try:
            screenshot = pyautogui.screenshot(region=region)
            
            if save_path:
                screenshot.save(save_path)
                return ActionResult(
                    success=True,
                    message=f"截图已保存: {save_path}",
                    data={'path': save_path}
                )
            else:
                buffer = io.BytesIO()
                screenshot.save(buffer, format='PNG')
                img_base64 = base64.b64encode(buffer.getvalue()).decode()
                
                return ActionResult(
                    success=True,
                    message="截图成功",
                    data={'image': img_base64, 'size': screenshot.size}
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"截图失败: {str(e)}"
            )
    
    def get_required_params(self) -> List[str]:
        return []
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'region': None,
            'save_path': None
        }


class GetMousePosAction(BaseAction):
    """Get the current mouse cursor position."""
    action_type = "get_mouse_pos"
    display_name = "获取鼠标位置"
    description = "获取当前鼠标位置坐标"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute getting the current mouse position.
        
        Args:
            context: Execution context.
            params: Dict with output_var.
            
        Returns:
            ActionResult with x, y coordinates.
        """
        output_var = params.get('output_var', 'mouse_pos')
        
        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)
        
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
    
    def get_required_params(self) -> List[str]:
        return []
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'output_var': 'mouse_pos'
        }


class AlertAction(BaseAction):
    """Display alert, confirm, or prompt dialogs."""
    action_type = "alert"
    display_name = "弹出提示"
    description = "显示提示对话框"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute an alert/confirm/prompt dialog.
        
        Args:
            context: Execution context.
            params: Dict with title, message, alert_type.
            
        Returns:
            ActionResult with user response (if applicable).
        """
        title = params.get('title', 'RabAI AutoClick')
        message = params.get('message', '')
        alert_type = params.get('alert_type', 'info')
        
        # Validate title
        valid, msg = self.validate_type(title, str, 'title')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        # Validate message
        valid, msg = self.validate_type(message, str, 'message')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        # Validate alert_type
        valid, msg = self.validate_in(
            alert_type, VALID_ALERT_TYPES, 'alert_type'
        )
        if not valid:
            return ActionResult(success=False, message=msg)
        
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
            else:  # 'info'
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
    
    def get_required_params(self) -> List[str]:
        return ['message']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'title': 'RabAI AutoClick',
            'alert_type': 'info'
        }
