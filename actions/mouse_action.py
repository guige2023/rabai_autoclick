"""Mouse action module for RabAI AutoClick.

Provides mouse operations:
- MouseClickAction: Click at position
- MouseDoubleClickAction: Double click at position
- MouseRightClickAction: Right click at position
- MouseMoveAction: Move mouse to position
- MouseGetPositionAction: Get current mouse position
"""

from typing import Any, Dict, List, Tuple

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MouseClickAction(BaseAction):
    """Click at position."""
    action_type = "mouse_click"
    display_name = "鼠标点击"
    description = "在指定位置点击鼠标"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute mouse click.

        Args:
            context: Execution context.
            params: Dict with x, y, button, output_var.

        Returns:
            ActionResult with click status.
        """
        x = params.get('x', 0)
        y = params.get('y', 0)
        button = params.get('button', 'left')
        output_var = params.get('output_var', 'click_status')

        try:
            import pyautogui

            resolved_x = int(context.resolve_value(x))
            resolved_y = int(context.resolve_value(y))
            resolved_button = context.resolve_value(button) if button else 'left'

            pyautogui.click(resolved_x, resolved_y, button=resolved_button)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"鼠标点击完成: ({resolved_x}, {resolved_y})",
                data={
                    'position': (resolved_x, resolved_y),
                    'button': resolved_button,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="鼠标点击失败: 未安装pyautogui库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"鼠标点击失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['x', 'y']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'button': 'left', 'output_var': 'click_status'}


class MouseDoubleClickAction(BaseAction):
    """Double click at position."""
    action_type = "mouse_double_click"
    display_name = "鼠标双击"
    description = "在指定位置双击鼠标"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute mouse double click.

        Args:
            context: Execution context.
            params: Dict with x, y, button, output_var.

        Returns:
            ActionResult with double click status.
        """
        x = params.get('x', 0)
        y = params.get('y', 0)
        button = params.get('button', 'left')
        output_var = params.get('output_var', 'double_click_status')

        try:
            import pyautogui

            resolved_x = int(context.resolve_value(x))
            resolved_y = int(context.resolve_value(y))
            resolved_button = context.resolve_value(button) if button else 'left'

            pyautogui.doubleClick(resolved_x, resolved_y, button=resolved_button)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"鼠标双击完成: ({resolved_x}, {resolved_y})",
                data={
                    'position': (resolved_x, resolved_y),
                    'button': resolved_button,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="鼠标双击失败: 未安装pyautogui库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"鼠标双击失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['x', 'y']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'button': 'left', 'output_var': 'double_click_status'}


class MouseRightClickAction(BaseAction):
    """Right click at position."""
    action_type = "mouse_right_click"
    display_name = "鼠标右键点击"
    description = "在指定位置右键点击"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute mouse right click.

        Args:
            context: Execution context.
            params: Dict with x, y, output_var.

        Returns:
            ActionResult with right click status.
        """
        x = params.get('x', 0)
        y = params.get('y', 0)
        output_var = params.get('output_var', 'right_click_status')

        try:
            import pyautogui

            resolved_x = int(context.resolve_value(x))
            resolved_y = int(context.resolve_value(y))

            pyautogui.click(resolved_x, resolved_y, button='right')
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"鼠标右键点击完成: ({resolved_x}, {resolved_y})",
                data={
                    'position': (resolved_x, resolved_y),
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="鼠标右键点击失败: 未安装pyautogui库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"鼠标右键点击失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['x', 'y']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'right_click_status'}


class MouseMoveAction(BaseAction):
    """Move mouse to position."""
    action_type = "mouse_move"
    display_name = "鼠标移动"
    description = "移动鼠标到指定位置"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute mouse move.

        Args:
            context: Execution context.
            params: Dict with x, y, duration, output_var.

        Returns:
            ActionResult with move status.
        """
        x = params.get('x', 0)
        y = params.get('y', 0)
        duration = params.get('duration', 0)
        output_var = params.get('output_var', 'move_status')

        try:
            import pyautogui

            resolved_x = int(context.resolve_value(x))
            resolved_y = int(context.resolve_value(y))
            resolved_duration = float(context.resolve_value(duration)) if duration else 0

            pyautogui.moveTo(resolved_x, resolved_y, duration=resolved_duration)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"鼠标移动完成: ({resolved_x}, {resolved_y})",
                data={
                    'position': (resolved_x, resolved_y),
                    'duration': resolved_duration,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="鼠标移动失败: 未安装pyautogui库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"鼠标移动失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['x', 'y']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'duration': 0, 'output_var': 'move_status'}


class MouseGetPositionAction(BaseAction):
    """Get current mouse position."""
    action_type = "mouse_get_position"
    display_name = "获取鼠标位置"
    description = "获取当前鼠标位置"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get mouse position.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with current position.
        """
        output_var = params.get('output_var', 'mouse_position')

        try:
            import pyautogui

            x, y = pyautogui.position()
            context.set(output_var, (x, y))

            return ActionResult(
                success=True,
                message=f"获取鼠标位置: ({x}, {y})",
                data={
                    'position': (x, y),
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="获取鼠标位置失败: 未安装pyautogui库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取鼠标位置失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'mouse_position'}