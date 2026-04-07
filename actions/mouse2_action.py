"""Mouse2 action module for RabAI AutoClick.

Provides advanced mouse operations:
- MouseDoubleClickAction: Double click
- MouseRightClickAction: Right click
- MouseDragAction: Drag mouse
- MouseScrollAction: Scroll mouse
"""

import subprocess
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MouseDoubleClickAction(BaseAction):
    """Double click."""
    action_type = "mouse_double_click"
    display_name = "双击"
    description = "双击鼠标"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute double click.

        Args:
            context: Execution context.
            params: Dict with x, y.

        Returns:
            ActionResult indicating success.
        """
        x = params.get('x', 0)
        y = params.get('y', 0)

        try:
            resolved_x = int(context.resolve_value(x))
            resolved_y = int(context.resolve_value(y))

            # Use cliclick on macOS
            subprocess.run(['cliclick', 'dc', f'{resolved_x},{resolved_y}'], capture_output=True)

            return ActionResult(
                success=True,
                message=f"已双击: ({resolved_x}, {resolved_y})"
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="双击失败: cliclick未安装"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"双击失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['x', 'y']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class MouseRightClickAction(BaseAction):
    """Right click."""
    action_type = "mouse_right_click"
    display_name = "右键点击"
    description = "鼠标右键点击"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute right click.

        Args:
            context: Execution context.
            params: Dict with x, y.

        Returns:
            ActionResult indicating success.
        """
        x = params.get('x', 0)
        y = params.get('y', 0)

        try:
            resolved_x = int(context.resolve_value(x))
            resolved_y = int(context.resolve_value(y))

            # Use cliclick on macOS
            subprocess.run(['cliclick', 'rc', f'{resolved_x},{resolved_y}'], capture_output=True)

            return ActionResult(
                success=True,
                message=f"已右键点击: ({resolved_x}, {resolved_y})"
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="右键点击失败: cliclick未安装"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"右键点击失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['x', 'y']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class MouseDragAction(BaseAction):
    """Drag mouse."""
    action_type = "mouse_drag"
    display_name = "拖拽鼠标"
    description = "鼠标拖拽操作"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute drag.

        Args:
            context: Execution context.
            params: Dict with start_x, start_y, end_x, end_y.

        Returns:
            ActionResult indicating success.
        """
        start_x = params.get('start_x', 0)
        start_y = params.get('start_y', 0)
        end_x = params.get('end_x', 100)
        end_y = params.get('end_y', 100)

        try:
            resolved_start_x = int(context.resolve_value(start_x))
            resolved_start_y = int(context.resolve_value(start_y))
            resolved_end_x = int(context.resolve_value(end_x))
            resolved_end_y = int(context.resolve_value(end_y))

            # Use cliclick on macOS for drag
            subprocess.run(
                ['cliclick', 'dd', f'{resolved_start_x},{resolved_start_y}', 'du', f'{resolved_end_x},{resolved_end_y}'],
                capture_output=True
            )

            return ActionResult(
                success=True,
                message=f"已拖拽: ({resolved_start_x},{resolved_start_y}) -> ({resolved_end_x},{resolved_end_y})"
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="拖拽失败: cliclick未安装"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"拖拽失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['start_x', 'start_y', 'end_x', 'end_y']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class MouseScrollAction(BaseAction):
    """Scroll mouse."""
    action_type = "mouse_scroll"
    display_name = "滚动鼠标"
    description = "鼠标滚动操作"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute scroll.

        Args:
            context: Execution context.
            params: Dict with x, y, amount.

        Returns:
            ActionResult indicating success.
        """
        x = params.get('x', 0)
        y = params.get('y', 0)
        amount = params.get('amount', 5)

        try:
            resolved_x = int(context.resolve_value(x))
            resolved_y = int(context.resolve_value(y))
            resolved_amount = int(context.resolve_value(amount))

            # Use cliclick on macOS for scroll (positive = down, negative = up)
            subprocess.run(
                ['cliclick', 's', f'{resolved_x},{resolved_y},{resolved_amount}'],
                capture_output=True
            )

            return ActionResult(
                success=True,
                message=f"已滚动: {resolved_amount} at ({resolved_x}, {resolved_y})"
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="滚动失败: cliclick未安装"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"滚动失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['x', 'y', 'amount']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}