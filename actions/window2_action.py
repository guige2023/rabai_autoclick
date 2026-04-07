"""Window2 action module for RabAI AutoClick.

Provides additional window operations:
- WindowMinimizeAction: Minimize window
- WindowMaximizeAction: Maximize window
- WindowRestoreAction: Restore window
- WindowCenterAction: Center window
- WindowSetTransparencyAction: Set window transparency
"""

import pyautogui
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WindowMinimizeAction(BaseAction):
    """Minimize window."""
    action_type = "window2_minimize"
    display_name = "最小化窗口"
    description = "最小化当前窗口"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute minimize.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with minimize result.
        """
        output_var = params.get('output_var', 'window_result')

        try:
            import pygetwindow as gw

            active = gw.getActiveWindow()
            if active:
                active.minimize()
                context.set(output_var, True)
                return ActionResult(
                    success=True,
                    message=f"窗口已最小化: {active.title}",
                    data={'output_var': output_var}
                )
            else:
                return ActionResult(
                    success=False,
                    message="没有活动窗口"
                )
        except ImportError:
            return ActionResult(
                success=False,
                message="需要 pygetwindow 库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"最小化窗口失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'window_result'}


class WindowMaximizeAction(BaseAction):
    """Maximize window."""
    action_type = "window2_maximize"
    display_name = "最大化窗口"
    description = "最大化当前窗口"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute maximize.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with maximize result.
        """
        output_var = params.get('output_var', 'window_result')

        try:
            import pygetwindow as gw

            active = gw.getActiveWindow()
            if active:
                active.maximize()
                context.set(output_var, True)
                return ActionResult(
                    success=True,
                    message=f"窗口已最大化: {active.title}",
                    data={'output_var': output_var}
                )
            else:
                return ActionResult(
                    success=False,
                    message="没有活动窗口"
                )
        except ImportError:
            return ActionResult(
                success=False,
                message="需要 pygetwindow 库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"最大化窗口失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'window_result'}


class WindowRestoreAction(BaseAction):
    """Restore window."""
    action_type = "window2_restore"
    display_name = "还原窗口"
    description = "还原窗口到原始大小"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute restore.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with restore result.
        """
        output_var = params.get('output_var', 'window_result')

        try:
            import pygetwindow as gw

            active = gw.getActiveWindow()
            if active:
                active.restore()
                context.set(output_var, True)
                return ActionResult(
                    success=True,
                    message=f"窗口已还原: {active.title}",
                    data={'output_var': output_var}
                )
            else:
                return ActionResult(
                    success=False,
                    message="没有活动窗口"
                )
        except ImportError:
            return ActionResult(
                success=False,
                message="需要 pygetwindow 库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"还原窗口失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'window_result'}


class WindowCenterAction(BaseAction):
    """Center window."""
    action_type = "window2_center"
    display_name = "居中窗口"
    description = "将窗口移到屏幕中央"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute center.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with center result.
        """
        output_var = params.get('output_var', 'window_result')

        try:
            import pygetwindow as gw

            active = gw.getActiveWindow()
            if active:
                screen_w, screen_h = pyautogui.size()
                win_w, win_h = active.size
                x = (screen_w - win_w) // 2
                y = (screen_h - win_h) // 2
                active.moveTo(x, y)
                context.set(output_var, True)
                return ActionResult(
                    success=True,
                    message=f"窗口已居中",
                    data={'position': (x, y), 'output_var': output_var}
                )
            else:
                return ActionResult(
                    success=False,
                    message="没有活动窗口"
                )
        except ImportError:
            return ActionResult(
                success=False,
                message="需要 pygetwindow 库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"居中窗口失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'window_result'}


class WindowSetTransparencyAction(BaseAction):
    """Set window transparency."""
    action_type = "window2_transparency"
    display_name = "设置窗口透明度"
    description = "设置窗口透明度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set transparency.

        Args:
            context: Execution context.
            params: Dict with alpha, output_var.

        Returns:
            ActionResult with transparency result.
        """
        alpha = params.get('alpha', 1.0)
        output_var = params.get('output_var', 'window_result')

        try:
            import pywinstyles

            resolved_alpha = float(context.resolve_value(alpha))
            if not (0 <= resolved_alpha <= 1):
                return ActionResult(
                    success=False,
                    message="透明度必须在0-1之间"
                )

            hwnd = pyautogui.getActiveWindow()
            if hwnd:
                pywinstyles.set_opacity(hwnd, resolved_alpha)
                context.set(output_var, True)
                return ActionResult(
                    success=True,
                    message=f"透明度已设置为: {int(resolved_alpha * 100)}%",
                    data={'alpha': resolved_alpha, 'output_var': output_var}
                )
            else:
                return ActionResult(
                    success=False,
                    message="没有活动窗口"
                )
        except ImportError:
            return ActionResult(
                success=False,
                message="需要 pywinstyles 库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"设置透明度失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['alpha']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'window_result'}