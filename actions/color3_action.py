"""Color3 action module for RabAI AutoClick.

Provides additional color operations:
- ColorInvertAction: Invert color
- ColorLightenAction: Lighten color
- ColorDarkenAction: Darken color
- ColorDesaturateAction: Desaturate color
- ColorComplementaryAction: Get complementary color
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ColorInvertAction(BaseAction):
    """Invert color."""
    action_type = "color3_invert"
    display_name = "反转颜色"
    description = "反转颜色值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute invert color.

        Args:
            context: Execution context.
            params: Dict with color, output_var.

        Returns:
            ActionResult with inverted color.
        """
        color = params.get('color', '#000000')
        output_var = params.get('output_var', 'inverted_color')

        valid, msg = self.validate_type(color, str, 'color')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(color)

            if resolved.startswith('#'):
                resolved = resolved[1:]
                if len(resolved) == 6:
                    r = 255 - int(resolved[0:2], 16)
                    g = 255 - int(resolved[2:4], 16)
                    b = 255 - int(resolved[4:6], 16)
                    result = '#{:02x}{:02x}{:02x}'.format(r, g, b)
                else:
                    return ActionResult(success=False, message="颜色格式不正确")
            else:
                return ActionResult(success=False, message="仅支持十六进制颜色格式")

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"反转颜色: {resolved} -> {result}",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"反转颜色失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['color']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'inverted_color'}


class ColorLightenAction(BaseAction):
    """Lighten color."""
    action_type = "color3_lighten"
    display_name = "提亮颜色"
    description = "提亮颜色值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute lighten color.

        Args:
            context: Execution context.
            params: Dict with color, amount, output_var.

        Returns:
            ActionResult with lightened color.
        """
        color = params.get('color', '#000000')
        amount = params.get('amount', 30)
        output_var = params.get('output_var', 'lightened_color')

        valid, msg = self.validate_type(color, str, 'color')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(color)
            resolved_amount = int(context.resolve_value(amount))

            if resolved.startswith('#'):
                resolved = resolved[1:]
                if len(resolved) == 6:
                    r = min(255, int(resolved[0:2], 16) + resolved_amount)
                    g = min(255, int(resolved[2:4], 16) + resolved_amount)
                    b = min(255, int(resolved[4:6], 16) + resolved_amount)
                    result = '#{:02x}{:02x}{:02x}'.format(r, g, b)
                else:
                    return ActionResult(success=False, message="颜色格式不正确")
            else:
                return ActionResult(success=False, message="仅支持十六进制颜色格式")

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"提亮颜色: {result}",
                data={
                    'original': resolved,
                    'amount': resolved_amount,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"提亮颜色失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['color']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'amount': 30, 'output_var': 'lightened_color'}


class ColorDarkenAction(BaseAction):
    """Darken color."""
    action_type = "color3_darken"
    display_name = "暗化颜色"
    description = "暗化颜色值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute darken color.

        Args:
            context: Execution context.
            params: Dict with color, amount, output_var.

        Returns:
            ActionResult with darkened color.
        """
        color = params.get('color', '#ffffff')
        amount = params.get('amount', 30)
        output_var = params.get('output_var', 'darkened_color')

        valid, msg = self.validate_type(color, str, 'color')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(color)
            resolved_amount = int(context.resolve_value(amount))

            if resolved.startswith('#'):
                resolved = resolved[1:]
                if len(resolved) == 6:
                    r = max(0, int(resolved[0:2], 16) - resolved_amount)
                    g = max(0, int(resolved[2:4], 16) - resolved_amount)
                    b = max(0, int(resolved[4:6], 16) - resolved_amount)
                    result = '#{:02x}{:02x}{:02x}'.format(r, g, b)
                else:
                    return ActionResult(success=False, message="颜色格式不正确")
            else:
                return ActionResult(success=False, message="仅支持十六进制颜色格式")

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"暗化颜色: {result}",
                data={
                    'original': resolved,
                    'amount': resolved_amount,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"暗化颜色失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['color']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'amount': 30, 'output_var': 'darkened_color'}


class ColorDesaturateAction(BaseAction):
    """Desaturate color."""
    action_type = "color3_desaturate"
    display_name = "去饱和度"
    description = "降低颜色饱和度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute desaturate color.

        Args:
            context: Execution context.
            params: Dict with color, amount, output_var.

        Returns:
            ActionResult with desaturated color.
        """
        color = params.get('color', '#000000')
        amount = params.get('amount', 50)
        output_var = params.get('output_var', 'desaturated_color')

        valid, msg = self.validate_type(color, str, 'color')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(color)
            resolved_amount = int(context.resolve_value(amount)) / 100

            if resolved.startswith('#'):
                resolved = resolved[1:]
                if len(resolved) == 6:
                    r = int(resolved[0:2], 16)
                    g = int(resolved[2:4], 16)
                    b = int(resolved[4:6], 16)

                    gray = int((r + g + b) / 3)
                    r = int(r + (gray - r) * resolved_amount)
                    g = int(g + (gray - g) * resolved_amount)
                    b = int(b + (gray - b) * resolved_amount)

                    result = '#{:02x}{:02x}{:02x}'.format(
                        max(0, min(255, r)),
                        max(0, min(255, g)),
                        max(0, min(255, b))
                    )
                else:
                    return ActionResult(success=False, message="颜色格式不正确")
            else:
                return ActionResult(success=False, message="仅支持十六进制颜色格式")

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"去饱和度: {result}",
                data={
                    'original': resolved,
                    'amount': resolved_amount,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"去饱和度失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['color']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'amount': 50, 'output_var': 'desaturated_color'}


class ColorComplementaryAction(BaseAction):
    """Get complementary color."""
    action_type = "color3_complementary"
    display_name = "互补色"
    description = "获取颜色的互补色"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute complementary color.

        Args:
            context: Execution context.
            params: Dict with color, output_var.

        Returns:
            ActionResult with complementary color.
        """
        color = params.get('color', '#000000')
        output_var = params.get('output_var', 'complementary_color')

        valid, msg = self.validate_type(color, str, 'color')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(color)

            if resolved.startswith('#'):
                resolved = resolved[1:]
                if len(resolved) == 6:
                    r = 255 - int(resolved[0:2], 16)
                    g = 255 - int(resolved[2:4], 16)
                    b = 255 - int(resolved[4:6], 16)
                    result = '#{:02x}{:02x}{:02x}'.format(r, g, b)
                else:
                    return ActionResult(success=False, message="颜色格式不正确")
            else:
                return ActionResult(success=False, message="仅支持十六进制颜色格式")

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"互补色: {result}",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取互补色失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['color']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'complementary_color'}
