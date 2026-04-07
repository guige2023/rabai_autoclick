"""Color4 action module for RabAI AutoClick.

Provides additional color operations:
- ColorLightenAction: Lighten color
- ColorDarkenAction: Darken color
- ColorComplementaryAction: Get complementary color
- ColorRgbToHslAction: RGB to HSL
- ColorHslToRgbAction: HSL to RGB
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ColorLightenAction(BaseAction):
    """Lighten color."""
    action_type = "color4_lighten"
    display_name = "颜色变亮"
    description = "使颜色变亮"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute lighten.

        Args:
            context: Execution context.
            params: Dict with color, amount, output_var.

        Returns:
            ActionResult with lightened color.
        """
        color = params.get('color', '#000000')
        amount = params.get('amount', 20)
        output_var = params.get('output_var', 'lightened_color')

        try:
            resolved_color = context.resolve_value(color)
            resolved_amount = int(context.resolve_value(amount))

            hex_color = resolved_color.lstrip('#')
            rgb = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))

            rgb = tuple(min(255, c + resolved_amount) for c in rgb)
            result = '#{:02x}{:02x}{:02x}'.format(*rgb)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"颜色变亮: {resolved_color} -> {result}",
                data={
                    'original': resolved_color,
                    'amount': resolved_amount,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"颜色变亮失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['color']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'amount': 20, 'output_var': 'lightened_color'}


class ColorDarkenAction(BaseAction):
    """Darken color."""
    action_type = "color4_darken"
    display_name = "颜色变暗"
    description = "使颜色变暗"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute darken.

        Args:
            context: Execution context.
            params: Dict with color, amount, output_var.

        Returns:
            ActionResult with darkened color.
        """
        color = params.get('color', '#ffffff')
        amount = params.get('amount', 20)
        output_var = params.get('output_var', 'darkened_color')

        try:
            resolved_color = context.resolve_value(color)
            resolved_amount = int(context.resolve_value(amount))

            hex_color = resolved_color.lstrip('#')
            rgb = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))

            rgb = tuple(max(0, c - resolved_amount) for c in rgb)
            result = '#{:02x}{:02x}{:02x}'.format(*rgb)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"颜色变暗: {resolved_color} -> {result}",
                data={
                    'original': resolved_color,
                    'amount': resolved_amount,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"颜色变暗失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['color']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'amount': 20, 'output_var': 'darkened_color'}


class ColorComplementaryAction(BaseAction):
    """Get complementary color."""
    action_type = "color4_complementary"
    display_name = "互补色"
    description = "获取互补颜色"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute complementary.

        Args:
            context: Execution context.
            params: Dict with color, output_var.

        Returns:
            ActionResult with complementary color.
        """
        color = params.get('color', '#000000')
        output_var = params.get('output_var', 'complementary_color')

        try:
            resolved_color = context.resolve_value(color)

            hex_color = resolved_color.lstrip('#')
            rgb = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))

            rgb = tuple(255 - c for c in rgb)
            result = '#{:02x}{:02x}{:02x}'.format(*rgb)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"互补色: {resolved_color} -> {result}",
                data={
                    'original': resolved_color,
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


class ColorRgbToHslAction(BaseAction):
    """RGB to HSL."""
    action_type = "color4_rgb_to_hsl"
    display_name = "RGB转HSL"
    description = "将RGB颜色转换为HSL"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute RGB to HSL.

        Args:
            context: Execution context.
            params: Dict with r, g, b, output_var.

        Returns:
            ActionResult with HSL color.
        """
        r = params.get('r', 0)
        g = params.get('g', 0)
        b = params.get('b', 0)
        output_var = params.get('output_var', 'hsl_color')

        try:
            resolved_r = int(context.resolve_value(r))
            resolved_g = int(context.resolve_value(g))
            resolved_b = int(context.resolve_value(b))

            r_norm = resolved_r / 255.0
            g_norm = resolved_g / 255.0
            b_norm = resolved_b / 255.0

            max_val = max(r_norm, g_norm, b_norm)
            min_val = min(r_norm, g_norm, b_norm)
            l = (max_val + min_val) / 2

            if max_val == min_val:
                h = s = 0
            else:
                d = max_val - min_val
                s = d / (2 - max_val - min_val) if l > 0.5 else d / (max_val + min_val)

                if max_val == r_norm:
                    h = (g_norm - b_norm) / d + (6 if g_norm < b_norm else 0)
                elif max_val == g_norm:
                    h = (b_norm - r_norm) / d + 2
                else:
                    h = (r_norm - g_norm) / d + 4

                h /= 6

            h = round(h * 360)
            s = round(s * 100)
            l = round(l * 100)

            result = f"hsl({h}, {s}%, {l}%)"
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"RGB转HSL: ({resolved_r}, {resolved_g}, {resolved_b}) -> {result}",
                data={
                    'rgb': (resolved_r, resolved_g, resolved_b),
                    'result': result,
                    'hsl': (h, s, l),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"RGB转HSL失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['r', 'g', 'b']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hsl_color'}


class ColorHslToRgbAction(BaseAction):
    """HSL to RGB."""
    action_type = "color4_hsl_to_rgb"
    display_name = "HSL转RGB"
    description = "将HSL颜色转换为RGB"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HSL to RGB.

        Args:
            context: Execution context.
            params: Dict with h, s, l, output_var.

        Returns:
            ActionResult with RGB color.
        """
        h = params.get('h', 0)
        s = params.get('s', 0)
        l = params.get('l', 0)
        output_var = params.get('output_var', 'rgb_color')

        try:
            resolved_h = int(context.resolve_value(h))
            resolved_s = int(context.resolve_value(s))
            resolved_l = int(context.resolve_value(l))

            s_norm = resolved_s / 100.0
            l_norm = resolved_l / 100.0

            if s_norm == 0:
                r = g = b = l_norm
            else:
                def hue_to_rgb(p, q, t):
                    if t < 0:
                        t += 1
                    if t > 1:
                        t -= 1
                    if t < 1/6:
                        return p + (q - p) * 6 * t
                    if t < 1/2:
                        return q
                    if t < 2/3:
                        return p + (q - p) * (2/3 - t) * 6
                    return p

                q = l_norm * (1 + s_norm) if l_norm < 0.5 else l_norm + s_norm - l_norm * s_norm
                p = 2 * l_norm - q
                h_norm = resolved_h / 360.0

                r = hue_to_rgb(p, q, h_norm + 1/3)
                g = hue_to_rgb(p, q, h_norm)
                b = hue_to_rgb(p, q, h_norm - 1/3)

            r = round(r * 255)
            g = round(g * 255)
            b = round(b * 255)

            result = f"rgb({r}, {g}, {b})"
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HSL转RGB: ({resolved_h}, {resolved_s}%, {resolved_l}%) -> {result}",
                data={
                    'hsl': (resolved_h, resolved_s, resolved_l),
                    'result': result,
                    'rgb': (r, g, b),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HSL转RGB失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['h', 's', 'l']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'rgb_color'}