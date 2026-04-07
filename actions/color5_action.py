"""Color5 action module for RabAI AutoClick.

Provides additional color operations:
- ColorRGBToHSLAction: RGB to HSL conversion
- ColorHSLToRGBAction: HSL to RGB conversion
- ColorRGBToHexAction: RGB to Hex conversion
- ColorHexToRGBAction: Hex to RGB conversion
- ColorBlendAction: Blend two colors
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ColorRGBToHSLAction(BaseAction):
    """RGB to HSL conversion."""
    action_type = "color5_rgb_to_hsl"
    display_name = "RGB转HSL"
    description = "将RGB颜色转换为HSL"
    version = "5.0"

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
            resolved_r = int(context.resolve_value(r)) if r else 0
            resolved_g = int(context.resolve_value(g)) if g else 0
            resolved_b = int(context.resolve_value(b)) if b else 0

            r_norm = resolved_r / 255
            g_norm = resolved_g / 255
            b_norm = resolved_b / 255

            max_val = max(r_norm, g_norm, b_norm)
            min_val = min(r_norm, g_norm, b_norm)
            l = (max_val + min_val) / 2

            if max_val == min_val:
                h = s = 0
            else:
                d = max_val - min_val
                s = l > 0.5 and d / (2 - max_val - min_val) or d / (max_val + min_val)

                if max_val == r_norm:
                    h = (g_norm - b_norm) / d + (g_norm < b_norm and 6 or 0)
                elif max_val == g_norm:
                    h = (b_norm - r_norm) / d + 2
                else:
                    h = (r_norm - g_norm) / d + 4
                h /= 6

            h = int(h * 360)
            s = int(s * 100)
            l = int(l * 100)

            result = f'hsl({h}, {s}%, {l}%)'

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"RGB转HSL: {result}",
                data={
                    'rgb': (resolved_r, resolved_g, resolved_b),
                    'hsl': (h, s, l),
                    'hsl_string': result,
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


class ColorHSLToRGBAction(BaseAction):
    """HSL to RGB conversion."""
    action_type = "color5_hsl_to_rgb"
    display_name = "HSL转RGB"
    description = "将HSL颜色转换为RGB"
    version = "5.0"

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
            resolved_h = int(context.resolve_value(h)) if h else 0
            resolved_s = int(context.resolve_value(s)) if s else 0
            resolved_l = int(context.resolve_value(l)) if l else 0

            h_norm = resolved_h / 360
            s_norm = resolved_s / 100
            l_norm = resolved_l / 100

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

                q = l_norm < 0.5 and l_norm * (1 + s_norm) or l_norm + s_norm - l_norm * s_norm
                p = 2 * l_norm - q
                r = hue_to_rgb(p, q, h_norm + 1/3)
                g = hue_to_rgb(p, q, h_norm)
                b = hue_to_rgb(p, q, h_norm - 1/3)

            result = (int(r * 255), int(g * 255), int(b * 255))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HSL转RGB: rgb({result[0]}, {result[1]}, {result[2]})",
                data={
                    'hsl': (resolved_h, resolved_s, resolved_l),
                    'rgb': result,
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


class ColorRGBToHexAction(BaseAction):
    """RGB to Hex conversion."""
    action_type = "color5_rgb_to_hex"
    display_name = "RGB转十六进制"
    description = "将RGB颜色转换为十六进制"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute RGB to Hex.

        Args:
            context: Execution context.
            params: Dict with r, g, b, output_var.

        Returns:
            ActionResult with Hex color.
        """
        r = params.get('r', 0)
        g = params.get('g', 0)
        b = params.get('b', 0)
        output_var = params.get('output_var', 'hex_color')

        try:
            resolved_r = int(context.resolve_value(r)) if r else 0
            resolved_g = int(context.resolve_value(g)) if g else 0
            resolved_b = int(context.resolve_value(b)) if b else 0

            result = '#{:02x}{:02x}{:02x}'.format(resolved_r, resolved_g, resolved_b)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"RGB转十六进制: {result}",
                data={
                    'rgb': (resolved_r, resolved_g, resolved_b),
                    'hex': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"RGB转十六进制失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['r', 'g', 'b']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hex_color'}


class ColorHexToRGBAction(BaseAction):
    """Hex to RGB conversion."""
    action_type = "color5_hex_to_rgb"
    display_name = "十六进制转RGB"
    description = "将十六进制颜色转换为RGB"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Hex to RGB.

        Args:
            context: Execution context.
            params: Dict with hex, output_var.

        Returns:
            ActionResult with RGB color.
        """
        hex_color = params.get('hex', '#000000')
        output_var = params.get('output_var', 'rgb_color')

        try:
            resolved = context.resolve_value(hex_color)
            hex_str = resolved.lstrip('#')

            if len(hex_str) == 3:
                hex_str = ''.join([c*2 for c in hex_str])

            r = int(hex_str[0:2], 16)
            g = int(hex_str[2:4], 16)
            b = int(hex_str[4:6], 16)

            result = (r, g, b)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"十六进制转RGB: rgb({r}, {g}, {b})",
                data={
                    'hex': resolved,
                    'rgb': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"十六进制转RGB失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['hex']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'rgb_color'}


class ColorBlendAction(BaseAction):
    """Blend two colors."""
    action_type = "color5_blend"
    display_name = "混合颜色"
    description = "混合两个颜色"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute blend colors.

        Args:
            context: Execution context.
            params: Dict with color1, color2, ratio, output_var.

        Returns:
            ActionResult with blended color.
        """
        color1 = params.get('color1', '#000000')
        color2 = params.get('color2', '#ffffff')
        ratio = params.get('ratio', 0.5)
        output_var = params.get('output_var', 'blended_color')

        try:
            resolved1 = context.resolve_value(color1)
            resolved2 = context.resolve_value(color2)
            resolved_ratio = float(context.resolve_value(ratio)) if ratio else 0.5

            hex1 = resolved1.lstrip('#')
            if len(hex1) == 3:
                hex1 = ''.join([c*2 for c in hex1])
            r1, g1, b1 = int(hex1[0:2], 16), int(hex1[2:4], 16), int(hex1[4:6], 16)

            hex2 = resolved2.lstrip('#')
            if len(hex2) == 3:
                hex2 = ''.join([c*2 for c in hex2])
            r2, g2, b2 = int(hex2[0:2], 16), int(hex2[2:4], 16), int(hex2[4:6], 16)

            r = int(r1 + (r2 - r1) * resolved_ratio)
            g = int(g1 + (g2 - g1) * resolved_ratio)
            b = int(b1 + (b2 - b1) * resolved_ratio)

            result = '#{:02x}{:02x}{:02x}'.format(r, g, b)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"混合颜色: {result}",
                data={
                    'color1': resolved1,
                    'color2': resolved2,
                    'ratio': resolved_ratio,
                    'blended': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"混合颜色失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['color1', 'color2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'ratio': 0.5, 'output_var': 'blended_color'}