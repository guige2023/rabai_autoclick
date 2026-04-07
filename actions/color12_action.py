"""Color12 action module for RabAI AutoClick.

Provides additional color operations:
- ColorRGBToHexAction: Convert RGB to HEX
- ColorHexToRGBAction: Convert HEX to RGB
- ColorHSLToRGBAction: Convert HSL to RGB
- ColorLightenAction: Lighten color
- ColorDarkenAction: Darken color
- ColorBlendAction: Blend two colors
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ColorRGBToHexAction(BaseAction):
    """Convert RGB to HEX."""
    action_type = "color12_rgb_to_hex"
    display_name = "RGB转HEX"
    description = "RGB转HEX颜色"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute RGB to HEX.

        Args:
            context: Execution context.
            params: Dict with r, g, b, output_var.

        Returns:
            ActionResult with HEX color.
        """
        r = params.get('r', 0)
        g = params.get('g', 0)
        b = params.get('b', 0)
        output_var = params.get('output_var', 'hex_color')

        try:
            resolved_r = int(context.resolve_value(r)) if r else 0
            resolved_g = int(context.resolve_value(g)) if g else 0
            resolved_b = int(context.resolve_value(b)) if b else 0

            # Clamp values
            resolved_r = max(0, min(255, resolved_r))
            resolved_g = max(0, min(255, resolved_g))
            resolved_b = max(0, min(255, resolved_b))

            result = f'#{resolved_r:02x}{resolved_g:02x}{resolved_b:02x}'

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"RGB转HEX: {result}",
                data={
                    'r': resolved_r,
                    'g': resolved_g,
                    'b': resolved_b,
                    'hex': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"RGB转HEX失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['r', 'g', 'b']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hex_color'}


class ColorHexToRGBAction(BaseAction):
    """Convert HEX to RGB."""
    action_type = "color12_hex_to_rgb"
    display_name = "HEX转RGB"
    description = "HEX转RGB颜色"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HEX to RGB.

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

            # Remove # if present
            if resolved.startswith('#'):
                resolved = resolved[1:]

            # Handle 3-digit hex
            if len(resolved) == 3:
                resolved = ''.join([c*2 for c in resolved])

            if len(resolved) != 6:
                return ActionResult(
                    success=False,
                    message=f"无效的HEX颜色: {hex_color}"
                )

            r = int(resolved[0:2], 16)
            g = int(resolved[2:4], 16)
            b = int(resolved[4:6], 16)

            result = {'r': r, 'g': g, 'b': b}

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HEX转RGB: rgb({r}, {g}, {b})",
                data={
                    'hex': resolved,
                    'rgb': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HEX转RGB失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['hex']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'rgb_color'}


class ColorHSLToRGBAction(BaseAction):
    """Convert HSL to RGB."""
    action_type = "color12_hsl_to_rgb"
    display_name = "HSL转RGB"
    description = "HSL转RGB颜色"
    version = "12.0"

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
            resolved_h = float(context.resolve_value(h)) if h else 0
            resolved_s = float(context.resolve_value(s)) if s else 0
            resolved_l = float(context.resolve_value(l)) if l else 0

            # Normalize values
            resolved_h = resolved_h % 360
            resolved_s = max(0, min(100, resolved_s)) / 100
            resolved_l = max(0, min(100, resolved_l)) / 100

            # Calculate RGB
            if resolved_s == 0:
                r = g = b = resolved_l
            else:
                def hue_to_rgb(p, q, t):
                    if t < 0: t += 1
                    if t > 1: t -= 1
                    if t < 1/6: return p + (q - p) * 6 * t
                    if t < 1/2: return q
                    if t < 2/3: return p + (q - p) * (2/3 - t) * 6
                    return p

                q = resolved_l * (1 + resolved_s) if resolved_l < 0.5 else resolved_l + resolved_s - resolved_l * resolved_s
                p = 2 * resolved_l - q
                r = hue_to_rgb(p, q, resolved_h / 360 + 1/3)
                g = hue_to_rgb(p, q, resolved_h / 360)
                b = hue_to_rgb(p, q, resolved_h / 360 - 1/3)

            result = {
                'r': int(r * 255),
                'g': int(g * 255),
                'b': int(b * 255)
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HSL转RGB: rgb({result['r']}, {result['g']}, {result['b']})",
                data={
                    'h': resolved_h,
                    's': resolved_s * 100,
                    'l': resolved_l * 100,
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


class ColorLightenAction(BaseAction):
    """Lighten color."""
    action_type = "color12_lighten"
    display_name = "颜色变亮"
    description = "使颜色变亮"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute lighten.

        Args:
            context: Execution context.
            params: Dict with hex, amount, output_var.

        Returns:
            ActionResult with lightened color.
        """
        hex_color = params.get('hex', '#000000')
        amount = params.get('amount', 10)
        output_var = params.get('output_var', 'lightened_color')

        try:
            resolved = context.resolve_value(hex_color)

            # Remove # if present
            if resolved.startswith('#'):
                resolved = resolved[1:]

            # Handle 3-digit hex
            if len(resolved) == 3:
                resolved = ''.join([c*2 for c in resolved])

            r = int(resolved[0:2], 16)
            g = int(resolved[2:4], 16)
            b = int(resolved[4:6], 16)

            resolved_amount = int(context.resolve_value(amount)) if amount else 10
            amount_factor = resolved_amount / 100

            r = min(255, int(r + (255 - r) * amount_factor))
            g = min(255, int(g + (255 - g) * amount_factor))
            b = min(255, int(b + (255 - b) * amount_factor))

            result = f'#{r:02x}{g:02x}{b:02x}'

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"颜色变亮: {result}",
                data={
                    'original': hex_color,
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
        return ['hex', 'amount']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'lightened_color'}


class ColorDarkenAction(BaseAction):
    """Darken color."""
    action_type = "color12_darken"
    display_name = "颜色变暗"
    description = "使颜色变暗"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute darken.

        Args:
            context: Execution context.
            params: Dict with hex, amount, output_var.

        Returns:
            ActionResult with darkened color.
        """
        hex_color = params.get('hex', '#000000')
        amount = params.get('amount', 10)
        output_var = params.get('output_var', 'darkened_color')

        try:
            resolved = context.resolve_value(hex_color)

            # Remove # if present
            if resolved.startswith('#'):
                resolved = resolved[1:]

            # Handle 3-digit hex
            if len(resolved) == 3:
                resolved = ''.join([c*2 for c in resolved])

            r = int(resolved[0:2], 16)
            g = int(resolved[2:4], 16)
            b = int(resolved[4:6], 16)

            resolved_amount = int(context.resolve_value(amount)) if amount else 10
            amount_factor = resolved_amount / 100

            r = max(0, int(r * (1 - amount_factor)))
            g = max(0, int(g * (1 - amount_factor)))
            b = max(0, int(b * (1 - amount_factor)))

            result = f'#{r:02x}{g:02x}{b:02x}'

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"颜色变暗: {result}",
                data={
                    'original': hex_color,
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
        return ['hex', 'amount']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'darkened_color'}


class ColorBlendAction(BaseAction):
    """Blend two colors."""
    action_type = "color12_blend"
    display_name = "混合颜色"
    description = "混合两种颜色"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute blend.

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

            # Remove # if present
            for color in [resolved1, resolved2]:
                if color.startswith('#'):
                    color = color[1:]
                if len(color) == 3:
                    color = ''.join([c*2 for c in color])

            r1 = int(resolved1[0:2], 16)
            g1 = int(resolved1[2:4], 16)
            b1 = int(resolved1[4:6], 16)

            r2 = int(resolved2[0:2], 16)
            g2 = int(resolved2[2:4], 16)
            b2 = int(resolved2[4:6], 16)

            r = int(r1 * resolved_ratio + r2 * (1 - resolved_ratio))
            g = int(g1 * resolved_ratio + g2 * (1 - resolved_ratio))
            b = int(b1 * resolved_ratio + b2 * (1 - resolved_ratio))

            result = f'#{r:02x}{g:02x}{b:02x}'

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"混合颜色: {result}",
                data={
                    'color1': color1,
                    'color2': color2,
                    'ratio': resolved_ratio,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"混合颜色失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['color1', 'color2', 'ratio']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'blended_color'}