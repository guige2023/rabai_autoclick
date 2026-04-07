"""Color action module for RabAI AutoClick.

Provides color operations:
- ColorRgbToHexAction: Convert RGB to HEX
- ColorHexToRgbAction: Convert HEX to RGB
- ColorRgbToHslAction: Convert RGB to HSL
- ColorHslToRgbAction: Convert HSL to RGB
"""

import re
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ColorRgbToHexAction(BaseAction):
    """Convert RGB to HEX."""
    action_type = "color_rgb_to_hex"
    display_name = "RGB转HEX"
    description = "将RGB颜色转换为HEX"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute RGB to HEX conversion.

        Args:
            context: Execution context.
            params: Dict with r, g, b, output_var.

        Returns:
            ActionResult with HEX color.
        """
        r = params.get('r', 0)
        g = params.get('g', 0)
        b = params.get('b', 0)
        output_var = params.get('output_var', 'color_result')

        try:
            resolved_r = int(context.resolve_value(r))
            resolved_g = int(context.resolve_value(g))
            resolved_b = int(context.resolve_value(b))

            # Clamp values
            resolved_r = max(0, min(255, resolved_r))
            resolved_g = max(0, min(255, resolved_g))
            resolved_b = max(0, min(255, resolved_b))

            result = f"#{resolved_r:02x}{resolved_g:02x}{resolved_b:02x}".upper()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"RGB转HEX: {result}",
                data={
                    'result': result,
                    'rgb': (resolved_r, resolved_g, resolved_b),
                    'output_var': output_var
                }
            )
        except (ValueError, TypeError) as e:
            return ActionResult(
                success=False,
                message=f"RGB转HEX失败: 无效的RGB值 - {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"RGB转HEX失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['r', 'g', 'b']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'color_result'}


class ColorHexToRgbAction(BaseAction):
    """Convert HEX to RGB."""
    action_type = "color_hex_to_rgb"
    display_name = "HEX转RGB"
    description = "将HEX颜色转换为RGB"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HEX to RGB conversion.

        Args:
            context: Execution context.
            params: Dict with hex, output_var.

        Returns:
            ActionResult with RGB tuple.
        """
        hex_color = params.get('hex', '#000000')
        output_var = params.get('output_var', 'color_result')

        valid, msg = self.validate_type(hex_color, str, 'hex')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(hex_color)

            # Remove # if present
            hex_val = resolved.lstrip('#')

            # Handle shorthand (e.g., #FFF)
            if len(hex_val) == 3:
                hex_val = ''.join([c*2 for c in hex_val])

            if len(hex_val) != 6:
                return ActionResult(
                    success=False,
                    message="无效的HEX颜色值"
                )

            r = int(hex_val[0:2], 16)
            g = int(hex_val[2:4], 16)
            b = int(hex_val[4:6], 16)

            result = (r, g, b)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HEX转RGB: {result}",
                data={
                    'result': result,
                    'hex': resolved,
                    'output_var': output_var
                }
            )
        except ValueError:
            return ActionResult(
                success=False,
                message="无效的HEX颜色值"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HEX转RGB失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['hex']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'color_result'}


class ColorRgbToHslAction(BaseAction):
    """Convert RGB to HSL."""
    action_type = "color_rgb_to_hsl"
    display_name = "RGB转HSL"
    description = "将RGB颜色转换为HSL"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute RGB to HSL conversion.

        Args:
            context: Execution context.
            params: Dict with r, g, b, output_var.

        Returns:
            ActionResult with HSL tuple.
        """
        r = params.get('r', 0)
        g = params.get('g', 0)
        b = params.get('b', 0)
        output_var = params.get('output_var', 'color_result')

        try:
            resolved_r = int(context.resolve_value(r)) / 255.0
            resolved_g = int(context.resolve_value(g)) / 255.0
            resolved_b = int(context.resolve_value(b)) / 255.0

            max_c = max(resolved_r, resolved_g, resolved_b)
            min_c = min(resolved_r, resolved_g, resolved_b)
            diff = max_c - min_c

            # Lightness
            l = (max_c + min_c) / 2

            # Hue
            if diff == 0:
                h = 0
            elif max_c == resolved_r:
                h = (60 * ((resolved_g - resolved_b) / diff) + 360) % 360
            elif max_c == resolved_g:
                h = (60 * ((resolved_b - resolved_r) / diff) + 120) % 360
            else:
                h = (60 * ((resolved_r - resolved_g) / diff) + 240) % 360

            # Saturation
            if diff == 0:
                s = 0
            else:
                s = diff / (1 - abs(2 * l - 1))

            result = (round(h), round(s * 100), round(l * 100))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"RGB转HSL: {result}",
                data={
                    'result': result,
                    'rgb': (int(resolved_r * 255), int(resolved_g * 255), int(resolved_b * 255)),
                    'output_var': output_var
                }
            )
        except (ValueError, TypeError) as e:
            return ActionResult(
                success=False,
                message=f"RGB转HSL失败: 无效的RGB值 - {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"RGB转HSL失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['r', 'g', 'b']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'color_result'}


class ColorHslToRgbAction(BaseAction):
    """Convert HSL to RGB."""
    action_type = "color_hsl_to_rgb"
    display_name = "HSL转RGB"
    description = "将HSL颜色转换为RGB"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HSL to RGB conversion.

        Args:
            context: Execution context.
            params: Dict with h, s, l, output_var.

        Returns:
            ActionResult with RGB tuple.
        """
        h = params.get('h', 0)
        s = params.get('s', 0)
        l = params.get('l', 0)
        output_var = params.get('output_var', 'color_result')

        try:
            resolved_h = float(context.resolve_value(h))
            resolved_s = float(context.resolve_value(s)) / 100.0
            resolved_l = float(context.resolve_value(l)) / 100.0

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
                r = hue_to_rgb(p, q, (resolved_h / 360) + 1/3)
                g = hue_to_rgb(p, q, resolved_h / 360)
                b = hue_to_rgb(p, q, (resolved_h / 360) - 1/3)

            result = (int(r * 255), int(g * 255), int(b * 255))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HSL转RGB: {result}",
                data={
                    'result': result,
                    'hsl': (resolved_h, resolved_s * 100, resolved_l * 100),
                    'output_var': output_var
                }
            )
        except (ValueError, TypeError) as e:
            return ActionResult(
                success=False,
                message=f"HSL转RGB失败: 无效的HSL值 - {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HSL转RGB失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['h', 's', 'l']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'color_result'}