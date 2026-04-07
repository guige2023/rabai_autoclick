"""Color2 action module for RabAI AutoClick.

Provides additional color operations:
- ColorHsvToRgbAction: HSV to RGB
- ColorRgbToHsvAction: RGB to HSV
- ColorHexToRgbAction: Hex to RGB
- ColorRgbToHexAction: RGB to Hex
- ColorBlendAction: Blend two colors
"""

from typing import Any, Dict, List, Tuple

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ColorHsvToRgbAction(BaseAction):
    """HSV to RGB."""
    action_type = "color_hsv_to_rgb"
    display_name = "HSV转RGB"
    description = "将HSV颜色转换为RGB"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HSV to RGB.

        Args:
            context: Execution context.
            params: Dict with h, s, v, output_var.

        Returns:
            ActionResult with RGB tuple.
        """
        h = params.get('h', 0)
        s = params.get('s', 0)
        v = params.get('v', 0)
        output_var = params.get('output_var', 'rgb_color')

        try:
            resolved_h = float(context.resolve_value(h))
            resolved_s = float(context.resolve_value(s))
            resolved_v = float(context.resolve_value(v))

            if not (0 <= resolved_h <= 360):
                return ActionResult(success=False, message="色相必须在0-360之间")
            if not (0 <= resolved_s <= 1):
                return ActionResult(success=False, message="饱和度必须在0-1之间")
            if not (0 <= resolved_v <= 1):
                return ActionResult(success=False, message="亮度必须在0-1之间")

            h = resolved_h / 360
            s = resolved_s
            v = resolved_v

            i = int(h * 6)
            f = h * 6 - i
            p = v * (1 - s)
            q = v * (1 - f * s)
            t = v * (1 - (1 - f) * s)

            i = i % 6

            if i == 0:
                r, g, b = v, t, p
            elif i == 1:
                r, g, b = q, v, p
            elif i == 2:
                r, g, b = p, v, t
            elif i == 3:
                r, g, b = p, q, v
            elif i == 4:
                r, g, b = t, p, v
            else:
                r, g, b = v, p, q

            result = (int(r * 255), int(g * 255), int(b * 255))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HSV转RGB: {result}",
                data={
                    'hsv': (resolved_h, resolved_s, resolved_v),
                    'rgb': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HSV转RGB失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['h', 's', 'v']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'rgb_color'}


class ColorRgbToHsvAction(BaseAction):
    """RGB to HSV."""
    action_type = "color_rgb_to_hsv"
    display_name = "RGB转HSV"
    description = "将RGB颜色转换为HSV"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute RGB to HSV.

        Args:
            context: Execution context.
            params: Dict with r, g, b, output_var.

        Returns:
            ActionResult with HSV tuple.
        """
        r = params.get('r', 0)
        g = params.get('g', 0)
        b = params.get('b', 0)
        output_var = params.get('output_var', 'hsv_color')

        try:
            resolved_r = float(context.resolve_value(r))
            resolved_g = float(context.resolve_value(g))
            resolved_b = float(context.resolve_value(b))

            if not (0 <= resolved_r <= 255):
                return ActionResult(success=False, message="红色分量必须在0-255之间")
            if not (0 <= resolved_g <= 255):
                return ActionResult(success=False, message="绿色分量必须在0-255之间")
            if not (0 <= resolved_b <= 255):
                return ActionResult(success=False, message="蓝色分量必须在0-255之间")

            r, g, b = resolved_r / 255, resolved_g / 255, resolved_b / 255

            max_c = max(r, g, b)
            min_c = min(r, g, b)
            diff = max_c - min_c

            if max_c == min_c:
                h = 0
            elif max_c == r:
                h = (60 * ((g - b) / diff) + 360) % 360
            elif max_c == g:
                h = (60 * ((b - r) / diff) + 120) % 360
            else:
                h = (60 * ((r - g) / diff) + 240) % 360

            s = 0 if max_c == 0 else (diff / max_c)
            v = max_c

            result = (round(h, 2), round(s, 2), round(v, 2))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"RGB转HSV: {result}",
                data={
                    'rgb': (resolved_r, resolved_g, resolved_b),
                    'hsv': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"RGB转HSV失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['r', 'g', 'b']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hsv_color'}


class ColorHexToRgbAction(BaseAction):
    """Hex to RGB."""
    action_type = "color_hex_to_rgb"
    display_name = "十六进制转RGB"
    description = "将十六进制颜色转换为RGB"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Hex to RGB.

        Args:
            context: Execution context.
            params: Dict with hex_color, output_var.

        Returns:
            ActionResult with RGB tuple.
        """
        hex_color = params.get('hex_color', '#000000')
        output_var = params.get('output_var', 'rgb_color')

        try:
            resolved = str(context.resolve_value(hex_color)).strip()

            if resolved.startswith('#'):
                resolved = resolved[1:]

            if len(resolved) == 3:
                resolved = ''.join([c * 2 for c in resolved])

            if len(resolved) != 6:
                return ActionResult(success=False, message="无效的十六进制颜色格式")

            r = int(resolved[0:2], 16)
            g = int(resolved[2:4], 16)
            b = int(resolved[4:6], 16)

            result = (r, g, b)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"十六进制转RGB: {result}",
                data={
                    'hex': hex_color,
                    'rgb': result,
                    'output_var': output_var
                }
            )
        except ValueError:
            return ActionResult(success=False, message="无效的十六进制颜色格式")
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"十六进制转RGB失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['hex_color']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'rgb_color'}


class ColorRgbToHexAction(BaseAction):
    """RGB to Hex."""
    action_type = "color_rgb_to_hex"
    display_name = "RGB转十六进制"
    description = "将RGB颜色转换为十六进制"

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
            ActionResult with hex string.
        """
        r = params.get('r', 0)
        g = params.get('g', 0)
        b = params.get('b', 0)
        output_var = params.get('output_var', 'hex_color')

        try:
            resolved_r = int(context.resolve_value(r))
            resolved_g = int(context.resolve_value(g))
            resolved_b = int(context.resolve_value(b))

            if not (0 <= resolved_r <= 255):
                return ActionResult(success=False, message="红色分量必须在0-255之间")
            if not (0 <= resolved_g <= 255):
                return ActionResult(success=False, message="绿色分量必须在0-255之间")
            if not (0 <= resolved_b <= 255):
                return ActionResult(success=False, message="蓝色分量必须在0-255之间")

            result = '#{:02X}{:02X}{:02X}'.format(resolved_r, resolved_g, resolved_b)
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


class ColorBlendAction(BaseAction):
    """Blend two colors."""
    action_type = "color_blend"
    display_name = "混合颜色"
    description = "混合两种颜色"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute color blend.

        Args:
            context: Execution context.
            params: Dict with color1, color2, ratio, output_var.

        Returns:
            ActionResult with blended color.
        """
        color1 = params.get('color1', '#000000')
        color2 = params.get('color2', '#FFFFFF')
        ratio = params.get('ratio', 0.5)
        output_var = params.get('output_var', 'blended_color')

        try:
            resolved_color1 = str(context.resolve_value(color1))
            resolved_color2 = str(context.resolve_value(color2))
            resolved_ratio = float(context.resolve_value(ratio))

            if resolved_ratio < 0 or resolved_ratio > 1:
                return ActionResult(success=False, message="混合比例必须在0-1之间")

            def hex_to_rgb(hex_str):
                h = hex_str.strip()
                if h.startswith('#'):
                    h = h[1:]
                if len(h) == 3:
                    h = ''.join([c * 2 for c in h])
                return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))

            rgb1 = hex_to_rgb(resolved_color1)
            rgb2 = hex_to_rgb(resolved_color2)

            r = int(rgb1[0] * (1 - resolved_ratio) + rgb2[0] * resolved_ratio)
            g = int(rgb1[1] * (1 - resolved_ratio) + rgb2[1] * resolved_ratio)
            b = int(rgb1[2] * (1 - resolved_ratio) + rgb2[2] * resolved_ratio)

            result = '#{:02X}{:02X}{:02X}'.format(r, g, b)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"混合颜色: {result}",
                data={
                    'color1': resolved_color1,
                    'color2': resolved_color2,
                    'ratio': resolved_ratio,
                    'result': result,
                    'output_var': output_var
                }
            )
        except ValueError:
            return ActionResult(success=False, message="无效的颜色格式")
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"混合颜色失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['color1', 'color2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'ratio': 0.5, 'output_var': 'blended_color'}