"""Image action module for RabAI AutoClick.

Provides image operations:
- ImageResizeAction: Resize image
- ImageCropAction: Crop image
- ImageRotateAction: Rotate image
- ImageFlipAction: Flip image
"""

from PIL import Image
import os
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ImageResizeAction(BaseAction):
    """Resize image."""
    action_type = "image_resize"
    display_name = "调整图像大小"
    description = "调整图像大小"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute resize.

        Args:
            context: Execution context.
            params: Dict with image_path, width, height, output_path.

        Returns:
            ActionResult indicating success.
        """
        image_path = params.get('image_path', '')
        width = params.get('width', 100)
        height = params.get('height', 100)
        output_path = params.get('output_path', '')

        valid, msg = self.validate_type(image_path, str, 'image_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(image_path)
            resolved_width = context.resolve_value(width)
            resolved_height = context.resolve_value(height)

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"图像文件不存在: {resolved_path}"
                )

            with Image.open(resolved_path) as img:
                resized = img.resize((int(resolved_width), int(resolved_height)))

                if not output_path:
                    output_path = resolved_path

                resolved_output = context.resolve_value(output_path)
                resized.save(resolved_output)

            return ActionResult(
                success=True,
                message=f"图像已调整大小: {resolved_width}x{resolved_height}",
                data={
                    'width': resolved_width,
                    'height': resolved_height,
                    'output_path': resolved_output
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"调整图像大小失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['image_path', 'width', 'height']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_path': ''}


class ImageCropAction(BaseAction):
    """Crop image."""
    action_type = "image_crop"
    display_name = "裁剪图像"
    description = "裁剪图像"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute crop.

        Args:
            context: Execution context.
            params: Dict with image_path, left, top, right, bottom, output_path.

        Returns:
            ActionResult indicating success.
        """
        image_path = params.get('image_path', '')
        left = params.get('left', 0)
        top = params.get('top', 0)
        right = params.get('right', 100)
        bottom = params.get('bottom', 100)
        output_path = params.get('output_path', '')

        valid, msg = self.validate_type(image_path, str, 'image_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(image_path)

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"图像文件不存在: {resolved_path}"
                )

            resolved_left = int(context.resolve_value(left))
            resolved_top = int(context.resolve_value(top))
            resolved_right = int(context.resolve_value(right))
            resolved_bottom = int(context.resolve_value(bottom))

            with Image.open(resolved_path) as img:
                cropped = img.crop((resolved_left, resolved_top, resolved_right, resolved_bottom))

                if not output_path:
                    output_path = resolved_path

                resolved_output = context.resolve_value(output_path)
                cropped.save(resolved_output)

            return ActionResult(
                success=True,
                message=f"图像已裁剪",
                data={
                    'box': (resolved_left, resolved_top, resolved_right, resolved_bottom),
                    'output_path': resolved_output
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"裁剪图像失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['image_path', 'left', 'top', 'right', 'bottom']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_path': ''}


class ImageRotateAction(BaseAction):
    """Rotate image."""
    action_type = "image_rotate"
    display_name = "旋转图像"
    description = "旋转图像"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute rotate.

        Args:
            context: Execution context.
            params: Dict with image_path, angle, output_path.

        Returns:
            ActionResult indicating success.
        """
        image_path = params.get('image_path', '')
        angle = params.get('angle', 90)
        output_path = params.get('output_path', '')

        valid, msg = self.validate_type(image_path, str, 'image_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(image_path)
            resolved_angle = context.resolve_value(angle)

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"图像文件不存在: {resolved_path}"
                )

            with Image.open(resolved_path) as img:
                rotated = img.rotate(float(resolved_angle), expand=True)

                if not output_path:
                    output_path = resolved_path

                resolved_output = context.resolve_value(output_path)
                rotated.save(resolved_output)

            return ActionResult(
                success=True,
                message=f"图像已旋转: {resolved_angle}度",
                data={
                    'angle': resolved_angle,
                    'output_path': resolved_output
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"旋转图像失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['image_path', 'angle']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_path': ''}


class ImageFlipAction(BaseAction):
    """Flip image."""
    action_type = "image_flip"
    display_name = "翻转图像"
    description = "翻转图像"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute flip.

        Args:
            context: Execution context.
            params: Dict with image_path, direction, output_path.

        Returns:
            ActionResult indicating success.
        """
        image_path = params.get('image_path', '')
        direction = params.get('direction', 'horizontal')
        output_path = params.get('output_path', '')

        valid, msg = self.validate_type(image_path, str, 'image_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(image_path)
            resolved_direction = context.resolve_value(direction)

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"图像文件不存在: {resolved_path}"
                )

            with Image.open(resolved_path) as img:
                if resolved_direction == 'horizontal':
                    flipped = img.transpose(Image.FLIP_LEFT_RIGHT)
                elif resolved_direction == 'vertical':
                    flipped = img.transpose(Image.FLIP_TOP_BOTTOM)
                else:
                    return ActionResult(
                        success=False,
                        message=f"无效的翻转方向: {resolved_direction}"
                    )

                if not output_path:
                    output_path = resolved_path

                resolved_output = context.resolve_value(output_path)
                flipped.save(resolved_output)

            return ActionResult(
                success=True,
                message=f"图像已翻转: {resolved_direction}",
                data={
                    'direction': resolved_direction,
                    'output_path': resolved_output
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"翻转图像失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['image_path', 'direction']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_path': ''}