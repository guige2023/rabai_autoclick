"""Image4 action module for RabAI AutoClick.

Provides additional image operations:
- ImageResizeAction: Resize image
- ImageRotateAction: Rotate image
- ImageFlipAction: Flip image
- ImageCropAction: Crop image
- ImageBlurAction: Blur image
"""

from typing import Any, Dict, List, Tuple

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ImageResizeAction(BaseAction):
    """Resize image."""
    action_type = "image4_resize"
    display_name = "图片缩放"
    description = "调整图片大小"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute image resize.

        Args:
            context: Execution context.
            params: Dict with file_path, width, height, output_var.

        Returns:
            ActionResult with resized image path.
        """
        file_path = params.get('file_path', '')
        width = params.get('width', 100)
        height = params.get('height', 100)
        output_var = params.get('output_var', 'resized_path')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_width = int(context.resolve_value(width))
            resolved_height = int(context.resolve_value(height))

            try:
                from PIL import Image
                img = Image.open(resolved_path)
                resized = img.resize((resolved_width, resolved_height), Image.Resampling.LANCZOS)

                output_path = resolved_path.replace('.', '_resized.')
                resized.save(output_path)

            except ImportError:
                return ActionResult(
                    success=False,
                    message="图片缩放失败: 未安装Pillow库"
                )

            context.set(output_var, output_path)

            return ActionResult(
                success=True,
                message=f"图片缩放完成: {output_path}",
                data={
                    'original': resolved_path,
                    'resized': output_path,
                    'size': (resolved_width, resolved_height),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"图片缩放失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path', 'width', 'height']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'resized_path'}


class ImageRotateAction(BaseAction):
    """Rotate image."""
    action_type = "image4_rotate"
    display_name = "图片旋转"
    description = "旋转图片"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute image rotate.

        Args:
            context: Execution context.
            params: Dict with file_path, angle, output_var.

        Returns:
            ActionResult with rotated image path.
        """
        file_path = params.get('file_path', '')
        angle = params.get('angle', 90)
        output_var = params.get('output_var', 'rotated_path')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_angle = int(context.resolve_value(angle))

            try:
                from PIL import Image
                img = Image.open(resolved_path)
                rotated = img.rotate(resolved_angle, expand=True)

                output_path = resolved_path.replace('.', '_rotated.')
                rotated.save(output_path)

            except ImportError:
                return ActionResult(
                    success=False,
                    message="图片旋转失败: 未安装Pillow库"
                )

            context.set(output_var, output_path)

            return ActionResult(
                success=True,
                message=f"图片旋转完成: {output_path}",
                data={
                    'original': resolved_path,
                    'rotated': output_path,
                    'angle': resolved_angle,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"图片旋转失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'angle': 90, 'output_var': 'rotated_path'}


class ImageFlipAction(BaseAction):
    """Flip image."""
    action_type = "image4_flip"
    display_name = "图片翻转"
    description = "翻转图片"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute image flip.

        Args:
            context: Execution context.
            params: Dict with file_path, direction, output_var.

        Returns:
            ActionResult with flipped image path.
        """
        file_path = params.get('file_path', '')
        direction = params.get('direction', 'horizontal')
        output_var = params.get('output_var', 'flipped_path')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_direction = context.resolve_value(direction)

            try:
                from PIL import Image
                img = Image.open(resolved_path)

                if resolved_direction == 'horizontal':
                    flipped = img.transpose(Image.Transpose.FLIP_LEFT_RIGHT)
                elif resolved_direction == 'vertical':
                    flipped = img.transpose(Image.Transpose.FLIP_TOP_BOTTOM)
                else:
                    return ActionResult(
                        success=False,
                        message=f"图片翻转失败: 无效方向 '{resolved_direction}'"
                    )

                output_path = resolved_path.replace('.', '_flipped.')
                flipped.save(output_path)

            except ImportError:
                return ActionResult(
                    success=False,
                    message="图片翻转失败: 未安装Pillow库"
                )

            context.set(output_var, output_path)

            return ActionResult(
                success=True,
                message=f"图片翻转完成: {output_path}",
                data={
                    'original': resolved_path,
                    'flipped': output_path,
                    'direction': resolved_direction,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"图片翻转失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'direction': 'horizontal', 'output_var': 'flipped_path'}


class ImageCropAction(BaseAction):
    """Crop image."""
    action_type = "image4_crop"
    display_name = "图片裁剪"
    description = "裁剪图片"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute image crop.

        Args:
            context: Execution context.
            params: Dict with file_path, left, top, right, bottom, output_var.

        Returns:
            ActionResult with cropped image path.
        """
        file_path = params.get('file_path', '')
        left = params.get('left', 0)
        top = params.get('top', 0)
        right = params.get('right', 100)
        bottom = params.get('bottom', 100)
        output_var = params.get('output_var', 'cropped_path')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_box = (
                int(context.resolve_value(left)),
                int(context.resolve_value(top)),
                int(context.resolve_value(right)),
                int(context.resolve_value(bottom))
            )

            try:
                from PIL import Image
                img = Image.open(resolved_path)
                cropped = img.crop(resolved_box)

                output_path = resolved_path.replace('.', '_cropped.')
                cropped.save(output_path)

            except ImportError:
                return ActionResult(
                    success=False,
                    message="图片裁剪失败: 未安装Pillow库"
                )

            context.set(output_var, output_path)

            return ActionResult(
                success=True,
                message=f"图片裁剪完成: {output_path}",
                data={
                    'original': resolved_path,
                    'cropped': output_path,
                    'box': resolved_box,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"图片裁剪失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path', 'left', 'top', 'right', 'bottom']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cropped_path'}


class ImageBlurAction(BaseAction):
    """Blur image."""
    action_type = "image4_blur"
    display_name = "图片模糊"
    description = "模糊图片"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute image blur.

        Args:
            context: Execution context.
            params: Dict with file_path, radius, output_var.

        Returns:
            ActionResult with blurred image path.
        """
        file_path = params.get('file_path', '')
        radius = params.get('radius', 5)
        output_var = params.get('output_var', 'blurred_path')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_radius = int(context.resolve_value(radius))

            try:
                from PIL import Image, ImageFilter
                img = Image.open(resolved_path)
                blurred = img.filter(ImageFilter.GaussianBlur(resolved_radius))

                output_path = resolved_path.replace('.', '_blurred.')
                blurred.save(output_path)

            except ImportError:
                return ActionResult(
                    success=False,
                    message="图片模糊失败: 未安装Pillow库"
                )

            context.set(output_var, output_path)

            return ActionResult(
                success=True,
                message=f"图片模糊完成: {output_path}",
                data={
                    'original': resolved_path,
                    'blurred': output_path,
                    'radius': resolved_radius,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"图片模糊失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'radius': 5, 'output_var': 'blurred_path'}