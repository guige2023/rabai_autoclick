"""Image2 action module for RabAI AutoClick.

Provides additional image operations:
- ImageResizeAction: Resize image
- ImageRotateAction: Rotate image
- ImageFlipAction: Flip image
- ImageCropAction: Crop image
- ImageGrayscaleAction: Convert to grayscale
"""

from typing import Any, Dict, List, Tuple

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ImageResizeAction(BaseAction):
    """Resize image."""
    action_type = "image2_resize"
    display_name = "调整图片大小"
    description = "调整图片尺寸"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute resize.

        Args:
            context: Execution context.
            params: Dict with image_path, width, height, output_var.

        Returns:
            ActionResult with resized image path.
        """
        image_path = params.get('image_path', '')
        width = params.get('width', 100)
        height = params.get('height', 100)
        output_var = params.get('output_var', 'resized_image')

        valid, msg = self.validate_type(image_path, str, 'image_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            from PIL import Image

            resolved_path = context.resolve_value(image_path)
            resolved_width = int(context.resolve_value(width))
            resolved_height = int(context.resolve_value(height))

            image = Image.open(resolved_path)
            resized = image.resize((resolved_width, resolved_height))

            output_path = resolved_path.rsplit('.', 1)[0] + '_resized.png'
            resized.save(output_path)

            context.set(output_var, output_path)

            return ActionResult(
                success=True,
                message=f"图片调整大小: {resolved_width}x{resolved_height}",
                data={
                    'original': resolved_path,
                    'output': output_path,
                    'size': (resolved_width, resolved_height),
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="需要 Pillow 库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"调整图片大小失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['image_path', 'width', 'height']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'resized_image'}


class ImageRotateAction(BaseAction):
    """Rotate image."""
    action_type = "image2_rotate"
    display_name = "旋转图片"
    description = "旋转图片角度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute rotate.

        Args:
            context: Execution context.
            params: Dict with image_path, angle, output_var.

        Returns:
            ActionResult with rotated image path.
        """
        image_path = params.get('image_path', '')
        angle = params.get('angle', 90)
        output_var = params.get('output_var', 'rotated_image')

        valid, msg = self.validate_type(image_path, str, 'image_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            from PIL import Image

            resolved_path = context.resolve_value(image_path)
            resolved_angle = float(context.resolve_value(angle))

            image = Image.open(resolved_path)
            rotated = image.rotate(resolved_angle, expand=True)

            output_path = resolved_path.rsplit('.', 1)[0] + '_rotated.png'
            rotated.save(output_path)

            context.set(output_var, output_path)

            return ActionResult(
                success=True,
                message=f"图片旋转: {resolved_angle}°",
                data={
                    'original': resolved_path,
                    'output': output_path,
                    'angle': resolved_angle,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="需要 Pillow 库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"旋转图片失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['image_path', 'angle']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'rotated_image'}


class ImageFlipAction(BaseAction):
    """Flip image."""
    action_type = "image2_flip"
    display_name = "翻转图片"
    description = "翻转图片"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute flip.

        Args:
            context: Execution context.
            params: Dict with image_path, direction, output_var.

        Returns:
            ActionResult with flipped image path.
        """
        image_path = params.get('image_path', '')
        direction = params.get('direction', 'horizontal')
        output_var = params.get('output_var', 'flipped_image')

        valid, msg = self.validate_type(image_path, str, 'image_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            from PIL import Image

            resolved_path = context.resolve_value(image_path)
            resolved_direction = context.resolve_value(direction)

            image = Image.open(resolved_path)

            if resolved_direction == 'horizontal':
                flipped = image.transpose(Image.FLIP_LEFT_RIGHT)
            elif resolved_direction == 'vertical':
                flipped = image.transpose(Image.FLIP_TOP_BOTTOM)
            else:
                return ActionResult(
                    success=False,
                    message="方向必须是 'horizontal' 或 'vertical'"
                )

            output_path = resolved_path.rsplit('.', 1)[0] + '_flipped.png'
            flipped.save(output_path)

            context.set(output_var, output_path)

            return ActionResult(
                success=True,
                message=f"图片翻转: {resolved_direction}",
                data={
                    'original': resolved_path,
                    'output': output_path,
                    'direction': resolved_direction,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="需要 Pillow 库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"翻转图片失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['image_path', 'direction']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'flipped_image'}


class ImageCropAction(BaseAction):
    """Crop image."""
    action_type = "image2_crop"
    display_name = "裁剪图片"
    description = "裁剪图片区域"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute crop.

        Args:
            context: Execution context.
            params: Dict with image_path, left, top, right, bottom, output_var.

        Returns:
            ActionResult with cropped image path.
        """
        image_path = params.get('image_path', '')
        left = params.get('left', 0)
        top = params.get('top', 0)
        right = params.get('right', 100)
        bottom = params.get('bottom', 100)
        output_var = params.get('output_var', 'cropped_image')

        valid, msg = self.validate_type(image_path, str, 'image_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            from PIL import Image

            resolved_path = context.resolve_value(image_path)
            resolved_left = int(context.resolve_value(left))
            resolved_top = int(context.resolve_value(top))
            resolved_right = int(context.resolve_value(right))
            resolved_bottom = int(context.resolve_value(bottom))

            image = Image.open(resolved_path)
            cropped = image.crop((resolved_left, resolved_top, resolved_right, resolved_bottom))

            output_path = resolved_path.rsplit('.', 1)[0] + '_cropped.png'
            cropped.save(output_path)

            context.set(output_var, output_path)

            return ActionResult(
                success=True,
                message=f"图片裁剪: ({resolved_left},{resolved_top})-({resolved_right},{resolved_bottom})",
                data={
                    'original': resolved_path,
                    'output': output_path,
                    'box': (resolved_left, resolved_top, resolved_right, resolved_bottom),
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="需要 Pillow 库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"裁剪图片失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['image_path', 'left', 'top', 'right', 'bottom']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cropped_image'}


class ImageGrayscaleAction(BaseAction):
    """Convert to grayscale."""
    action_type = "image2_grayscale"
    display_name = "灰度图片"
    description = "将图片转换为灰度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute grayscale.

        Args:
            context: Execution context.
            params: Dict with image_path, output_var.

        Returns:
            ActionResult with grayscale image path.
        """
        image_path = params.get('image_path', '')
        output_var = params.get('output_var', 'grayscale_image')

        valid, msg = self.validate_type(image_path, str, 'image_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            from PIL import Image

            resolved_path = context.resolve_value(image_path)

            image = Image.open(resolved_path)
            grayscale = image.convert('L')

            output_path = resolved_path.rsplit('.', 1)[0] + '_gray.png'
            grayscale.save(output_path)

            context.set(output_var, output_path)

            return ActionResult(
                success=True,
                message=f"灰度图片保存: {output_path}",
                data={
                    'original': resolved_path,
                    'output': output_path,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="需要 Pillow 库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"灰度图片失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['image_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'grayscale_image'}