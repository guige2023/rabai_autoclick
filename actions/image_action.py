"""Image processing action module for RabAI AutoClick.

Provides image operations:
- ImageResizeAction: Resize image
- ImageCropAction: Crop image
- ImageRotateAction: Rotate image
- ImageConvertAction: Convert image format
- ImageInfoAction: Get image info
- ImageThumbnailAction: Create thumbnail
"""

from __future__ import annotations

import sys
import os
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ImageResizeAction(BaseAction):
    """Resize image."""
    action_type = "image_resize"
    display_name = "图片缩放"
    description = "缩放图片"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute image resize."""
        input_path = params.get('input_path', '')
        output_path = params.get('output_path', '')
        width = params.get('width', None)
        height = params.get('height', None)
        maintain_aspect = params.get('maintain_aspect', True)
        output_var = params.get('output_var', 'resize_result')

        if not input_path or not output_path:
            return ActionResult(success=False, message="input_path and output_path are required")

        try:
            from PIL import Image

            resolved_input = context.resolve_value(input_path) if context else input_path
            resolved_output = context.resolve_value(output_path) if context else output_path
            resolved_w = context.resolve_value(width) if context else width
            resolved_h = context.resolve_value(height) if context else height
            resolved_aspect = context.resolve_value(maintain_aspect) if context else maintain_aspect

            img = Image.open(resolved_input)

            if resolved_aspect and resolved_w and not resolved_h:
                ratio = resolved_w / img.width
                resolved_h = int(img.height * ratio)
            elif resolved_aspect and resolved_h and not resolved_w:
                ratio = resolved_h / img.height
                resolved_w = int(img.width * ratio)

            size = (resolved_w or img.width, resolved_h or img.height)
            resized = img.resize(size, Image.Resampling.LANCZOS)

            _os.makedirs(_os.path.dirname(resolved_output) or '.', exist_ok=True)
            resized.save(resolved_output)

            result = {'output_path': resolved_output, 'size': size}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Resized to {size}", data=result)
        except ImportError:
            return ActionResult(success=False, message="Pillow not installed. Run: pip install Pillow")
        except Exception as e:
            return ActionResult(success=False, message=f"Image resize error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['input_path', 'output_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'width': None, 'height': None, 'maintain_aspect': True, 'output_var': 'resize_result'}


class ImageCropAction(BaseAction):
    """Crop image."""
    action_type = "image_crop"
    display_name = "图片裁剪"
    description = "裁剪图片"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute image crop."""
        input_path = params.get('input_path', '')
        output_path = params.get('output_path', '')
        left = params.get('left', 0)
        top = params.get('top', 0)
        right = params.get('right', None)
        bottom = params.get('bottom', None)
        output_var = params.get('output_var', 'crop_result')

        if not input_path or not output_path:
            return ActionResult(success=False, message="input_path and output_path are required")

        try:
            from PIL import Image

            resolved_input = context.resolve_value(input_path) if context else input_path
            resolved_output = context.resolve_value(output_path) if context else output_path
            resolved_left = context.resolve_value(left) if context else left
            resolved_top = context.resolve_value(top) if context else top
            resolved_right = context.resolve_value(right) if context else right
            resolved_bottom = context.resolve_value(bottom) if context else bottom

            img = Image.open(resolved_input)

            if resolved_right is None:
                resolved_right = img.width
            if resolved_bottom is None:
                resolved_bottom = img.height

            cropped = img.crop((resolved_left, resolved_top, resolved_right, resolved_bottom))

            _os.makedirs(_os.path.dirname(resolved_output) or '.', exist_ok=True)
            cropped.save(resolved_output)

            result = {'output_path': resolved_output, 'size': cropped.size}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Cropped to {cropped.size}", data=result)
        except ImportError:
            return ActionResult(success=False, message="Pillow not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"Image crop error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['input_path', 'output_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'left': 0, 'top': 0, 'right': None, 'bottom': None, 'output_var': 'crop_result'}


class ImageRotateAction(BaseAction):
    """Rotate image."""
    action_type = "image_rotate"
    display_name = "图片旋转"
    description = "旋转图片"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute image rotate."""
        input_path = params.get('input_path', '')
        output_path = params.get('output_path', '')
        angle = params.get('angle', 90)
        expand = params.get('expand', True)
        output_var = params.get('output_var', 'rotate_result')

        if not input_path or not output_path:
            return ActionResult(success=False, message="input_path and output_path are required")

        try:
            from PIL import Image

            resolved_input = context.resolve_value(input_path) if context else input_path
            resolved_output = context.resolve_value(output_path) if context else output_path
            resolved_angle = context.resolve_value(angle) if context else angle
            resolved_expand = context.resolve_value(expand) if context else expand

            img = Image.open(resolved_input)
            rotated = img.rotate(resolved_angle, expand=resolved_expand)

            _os.makedirs(_os.path.dirname(resolved_output) or '.', exist_ok=True)
            rotated.save(resolved_output)

            result = {'output_path': resolved_output, 'angle': resolved_angle}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Rotated {resolved_angle}°", data=result)
        except ImportError:
            return ActionResult(success=False, message="Pillow not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"Image rotate error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['input_path', 'output_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'angle': 90, 'expand': True, 'output_var': 'rotate_result'}


class ImageConvertAction(BaseAction):
    """Convert image format."""
    action_type = "image_convert"
    display_name = "图片格式转换"
    description = "转换图片格式"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute image convert."""
        input_path = params.get('input_path', '')
        output_path = params.get('output_path', '')
        format = params.get('format', None)  # PNG, JPEG, WEBP, etc.
        quality = params.get('quality', 95)
        output_var = params.get('output_var', 'convert_result')

        if not input_path or not output_path:
            return ActionResult(success=False, message="input_path and output_path are required")

        try:
            from PIL import Image

            resolved_input = context.resolve_value(input_path) if context else input_path
            resolved_output = context.resolve_value(output_path) if context else output_path
            resolved_format = context.resolve_value(format) if context else format
            resolved_quality = context.resolve_value(quality) if context else quality

            img = Image.open(resolved_input)

            if img.mode == 'RGBA' and (resolved_format or '').upper() in ('JPEG', 'JPG'):
                rgb = Image.new('RGB', img.size, (255, 255, 255))
                rgb.paste(img, mask=img.split()[3])
                img = rgb

            save_kwargs = {}
            if resolved_format and resolved_format.upper() in ('JPEG', 'JPG'):
                save_kwargs['quality'] = resolved_quality
                save_kwargs['optimize'] = True
            elif resolved_format and resolved_format.upper() == 'PNG':
                save_kwargs['optimize'] = True

            if not resolved_format:
                resolved_format = resolved_output.split('.')[-1].upper()

            _os.makedirs(_os.path.dirname(resolved_output) or '.', exist_ok=True)
            img.save(resolved_output, format=resolved_format, **save_kwargs)

            result = {'output_path': resolved_output, 'format': resolved_format}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Converted to {resolved_format}", data=result)
        except ImportError:
            return ActionResult(success=False, message="Pillow not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"Image convert error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['input_path', 'output_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': None, 'quality': 95, 'output_var': 'convert_result'}


class ImageInfoAction(BaseAction):
    """Get image info."""
    action_type = "image_info"
    display_name = "图片信息"
    description = "获取图片信息"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute image info."""
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'image_info')

        if not file_path:
            return ActionResult(success=False, message="file_path is required")

        try:
            from PIL import Image

            resolved_path = context.resolve_value(file_path) if context else file_path

            img = Image.open(resolved_path)
            info = {
                'width': img.width,
                'height': img.height,
                'mode': img.mode,
                'format': img.format,
                'size_bytes': _os.path.getsize(resolved_path),
            }

            if context:
                context.set(output_var, info)
            return ActionResult(success=True, message=f"{img.width}x{img.height} {img.format} {img.mode}", data=info)
        except ImportError:
            return ActionResult(success=False, message="Pillow not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"Image info error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'image_info'}


class ImageThumbnailAction(BaseAction):
    """Create image thumbnail."""
    action_type = "image_thumbnail"
    display_name = "图片缩略图"
    description = "创建缩略图"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute thumbnail."""
        input_path = params.get('input_path', '')
        output_path = params.get('output_path', '')
        max_size = params.get('max_size', (256, 256))
        output_var = params.get('output_var', 'thumbnail_result')

        if not input_path or not output_path:
            return ActionResult(success=False, message="input_path and output_path are required")

        try:
            from PIL import Image

            resolved_input = context.resolve_value(input_path) if context else input_path
            resolved_output = context.resolve_value(output_path) if context else output_path
            resolved_size = context.resolve_value(max_size) if context else max_size

            img = Image.open(resolved_input)
            img.thumbnail(resolved_size, Image.Resampling.LANCZOS)

            _os.makedirs(_os.path.dirname(resolved_output) or '.', exist_ok=True)
            img.save(resolved_output)

            result = {'output_path': resolved_output, 'size': img.size}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Thumbnail: {img.size}", data=result)
        except ImportError:
            return ActionResult(success=False, message="Pillow not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"Thumbnail error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['input_path', 'output_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'max_size': (256, 256), 'output_var': 'thumbnail_result'}
