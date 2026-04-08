"""Image utilities action module for RabAI AutoClick.

Provides image processing operations including
resize, crop, format conversion, and thumbnail generation.
"""

import os
import sys
import io
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ImageResizeAction(BaseAction):
    """Resize an image.
    
    Supports absolute dimensions, percentage scaling,
    and aspect ratio preservation.
    """
    action_type = "image_resize"
    display_name = "调整图片大小"
    description = "调整图片尺寸"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Resize image.
        
        Args:
            context: Execution context.
            params: Dict with keys: path, width, height, scale,
                   keep_aspect, output_path, save_to_var.
        
        Returns:
            ActionResult with resize result.
        """
        path = params.get('path', '')
        width = params.get('width', None)
        height = params.get('height', None)
        scale = params.get('scale', None)
        keep_aspect = params.get('keep_aspect', True)
        output_path = params.get('output_path', None)
        save_to_var = params.get('save_to_var', None)

        if not path:
            return ActionResult(success=False, message="Image path is required")

        if not os.path.exists(path):
            return ActionResult(success=False, message=f"Image not found: {path}")

        if scale is None and width is None and height is None:
            return ActionResult(
                success=False,
                message="Must specify width, height, or scale"
            )

        try:
            from PIL import Image
        except ImportError:
            return ActionResult(
                success=False,
                message="PIL/Pillow not installed: pip install Pillow"
            )

        try:
            with Image.open(path) as img:
                original_width, original_height = img.size

                # Calculate new dimensions
                if scale:
                    new_width = int(original_width * scale)
                    new_height = int(original_height * scale)
                elif width and height:
                    if keep_aspect:
                        ratio = min(width / original_width, height / original_height)
                        new_width = int(original_width * ratio)
                        new_height = int(original_height * ratio)
                    else:
                        new_width = width
                        new_height = height
                elif width:
                    ratio = width / original_width
                    new_width = width
                    new_height = int(original_height * ratio)
                else:
                    ratio = height / original_height
                    new_width = int(original_width * ratio)
                    new_height = height

                # Resize
                resized = img.resize((new_width, new_height), Image.Resampling.LANCZOS)

                # Save
                if not output_path:
                    name, ext = os.path.splitext(path)
                    output_path = f"{name}_resized{ext}"

                # Determine format
                fmt = img.format
                if output_path.lower().endswith('.png'):
                    fmt = 'PNG'
                elif output_path.lower().endswith(('.jpg', '.jpeg')):
                    fmt = 'JPEG'

                resized.save(output_path, format=fmt)

                result_data = {
                    'output_path': output_path,
                    'original_size': (original_width, original_height),
                    'new_size': (new_width, new_height),
                    'scale': scale
                }

                if save_to_var:
                    context.variables[save_to_var] = result_data

                return ActionResult(
                    success=True,
                    message=f"调整大小完成: {original_width}x{original_height} -> {new_width}x{new_height}",
                    data=result_data
                )

        except ImportError:
            return ActionResult(
                success=False,
                message="PIL/Pillow not installed"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"调整大小失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'width': None,
            'height': None,
            'scale': None,
            'keep_aspect': True,
            'output_path': None,
            'save_to_var': None
        }


class ImageInfoAction(BaseAction):
    """Get image information.
    
    Returns dimensions, format, mode, and file size.
    """
    action_type = "image_info"
    display_name = "图片信息"
    description = "获取图片信息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Get image info.
        
        Args:
            context: Execution context.
            params: Dict with keys: path, save_to_var.
        
        Returns:
            ActionResult with image info.
        """
        path = params.get('path', '')
        save_to_var = params.get('save_to_var', None)

        if not path:
            return ActionResult(success=False, message="Image path is required")

        if not os.path.exists(path):
            return ActionResult(success=False, message=f"Image not found: {path}")

        try:
            from PIL import Image

            with Image.open(path) as img:
                width, height = img.size
                file_size = os.path.getsize(path)

                result_data = {
                    'path': path,
                    'width': width,
                    'height': height,
                    'format': img.format,
                    'mode': img.mode,
                    'file_size': file_size,
                    'aspect_ratio': round(width / height, 2) if height > 0 else 0
                }

                if save_to_var:
                    context.variables[save_to_var] = result_data

                return ActionResult(
                    success=True,
                    message=f"图片信息: {width}x{height} {img.format}",
                    data=result_data
                )

        except ImportError:
            return ActionResult(
                success=False,
                message="PIL/Pillow not installed"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取图片信息失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'save_to_var': None}
