"""Image5 action module for RabAI AutoClick.

Provides additional image operations:
- ImageConvertFormatAction: Convert image format
- ImageGrayscaleAction: Convert to grayscale
- ImageContrastAction: Adjust contrast
- ImageBrightnessAction: Adjust brightness
- ImageSharpnessAction: Adjust sharpness
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ImageConvertFormatAction(BaseAction):
    """Convert image format."""
    action_type = "image5_convert"
    display_name = "图片格式转换"
    description = "转换图片格式"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute convert format.

        Args:
            context: Execution context.
            params: Dict with file_path, format, output_var.

        Returns:
            ActionResult with converted image path.
        """
        file_path = params.get('file_path', '')
        format_str = params.get('format', 'PNG')
        output_var = params.get('output_var', 'converted_path')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_format = context.resolve_value(format_str).upper() if format_str else 'PNG'

            try:
                from PIL import Image
                img = Image.open(resolved_path)

                output_path = resolved_path.rsplit('.', 1)[0] + '.' + resolved_format.lower()
                img.save(output_path, format=resolved_format)

            except ImportError:
                return ActionResult(
                    success=False,
                    message="图片格式转换失败: 未安装Pillow库"
                )

            context.set(output_var, output_path)

            return ActionResult(
                success=True,
                message=f"图片格式转换完成: {output_path}",
                data={
                    'original': resolved_path,
                    'converted': output_path,
                    'format': resolved_format,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"图片格式转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path', 'format']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'converted_path'}


class ImageGrayscaleAction(BaseAction):
    """Convert to grayscale."""
    action_type = "image5_grayscale"
    display_name = "图片灰度化"
    description = "将图片转换为灰度"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute grayscale.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with grayscale image path.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'grayscale_path')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)

            try:
                from PIL import Image
                img = Image.open(resolved_path)
                grayscale = img.convert('L')

                output_path = resolved_path.replace('.', '_grayscale.')
                grayscale.save(output_path)

            except ImportError:
                return ActionResult(
                    success=False,
                    message="图片灰度化失败: 未安装Pillow库"
                )

            context.set(output_var, output_path)

            return ActionResult(
                success=True,
                message=f"图片灰度化完成: {output_path}",
                data={
                    'original': resolved_path,
                    'grayscale': output_path,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"图片灰度化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'grayscale_path'}


class ImageContrastAction(BaseAction):
    """Adjust contrast."""
    action_type = "image5_contrast"
    display_name = "调整对比度"
    description = "调整图片对比度"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute contrast.

        Args:
            context: Execution context.
            params: Dict with file_path, level, output_var.

        Returns:
            ActionResult with adjusted image path.
        """
        file_path = params.get('file_path', '')
        level = params.get('level', 1.0)
        output_var = params.get('output_var', 'contrast_path')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_level = float(context.resolve_value(level)) if level else 1.0

            try:
                from PIL import Image, ImageEnhance
                img = Image.open(resolved_path)
                enhancer = ImageEnhance.Contrast(img)
                adjusted = enhancer.enhance(resolved_level)

                output_path = resolved_path.replace('.', '_contrast.')
                adjusted.save(output_path)

            except ImportError:
                return ActionResult(
                    success=False,
                    message="调整对比度失败: 未安装Pillow库"
                )

            context.set(output_var, output_path)

            return ActionResult(
                success=True,
                message=f"调整对比度完成: {output_path}",
                data={
                    'original': resolved_path,
                    'adjusted': output_path,
                    'level': resolved_level,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"调整对比度失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'level': 1.0, 'output_var': 'contrast_path'}


class ImageBrightnessAction(BaseAction):
    """Adjust brightness."""
    action_type = "image5_brightness"
    display_name = "调整亮度"
    description = "调整图片亮度"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute brightness.

        Args:
            context: Execution context.
            params: Dict with file_path, level, output_var.

        Returns:
            ActionResult with adjusted image path.
        """
        file_path = params.get('file_path', '')
        level = params.get('level', 1.0)
        output_var = params.get('output_var', 'brightness_path')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_level = float(context.resolve_value(level)) if level else 1.0

            try:
                from PIL import Image, ImageEnhance
                img = Image.open(resolved_path)
                enhancer = ImageEnhance.Brightness(img)
                adjusted = enhancer.enhance(resolved_level)

                output_path = resolved_path.replace('.', '_brightness.')
                adjusted.save(output_path)

            except ImportError:
                return ActionResult(
                    success=False,
                    message="调整亮度失败: 未安装Pillow库"
                )

            context.set(output_var, output_path)

            return ActionResult(
                success=True,
                message=f"调整亮度完成: {output_path}",
                data={
                    'original': resolved_path,
                    'adjusted': output_path,
                    'level': resolved_level,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"调整亮度失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'level': 1.0, 'output_var': 'brightness_path'}


class ImageSharpnessAction(BaseAction):
    """Adjust sharpness."""
    action_type = "image5_sharpness"
    display_name = "调整锐度"
    description = "调整图片锐度"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sharpness.

        Args:
            context: Execution context.
            params: Dict with file_path, level, output_var.

        Returns:
            ActionResult with adjusted image path.
        """
        file_path = params.get('file_path', '')
        level = params.get('level', 1.0)
        output_var = params.get('output_var', 'sharpness_path')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_level = float(context.resolve_value(level)) if level else 1.0

            try:
                from PIL import Image, ImageEnhance
                img = Image.open(resolved_path)
                enhancer = ImageEnhance.Sharpness(img)
                adjusted = enhancer.enhance(resolved_level)

                output_path = resolved_path.replace('.', '_sharpness.')
                adjusted.save(output_path)

            except ImportError:
                return ActionResult(
                    success=False,
                    message="调整锐度失败: 未安装Pillow库"
                )

            context.set(output_var, output_path)

            return ActionResult(
                success=True,
                message=f"调整锐度完成: {output_path}",
                data={
                    'original': resolved_path,
                    'adjusted': output_path,
                    'level': resolved_level,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"调整锐度失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'level': 1.0, 'output_var': 'sharpness_path'}