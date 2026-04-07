"""Image3 action module for RabAI AutoClick.

Provides additional image operations:
- ImageGetSizeAction: Get image size
- ImageGetFormatAction: Get image format
- ImageGetModeAction: Get image mode
- ImageGetInfoAction: Get image info
- ImageConvertModeAction: Convert image mode
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ImageGetSizeAction(BaseAction):
    """Get image size."""
    action_type = "image3_get_size"
    display_name = "获取图片尺寸"
    description = "获取图片的宽度和高度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get size.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with image size.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'image_size')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            from PIL import Image
            resolved = context.resolve_value(path)

            if not os.path.exists(resolved):
                return ActionResult(success=False, message=f"文件不存在: {resolved}")

            with Image.open(resolved) as img:
                result = {'width': img.width, 'height': img.height}
                context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"图片尺寸: {img.width}x{img.height}",
                data={
                    'path': resolved,
                    'size': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(success=False, message="Pillow库未安装")
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取图片尺寸失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'image_size'}


class ImageGetFormatAction(BaseAction):
    """Get image format."""
    action_type = "image3_get_format"
    display_name = "获取图片格式"
    description = "获取图片的格式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get format.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with image format.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'image_format')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            from PIL import Image
            resolved = context.resolve_value(path)

            if not os.path.exists(resolved):
                return ActionResult(success=False, message=f"文件不存在: {resolved}")

            with Image.open(resolved) as img:
                result = img.format
                context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"图片格式: {result}",
                data={
                    'path': resolved,
                    'format': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(success=False, message="Pillow库未安装")
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取图片格式失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'image_format'}


class ImageGetModeAction(BaseAction):
    """Get image mode."""
    action_type = "image3_get_mode"
    display_name = "获取图片模式"
    description = "获取图片的颜色模式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get mode.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with image mode.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'image_mode')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            from PIL import Image
            resolved = context.resolve_value(path)

            if not os.path.exists(resolved):
                return ActionResult(success=False, message=f"文件不存在: {resolved}")

            with Image.open(resolved) as img:
                result = img.mode
                context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"图片模式: {result}",
                data={
                    'path': resolved,
                    'mode': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(success=False, message="Pillow库未安装")
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取图片模式失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'image_mode'}


class ImageGetInfoAction(BaseAction):
    """Get image info."""
    action_type = "image3_get_info"
    display_name = "获取图片信息"
    description = "获取图片的详细信息"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get info.

        Args:
            context: Execution context.
            params: Dict with path, output_var.

        Returns:
            ActionResult with image info.
        """
        path = params.get('path', '')
        output_var = params.get('output_var', 'image_info')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            from PIL import Image
            resolved = context.resolve_value(path)

            if not os.path.exists(resolved):
                return ActionResult(success=False, message=f"文件不存在: {resolved}")

            with Image.open(resolved) as img:
                result = {
                    'format': img.format,
                    'mode': img.mode,
                    'width': img.width,
                    'height': img.height,
                    'size': img.size
                }
                context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"图片信息: {img.width}x{img.height} {img.format}",
                data={
                    'path': resolved,
                    'info': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(success=False, message="Pillow库未安装")
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取图片信息失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'image_info'}


class ImageConvertModeAction(BaseAction):
    """Convert image mode."""
    action_type = "image3_convert_mode"
    display_name = "转换图片模式"
    description = "转换图片的颜色模式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute convert mode.

        Args:
            context: Execution context.
            params: Dict with path, mode, output_var.

        Returns:
            ActionResult with conversion result.
        """
        path = params.get('path', '')
        mode = params.get('mode', 'RGB')
        output_var = params.get('output_var', 'converted_path')

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(mode, str, 'mode')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            from PIL import Image
            resolved_path = context.resolve_value(path)
            resolved_mode = context.resolve_value(mode)

            if not os.path.exists(resolved_path):
                return ActionResult(success=False, message=f"文件不存在: {resolved_path}")

            with Image.open(resolved_path) as img:
                converted = img.convert(resolved_mode)
                name, ext = os.path.splitext(resolved_path)
                output_path = f"{name}_converted{ext}"
                converted.save(output_path)
                context.set(output_var, output_path)

            return ActionResult(
                success=True,
                message=f"图片模式转换完成: {resolved_mode}",
                data={
                    'original': resolved_path,
                    'mode': resolved_mode,
                    'output_path': output_path,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(success=False, message="Pillow库未安装")
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换图片模式失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path', 'mode']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'converted_path'}
