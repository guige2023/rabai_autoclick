"""Image processing action module for RabAI AutoClick.

Provides image processing operations:
- ImageResizeAction: Resize images
- ImageCropAction: Crop images
- ImageRotateAction: Rotate images
- ImageFilterAction: Apply filters
- ImageConvertAction: Convert formats
- ImageMetadataAction: Extract metadata
"""

import io
import os
import struct
from typing import Any, Dict, List, Optional, Tuple

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ImageResizeAction(BaseAction):
    """Resize images."""
    action_type = "image_resize"
    display_name = "图像缩放"
    description = "缩放图像尺寸"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            input_path = params.get("input_path", "")
            output_path = params.get("output_path", "")
            width = params.get("width", None)
            height = params.get("height", None)
            maintain_aspect = params.get("maintain_aspect", True)

            if not input_path:
                return ActionResult(success=False, message="input_path is required")

            if not os.path.exists(input_path):
                return ActionResult(success=False, message=f"Input file not found: {input_path}")

            try:
                from PIL import Image
                img = Image.open(input_path)
                original_width, original_height = img.size

                if width and height:
                    if maintain_aspect:
                        img.thumbnail((width, height), Image.Resampling.LANCZOS)
                    else:
                        img = img.resize((width, height), Image.Resampling.LANCZOS)
                elif width:
                    ratio = width / original_width
                    new_height = int(original_height * ratio)
                    img = img.resize((width, new_height), Image.Resampling.LANCZOS)
                elif height:
                    ratio = height / original_height
                    new_width = int(original_width * ratio)
                    img = img.resize((new_width, height), Image.Resampling.LANCZOS)

                if not output_path:
                    output_path = input_path.rsplit(".", 1)[0] + "_resized." + input_path.rsplit(".", 1)[-1]

                img.save(output_path)
                return ActionResult(
                    success=True,
                    message=f"Resized from {original_width}x{original_height} to {img.size[0]}x{img.size[1]}",
                    data={"output_path": output_path, "new_size": img.size}
                )

            except ImportError:
                return ActionResult(success=False, message="PIL not available")
            except Exception as e:
                return ActionResult(success=False, message=f"Resize error: {str(e)}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class ImageCropAction(BaseAction):
    """Crop images."""
    action_type = "image_crop"
    display_name = "图像裁剪"
    description = "裁剪图像"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            input_path = params.get("input_path", "")
            output_path = params.get("output_path", "")
            x = params.get("x", 0)
            y = params.get("y", 0)
            width = params.get("width", 100)
            height = params.get("height", 100)

            if not input_path:
                return ActionResult(success=False, message="input_path is required")

            if not os.path.exists(input_path):
                return ActionResult(success=False, message=f"Input file not found: {input_path}")

            try:
                from PIL import Image
                img = Image.open(input_path)

                crop_box = (x, y, x + width, y + height)
                cropped = img.crop(crop_box)

                if not output_path:
                    output_path = input_path.rsplit(".", 1)[0] + "_cropped." + input_path.rsplit(".", 1)[-1]

                cropped.save(output_path)
                return ActionResult(
                    success=True,
                    message=f"Cropped to {width}x{height} at ({x}, {y})",
                    data={"output_path": output_path, "crop_size": (width, height)}
                )

            except ImportError:
                return ActionResult(success=False, message="PIL not available")
            except Exception as e:
                return ActionResult(success=False, message=f"Crop error: {str(e)}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class ImageRotateAction(BaseAction):
    """Rotate images."""
    action_type = "image_rotate"
    display_name = "图像旋转"
    description = "旋转图像"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            input_path = params.get("input_path", "")
            output_path = params.get("output_path", "")
            angle = params.get("angle", 90)
            expand = params.get("expand", True)

            if not input_path:
                return ActionResult(success=False, message="input_path is required")

            if not os.path.exists(input_path):
                return ActionResult(success=False, message=f"Input file not found: {input_path}")

            try:
                from PIL import Image
                img = Image.open(input_path)
                rotated = img.rotate(angle, expand=expand, resample=Image.Resampling.BICUBIC)

                if not output_path:
                    output_path = input_path.rsplit(".", 1)[0] + "_rotated." + input_path.rsplit(".", 1)[-1]

                rotated.save(output_path)
                return ActionResult(
                    success=True,
                    message=f"Rotated by {angle} degrees",
                    data={"output_path": output_path, "angle": angle}
                )

            except ImportError:
                return ActionResult(success=False, message="PIL not available")
            except Exception as e:
                return ActionResult(success=False, message=f"Rotate error: {str(e)}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class ImageFilterAction(BaseAction):
    """Apply filters to images."""
    action_type = "image_filter"
    display_name = "图像滤镜"
    description = "应用图像滤镜"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            input_path = params.get("input_path", "")
            output_path = params.get("output_path", "")
            filter_type = params.get("filter", "blur")

            if not input_path:
                return ActionResult(success=False, message="input_path is required")

            if not os.path.exists(input_path):
                return ActionResult(success=False, message=f"Input file not found: {input_path}")

            try:
                from PIL import Image, ImageFilter

                img = Image.open(input_path)

                filter_map = {
                    "blur": ImageFilter.BLUR,
                    "contour": ImageFilter.CONTOUR,
                    "detail": ImageFilter.DETAIL,
                    "edge_enhance": ImageFilter.EDGE_ENHANCE,
                    "edge_enhance_more": ImageFilter.EDGE_ENHANCE_MORE,
                    "emboss": ImageFilter.EMBOSS,
                    "find_edges": ImageFilter.FIND_EDGES,
                    "sharpen": ImageFilter.SHARPEN,
                    "smooth": ImageFilter.SMOOTH,
                    "smooth_more": ImageFilter.SMOOTH_MORE,
                }

                filtered = img.filter(filter_map.get(filter_type, ImageFilter.BLUR))

                if not output_path:
                    output_path = input_path.rsplit(".", 1)[0] + f"_{filter_type}." + input_path.rsplit(".", 1)[-1]

                filtered.save(output_path)
                return ActionResult(
                    success=True,
                    message=f"Applied {filter_type} filter",
                    data={"output_path": output_path, "filter": filter_type}
                )

            except ImportError:
                return ActionResult(success=False, message="PIL not available")
            except Exception as e:
                return ActionResult(success=False, message=f"Filter error: {str(e)}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class ImageConvertAction(BaseAction):
    """Convert image formats."""
    action_type = "image_convert"
    display_name = "图像格式转换"
    description = "转换图像格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            input_path = params.get("input_path", "")
            output_path = params.get("output_path", "")
            format = params.get("format", "PNG")

            if not input_path:
                return ActionResult(success=False, message="input_path is required")

            if not os.path.exists(input_path):
                return ActionResult(success=False, message=f"Input file not found: {input_path}")

            try:
                from PIL import Image
                img = Image.open(input_path)

                if not output_path:
                    base = input_path.rsplit(".", 1)[0]
                    output_path = f"{base}.{format.lower()}"

                if format.upper() == "JPEG" and img.mode in ("RGBA", "LA", "P"):
                    rgb = Image.new("RGB", img.size, (255, 255, 255))
                    if img.mode == "P":
                        img = img.convert("RGBA")
                    rgb.paste(img, mask=img.split()[-1] if img.mode in ("RGBA", "LA") else None)
                    rgb.save(output_path, format="JPEG")
                else:
                    img.save(output_path, format=format.upper())

                return ActionResult(
                    success=True,
                    message=f"Converted to {format}",
                    data={"output_path": output_path, "format": format}
                )

            except ImportError:
                return ActionResult(success=False, message="PIL not available")
            except Exception as e:
                return ActionResult(success=False, message=f"Convert error: {str(e)}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class ImageMetadataAction(BaseAction):
    """Extract image metadata."""
    action_type = "image_metadata"
    display_name = "图像元数据"
    description = "提取图像元数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            input_path = params.get("input_path", "")

            if not input_path:
                return ActionResult(success=False, message="input_path is required")

            if not os.path.exists(input_path):
                return ActionResult(success=False, message=f"Input file not found: {input_path}")

            try:
                from PIL import Image
                img = Image.open(input_path)

                metadata = {
                    "format": img.format,
                    "mode": img.mode,
                    "size": img.size,
                    "width": img.size[0],
                    "height": img.size[1],
                    "file_size": os.path.getsize(input_path)
                }

                if hasattr(img, "_getexif") and img._getexif():
                    exif = img._getexif()
                    metadata["exif"] = {k: str(v) for k, v in exif.items() if v}

                return ActionResult(
                    success=True,
                    message=f"Extracted metadata from {input_path}",
                    data={"metadata": metadata}
                )

            except ImportError:
                return ActionResult(success=False, message="PIL not available")
            except Exception as e:
                return ActionResult(success=False, message=f"Metadata error: {str(e)}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
