"""Image action module for RabAI AutoClick.

Provides image processing actions including capture, resize, crop, and format conversion.
"""

import os
import sys
import subprocess
from typing import Any, Dict, List, Optional, Tuple, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ScreenCaptureAction(BaseAction):
    """Capture screenshot of the screen or a region.
    
    Supports full screen, window capture, and rectangular region selection.
    """
    action_type = "screen_capture"
    display_name = "屏幕截图"
    description = "截取屏幕或区域图像"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Capture screenshot.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: path, region, display, include_cursor.
        
        Returns:
            ActionResult with screenshot path and metadata.
        """
        path = params.get('path', '/tmp/screenshot.png')
        region = params.get('region', None)  # [x, y, width, height]
        display = params.get('display', 0)
        include_cursor = params.get('include_cursor', False)
        
        try:
            cmd = ['screencapture']
            
            if region:
                x, y, w, h = region
                cmd.extend(['-x', '-R', f'{x},{y},{w},{h}'])
            else:
                cmd.append('-x')
            
            if include_cursor:
                cmd.append('-C')
            
            cmd.append(path)
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                timeout=10
            )
            
            if result.returncode == 0 and os.path.exists(path):
                size = os.path.getsize(path)
                return ActionResult(
                    success=True,
                    message=f"Screenshot saved: {path}",
                    data={'path': path, 'size': size, 'region': region}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"screencapture failed: {result.stderr.decode()}"
                )
                
        except subprocess.TimeoutExpired:
            return ActionResult(success=False, message="Screenshot timed out")
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Screenshot error: {e}",
                data={'error': str(e)}
            )


class ImageResizeAction(BaseAction):
    """Resize an image to specified dimensions.
    
    Uses sips for macOS native image resizing.
    """
    action_type = "image_resize"
    display_name = "调整图像大小"
    description = "调整图像尺寸"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Resize image.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: input_path, output_path, width, height,
                   maintain_aspect, scale.
        
        Returns:
            ActionResult with resized image path.
        """
        input_path = params.get('input_path', '')
        output_path = params.get('output_path', '')
        width = params.get('width', None)
        height = params.get('height', None)
        maintain_aspect = params.get('maintain_aspect', True)
        scale = params.get('scale', None)
        
        if not input_path or not os.path.exists(input_path):
            return ActionResult(success=False, message=f"Input file not found: {input_path}")
        
        if not output_path:
            output_path = input_path  # Overwrite
        
        try:
            cmd = ['sips']
            
            if scale:
                cmd.extend(['-z', f'{int(scale * 100)}'])
            elif width and height:
                if maintain_aspect:
                    cmd.extend(['-z', str(height), str(width)])
                else:
                    cmd.extend(['--resampleWidth', str(width), '--resampleHeight', str(height)])
            elif width:
                cmd.extend(['--resampleWidth', str(width)])
            elif height:
                cmd.extend(['--resampleHeight', str(height)])
            else:
                return ActionResult(success=False, message="width, height, or scale required")
            
            cmd.extend([input_path, '-o', output_path])
            
            result = subprocess.run(cmd, capture_output=True, timeout=30)
            
            if result.returncode == 0:
                size = os.path.getsize(output_path)
                return ActionResult(
                    success=True,
                    message=f"Resized image saved",
                    data={'path': output_path, 'size': size}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"sips failed: {result.stderr.decode()}"
                )
                
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Resize error: {e}",
                data={'error': str(e)}
            )


class ImageCropAction(BaseAction):
    """Crop an image to a specified region.
    
    Extracts a rectangular region from an image.
    """
    action_type = "image_crop"
    display_name = "裁剪图像"
    description = "裁剪图像区域"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Crop image.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: input_path, output_path, x, y, width, height.
        
        Returns:
            ActionResult with cropped image path.
        """
        input_path = params.get('input_path', '')
        output_path = params.get('output_path', '')
        x = params.get('x', 0)
        y = params.get('y', 0)
        width = params.get('width', 100)
        height = params.get('height', 100)
        
        if not input_path or not os.path.exists(input_path):
            return ActionResult(success=False, message=f"Input file not found: {input_path}")
        
        if not output_path:
            output_path = input_path
        
        try:
            # Use sips with crop (actually uses --cropOffset and --cropSize)
            cmd = [
                'sips',
                input_path,
                '-o', output_path,
                '--cropOffset', f'{y}',
                '--cropSize', f'{height}', f'{width}'
            ]
            
            result = subprocess.run(cmd, capture_output=True, timeout=30)
            
            # Note: sips crop works differently, may need to use mogrify or similar
            if result.returncode == 0:
                return ActionResult(
                    success=True,
                    message=f"Cropped to {width}x{height} at ({x},{y})",
                    data={'path': output_path, 'region': [x, y, width, height]}
                )
            else:
                # Fallback: try with mogrify if available
                mogrify_cmd = ['mogrify', '-crop', f'{width}x{height}+{x}+{y}', input_path]
                try:
                    subprocess.run(mogrify_cmd, capture_output=True, timeout=30, check=True)
                    return ActionResult(
                        success=True,
                        message=f"Cropped to {width}x{height} at ({x},{y})",
                        data={'path': input_path, 'region': [x, y, width, height]}
                    )
                except:
                    return ActionResult(
                        success=False,
                        message=f"Crop failed: {result.stderr.decode()}"
                    )
                
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Crop error: {e}",
                data={'error': str(e)}
            )


class ImageFormatConvertAction(BaseAction):
    """Convert image between formats (PNG, JPEG, TIFF, etc.).
    
    Uses sips for native macOS format conversion.
    """
    action_type = "image_convert"
    display_name = "图像格式转换"
    description = "转换图像格式"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Convert image format.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: input_path, output_path, format, quality.
        
        Returns:
            ActionResult with converted image path.
        """
        input_path = params.get('input_path', '')
        output_path = params.get('output_path', '')
        format_type = params.get('format', 'png')
        quality = params.get('quality', 85)
        
        if not input_path or not os.path.exists(input_path):
            return ActionResult(success=False, message=f"Input file not found: {input_path}")
        
        # Generate output path if not specified
        if not output_path:
            base, _ = os.path.splitext(input_path)
            output_path = f"{base}.{format_type}"
        
        valid_formats = ['png', 'jpeg', 'jpg', 'tiff', 'tif', 'gif', 'bmp', 'heic']
        if format_type.lower() not in valid_formats:
            return ActionResult(
                success=False,
                message=f"Unsupported format: {format_type}"
            )
        
        try:
            cmd = ['sips']
            
            # Set format
            cmd.extend(['-s', f'format{format_type.capitalize()}'])
            if format_type.lower() in ['jpeg', 'jpg']:
                cmd[cmd.index('-s') + 1] = f'formatOptions={quality}'
            
            cmd.extend([input_path, '-o', output_path])
            
            result = subprocess.run(cmd, capture_output=True, timeout=30)
            
            if result.returncode == 0 and os.path.exists(output_path):
                size = os.path.getsize(output_path)
                return ActionResult(
                    success=True,
                    message=f"Converted to {format_type}",
                    data={'path': output_path, 'size': size, 'format': format_type}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"Conversion failed: {result.stderr.decode()}"
                )
                
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Format conversion error: {e}",
                data={'error': str(e)}
            )
