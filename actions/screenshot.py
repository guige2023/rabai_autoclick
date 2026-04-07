"""Screenshot action module for RabAI AutoClick.

Provides screenshot capture actions:
- ScreenshotAction: Capture screen or region
- CompareImagesAction: Compare two images for similarity
"""

import time
import cv2
import numpy as np
import pyautogui
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


# Default screenshot directory
DEFAULT_SCREENSHOT_DIR: str = "screenshots"


class ScreenshotAction(BaseAction):
    """Capture a screenshot of the screen or a region."""
    action_type = "screenshot"
    display_name = "截图"
    description = "截取屏幕或指定区域的图像"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a screenshot action.

        Args:
            context: Execution context.
            params: Dict with path, region, save (bool).

        Returns:
            ActionResult with screenshot path and details.
        """
        output_path = params.get('path', '')
        region = params.get('region', None)
        save = params.get('save', True)
        timestamp = params.get('timestamp', True)

        # Validate region if provided
        if region is not None:
            valid, msg = self.validate_type(region, (tuple, list), 'region')
            if not valid:
                return ActionResult(success=False, message=msg)
            if len(region) != 4:
                return ActionResult(
                    success=False,
                    message=f"Region must have 4 values (x, y, width, height), got {len(region)}"
                )

        # Determine output path
        if save:
            if not output_path:
                screenshot_dir = params.get('screenshot_dir', DEFAULT_SCREENSHOT_DIR)
                Path(screenshot_dir).mkdir(parents=True, exist_ok=True)

                if timestamp:
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                    output_path = os.path.join(screenshot_dir, f"screenshot_{ts}.png")
                else:
                    output_path = os.path.join(screenshot_dir, "screenshot.png")

            # Validate output path
            valid, msg = self.validate_type(output_path, str, 'path')
            if not valid:
                return ActionResult(success=False, message=msg)

        try:
            # Capture screenshot
            img = pyautogui.screenshot(region=region)

            # Get image dimensions
            width, height = img.size

            if save:
                img.save(output_path)
                return ActionResult(
                    success=True,
                    message=f"截图保存成功: {output_path}",
                    data={
                        'path': output_path,
                        'width': width,
                        'height': height,
                        'region': region
                    }
                )
            else:
                # Return image data as numpy array
                img_array = np.array(img)
                return ActionResult(
                    success=True,
                    message=f"截图成功: {width}x{height}",
                    data={
                        'width': width,
                        'height': height,
                        'region': region,
                        'image_data': img_array.tolist()
                    }
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"截图失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'path': '',
            'region': None,
            'save': True,
            'timestamp': True,
            'screenshot_dir': DEFAULT_SCREENSHOT_DIR
        }


class CompareImagesAction(BaseAction):
    """Compare two images and return similarity score."""
    action_type = "compare_images"
    display_name = "图像对比"
    description = "比较两张图像的相似度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute an image comparison action.

        Args:
            context: Execution context.
            params: Dict with image1, image2, method, threshold.

        Returns:
            ActionResult with similarity score and match status.
        """
        image1_path = params.get('image1', '')
        image2_path = params.get('image2', '')
        method = params.get('method', 'mse')
        threshold = params.get('threshold', 0.95)

        # Validate image1
        if not image1_path:
            return ActionResult(
                success=False,
                message="未指定第一张图像路径"
            )
        valid, msg = self.validate_type(image1_path, str, 'image1')
        if not valid:
            return ActionResult(success=False, message=msg)

        if not Path(image1_path).exists():
            return ActionResult(
                success=False,
                message=f"第一张图像不存在: {image1_path}"
            )

        # Validate image2
        if not image2_path:
            return ActionResult(
                success=False,
                message="未指定第二张图像路径"
            )
        valid, msg = self.validate_type(image2_path, str, 'image2')
        if not valid:
            return ActionResult(success=False, message=msg)

        if not Path(image2_path).exists():
            return ActionResult(
                success=False,
                message=f"第二张图像不存在: {image2_path}"
            )

        # Validate method
        valid_methods = ['mse', 'ssim', 'histogram']
        valid, msg = self.validate_in(method, valid_methods, 'method')
        if not valid:
            return ActionResult(success=False, message=msg)

        # Validate threshold
        valid, msg = self.validate_type(threshold, (int, float), 'threshold')
        if not valid:
            return ActionResult(success=False, message=msg)
        valid, msg = self.validate_range(threshold, 0.0, 1.0, 'threshold')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            # Load images
            img1 = cv2.imread(image1_path)
            img2 = cv2.imread(image2_path)

            if img1 is None:
                return ActionResult(
                    success=False,
                    message=f"无法读取第一张图像: {image1_path}"
                )
            if img2 is None:
                return ActionResult(
                    success=False,
                    message=f"无法读取第二张图像: {image2_path}"
                )

            # Resize if different sizes
            if img1.shape != img2.shape:
                img2 = cv2.resize(img2, (img1.shape[1], img1.shape[0]))

            # Calculate similarity
            if method == 'mse':
                score = self._compare_mse(img1, img2)
                match = score <= (1 - threshold)
            elif method == 'ssim':
                score = self._compare_ssim(img1, img2)
                match = score >= threshold
            else:  # histogram
                score = self._compare_histogram(img1, img2)
                match = score >= threshold

            return ActionResult(
                success=True,
                message=f"图像对比完成: {score:.4f} ({'匹配' if match else '不匹配'})",
                data={
                    'score': score,
                    'match': match,
                    'method': method,
                    'threshold': threshold
                }
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"图像对比失败: {str(e)}"
            )

    def _compare_mse(self, img1: np.ndarray, img2: np.ndarray) -> float:
        """Compare images using Mean Squared Error.

        Returns:
            Similarity score (0 = identical, 1 = completely different).
        """
        gray1 = cv2.cvtColor(img1, cv2.COLOR_BGR2GRAY)
        gray2 = cv2.cvtColor(img2, cv2.COLOR_BGR2GRAY)

        mse = np.mean((gray1.astype(float) - gray2.astype(float)) ** 2)
        max_mse = 255.0 ** 2

        return min(1.0, mse / max_mse)

    def _compare_ssim(self, img1: np.ndarray, img2: np.ndarray) -> float:
        """Compare images using Structural Similarity Index.

        Returns:
            Similarity score (0 = completely different, 1 = identical).
        """
        gray1 = cv2.cvtColor(img1, cv2.COLOR_BGR2GRAY)
        gray2 = cv2.cvtColor(img2, cv2.COLOR_BGR2GRAY)

        score = cv2.matchTemplate(gray1, gray2, cv2.TM_CCOEFF_NORMED)
        return float(np.max(score))

    def _compare_histogram(self, img1: np.ndarray, img2: np.ndarray) -> float:
        """Compare images using histogram correlation.

        Returns:
            Similarity score (0 = completely different, 1 = identical).
        """
        hist1 = cv2.calcHist([img1], [0], None, [256], [0, 256])
        hist2 = cv2.calcHist([img2], [0], None, [256], [0, 256])

        hist1 = cv2.normalize(hist1, hist1).flatten()
        hist2 = cv2.normalize(hist2, hist2).flatten()

        correlation = cv2.compareHist(
            hist1.reshape(-1).astype(np.float32),
            hist2.reshape(-1).astype(np.float32),
            cv2.HISTCMP_CORREL
        )

        return float(max(0.0, min(1.0, (correlation + 1) / 2)))

    def get_required_params(self) -> List[str]:
        return ['image1', 'image2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'method': 'mse',
            'threshold': 0.95
        }