"""Image match action module for RabAI AutoClick.

Provides image template matching actions:
- ImageMatchAction: Find image on screen and click it
- FindImageAction: Find image on screen without clicking
"""

import time
import cv2
import numpy as np
import pyautogui
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from rabai_autoclick.core.base_action import BaseAction, ActionResult
from rabai_autoclick.utils.mouse_utils import macos_click


# Confidence range bounds
MIN_CONFIDENCE: float = 0.0
MAX_CONFIDENCE: float = 1.0


class ImageMatchAction(BaseAction):
    """Find an image template on screen and click at its location."""
    action_type = "click_image"
    display_name = "图像识别点击"
    description = "通过图像模板匹配定位并点击目标"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute an image-match click.
        
        Args:
            context: Execution context.
            params: Dict with template, confidence, region, click_offset, 
                   offset_x, offset_y, click_center, double_click, 
                   move_duration, button.
            
        Returns:
            ActionResult indicating success or failure.
        """
        template_path = params.get('template', '')
        confidence = params.get('confidence', 0.8)
        region = params.get('region', None)
        click_offset = params.get('click_offset', (0, 0))
        offset_x = params.get('offset_x', 0)
        offset_y = params.get('offset_y', 0)
        double_click = params.get('double_click', False)
        move_duration = params.get('move_duration', 0.2)
        button = params.get('button', 'left')
        
        # Validate template_path
        if not template_path:
            return ActionResult(
                success=False,
                message="未指定模板图片路径"
            )
        valid, msg = self.validate_type(template_path, str, 'template')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        # Validate template file exists
        if not Path(template_path).exists():
            return ActionResult(
                success=False,
                message=f"模板图片不存在: {template_path}"
            )
        
        # Validate confidence
        valid, msg = self.validate_type(confidence, (int, float), 'confidence')
        if not valid:
            return ActionResult(success=False, message=msg)
        valid, msg = self.validate_range(
            confidence, MIN_CONFIDENCE, MAX_CONFIDENCE, 'confidence'
        )
        if not valid:
            return ActionResult(success=False, message=msg)
        
        # Validate button
        valid, msg = self.validate_in(button, self.VALID_BUTTONS, 'button')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        # Validate double_click
        valid, msg = self.validate_type(double_click, bool, 'double_click')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        # Validate move_duration
        valid, msg = self.validate_type(move_duration, (int, float), 'move_duration')
        if not valid:
            return ActionResult(success=False, message=msg)
        if move_duration < 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'move_duration' must be >= 0, got {move_duration}"
            )
        
        try:
            position = self._find_image(template_path, confidence, region)
            
            if position is None:
                return ActionResult(
                    success=False,
                    message=f"未找到匹配图像: {template_path}"
                )
            
            center_x, center_y = position
            
            # Apply offset from click_offset tuple
            if isinstance(click_offset, (tuple, list)) and len(click_offset) >= 2:
                center_x += int(click_offset[0])
                center_y += int(click_offset[1])
            
            # Apply individual offset parameters
            center_x += int(offset_x)
            center_y += int(offset_y)
            
            pyautogui.moveTo(center_x, center_y, duration=move_duration)
            time.sleep(0.2)
            
            click_count = 2 if double_click else 1
            macos_click(center_x, center_y, click_count, button)
            
            time.sleep(0.1)
            
            click_type = "双击" if double_click else "单击"
            button_name = "右键" if button == 'right' else "左键" if button == 'left' else "中键"
            
            return ActionResult(
                success=True,
                message=f"图像{click_type}{button_name}成功: ({center_x}, {center_y})",
                data={'x': center_x, 'y': center_y, 'template': template_path}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"图像匹配失败: {str(e)}"
            )
    
    def _find_image(
        self, 
        template_path: str, 
        confidence: float,
        region: Optional[Tuple[int, int, int, int]] = None
    ) -> Optional[Tuple[int, int]]:
        """Find image template on screen.
        
        Args:
            template_path: Path to template image file.
            confidence: Match confidence threshold (0.0-1.0).
            region: Optional screen region (left, top, width, height).
            
        Returns:
            Tuple of (x, y) center coordinates if found, else None.
        """
        screenshot = pyautogui.screenshot(region=region)
        screenshot_cv = cv2.cvtColor(np.array(screenshot), cv2.COLOR_RGB2BGR)
        
        template = cv2.imread(template_path)
        if template is None:
            return None
        
        template_gray = cv2.cvtColor(template, cv2.COLOR_BGR2GRAY)
        gray_screenshot = cv2.cvtColor(screenshot_cv, cv2.COLOR_BGR2GRAY)
        
        result = cv2.matchTemplate(gray_screenshot, template_gray, cv2.TM_CCOEFF_NORMED)
        min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(result)
        
        if max_val >= confidence:
            h, w = template_gray.shape
            center_x = max_loc[0] + w // 2
            center_y = max_loc[1] + h // 2
            
            if region:
                center_x += region[0]
                center_y += region[1]
            
            return (center_x, center_y)
        
        return None
    
    def get_required_params(self) -> List[str]:
        return ['template']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'confidence': 0.8,
            'region': None,
            'click_offset': (0, 0),
            'offset_x': 0,
            'offset_y': 0,
            'click_center': True,
            'double_click': False,
            'move_duration': 0.2,
            'button': 'left'
        }


class FindImageAction(BaseAction):
    """Find image templates on screen without clicking."""
    action_type = "find_image"
    display_name = "查找图像"
    description = "查找屏幕上的图像并返回坐标，不执行点击"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute an image search without clicking.
        
        Args:
            context: Execution context.
            params: Dict with template, confidence, region, find_all.
            
        Returns:
            ActionResult with position(s) if found.
        """
        template_path = params.get('template', '')
        confidence = params.get('confidence', 0.8)
        region = params.get('region', None)
        find_all = params.get('find_all', False)
        
        # Validate template_path
        if not template_path:
            return ActionResult(
                success=False,
                message="未指定模板图片路径"
            )
        valid, msg = self.validate_type(template_path, str, 'template')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        # Validate template file exists
        if not Path(template_path).exists():
            return ActionResult(
                success=False,
                message=f"模板图片不存在: {template_path}"
            )
        
        # Validate confidence
        valid, msg = self.validate_type(confidence, (int, float), 'confidence')
        if not valid:
            return ActionResult(success=False, message=msg)
        valid, msg = self.validate_range(
            confidence, MIN_CONFIDENCE, MAX_CONFIDENCE, 'confidence'
        )
        if not valid:
            return ActionResult(success=False, message=msg)
        
        # Validate find_all
        valid, msg = self.validate_type(find_all, bool, 'find_all')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            if find_all:
                positions = self._find_all_images(template_path, confidence, region)
                return ActionResult(
                    success=True,
                    message=f"找到 {len(positions)} 个匹配",
                    data={'positions': positions, 'count': len(positions)}
                )
            else:
                position = self._find_image(template_path, confidence, region)
                if position:
                    return ActionResult(
                        success=True,
                        message=f"找到图像: {position}",
                        data={'x': position[0], 'y': position[1], 'found': True}
                    )
                else:
                    return ActionResult(
                        success=True,
                        message="未找到匹配图像",
                        data={'found': False}
                    )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"查找图像失败: {str(e)}"
            )
    
    def _find_image(
        self, 
        template_path: str, 
        confidence: float,
        region: Optional[Tuple[int, int, int, int]] = None
    ) -> Optional[Tuple[int, int]]:
        """Find single image template on screen.
        
        Args:
            template_path: Path to template image file.
            confidence: Match confidence threshold.
            region: Optional screen region.
            
        Returns:
            Tuple of (x, y) center coordinates if found, else None.
        """
        screenshot = pyautogui.screenshot(region=region)
        screenshot_cv = cv2.cvtColor(np.array(screenshot), cv2.COLOR_RGB2BGR)
        
        template = cv2.imread(template_path)
        if template is None:
            return None
        
        template_gray = cv2.cvtColor(template, cv2.COLOR_BGR2GRAY)
        gray_screenshot = cv2.cvtColor(screenshot_cv, cv2.COLOR_BGR2GRAY)
        
        result = cv2.matchTemplate(gray_screenshot, template_gray, cv2.TM_CCOEFF_NORMED)
        min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(result)
        
        if max_val >= confidence:
            h, w = template_gray.shape
            center_x = max_loc[0] + w // 2
            center_y = max_loc[1] + h // 2
            
            if region:
                center_x += region[0]
                center_y += region[1]
            
            return (center_x, center_y)
        return None
    
    def _find_all_images(
        self, 
        template_path: str, 
        confidence: float,
        region: Optional[Tuple[int, int, int, int]] = None
    ) -> List[Tuple[int, int]]:
        """Find all occurrences of image template on screen.
        
        Args:
            template_path: Path to template image file.
            confidence: Match confidence threshold.
            region: Optional screen region.
            
        Returns:
            List of (x, y) tuples for all matches.
        """
        screenshot = pyautogui.screenshot(region=region)
        screenshot_cv = cv2.cvtColor(np.array(screenshot), cv2.COLOR_RGB2BGR)
        
        template = cv2.imread(template_path)
        if template is None:
            return []
        
        template_gray = cv2.cvtColor(template, cv2.COLOR_BGR2GRAY)
        gray_screenshot = cv2.cvtColor(screenshot_cv, cv2.COLOR_BGR2GRAY)
        
        result = cv2.matchTemplate(gray_screenshot, template_gray, cv2.TM_CCOEFF_NORMED)
        h, w = template_gray.shape
        
        locations = np.where(result >= confidence)
        positions = []
        
        for pt in zip(*locations[::-1]):
            center_x = pt[0] + w // 2
            center_y = pt[1] + h // 2
            
            if region:
                center_x += region[0]
                center_y += region[1]
            
            positions.append((center_x, center_y))
        
        return positions
    
    def get_required_params(self) -> List[str]:
        return ['template']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'confidence': 0.8,
            'region': None,
            'find_all': False
        }
