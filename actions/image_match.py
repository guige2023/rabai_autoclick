import cv2
import numpy as np
import pyautogui
import time
from typing import Dict, Any, Optional, Tuple
from pathlib import Path
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ImageMatchAction(BaseAction):
    action_type = "click_image"
    display_name = "图像识别点击"
    description = "通过图像模板匹配定位并点击目标"
    
    def execute(self, context, params: Dict[str, Any]) -> ActionResult:
        template_path = params.get('template', '')
        confidence = params.get('confidence', 0.8)
        region = params.get('region', None)
        click_offset = params.get('click_offset', (0, 0))
        click_center = params.get('click_center', True)
        double_click = params.get('double_click', False)
        move_duration = params.get('move_duration', 0.2)
        
        if not template_path:
            return ActionResult(
                success=False,
                message="未指定模板图片路径"
            )
        
        if not Path(template_path).exists():
            return ActionResult(
                success=False,
                message=f"模板图片不存在: {template_path}"
            )
        
        try:
            position = self._find_image(template_path, confidence, region)
            
            if position is None:
                return ActionResult(
                    success=False,
                    message=f"未找到匹配图像: {template_path}"
                )
            
            center_x, center_y = position
            
            if click_offset:
                center_x += click_offset[0]
                center_y += click_offset[1]
            
            pyautogui.moveTo(center_x, center_y, duration=move_duration)
            time.sleep(0.05)
            
            if double_click:
                pyautogui.doubleClick(center_x, center_y)
            else:
                pyautogui.click(center_x, center_y)
            
            return ActionResult(
                success=True,
                message=f"图像点击成功: ({center_x}, {center_y})",
                data={'x': center_x, 'y': center_y, 'template': template_path}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"图像匹配失败: {str(e)}"
            )
    
    def _find_image(self, template_path: str, confidence: float, 
                    region: Optional[Tuple[int, int, int, int]] = None) -> Optional[Tuple[int, int]]:
        screenshot = pyautogui.screenshot(region=region)
        screenshot_cv = cv2.cvtColor(np.array(screenshot), cv2.COLOR_RGB2BGR)
        
        template = cv2.imread(template_path)
        if template is None:
            return None
        
        template = cv2.cvtColor(template, cv2.COLOR_BGR2GRAY)
        gray_screenshot = cv2.cvtColor(screenshot_cv, cv2.COLOR_BGR2GRAY)
        
        result = cv2.matchTemplate(gray_screenshot, template, cv2.TM_CCOEFF_NORMED)
        min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(result)
        
        if max_val >= confidence:
            h, w = template.shape
            center_x = max_loc[0] + w // 2
            center_y = max_loc[1] + h // 2
            
            if region:
                center_x += region[0]
                center_y += region[1]
            
            return (center_x, center_y)
        
        return None
    
    def get_required_params(self) -> list:
        return ['template']
    
    def get_optional_params(self) -> dict:
        return {
            'confidence': 0.8,
            'region': None,
            'click_offset': (0, 0),
            'click_center': True,
            'double_click': False,
            'move_duration': 0.2
        }


class FindImageAction(BaseAction):
    action_type = "find_image"
    display_name = "查找图像"
    description = "查找屏幕上的图像并返回坐标，不执行点击"
    
    def execute(self, context, params: Dict[str, Any]) -> ActionResult:
        template_path = params.get('template', '')
        confidence = params.get('confidence', 0.8)
        region = params.get('region', None)
        find_all = params.get('find_all', False)
        
        if not template_path:
            return ActionResult(
                success=False,
                message="未指定模板图片路径"
            )
        
        if not Path(template_path).exists():
            return ActionResult(
                success=False,
                message=f"模板图片不存在: {template_path}"
            )
        
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
    
    def _find_image(self, template_path: str, confidence: float, 
                    region: Optional[Tuple[int, int, int, int]]) -> Optional[Tuple[int, int]]:
        screenshot = pyautogui.screenshot(region=region)
        screenshot_cv = cv2.cvtColor(np.array(screenshot), cv2.COLOR_RGB2BGR)
        
        template = cv2.imread(template_path)
        template = cv2.cvtColor(template, cv2.COLOR_BGR2GRAY)
        gray_screenshot = cv2.cvtColor(screenshot_cv, cv2.COLOR_BGR2GRAY)
        
        result = cv2.matchTemplate(gray_screenshot, template, cv2.TM_CCOEFF_NORMED)
        min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(result)
        
        if max_val >= confidence:
            h, w = template.shape
            center_x = max_loc[0] + w // 2
            center_y = max_loc[1] + h // 2
            
            if region:
                center_x += region[0]
                center_y += region[1]
            
            return (center_x, center_y)
        return None
    
    def _find_all_images(self, template_path: str, confidence: float,
                         region: Optional[Tuple[int, int, int, int]]) -> list:
        screenshot = pyautogui.screenshot(region=region)
        screenshot_cv = cv2.cvtColor(np.array(screenshot), cv2.COLOR_RGB2BGR)
        
        template = cv2.imread(template_path)
        template = cv2.cvtColor(template, cv2.COLOR_BGR2GRAY)
        gray_screenshot = cv2.cvtColor(screenshot_cv, cv2.COLOR_BGR2GRAY)
        
        result = cv2.matchTemplate(gray_screenshot, template, cv2.TM_CCOEFF_NORMED)
        h, w = template.shape
        
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
    
    def get_required_params(self) -> list:
        return ['template']
    
    def get_optional_params(self) -> dict:
        return {
            'confidence': 0.8,
            'region': None,
            'find_all': False
        }
