import pyautogui
import time
from typing import Dict, Any, Optional, Tuple
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

try:
    from paddleocr import PaddleOCR
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False


class OCRAction(BaseAction):
    action_type = "ocr"
    display_name = "OCR文字识别"
    description = "识别屏幕指定区域的文字内容"
    
    _ocr_instance = None
    
    @classmethod
    def get_ocr(cls):
        if cls._ocr_instance is None and OCR_AVAILABLE:
            cls._ocr_instance = PaddleOCR(use_angle_cls=True, lang='ch', show_log=False)
        return cls._ocr_instance
    
    def execute(self, context, params: Dict[str, Any]) -> ActionResult:
        if not OCR_AVAILABLE:
            return ActionResult(
                success=False,
                message="PaddleOCR未安装，请运行: pip install paddleocr"
            )
        
        region = params.get('region', None)
        click_text = params.get('click_text', None)
        click_index = params.get('click_index', 0)
        contains = params.get('contains', None)
        move_duration = params.get('move_duration', 0.2)
        
        try:
            screenshot = pyautogui.screenshot(region=region)
            
            import numpy as np
            img_array = np.array(screenshot)
            
            ocr = self.get_ocr()
            result = ocr.ocr(img_array, cls=True)
            
            if not result or not result[0]:
                return ActionResult(
                    success=True,
                    message="未识别到文字",
                    data={'text': '', 'results': [], 'found': False}
                )
            
            ocr_results = []
            all_text = []
            
            for line in result[0]:
                box = line[0]
                text = line[1][0]
                confidence = line[1][1]
                
                center_x = int((box[0][0] + box[2][0]) / 2)
                center_y = int((box[0][1] + box[2][1]) / 2)
                
                if region:
                    center_x += region[0]
                    center_y += region[1]
                
                ocr_results.append({
                    'text': text,
                    'confidence': confidence,
                    'x': center_x,
                    'y': center_y,
                    'box': box
                })
                all_text.append(text)
            
            full_text = '\n'.join(all_text)
            
            if click_text:
                matched_results = [r for r in ocr_results if click_text in r['text']]
                
                if not matched_results:
                    return ActionResult(
                        success=False,
                        message=f"未找到包含 '{click_text}' 的文字",
                        data={'text': full_text, 'results': ocr_results, 'found': False}
                    )
                
                if click_index >= len(matched_results):
                    click_index = 0
                
                target = matched_results[click_index]
                pyautogui.moveTo(target['x'], target['y'], duration=move_duration)
                time.sleep(0.05)
                pyautogui.click(target['x'], target['y'])
                
                return ActionResult(
                    success=True,
                    message=f"点击文字成功: {target['text']}",
                    data={
                        'text': full_text,
                        'results': ocr_results,
                        'clicked': target,
                        'found': True
                    }
                )
            
            if contains:
                matched = [r for r in ocr_results if contains in r['text']]
                if matched:
                    return ActionResult(
                        success=True,
                        message=f"找到包含 '{contains}' 的文字",
                        data={'text': full_text, 'results': ocr_results, 'matched': matched, 'found': True}
                    )
                else:
                    return ActionResult(
                        success=True,
                        message=f"未找到包含 '{contains}' 的文字",
                        data={'text': full_text, 'results': ocr_results, 'found': False}
                    )
            
            return ActionResult(
                success=True,
                message=f"识别成功，共 {len(ocr_results)} 行文字",
                data={'text': full_text, 'results': ocr_results}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"OCR识别失败: {str(e)}"
            )
    
    def get_required_params(self) -> list:
        return []
    
    def get_optional_params(self) -> dict:
        return {
            'region': None,
            'click_text': None,
            'click_index': 0,
            'contains': None,
            'move_duration': 0.2
        }
