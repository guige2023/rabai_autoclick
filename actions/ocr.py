import pyautogui
import time
from typing import Dict, Any, Optional, Tuple, List
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

OCR_AVAILABLE = False
OCR_BACKEND = None

try:
    from rapidocr_onnxruntime import RapidOCR
    OCR_AVAILABLE = True
    OCR_BACKEND = 'rapidocr'
except ImportError:
    pass

if not OCR_AVAILABLE:
    try:
        import easyocr
        OCR_AVAILABLE = True
        OCR_BACKEND = 'easyocr'
    except ImportError:
        pass

if not OCR_AVAILABLE:
    try:
        os.environ['FLAGS_use_mkldnn'] = '0'
        os.environ['FLAGS_enable_onednn_backend'] = '0'
        from paddleocr import PaddleOCR
        OCR_AVAILABLE = True
        OCR_BACKEND = 'paddleocr'
    except ImportError:
        pass


def preprocess_image_enhanced(img_array, mode='auto'):
    """增强图像预处理 - 多种模式针对不同场景"""
    try:
        import cv2
        import numpy as np
        
        if len(img_array.shape) == 3:
            gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2BGR)
            gray = cv2.cvtColor(gray, cv2.COLOR_BGR2GRAY)
        else:
            gray = img_array.copy()
        
        processed_images = []
        
        if mode == 'auto':
            processed_images.append(('original', img_array))
            
        elif mode == 'all':
            processed_images.append(('original', img_array))
            
            clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(8, 8))
            enhanced = clahe.apply(gray)
            processed_images.append(('clahe', cv2.cvtColor(enhanced, cv2.COLOR_GRAY2RGB)))
            
            _, binary_otsu = cv2.threshold(enhanced, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
            processed_images.append(('otsu', cv2.cvtColor(binary_otsu, cv2.COLOR_GRAY2RGB)))
            
            binary_adaptive = cv2.adaptiveThreshold(
                enhanced, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2
            )
            processed_images.append(('adaptive', cv2.cvtColor(binary_adaptive, cv2.COLOR_GRAY2RGB)))
            
            kernel = np.array([[-1,-1,-1], [-1,9,-1], [-1,-1,-1]])
            sharpened = cv2.filter2D(gray, -1, kernel)
            processed_images.append(('sharpen', cv2.cvtColor(sharpened, cv2.COLOR_GRAY2RGB)))
            
            denoised = cv2.fastNlMeansDenoising(gray, None, 10, 7, 21)
            processed_images.append(('denoise', cv2.cvtColor(denoised, cv2.COLOR_GRAY2RGB)))
            
        elif mode == 'contrast':
            clahe = cv2.createCLAHE(clipLimit=4.0, tileGridSize=(8, 8))
            enhanced = clahe.apply(gray)
            processed_images.append(('contrast', cv2.cvtColor(enhanced, cv2.COLOR_GRAY2RGB)))
            
        elif mode == 'binary':
            _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
            processed_images.append(('binary', cv2.cvtColor(binary, cv2.COLOR_GRAY2RGB)))
            
        elif mode == 'denoise':
            denoised = cv2.fastNlMeansDenoising(gray, None, 10, 7, 21)
            processed_images.append(('denoise', cv2.cvtColor(denoised, cv2.COLOR_GRAY2RGB)))
            
        else:
            processed_images.append(('original', img_array))
        
        return processed_images
        
    except Exception as e:
        return [('original', img_array)]


class OCRAction(BaseAction):
    action_type = "ocr"
    display_name = "OCR文字识别"
    description = "识别屏幕指定区域的文字内容，支持精确匹配和模糊匹配"
    
    _ocr_instance = None
    _ocr_backend = None
    
    @classmethod
    def get_ocr(cls):
        if cls._ocr_instance is None and OCR_AVAILABLE:
            if OCR_BACKEND == 'rapidocr':
                cls._ocr_instance = RapidOCR(
                    det_limit_side_len=1280,
                    det_db_thresh=0.2,
                    det_db_box_thresh=0.4,
                    det_db_unclip_ratio=1.8,
                    det_db_score_mode='fast',
                    use_det=True,
                    use_cls=True,
                    use_rec=True,
                    print_verbose=False
                )
                cls._ocr_backend = 'rapidocr'
            elif OCR_BACKEND == 'easyocr':
                cls._ocr_instance = easyocr.Reader(['ch_sim', 'en'], gpu=False, verbose=False)
                cls._ocr_backend = 'easyocr'
            elif OCR_BACKEND == 'paddleocr':
                cls._ocr_instance = PaddleOCR(
                    use_angle_cls=True, 
                    lang='ch', 
                    show_log=False, 
                    use_gpu=False,
                    det_db_thresh=0.2,
                    det_db_box_thresh=0.4,
                    det_db_unclip_ratio=1.8
                )
                cls._ocr_backend = 'paddleocr'
        return cls._ocr_instance
    
    def _sort_by_position(self, results: List[dict]) -> List[dict]:
        """按位置排序：从上到下，从左到右"""
        return sorted(results, key=lambda r: (r['y'], r['x']))
    
    def _find_matches(self, results: List[dict], text: str, exact_match: bool = False) -> List[dict]:
        """查找匹配的文字"""
        if exact_match:
            matched = [r for r in results if r['text'] == text]
        else:
            matched = [r for r in results if text in r['text']]
        
        return self._sort_by_position(matched)
    
    def _merge_results(self, all_results: List[tuple]) -> List[dict]:
        """合并多次识别结果，去重并保留最佳置信度"""
        merged = {}
        
        for mode, results in all_results:
            for r in results:
                text = r['text']
                key = (r['x'], r['y'], text)
                
                if key not in merged or r['confidence'] > merged[key]['confidence']:
                    merged[key] = r
        
        return list(merged.values())
    
    def _do_ocr(self, img_array, ocr) -> List[dict]:
        """执行OCR识别"""
        results = []
        
        if self._ocr_backend == 'rapidocr':
            result, elapse = ocr(img_array)
            
            if result is not None and len(result) > 0:
                for item in result:
                    box = item[0]
                    text = item[1]
                    confidence = item[2]
                    
                    center_x = int((box[0][0] + box[2][0]) / 2)
                    center_y = int((box[0][1] + box[2][1]) / 2)
                    
                    results.append({
                        'text': text,
                        'confidence': float(confidence),
                        'x': center_x,
                        'y': center_y,
                        'box': box.tolist() if hasattr(box, 'tolist') else box
                    })
        
        elif self._ocr_backend == 'easyocr':
            easy_results = ocr.readtext(img_array)
            
            for detection in easy_results:
                box = detection[0]
                text = detection[1]
                confidence = detection[2]
                
                center_x = int((box[0][0] + box[2][0]) / 2)
                center_y = int((box[0][1] + box[2][1]) / 2)
                
                results.append({
                    'text': text,
                    'confidence': confidence,
                    'x': center_x,
                    'y': center_y,
                    'box': box
                })
        
        else:
            paddle_result = ocr.ocr(img_array, cls=True)
            
            if paddle_result and paddle_result[0]:
                for line in paddle_result[0]:
                    box = line[0]
                    text = line[1][0]
                    confidence = line[1][1]
                    
                    center_x = int((box[0][0] + box[2][0]) / 2)
                    center_y = int((box[0][1] + box[2][1]) / 2)
                    
                    results.append({
                        'text': text,
                        'confidence': confidence,
                        'x': center_x,
                        'y': center_y,
                        'box': box
                    })
        
        return results
    
    def execute(self, context, params: Dict[str, Any]) -> ActionResult:
        if not OCR_AVAILABLE:
            return ActionResult(
                success=False,
                message="OCR未安装，请运行: pip install rapidocr-onnxruntime"
            )
        
        region = params.get('region', None)
        click_text = params.get('click_text', None)
        click_index = params.get('click_index', 0)
        contains = params.get('contains', None)
        exact_match = params.get('exact_match', False)
        move_duration = params.get('move_duration', 0.2)
        preprocess_mode = params.get('preprocess_mode', 'auto')
        retry_count = params.get('retry_count', 3)
        
        try:
            screenshot = pyautogui.screenshot(region=region)
            
            import numpy as np
            img_array = np.array(screenshot)
            
            ocr = self.get_ocr()
            
            processed_images = preprocess_image_enhanced(img_array, preprocess_mode)
            
            all_results = []
            
            for mode, processed_img in processed_images[:retry_count]:
                results = self._do_ocr(processed_img, ocr)
                if results:
                    for r in results:
                        if region:
                            r['x'] += region[0]
                            r['y'] += region[1]
                    all_results.append((mode, results))
            
            if not all_results:
                return ActionResult(
                    success=True,
                    message="未识别到文字",
                    data={'text': '', 'results': [], 'found': False}
                )
            
            ocr_results = self._merge_results(all_results)
            
            if not ocr_results:
                return ActionResult(
                    success=True,
                    message="未识别到文字",
                    data={'text': '', 'results': [], 'found': False}
                )
            
            all_text = [r['text'] for r in ocr_results]
            full_text = '\n'.join(all_text)
            ocr_results = self._sort_by_position(ocr_results)
            
            if click_text:
                matched_results = self._find_matches(ocr_results, click_text, exact_match)
                
                if not matched_results:
                    display_text = full_text[:200] + '...' if len(full_text) > 200 else full_text
                    return ActionResult(
                        success=True,
                        message=f"未找到 '{click_text}' (精确匹配: {exact_match})，已识别 {len(ocr_results)} 项: {display_text}",
                        data={'text': full_text, 'results': ocr_results, 'found': False, 'count': len(ocr_results)}
                    )
                
                if click_index < 0:
                    click_index = 0
                elif click_index >= len(matched_results):
                    click_index = len(matched_results) - 1
                
                target = matched_results[click_index]
                pyautogui.moveTo(target['x'], target['y'], duration=move_duration)
                time.sleep(0.05)
                pyautogui.click(target['x'], target['y'])
                
                match_info = f"第{click_index + 1}个匹配项(共{len(matched_results)}个)"
                return ActionResult(
                    success=True,
                    message=f"点击成功: '{target['text']}' [{match_info}]",
                    data={
                        'text': full_text,
                        'results': ocr_results,
                        'clicked': target,
                        'found': True,
                        'match_count': len(matched_results),
                        'match_index': click_index
                    }
                )
            
            if contains:
                matched = self._find_matches(ocr_results, contains, exact_match)
                if matched:
                    return ActionResult(
                        success=True,
                        message=f"找到 {len(matched)} 个包含 '{contains}' 的文字",
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
                message=f"识别成功，共 {len(ocr_results)} 行文字 (使用{self._ocr_backend})",
                data={'text': full_text, 'results': ocr_results}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"OCR识别异常: {str(e)}",
                data={'found': False}
            )
    
    def get_required_params(self) -> list:
        return []
    
    def get_optional_params(self) -> dict:
        return {
            'region': None,
            'click_text': None,
            'click_index': 0,
            'exact_match': False,
            'contains': None,
            'move_duration': 0.2,
            'preprocess_mode': 'auto',
            'retry_count': 3
        }
