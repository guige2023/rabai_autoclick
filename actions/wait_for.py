"""Wait-for action module for RabAI AutoClick.

Provides actions that wait for conditions before proceeding.
"""

import time
import re
from typing import Any, Dict, List, Optional

from rabai_autoclick.core.base_action import BaseAction, ActionResult


class WaitForImageAction(BaseAction):
    """Wait for a template image to appear on screen."""
    
    action_type = "wait_for_image"
    display_name = "等待图像"
    description = "等待指定图像出现在屏幕上，超时则失败"
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        template = params.get("template")
        timeout = params.get("timeout", 30)
        confidence = params.get("confidence", 0.8)
        check_interval = params.get("check_interval", 0.5)
        
        if not template:
            return ActionResult(success=False, message="template 参数是必需的")
        
        try:
            import cv2
            import numpy as np
        except ImportError:
            return ActionResult(success=False, message="opencv-python 未安装")
        
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                # Try to find template on screen using image_match logic
                screenshot_var = params.get("screenshot_var", "screenshot")
                screenshot = context.get(screenshot_var)
                
                if screenshot is None:
                    # Try to take a screenshot
                    import pyautogui
                    img = pyautogui.screenshot()
                    screenshot = cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)
                
                # Load template
                if isinstance(template, str):
                    template_img = cv2.imread(template)
                    if template_img is None:
                        return ActionResult(success=False, message=f"无法加载模板图像: {template}")
                else:
                    template_img = template
                
                # Template matching
                result = cv2.matchTemplate(screenshot, template_img, cv2.TM_CCOEFF_NORMED)
                _, max_val, _, _ = cv2.minMaxLoc(result)
                
                if max_val >= confidence:
                    return ActionResult(
                        success=True,
                        message=f"在置信度 {max_val:.2f} 找到图像",
                        output_var=params.get("output_var"),
                        data={"confidence": float(max_val)}
                    )
                
                time.sleep(check_interval)
            except Exception as e:
                return ActionResult(success=False, message=f"等待图像时出错: {str(e)}")
        
        return ActionResult(success=False, message=f"等待图像超时 ({timeout}s)")
    
    def get_required_params(self) -> List[str]:
        return ["template"]
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "timeout": 30,
            "confidence": 0.8,
            "check_interval": 0.5,
            "screenshot_var": "screenshot",
            "output_var": None
        }


class WaitForTextAction(BaseAction):
    """Wait for OCR text to appear on screen."""
    
    action_type = "wait_for_text"
    display_name = "等待文本"
    description = "等待 OCR 在屏幕上找到指定文本，超时则失败"
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        text = params.get("text")
        timeout = params.get("timeout", 30)
        confidence = params.get("confidence", 0.6)
        check_interval = params.get("check_interval", 1.0)
        
        if not text:
            return ActionResult(success=False, message="text 参数是必需的")
        
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                # Try to get OCR result from context
                ocr_result_var = params.get("ocr_result_var")
                
                if ocr_result_var:
                    ocr_result = context.get(ocr_result_var)
                else:
                    # Perform OCR on current screen
                    import pyautogui
                    from PIL import Image
                    import numpy as np
                    
                    img = pyautogui.screenshot()
                    img_array = np.array(img)
                    
                    # Use RapidOCR if available
                    try:
                        from rapidocr_onnxruntime import RapidOCR
                        ocr_engine = RapidOCR()
                        result, _ = ocr_engine(img_array)
                        
                        if result:
                            all_text = " ".join([item[1] for item in result])
                            ocr_result = all_text
                        else:
                            ocr_result = ""
                    except ImportError:
                        return ActionResult(success=False, message="rapidocr-onnxruntime 未安装")
                
                # Check if text is found (case-insensitive substring match)
                if text.lower() in ocr_result.lower():
                    # Find position
                    try:
                        from rapidocr_onnxruntime import RapidOCR
                        ocr_engine = RapidOCR()
                        result, _ = ocr_engine(np.array(pyautogui.screenshot()))
                        if result:
                            for item in result:
                                if text.lower() in item[1].lower():
                                    return ActionResult(
                                        success=True,
                                        message=f"找到文本: {text}",
                                        output_var=params.get("output_var"),
                                        data={"text": item[1], "position": [item[0][0], item[0][1]]}
                                    )
                    except:
                        pass
                    
                    return ActionResult(
                        success=True,
                        message=f"找到文本: {text}",
                        output_var=params.get("output_var")
                    )
                
                time.sleep(check_interval)
            except Exception as e:
                return ActionResult(success=False, message=f"等待文本时出错: {str(e)}")
        
        return ActionResult(success=False, message=f"等待文本超时 ({timeout}s): 未找到 '{text}'")
    
    def get_required_params(self) -> List[str]:
        return ["text"]
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "timeout": 30,
            "confidence": 0.6,
            "check_interval": 1.0,
            "ocr_result_var": None,
            "output_var": None
        }


class WaitForElementAction(BaseAction):
    """Wait for a UI element to be in a specific state."""
    
    action_type = "wait_for_element"
    display_name = "等待元素"
    description = "等待 UI 元素达到指定状态（可见/隐藏/启用/禁用）"
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        element_id = params.get("element_id")
        state = params.get("state", "visible")  # visible, hidden, enabled, disabled
        timeout = params.get("timeout", 30)
        check_interval = params.get("check_interval", 0.5)
        
        if not element_id:
            return ActionResult(success=False, message="element_id 参数是必需的")
        
        from rabai_autoclick.utils.accessibility_utils import get_element_state
        
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                element_state = get_element_state(element_id)
                
                if element_state is None:
                    time.sleep(check_interval)
                    continue
                
                desired_met = {
                    "visible": element_state.get("visible", False),
                    "hidden": not element_state.get("visible", True),
                    "enabled": element_state.get("enabled", False),
                    "disabled": not element_state.get("enabled", True),
                }.get(state, False)
                
                if desired_met:
                    return ActionResult(
                        success=True,
                        message=f"元素 {element_id} 状态为 {state}",
                        output_var=params.get("output_var"),
                        data=element_state
                    )
                
                time.sleep(check_interval)
            except Exception as e:
                return ActionResult(success=False, message=f"等待元素时出错: {str(e)}")
        
        return ActionResult(success=False, message=f"等待元素 {element_id} 超时，状态仍非 {state}")
    
    def get_required_params(self) -> List[str]:
        return ["element_id"]
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "state": "visible",
            "timeout": 30,
            "check_interval": 0.5,
            "output_var": None
        }
