"""Wait action module for RabAI AutoClick.

Provides wait/delay actions:
- WaitAction: Fixed duration wait
- WaitForImageAction: Wait until an image appears or disappears
- WaitForConditionAction: Wait for a custom condition to be met
"""

import time
import pyautogui
import cv2
import numpy as np
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


# Default confidence for image matching
DEFAULT_CONFIDENCE: float = 0.8


class WaitAction(BaseAction):
    """Wait for a fixed duration."""
    action_type = "wait"
    display_name = "等待"
    description = "等待指定的时间长度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a fixed duration wait.

        Args:
            context: Execution context.
            params: Dict with duration (seconds).

        Returns:
            ActionResult indicating success.
        """
        duration = params.get('duration', 1.0)

        valid, msg = self.validate_type(duration, (int, float), 'duration')
        if not valid:
            return ActionResult(success=False, message=msg)

        if duration < 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'duration' must be >= 0, got {duration}"
            )

        try:
            time.sleep(duration)
            return ActionResult(
                success=True,
                message=f"等待完成: {duration}秒",
                data={'duration': duration}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"等待失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'duration': 1.0}


class WaitForImageAction(BaseAction):
    """Wait until an image appears or disappears on screen."""
    action_type = "wait_for_image"
    display_name = "等待图像"
    description = "等待图像出现在屏幕上或从屏幕消失"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a wait for image action.

        Args:
            context: Execution context.
            params: Dict with template, timeout, confidence,
                   disappear (bool), check_interval, region.

        Returns:
            ActionResult with success/failure and position if found.
        """
        template_path = params.get('template', '')
        timeout = params.get('timeout', 10.0)
        confidence = params.get('confidence', DEFAULT_CONFIDENCE)
        disappear = params.get('disappear', False)
        check_interval = params.get('check_interval', 0.5)
        region = params.get('region', None)

        # Validate template
        if not template_path:
            return ActionResult(
                success=False,
                message="未指定模板图片路径"
            )
        valid, msg = self.validate_type(template_path, str, 'template')
        if not valid:
            return ActionResult(success=False, message=msg)

        if not Path(template_path).exists():
            return ActionResult(
                success=False,
                message=f"模板图片不存在: {template_path}"
            )

        # Validate timeout
        valid, msg = self.validate_type(timeout, (int, float), 'timeout')
        if not valid:
            return ActionResult(success=False, message=msg)
        if timeout <= 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'timeout' must be > 0, got {timeout}"
            )

        # Validate check_interval
        valid, msg = self.validate_type(check_interval, (int, float), 'check_interval')
        if not valid:
            return ActionResult(success=False, message=msg)
        if check_interval <= 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'check_interval' must be > 0, got {check_interval}"
            )

        # Validate confidence
        valid, msg = self.validate_type(confidence, (int, float), 'confidence')
        if not valid:
            return ActionResult(success=False, message=msg)
        valid, msg = self.validate_range(confidence, 0.0, 1.0, 'confidence')
        if not valid:
            return ActionResult(success=False, message=msg)

        # Validate disappear
        valid, msg = self.validate_type(disappear, bool, 'disappear')
        if not valid:
            return ActionResult(success=False, message=msg)

        start_time = time.time()

        try:
            while True:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    state = "消失" if disappear else "出现"
                    return ActionResult(
                        success=False,
                        message=f"等待图像{state}超时: {template_path}"
                    )

                position = self._find_image(template_path, confidence, region)
                found = position is not None

                if disappear and not found:
                    return ActionResult(
                        success=True,
                        message=f"图像已消失: {template_path}",
                        data={'elapsed': elapsed}
                    )
                if not disappear and found:
                    return ActionResult(
                        success=True,
                        message=f"找到图像: {position}",
                        data={'position': position, 'elapsed': elapsed}
                    )

                time.sleep(check_interval)

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"等待图像失败: {str(e)}"
            )

    def _find_image(
        self,
        template_path: str,
        confidence: float,
        region: Optional[Tuple[int, int, int, int]] = None
    ) -> Optional[Tuple[int, int]]:
        """Find image template on screen.

        Args:
            template_path: Path to template image.
            confidence: Match confidence threshold.
            region: Optional screen region.

        Returns:
            Tuple of (x, y) if found, else None.
        """
        try:
            screenshot = pyautogui.screenshot(region=region)
            screenshot_cv = cv2.cvtColor(np.array(screenshot), cv2.COLOR_RGB2BGR)

            template = cv2.imread(template_path)
            if template is None:
                return None

            template_gray = cv2.cvtColor(template, cv2.COLOR_BGR2GRAY)
            gray_screenshot = cv2.cvtColor(screenshot_cv, cv2.COLOR_BGR2GRAY)

            result = cv2.matchTemplate(
                gray_screenshot, template_gray, cv2.TM_CCOEFF_NORMED
            )
            min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(result)

            if max_val >= confidence:
                h, w = template_gray.shape
                center_x = max_loc[0] + w // 2
                center_y = max_loc[1] + h // 2

                if region:
                    center_x += region[0]
                    center_y += region[1]

                return (center_x, center_y)
        except Exception:
            pass

        return None

    def get_required_params(self) -> List[str]:
        return ['template']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'timeout': 10.0,
            'confidence': DEFAULT_CONFIDENCE,
            'disappear': False,
            'check_interval': 0.5,
            'region': None
        }


class WaitForConditionAction(BaseAction):
    """Wait for a custom condition expression to be evaluated as true."""
    action_type = "wait_for_condition"
    display_name = "等待条件"
    description = "等待条件表达式满足"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a wait for condition action.

        Args:
            context: Execution context.
            params: Dict with condition (expr string), timeout,
                   check_interval, negate (bool).

        Returns:
            ActionResult indicating if condition was met.
        """
        condition = params.get('condition', '')
        timeout = params.get('timeout', 10.0)
        check_interval = params.get('check_interval', 0.5)
        negate = params.get('negate', False)

        # Validate condition
        if not condition:
            return ActionResult(
                success=False,
                message="未指定条件表达式"
            )
        valid, msg = self.validate_type(condition, str, 'condition')
        if not valid:
            return ActionResult(success=False, message=msg)

        # Validate timeout
        valid, msg = self.validate_type(timeout, (int, float), 'timeout')
        if not valid:
            return ActionResult(success=False, message=msg)
        if timeout <= 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'timeout' must be > 0, got {timeout}"
            )

        # Validate check_interval
        valid, msg = self.validate_type(check_interval, (int, float), 'check_interval')
        if not valid:
            return ActionResult(success=False, message=msg)
        if check_interval <= 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'check_interval' must be > 0, got {check_interval}"
            )

        # Validate negate
        valid, msg = self.validate_type(negate, bool, 'negate')
        if not valid:
            return ActionResult(success=False, message=msg)

        start_time = time.time()

        try:
            while True:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    return ActionResult(
                        success=False,
                        message=f"等待条件满足超时: {condition}"
                    )

                try:
                    result = context.safe_exec(condition)
                    condition_met = bool(result)
                except Exception:
                    condition_met = False

                if negate:
                    condition_met = not condition_met

                if condition_met:
                    return ActionResult(
                        success=True,
                        message=f"条件满足: {condition}",
                        data={'elapsed': elapsed, 'result': result}
                    )

                time.sleep(check_interval)

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"等待条件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['condition']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'timeout': 10.0,
            'check_interval': 0.5,
            'negate': False
        }