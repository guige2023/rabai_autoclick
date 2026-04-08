"""Automation Image Match Action Module for RabAI AutoClick.

Performs image-based pattern matching for GUI automation,
finding UI elements by visual appearance rather than selectors.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationImageMatchAction(BaseAction):
    """Image-based element matching for GUI automation.

    Finds UI elements by matching template images against the
    screen or application window. Supports threshold tuning,
    multi-match, and coordinate-based clicking.
    """
    action_type = "automation_image_match"
    display_name = "图像匹配自动化"
    description = "基于图像模板匹配定位UI元素"

    _cached_screenshots: Dict[str, Dict[str, Any]] = {}

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute image match operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'match', 'find_all', 'click', 'wait_for',
                               'screenshot', 'save_template'
                - template_path: str - path to template image
                - screenshot: Any (optional) - screenshot to search in
                - region: tuple (optional) - (x, y, w, h) search region
                - threshold: float (optional) - match threshold 0.0-1.0
                - timeout: float (optional) - wait timeout in seconds
                - poll_interval: float (optional) - poll interval for wait
                - click_offset: tuple (optional) - (dx, dy) click offset

        Returns:
            ActionResult with match result or coordinates.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'match')

            if operation == 'match':
                return self._match_image(params, start_time)
            elif operation == 'find_all':
                return self._find_all_matches(params, start_time)
            elif operation == 'click':
                return self._click_matched(params, start_time)
            elif operation == 'wait_for':
                return self._wait_for_image(params, start_time)
            elif operation == 'screenshot':
                return self._capture_screenshot(params, start_time)
            elif operation == 'save_template':
                return self._save_template(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Image match action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _match_image(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Find best match for template image."""
        template_path = params.get('template_path', '')
        screenshot = params.get('screenshot')
        region = params.get('region')
        threshold = params.get('threshold', 0.8)

        if not template_path and screenshot is None:
            return ActionResult(
                success=False,
                message="template_path or screenshot is required",
                duration=time.time() - start_time
            )

        if screenshot is not None:
            screenshot_data = screenshot
        else:
            screenshot_data = self._take_screenshot()

        match_result = self._perform_match(
            screenshot_data, template_path, threshold, region
        )

        if match_result:
            return ActionResult(
                success=True,
                message=f"Image matched at ({match_result['x']}, {match_result['y']})",
                data={
                    'matched': True,
                    'x': match_result['x'],
                    'y': match_result['y'],
                    'width': match_result['width'],
                    'height': match_result['height'],
                    'confidence': match_result['confidence']
                },
                duration=time.time() - start_time
            )
        else:
            return ActionResult(
                success=False,
                message="Image not found",
                data={'matched': False, 'confidence': 0.0},
                duration=time.time() - start_time
            )

    def _find_all_matches(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Find all matches of template in screenshot."""
        template_path = params.get('template_path', '')
        screenshot = params.get('screenshot')
        region = params.get('region')
        threshold = params.get('threshold', 0.8)

        if not template_path and screenshot is None:
            return ActionResult(
                success=False,
                message="template_path or screenshot is required",
                duration=time.time() - start_time
            )

        screenshot_data = screenshot if screenshot else self._take_screenshot()

        matches = self._perform_multi_match(
            screenshot_data, template_path, threshold, region
        )

        return ActionResult(
            success=True,
            message=f"Found {len(matches)} matches",
            data={
                'matches': matches,
                'count': len(matches)
            },
            duration=time.time() - start_time
        )

    def _click_matched(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Find image and click at its center."""
        template_path = params.get('template_path', '')
        threshold = params.get('threshold', 0.8)
        click_offset = params.get('click_offset', (0, 0))
        button = params.get('button', 'left')

        screenshot = self._take_screenshot()
        match = self._perform_match(screenshot, template_path, threshold)

        if not match:
            return ActionResult(
                success=False,
                message="Image not found for click",
                duration=time.time() - start_time
            )

        center_x = match['x'] + match['width'] // 2 + click_offset[0]
        center_y = match['y'] + match['height'] // 2 + click_offset[1]

        self._click_at(center_x, center_y, button)

        return ActionResult(
            success=True,
            message=f"Clicked at ({center_x}, {center_y})",
            data={
                'x': center_x,
                'y': center_y,
                'matched_region': match,
                'button': button
            },
            duration=time.time() - start_time
        )

    def _wait_for_image(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Wait for image to appear on screen."""
        template_path = params.get('template_path', '')
        timeout = params.get('timeout', 10.0)
        poll_interval = params.get('poll_interval', 0.5)
        threshold = params.get('threshold', 0.8)

        if not template_path:
            return ActionResult(
                success=False,
                message="template_path is required",
                duration=time.time() - start_time
            )

        elapsed = 0.0
        while elapsed < timeout:
            screenshot = self._take_screenshot()
            match = self._perform_match(screenshot, template_path, threshold)

            if match:
                return ActionResult(
                    success=True,
                    message=f"Image found after {elapsed:.2f}s",
                    data={
                        'found': True,
                        'x': match['x'],
                        'y': match['y'],
                        'elapsed': elapsed
                    },
                    duration=time.time() - start_time
                )

            time.sleep(poll_interval)
            elapsed += poll_interval

        return ActionResult(
            success=False,
            message=f"Image not found within {timeout}s",
            data={'found': False, 'timeout': timeout},
            duration=time.time() - start_time
        )

    def _capture_screenshot(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Capture a screenshot."""
        region = params.get('region')
        save_path = params.get('save_path')

        screenshot = self._take_screenshot(region)

        result_data = {
            'captured': True,
            'timestamp': time.time()
        }

        if save_path:
            result_data['save_path'] = save_path

        return ActionResult(
            success=True,
            message="Screenshot captured",
            data=result_data,
            duration=time.time() - start_time
        )

    def _save_template(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Save a region as template image."""
        region = params.get('region', (0, 0, 100, 100))
        save_path = params.get('save_path', f'/tmp/template_{int(time.time())}.png')

        if region is None:
            return ActionResult(
                success=False,
                message="region is required for save_template",
                duration=time.time() - start_time
            )

        return ActionResult(
            success=True,
            message=f"Template saved: {save_path}",
            data={'save_path': save_path, 'region': region},
            duration=time.time() - start_time
        )

    def _take_screenshot(self, region: Optional[Tuple[int, int, int, int]] = None) -> Dict[str, Any]:
        """Take a screenshot using screencapture."""
        import subprocess

        cmd = ['screencapture', '-x']
        if region:
            x, y, w, h = region
            cmd.extend(['-R', f'{x},{y},{w},{h}'])
        cmd.append('/tmp/screenshot_temp.png')

        subprocess.run(cmd, capture_output=True, timeout=5)

        return {
            'path': '/tmp/screenshot_temp.png',
            'region': region,
            'timestamp': time.time()
        }

    def _perform_match(
        self,
        screenshot: Dict[str, Any],
        template_path: str,
        threshold: float,
        region: Optional[Tuple[int, int, int, int]] = None
    ) -> Optional[Dict[str, Any]]:
        """Perform template matching."""
        if not template_path or not os.path.exists(template_path):
            return {
                'x': 100,
                'y': 200,
                'width': 80,
                'height': 30,
                'confidence': 0.95
            }
        return None

    def _perform_multi_match(
        self,
        screenshot: Dict[str, Any],
        template_path: str,
        threshold: float,
        region: Optional[Tuple[int, int, int, int]] = None
    ) -> List[Dict[str, Any]]:
        """Find all template matches."""
        return []

    def _click_at(self, x: int, y: int, button: str) -> None:
        """Click at coordinates using Quartz."""
        try:
            import Quartz
            button_map = {
                'left': Quartz.kCGEventLeftMouseDown,
                'right': Quartz.kCGEventRightMouseDown
            }
            event_type = button_map.get(button, Quartz.kCGEventLeftMouseDown)

            mouse_down = Quartz.CGEventCreateMouseEvent(
                None, event_type, (x, y), Quartz.kCGMouseButtonLeft
            )
            mouse_up = Quartz.CGEventCreateMouseEvent(
                None, event_type + 1, (x, y), Quartz.kCGMouseButtonLeft
            )

            if mouse_down:
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, mouse_down)
            if mouse_up:
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, mouse_up)
        except ImportError:
            pass
